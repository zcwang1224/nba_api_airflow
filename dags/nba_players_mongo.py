"""
NBA 球員資料爬取 DAG
每天早上 8 點執行，將所有球員（含歷史退役）資訊與本季統計存入 MongoDB
Collection: nba.players
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

MONGO_URI = os.getenv("MONGO_URI", "mongodb://airflow:airflow@mongodb:27017/")
SEASON = "2024-25"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def fetch_players(**context) -> list[dict]:
    """爬取所有球員（含歷史退役）基本資料與球隊歸屬，合併後推入 XCom"""
    from nba_api.stats.endpoints import commonallplayers
    from nba_api.stats.static import players

    static = {p["id"]: p for p in players.get_players()}

    resp = commonallplayers.CommonAllPlayers(
        is_only_current_season=0,
        league_id="00",
        season=SEASON,
    )
    api_df = resp.get_data_frames()[0]

    wanted_cols = [
        "PERSON_ID",
        "DISPLAY_LAST_COMMA_FIRST",
        "ROSTERSTATUS",
        "FROM_YEAR",
        "TO_YEAR",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "TEAM_CITY",
        "TEAM_NAME",
    ]
    existing_cols = [c for c in wanted_cols if c in api_df.columns]
    api_records = {int(r["PERSON_ID"]): r for r in api_df[existing_cols].to_dict(orient="records")}

    records = []
    for player_id, info in static.items():
        doc = {
            "player_id": player_id,
            "full_name": info["full_name"],
            "first_name": info["first_name"],
            "last_name": info["last_name"],
            "is_active": info["is_active"],
            "season": SEASON,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if player_id in api_records:
            api = api_records[player_id]
            doc["roster_status"] = api.get("ROSTERSTATUS")
            doc["from_year"] = api.get("FROM_YEAR")
            doc["to_year"] = api.get("TO_YEAR")
            doc["team_id"] = int(api["TEAM_ID"]) if api.get("TEAM_ID") else None
            doc["team_abbreviation"] = api.get("TEAM_ABBREVIATION")
            doc["team_city"] = api.get("TEAM_CITY")
            doc["team_name"] = api.get("TEAM_NAME")
        records.append(doc)

    print(f"爬取到 {len(records)} 位球員資料（含歷史退役）")
    return records


def fetch_player_stats(**context) -> dict[int, dict]:
    """爬取本季球員場均數據，推入 XCom（以 player_id 為 key）"""
    from nba_api.stats.endpoints import leaguedashplayerstats

    stats_df = leaguedashplayerstats.LeagueDashPlayerStats(
        season=SEASON,
        per_mode_detailed="PerGame",
    ).get_data_frames()[0]

    stat_cols = [
        "PLAYER_ID",
        "GP",
        "MIN",
        "FGM",
        "FGA",
        "FG_PCT",
        "FG3M",
        "FG3A",
        "FG3_PCT",
        "FTM",
        "FTA",
        "FT_PCT",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TOV",
        "PTS",
    ]
    existing_cols = [c for c in stat_cols if c in stats_df.columns]
    stats = {int(r["PLAYER_ID"]): r for r in stats_df[existing_cols].to_dict(orient="records")}

    print(f"爬取到 {len(stats)} 位球員本季場均數據")
    return stats


def save_to_mongo(**context):
    """合併球員基本資料與本季場均數據，upsert 至 MongoDB nba.players"""
    from pymongo import MongoClient, UpdateOne

    ti = context["ti"]
    records: list[dict] = ti.xcom_pull(task_ids="fetch_players")
    stats: dict = ti.xcom_pull(task_ids="fetch_player_stats")

    if not records:
        print("無球員資料可寫入")
        return

    stats = stats or {}

    for doc in records:
        pid = doc["player_id"]
        if pid in stats:
            s = stats[pid]
            doc["stats"] = {
                "gp": s.get("GP"),
                "min": s.get("MIN"),
                "pts": s.get("PTS"),
                "reb": s.get("REB"),
                "ast": s.get("AST"),
                "stl": s.get("STL"),
                "blk": s.get("BLK"),
                "tov": s.get("TOV"),
                "fg_pct": s.get("FG_PCT"),
                "fg3_pct": s.get("FG3_PCT"),
                "ft_pct": s.get("FT_PCT"),
            }

    client = MongoClient(MONGO_URI)
    collection = client["nba"]["players"]

    operations = [
        UpdateOne(
            {"player_id": doc["player_id"]},
            {"$set": doc},
            upsert=True,
        )
        for doc in records
    ]
    result = collection.bulk_write(operations)
    client.close()

    print(
        f"MongoDB 寫入完成 — upserted: {result.upserted_count}, "
        f"modified: {result.modified_count}, matched: {result.matched_count}"
    )


with DAG(
    dag_id="nba_players_mongo",
    default_args=default_args,
    description="每天爬取 NBA 所有球員資料（含歷史退役）與本季場均數據並 upsert 至 MongoDB",
    schedule="0 8 * * *",  # 每天早上 8 點
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "players", "mongodb"],
) as dag:
    fetch_players_task = PythonOperator(
        task_id="fetch_players",
        python_callable=fetch_players,
    )

    fetch_stats_task = PythonOperator(
        task_id="fetch_player_stats",
        python_callable=fetch_player_stats,
    )

    save_task = PythonOperator(
        task_id="save_to_mongo",
        python_callable=save_to_mongo,
    )

    [fetch_players_task, fetch_stats_task] >> save_task
