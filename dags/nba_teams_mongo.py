"""
NBA 球隊資料爬取 DAG
每週一早上 6 點執行，將所有球隊靜態資訊與本季積分榜存入 MongoDB
Collection: nba.teams
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


def fetch_teams(**context) -> list[dict]:
    """爬取所有球隊靜態資料與本季積分榜，合併後推入 XCom"""
    from nba_api.stats.endpoints import leaguestandings
    from nba_api.stats.static import teams

    # 靜態基本資料
    static = {t["id"]: t for t in teams.get_teams()}

    # 本季積分榜
    standings_df = leaguestandings.LeagueStandings(season=SEASON).get_data_frames()[0]
    standings_cols = [
        "TeamID", "Conference", "Division",
        "WINS", "LOSSES", "WinPCT",
        "ConferenceRecord", "HOME", "ROAD",
        "L10", "ClinchedPlayoffBirth",
    ]
    # 只保留存在的欄位（防版本差異）
    existing_cols = [c for c in standings_cols if c in standings_df.columns]
    standings = standings_df[existing_cols].to_dict(orient="records")
    standings_map = {int(r["TeamID"]): r for r in standings}

    records = []
    for team_id, info in static.items():
        doc = {
            "team_id": team_id,
            "full_name": info["full_name"],
            "abbreviation": info["abbreviation"],
            "nickname": info["nickname"],
            "city": info["city"],
            "state": info["state"],
            "year_founded": info["year_founded"],
            "season": SEASON,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if team_id in standings_map:
            s = standings_map[team_id]
            doc["conference"] = s.get("Conference")
            doc["division"] = s.get("Division")
            doc["wins"] = s.get("WINS")
            doc["losses"] = s.get("LOSSES")
            doc["win_pct"] = s.get("WinPCT")
            doc["conference_record"] = s.get("ConferenceRecord")
            doc["home_record"] = s.get("HOME")
            doc["road_record"] = s.get("ROAD")
            doc["last_10"] = s.get("L10")
            doc["clinched_playoff"] = s.get("ClinchedPlayoffBirth")
        records.append(doc)

    print(f"爬取到 {len(records)} 支球隊資料")
    return records


def save_to_mongo(**context):
    """將球隊資料 upsert 至 MongoDB nba.teams"""
    from pymongo import MongoClient, UpdateOne

    records: list[dict] = context["ti"].xcom_pull(task_ids="fetch_teams")
    if not records:
        print("無資料可寫入")
        return

    client = MongoClient(MONGO_URI)
    collection = client["nba"]["teams"]

    operations = [
        UpdateOne(
            {"team_id": doc["team_id"]},
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
    dag_id="nba_teams_mongo",
    default_args=default_args,
    description="每週爬取 NBA 球隊資料並 upsert 至 MongoDB",
    schedule="0 6 * * 1",  # 每週一早上 6 點
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "teams", "mongodb"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_teams",
        python_callable=fetch_teams,
    )

    save_task = PythonOperator(
        task_id="save_to_mongo",
        python_callable=save_to_mongo,
    )

    fetch_task >> save_task
