"""
NBA 對戰資訊爬取 DAG
每天早上 9 點執行，將本季所有比賽對戰資訊（常規賽 + 季後賽）upsert 至 MongoDB
Collection: nba.games

每份文件以 game_id 為唯一鍵，包含主客隊資訊與雙方數據。
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

MONGO_URI = os.getenv("MONGO_URI", "mongodb://airflow:airflow@mongodb:27017/")
def _current_season() -> str:
    now = datetime.utcnow()
    year = now.year if now.month >= 10 else now.year - 1
    return f"{year}-{str(year + 1)[-2:]}"

SEASON = _current_season()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

GAME_STAT_COLS = [
    "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME",
    "WL", "MIN", "PTS",
    "FGM", "FGA", "FG_PCT",
    "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT",
    "OREB", "DREB", "REB",
    "AST", "STL", "BLK", "TOV", "PF", "PLUS_MINUS",
]


def fetch_games(**context) -> list[dict]:
    """爬取本季所有比賽，合併主客隊數據，推入 XCom"""
    import pandas as pd
    from nba_api.stats.endpoints import leaguegamefinder

    all_rows: list[pd.DataFrame] = []
    for season_type in ("Regular Season", "Playoffs"):
        df = leaguegamefinder.LeagueGameFinder(  # pylint: disable=unexpected-keyword-arg
            season_nullable=SEASON,
            season_type_nullable=season_type,
            league_id_nullable="00",
        ).get_data_frames()[0]
        if not df.empty:
            df["season_type"] = season_type
            all_rows.append(df)

    if not all_rows:
        print("無賽程資料")
        return []

    df = pd.concat(all_rows, ignore_index=True)

    # LeagueGameFinder 每場比賽有兩列（各隊一列）
    # MATCHUP 含 "vs." 為主隊，含 "@" 為客隊
    home_df = df[df["MATCHUP"].str.contains("vs\.")].copy()
    away_df = df[df["MATCHUP"].str.contains("@")].copy()

    home_df = home_df.set_index("GAME_ID")
    away_df = away_df.set_index("GAME_ID")

    def team_doc(row: pd.Series) -> dict:
        existing = [c for c in GAME_STAT_COLS if c in row.index]
        doc = {c.lower(): row[c] for c in existing}
        # 數值型別轉換
        for key in ("team_id", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
                    "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "pts"):
            if key in doc and doc[key] is not None:
                try:
                    doc[key] = int(doc[key])
                except (ValueError, TypeError):
                    pass
        for key in ("fg_pct", "fg3_pct", "ft_pct", "plus_minus", "min"):
            if key in doc and doc[key] is not None:
                try:
                    doc[key] = float(doc[key])
                except (ValueError, TypeError):
                    pass
        return doc

    records: list[dict] = []
    for game_id in home_df.index.intersection(away_df.index):
        home_row = home_df.loc[game_id]
        away_row = away_df.loc[game_id]
        records.append({
            "game_id": game_id,
            "game_date": home_row["GAME_DATE"],
            "season": SEASON,
            "season_type": home_row.get("season_type", ""),
            "matchup": home_row["MATCHUP"],
            "home_team": team_doc(home_row),
            "away_team": team_doc(away_row),
            "updated_at": datetime.utcnow().isoformat(),
        })

    print(f"爬取到 {len(records)} 場比賽對戰資料")
    return records


def save_to_mongo(**context):
    """將對戰資料 upsert 至 MongoDB nba.games"""
    from pymongo import MongoClient, UpdateOne

    records: list[dict] = context["ti"].xcom_pull(task_ids="fetch_games")
    if not records:
        print("無資料可寫入")
        return

    client = MongoClient(MONGO_URI)
    collection = client["nba"]["games"]

    operations = [
        UpdateOne(
            {"game_id": doc["game_id"]},
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
    dag_id="nba_games_mongo",
    default_args=default_args,
    description="每天爬取本季 NBA 所有對戰資訊並 upsert 至 MongoDB",
    schedule="0 9 * * *",  # 每天早上 9 點（前一晚比賽結果已入庫）
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "games", "mongodb"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_games",
        python_callable=fetch_games,
    )

    save_task = PythonOperator(
        task_id="save_to_mongo",
        python_callable=save_to_mongo,
    )

    fetch_task >> save_task
