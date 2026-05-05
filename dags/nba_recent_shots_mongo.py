"""
NBA 近期比賽出手數據 DAG
每小時執行，爬取今日前三日內比賽出手數據，upsert 至 MongoDB nba.shots
Collection: nba.shots
唯一鍵: (GAME_ID, GAME_EVENT_ID)
"""

import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

MONGO_URI = os.getenv("MONGO_URI", "mongodb://airflow:airflow@mongodb:27017/")

SHOT_COLS = [
    "GAME_ID",
    "GAME_EVENT_ID",
    "PLAYER_ID",
    "PLAYER_NAME",
    "TEAM_ID",
    "TEAM_NAME",
    "PERIOD",
    "MINUTES_REMAINING",
    "SECONDS_REMAINING",
    "EVENT_TYPE",
    "ACTION_TYPE",
    "SHOT_TYPE",
    "SHOT_ZONE_BASIC",
    "SHOT_ZONE_AREA",
    "SHOT_ZONE_RANGE",
    "SHOT_DISTANCE",
    "LOC_X",
    "LOC_Y",
    "SHOT_ATTEMPTED_FLAG",
    "SHOT_MADE_FLAG",
    "GAME_DATE",
    "HTM",
    "VTM",
]

INT_COLS = {
    "GAME_EVENT_ID", "PLAYER_ID", "TEAM_ID", "PERIOD",
    "MINUTES_REMAINING", "SECONDS_REMAINING",
    "SHOT_DISTANCE", "SHOT_ATTEMPTED_FLAG", "SHOT_MADE_FLAG",
}
FLOAT_COLS = {"LOC_X", "LOC_Y"}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_recent_game_ids(**context) -> list[str]:
    """從 MongoDB nba.games 查詢今日前三日內的比賽 game_id"""
    from pymongo import MongoClient

    cutoff = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    game_ids = [
        doc["game_id"]
        for doc in client["nba"]["games"].find(
            {"game_date": {"$gte": cutoff, "$lte": today}},
            {"game_id": 1, "_id": 0},
        )
        if doc.get("game_id")
    ]
    client.close()

    print(f"找到 {len(game_ids)} 場近期比賽（{cutoff} ~ {today}）")
    for gid in game_ids:
        print(f"  {gid}")
    return game_ids


def fetch_and_upsert_shots(**context) -> dict:
    """爬取近期比賽出手數據並 upsert 至 MongoDB nba.shots"""
    import pandas as pd
    from nba_api.stats.endpoints import shotchartdetail
    from pymongo import MongoClient, UpdateOne

    game_ids: list[str] = context["ti"].xcom_pull(task_ids="get_recent_game_ids")
    if not game_ids:
        print("無近期比賽，跳過")
        return {"upserted": 0, "modified": 0, "matched": 0}

    client = MongoClient(MONGO_URI)
    collection = client["nba"]["shots"]
    updated_at = datetime.utcnow().isoformat()

    total_upserted = total_modified = total_matched = 0

    for game_id in game_ids:
        print(f"爬取比賽 {game_id} 出手數據...")
        try:
            sc = shotchartdetail.ShotChartDetail(  # pylint: disable=unexpected-keyword-arg
                team_id=0,
                player_id=0,
                game_id_nullable=game_id,
                context_measure_simple="FGA",
            )
            df = sc.get_data_frames()[0]

            if df.empty:
                print(f"  → 比賽 {game_id} 無出手資料（尚未開打或資料缺漏）")
                time.sleep(1)
                continue

            existing_cols = [c for c in SHOT_COLS if c in df.columns]
            df = df[existing_cols].copy()

            for col in INT_COLS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            for col in FLOAT_COLS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            operations = []
            for r in df.to_dict("records"):
                clean = {}
                for k, v in r.items():
                    if hasattr(v, "item"):
                        clean[k] = v.item()
                    elif not isinstance(v, (str, list, dict)) and pd.isna(v):
                        clean[k] = None
                    else:
                        clean[k] = v
                clean["updated_at"] = updated_at
                operations.append(
                    UpdateOne(
                        {"GAME_ID": clean["GAME_ID"], "GAME_EVENT_ID": clean["GAME_EVENT_ID"]},
                        {"$set": clean},
                        upsert=True,
                    )
                )

            result = collection.bulk_write(operations)
            total_upserted += result.upserted_count
            total_modified += result.modified_count
            total_matched += result.matched_count
            print(
                f"  → {len(operations)} 筆出手  "
                f"upserted={result.upserted_count}  "
                f"modified={result.modified_count}  "
                f"matched={result.matched_count}"
            )

        except Exception as e:
            print(f"  ✗ 比賽 {game_id} 爬取失敗：{e}")

        time.sleep(1)

    client.close()
    summary = {
        "upserted": total_upserted,
        "modified": total_modified,
        "matched": total_matched,
    }
    print(f"\n全部完成 — {summary}")
    return summary


with DAG(
    dag_id="nba_recent_shots_mongo",
    default_args=default_args,
    description="每小時爬取今日前三日內 NBA 比賽出手數據並 upsert 至 MongoDB nba.shots",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "shots", "mongodb"],
) as dag:
    get_games_task = PythonOperator(
        task_id="get_recent_game_ids",
        python_callable=get_recent_game_ids,
    )

    fetch_shots_task = PythonOperator(
        task_id="fetch_and_upsert_shots",
        python_callable=fetch_and_upsert_shots,
    )

    get_games_task >> fetch_shots_task
