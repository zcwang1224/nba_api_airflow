from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def fetch_nba_scores(**context):
    """從 NBA API 爬取比賽結果"""
    import requests

    date = context["ds"]
    url = f"https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"[{date}] 取得 {len(data.get('scoreboard', {}).get('games', []))} 場比賽資料")
    return data


def save_to_db(**context):
    """將爬取資料寫入 PostgreSQL"""
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="fetch_scores")
    games = data.get("scoreboard", {}).get("games", [])
    print(f"準備寫入 {len(games)} 筆比賽記錄")
    # TODO: 實作 DB 寫入邏輯


with DAG(
    dag_id="nba_data_pipeline",
    default_args=default_args,
    description="每日爬取 NBA 比賽數據並存入資料庫",
    schedule="0 8 * * *",  # 每天早上 8 點執行
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nba", "etl"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_scores",
        python_callable=fetch_nba_scores,
    )

    save_task = PythonOperator(
        task_id="save_to_db",
        python_callable=save_to_db,
    )

    fetch_task >> save_task
