"""
批次爬取所有歷史比賽出手數據，寫入 MongoDB nba.shots

流程：
  1. 從 nba.games 讀取所有 game_id
  2. 比對 nba.shots 已存在的 game_id，跳過已完成的
  3. 逐場爬取，每場立即寫入 MongoDB
  4. 失敗自動重試，持續失敗則記錄並繼續下一場

用法：
    python fetch_all_shots.py                  # 爬取全部缺漏比賽
    python fetch_all_shots.py --dry-run        # 只顯示待爬數量，不實際執行
    python fetch_all_shots.py --sleep 0.8      # 調整每次請求間隔秒數（預設 0.6）
    python fetch_all_shots.py --limit 100      # 只爬前 N 場（測試用）
    python fetch_all_shots.py --season 2024-25 # 只爬特定球季

背景執行（nohup）：
    nohup python fetch_all_shots.py > /tmp/shots_progress.log 2>&1 &
    echo $! > /tmp/shots.pid        # 將 PID 存檔，方便後續查詢

    # 查看即時進度
    tail -f /tmp/shots_progress.log

    # 查詢 PID
    cat /tmp/shots.pid

    # 中止背景執行
    kill $(cat /tmp/shots.pid)
"""

import argparse
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from nba_api.stats.endpoints import shotchartdetail
from pymongo import MongoClient

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
    "GAME_EVENT_ID",
    "PLAYER_ID",
    "TEAM_ID",
    "PERIOD",
    "MINUTES_REMAINING",
    "SECONDS_REMAINING",
    "SHOT_DISTANCE",
    "SHOT_ATTEMPTED_FLAG",
    "SHOT_MADE_FLAG",
}
FLOAT_COLS = {"LOC_X", "LOC_Y"}


def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def load_pending_game_ids(season: str | None = None) -> list[str]:
    """從 nba.games 讀取 game_id，排除 nba.shots 已有資料的場次。"""
    client = get_mongo_client()
    db = client["nba"]

    query = {}
    if season:
        query["season"] = season

    all_ids = set(
        doc["game_id"]
        for doc in db["games"].find(query, {"game_id": 1, "_id": 0})
        if doc.get("game_id")
    )

    done_ids = set(
        doc["_id"]
        for doc in db["shots"].aggregate(
            [
                {"$group": {"_id": "$GAME_ID"}},
            ]
        )
    )

    client.close()

    pending = sorted(all_ids - done_ids)
    return pending


def fetch_game_shots(game_id: str) -> pd.DataFrame:
    """呼叫 ShotChartDetail 取得單場所有出手。"""
    sc = shotchartdetail.ShotChartDetail(  # pylint: disable=unexpected-keyword-arg
        team_id=0,
        player_id=0,
        game_id_nullable=game_id,
        context_measure_simple="FGA",
    )
    df = sc.get_data_frames()[0]
    if df.empty:
        return df

    existing = [c for c in SHOT_COLS if c in df.columns]
    df = df[existing].copy()

    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.reset_index(drop=True)


def insert_shots(collection, records: list[dict]) -> int:
    """批次插入出手資料，回傳實際插入筆數；重複資料自動略過。"""
    if not records:
        return 0
    result = collection.insert_many(records, ordered=False)
    return len(result.inserted_ids)


def to_python_types(record: dict) -> dict:
    """將 pandas 數值型別轉為 Python 原生型別，避免 MongoDB 序列化錯誤。"""
    out = {}
    for k, v in record.items():
        if hasattr(v, "item"):  # numpy / pandas scalar
            out[k] = v.item()
        elif pd.isna(v) if not isinstance(v, (str, list, dict)) else False:
            out[k] = None
        else:
            out[k] = v
    return out


def format_eta(seconds: float) -> str:
    td = timedelta(seconds=int(seconds))
    h, rem = divmod(td.seconds, 3600)
    m, s = divmod(rem, 60)
    if td.days:
        return f"{td.days}d {h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"


def run(
    pending: list[str],
    sleep_sec: float = 0.6,
    max_retries: int = 3,
    retry_sleep: float = 5.0,
) -> None:
    total = len(pending)
    client = get_mongo_client()
    collection = client["nba"]["shots"]
    failed: list[str] = []

    updated_at = datetime.utcnow().isoformat()
    start_time = time.time()

    print(f"\n開始爬取，共 {total} 場比賽待處理")
    print(f"每次間隔 {sleep_sec}s，預估總時間約 {format_eta(total * (sleep_sec + 1.2))}\n")

    for idx, game_id in enumerate(pending, 1):
        # 進度與 ETA
        elapsed = time.time() - start_time
        avg = elapsed / idx if idx > 1 else sleep_sec + 1.2
        eta = avg * (total - idx)
        pct = idx / total * 100
        print(f"[{idx:>6}/{total}] {pct:5.1f}%  ETA {format_eta(eta)}  game={game_id}")

        # 重試邏輯
        df = pd.DataFrame()
        for attempt in range(1, max_retries + 1):
            try:
                t0 = time.time()
                df = fetch_game_shots(game_id)
                fetch_ms = (time.time() - t0) * 1000
                break
            except Exception as e:
                if attempt < max_retries:
                    print(f"⚠ retry {attempt}/{max_retries} ({e})", end=" ")
                    time.sleep(retry_sleep * attempt)
                else:
                    print(f"✗ 失敗，跳過 ({e})")
                    failed.append(game_id)

        if df.empty:
            if game_id not in failed:
                print(f"→ 無出手資料（可能是明星賽或資料缺漏）  fetch {fetch_ms:.0f}ms")
            time.sleep(sleep_sec)
            continue
        print(f"→ 爬取 {len(df)} 筆出手資料  fetch {fetch_ms:.0f}ms")
        records = [to_python_types({**r, "updated_at": updated_at}) for r in df.to_dict("records")]
        t0 = time.time()
        inserted = insert_shots(collection, records)
        write_ms = (time.time() - t0) * 1000
        print(f"→ {inserted:>3}/{len(records)} 筆寫入  write {write_ms:.0f}ms")
        print("─" * 60)
        time.sleep(sleep_sec)

    client.close()
    elapsed_total = time.time() - start_time

    print(f"\n{'─' * 60}")
    print(f"完成！耗時 {format_eta(elapsed_total)}")
    print(f"成功：{total - len(failed)} 場　失敗：{len(failed)} 場")
    if failed:
        print(f"\n失敗的 game_id：")
        for gid in failed:
            print(f"  {gid}")


def main():
    parser = argparse.ArgumentParser(description="批次爬取所有歷史比賽出手數據")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只顯示待爬數量，不實際爬取",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.6,
        metavar="SEC",
        help="每次請求間隔秒數（預設 0.6）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="最多爬取 N 場（測試用）",
    )
    parser.add_argument(
        "--season",
        default=None,
        metavar="YYYY-YY",
        help="只爬指定球季（例如 2024-25）",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        metavar="N",
        help="失敗重試次數（預設 3）",
    )

    args = parser.parse_args()

    print("NBA 出手數據批次爬取")
    print(f"  MongoDB : {MONGO_URI}")
    if args.season:
        print(f"  球季篩選: {args.season}")
    print()

    print("正在從 MongoDB 計算待爬場次...")
    pending = load_pending_game_ids(season=args.season)

    if not pending:
        print("所有比賽出手數據已是最新，無需爬取。")
        return

    print(f"待爬取：{len(pending)} 場　（已完成場次自動跳過）")

    if args.dry_run:
        print("\n[dry-run] 未實際執行。移除 --dry-run 開始爬取。")
        return

    if args.limit:
        pending = pending[: args.limit]
        print(f"[limit] 僅處理前 {args.limit} 場")

    run(pending, sleep_sec=args.sleep, max_retries=args.retries)


if __name__ == "__main__":
    main()
