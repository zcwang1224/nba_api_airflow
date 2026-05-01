"""
根據比賽 ID 取得該場比賽所有出手數據

使用 ShotChartDetail（支援歷史比賽），回傳每次出手的：
球員、隊伍、節次、時鐘、出手類型、投籃區域、座標、是否命中

用法：
    python get_game_shots.py 0022401185                      # 單場比賽，只印摘要
    python get_game_shots.py 0022401185 0022401186           # 多場比賽
    python get_game_shots.py 0022401185 --mongo              # 寫入 MongoDB
    python get_game_shots.py 0022401185 --output shots.csv   # 輸出 CSV
"""

import argparse
import os
from datetime import datetime

import pandas as pd
from nba_api.stats.endpoints import shotchartdetail

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
    "EVENT_TYPE",       # Made Shot / Missed Shot
    "ACTION_TYPE",      # Jump Shot / Layup Shot / Dunk Shot …
    "SHOT_TYPE",        # 2PT Field Goal / 3PT Field Goal
    "SHOT_ZONE_BASIC",  # Mid-Range / In The Paint / Above the Break 3 …
    "SHOT_ZONE_AREA",   # Left Side / Right Side Center …
    "SHOT_ZONE_RANGE",  # Less Than 8 ft. / 8-16 ft. …
    "SHOT_DISTANCE",    # 距離籃框英尺數
    "LOC_X",            # 球場 X 座標（以籃框為原點）
    "LOC_Y",            # 球場 Y 座標
    "SHOT_ATTEMPTED_FLAG",
    "SHOT_MADE_FLAG",
    "GAME_DATE",
    "HTM",              # 主隊縮寫
    "VTM",              # 客隊縮寫
]


def fetch_game_shots(game_id: str) -> pd.DataFrame:
    """
    取得單場比賽所有出手紀錄。

    Args:
        game_id: NBA 比賽 ID，例如 '0022401185'

    回傳 DataFrame，欄位見 SHOT_COLS。
    """
    # team_id=0 + player_id=0 + game_id_nullable = 全場所有球員出手
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

    # 統一型別
    for col in ("SHOT_ATTEMPTED_FLAG", "SHOT_MADE_FLAG", "SHOT_DISTANCE",
                "LOC_X", "LOC_Y", "PLAYER_ID", "TEAM_ID",
                "PERIOD", "MINUTES_REMAINING", "SECONDS_REMAINING", "GAME_EVENT_ID"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.reset_index(drop=True)


def fetch_multiple_games(game_ids: list[str]) -> pd.DataFrame:
    """批次取得多場比賽出手紀錄"""
    frames: list[pd.DataFrame] = []
    for game_id in game_ids:
        print(f"  正在爬取比賽 {game_id}...")
        df = fetch_game_shots(game_id)
        if df.empty:
            print(f"    → 無出手資料（比賽 ID 可能有誤）")
        else:
            print(f"    → 取得 {len(df)} 筆出手紀錄")
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def save_to_mongo(df: pd.DataFrame) -> None:
    """將出手資料 upsert 至 MongoDB nba.shots，以 (game_id, game_event_id) 為唯一鍵。"""
    from pymongo import MongoClient, UpdateOne

    updated_at = datetime.utcnow().isoformat()
    records = df.to_dict("records")
    for r in records:
        r["updated_at"] = updated_at
        # pandas int64 → Python int，避免 MongoDB 序列化問題
        for k, v in r.items():
            if hasattr(v, "item"):
                r[k] = v.item()

    client = MongoClient(MONGO_URI)
    collection = client["nba"]["shots"]

    operations = [
        UpdateOne(
            {"GAME_ID": r["GAME_ID"], "GAME_EVENT_ID": r["GAME_EVENT_ID"]},
            {"$set": r},
            upsert=True,
        )
        for r in records
    ]
    result = collection.bulk_write(operations)
    client.close()

    print(
        f"MongoDB 寫入完成 — upserted: {result.upserted_count}, "
        f"modified: {result.modified_count}, matched: {result.matched_count}"
    )


def print_summary(df: pd.DataFrame) -> None:
    """印出出手摘要"""
    if df.empty:
        print("無出手資料")
        return

    total = len(df)
    made = int(df["SHOT_MADE_FLAG"].sum())
    print(f"\n{'─' * 68}")
    print(f"  總出手：{total}　命中：{made}　命中率：{made/total:.1%}")

    if "GAME_ID" in df.columns:
        for game_id, gdf in df.groupby("GAME_ID"):
            date = gdf["GAME_DATE"].iloc[0] if "GAME_DATE" in gdf.columns else ""
            htm = gdf["HTM"].iloc[0] if "HTM" in gdf.columns else ""
            vtm = gdf["VTM"].iloc[0] if "VTM" in gdf.columns else ""
            g_total = len(gdf)
            g_made = int(gdf["SHOT_MADE_FLAG"].sum())
            print(f"\n  [{date}] {vtm} @ {htm}  (game_id={game_id})")
            print(f"  全場出手 {g_total} 次，命中 {g_made} 次（{g_made/g_total:.1%}）")

            # 依球員統計
            player_stat = (
                gdf.groupby(["PLAYER_NAME", "TEAM_NAME"])
                .agg(
                    出手=("SHOT_ATTEMPTED_FLAG", "sum"),
                    命中=("SHOT_MADE_FLAG", "sum"),
                )
                .assign(命中率=lambda d: (d["命中"] / d["出手"]).map("{:.1%}".format))
                .sort_values("出手", ascending=False)
            )
            print(f"\n  球員出手排行（前 10）：")
            print(f"  {'球員':<22} {'隊伍':<24} {'出手':>4} {'命中':>4} {'命中率':>6}")
            print(f"  {'─'*60}")
            for (pname, tname), row in player_stat.head(10).iterrows():
                print(f"  {pname:<22} {tname:<24} {int(row['出手']):>4} {int(row['命中']):>4} {row['命中率']:>6}")

            # 依投籃區域統計
            if "SHOT_ZONE_BASIC" in gdf.columns:
                zone_stat = (
                    gdf.groupby("SHOT_ZONE_BASIC")
                    .agg(出手=("SHOT_ATTEMPTED_FLAG", "sum"), 命中=("SHOT_MADE_FLAG", "sum"))
                    .assign(命中率=lambda d: (d["命中"] / d["出手"]).map("{:.1%}".format))
                    .sort_values("出手", ascending=False)
                )
                print(f"\n  投籃區域分佈：")
                print(f"  {'區域':<28} {'出手':>4} {'命中':>4} {'命中率':>6}")
                print(f"  {'─'*44}")
                for zone, row in zone_stat.iterrows():
                    print(f"  {zone:<28} {int(row['出手']):>4} {int(row['命中']):>4} {row['命中率']:>6}")

    print(f"\n{'─' * 68}")


def main():
    parser = argparse.ArgumentParser(description="根據比賽 ID 取得所有出手數據")
    parser.add_argument(
        "game_ids",
        nargs="+",
        metavar="GAME_ID",
        help="一或多個 NBA 比賽 ID，例如 0022401185",
    )
    parser.add_argument(
        "--mongo",
        action="store_true",
        help=f"將結果 upsert 至 MongoDB nba.shots（URI: {MONGO_URI}）",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="FILE.csv",
        help="輸出 CSV 路徑",
    )

    args = parser.parse_args()

    print(f"NBA 比賽出手數據爬取")
    print(f"  比賽 ID：{', '.join(args.game_ids)}\n")

    df = fetch_multiple_games(args.game_ids)
    print_summary(df)

    if df.empty:
        return df

    if args.mongo:
        print(f"\n正在寫入 MongoDB（nba.shots）...")
        save_to_mongo(df)

    if args.output:
        df.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"\n已儲存至 {args.output}")

    return df


if __name__ == "__main__":
    main()
