"""
取得今日以前所有 NBA 比賽資訊
包含：比賽日期、比賽 ID、主客隊縮寫、主客隊比分

用法：
    python get_historical_games.py                              # 預設：當季至昨日，只印摘要
    python get_historical_games.py --seasons 2023-24 2024-25   # 指定多個球季
    python get_historical_games.py --from 2025-01-01           # 指定起始日期（自動推算球季）
    python get_historical_games.py --from 2025-01-01 --mongo   # 寫入 MongoDB
    python get_historical_games.py --output games.csv          # 輸出至 CSV
"""

import argparse
import os
from datetime import date, datetime, timedelta

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder

MONGO_URI = os.getenv("MONGO_URI", "mongodb://airflow:airflow@mongodb:27017/")


def _season_str(year: int) -> str:
    return f"{year}-{str(year + 1)[-2:]}"


def _current_season() -> str:
    today = date.today()
    year = today.year if today.month >= 10 else today.year - 1
    return _season_str(year)


def fetch_games_for_season(
    season: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """
    取得指定球季的所有比賽（常規賽 + 季後賽），每場整合成一列。

    回傳欄位：
        game_date, game_id, season, season_type,
        home_team, away_team, home_score, away_score, winner
    """
    rows: list[pd.DataFrame] = []

    for season_type in ("Regular Season", "Playoffs"):
        kwargs: dict = dict(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable="00",
        )
        if date_from:
            kwargs["date_from_nullable"] = date_from
        if date_to:
            kwargs["date_to_nullable"] = date_to

        df = leaguegamefinder.LeagueGameFinder(**kwargs).get_data_frames()[0]  # pylint: disable=unexpected-keyword-arg
        if not df.empty:
            df["season_type"] = season_type
            rows.append(df)

    if not rows:
        return pd.DataFrame()

    df = pd.concat(rows, ignore_index=True)

    # LeagueGameFinder 每場比賽有兩列（各隊一列）
    # MATCHUP 含 "vs." 表示主隊，含 "@" 表示客隊
    home_df = df[df["MATCHUP"].str.contains(r"vs\.")].set_index("GAME_ID")
    away_df = df[df["MATCHUP"].str.contains("@")].set_index("GAME_ID")

    common_ids = home_df.index.intersection(away_df.index)

    records: list[dict] = []
    for game_id in common_ids:
        h = home_df.loc[game_id]
        a = away_df.loc[game_id]

        home_score = int(h["PTS"]) if pd.notna(h["PTS"]) else None
        away_score = int(a["PTS"]) if pd.notna(a["PTS"]) else None

        if home_score is not None and away_score is not None:
            winner = h["TEAM_ABBREVIATION"] if home_score > away_score else a["TEAM_ABBREVIATION"]
        else:
            winner = None

        records.append({
            "game_date":   h["GAME_DATE"],
            "game_id":     game_id,
            "season":      season,
            "season_type": h.get("season_type", ""),
            "home_team":   h["TEAM_ABBREVIATION"],
            "away_team":   a["TEAM_ABBREVIATION"],
            "home_score":  home_score,
            "away_score":  away_score,
            "winner":      winner,
        })

    result = pd.DataFrame(records)
    if not result.empty:
        result = result.sort_values("game_date").reset_index(drop=True)
    return result


def fetch_all_historical_games(
    seasons: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """跨多個球季彙整比賽資訊"""
    all_frames: list[pd.DataFrame] = []

    for season in seasons:
        print(f"  正在爬取 {season} 球季資料...")
        df = fetch_games_for_season(season, date_from=date_from, date_to=date_to)
        if not df.empty:
            print(f"    → 取得 {len(df)} 場比賽")
            all_frames.append(df)
        else:
            print(f"    → 無資料")

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset="game_id").sort_values("game_date").reset_index(drop=True)
    return combined


def print_summary(df: pd.DataFrame) -> None:
    """印出比賽摘要表"""
    if df.empty:
        print("無比賽資料")
        return

    print(f"\n{'─' * 72}")
    print(f"  {'日期':<12} {'比賽ID':<12} {'客隊':>4}  {'比分':^9}  {'主隊':<4}  {'勝者':<4}  {'賽季類型'}")
    print(f"{'─' * 72}")

    for _, row in df.iterrows():
        score = f"{row['away_score']:>3} - {row['home_score']:<3}" if row["home_score"] is not None else "  -   "
        print(
            f"  {row['game_date']:<12} {row['game_id']:<12} "
            f"{row['away_team']:>4}  {score}  {row['home_team']:<4}  "
            f"{str(row['winner']):<4}  {row['season_type']}"
        )

    print(f"{'─' * 72}")
    print(f"  共 {len(df)} 場比賽")
    print(f"  日期範圍：{df['game_date'].min()} ～ {df['game_date'].max()}")

    by_type = df.groupby("season_type").size()
    for stype, cnt in by_type.items():
        print(f"  {stype}: {cnt} 場")


def save_to_mongo(records: list[dict]) -> None:
    """將比賽資料 upsert 至 MongoDB nba.games，以 game_id 為唯一鍵。"""
    from pymongo import MongoClient, UpdateOne

    client = MongoClient(MONGO_URI)
    collection = client["nba"]["games"]

    operations = [
        UpdateOne({"game_id": doc["game_id"]}, {"$set": doc}, upsert=True)
        for doc in records
    ]
    result = collection.bulk_write(operations)
    client.close()

    print(
        f"MongoDB 寫入完成 — upserted: {result.upserted_count}, "
        f"modified: {result.modified_count}, matched: {result.matched_count}"
    )


def _seasons_from_date_range(date_from: str | None, date_to: str) -> list[str]:
    """根據日期範圍自動推算涵蓋的球季清單。

    NBA 球季規則：每年 10 月開始，次年 6 月結束。
    例如 2025-01-01 屬於 2024-25 球季（2024 年 10 月開始）。
    """
    def season_year(d: date) -> int:
        return d.year if d.month >= 10 else d.year - 1

    end = datetime.strptime(date_to, "%Y-%m-%d").date()
    start = datetime.strptime(date_from, "%Y-%m-%d").date() if date_from else end

    start_year = season_year(start)
    end_year = season_year(end)
    return [_season_str(y) for y in range(start_year, end_year + 1)]


def main():
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    current_season = _current_season()

    parser = argparse.ArgumentParser(description="取得今日以前所有 NBA 比賽資訊")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=None,
        metavar="SEASON",
        help="球季清單，格式 YYYY-YY（不指定時依日期範圍自動推算）",
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        default=None,
        metavar="YYYY-MM-DD",
        help="起始日期（預設：球季開始）",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        default=yesterday,
        metavar="YYYY-MM-DD",
        help=f"截止日期（預設：昨日 {yesterday}）",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="FILE.csv",
        help="輸出 CSV 路徑（不指定則只印出摘要）",
    )
    parser.add_argument(
        "--mongo",
        action="store_true",
        help=f"將結果 upsert 至 MongoDB（URI: {MONGO_URI}）",
    )

    args = parser.parse_args()

    # 未指定球季時，從日期範圍自動推算
    if args.seasons:
        seasons = args.seasons
    else:
        seasons = _seasons_from_date_range(args.date_from, args.date_to)

    print(f"NBA 歷史比賽爬取")
    print(f"  球季：{', '.join(seasons)}")
    print(f"  截止：{args.date_to}")
    if args.date_from:
        print(f"  起始：{args.date_from}")
    print()

    df = fetch_all_historical_games(
        seasons=seasons,
        date_from=args.date_from,
        date_to=args.date_to,
    )

    print_summary(df)

    if df.empty:
        return df

    if args.mongo:
        updated_at = datetime.utcnow().isoformat()
        records = [{**row, "updated_at": updated_at} for row in df.to_dict("records")]
        print(f"\n正在寫入 MongoDB（nba.games）...")
        save_to_mongo(records)

    if args.output:
        df.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"\n已儲存至 {args.output}")

    return df


if __name__ == "__main__":
    main()
