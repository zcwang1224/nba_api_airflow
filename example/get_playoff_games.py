"""
NBA 季後賽比賽資訊範例程式
使用 nba_api 取得季後賽對戰結構、系列賽戰況、球隊數據與 Box Score
"""

import pandas as pd
from nba_api.stats.endpoints import (
    boxscoretraditionalv3,
    commonplayoffseries,
    leaguegamefinder,
    playbyplayv3,
)
from nba_api.stats.static import teams as teams_static

ROUND_NAMES = {
    1: "First Round",
    2: "Conference Semifinals",
    3: "Conference Finals",
    4: "NBA Finals",
}

# team_id -> abbreviation 對照表（避免重複建立）
_TEAM_MAP: dict[int, str] = {t["id"]: t["abbreviation"] for t in teams_static.get_teams()}


def _abbv(team_id: int) -> str:
    return _TEAM_MAP.get(team_id, str(team_id))


def get_playoff_bracket(season: str = "2025-26") -> pd.DataFrame:
    """
    取得整季季後賽對戰總覽（各輪各組的系列賽勝負）
    回傳欄位: round, round_name, series_id, home, away, home_wins, away_wins, games_played
    """
    # 取得每場比賽的勝負
    lgf = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        season_type_nullable="Playoffs",
        league_id_nullable="00",
    )
    games_df = lgf.get_data_frames()[0]

    # 取得系列賽結構（home/away 配置 per game）
    ps = commonplayoffseries.CommonPlayoffSeries(season=season, league_id="00")
    series_df = ps.get_data_frames()[0]
    series_df["round"] = series_df["SERIES_ID"].str[7].astype(int)

    # 每個 series 的主客場球隊（以 GAME_NUM=1 判斷種子隊主場）
    game1 = series_df[series_df["GAME_NUM"] == 1][
        ["SERIES_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "round"]
    ]

    # 計算每支球隊在每個 series 的勝場數
    # GAME_ID 的 [7:9] 標示 round+series_seq
    games_df["series_key"] = games_df["GAME_ID"].str[7:9]
    games_df["season_short"] = games_df["GAME_ID"].str[4:6]
    wins = (
        games_df[games_df["WL"] == "W"]
        .groupby(["TEAM_ID", "series_key"])
        .size()
        .reset_index(name="wins")
    )

    rows = []
    for _, row in game1.iterrows():
        sid = row["SERIES_ID"]
        rnd = row["round"]
        home_id = row["HOME_TEAM_ID"]
        away_id = row["VISITOR_TEAM_ID"]
        s_key = sid[7:9]  # e.g. "10" for round1 series0

        def _wins(team_id: int) -> int:
            mask = (wins["TEAM_ID"] == team_id) & (wins["series_key"] == s_key)
            w = wins.loc[mask, "wins"]
            return int(w.iloc[0]) if len(w) else 0

        home_w = _wins(home_id)
        away_w = _wins(away_id)
        total = home_w + away_w
        rows.append(
            {
                "round": rnd,
                "round_name": ROUND_NAMES.get(rnd, f"Round {rnd}"),
                "series_id": sid,
                "home": _abbv(home_id),
                "away": _abbv(away_id),
                "home_wins": home_w,
                "away_wins": away_w,
                "games_played": total,
            }
        )

    return pd.DataFrame(rows).sort_values(["round", "series_id"]).reset_index(drop=True)


def get_series_game_log(series_id: str, season: str = "2025-26") -> pd.DataFrame:
    """取得某系列賽每場比賽的比分與勝負（去重後按日期排序）"""
    lgf = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        season_type_nullable="Playoffs",
        league_id_nullable="00",
    )
    all_games = lgf.get_data_frames()[0]
    s_key = series_id[7:9]
    mask = all_games["GAME_ID"].str[7:9] == s_key
    series_games = all_games[mask].copy()

    # 每場比賽取兩隊資料，整合成一行
    agg = []
    for gid, grp in series_games.groupby("GAME_ID"):
        if len(grp) != 2:
            continue
        w = grp[grp["WL"] == "W"].iloc[0]
        l = grp[grp["WL"] == "L"].iloc[0]
        agg.append(
            {
                "game_id": gid,
                "game_date": w["GAME_DATE"],
                "winner": w["TEAM_ABBREVIATION"],
                "winner_pts": int(w["PTS"]),
                "loser": l["TEAM_ABBREVIATION"],
                "loser_pts": int(l["PTS"]),
            }
        )

    if not agg:
        return pd.DataFrame(columns=["game_id", "game_date", "winner", "winner_pts", "loser", "loser_pts"])
    return pd.DataFrame(agg).sort_values("game_date").reset_index(drop=True)


def get_team_playoff_games(team_id: int, season: str = "2025-26") -> pd.DataFrame:
    """取得球隊在季後賽的所有比賽數據"""
    lgf = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=team_id,
        season_nullable=season,
        season_type_nullable="Playoffs",
        league_id_nullable="00",
    )
    df = lgf.get_data_frames()[0]
    df["round"] = df["GAME_ID"].str[7].astype(int)
    df["round_name"] = df["round"].map(ROUND_NAMES)
    cols = [
        "round_name",
        "GAME_DATE",
        "MATCHUP",
        "WL",
        "PTS",
        "FG_PCT",
        "FG3_PCT",
        "REB",
        "AST",
        "TOV",
        "PLUS_MINUS",
    ]
    return df[cols].sort_values("GAME_DATE").reset_index(drop=True)


def get_player_boxscore(game_id: str) -> pd.DataFrame:
    """取得比賽球員 Box Score"""
    bt = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
    df = bt.get_data_frames()[0]
    cols = [
        "teamTricode",
        "nameI",
        "position",
        "minutes",
        "points",
        "reboundsTotal",
        "assists",
        "fieldGoalsMade",
        "fieldGoalsAttempted",
        "fieldGoalsPercentage",
        "threePointersMade",
        "threePointersAttempted",
        "steals",
        "blocks",
        "turnovers",
        "plusMinusPoints",
    ]
    return df[cols]


def get_play_by_play(game_id: str, period: int | None = None) -> pd.DataFrame:
    """取得比賽逐球紀錄，可指定節次"""
    pbp = playbyplayv3.PlayByPlayV3(game_id=game_id)
    df = pbp.get_data_frames()[0]
    if period is not None:
        df = df[df["period"] == period].reset_index(drop=True)
    cols = [
        "period",
        "clock",
        "teamTricode",
        "playerNameI",
        "actionType",
        "description",
        "scoreHome",
        "scoreAway",
    ]
    return df[cols]


if __name__ == "__main__":
    season = "2025-26"

    # 1. 完整季後賽對戰總覽
    print(f"=== {season} 季後賽對戰總覽 ===")
    bracket = get_playoff_bracket(season)
    for rnd, grp in bracket.groupby("round"):
        print(f"\n【{ROUND_NAMES[rnd]}】")
        for _, row in grp.iterrows():
            result = f"{row['home']} {row['home_wins']} - {row['away_wins']} {row['away']}"
            print(f"  {result:30s}  ({row['games_played']} games)")

    print()

    # 2. NBA Finals 系列賽逐場比分（從 bracket 動態取得 series_id）
    finals_rows = bracket[bracket["round"] == 4]
    if finals_rows.empty:
        print("=== NBA Finals 尚未開始或無資料 ===")
        print()
    else:
        finals_series_id = finals_rows.iloc[0]["series_id"]
        print(f"=== NBA Finals 系列賽逐場比分 ===")
        finals_log = get_series_game_log(finals_series_id, season)
        if finals_log.empty:
            print("  （尚無比賽紀錄）")
        else:
            for idx, row in finals_log.iterrows():
                print(
                    f"  Game {idx + 1}  {row['game_date']}  "
                    f"{row['winner']} {row['winner_pts']} - {row['loser_pts']} {row['loser']}"
                )
        print()

        # 3. Finals 冠軍隊季後賽所有比賽
        champ_abv = finals_rows.iloc[0]["home"] if finals_rows.iloc[0]["home_wins"] > finals_rows.iloc[0]["away_wins"] else finals_rows.iloc[0]["away"]
        champ_id = next((t["id"] for t in teams_static.get_teams() if t["abbreviation"] == champ_abv), None)
        print(f"=== {champ_abv} {season} 季後賽戰績 ===")
        if champ_id:
            champ_games = get_team_playoff_games(champ_id, season)
            print(champ_games.to_string(index=False))
        print()

        # 4. Finals 最後一場 Box Score（球員前 10）
        if not finals_log.empty:
            last_game_id = finals_log.iloc[-1]["game_id"]
            print(f"=== Finals 最終戰 ({last_game_id}) 球員數據 ===")
            player_bs = get_player_boxscore(last_game_id)
            top10 = player_bs.sort_values("points", ascending=False).head(10)
            print(
                top10[
                    ["teamTricode", "nameI", "minutes", "points",
                     "reboundsTotal", "assists", "plusMinusPoints"]
                ].to_string(index=False)
            )
            print()

            # 5. Finals 最終戰 第 4 節逐球（最後 10 筆）
            print(f"=== Finals 最終戰 第 4 節最後 10 筆逐球 ===")
            pbp = get_play_by_play(last_game_id, period=4)
            print(
                pbp.tail(10)[
                    ["clock", "teamTricode", "playerNameI", "description", "scoreHome", "scoreAway"]
                ].to_string(index=False)
            )
