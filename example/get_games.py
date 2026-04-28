"""
NBA 比賽資訊範例程式
使用 nba_api 取得每日賽程、Box Score、球員數據與逐球記錄
"""

import pandas as pd
from nba_api.stats.endpoints import (
    boxscoretraditionalv3,
    leaguegamefinder,
    playbyplayv3,
    scoreboardv3,
)


def get_games_by_date(game_date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    取得指定日期的所有比賽資訊
    game_date 格式: 'YYYY-MM-DD'
    回傳 (比賽摘要, 兩隊分數)
    """
    sb = scoreboardv3.ScoreboardV3(game_date=game_date, league_id="00")
    games_df = sb.get_data_frames()[1]  # 比賽列表
    scores_df = sb.get_data_frames()[2]  # 各隊得分
    return games_df, scores_df


def get_game_leaders(game_date: str) -> pd.DataFrame:
    """取得指定日期每場比賽的當日最佳球員"""
    sb = scoreboardv3.ScoreboardV3(game_date=game_date, league_id="00")
    return sb.get_data_frames()[3]


def get_player_boxscore(game_id: str) -> pd.DataFrame:
    """取得比賽的球員 Box Score（個人數據）"""
    bt = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
    df = bt.get_data_frames()[0]
    cols = [
        "gameId",
        "teamTricode",
        "nameI",
        "position",
        "jerseyNum",
        "minutes",
        "points",
        "reboundsTotal",
        "assists",
        "fieldGoalsMade",
        "fieldGoalsAttempted",
        "fieldGoalsPercentage",
        "threePointersMade",
        "threePointersAttempted",
        "freeThrowsMade",
        "freeThrowsAttempted",
        "steals",
        "blocks",
        "turnovers",
        "plusMinusPoints",
    ]
    return df[cols]


def get_team_boxscore(game_id: str) -> pd.DataFrame:
    """取得比賽的球隊 Box Score（整隊數據）"""
    bt = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
    df = bt.get_data_frames()[2]
    cols = [
        "teamTricode",
        "points",
        "reboundsTotal",
        "assists",
        "fieldGoalsMade",
        "fieldGoalsAttempted",
        "fieldGoalsPercentage",
        "threePointersMade",
        "threePointersAttempted",
        "threePointersPercentage",
        "freeThrowsMade",
        "freeThrowsAttempted",
        "freeThrowsPercentage",
        "steals",
        "blocks",
        "turnovers",
    ]
    return df[cols]


def get_play_by_play(game_id: str, period: int | None = None) -> pd.DataFrame:
    """
    取得比賽逐球記錄
    period: 指定節次（1-4），None 則回傳全場
    """
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
        "subType",
        "description",
        "scoreHome",
        "scoreAway",
    ]
    return df[cols]


def find_games(
    team_id: int,
    season: str = "2025-26",
    season_type: str = "Regular Season",
) -> pd.DataFrame:
    """搜尋指定球隊在某球季的所有比賽"""
    lgf = leaguegamefinder.LeagueGameFinder(
        team_id_nullable=team_id,
        season_nullable=season,
        season_type_nullable=season_type,
    )
    df = lgf.get_data_frames()[0]
    cols = [
        "GAME_ID",
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
    return df[cols]


if __name__ == "__main__":
    # 1. 指定日期賽程
    date = "2025-04-11"
    print(f"=== {date} 賽程 ===")
    games_df, scores_df = get_games_by_date(date)
    # 合併比賽狀態與分數
    merged = scores_df.merge(games_df[["gameId", "gameStatusText"]], on="gameId").sort_values(
        ["gameId", "teamId"]
    )
    for game_id, grp in merged.groupby("gameId"):
        teams = grp[["teamTricode", "score", "wins", "losses"]].values
        t1, t2 = teams[0], teams[1]
        status = grp["gameStatusText"].iloc[0]
        print(f"  {t1[0]} {t1[1]:>3}  vs  {t2[1]:>3} {t2[0]}   ({t2[2]}-{t2[3]})  [{status}]")
    print()

    # 2. 當日最佳球員
    print(f"=== {date} 各場最佳球員 ===")
    leaders_df = get_game_leaders(date)
    for _, row in leaders_df.iterrows():
        print(
            f"  [{row['teamTricode']}] {row['name']:22s}"
            f"  PTS {row['points']}  REB {row['rebounds']}  AST {row['assists']}"
        )
    print()

    # 3. 取一場比賽做 Box Score 分析（Lakers vs Rockets）
    game_id = "0022401185"
    print(f"=== Game {game_id} 球隊 Box Score ===")
    team_bs = get_team_boxscore(game_id)
    print(team_bs.to_string(index=False))
    print()

    print(f"=== Game {game_id} 球員 Box Score（前 10 名依得分排序）===")
    player_bs = get_player_boxscore(game_id)
    top10 = player_bs.sort_values("points", ascending=False).head(10)
    print(
        top10[
            [
                "teamTricode",
                "nameI",
                "minutes",
                "points",
                "reboundsTotal",
                "assists",
                "plusMinusPoints",
            ]
        ].to_string(index=False)
    )
    print()

    # 4. 逐球記錄（第 1 節前 10 筆）
    print(f"=== Game {game_id} 第 1 節逐球紀錄（前 10 筆）===")
    pbp_df = get_play_by_play(game_id, period=1)
    print(
        pbp_df.head(10)[
            ["clock", "teamTricode", "playerNameI", "description", "scoreHome", "scoreAway"]
        ].to_string(index=False)
    )
    print()

    # 5. 球隊全季比賽查詢（Lakers 最後 5 場）
    lakers_id = 1610612747
    print(f"=== Lakers 2025-26 例行賽最後 5 場 ===")
    season_games = find_games(lakers_id, season="2025-26")
    print(season_games.head(5).to_string(index=False))
