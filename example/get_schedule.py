"""
NBA 對戰表範例程式
- 今日對戰表：Live Scoreboard（即時比分）
- 指定日期對戰表：ScoreboardV2
- 整季賽程：LeagueGameFinder
"""

import pandas as pd
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard
from nba_api.stats.endpoints import leaguegamefinder, scoreboardv3
from nba_api.stats.static import teams


def get_today_scoreboard() -> pd.DataFrame:
    """取得今日對戰表與即時比分（Live）

    回傳欄位：
        game_id, away_team, away_score, home_team, home_score,
        status, period, game_clock, game_time_utc
    """
    games = live_scoreboard.ScoreBoard().get_dict()["scoreboard"]["games"]
    rows = [
        {
            "game_id": g["gameId"],
            "away_team": g["awayTeam"]["teamTricode"],
            "away_score": g["awayTeam"]["score"],
            "home_team": g["homeTeam"]["teamTricode"],
            "home_score": g["homeTeam"]["score"],
            "status": g["gameStatusText"],
            "period": g["period"],
            "game_clock": g["gameClock"],
            "game_time_utc": g["gameTimeUTC"],
        }
        for g in games
    ]
    return pd.DataFrame(rows)


def get_date_scoreboard(game_date: str) -> pd.DataFrame:
    """取得指定日期的對戰表

    Args:
        game_date: 日期字串，格式 'YYYY-MM-DD'，例如 '2025-04-20'

    回傳欄位：
        game_id, away_team, home_team, status, game_time_utc,
        series_text, game_label
    """
    dfs = scoreboardv3.ScoreboardV3(game_date=game_date, league_id="00").get_data_frames()
    games_df = dfs[1]  # 每場比賽一列

    # gameCode 格式：YYYYMMDD/AWAYTEAMHOMETEAM，三碼球隊縮寫
    games_df = games_df.copy()
    games_df["away_team"] = games_df["gameCode"].str[-6:-3]
    games_df["home_team"] = games_df["gameCode"].str[-3:]

    cols = [
        "gameId",
        "away_team",
        "home_team",
        "gameStatusText",
        "gameTimeUTC",
        "seriesText",
        "gameLabel",
    ]
    existing = [c for c in cols if c in games_df.columns]
    return games_df[existing].rename(
        columns={
            "gameId": "game_id",
            "gameStatusText": "status",
            "gameTimeUTC": "game_time_utc",
            "seriesText": "series_text",
            "gameLabel": "game_label",
        }
    )


def get_season_schedule(
    season: str = "2024-25",
    season_type: str = "Regular Season",
    team_id: int | None = None,
) -> pd.DataFrame:
    """取得整季賽程（所有球隊或指定球隊）

    Args:
        season: 球季，例如 '2024-25'
        season_type: 'Regular Season' / 'Playoffs' / 'Pre Season'
        team_id: 指定球隊 ID，None 代表取全聯盟

    回傳欄位：
        GAME_ID, GAME_DATE, MATCHUP, WL, PTS, PLUS_MINUS ...
    """
    kwargs = dict(
        season_nullable=season,
        season_type_nullable=season_type,
        league_id_nullable="00",
    )
    if team_id is not None:
        kwargs["team_id_nullable"] = team_id

    df = leaguegamefinder.LeagueGameFinder(**kwargs).get_data_frames()[
        0
    ]  # pylint: disable=unexpected-keyword-arg
    return df.sort_values("GAME_DATE")


if __name__ == "__main__":
    # ── 1. 今日對戰表（Live，含即時比分）─────────────────────────────────────
    print("=== 今日對戰表 ===")
    today_df = get_today_scoreboard()
    if today_df.empty:
        print("今日無比賽")
    else:
        print(today_df.to_string(index=False))
    print()

    # ── 2. 指定日期對戰表 ─────────────────────────────────────────────────────
    date = "2026-04-29"
    print(f"=== {date} 對戰表 ===")
    date_df = get_date_scoreboard(date)
    print(date_df.to_string(index=False))
    print()

    # ── 3. 整季賽程（全聯盟）─────────────────────────────────────────────────
    SEASON = "2024-25"
    print(f"=== {SEASON} 常規賽賽程（前 10 場）===")
    schedule_df = get_season_schedule(season=SEASON)
    display_cols = ["GAME_ID", "GAME_DATE", "TEAM_ABBREVIATION", "MATCHUP", "WL", "PTS"]
    existing = [c for c in display_cols if c in schedule_df.columns]
    print(schedule_df[existing].head(10).to_string(index=False))
    print(f"\n共 {len(schedule_df)} 筆賽程紀錄\n")

    # ── 4. 指定球隊賽程（以 Boston Celtics 為例）─────────────────────────────
    team_name = "Boston Celtics"
    team = teams.find_teams_by_full_name(team_name)[0]
    print(f"=== {team_name} {SEASON} 常規賽賽程 ===")
    team_schedule = get_season_schedule(season=SEASON, team_id=team["id"])
    print(team_schedule[existing].to_string(index=False))
    print(f"\n共 {len(team_schedule)} 場")
