"""
NBA 球隊資料範例程式
使用 nba_api 取得球隊靜態資訊、球隊詳細資料與本季戰績
"""

import pandas as pd
from nba_api.stats.endpoints import leaguestandings, teamgamelog, teaminfocommon
from nba_api.stats.static import teams


def get_all_teams() -> pd.DataFrame:
    """取得所有 NBA 球隊的靜態資料"""
    all_teams = teams.get_teams()
    df = pd.DataFrame(all_teams)
    return df


def find_team_by_name(name: str) -> dict | None:
    """依名稱（全名、縮寫或城市）搜尋球隊"""
    results = teams.find_teams_by_full_name(name)
    if not results:
        results = teams.find_teams_by_abbreviation(name.upper())
    if not results:
        results = teams.find_teams_by_city(name)
    return results[0] if results else None


def get_team_info(team_id: int) -> pd.DataFrame:
    """取得球隊詳細資訊（成立年份、主場、分區等）"""
    info = teaminfocommon.TeamInfoCommon(team_id=team_id)
    df = info.get_data_frames()[0]
    return df


def get_team_game_log(team_id: int, season: str = "2024-25") -> pd.DataFrame:
    """取得球隊本季比賽紀錄"""
    log = teamgamelog.TeamGameLog(team_id=team_id, season=season)
    df = log.get_data_frames()[0]
    return df


def get_league_standings(season: str = "2024-25") -> pd.DataFrame:
    """取得聯盟積分榜"""
    standings = leaguestandings.LeagueStandings(season=season)
    df = standings.get_data_frames()[0]
    cols = [
        "TeamID",
        "TeamCity",
        "TeamName",
        "Conference",
        "Division",
        "WINS",
        "LOSSES",
        "WinPCT",
        "ConferenceRecord",
        "HOME",
        "ROAD",
    ]
    return df[cols]


if __name__ == "__main__":
    # 1. 列出所有球隊
    print("=== 所有 NBA 球隊 ===")
    all_teams_df = get_all_teams()
    print(all_teams_df[["id", "full_name", "abbreviation", "city", "state"]].to_string(index=False))
    print(f"\n共 {len(all_teams_df)} 支球隊\n")

    # 2. 搜尋特定球隊（以 Lakers 為例）
    team_name = "Lakers"
    print(f"=== 搜尋球隊: {team_name} ===")
    team = find_team_by_name(team_name)
    if team is None:
        print(f"找不到球隊: {team_name}")
        exit(1)
    print(f"球隊名稱 : {team['full_name']}")
    print(f"縮寫     : {team['abbreviation']}")
    print(f"城市     : {team['city']}")
    print(f"球隊 ID  : {team['id']}\n")

    team_id = team["id"]

    # 3. 球隊詳細資訊
    print("=== 球隊詳細資訊 ===")
    team_info_df = get_team_info(team_id)
    info_cols = [
        "TEAM_NAME",
        "TEAM_CITY",
        "TEAM_CONFERENCE",
        "TEAM_DIVISION",
        "TEAM_CODE",
        "W",
        "L",
        "PCT",
        "CONF_RANK",
        "DIV_RANK",
    ]
    print(team_info_df[info_cols].to_string(index=False))
    print()

    # 4. 本季比賽紀錄（最近 10 場）
    season = "2025-26"
    print(f"=== {team['full_name']} {season} 球季最近 10 場戰績 ===")
    game_log_df = get_team_game_log(team_id, season)
    recent = game_log_df.head(10)[["Game_ID", "GAME_DATE", "MATCHUP", "WL", "PTS", "REB", "AST"]]
    print(recent.to_string(index=False))
    print()

    # 5. 聯盟積分榜（東區前 5）
    print(f"=== {season} 東區積分榜前 5 ===")
    standings_df = get_league_standings(season)
    east = standings_df[standings_df["Conference"] == "East"].head(5)
    print(east.to_string(index=False))
