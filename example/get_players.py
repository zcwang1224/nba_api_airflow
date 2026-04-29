"""
NBA 球員資料範例程式
使用 nba_api 取得球員靜態資訊、詳細資料與生涯統計數據
"""

import pandas as pd
from nba_api.stats.endpoints import commonallplayers, commonplayerinfo, playercareerstats
from nba_api.stats.static import players


def get_all_players(active_only: bool = True) -> pd.DataFrame:
    """取得所有 NBA 球員靜態資料（本地快取，無需 API 請求）"""
    all_players = players.get_active_players() if active_only else players.get_players()
    df = pd.DataFrame(all_players)
    return df


def find_player_by_name(name: str) -> dict | None:
    """依姓名搜尋球員（支援全名或姓氏）"""
    results = players.find_players_by_full_name(name)
    if not results:
        results = players.find_players_by_last_name(name)
    if not results:
        results = players.find_players_by_first_name(name)
    return results[0] if results else None


def get_all_players_from_api(season: str = "2024-25") -> pd.DataFrame:
    """透過 API 取得指定球季所有球員（含球隊歸屬）"""
    resp = commonallplayers.CommonAllPlayers(
        is_only_current_season=1,
        league_id="00",
        season=season,
    )
    df = resp.get_data_frames()[0]
    cols = [
        "PERSON_ID",
        "DISPLAY_LAST_COMMA_FIRST",
        "ROSTERSTATUS",
        "FROM_YEAR",
        "TO_YEAR",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "TEAM_CITY",
        "TEAM_NAME",
    ]
    return df[cols]


def get_player_info(player_id: int) -> pd.DataFrame:
    """取得單一球員詳細資訊（生日、身高、體重、選秀等）"""
    info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
    df = info.get_data_frames()[0]
    cols = [
        "DISPLAY_FIRST_LAST",
        "TEAM_NAME",
        "JERSEY",
        "POSITION",
        "HEIGHT",
        "WEIGHT",
        "BIRTHDATE",
        "COUNTRY",
        "SCHOOL",
        "DRAFT_YEAR",
        "DRAFT_ROUND",
        "DRAFT_NUMBER",
        "SEASON_EXP",
    ]
    return df[cols]


def get_player_career_stats(player_id: int) -> pd.DataFrame:
    """取得球員生涯逐季常規賽統計數據"""
    career = playercareerstats.PlayerCareerStats(player_id=player_id)
    df = career.get_data_frames()[0]  # index 0 = 逐季數據
    cols = [
        "SEASON_ID",
        "TEAM_ABBREVIATION",
        "GP",
        "GS",
        "MIN",
        "FGM",
        "FGA",
        "FG_PCT",
        "FG3M",
        "FG3A",
        "FG3_PCT",
        "FTM",
        "FTA",
        "FT_PCT",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TOV",
        "PTS",
    ]
    return df[cols]


if __name__ == "__main__":
    # 1. 列出所有現役球員（本地快取，速度快）
    print("=== 現役 NBA 球員（靜態資料）===")
    active_df = get_all_players(active_only=True)
    print(active_df[["id", "full_name", "is_active"]].head(10).to_string(index=False))
    print(f"\n共 {len(active_df)} 位現役球員\n")

    # 2. 搜尋特定球員（以 LeBron James 為例）
    player_name = "LeBron James"
    print(f"=== 搜尋球員: {player_name} ===")
    player = find_player_by_name(player_name)
    if player is None:
        print(f"找不到球員: {player_name}")
        exit(1)
    print(f"球員姓名 : {player['full_name']}")
    print(f"球員 ID  : {player['id']}")
    print(f"是否現役 : {player['is_active']}\n")

    player_id = player["id"]

    # 3. 透過 API 取得本季所有球員及球隊歸屬
    season = "2024-25"
    print(f"=== {season} 球季球員名單（API，前 10 筆）===")
    api_players_df = get_all_players_from_api(season)
    print(api_players_df.head(10).to_string(index=False))
    print(f"\n共 {len(api_players_df)} 位球員\n")

    # 4. 球員詳細個人資訊
    print(f"=== {player['full_name']} 個人資訊 ===")
    info_df = get_player_info(player_id)
    for col in info_df.columns:
        print(f"{col:<25}: {info_df[col].values[0]}")
    print()

    # 5. 生涯逐季統計數據
    print(f"=== {player['full_name']} 生涯統計數據 ===")
    career_df = get_player_career_stats(player_id)
    display_cols = ["SEASON_ID", "TEAM_ABBREVIATION", "GP", "MIN", "PTS", "REB", "AST", "FG_PCT"]
    print(career_df[display_cols].to_string(index=False))
