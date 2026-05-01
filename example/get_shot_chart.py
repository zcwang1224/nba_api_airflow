"""
NBA 出手紀錄範例程式
- 整季出手紀錄：使用 ShotChartDetail（球員 / 球隊）
- 單場 / 今日比賽出手紀錄：使用 Live PlayByPlay
"""

import pandas as pd
from nba_api.live.nba.endpoints import playbyplay, scoreboard
from nba_api.stats.endpoints import shotchartdetail
from nba_api.stats.static import players, teams


def get_player_shots(
    player_id: int,
    season: str = "2025-26",
    season_type: str = "Regular Season",
) -> pd.DataFrame:
    """取得單一球員整季所有出手紀錄"""
    shot_df, _ = shotchartdetail.ShotChartDetail(  # pylint: disable=unexpected-keyword-arg
        team_id=0,
        player_id=player_id,
        context_measure_simple="FGA",
        season_nullable=season,
        season_type_all_star=season_type,
    ).get_data_frames()
    return shot_df


def get_team_shots(
    team_id: int,
    season: str = "2025-26",
    season_type: str = "Regular Season",
) -> pd.DataFrame:
    """取得整支球隊整季所有出手紀錄（player_id=0 代表全隊）"""
    shot_df, _ = shotchartdetail.ShotChartDetail(  # pylint: disable=unexpected-keyword-arg
        team_id=team_id,
        player_id=0,
        context_measure_simple="FGA",
        season_nullable=season,
        season_type_all_star=season_type,
    ).get_data_frames()
    return shot_df


def get_game_shots(game_id: str) -> pd.DataFrame:
    """取得單場比賽的所有出手紀錄（使用 Live PlayByPlay）

    Args:
        game_id: NBA 比賽 ID，例如 '0042500115'

    回傳欄位：
        action_number, game_id, away_team, home_team, period, clock,
        player_name, team_tricode, action_type, sub_type,
        shot_result, shot_distance, x, y, description
    """
    pbp = playbyplay.PlayByPlay(game_id=game_id).get_dict()["game"]

    # Live endpoint omits awayTeam/homeTeam for completed/historical games
    away = (pbp.get("awayTeam") or {}).get("teamTricode", "")
    home = (pbp.get("homeTeam") or {}).get("teamTricode", "")

    if not away or not home:
        tricodes = list(dict.fromkeys(
            a["teamTricode"] for a in pbp.get("actions", []) if a.get("teamTricode")
        ))
        away = tricodes[0] if len(tricodes) > 0 else ""
        home = tricodes[1] if len(tricodes) > 1 else ""

    shots = [
        {
            "action_number": a["actionNumber"],
            "game_id": game_id,
            "away_team": away,
            "home_team": home,
            "period": a["period"],
            "clock": a["clock"],
            "player_name": a.get("playerNameI", ""),
            "team_tricode": a.get("teamTricode", ""),
            "action_type": a["actionType"],
            "sub_type": a.get("subType", ""),
            "shot_result": a.get("shotResult", ""),
            "shot_distance": a.get("shotDistance"),
            "x": a.get("xLegacy"),
            "y": a.get("yLegacy"),
            "description": a.get("description", ""),
        }
        for a in pbp["actions"]
        if a["actionType"] in ("2pt", "3pt")
    ]
    return pd.DataFrame(shots)


def get_today_shots() -> pd.DataFrame:
    """取得今日所有比賽的出手紀錄（使用 Live PlayByPlay）

    回傳欄位：
        game_id, away_team, home_team, period, clock,
        player_name, team_tricode, action_type, sub_type,
        shot_result, shot_distance, x, y, description
    """
    sb = scoreboard.ScoreBoard()
    games = sb.get_dict()["scoreboard"]["games"]

    if not games:
        print("今日無比賽")
        return pd.DataFrame()

    return pd.concat(
        [get_game_shots(g["gameId"]) for g in games],
        ignore_index=True,
    )


def summarize_by_zone(shot_df: pd.DataFrame) -> pd.DataFrame:
    """依投籃區域統計出手數、命中數、命中率"""
    return (
        shot_df.groupby("SHOT_ZONE_BASIC")
        .agg(
            出手數=("SHOT_ATTEMPTED_FLAG", "sum"),
            命中數=("SHOT_MADE_FLAG", "sum"),
        )
        .assign(命中率=lambda df: (df["命中數"] / df["出手數"]).map("{:.1%}".format))
        .sort_values("出手數", ascending=False)
    )


def summarize_by_game(shot_df: pd.DataFrame) -> pd.DataFrame:
    """依比賽日期統計每場出手與命中數"""
    return (
        shot_df.groupby(["GAME_DATE", "HTM", "VTM"])
        .agg(
            出手數=("SHOT_ATTEMPTED_FLAG", "sum"),
            命中數=("SHOT_MADE_FLAG", "sum"),
        )
        .assign(命中率=lambda df: (df["命中數"] / df["出手數"]).map("{:.1%}".format))
        .reset_index()
        .sort_values("GAME_DATE")
    )


if __name__ == "__main__":
    SEASON = "2025-26"

    # ── 1. 單一球員出手紀錄（以 Stephen Curry 為例）──────────────────────────
    player_name = "Stephen Curry"
    player = players.find_players_by_full_name(player_name)[0]
    player_id = player["id"]

    print(f"=== {player_name} {SEASON} 出手紀錄 ===")
    shot_df = get_player_shots(player_id, season=SEASON)
    print(f"共 {len(shot_df)} 筆出手紀錄\n")

    # 欄位預覽
    preview_cols = [
        "GAME_DATE",
        "PLAYER_NAME",
        "ACTION_TYPE",
        "SHOT_TYPE",
        "SHOT_ZONE_BASIC",
        "SHOT_DISTANCE",
        "LOC_X",
        "LOC_Y",
        "SHOT_MADE_FLAG",
    ]
    print(shot_df[preview_cols].head(10).to_string(index=False))
    print()

    # 投籃區域分佈
    print(f"=== {player_name} 投籃區域分佈 ===")
    print(summarize_by_zone(shot_df).to_string())
    print()

    # 逐場統計（前 5 場）
    print(f"=== {player_name} 逐場出手統計（前 5 場）===")
    print(summarize_by_game(shot_df).head(5).to_string(index=False))
    print()

    # ── 2. 整隊出手紀錄（以 Golden State Warriors 為例）──────────────────────
    team_name = "Golden State Warriors"
    team = teams.find_teams_by_full_name(team_name)[0]
    team_id = team["id"]

    print(f"=== {team_name} {SEASON} 全隊出手紀錄 ===")
    team_shots_df = get_team_shots(team_id, season=SEASON)
    print(f"共 {len(team_shots_df)} 筆出手紀錄\n")

    # 球員出手排行
    print("各球員出手數排行：")
    player_summary = (
        team_shots_df.groupby("PLAYER_NAME")
        .agg(
            出手數=("SHOT_ATTEMPTED_FLAG", "sum"),
            命中數=("SHOT_MADE_FLAG", "sum"),
        )
        .assign(命中率=lambda df: (df["命中數"] / df["出手數"]).map("{:.1%}".format))
        .sort_values("出手數", ascending=False)
    )
    print(player_summary.head(10).to_string())
    print()

    # 全隊投籃區域分佈
    print(f"=== {team_name} 投籃區域分佈 ===")
    print(summarize_by_zone(team_shots_df).to_string())
    print()

    # ── 3. 單場比賽出手紀錄（指定 game_id）──────────────────────────────────
    sample_game_id = "0042500115"  # PHI @ BOS
    print(f"=== 單場出手紀錄 (game_id={sample_game_id}) ===")
    game_df = get_game_shots(sample_game_id)
    print(f"共 {len(game_df)} 筆出手紀錄\n")
    print(game_df.head(10).to_string(index=False))
    print()

    # 依球員統計
    print("球員出手統計：")
    print(
        game_df.groupby(["team_tricode", "player_name"])
        .agg(出手數=("action_type", "count"), 命中數=("shot_result", lambda s: (s == "Made").sum()))
        .assign(命中率=lambda d: (d["命中數"] / d["出手數"]).map("{:.1%}".format))
        .sort_values("出手數", ascending=False)
        .head(10)
        .to_string()
    )
    print()

    # ── 4. 今日比賽出手紀錄（Live PlayByPlay）────────────────────────────────
    print("=== 今日比賽出手紀錄 ===")
    today_df = get_today_shots()
    if today_df.empty:
        print("今日無比賽或尚未開賽")
    else:
        print(f"共 {len(today_df)} 筆出手紀錄\n")
        print(today_df.head(10).to_string(index=False))
        print()

        # 依比賽分組統計
        print("各場比賽出手統計：")
        game_summary = (
            today_df.groupby(["game_id", "away_team", "home_team"])
            .agg(
                出手數=("action_type", "count"),
                命中數=("shot_result", lambda s: (s == "Made").sum()),
            )
            .assign(命中率=lambda df: (df["命中數"] / df["出手數"]).map("{:.1%}".format))
            .reset_index()
        )
        print(game_summary.to_string(index=False))
