"""
nba_api.live.nba.endpoints.boxscore 範例程式

BoxScore 提供的 DataSet 屬性：
  bs.game_details        — 比賽基本資訊（時間、狀態、節次、場館、出席等）
  bs.arena               — 場館資訊
  bs.officials           — 裁判名單
  bs.home_team_stats     — 主隊整體數據（不含球員）
  bs.away_team_stats     — 客隊整體數據（不含球員）
  bs.home_team_player_stats — 主隊球員數據
  bs.away_team_player_stats — 客隊球員數據

每個 DataSet 都有：
  .get_dict()        → dict / list[dict]
  pd.json_normalize(.get_dict())  → pd.DataFrame（DataSet 不提供 get_data_frame，需手動轉換）
"""

import re
import sys

import pandas as pd
from nba_api.live.nba.endpoints import boxscore
from nba_api.live.nba.endpoints import scoreboard


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def _parse_minutes(raw: str) -> str:
    """'PT25M01.00S'  →  '25:01'"""
    if not raw:
        return ""
    m = re.match(r"PT(\d+)M([\d.]+)S", raw)
    if not m:
        return raw
    return f"{int(m.group(1))}:{int(float(m.group(2))):02d}"


def _pick_game_id() -> str:
    """從今日賽程選一場有資料的比賽（優先取進行中，其次已結束）"""
    games = scoreboard.ScoreBoard().get_dict()["scoreboard"]["games"]
    if not games:
        print("今日無賽事，請手動指定 game_id。")
        sys.exit(0)
    target = (
        next((g for g in games if g["gameStatus"] == 2), None)
        or next((g for g in games if g["gameStatus"] == 3), None)
        or games[0]
    )
    away = target["awayTeam"]["teamTricode"]
    home = target["homeTeam"]["teamTricode"]
    status = {1: "未開始", 2: "進行中", 3: "Final"}.get(target["gameStatus"], "?")
    print(f"選取比賽: {away} @ {home}  ({status})  game_id={target['gameId']}\n")
    return target["gameId"]


# ---------------------------------------------------------------------------
# 1. 建立 BoxScore 物件
# ---------------------------------------------------------------------------

def demo_raw_dict(game_id: str) -> None:
    """示範用 get_dict() 存取原始 JSON 結構"""
    bs = boxscore.BoxScore(game_id=game_id)
    game = bs.get_dict()["game"]

    home = game["homeTeam"]
    away = game["awayTeam"]
    print(f"=== 原始 dict 存取 ===")
    print(f"比賽代碼  : {game['gameCode']}")
    print(f"比賽狀態  : {game['gameStatusText']}  (status={game['gameStatus']})")
    print(f"節次/時鐘 : Q{game['period']}  {game.get('gameClock','')}")
    print(f"比分      : {away['teamTricode']} {away['score']}  –  {home['score']} {home['teamTricode']}")
    print()


# ---------------------------------------------------------------------------
# 2. 用 DataSet 屬性取得各區塊
# ---------------------------------------------------------------------------

def demo_dataset_api(game_id: str) -> None:
    """示範 BoxScore 各 DataSet 屬性"""
    bs = boxscore.BoxScore(game_id=game_id)

    # --- 比賽基本資訊 ---
    details = bs.game_details.get_dict()
    print("=== game_details ===")
    for k in ("gameId", "gameCode", "gameStatusText", "period", "gameClock",
              "duration", "attendance", "regulationPeriods"):
        print(f"  {k}: {details.get(k)}")
    print()

    # --- 場館 ---
    arena = bs.arena.get_dict()
    print("=== arena ===")
    print(f"  {arena.get('arenaName')}, {arena.get('arenaCity')}, {arena.get('arenaState')}")
    print()

    # --- 裁判 ---
    officials = bs.officials.get_dict()
    print("=== officials ===")
    for o in officials:
        print(f"  #{o['jerseyNum']}  {o['name']}  ({o['assignment']})")
    print()

    # --- 球隊整體數據（DataFrame）---
    # DataSet 只有 get_dict() / get_json()，用 pd.json_normalize 轉 DataFrame
    home_stats_df = pd.json_normalize(bs.home_team_stats.get_dict())
    away_stats_df = pd.json_normalize(bs.away_team_stats.get_dict())

    TEAM_COLS = [
        "teamTricode", "score",
        "statistics.fieldGoalsMade", "statistics.fieldGoalsAttempted", "statistics.fieldGoalsPercentage",
        "statistics.threePointersMade", "statistics.threePointersAttempted", "statistics.threePointersPercentage",
        "statistics.freeThrowsMade", "statistics.freeThrowsAttempted", "statistics.freeThrowsPercentage",
        "statistics.reboundsTotal", "statistics.reboundsOffensive", "statistics.reboundsDefensive",
        "statistics.assists", "statistics.steals", "statistics.blocks", "statistics.turnovers",
        "statistics.pointsInThePaint", "statistics.pointsFastBreak", "statistics.benchPoints",
        "statistics.biggestLead", "statistics.timeLeading",
    ]
    # 只取存在的欄位，並去掉 "statistics." 前綴讓欄位名稱更簡潔
    available = [c for c in TEAM_COLS if c in home_stats_df.columns]
    combined = pd.concat(
        [away_stats_df[available], home_stats_df[available]],
        ignore_index=True,
    ).rename(columns=lambda c: c.replace("statistics.", ""))
    print("=== 球隊整體數據 ===")
    print(combined.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 球員數據 DataFrame
# ---------------------------------------------------------------------------

def demo_player_stats(game_id: str) -> None:
    """示範球員 DataSet → DataFrame，並做基本整理"""
    bs = boxscore.BoxScore(game_id=game_id)

    rows = []
    for side, ds in (
        ("home", bs.home_team_player_stats),
        ("away", bs.away_team_player_stats),
    ):
        team_tricode = (
            bs.home_team_stats.get_dict()["teamTricode"] if side == "home"
            else bs.away_team_stats.get_dict()["teamTricode"]
        )
        for p in ds.get_dict():
            if p.get("status") != "ACTIVE":
                continue
            s = p["statistics"]
            rows.append({
                "team": team_tricode,
                "name": p["nameI"],
                "pos": p.get("position", "-"),
                "starter": "●" if p.get("starter") == "1" else "",
                "oncourt": "▶" if p.get("oncourt") == "1" else "",
                "min": _parse_minutes(s.get("minutes", "")),
                "pts": s["points"],
                "reb": s["reboundsTotal"],
                "ast": s["assists"],
                "stl": s["steals"],
                "blk": s["blocks"],
                "tov": s["turnovers"],
                "fg": f"{s['fieldGoalsMade']}/{s['fieldGoalsAttempted']}",
                "3p": f"{s['threePointersMade']}/{s['threePointersAttempted']}",
                "ft": f"{s['freeThrowsMade']}/{s['freeThrowsAttempted']}",
                "fg%": f"{s['fieldGoalsPercentage']:.3f}",
                "+/-": s["plusMinusPoints"],
                "pts_paint": s["pointsInThePaint"],
                "pts_fb": s["pointsFastBreak"],
            })

    df = pd.DataFrame(rows)
    if df.empty:
        print("無球員數據")
        return

    display_cols = ["team", "name", "starter", "oncourt", "min",
                    "pts", "reb", "ast", "fg", "fg%", "3p", "ft", "stl", "blk", "tov", "+/-"]
    print("=== 球員數據（依得分排序）===")
    print(
        df.sort_values("pts", ascending=False)[display_cols].to_string(index=False)
    )
    print()

    # 首發 vs 替補 得分比較
    starters = df[df["starter"] == "●"].groupby("team")["pts"].sum()
    bench    = df[df["starter"] == "" ].groupby("team")["pts"].sum()
    print("=== 首發 / 替補 得分 ===")
    summary = pd.DataFrame({"首發": starters, "替補": bench}).fillna(0).astype(int)
    print(summary.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 進階：TS%、AST/TO、eFG% 計算
# ---------------------------------------------------------------------------

def demo_advanced_stats(game_id: str) -> None:
    """從球隊數據計算進階效率指標"""
    bs = boxscore.BoxScore(game_id=game_id)

    records = []
    for ds in (bs.away_team_stats, bs.home_team_stats):
        d = ds.get_dict()
        s = d["statistics"]
        fga  = s["fieldGoalsAttempted"]
        fgm  = s["fieldGoalsMade"]
        tpm  = s["threePointersMade"]
        fta  = s["freeThrowsAttempted"]
        ftm  = s["freeThrowsMade"]
        pts  = s["points"]
        ast  = s["assists"]
        tov  = s["turnovers"]

        ts_attempts = fga + 0.44 * fta
        ts_pct      = pts / (2 * ts_attempts) if ts_attempts else 0
        efg_pct     = (fgm + 0.5 * tpm) / fga if fga else 0
        ast_to      = ast / tov if tov else float("inf")

        records.append({
            "team":   d["teamTricode"],
            "pts":    pts,
            "eFG%":   f"{efg_pct:.3f}",
            "TS%":    f"{ts_pct:.3f}",
            "AST/TO": f"{ast_to:.2f}",
            "TOV%":   f"{s['turnovers'] / (fga + 0.44*fta + s['turnovers']):.3f}" if (fga + 0.44*fta + s['turnovers']) else "—",
        })

    print("=== 進階效率指標 ===")
    print(pd.DataFrame(records).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # 可從命令列傳入 game_id，否則自動從今日賽程選取
    game_id = sys.argv[1] if len(sys.argv) > 1 else _pick_game_id()

    demo_raw_dict(game_id)
    demo_dataset_api(game_id)
    demo_player_stats(game_id)
    demo_advanced_stats(game_id)
