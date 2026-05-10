"""
nba_api.live.nba.endpoints.boxscore 範例程式

執行方式：
  python example/live/boxscore.py              # 自動選取今日比賽
  python example/live/boxscore.py 0042400306   # 指定 game_id

BoxScore 資料結構：
  bs.game_details              — 比賽基本資訊（代碼、狀態、節次、時鐘、時長、出席人數）
  bs.arena                     — 場館資訊（名稱、城市、州別、國家）
  bs.officials                 — 裁判名單（list[dict]）
  bs.home_team_stats           — 主隊整體數據（含 statistics 子 dict）
  bs.away_team_stats           — 客隊整體數據
  bs.home_team_player_stats    — 主隊球員數據（list[dict]）
  bs.away_team_player_stats    — 客隊球員數據

各 DataSet 只提供 .get_dict() / .get_json()，不直接提供 DataFrame；
  需透過 pd.json_normalize(.get_dict()) 轉換為 DataFrame。

gameStatus：
  1 = 未開始 (Scheduled)
  2 = 進行中 (Live)
  3 = 已結束 (Final)
"""

import re
import sys

import pandas as pd
from nba_api.live.nba.endpoints import boxscore
from nba_api.live.nba.endpoints.scoreboard import ScoreBoard


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

STATUS = {1: "未開始", 2: "進行中", 3: "Final"}


def _parse_clock(raw: str) -> str:
    """'PT05M30.00S' → '5:30'"""
    if not raw:
        return ""
    m = re.match(r"PT(\d+)M([\d.]+)S", raw)
    return f"{int(m.group(1))}:{int(float(m.group(2))):02d}" if m else raw


def _parse_minutes(raw: str) -> str:
    """'PT25M01.00S' → '25:01'"""
    return _parse_clock(raw)


def _pick_game_id() -> str:
    """從今日賽程選一場有資料的比賽（優先進行中，其次已結束）"""
    games = ScoreBoard().get_dict()["scoreboard"]["games"]
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
    status = STATUS.get(target["gameStatus"], "?")
    print(f"選取比賽: {away} @ {home}  ({status})  game_id={target['gameId']}\n")
    return target["gameId"]


# ---------------------------------------------------------------------------
# 1. 比賽基本資訊、場館、裁判
# ---------------------------------------------------------------------------

def show_game_info(bs: boxscore.BoxScore) -> None:
    """比賽代碼、狀態、節次、場館、裁判"""
    details = bs.game_details.get_dict()
    arena   = bs.arena.get_dict()
    officials = bs.officials.get_dict()

    print("── 比賽基本資訊 " + "─" * 48)
    print(f"  代碼   : {details.get('gameCode')}")
    print(f"  狀態   : {details.get('gameStatusText')}  (status={details.get('gameStatus')})")
    print(f"  節次   : Q{details.get('period')}  {_parse_clock(details.get('gameClock', ''))}")
    print(f"  時長   : {details.get('duration', '—')}")
    print(f"  出席   : {details.get('attendance', 0):,}")
    print(f"  場館   : {arena.get('arenaName')}, {arena.get('arenaCity')}, {arena.get('arenaState')}")
    print("  裁判   :", "  |  ".join(
        f"#{o['jerseyNum']} {o['name']} ({o['assignment']})"
        for o in officials
    ))
    print()


# ---------------------------------------------------------------------------
# 2. 球隊整體數據
# ---------------------------------------------------------------------------

TEAM_COLS = {
    "teamTricode":                          "隊伍",
    "score":                                "分數",
    "statistics.fieldGoalsMade":            "FGM",
    "statistics.fieldGoalsAttempted":       "FGA",
    "statistics.fieldGoalsPercentage":      "FG%",
    "statistics.threePointersMade":         "3PM",
    "statistics.threePointersAttempted":    "3PA",
    "statistics.threePointersPercentage":   "3P%",
    "statistics.freeThrowsMade":            "FTM",
    "statistics.freeThrowsAttempted":       "FTA",
    "statistics.freeThrowsPercentage":      "FT%",
    "statistics.reboundsOffensive":         "OREB",
    "statistics.reboundsDefensive":         "DREB",
    "statistics.reboundsTotal":             "REB",
    "statistics.assists":                   "AST",
    "statistics.steals":                    "STL",
    "statistics.blocks":                    "BLK",
    "statistics.turnovers":                 "TOV",
    "statistics.foulsPersonal":             "PF",
    "statistics.pointsInThePaint":          "PAINT",
    "statistics.pointsFastBreak":           "FB",
    "statistics.benchPoints":               "BENCH",
    "statistics.biggestLead":               "LEAD",
    "statistics.timeLeading":               "TIME_LEAD",
}


def show_team_stats(bs: boxscore.BoxScore) -> None:
    """主客隊整體數據對比表"""
    frames = []
    for ds in (bs.away_team_stats, bs.home_team_stats):
        df = pd.json_normalize(ds.get_dict())
        available = [c for c in TEAM_COLS if c in df.columns]
        frames.append(df[available].rename(columns=TEAM_COLS))

    combined = pd.concat(frames, ignore_index=True)
    print("── 球隊整體數據 " + "─" * 48)
    print(combined.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 球員數據
# ---------------------------------------------------------------------------

PLAYER_DISPLAY_COLS = [
    "team", "name", "starter", "oncourt", "pos",
    "min", "pts", "reb", "ast",
    "fg", "3p", "ft", "fg%",
    "stl", "blk", "tov", "+/-",
]


def _build_player_rows(bs: boxscore.BoxScore) -> pd.DataFrame:
    rows = []
    for side, ds in (("home", bs.home_team_player_stats), ("away", bs.away_team_player_stats)):
        tricode = (
            bs.home_team_stats.get_dict()["teamTricode"] if side == "home"
            else bs.away_team_stats.get_dict()["teamTricode"]
        )
        for p in ds.get_dict():
            if p.get("status") != "ACTIVE":
                continue
            s = p["statistics"]
            rows.append({
                "team":     tricode,
                "name":     p["nameI"],
                "pos":      p.get("position", "-"),
                "starter":  "●" if p.get("starter") == "1" else "",
                "oncourt":  "▶" if p.get("oncourt") == "1" else "",
                "min":      _parse_minutes(s.get("minutes", "")),
                "pts":      s["points"],
                "reb":      s["reboundsTotal"],
                "ast":      s["assists"],
                "stl":      s["steals"],
                "blk":      s["blocks"],
                "tov":      s["turnovers"],
                "fg":       f"{s['fieldGoalsMade']}/{s['fieldGoalsAttempted']}",
                "3p":       f"{s['threePointersMade']}/{s['threePointersAttempted']}",
                "ft":       f"{s['freeThrowsMade']}/{s['freeThrowsAttempted']}",
                "fg%":      f"{s['fieldGoalsPercentage']:.3f}",
                "+/-":      s["plusMinusPoints"],
                "pts_paint": s["pointsInThePaint"],
            })
    return pd.DataFrame(rows)


def show_player_stats(bs: boxscore.BoxScore) -> None:
    """球員數據（依得分排序）+ 各隊分表 + 首發 / 替補得分統計"""
    df = _build_player_rows(bs)
    if df.empty:
        print("無球員數據\n")
        return

    # 全場合併（得分排序）
    print("── 球員數據（得分排序）" + "─" * 41)
    print(
        df.sort_values("pts", ascending=False)[PLAYER_DISPLAY_COLS]
        .to_string(index=False)
    )
    print()

    # 各隊分表（上場時間排序）
    for team, group in df.groupby("team", sort=False):
        print(f"── {team} 球員數據（上場時間排序）" + "─" * 35)
        print(
            group.sort_values("min", ascending=False)[PLAYER_DISPLAY_COLS]
            .to_string(index=False)
        )
        print()

    # 首發 / 替補得分
    starters = df[df["starter"] == "●"].groupby("team")["pts"].sum()
    bench    = df[df["starter"] == "" ].groupby("team")["pts"].sum()
    summary  = pd.DataFrame({"首發": starters, "替補": bench}).fillna(0).astype(int)
    print("── 首發 / 替補 得分 " + "─" * 44)
    print(summary.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 進階效率指標
# ---------------------------------------------------------------------------

def show_advanced_stats(bs: boxscore.BoxScore) -> None:
    """eFG%、TS%、AST/TO、TOV% 計算"""
    records = []
    for ds in (bs.away_team_stats, bs.home_team_stats):
        d = ds.get_dict()
        s = d["statistics"]
        fga = s["fieldGoalsAttempted"]
        fgm = s["fieldGoalsMade"]
        tpm = s["threePointersMade"]
        fta = s["freeThrowsAttempted"]
        ftm = s["freeThrowsMade"]
        pts = s["points"]
        ast = s["assists"]
        tov = s["turnovers"]

        ts_att  = fga + 0.44 * fta
        ts_pct  = pts / (2 * ts_att) if ts_att else 0
        efg_pct = (fgm + 0.5 * tpm) / fga if fga else 0
        ast_to  = ast / tov if tov else float("inf")
        tov_pct = tov / (fga + 0.44 * fta + tov) if (fga + 0.44 * fta + tov) else 0

        records.append({
            "team":   d["teamTricode"],
            "pts":    pts,
            "eFG%":   f"{efg_pct:.3f}",
            "TS%":    f"{ts_pct:.3f}",
            "AST":    ast,
            "TOV":    tov,
            "AST/TO": f"{ast_to:.2f}" if ast_to != float('inf') else "∞",
            "TOV%":   f"{tov_pct:.3f}",
            "PAINT":  s["pointsInThePaint"],
            "FB":     s["pointsFastBreak"],
            "BENCH":  s["benchPoints"],
        })

    print("── 進階效率指標 " + "─" * 48)
    print(pd.DataFrame(records).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    game_id = sys.argv[1] if len(sys.argv) > 1 else _pick_game_id()

    bs = boxscore.BoxScore(game_id=game_id)

    show_game_info(bs)
    show_team_stats(bs)
    show_player_stats(bs)
    show_advanced_stats(bs)
