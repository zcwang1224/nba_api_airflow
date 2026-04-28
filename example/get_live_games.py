"""
NBA 即時戰況範例程式
使用 nba_api.live 取得當日賽程、即時比分、球員數據與逐球記錄

gameStatus:
  1 = 未開始 (Scheduled)
  2 = 進行中 (Live)
  3 = 已結束 (Final)
"""

import re
import time
from datetime import datetime, timezone

import pandas as pd
from nba_api.live.nba.endpoints import boxscore, playbyplay, scoreboard


def _parse_clock(clock_str: str) -> str:
    """將 'PT05M30.00S' 格式轉為 '5:30'"""
    if not clock_str:
        return ""
    m = re.match(r"PT(\d+)M([\d.]+)S", clock_str)
    if not m:
        return clock_str
    mins = int(m.group(1))
    secs = int(float(m.group(2)))
    return f"{mins}:{secs:02d}"


STATUS_LABEL = {1: "未開始", 2: "進行中", 3: "Final"}


def get_today_scoreboard() -> list[dict]:
    """
    取得今日所有比賽的即時比分摘要
    回傳每場比賽的 dict，包含 gameId、狀態、節次、比分等
    """
    sb = scoreboard.ScoreBoard()
    games = sb.get_dict()["scoreboard"]["games"]

    results = []
    for g in games:
        home = g["homeTeam"]
        away = g["awayTeam"]

        # 每節得分
        home_qtrs = {p["period"]: p["score"] for p in home.get("periods", [])}
        away_qtrs = {p["period"]: p["score"] for p in away.get("periods", [])}
        max_period = max(list(home_qtrs.keys()) + list(away_qtrs.keys()), default=0)

        results.append({
            "game_id": g["gameId"],
            "game_et": g["gameEt"],
            "status": g["gameStatus"],
            "status_text": g.get("gameStatusText", ""),
            "period": g["period"],
            "clock": _parse_clock(g.get("gameClock", "")),
            "series_text": g.get("seriesText", ""),
            "game_label": g.get("gameLabel", ""),
            "home": home["teamTricode"],
            "home_score": home["score"],
            "home_record": f"{home['wins']}-{home['losses']}",
            "home_bonus": home.get("inBonus"),
            "home_timeouts": home.get("timeoutsRemaining"),
            "away": away["teamTricode"],
            "away_score": away["score"],
            "away_record": f"{away['wins']}-{away['losses']}",
            "away_bonus": away.get("inBonus"),
            "away_timeouts": away.get("timeoutsRemaining"),
            "quarter_scores": {
                q: f"{away_qtrs.get(q, 0)}-{home_qtrs.get(q, 0)}"
                for q in range(1, max_period + 1)
            },
        })
    return results


def get_live_boxscore(game_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    取得比賽的即時 Box Score
    回傳 (球員數據 DataFrame, 球隊整體數據 DataFrame)
    """
    bs = boxscore.BoxScore(game_id=game_id)
    game = bs.get_dict()["game"]

    player_rows = []
    team_rows = []

    for side in ("homeTeam", "awayTeam"):
        team = game[side]
        tricode = team["teamTricode"]

        # 球隊整體
        s = team["statistics"]
        team_rows.append({
            "team": tricode,
            "score": team["score"],
            "fg": f"{s['fieldGoalsMade']}/{s['fieldGoalsAttempted']}",
            "fg%": f"{s['fieldGoalsPercentage']:.3f}",
            "3p": f"{s['threePointersMade']}/{s['threePointersAttempted']}",
            "3p%": f"{s['threePointersPercentage']:.3f}",
            "ft": f"{s['freeThrowsMade']}/{s['freeThrowsAttempted']}",
            "reb": s["reboundsTotal"],
            "ast": s["assists"],
            "stl": s["steals"],
            "blk": s["blocks"],
            "tov": s["turnovers"],
            "paint_pts": s["pointsInThePaint"],
            "fast_break": s["pointsFastBreak"],
            "bench_pts": s["benchPoints"],
        })

        # 球員個人
        for p in team["players"]:
            if not p.get("played") and p.get("status") != "ACTIVE":
                continue
            ps = p["statistics"]
            player_rows.append({
                "team": tricode,
                "name": p["nameI"],
                "pos": p.get("position", ""),
                "starter": "●" if p.get("starter") == "1" else "",
                "min": ps["minutes"][2:7] if ps["minutes"].startswith("PT") else ps["minutes"],
                "pts": ps["points"],
                "reb": ps["reboundsTotal"],
                "ast": ps["assists"],
                "fg": f"{ps['fieldGoalsMade']}/{ps['fieldGoalsAttempted']}",
                "3p": f"{ps['threePointersMade']}/{ps['threePointersAttempted']}",
                "ft": f"{ps['freeThrowsMade']}/{ps['freeThrowsAttempted']}",
                "stl": ps["steals"],
                "blk": ps["blocks"],
                "tov": ps["turnovers"],
                "+/-": ps["plusMinusPoints"],
            })

    return pd.DataFrame(player_rows), pd.DataFrame(team_rows)


def get_live_play_by_play(game_id: str, period: int | None = None, last_n: int | None = None) -> pd.DataFrame:
    """
    取得即時逐球記錄
    period: 指定節次，None 則回傳全場
    last_n: 只回傳最後 N 筆
    """
    pbp = playbyplay.PlayByPlay(game_id=game_id)
    actions = pbp.get_dict()["game"]["actions"]

    rows = []
    for a in actions:
        if period is not None and a["period"] != period:
            continue
        rows.append({
            "period": a["period"],
            "clock": _parse_clock(a.get("clock", "")),
            "team": a.get("teamTricode", ""),
            "player": a.get("playerNameI", ""),
            "action": a.get("actionType", ""),
            "description": a.get("description", ""),
            "score": f"{a.get('scoreAway', '')}-{a.get('scoreHome', '')}",
        })

    df = pd.DataFrame(rows)
    if last_n and not df.empty:
        df = df.tail(last_n).reset_index(drop=True)
    return df


def display_scoreboard(games: list[dict]) -> None:
    """格式化輸出今日賽程"""
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    print(f"{'=' * 60}")
    print(f"  NBA 即時戰況  ({now})")
    print(f"{'=' * 60}")

    live, scheduled, final = [], [], []
    for g in games:
        if g["status"] == 2:
            live.append(g)
        elif g["status"] == 1:
            scheduled.append(g)
        else:
            final.append(g)

    def _print_games(title: str, lst: list[dict]) -> None:
        if not lst:
            return
        print(f"\n【{title}】")
        for g in lst:
            label = g["game_label"] or ""
            if g["status"] == 2:
                period_info = f"Q{g['period']} {g['clock']}"
                bonus_info = ""
                if g["home_bonus"]:
                    bonus_info += f" {g['home']}BONUS"
                if g["away_bonus"]:
                    bonus_info += f" {g['away']}BONUS"
                state = f"{period_info}{bonus_info}"
            elif g["status"] == 1:
                et = g["game_et"].replace("T", " ")[:16]
                state = f"開賽: {et} ET"
            else:
                state = "Final"

            score_line = (
                f"  {g['away']:3s} ({g['away_record']}) "
                f"{g['away_score']:>3}  @  "
                f"{g['home_score']:<3} ({g['home_record']}) {g['home']:3s}"
            )
            qtrs = "  |  " + "  ".join(
                f"Q{q}:{v}" for q, v in sorted(g["quarter_scores"].items())
            ) if g["quarter_scores"] else ""
            series = f"  [{g['series_text']}]" if g["series_text"] else ""
            print(f"{score_line}   {state}{series}")
            if qtrs:
                print(f"  {' ' * 18}{qtrs}")

    _print_games("進行中", live)
    _print_games("已結束", final)
    _print_games("未開始", scheduled)
    print()


if __name__ == "__main__":
    # 1. 今日即時賽程
    games = get_today_scoreboard()
    display_scoreboard(games)

    if not games:
        print("今日無賽事")
        exit(0)

    # 選一場有資料的比賽（優先取進行中，其次已結束）
    target = next((g for g in games if g["status"] == 2), None)
    if target is None:
        target = next((g for g in games if g["status"] == 3), None)
    if target is None:
        target = games[0]

    game_id = target["game_id"]
    label = f"{target['away']} vs {target['home']}"
    print(f"詳細資料來源: {label} ({game_id})\n")

    # 2. 即時球隊 Box Score
    print(f"=== {label} 球隊整體數據 ===")
    player_df, team_df = get_live_boxscore(game_id)
    print(team_df.to_string(index=False))
    print()

    # 3. 即時球員 Box Score（依得分排序前 10）
    print(f"=== {label} 球員數據（得分前 10）===")
    if not player_df.empty:
        top10 = player_df.sort_values("pts", ascending=False).head(10)
        print(top10[["team", "name", "starter", "min", "pts", "reb", "ast", "fg", "3p", "+/-"]].to_string(index=False))
    print()

    # 4. 即時逐球記錄（最後 15 筆）
    print(f"=== {label} 最近 15 筆逐球記錄 ===")
    pbp_df = get_live_play_by_play(game_id, last_n=15)
    if not pbp_df.empty:
        print(pbp_df[["period", "clock", "team", "player", "description", "score"]].to_string(index=False))
    print()

    # 5. 持續輪詢（進行中才啟動，Ctrl+C 停止）
    if target["status"] == 2:
        print("=== 開始輪詢即時比分（每 30 秒更新，Ctrl+C 停止）===")
        try:
            while True:
                time.sleep(30)
                games = get_today_scoreboard()
                display_scoreboard(games)
        except KeyboardInterrupt:
            print("\n已停止輪詢")
