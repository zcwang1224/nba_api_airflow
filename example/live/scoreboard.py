"""
nba_api.live.nba.endpoints.scoreboard 範例程式

執行方式：
  python example/live/scoreboard.py

ScoreBoard 資料結構：
  sb.score_board_date          — 比賽日期字串（"2025-05-10"）
  sb.games.get_dict()          — list[dict]，每個元素代表一場比賽
  sb.get_dict()["scoreboard"]  — 完整原始 dict

每場比賽（game dict）包含：
  gameId, gameCode, gameStatus, gameStatusText
  period, gameClock, gameTimeUTC, gameEt
  seriesGameNumber, seriesText           ← 季後賽對戰資訊
  homeTeam / awayTeam                    ← 球隊資訊（含每節得分）
  gameLeaders                            ← 雙方數據領先球員
  pbOdds                                 ← 賠率

gameStatus：
  1 = 未開始 (Scheduled)
  2 = 進行中 (Live)
  3 = 已結束 (Final)
"""

import re
from datetime import datetime, timezone

from nba_api.live.nba.endpoints.scoreboard import ScoreBoard


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

STATUS = {1: "未開始", 2: "進行中", 3: "Final"}


def _parse_clock(raw: str) -> str:
    """'PT05M30.00S' → '5:30'，空字串或格式不符原樣回傳"""
    if not raw:
        return ""
    m = re.match(r"PT(\d+)M([\d.]+)S", raw)
    return f"{int(m.group(1))}:{int(float(m.group(2))):02d}" if m else raw


def _quarter_scores(team: dict) -> dict[int, int]:
    """回傳 {節次: 得分} 的 dict"""
    return {p["period"]: p["score"] for p in team.get("periods", [])}


# ---------------------------------------------------------------------------
# 1. 取得今日賽程（原始 dict）
# ---------------------------------------------------------------------------

def fetch_scoreboard() -> tuple[str, list[dict]]:
    """
    回傳 (game_date, games)
    games 是 list[dict]，每個 dict 是一場比賽的完整資料
    """
    sb = ScoreBoard()
    data = sb.get_dict()["scoreboard"]
    return data["gameDate"], data["games"]


# ---------------------------------------------------------------------------
# 2. 解析單場比賽為易讀格式
# ---------------------------------------------------------------------------

def parse_game(g: dict) -> dict:
    home = g["homeTeam"]
    away = g["awayTeam"]
    home_qtrs = _quarter_scores(home)
    away_qtrs = _quarter_scores(away)
    max_period = max(list(home_qtrs) + list(away_qtrs), default=0)

    leaders = g.get("gameLeaders", {})
    hl = leaders.get("homeLeaders", {})
    al = leaders.get("awayLeaders", {})

    odds = g.get("pbOdds", {})

    return {
        # 比賽識別
        "game_id":       g["gameId"],
        "game_code":     g["gameCode"],           # "20250510/BOSNYК"
        "game_time_utc": g["gameTimeUTC"],
        "game_et":       g["gameEt"],

        # 狀態
        "status":        g["gameStatus"],
        "status_text":   g.get("gameStatusText", ""),
        "period":        g["period"],
        "clock":         _parse_clock(g.get("gameClock", "")),

        # 季後賽
        "series_game":   g.get("seriesGameNumber", ""),  # "6"
        "series_text":   g.get("seriesText", ""),        # "BOS leads 3-2"

        # 主隊
        "home":          home["teamTricode"],
        "home_id":       home["teamId"],
        "home_city":     home["teamCity"],
        "home_name":     home["teamName"],
        "home_score":    home["score"],
        "home_record":   f"{home['wins']}-{home['losses']}",
        "home_bonus":    home.get("inBonus") == "1",
        "home_timeouts": home.get("timeoutsRemaining", 0),
        "home_qtrs":     home_qtrs,

        # 客隊
        "away":          away["teamTricode"],
        "away_id":       away["teamId"],
        "away_city":     away["teamCity"],
        "away_name":     away["teamName"],
        "away_score":    away["score"],
        "away_record":   f"{away['wins']}-{away['losses']}",
        "away_bonus":    away.get("inBonus") == "1",
        "away_timeouts": away.get("timeoutsRemaining", 0),
        "away_qtrs":     away_qtrs,

        # 每節得分（away-home 格式）
        "quarter_scores": {
            q: f"{away_qtrs.get(q, 0)}-{home_qtrs.get(q, 0)}"
            for q in range(1, max_period + 1)
        },

        # 雙方領先球員
        "home_leader": {
            "name":    hl.get("name", ""),
            "pts":     hl.get("points", 0),
            "reb":     hl.get("rebounds", 0),
            "ast":     hl.get("assists", 0),
        },
        "away_leader": {
            "name":    al.get("name", ""),
            "pts":     al.get("points", 0),
            "reb":     al.get("rebounds", 0),
            "ast":     al.get("assists", 0),
        },

        # 賠率
        "odds_team":      odds.get("team"),
        "odds_value":     odds.get("odds", 0.0),
        "odds_suspended": odds.get("suspended") == 1,
    }


# ---------------------------------------------------------------------------
# 3. 顯示函式
# ---------------------------------------------------------------------------

def display_scoreboard(game_date: str, games: list[dict]) -> None:
    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
    print(f"{'=' * 65}")
    print(f"  NBA Scoreboard  {game_date}  (更新: {now_utc})")
    print(f"{'=' * 65}")

    if not games:
        print("  今日無賽事")
        return

    live      = [g for g in games if g["gameStatus"] == 2]
    finished  = [g for g in games if g["gameStatus"] == 3]
    scheduled = [g for g in games if g["gameStatus"] == 1]

    def _print_group(title: str, lst: list[dict]) -> None:
        if not lst:
            return
        print(f"\n── {title} {'─' * (55 - len(title))}")
        for g in lst:
            p = parse_game(g)

            # 狀態標籤
            if p["status"] == 2:
                state = f"Q{p['period']} {p['clock']}"
                if p["home_bonus"]:
                    state += f"  {p['home']}🔴"
                if p["away_bonus"]:
                    state += f"  {p['away']}🔴"
            elif p["status"] == 1:
                et = p["game_et"].replace("T", " ")[:16]
                state = f"開賽: {et} ET"
            else:
                state = "Final"

            series = f"  [{p['series_text']}]" if p["series_text"] else ""

            # 比分行
            print(
                f"  {p['away']:3s} ({p['away_record']:5s})  "
                f"{p['away_score']:>3}  @  "
                f"{p['home_score']:<3}  ({p['home_record']:5s}) {p['home']:3s}"
                f"   {state}{series}"
            )

            # 每節得分
            if p["quarter_scores"]:
                qtrs = "  ".join(
                    f"Q{q}:{v}" for q, v in sorted(p["quarter_scores"].items())
                )
                print(f"  {' ' * 28}{qtrs}")

            # 領先球員
            hl, al = p["home_leader"], p["away_leader"]
            if hl["name"] or al["name"]:
                def _fmt(ldr: dict, tricode: str) -> str:
                    if not ldr["name"]:
                        return ""
                    return f"{tricode} {ldr['name']} {ldr['pts']}pts/{ldr['reb']}reb/{ldr['ast']}ast"
                print(f"  {_fmt(al, p['away']):35s}  {_fmt(hl, p['home'])}")

            print()

    _print_group("進行中", live)
    _print_group("已結束", finished)
    _print_group("未開始", scheduled)


def display_game_ids(games: list[dict]) -> None:
    """列出今日所有 game_id，方便傳給 BoxScore / PlayByPlay"""
    print("今日 Game ID 清單：")
    for g in games:
        home = g["homeTeam"]["teamTricode"]
        away = g["awayTeam"]["teamTricode"]
        status = STATUS.get(g["gameStatus"], "?")
        print(f"  {g['gameId']}  {away} @ {home}  ({status})")


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    game_date, games = fetch_scoreboard()

    # 1. 完整戰況顯示
    display_scoreboard(game_date, games)

    # 2. Game ID 清單（給其他端點使用）
    display_game_ids(games)

    # 3. 原始 dict 示範（第一場）
    if games:
        print("\n── 第一場原始欄位預覽 ──")
        g0 = games[0]
        for k, v in g0.items():
            if k not in ("homeTeam", "awayTeam", "gameLeaders", "pbOdds"):
                print(f"  {k}: {v}")
