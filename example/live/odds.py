"""
nba_api.live.nba.endpoints.odds 範例程式

執行方式：
  python example/live/odds.py

Odds 資料結構：
  od.games           — DataSet，list[dict]，每個 dict 代表一場比賽的賠率資料
  od.get_dict()      — 完整原始 dict，頂層鍵為 "games"

game dict 欄位：
  gameId             — 比賽代碼（對應 ScoreBoard / BoxScore 的 gameId）
  sr_id              — Sportradar 比賽 ID
  srMatchId          — Sportradar Match ID
  homeTeamId         — 主隊 teamId（整數字串）
  awayTeamId         — 客隊 teamId
  markets            — list[dict]，各市場賠率

market dict：
  name               — 市場名稱（"moneyline", "spread", "total" …）
  odds_type_id       — 市場類型數字 ID
  group_name         — 分組（"game" / "half" / "quarter" …）
  books              — list[dict]，各博彩平台賠率

book dict：
  id                 — 博彩平台 ID
  name               — 博彩平台名稱（"DraftKings", "FanDuel", "BetMGM" …）
  url                — 博彩連結
  countryCode        — 適用國家代碼
  outcomes           — list[dict]，每個選項的賠率（通常 2 個：主/客 或 over/under）

outcome dict：
  type               — 選項類型（"home", "away", "over", "under"）
  odds               — 目前賠率（美式賠率字串，如 "-110", "+150"）
  opening_odds       — 開盤賠率
  odds_trend         — 賠率走勢（"up", "down", "" 或 None）
  spread             — 讓分數值（僅 spread 市場）
  opening_spread     — 開盤讓分
"""

import sys

import pandas as pd
from nba_api.live.nba.endpoints.odds import Odds
from nba_api.live.nba.endpoints.scoreboard import ScoreBoard


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

TREND_SYMBOL = {"up": "▲", "down": "▼", "": "─", None: "─"}


def _build_team_map() -> dict[str, str]:
    """從 ScoreBoard 建立 {teamId(str): teamTricode} 對照表"""
    games = ScoreBoard().get_dict()["scoreboard"]["games"]
    mapping: dict[str, str] = {}
    for g in games:
        for side in ("homeTeam", "awayTeam"):
            t = g[side]
            mapping[str(t["teamId"])] = t["teamTricode"]
    return mapping


def _fmt_odds(odds: str, trend: str | None) -> str:
    """將賠率與走勢合併為易讀字串，如 '-110▲'"""
    symbol = TREND_SYMBOL.get(trend, "─")
    return f"{odds}{symbol}" if odds else "—"


# ---------------------------------------------------------------------------
# 1. 全場賠率概覽
# ---------------------------------------------------------------------------

def show_overview(games: list[dict], team_map: dict[str, str]) -> None:
    """每場比賽的市場清單（不展開到 book 層）"""
    print("── 今日比賽賠率概覽 " + "─" * 44)
    if not games:
        print("  無賠率資料")
        print()
        return

    for g in games:
        away = team_map.get(str(g.get("awayTeamId", "")), g.get("awayTeamId", "?"))
        home = team_map.get(str(g.get("homeTeamId", "")), g.get("homeTeamId", "?"))
        markets = g.get("markets", [])
        market_names = ", ".join(
            f"{m['name']}({m.get('group_name', '')})" for m in markets
        ) or "—"
        print(f"  {away} @ {home}  ({g['gameId']})")
        print(f"    市場: {market_names}")
    print()


# ---------------------------------------------------------------------------
# 2. 單場 Moneyline 賠率比較表
# ---------------------------------------------------------------------------

def show_moneyline(game: dict, away: str, home: str) -> None:
    """各博彩平台的 moneyline 賠率對比"""
    markets = [m for m in game.get("markets", []) if m["name"] == "moneyline"]
    if not markets:
        print(f"── {away} @ {home} Moneyline：無資料\n")
        return

    rows = []
    for market in markets:
        group = market.get("group_name", "game")
        for book in market.get("books", []):
            outcomes = {o["type"]: o for o in book.get("outcomes", [])}
            away_o = outcomes.get("away", {})
            home_o = outcomes.get("home", {})
            rows.append({
                "group":   group,
                "book":    book["name"],
                f"{away}": _fmt_odds(away_o.get("odds", ""), away_o.get("odds_trend")),
                f"{home}": _fmt_odds(home_o.get("odds", ""), home_o.get("odds_trend")),
                f"{away}_open": away_o.get("opening_odds", "—"),
                f"{home}_open": home_o.get("opening_odds", "—"),
            })

    df = pd.DataFrame(rows)
    print(f"── {away} @ {home}  Moneyline（美式賠率，▲/▼ 為走勢）" + "─" * 20)
    print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 單場 Spread 賠率比較表
# ---------------------------------------------------------------------------

def show_spread(game: dict, away: str, home: str) -> None:
    """各博彩平台的讓分賠率"""
    markets = [m for m in game.get("markets", []) if m["name"] == "spread"]
    if not markets:
        print(f"── {away} @ {home} Spread：無資料\n")
        return

    rows = []
    for market in markets:
        group = market.get("group_name", "game")
        for book in market.get("books", []):
            outcomes = {o["type"]: o for o in book.get("outcomes", [])}
            away_o = outcomes.get("away", {})
            home_o = outcomes.get("home", {})
            rows.append({
                "group":          group,
                "book":           book["name"],
                f"{away}_spread": away_o.get("spread", "—"),
                f"{away}_odds":   _fmt_odds(away_o.get("odds", ""), away_o.get("odds_trend")),
                f"{home}_spread": home_o.get("spread", "—"),
                f"{home}_odds":   _fmt_odds(home_o.get("odds", ""), home_o.get("odds_trend")),
                "open_spread":    away_o.get("opening_spread", "—"),
            })

    df = pd.DataFrame(rows)
    print(f"── {away} @ {home}  Spread" + "─" * 40)
    print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 單場 Total（Over/Under）賠率比較表
# ---------------------------------------------------------------------------

def show_total(game: dict, away: str, home: str) -> None:
    """各博彩平台的大小分賠率"""
    markets = [m for m in game.get("markets", []) if m["name"] == "total"]
    if not markets:
        print(f"── {away} @ {home} Total：無資料\n")
        return

    rows = []
    for market in markets:
        group = market.get("group_name", "game")
        for book in market.get("books", []):
            outcomes = {o["type"]: o for o in book.get("outcomes", [])}
            over_o  = outcomes.get("over", {})
            under_o = outcomes.get("under", {})
            rows.append({
                "group":      group,
                "book":       book["name"],
                "total_pts":  over_o.get("spread", under_o.get("spread", "—")),
                "over_odds":  _fmt_odds(over_o.get("odds", ""), over_o.get("odds_trend")),
                "under_odds": _fmt_odds(under_o.get("odds", ""), under_o.get("odds_trend")),
                "open_total": over_o.get("opening_spread", "—"),
            })

    df = pd.DataFrame(rows)
    print(f"── {away} @ {home}  Total (Over/Under)" + "─" * 30)
    print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 賠率走勢：開盤 vs 目前
# ---------------------------------------------------------------------------

def show_odds_movement(game: dict, away: str, home: str) -> None:
    """列出有賠率變動（odds_trend 非空）的項目"""
    rows = []
    for market in game.get("markets", []):
        for book in market.get("books", []):
            for outcome in book.get("outcomes", []):
                trend = outcome.get("odds_trend") or ""
                if not trend:
                    continue
                rows.append({
                    "market":       market["name"],
                    "group":        market.get("group_name", ""),
                    "book":         book["name"],
                    "type":         outcome.get("type", ""),
                    "opening_odds": outcome.get("opening_odds", "—"),
                    "current_odds": outcome.get("odds", "—"),
                    "trend":        TREND_SYMBOL.get(trend, trend),
                    "spread":       outcome.get("spread") or "—",
                })

    print(f"── {away} @ {home}  賠率走勢（有變動項目）" + "─" * 30)
    if rows:
        print(pd.DataFrame(rows).to_string(index=False))
    else:
        print("  無賠率變動記錄")
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    od = Odds()
    games = od.games.get_dict()      # list[dict]

    if not games:
        print("目前無賠率資料（可能非比賽日或 API 尚未更新）")
        sys.exit(0)

    # 從 ScoreBoard 建立 teamId → tricode 對照表
    team_map = _build_team_map()

    # 1. 概覽
    show_overview(games, team_map)

    # 逐場展示各市場賠率（取第一場做完整示範）
    target = games[0]
    away_tricode = team_map.get(str(target.get("awayTeamId", "")), "AWAY")
    home_tricode = team_map.get(str(target.get("homeTeamId", "")), "HOME")

    print(f"=== 詳細示範：{away_tricode} @ {home_tricode} ===\n")

    # 2. Moneyline
    show_moneyline(target, away_tricode, home_tricode)

    # 3. Spread
    show_spread(target, away_tricode, home_tricode)

    # 4. Total
    show_total(target, away_tricode, home_tricode)

    # 5. 賠率走勢
    show_odds_movement(target, away_tricode, home_tricode)

    # 若有多場比賽，列出所有場次的 Moneyline 摘要
    if len(games) > 1:
        print("=== 所有比賽 Moneyline 摘要（第一個 book）===\n")
        summary_rows = []
        for g in games:
            away = team_map.get(str(g.get("awayTeamId", "")), g.get("awayTeamId", "?"))
            home = team_map.get(str(g.get("homeTeamId", "")), g.get("homeTeamId", "?"))
            ml_markets = [m for m in g.get("markets", []) if m["name"] == "moneyline" and m.get("group_name") == "game"]
            if not ml_markets or not ml_markets[0]["books"]:
                continue
            first_book = ml_markets[0]["books"][0]
            outcomes = {o["type"]: o for o in first_book.get("outcomes", [])}
            away_o = outcomes.get("away", {})
            home_o = outcomes.get("home", {})
            summary_rows.append({
                "matchup":      f"{away} @ {home}",
                "book":         first_book["name"],
                "away_ml":      _fmt_odds(away_o.get("odds", ""), away_o.get("odds_trend")),
                "home_ml":      _fmt_odds(home_o.get("odds", ""), home_o.get("odds_trend")),
            })
        if summary_rows:
            print(pd.DataFrame(summary_rows).to_string(index=False))
        print()
