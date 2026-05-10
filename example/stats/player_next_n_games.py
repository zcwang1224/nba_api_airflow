"""
nba_api.stats.endpoints.playernextngames 範例程式

執行方式：
  python example/stats/player_next_n_games.py                        # 預設球員，全部未來場次
  python example/stats/player_next_n_games.py "Stephen Curry"        # 依姓名查詢
  python example/stats/player_next_n_games.py 2544                   # 依 player_id 查詢
  python example/stats/player_next_n_games.py 2544 10                # 最多 10 場
  python example/stats/player_next_n_games.py 2544 20 Playoffs       # 只看季後賽賽程

PlayerNextNGames 參數：
  player_id              — 球員 ID（必填）
  number_of_games        — 查詢場次上限（預設 2147483647 = 全部剩餘場次）
  season_all             — 球季（預設當季 "2025-26"）；傳 "ALL" 可跨球季查詢
  season_type_all_star   — "Regular Season"（預設）/ "Playoffs" /
                           "PlayIn" / "Pre Season" / "All Star"
  league_id_nullable     — "00"=NBA（預設）

DataSet：
  resp.next_n_games  — 未來 N 場賽程，每列一場比賽

欄位說明：
  GAME_ID                    — 比賽 ID
  GAME_DATE                  — 比賽日期
  GAME_TIME                  — 比賽時間（美東時間）
  HOME_TEAM_ID               — 主場球隊 ID
  VISITOR_TEAM_ID            — 客場球隊 ID
  HOME_TEAM_NAME             — 主場球隊全名
  VISITOR_TEAM_NAME          — 客場球隊全名
  HOME_TEAM_ABBREVIATION     — 主場球隊縮寫
  VISITOR_TEAM_ABBREVIATION  — 客場球隊縮寫
  HOME_TEAM_NICKNAME         — 主場球隊暱稱
  VISITOR_TEAM_NICKNAME      — 客場球隊暱稱
  HOME_WL                    — 主場球隊勝負（已完成場次有值；未來場次為空）
  VISITOR_WL                 — 客場球隊勝負（已完成場次有值；未來場次為空）

季後賽補充說明：
  season_type_all_star="Playoffs" 時，回傳該輪次的完整賽程（已賽 + 未賽）。
  可用 HOME_WL / VISITOR_WL 推算系列賽目前比分，以及剩餘場次預測。
"""

import sys
import time
from collections import Counter

import pandas as pd
from nba_api.stats.endpoints.playernextngames import PlayerNextNGames
from nba_api.stats.library.parameters import SeasonAll, SeasonTypeAllStar
from nba_api.stats.static import players

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5

SEASON_TYPE_MAP = {
    "Regular":   SeasonTypeAllStar.regular,
    "Playoffs":  SeasonTypeAllStar.playoffs,
    "PlayIn":    SeasonTypeAllStar.playin,
    "PreSeason": SeasonTypeAllStar.preseason,
    "AllStar":   SeasonTypeAllStar.all_star,
}


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def _find_player_id(query: str) -> tuple[int, str]:
    """將字串 query 解析為 (player_id, full_name)；支援數字 ID 或姓名。"""
    if query.isdigit():
        pid = int(query)
        matched = [p for p in players.get_players() if p["id"] == pid]
        if not matched:
            print(f"找不到 player_id={pid} 的球員")
            sys.exit(1)
        return pid, matched[0]["full_name"]

    results = players.find_players_by_full_name(query)
    if not results:
        print(f"找不到球員：{query}")
        sys.exit(1)
    if len(results) > 1:
        print(f"找到多位球員，使用第一筆：{results[0]['full_name']}")
    return results[0]["id"], results[0]["full_name"]


def fetch_next_games(
    player_id: int,
    number_of_games: int = 2147483647,
    season: str = SeasonAll.default,
    season_type: str = SeasonTypeAllStar.regular,
) -> pd.DataFrame:
    """
    取得球員接下來 N 場賽程，失敗時自動重試。
    回傳 DataFrame 依 GAME_DATE 升冪排序。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            resp = PlayerNextNGames(
                player_id=player_id,
                number_of_games=str(number_of_games),
                season_all=season,
                season_type_all_star=season_type,
                timeout=TIMEOUT,
            )
            break
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)

    df = resp.next_n_games.get_data_frame()
    if df.empty:
        return df

    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    return df.sort_values("GAME_DATE").reset_index(drop=True)


def _detect_player_team(df: pd.DataFrame) -> int:
    """從賽程中推算球員所屬球隊 ID（每場都會出現的 team_id）。"""
    if df.empty:
        return 0
    all_ids = df["HOME_TEAM_ID"].tolist() + df["VISITOR_TEAM_ID"].tolist()
    counter = Counter(int(t) for t in all_ids if t)
    return next(
        (tid for tid, cnt in counter.most_common() if cnt == len(df)), 0
    )


def _opponent_name(row: pd.Series, player_team_id: int) -> tuple[str, str, bool]:
    """
    回傳 (對手全名, 對手縮寫, 是否主場)。
    player_team_id=0 時以主隊視角呈現。
    """
    is_home = int(row["HOME_TEAM_ID"]) == player_team_id if player_team_id else True
    if is_home:
        return row["VISITOR_TEAM_NAME"], row["VISITOR_TEAM_ABBREVIATION"], True
    return row["HOME_TEAM_NAME"], row["HOME_TEAM_ABBREVIATION"], False


# ---------------------------------------------------------------------------
# 1. 通用賽程表
# ---------------------------------------------------------------------------

def show_schedule(
    df: pd.DataFrame,
    full_name: str,
    player_team_id: int = 0,
    label: str = "",
) -> None:
    """以易讀格式印出賽程（支援常規賽與季後賽）。"""
    if df.empty:
        print(f"── {full_name} {label}賽程：無場次資料\n")
        return

    title = f"{full_name} {label}賽程（共 {len(df)} 場）"
    print(f"── {title} " + "─" * max(0, 52 - len(title)))
    print(f"  {'#':<4} {'日期':<12} {'時間(ET)':<12} {'主客':<5} {'對手':<28} {'已出賽結果'}")
    print("  " + "─" * 72)

    for i, (_, row) in enumerate(df.iterrows(), 1):
        date_str = row["GAME_DATE"].strftime("%Y-%m-%d") if pd.notna(row["GAME_DATE"]) else "—"
        time_str = str(row.get("GAME_TIME", "")).strip() or "TBD"

        if player_team_id:
            opp_name, opp_abbr, is_home = _opponent_name(row, player_team_id)
            venue = "主場 vs" if is_home else "客場 @"
            opponent = f"{venue} {opp_abbr}"
        else:
            opponent = f"{row['HOME_TEAM_ABBREVIATION']} vs {row['VISITOR_TEAM_ABBREVIATION']}"

        # 若 HOME_WL / VISITOR_WL 有值，表示已完賽
        home_wl    = str(row.get("HOME_WL",    "")).strip()
        visitor_wl = str(row.get("VISITOR_WL", "")).strip()
        if home_wl and visitor_wl:
            if player_team_id:
                my_wl  = home_wl if int(row["HOME_TEAM_ID"]) == player_team_id else visitor_wl
                result = f"{'勝' if my_wl == 'W' else '敗'} ({home_wl}-{visitor_wl})"
            else:
                result = f"{home_wl}-{visitor_wl}"
        else:
            result = "（待賽）"

        print(f"  {i:<4} {date_str:<12} {time_str:<12} {opponent:<34} {result}")
    print()


# ---------------------------------------------------------------------------
# 2. 季後賽系列賽分析
# ---------------------------------------------------------------------------

def show_playoff_series(
    df: pd.DataFrame,
    full_name: str,
    player_team_id: int,
) -> None:
    """
    分析季後賽系列賽狀況：
    - 已完賽場次 → 推算目前系列賽比分
    - 未完賽場次 → 計算剩餘場次，預估系列賽結束日期
    七戰四勝制：先贏 4 場者晉級。
    """
    if df.empty:
        print("── 季後賽系列賽分析：無資料\n")
        return

    played  = df[df["HOME_WL"].notna() & (df["HOME_WL"] != "")]
    pending = df[df["HOME_WL"].isna()  | (df["HOME_WL"] == "")]

    # 計算目前系列賽比分
    my_wins = opp_wins = 0
    for _, row in played.iterrows():
        is_home = int(row["HOME_TEAM_ID"]) == player_team_id
        my_wl   = str(row["HOME_WL"]) if is_home else str(row["VISITOR_WL"])
        if my_wl == "W":
            my_wins += 1
        else:
            opp_wins += 1

    # 對手名稱（取第一筆推算）
    opp_name = "對手"
    if not df.empty and player_team_id:
        _, opp_name, _ = _opponent_name(df.iloc[0], player_team_id)

    print(f"── 季後賽系列賽分析（七戰四勝）" + "─" * 34)
    print(f"  球隊        : {full_name.split()[-1]} 所屬球隊")
    print(f"  對手        : {opp_name}")
    print(f"  已賽場次    : {len(played)} 場")
    print(f"  目前比分    : {my_wins} - {opp_wins}  ", end="")

    if my_wins == 4:
        print("→ 本輪晉級！")
    elif opp_wins == 4:
        print("→ 本輪淘汰。")
    else:
        my_need  = 4 - my_wins
        opp_need = 4 - opp_wins
        print(f"（尚需贏 {my_need} 場晉級，對手尚需贏 {opp_need} 場）")

    print(f"  待賽場次    : {len(pending)} 場")

    # 最多可能還剩幾場
    max_remaining = 4 - max(my_wins, opp_wins)
    min_remaining = max_remaining  # 如果其中一方接近 4 勝，最少可能提早結束
    print(f"  本輪最多剩餘: {max_remaining} 場")

    if not pending.empty:
        next_game = pending.iloc[0]
        date_str  = next_game["GAME_DATE"].strftime("%Y-%m-%d") if pd.notna(next_game["GAME_DATE"]) else "TBD"
        time_str  = str(next_game.get("GAME_TIME", "")).strip() or "TBD"
        _, opp_abbr, is_home = _opponent_name(next_game, player_team_id)
        venue = "主場" if is_home else "客場"
        print(f"\n  下一場      : {date_str}  {time_str} ET  {venue} vs {opp_abbr}")

    print()

    # 印出已完賽明細
    if not played.empty:
        print(f"  ── 已完賽明細（{len(played)} 場）")
        for _, row in played.iterrows():
            date_str = row["GAME_DATE"].strftime("%Y-%m-%d") if pd.notna(row["GAME_DATE"]) else "—"
            is_home  = int(row["HOME_TEAM_ID"]) == player_team_id
            my_wl    = str(row["HOME_WL"]) if is_home else str(row["VISITOR_WL"])
            venue    = "主場" if is_home else "客場"
            result   = "勝" if my_wl == "W" else "敗"
            score    = f"{row['HOME_WL']}-{row['VISITOR_WL']}"
            print(f"     {date_str}  {venue}  {result}  ({score})")
        print()

    # 印出待賽明細
    if not pending.empty:
        print(f"  ── 待賽場次（{len(pending)} 場）")
        for _, row in pending.iterrows():
            date_str = row["GAME_DATE"].strftime("%Y-%m-%d") if pd.notna(row["GAME_DATE"]) else "TBD"
            time_str = str(row.get("GAME_TIME", "")).strip() or "TBD"
            _, opp_abbr, is_home = _opponent_name(row, player_team_id)
            venue = "主場" if is_home else "客場"
            print(f"     {date_str}  {time_str} ET  {venue} vs {opp_abbr}")
        print()


# ---------------------------------------------------------------------------
# 3. 主客場分佈
# ---------------------------------------------------------------------------

def show_home_away_breakdown(df: pd.DataFrame, player_team_id: int) -> None:
    """統計賽程中主客場場次比例。"""
    if df.empty or not player_team_id:
        return

    home = df[df["HOME_TEAM_ID"].astype(int) == player_team_id]
    away = df[df["HOME_TEAM_ID"].astype(int) != player_team_id]
    total = len(df)

    print(f"── 主客場分佈（共 {total} 場）" + "─" * 42)
    print(f"  主場 : {len(home)} 場  ({len(home)/total*100:.1f}%)")
    print(f"  客場 : {len(away)} 場  ({len(away)/total*100:.1f}%)")
    print()


# ---------------------------------------------------------------------------
# 4. 背靠背偵測
# ---------------------------------------------------------------------------

def show_back_to_back(df: pd.DataFrame) -> None:
    """找出背靠背（連續兩天有賽事）的場次組合。"""
    if df.empty or len(df) < 2:
        return

    b2b = []
    dates = df["GAME_DATE"].tolist()
    for i in range(1, len(dates)):
        if pd.notna(dates[i]) and pd.notna(dates[i - 1]):
            if (dates[i] - dates[i - 1]).days == 1:
                b2b.append((dates[i - 1], dates[i]))

    print(f"── 背靠背賽程偵測（共 {len(b2b)} 組）" + "─" * 36)
    if not b2b:
        print("  無背靠背場次")
    else:
        for d1, d2 in b2b:
            print(f"  {d1.strftime('%Y-%m-%d')}  →  {d2.strftime('%Y-%m-%d')}")
    print()


# ---------------------------------------------------------------------------
# 5. 多球員賽程對照（可橫跨常規賽或季後賽）
# ---------------------------------------------------------------------------

def show_multi_player_schedule(
    player_list: list[tuple[int, str]],
    number_of_games: int = 2147483647,
    season: str = SeasonAll.default,
    season_type: str = SeasonTypeAllStar.playoffs,
) -> None:
    """
    並排顯示多位球員接下來的賽程，★ 標記雙方同日出賽日期。
    預設使用季後賽視角，方便追蹤不同系列賽進度。
    """
    schedules:  dict[str, set]  = {}
    matchup_map: dict[str, dict] = {}

    for pid, name in player_list:
        try:
            df = fetch_next_games(
                pid, number_of_games=number_of_games,
                season=season, season_type=season_type,
            )
            date_set = set(df["GAME_DATE"].dt.strftime("%Y-%m-%d")) if not df.empty else set()
            schedules[name] = date_set

            opp_map: dict[str, str] = {}
            for _, row in df.iterrows():
                d = row["GAME_DATE"].strftime("%Y-%m-%d") if pd.notna(row["GAME_DATE"]) else ""
                home_abbr = row["HOME_TEAM_ABBREVIATION"]
                away_abbr = row["VISITOR_TEAM_ABBREVIATION"]
                # 已完賽場次加上結果標記
                home_wl = str(row.get("HOME_WL", "")).strip()
                vis_wl  = str(row.get("VISITOR_WL", "")).strip()
                suffix  = f" ({home_wl}-{vis_wl})" if home_wl else ""
                opp_map[d] = f"{home_abbr} vs {away_abbr}{suffix}"
            matchup_map[name] = opp_map
            print(f"  ✓ {name}（{len(date_set)} 場）")
        except Exception as e:
            print(f"  ✗ {name} 取得失敗: {e}")
            schedules[name] = set()

    if not schedules:
        return

    all_dates = sorted(set().union(*schedules.values()))
    if not all_dates:
        print("  無賽程資料\n")
        return

    names = [n for n, _ in player_list if n in schedules]
    col_w = 28
    header = f"  {'日期':<13}" + "".join(f"{n:<{col_w}}" for n in names)
    print()
    print("── 多球員賽程對照 " + "─" * max(0, len(header) - 18))
    print(header)
    print("  " + "─" * (11 + col_w * len(names)))

    common = set(all_dates)
    for name in names:
        common &= schedules[name]

    for date in all_dates:
        mark    = "★" if date in common else " "
        row_str = f"  {mark}{date:<12}"
        for name in names:
            cell = matchup_map.get(name, {}).get(date, "（休息）")
            row_str += f"{cell:<{col_w}}"
        print(row_str)

    print()
    print(f"  ★ 雙方同日出賽（共 {len(common)} 天）" +
          ("" if not common else "：" + "、".join(sorted(common))))
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"
DEMO_PLAYERS   = [
    (2544,   "LeBron James"),
    (201939, "Stephen Curry"),
]

if __name__ == "__main__":
    query           = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    number_of_games = int(sys.argv[2]) if len(sys.argv) > 2 else 2147483647
    st_arg          = sys.argv[3] if len(sys.argv) > 3 else "all"
    season          = SeasonAll.default

    player_id, full_name = _find_player_id(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}\n")

    # ── 常規賽剩餘賽程 ────────────────────────────────────────────────────
    print("=" * 65)
    print("  常規賽剩餘賽程")
    print("=" * 65 + "\n")

    reg_df = fetch_next_games(
        player_id, number_of_games=number_of_games,
        season=season, season_type=SeasonTypeAllStar.regular,
    )
    reg_team_id = _detect_player_team(reg_df)

    show_schedule(reg_df, full_name, reg_team_id, label="常規賽")

    if not reg_df.empty:
        show_home_away_breakdown(reg_df, reg_team_id)
        show_back_to_back(reg_df)

    # ── 季後賽賽程 ────────────────────────────────────────────────────────
    print("=" * 65)
    print("  季後賽賽程（含已賽場次與系列賽分析）")
    print("=" * 65 + "\n")

    po_df = fetch_next_games(
        player_id, number_of_games=number_of_games,
        season=season, season_type=SeasonTypeAllStar.playoffs,
    )
    po_team_id = _detect_player_team(po_df)

    show_schedule(po_df, full_name, po_team_id, label="季後賽")

    if not po_df.empty:
        show_playoff_series(po_df, full_name, po_team_id)
        show_home_away_breakdown(po_df, po_team_id)
        show_back_to_back(po_df)
    else:
        print(f"  {full_name} 本季無季後賽出賽記錄或尚未進入季後賽。\n")

    # ── 多球員季後賽賽程對照 ─────────────────────────────────────────────
    print("=" * 65)
    print("  多球員季後賽賽程對照")
    print("=" * 65 + "\n")

    show_multi_player_schedule(
        DEMO_PLAYERS,
        season=season,
        season_type=SeasonTypeAllStar.playoffs,
    )
