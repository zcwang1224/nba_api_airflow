"""
nba_api.stats.endpoints.playerdashboardbyyearoveryear 範例程式

執行方式：
  python example/stats/player_dashboard_by_year_over_year.py                   # 預設球員
  python example/stats/player_dashboard_by_year_over_year.py "Stephen Curry"   # 依姓名查詢
  python example/stats/player_dashboard_by_year_over_year.py 2544              # 依 player_id
  python example/stats/player_dashboard_by_year_over_year.py 2544 PerGame      # 指定 per_mode

  ── 預設行為：同時顯示常規賽 + 季後賽逐年趨勢，最後並排對比 ──

PlayerDashboardByYearOverYear 聚焦「跨球季年度比較」：
  - 只有 2 個 DataSet，但 ByYearPlayerDashboard 每季一列，可追蹤整個生涯
  - 與 PlayerCareerStats 的差異：此 endpoint 含 RANK 欄，
    可看出球員每季在全聯盟的相對排名是否提升

與其他 Dashboard 的差異：
  - 無 po_round_nullable 輪次過濾（直接切換 season_type 即可）
  - ByYearPlayerDashboard 含獨有欄位：
      TEAM_ID / TEAM_ABBREVIATION — 該季所屬球隊（球隊異動紀錄）
      MAX_GAME_DATE               — 該季最後出賽日期

DataSet 總覽（共 2 個）：
  overall_player_dashboard    — 當季整體（單列）
  by_year_player_dashboard    — 逐年（每季一列，GROUP_VALUE = 球季字串）

共用欄位：
  GROUP_VALUE / TEAM_ID / TEAM_ABBREVIATION / MAX_GAME_DATE
  GP / W / L / W_PCT / MIN
  FGM/FGA/FG_PCT / FG3M/FG3A/FG3_PCT / FTM/FTA/FT_PCT
  OREB/DREB/REB / AST/TOV/STL/BLK/BLKA/PF/PFD
  PTS / PLUS_MINUS / NBA_FANTASY_PTS / DD2 / TD3 / *_RANK

參數（與其他 Dashboard 相同）：
  player_id / season / season_type_playoffs / per_mode_detailed /
  measure_type_detailed / last_n_games 等
  ※ 無 po_round_nullable
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashboardbyyearoveryear import (
    PlayerDashboardByYearOverYear,
)
from nba_api.stats.library.parameters import (
    MeasureTypeDetailed,
    PerModeDetailed,
    Season,
    SeasonType,
)
from nba_api.stats.static import players

TIMEOUT     = 60
RETRIES     = 3
RETRY_DELAY = 5

SEASON_TYPE_REGULAR  = SeasonType.regular
SEASON_TYPE_PLAYOFFS = "Playoffs"

SKIP_COLS   = {"CFID", "CFPARAMS", "GROUP_SET"}
STR_COLS    = {"GROUP_VALUE", "TEAM_ABBREVIATION", "MAX_GAME_DATE"}
STAT_COLS   = [
    "GP", "W", "L", "W_PCT", "MIN",
    "PTS", "REB", "AST", "STL", "BLK", "TOV",
    "FG_PCT", "FG3_PCT", "FT_PCT",
    "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3",
]
RANK_COLS   = [
    "PTS_RANK", "REB_RANK", "AST_RANK", "STL_RANK", "BLK_RANK",
    "FG_PCT_RANK", "FG3_PCT_RANK", "PLUS_MINUS_RANK",
]
TREND_COLS  = ["PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "PLUS_MINUS"]
PER_MODE_LABEL = {
    "Totals":           "累計",
    "PerGame":          "場均",
    "Per36":            "Per36",
    "Per100Possessions":"Per100Poss",
}


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def _find_player_id(query: str) -> tuple[int, str]:
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


def fetch_dashboard(
    player_id: int,
    season: str = Season.default,
    season_type: str = SEASON_TYPE_REGULAR,
    per_mode: str = PerModeDetailed.per_game,
    measure_type: str = MeasureTypeDetailed.default,
    last_n_games: int = 0,
    month: int = 0,
    period: int = 0,
    date_from: str = "",
    date_to: str = "",
    location: str = "",
    outcome: str = "",
    season_segment: str = "",
    vs_conference: str = "",
) -> PlayerDashboardByYearOverYear:
    """取得 YearOverYear Dashboard，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashboardByYearOverYear(
                player_id=player_id,
                season=season,
                season_type_playoffs=season_type,
                per_mode_detailed=per_mode,
                measure_type_detailed=measure_type,
                last_n_games=str(last_n_games),
                month=str(month),
                period=str(period),
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                location_nullable=location,
                outcome_nullable=outcome,
                season_segment_nullable=season_segment,
                vs_conference_nullable=vs_conference,
                timeout=TIMEOUT,
            )
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    num_cols = [c for c in df.columns if c not in SKIP_COLS and c not in STR_COLS]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 1. 當季整體（Overall）含全聯盟排名
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashboardByYearOverYear, label: str, per_mode: str) -> None:
    df = _clean(resp.overall_player_dashboard.get_data_frame())
    if df.empty:
        print(f"── {label}當季整體：無資料\n")
        return
    mode_lbl = PER_MODE_LABEL.get(per_mode, per_mode)
    avail  = ["GROUP_VALUE", "TEAM_ABBREVIATION"] + [c for c in STAT_COLS if c in df.columns]
    r_avail = ["GROUP_VALUE"] + [c for c in RANK_COLS if c in df.columns]
    print(f"── {label}當季整體數據（{mode_lbl}）" + "─" * 38)
    print(df[avail].to_string(index=False))
    if len(r_avail) > 1:
        print(f"\n  全聯盟排名（數字越小 = 排名越前）：")
        print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 逐年生涯趨勢（ByYear — 核心 DataSet）
# ---------------------------------------------------------------------------

def show_by_year(resp: PlayerDashboardByYearOverYear, label: str, per_mode: str) -> None:
    """
    每季一列，包含 TEAM_ABBREVIATION（球隊異動紀錄）和 MAX_GAME_DATE。
    GROUP_VALUE = 球季字串（如 "2022-23"）。
    """
    df = _clean(resp.by_year_player_dashboard.get_data_frame())
    if df.empty:
        print(f"── {label}逐年趨勢：無資料\n")
        return

    mode_lbl = PER_MODE_LABEL.get(per_mode, per_mode)
    cols = ["GROUP_VALUE", "TEAM_ABBREVIATION", "GP"] + \
           [c for c in STAT_COLS if c in df.columns] + ["MAX_GAME_DATE"]
    avail = [c for c in cols if c in df.columns]

    print(f"── {label}逐年生涯趨勢（{mode_lbl}）" + "─" * 36)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 逐年全聯盟排名走勢
# ---------------------------------------------------------------------------

def show_rank_by_year(resp: PlayerDashboardByYearOverYear, label: str) -> None:
    """
    逐年在全聯盟的排名，可觀察球員在聯盟中的地位是否隨年齡提升或下滑。
    """
    df = _clean(resp.by_year_player_dashboard.get_data_frame())
    if df.empty:
        return
    r_avail = ["GROUP_VALUE", "TEAM_ABBREVIATION"] + [c for c in RANK_COLS if c in df.columns]
    if len(r_avail) <= 2:
        return
    print(f"── {label}逐年全聯盟排名走勢（數字越小 = 排名越前）" + "─" * 16)
    print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 球隊異動紀錄
# ---------------------------------------------------------------------------

def show_team_history(resp: PlayerDashboardByYearOverYear, label: str) -> None:
    """
    從 TEAM_ABBREVIATION 欄追蹤球員每季所屬球隊，
    標記球隊異動的球季。
    """
    df = _clean(resp.by_year_player_dashboard.get_data_frame())
    if df.empty or "TEAM_ABBREVIATION" not in df.columns:
        return

    prev_team = None
    changes = []
    for _, row in df.iterrows():
        team = str(row.get("TEAM_ABBREVIATION", "")).strip()
        season = str(row.get("GROUP_VALUE", "")).strip()
        gp = row.get("GP", 0)
        mark = ""
        if prev_team and team != prev_team and team:
            mark = " ← 球隊異動"
            changes.append((season, prev_team, team))
        prev_team = team if team else prev_team
        print(f"  {season:<10} {team:<6} {int(gp) if pd.notna(gp) else 0:>3} 場{mark}")

    print()
    if changes:
        print(f"  球隊異動紀錄（共 {len(changes)} 次）：")
        for season, old, new in changes:
            print(f"    {season}：{old} → {new}")
    print()


# ---------------------------------------------------------------------------
# 5. 年度進步/退步分析
# ---------------------------------------------------------------------------

def show_yoy_delta(resp: PlayerDashboardByYearOverYear, label: str) -> None:
    """
    計算相鄰球季的年度差值（Year-over-Year delta），
    正值 = 進步，負值 = 退步。
    """
    df = _clean(resp.by_year_player_dashboard.get_data_frame())
    if df.empty or len(df) < 2:
        print(f"── {label}年度差值：資料不足（需至少 2 季）\n")
        return

    trend_avail = [c for c in TREND_COLS if c in df.columns]
    seasons = df["GROUP_VALUE"].tolist()

    rows = []
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        entry = {
            "球季": f"{seasons[i-1]} → {seasons[i]}",
            "球隊": str(curr.get("TEAM_ABBREVIATION", "")).strip(),
        }
        for col in trend_avail:
            pv = prev.get(col)
            cv = curr.get(col)
            if pd.notna(pv) and pd.notna(cv):
                diff = round(float(cv) - float(pv), 3)
                entry[col] = f"{diff:+.3f}"
            else:
                entry[col] = "—"
        rows.append(entry)

    result = pd.DataFrame(rows).set_index("球季")
    print(f"── {label}年度進步 / 退步差值（+ = 進步，- = 退步）" + "─" * 16)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 6. 生涯巔峰球季
# ---------------------------------------------------------------------------

def show_peak_seasons(resp: PlayerDashboardByYearOverYear, label: str) -> None:
    """各項指標的生涯最佳球季（從 ByYear DataSet 找出）。"""
    df = _clean(resp.by_year_player_dashboard.get_data_frame())
    if df.empty:
        return

    peak_cols = [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                 ("STL","抄截"), ("BLK","火鍋"), ("FG_PCT","投籃%"),
                 ("FG3_PCT","三分%"), ("PLUS_MINUS","正負值")]

    print(f"── {label}生涯各項最佳球季 " + "─" * 42)
    for col, lbl in peak_cols:
        if col not in df.columns:
            continue
        sub = df[df[col].notna()]
        if sub.empty:
            continue
        idx    = sub[col].idxmax()
        row    = sub.loc[idx]
        season = row.get("GROUP_VALUE", "—")
        team   = str(row.get("TEAM_ABBREVIATION", "")).strip()
        val    = row[col]
        gp     = row.get("GP", "?")
        print(f"  {lbl:<10}: {val:>7.3f}  {season}  {team:<5} (GP={int(gp) if pd.notna(gp) else '?'})")
    print()


# ---------------------------------------------------------------------------
# 7. 多種 MeasureType 當季對比（Base / Advanced / Scoring / Usage）
# ---------------------------------------------------------------------------

MEASURE_COLS = {
    "Base":     ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FG_PCT", "PLUS_MINUS"],
    "Advanced": ["PTS", "REB", "AST", "PLUS_MINUS"],
    "Scoring":  ["PTS", "FG_PCT", "FG3_PCT", "FT_PCT"],
    "Usage":    ["PTS", "REB", "AST", "TOV"],
}


def show_multi_measure(
    player_id: int,
    season: str,
    season_type: str,
    per_mode: str,
    label: str,
) -> None:
    """跑 4 種 MeasureType 的整體數據並排。"""
    rows = []
    for measure in ["Base", "Advanced", "Scoring", "Usage"]:
        try:
            resp = fetch_dashboard(
                player_id=player_id,
                season=season,
                season_type=season_type,
                per_mode=per_mode,
                measure_type=measure,
            )
        except Exception as e:
            print(f"  [{measure}] 取得失敗: {e}")
            continue

        df = _clean(resp.overall_player_dashboard.get_data_frame())
        if df.empty:
            continue
        row = df.iloc[0]
        entry = {"MeasureType": measure}
        for col in MEASURE_COLS.get(measure, STAT_COLS[:8]):
            if col in df.columns:
                v = row.get(col)
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return
    result = pd.DataFrame(rows).set_index("MeasureType")
    print(f"── {label}多種 MeasureType 當季整體對比 " + "─" * 24)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 8. 常規賽 vs 季後賽逐年並排
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_by_year(
    reg_resp: PlayerDashboardByYearOverYear,
    po_resp: PlayerDashboardByYearOverYear,
) -> None:
    """
    逐年常規賽與季後賽核心數據並排，
    可觀察球員每季在常規賽與季後賽的表現差異，以及差異是否隨資歷縮小。
    """
    reg_df = _clean(reg_resp.by_year_player_dashboard.get_data_frame())
    po_df  = _clean(po_resp.by_year_player_dashboard.get_data_frame())

    if reg_df.empty:
        return

    # 以常規賽球季為主，對齊季後賽球季
    po_map: dict[str, pd.Series] = {}
    if not po_df.empty:
        for _, row in po_df.iterrows():
            season = str(row.get("GROUP_VALUE", "")).strip()
            po_map[season] = row

    key_cols = ["PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "PLUS_MINUS"]
    avail = [c for c in key_cols if c in reg_df.columns]

    rows = []
    for _, reg_row in reg_df.iterrows():
        season = str(reg_row.get("GROUP_VALUE", "")).strip()
        team   = str(reg_row.get("TEAM_ABBREVIATION", "")).strip()
        entry  = {"球季": season, "球隊": team}
        for col in avail:
            r_v = reg_row.get(col)
            entry[f"常規_{col}"] = round(float(r_v), 2) if pd.notna(r_v) else None
            po_row = po_map.get(season)
            if po_row is not None:
                p_v = po_row.get(col)
                entry[f"季後_{col}"] = round(float(p_v), 2) if pd.notna(p_v) else None
                diff = (float(p_v) - float(r_v)) if pd.notna(p_v) and pd.notna(r_v) else None
                entry[f"差_{col}"] = round(diff, 2) if diff is not None else None
            else:
                entry[f"季後_{col}"] = None
                entry[f"差_{col}"]   = None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("球季")
    print("── 逐年常規賽 vs 季後賽並排對比 " + "─" * 30)
    print("  ※ 差_* = 季後賽 - 常規賽（+ = 季後賽較佳）")
    print()
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"

PER_MODE_MAP = {
    "Totals":    PerModeDetailed.totals,
    "PerGame":   PerModeDetailed.per_game,
    "Per36":     PerModeDetailed.per_36,
    "Per100":    PerModeDetailed.per_100_possessions,
}

if __name__ == "__main__":
    query    = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    per_mode = PER_MODE_MAP.get(
        sys.argv[2] if len(sys.argv) > 2 else "PerGame",
        PerModeDetailed.per_game,
    )
    season = Season.default

    player_id, full_name = _find_player_id(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"PerMode  : {per_mode}  |  球季上限: {season}\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽生涯逐年趨勢  {full_name}")
    print("=" * 65 + "\n")

    reg_resp = fetch_dashboard(player_id, season=season,
                               season_type=SEASON_TYPE_REGULAR,
                               per_mode=per_mode)

    show_overall(reg_resp,      label="常規賽 ", per_mode=per_mode)
    show_by_year(reg_resp,      label="常規賽 ", per_mode=per_mode)
    show_rank_by_year(reg_resp, label="常規賽 ")

    print("=" * 65)
    print("  球隊異動紀錄 & 年度差值分析")
    print("=" * 65 + "\n")

    show_team_history(reg_resp, label="常規賽 ")
    show_yoy_delta(reg_resp,    label="常規賽 ")
    show_peak_seasons(reg_resp, label="常規賽 ")

    print("=" * 65)
    print("  當季多種 MeasureType 對比（各需一次額外請求）")
    print("=" * 65 + "\n")

    show_multi_measure(player_id, season, SEASON_TYPE_REGULAR,
                       per_mode, label="常規賽 ")

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽生涯逐年趨勢  {full_name}")
    print("=" * 65 + "\n")

    po_resp = None
    try:
        po_resp = fetch_dashboard(player_id, season=season,
                                  season_type=SEASON_TYPE_PLAYOFFS,
                                  per_mode=per_mode)

        po_overall = po_resp.overall_player_dashboard.get_data_frame()
        if po_overall.empty:
            print(f"  {full_name} 無季後賽出賽記錄。\n")
            po_resp = None
        else:
            show_overall(po_resp,      label="季後賽 ", per_mode=per_mode)
            show_by_year(po_resp,      label="季後賽 ", per_mode=per_mode)
            show_rank_by_year(po_resp, label="季後賽 ")
            show_yoy_delta(po_resp,    label="季後賽 ")
            show_peak_seasons(po_resp, label="季後賽 ")

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 常規賽 vs 季後賽逐年並排 ─────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  逐年常規賽 vs 季後賽並排對比")
        print("=" * 65 + "\n")
        show_regular_vs_playoffs_by_year(reg_resp, po_resp)
