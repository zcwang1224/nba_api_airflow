"""
nba_api.stats.endpoints.playerdashboardbygamesplits 範例程式

執行方式：
  python example/stats/player_dashboard_by_game_splits.py                   # 預設球員，本季
  python example/stats/player_dashboard_by_game_splits.py "Stephen Curry"   # 依姓名查詢
  python example/stats/player_dashboard_by_game_splits.py 2544              # 依 player_id
  python example/stats/player_dashboard_by_game_splits.py 2544 2023-24      # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashboardByGameSplits 聚焦「比賽情境」維度：
  - 逐節（Q1 / Q2 / Q3 / Q4 / OT）
  - 上下半場（First Half / Second Half）
  - 比分差勝負帶（Large Lead / Close Win / Close Loss / Large Trail 等）
  - 實際比分差距帶（-10以下 / -10~-6 / -5~0 / 平手 / +1~+5 / +6~+10 / +10以上）

與 GeneralSplits / ShootingSplits 的差異：
  - GeneralSplits：地點 / 月份 / 先發替補 / 休息天數
  - ShootingSplits：投籃區域 / 類型 / 距離 / 助攻占比
  - GameSplits（本 endpoint）：比賽節次 / 時段 / 比分情境

參數（與 PlayerDashboardByGeneralSplits 相同，略）。
po_round_nullable 同樣支援季後賽輪次篩選。

DataSet 總覽（共 5 個，欄位結構完全相同）：
  overall_player_dashboard            — 整體
  by_period_player_dashboard          — 逐節（Q1 / Q2 / Q3 / Q4 / OT）
  by_half_player_dashboard            — 上下半場
  by_score_margin_player_dashboard    — 比分差情境
  by_actual_margin_player_dashboard   — 實際比分差距帶

共用欄位：
  GROUP_VALUE   — 分組值
  GP / W / L / W_PCT / MIN
  FGM / FGA / FG_PCT / FG3M / FG3A / FG3_PCT / FTM / FTA / FT_PCT
  OREB / DREB / REB / AST / TOV / STL / BLK / BLKA / PF / PFD
  PTS / PLUS_MINUS / NBA_FANTASY_PTS / DD2 / TD3
  *_RANK  — 全聯盟排名
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashboardbygamesplits import (
    PlayerDashboardByGameSplits,
)
from nba_api.stats.library.parameters import (
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

SKIP_COLS = {"CFID", "CFPARAMS", "GROUP_SET"}
STAT_COLS = [
    "GP", "W", "L", "W_PCT", "MIN",
    "PTS", "REB", "AST", "STL", "BLK", "TOV",
    "FG_PCT", "FG3_PCT", "FT_PCT", "PLUS_MINUS",
    "NBA_FANTASY_PTS", "DD2", "TD3",
]
STAT_COLS_FULL = [
    "GP", "W", "L", "W_PCT", "MIN",
    "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT",
    "OREB", "DREB", "REB", "AST", "TOV", "STL", "BLK", "BLKA", "PF", "PFD",
    "PTS", "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3",
]
RANK_COLS = [
    "PTS_RANK", "REB_RANK", "AST_RANK", "STL_RANK", "BLK_RANK",
    "FG_PCT_RANK", "FG3_PCT_RANK", "PLUS_MINUS_RANK",
]
PO_ROUND_LABEL = {
    "1": "第一輪",
    "2": "第二輪",
    "3": "分區決賽",
    "4": "總冠軍賽",
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
    last_n_games: int = 0,
    month: int = 0,
    period: int = 0,
    date_from: str = "",
    date_to: str = "",
    location: str = "",
    outcome: str = "",
    season_segment: str = "",
    vs_conference: str = "",
    po_round: str = "",
) -> PlayerDashboardByGameSplits:
    """取得 GameSplits Dashboard，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashboardByGameSplits(
                player_id=player_id,
                season=season,
                season_type_playoffs=season_type,
                per_mode_detailed=per_mode,
                last_n_games=str(last_n_games),
                month=str(month),
                period=str(period),
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                location_nullable=location,
                outcome_nullable=outcome,
                season_segment_nullable=season_segment,
                vs_conference_nullable=vs_conference,
                po_round_nullable=po_round,
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
    num_cols = [c for c in df.columns if c not in SKIP_COLS and c != "GROUP_VALUE"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


def _show_split(
    df: pd.DataFrame,
    title: str,
    cols: list[str] = STAT_COLS,
    show_rank: bool = False,
) -> None:
    if df.empty:
        print(f"── {title}：無資料\n")
        return
    df = _clean(df)
    avail = ["GROUP_VALUE"] + [c for c in cols if c in df.columns]
    print(f"── {title} " + "─" * max(0, 58 - len(title)))
    print(df[avail].to_string(index=False))
    if show_rank:
        r_avail = ["GROUP_VALUE"] + [c for c in RANK_COLS if c in df.columns]
        if len(r_avail) > 1:
            print(f"\n  排名（數字越小 = 全聯盟越前）：")
            print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 1. 整體（Overall）
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashboardByGameSplits, label: str) -> None:
    df = resp.overall_player_dashboard.get_data_frame()
    _show_split(df, f"{label}整體數據", show_rank=True)


# ---------------------------------------------------------------------------
# 2. 逐節（ByPeriod：Q1 / Q2 / Q3 / Q4 / OT）
# ---------------------------------------------------------------------------

def show_by_period(resp: PlayerDashboardByGameSplits, label: str) -> None:
    """
    各節次數據。
    GROUP_VALUE 值：Q1 / Q2 / Q3 / Q4 / OT1 / OT2 …
    此 DataSet 的 PTS 為該節得分，MIN 為該節上場時間。
    """
    df = resp.by_period_player_dashboard.get_data_frame()
    period_cols = ["GP", "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
                   "FG_PCT", "FG3_PCT", "FT_PCT", "PLUS_MINUS"]
    _show_split(df, f"{label}逐節數據（Q1 / Q2 / Q3 / Q4 / OT）",
                cols=period_cols)


# ---------------------------------------------------------------------------
# 3. 上下半場（ByHalf）
# ---------------------------------------------------------------------------

def show_by_half(resp: PlayerDashboardByGameSplits, label: str) -> None:
    """
    GROUP_VALUE：First Half / Second Half
    可觀察球員是否在上半場或下半場表現較佳。
    """
    df = resp.by_half_player_dashboard.get_data_frame()
    half_cols = ["GP", "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
                 "FG_PCT", "FG3_PCT", "FT_PCT", "PLUS_MINUS",
                 "NBA_FANTASY_PTS"]
    _show_split(df, f"{label}上半場 vs 下半場（ByHalf）", cols=half_cols)


# ---------------------------------------------------------------------------
# 4. 比分差情境（ByScoreMargin）
# ---------------------------------------------------------------------------

def show_by_score_margin(resp: PlayerDashboardByGameSplits, label: str) -> None:
    """
    依比賽最終比分差距分組，常見 GROUP_VALUE：
    Large Lead（大勝）/ Small Lead（小勝）/ Small Trail（小輸）/ Large Trail（大輸）
    可觀察球員在輕鬆勝場、膠著戰、逆境中的表現差異。
    """
    df = resp.by_score_margin_player_dashboard.get_data_frame()
    margin_cols = ["GP", "W_PCT", "MIN", "PTS", "REB", "AST", "STL", "BLK",
                   "TOV", "FG_PCT", "FG3_PCT", "PLUS_MINUS", "NBA_FANTASY_PTS"]
    _show_split(df, f"{label}比分差情境（ByScoreMargin）", cols=margin_cols)


# ---------------------------------------------------------------------------
# 5. 實際比分差距帶（ByActualMargin）
# ---------------------------------------------------------------------------

def show_by_actual_margin(resp: PlayerDashboardByGameSplits, label: str) -> None:
    """
    以具體比分差距帶分組（如 -10以下 / -6~-10 / -1~-5 / 0 / +1~+5 / +6~+10 / +10以上）
    可觀察球員在各種緊張度下的表現，找出「壓力型球員」或「順風型球員」特徵。
    """
    df = resp.by_actual_margin_player_dashboard.get_data_frame()
    actual_cols = ["GP", "W_PCT", "MIN", "PTS", "REB", "AST",
                   "FG_PCT", "FG3_PCT", "PLUS_MINUS"]
    _show_split(df, f"{label}實際比分差距帶（ByActualMargin）", cols=actual_cols)


# ---------------------------------------------------------------------------
# 6. 關鍵時刻分析（Clutch 指標衍生）
# ---------------------------------------------------------------------------

def show_clutch_profile(resp: PlayerDashboardByGameSplits, label: str) -> None:
    """
    從 ByScoreMargin 和 ByActualMargin 的資料，
    萃取「膠著戰」（差距在 5 分以內）vs「非膠著戰」的數據對比。
    """
    margin_df  = _clean(resp.by_score_margin_player_dashboard.get_data_frame())
    actual_df  = _clean(resp.by_actual_margin_player_dashboard.get_data_frame())

    if margin_df.empty and actual_df.empty:
        return

    print(f"── {label}關鍵時刻側寫 " + "─" * 42)

    # ByScoreMargin：找出膠著勝 / 膠著負
    if not margin_df.empty and "GROUP_VALUE" in margin_df.columns:
        close = margin_df[
            margin_df["GROUP_VALUE"].str.upper().str.contains("SMALL|CLOSE", na=False)
        ]
        blowout = margin_df[
            margin_df["GROUP_VALUE"].str.upper().str.contains("LARGE", na=False)
        ]
        for sub, name in [(close, "膠著場次"), (blowout, "大差距場次")]:
            if sub.empty:
                continue
            cols = ["PTS", "REB", "AST", "FG_PCT", "PLUS_MINUS"]
            avail = [c for c in cols if c in sub.columns]
            gp_total = sub["GP"].sum() if "GP" in sub.columns else 0
            print(f"  {name}（GP={int(gp_total)}）：", end="")
            means = sub[avail].mean()
            print("  ".join(f"{c} {means[c]:.2f}" for c in avail))

    # ByActualMargin：找出正負 5 分以內的差距帶
    if not actual_df.empty and "GROUP_VALUE" in actual_df.columns:
        close_bands = actual_df[
            actual_df["GROUP_VALUE"].str.contains(r"[-+]?[0-5]", regex=True, na=False)
        ]
        if not close_bands.empty:
            cols = ["PTS", "REB", "AST", "FG_PCT", "PLUS_MINUS"]
            avail = [c for c in cols if c in close_bands.columns]
            gp_total = close_bands["GP"].sum() if "GP" in close_bands.columns else 0
            print(f"  ±5分差距帶（GP≈{int(gp_total)}）：", end="")
            means = close_bands[avail].mean()
            print("  ".join(f"{c} {means[c]:.2f}" for c in avail))
    print()


# ---------------------------------------------------------------------------
# 7. 常規賽 vs 季後賽 Overall 並排對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashboardByGameSplits,
    po_resp: PlayerDashboardByGameSplits,
) -> None:
    reg_df = _clean(reg_resp.overall_player_dashboard.get_data_frame())
    po_df  = _clean(po_resp.overall_player_dashboard.get_data_frame())
    if reg_df.empty or po_df.empty:
        return

    avail = [c for c in STAT_COLS_FULL if c in reg_df.columns and c in po_df.columns]
    reg_row = reg_df.iloc[0]
    po_row  = po_df.iloc[0]

    cmp = pd.DataFrame({
        "常規賽": {c: reg_row[c] for c in avail},
        "季後賽": {c: po_row[c] for c in avail},
    }).round(3)
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽 Overall 對比 " + "─" * 30)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 8. 常規賽 vs 季後賽逐節對比
# ---------------------------------------------------------------------------

def show_period_comparison(
    reg_resp: PlayerDashboardByGameSplits,
    po_resp: PlayerDashboardByGameSplits,
) -> None:
    """常規賽與季後賽各節次場均得分 / 命中率並排。"""
    reg_df = _clean(reg_resp.by_period_player_dashboard.get_data_frame())
    po_df  = _clean(po_resp.by_period_player_dashboard.get_data_frame())

    if reg_df.empty or po_df.empty:
        return

    cmp_cols = ["PTS", "FG_PCT", "FG3_PCT", "AST", "PLUS_MINUS"]

    rows = []
    for _, reg_row in reg_df.iterrows():
        gv = reg_row["GROUP_VALUE"]
        po_row_match = po_df[po_df["GROUP_VALUE"] == gv]
        if po_row_match.empty:
            continue
        po_row = po_row_match.iloc[0]
        entry = {"節次": gv}
        for col in cmp_cols:
            if col in reg_df.columns:
                entry[f"常規_{col}"] = round(float(reg_row[col]), 3) if pd.notna(reg_row.get(col)) else None
                entry[f"季後_{col}"] = round(float(po_row[col]), 3) if pd.notna(po_row.get(col)) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("節次")
    print("── 常規賽 vs 季後賽逐節對比 " + "─" * 36)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 9. 季後賽各輪次分析（po_round_nullable）
# ---------------------------------------------------------------------------

def show_playoff_by_round(player_id: int, season: str, per_mode: str) -> None:
    """逐輪抓取 Overall + 逐節數據，比較各輪次整體表現。"""
    rows = []
    for round_num, round_lbl in PO_ROUND_LABEL.items():
        try:
            resp = fetch_dashboard(
                player_id=player_id,
                season=season,
                season_type=SEASON_TYPE_PLAYOFFS,
                per_mode=per_mode,
                po_round=round_num,
            )
        except Exception as e:
            print(f"  {round_lbl} 取得失敗: {e}")
            continue

        df = _clean(resp.overall_player_dashboard.get_data_frame())
        if df.empty:
            continue

        row   = df.iloc[0]
        entry = {"輪次": round_lbl}
        for col in ["GP", "W", "L", "W_PCT", "PTS", "REB", "AST",
                    "STL", "BLK", "TOV", "FG_PCT", "FG3_PCT",
                    "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3"]:
            if col in df.columns:
                v = row.get(col)
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        print("  無各輪次資料（可能尚未進入該輪次）\n")
        return

    result = pd.DataFrame(rows).set_index("輪次")
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"
PER_MODE = PerModeDetailed.per_game

if __name__ == "__main__":
    query  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    season = sys.argv[2] if len(sys.argv) > 2 else Season.default

    player_id, full_name = _find_player_id(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}  |  PerMode: {PER_MODE}\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    reg_resp = fetch_dashboard(player_id, season=season,
                               season_type=SEASON_TYPE_REGULAR,
                               per_mode=PER_MODE)

    show_overall(reg_resp,          label="常規賽 ")
    show_by_period(reg_resp,        label="常規賽 ")
    show_by_half(reg_resp,          label="常規賽 ")
    show_by_score_margin(reg_resp,  label="常規賽 ")
    show_by_actual_margin(reg_resp, label="常規賽 ")
    show_clutch_profile(reg_resp,   label="常規賽 ")

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    po_resp = None
    try:
        po_resp = fetch_dashboard(player_id, season=season,
                                  season_type=SEASON_TYPE_PLAYOFFS,
                                  per_mode=PER_MODE)

        po_overall = po_resp.overall_player_dashboard.get_data_frame()
        if po_overall.empty:
            print(f"  {full_name} 本季無季後賽出賽記錄（或尚未進入季後賽）。\n")
            po_resp = None
        else:
            show_overall(po_resp,          label="季後賽 ")
            show_by_period(po_resp,        label="季後賽 ")
            show_by_half(po_resp,          label="季後賽 ")
            show_by_score_margin(po_resp,  label="季後賽 ")
            show_by_actual_margin(po_resp, label="季後賽 ")
            show_clutch_profile(po_resp,   label="季後賽 ")

            print("=" * 65)
            print("  季後賽各輪次分析（po_round_nullable）")
            print("=" * 65 + "\n")
            show_playoff_by_round(player_id, season, PER_MODE)

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 並排對比 ──────────────────────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽並排對比")
        print("=" * 65 + "\n")

        show_regular_vs_playoffs_comparison(reg_resp, po_resp)
        show_period_comparison(reg_resp, po_resp)
