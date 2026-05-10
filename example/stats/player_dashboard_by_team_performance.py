"""
nba_api.stats.endpoints.playerdashboardbyteamperformance 範例程式

執行方式：
  python example/stats/player_dashboard_by_team_performance.py                   # 預設球員，本季
  python example/stats/player_dashboard_by_team_performance.py "Stephen Curry"   # 依姓名查詢
  python example/stats/player_dashboard_by_team_performance.py 2544              # 依 player_id
  python example/stats/player_dashboard_by_team_performance.py 2544 2023-24      # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashboardByTeamPerformance 聚焦「球隊整體表現情境」：
  - 球員在「球隊高得分場次」vs「低得分場次」的個人數據
  - 球員在「對手高得分場次」vs「對手低得分場次」的個人數據
  - 球員在「大勝場次」vs「膠著戰」vs「大敗場次」的個人數據
  → 可判斷球員是否為真正的差異製造者，還是只在輕鬆場次衝高數據

與 GameSplits（ByActualMargin / ByScoreMargin）的差異：
  - GameSplits 以「比賽節次 / 時段 / 比分差情境」分組
  - TeamPerformance 以「球隊進攻端 / 防守端 / 最終比分差」的分組帶為維度

DataSet 總覽（共 4 個）：
  overall_player_dashboard              — 整體（單列）
  points_scored_player_dashboard        — 球隊得分帶分組
                                          （GROUP_VALUE：球隊得幾分的場次）
  ponts_against_player_dashboard        — 對手得分帶分組
                                          （GROUP_VALUE：對手得幾分的場次）
                                          ※ 注意：API 原始名稱拼字為 "Ponts" 非 "Points"
  score_differential_player_dashboard   — 最終比分差距帶分組
                                          （GROUP_VALUE：最終輸贏差多少分）

獨有欄位（PointsScored / PontsAgainst / ScoreDifferential 三個 DataSet）：
  GROUP_VALUE_ORDER — 排序用數字（由低分帶到高分帶）
  GROUP_VALUE_2     — 分帶上界或次要標籤

共用欄位（同其他 Dashboard）：
  GROUP_VALUE / GP / W / L / W_PCT / MIN
  FGM/FGA/FG_PCT / FG3M/FG3A/FG3_PCT / FTM/FTA/FT_PCT
  OREB/DREB/REB / AST/TOV/STL/BLK/BLKA/PF/PFD
  PTS / PLUS_MINUS / NBA_FANTASY_PTS / DD2 / TD3 / *_RANK

參數（同其他 Dashboard）：
  player_id / season / season_type_playoffs / per_mode_detailed /
  measure_type_detailed / po_round_nullable 等
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashboardbyteamperformance import (
    PlayerDashboardByTeamPerformance,
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
STR_COLS  = {"GROUP_VALUE", "GROUP_VALUE_2"}

STAT_COLS = [
    "GP", "W", "L", "W_PCT", "MIN",
    "PTS", "REB", "AST", "STL", "BLK", "TOV",
    "FG_PCT", "FG3_PCT", "FT_PCT",
    "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3",
]
RANK_COLS = [
    "PTS_RANK", "REB_RANK", "AST_RANK",
    "FG_PCT_RANK", "FG3_PCT_RANK", "PLUS_MINUS_RANK",
]
KEY_COLS = ["GP", "W_PCT", "PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "PLUS_MINUS"]
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
) -> PlayerDashboardByTeamPerformance:
    """取得 TeamPerformance Dashboard，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashboardByTeamPerformance(
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
    num_cols = [c for c in df.columns if c not in SKIP_COLS and c not in STR_COLS
                and c != "GROUP_VALUE_ORDER"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    if "GROUP_VALUE_ORDER" in df.columns:
        df["GROUP_VALUE_ORDER"] = pd.to_numeric(df["GROUP_VALUE_ORDER"], errors="coerce")
    return df


def _show_split(
    df: pd.DataFrame,
    title: str,
    id_cols: list[str],
    stat_cols: list[str] = KEY_COLS,
    show_rank: bool = False,
    sort_by: str = "GROUP_VALUE_ORDER",
) -> None:
    """通用：印出分組得分帶數據。"""
    if df.empty:
        print(f"── {title}：無資料\n")
        return
    df = _clean(df)
    if sort_by in df.columns:
        df = df.sort_values(sort_by)
    avail = [c for c in id_cols + stat_cols if c in df.columns]
    print(f"── {title} " + "─" * max(0, 58 - len(title)))
    print(df[avail].to_string(index=False))
    if show_rank:
        r_avail = [id_cols[0]] + [c for c in RANK_COLS if c in df.columns]
        if len(r_avail) > 1:
            print(f"\n  排名（數字越小 = 全聯盟越前）：")
            print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 1. 整體（Overall）
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashboardByTeamPerformance, label: str) -> None:
    df = _clean(resp.overall_player_dashboard.get_data_frame())
    if df.empty:
        print(f"── {label}整體：無資料\n")
        return
    avail  = ["GROUP_VALUE"] + [c for c in STAT_COLS if c in df.columns]
    r_avail = ["GROUP_VALUE"] + [c for c in RANK_COLS if c in df.columns]
    print(f"── {label}整體數據 " + "─" * 50)
    print(df[avail].to_string(index=False))
    if len(r_avail) > 1:
        print(f"\n  全聯盟排名：")
        print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 球隊得分帶（PointsScored）
# ---------------------------------------------------------------------------

def show_points_scored(resp: PlayerDashboardByTeamPerformance, label: str) -> None:
    """
    依球隊最終得分分帶顯示球員個人數據。
    GROUP_VALUE：球隊得幾分時（如 "80-89", "90-99", "100-109", "110+"）
    可觀察球員是否在球隊大爆發時衝高數據，或在低得分場次也能貢獻。
    """
    df = resp.points_scored_player_dashboard.get_data_frame()
    _show_split(
        df,
        f"{label}球隊得分帶（PointsScored）",
        id_cols=["GROUP_VALUE", "GROUP_VALUE_2"],
    )


# ---------------------------------------------------------------------------
# 3. 對手得分帶（PontsAgainst）— 注意 API 拼字為 "Ponts"
# ---------------------------------------------------------------------------

def show_points_against(resp: PlayerDashboardByTeamPerformance, label: str) -> None:
    """
    依對手最終得分分帶顯示球員個人數據。
    GROUP_VALUE：對手得幾分時（如 "80-89 pts allowed", "110+ pts allowed"）
    可觀察球員在防守崩盤場次（對手高得分）是否還能維持個人輸出。
    ※ 注意：API DataSet 名稱原始拼字為 "PontsAgainst"（少一個 i）。
    """
    df = resp.ponts_against_player_dashboard.get_data_frame()
    _show_split(
        df,
        f"{label}對手得分帶（PontsAgainst）",
        id_cols=["GROUP_VALUE", "GROUP_VALUE_2"],
    )


# ---------------------------------------------------------------------------
# 4. 比分差距帶（ScoreDifferential）
# ---------------------------------------------------------------------------

def show_score_differential(resp: PlayerDashboardByTeamPerformance, label: str) -> None:
    """
    依最終比分差分帶顯示球員個人數據。
    GROUP_VALUE：比賽以幾分差結束（如 "-20+", "-11 to -20", "-1 to -10",
                                    "+1 to +10", "+11 to +20", "+20+"）
    可看出球員在大勝 / 小勝 / 小敗 / 大敗中的個人表現差異，
    判斷是否有「得到就算」的問題。
    """
    df = resp.score_differential_player_dashboard.get_data_frame()
    _show_split(
        df,
        f"{label}比分差距帶（ScoreDifferential）",
        id_cols=["GROUP_VALUE", "GROUP_VALUE_2"],
    )


# ---------------------------------------------------------------------------
# 5. 綜合分析：差異製造者 vs 數據刷分者
# ---------------------------------------------------------------------------

def show_impact_analysis(resp: PlayerDashboardByTeamPerformance, label: str) -> None:
    """
    從三個維度綜合評估：
    1. 球隊低得分（<100）vs 高得分（≥110）的個人產出差異
    2. 膠著勝負（差距 ≤10分）vs 大差距場次（>10分）的數據差異
    → 如果球員在低得分 / 膠著場次維持高水準，才是真正的球隊基石
    """
    print(f"── {label}差異製造者分析 " + "─" * 44)

    # ── 球隊得分：低 vs 高 ──────────────────────────────────────────────
    ps_df = _clean(resp.points_scored_player_dashboard.get_data_frame())
    if not ps_df.empty and "GROUP_VALUE" in ps_df.columns:
        ps_df = ps_df.sort_values("GROUP_VALUE_ORDER") if "GROUP_VALUE_ORDER" in ps_df.columns else ps_df
        low_team  = ps_df[ps_df["GROUP_VALUE"].str.contains(r"^[89]\d|^[0-7]\d", regex=True, na=False)]
        high_team = ps_df[ps_df["GROUP_VALUE"].str.contains(r"^11\d|^1[2-9]\d|120", regex=True, na=False)]

        def _fmt_row(sub: pd.DataFrame, lbl: str) -> None:
            if sub.empty:
                return
            cols = ["PTS", "FG_PCT", "AST", "PLUS_MINUS"]
            avail = [c for c in cols if c in sub.columns]
            means = sub[avail].mean()
            gp = int(sub["GP"].sum()) if "GP" in sub.columns else "?"
            vals = "  ".join(f"{c} {means[c]:.2f}" for c in avail)
            print(f"  {lbl:<20} (GP≈{gp}): {vals}")

        print("  球隊得分帶：")
        _fmt_row(low_team,  "低得分場次（<100分）")
        _fmt_row(high_team, "高得分場次（≥110分）")
        print()

    # ── 比分差：膠著 vs 大差距 ─────────────────────────────────────────
    sd_df = _clean(resp.score_differential_player_dashboard.get_data_frame())
    if not sd_df.empty and "GROUP_VALUE" in sd_df.columns:
        sd_df = sd_df.sort_values("GROUP_VALUE_ORDER") if "GROUP_VALUE_ORDER" in sd_df.columns else sd_df
        close  = sd_df[sd_df["GROUP_VALUE"].str.contains(r"[+-]?1 to|[+-]?[1-9] to [+-]?10", regex=True, na=False)]
        blowout = sd_df[sd_df["GROUP_VALUE"].str.contains(r"20\+|20 \+|\+20|[2-9]\d", regex=True, na=False)]

        print("  比分差距帶：")
        _fmt_row(close,   "膠著場次（差距 ≤10分）")
        _fmt_row(blowout, "大差距場次（差距 >20分）")
        print()


# ---------------------------------------------------------------------------
# 6. 常規賽 vs 季後賽 Overall 並排對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashboardByTeamPerformance,
    po_resp: PlayerDashboardByTeamPerformance,
) -> None:
    reg_df = _clean(reg_resp.overall_player_dashboard.get_data_frame())
    po_df  = _clean(po_resp.overall_player_dashboard.get_data_frame())
    if reg_df.empty or po_df.empty:
        return

    avail = [c for c in STAT_COLS if c in reg_df.columns and c in po_df.columns]
    reg_row = reg_df.iloc[0]
    po_row  = po_df.iloc[0]
    cmp = pd.DataFrame({
        "常規賽": {c: reg_row[c] for c in avail},
        "季後賽": {c: po_row[c]  for c in avail},
    }).round(3)
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽 Overall 對比 " + "─" * 30)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 7. 常規賽 vs 季後賽比分差帶對比
# ---------------------------------------------------------------------------

def show_score_diff_comparison(
    reg_resp: PlayerDashboardByTeamPerformance,
    po_resp: PlayerDashboardByTeamPerformance,
) -> None:
    """比較常規賽與季後賽在各比分差帶的 PTS / FG% / W_PCT。"""
    reg_df = _clean(reg_resp.score_differential_player_dashboard.get_data_frame())
    po_df  = _clean(po_resp.score_differential_player_dashboard.get_data_frame())

    if reg_df.empty:
        return

    po_map: dict[str, pd.Series] = {}
    if not po_df.empty:
        for _, row in po_df.iterrows():
            po_map[str(row.get("GROUP_VALUE", ""))] = row

    cmp_cols = ["W_PCT", "PTS", "FG_PCT", "PLUS_MINUS"]

    rows = []
    if "GROUP_VALUE_ORDER" in reg_df.columns:
        reg_df = reg_df.sort_values("GROUP_VALUE_ORDER")

    for _, reg_row in reg_df.iterrows():
        gv = str(reg_row.get("GROUP_VALUE", "")).strip()
        entry = {"比分差帶": gv}
        for col in cmp_cols:
            r_v = reg_row.get(col)
            entry[f"常規_{col}"] = round(float(r_v), 3) if pd.notna(r_v) else None
            po_row = po_map.get(gv)
            if po_row is not None:
                p_v = po_row.get(col)
                entry[f"季後_{col}"] = round(float(p_v), 3) if pd.notna(p_v) else None
            else:
                entry[f"季後_{col}"] = None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("比分差帶")
    print("── 常規賽 vs 季後賽比分差帶對比 " + "─" * 30)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 8. 季後賽各輪次分析（po_round_nullable）
# ---------------------------------------------------------------------------

def show_playoff_by_round(player_id: int, season: str, per_mode: str) -> None:
    """逐輪比較整體 + 比分差帶的核心數據。"""
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

        row = df.iloc[0]
        entry = {"輪次": round_lbl}
        for col in ["GP", "W", "L", "W_PCT", "PTS", "REB", "AST",
                    "FG_PCT", "FG3_PCT", "PLUS_MINUS", "NBA_FANTASY_PTS"]:
            if col in df.columns:
                v = row.get(col)
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        print("  無各輪次資料\n")
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

    show_overall(reg_resp,           label="常規賽 ")
    show_points_scored(reg_resp,     label="常規賽 ")
    show_points_against(reg_resp,    label="常規賽 ")
    show_score_differential(reg_resp, label="常規賽 ")
    show_impact_analysis(reg_resp,   label="常規賽 ")

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
            show_overall(reg_resp,           label="季後賽 ")
            show_points_scored(po_resp,      label="季後賽 ")
            show_points_against(po_resp,     label="季後賽 ")
            show_score_differential(po_resp, label="季後賽 ")
            show_impact_analysis(po_resp,    label="季後賽 ")

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
        show_score_diff_comparison(reg_resp, po_resp)
