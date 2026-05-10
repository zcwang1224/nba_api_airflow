"""
nba_api.stats.endpoints.playerdashboardbyshootingsplits 範例程式

執行方式：
  python example/stats/player_dashboard_by_shooting_splits.py                   # 預設球員，本季
  python example/stats/player_dashboard_by_shooting_splits.py "Stephen Curry"   # 依姓名查詢
  python example/stats/player_dashboard_by_shooting_splits.py 2544              # 依 player_id
  python example/stats/player_dashboard_by_shooting_splits.py 2544 2023-24      # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽（各自完整 8 個 DataSet），最後並排對比 ──

PlayerDashboardByShootingSplits 與 PlayerDashboardByGeneralSplits 差異：
  - 聚焦「投籃」維度：出手距離 / 區域 / 類型 / 有無助攻
  - 無 PTS / REB / AST / STL / BLK 等一般數據
  - 獨有欄位：EFG_PCT（有效命中率）、PCT_AST_2PM / PCT_UAST_2PM 等助攻占比
  - AssistedBy DataSet 結構特殊：每列為助攻該球員的隊友

參數（與 PlayerDashboardByGeneralSplits 相同，略）：
  player_id / season / season_type_playoffs / per_mode_detailed /
  measure_type_detailed / last_n_games / po_round_nullable 等

DataSet 總覽（共 8 個）：
  overall_player_dashboard             — 整體投籃數據
  shot_area_player_dashboard           — 投籃區域
                                         (Left Side C / Left Side / Center /
                                          Right Side / Right Side C / Back Court)
  shot_type_player_dashboard           — 投籃類型明細
                                         (Alley Oop / Dunk / Hook Shot /
                                          Jump Shot / Layup / Tip Shot)
  shot_type_summary_player_dashboard   — 投籃類型摘要（2-Pointer / 3-Pointer）
  shot5_ft_player_dashboard            — 5 英尺距離帶（0-5 / 5-10 / 10-15…）
  shot8_ft_player_dashboard            — 8 英尺距離帶（0-8 / 8-16 / 16-24 / 24+）
  assited_shot_player_dashboard        — 有助攻 vs 無助攻投籃
                                         ※ 注意：API 原始名稱拼字為 "Assited"
  assisted_by                          — 各隊友助攻該球員的投籃統計
                                         （GROUP_VALUE 替換為 PLAYER_ID / PLAYER_NAME）

共用欄位（最多 DataSet 使用）：
  GROUP_VALUE   — 分組值（如 "Left Side C", "Jump Shot", "Assisted"）
  FGM / FGA / FG_PCT     — 投籃命中 / 出手 / 命中率
  FG3M / FG3A / FG3_PCT  — 三分命中 / 出手 / 命中率
  EFG_PCT       — 有效命中率（= (FGM + 0.5*FG3M) / FGA）
  BLKA          — 被封蓋次數
  PCT_AST_2PM   — 2 分球命中中有助攻的比例
  PCT_UAST_2PM  — 2 分球命中中無助攻的比例
  PCT_AST_3PM   — 3 分球命中中有助攻的比例
  PCT_UAST_3PM  — 3 分球命中中無助攻的比例
  PCT_AST_FGM   — 所有命中中有助攻的比例
  PCT_UAST_FGM  — 所有命中中無助攻的比例
  *_RANK        — 各項目全聯盟排名（ShotTypeSummary 無 RANK 欄）

AssistedBy 特殊欄位（無 GROUP_VALUE）：
  PLAYER_ID / PLAYER_NAME — 助攻球員
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashboardbyshootingsplits import (
    PlayerDashboardByShootingSplits,
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

# 各 DataSet 要顯示的核心欄位
SHOT_CORE = [
    "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "EFG_PCT", "BLKA",
    "PCT_AST_FGM", "PCT_UAST_FGM",
    "PCT_AST_2PM", "PCT_UAST_2PM",
    "PCT_AST_3PM", "PCT_UAST_3PM",
]
SHOT_RANK = [
    "FG_PCT_RANK", "FG3_PCT_RANK", "EFG_PCT_RANK",
    "PCT_AST_FGM_RANK", "PCT_UAST_FGM_RANK",
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
) -> PlayerDashboardByShootingSplits:
    """取得 ShootingSplits Dashboard，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashboardByShootingSplits(
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


def _clean(df: pd.DataFrame, id_col: str = "GROUP_VALUE") -> pd.DataFrame:
    """數值欄位轉型，保留 id_col 為字串。"""
    if df.empty:
        return df
    num_cols = [c for c in df.columns if c not in SKIP_COLS and c != id_col]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


def _show_split(
    df: pd.DataFrame,
    title: str,
    id_col: str = "GROUP_VALUE",
    core_cols: list[str] = SHOT_CORE,
    rank_cols: list[str] = SHOT_RANK,
    show_rank: bool = False,
) -> None:
    """通用：印出一個 DataSet 的分組投籃數據。"""
    if df.empty:
        print(f"── {title}：無資料\n")
        return
    df = _clean(df, id_col)
    avail = [id_col] + [c for c in core_cols if c in df.columns]
    print(f"── {title} " + "─" * max(0, 58 - len(title)))
    print(df[avail].to_string(index=False))
    if show_rank and rank_cols:
        r_avail = [id_col] + [c for c in rank_cols if c in df.columns]
        if len(r_avail) > 1:
            print(f"\n  排名（數字越小 = 全聯盟越前）：")
            print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 1. 整體（Overall）
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashboardByShootingSplits, label: str) -> None:
    df = resp.overall_player_dashboard.get_data_frame()
    _show_split(df, f"{label}整體投籃數據", show_rank=True)


# ---------------------------------------------------------------------------
# 2. 投籃區域（Shot Area）
# ---------------------------------------------------------------------------

def show_shot_area(resp: PlayerDashboardByShootingSplits, label: str) -> None:
    """
    投籃區域細分：
    Left Side C / Left Side / Center / Right Side / Right Side C / Back Court
    """
    df = resp.shot_area_player_dashboard.get_data_frame()
    # 只顯示核心命中率欄，不顯示助攻占比，區域數據看命中率更直觀
    area_cols = ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "EFG_PCT", "BLKA"]
    _show_split(df, f"{label}投籃區域（Shot Area）", core_cols=area_cols,
                rank_cols=["FG_PCT_RANK", "FG3_PCT_RANK", "EFG_PCT_RANK"], show_rank=False)


# ---------------------------------------------------------------------------
# 3. 投籃類型摘要（Shot Type Summary）
# ---------------------------------------------------------------------------

def show_shot_type_summary(resp: PlayerDashboardByShootingSplits, label: str) -> None:
    """
    2-Pointer vs 3-Pointer 摘要（此 DataSet 無 RANK 欄）。
    """
    df = resp.shot_type_summary_player_dashboard.get_data_frame()
    summary_cols = ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
                    "EFG_PCT", "PCT_AST_FGM", "PCT_UAST_FGM"]
    _show_split(df, f"{label}投籃類型摘要（2-Pointer / 3-Pointer）",
                core_cols=summary_cols, show_rank=False)


# ---------------------------------------------------------------------------
# 4. 投籃類型明細（Shot Type）
# ---------------------------------------------------------------------------

def show_shot_type(resp: PlayerDashboardByShootingSplits, label: str) -> None:
    """
    各種投籃動作：
    Alley Oop / Dunk / Hook Shot / Jump Shot / Layup / Tip Shot 等
    """
    df = resp.shot_type_player_dashboard.get_data_frame()
    type_cols = ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
                 "EFG_PCT", "BLKA", "PCT_AST_FGM", "PCT_UAST_FGM"]
    _show_split(df, f"{label}投籃類型明細（Shot Type）", core_cols=type_cols)


# ---------------------------------------------------------------------------
# 5. 距離帶：5 英尺與 8 英尺
# ---------------------------------------------------------------------------

def show_distance_splits(resp: PlayerDashboardByShootingSplits, label: str) -> None:
    """
    Shot5FT：按 5 英尺一帶分（0-5 / 5-10 / 10-15 / 15-19 / 19-24 / 24+）
    Shot8FT：按 8 英尺一帶分（0-8 / 8-16 / 16-24 / 24+）
    """
    dist_cols = ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "EFG_PCT", "BLKA"]

    df5 = resp.shot5_ft_player_dashboard.get_data_frame()
    _show_split(df5, f"{label}距離帶（5 英尺）", core_cols=dist_cols)

    df8 = resp.shot8_ft_player_dashboard.get_data_frame()
    _show_split(df8, f"{label}距離帶（8 英尺）", core_cols=dist_cols)


# ---------------------------------------------------------------------------
# 6. 有無助攻分析（Assisted Shot）
# ---------------------------------------------------------------------------

def show_assisted_shot(resp: PlayerDashboardByShootingSplits, label: str) -> None:
    """
    有助攻（Assisted）vs 無助攻（Unassisted）的投籃效率對比。
    ※ 注意 API 原始 DataSet 名稱拼字為 "Assited"（非 "Assisted"）。
    """
    df = resp.assited_shot_player_dashboard.get_data_frame()
    ast_cols = ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "EFG_PCT",
                "PCT_AST_FGM", "PCT_UAST_FGM", "PCT_AST_2PM", "PCT_AST_3PM"]
    _show_split(df, f"{label}有助攻 vs 無助攻投籃（AssistedShot）",
                core_cols=ast_cols, show_rank=False)


# ---------------------------------------------------------------------------
# 7. 助攻來源：被哪些隊友助攻（Assisted By）
# ---------------------------------------------------------------------------

def show_assisted_by(resp: PlayerDashboardByShootingSplits, label: str) -> None:
    """
    AssistedBy DataSet：每列為助攻該球員的隊友。
    結構與其他 DataSet 不同：GROUP_VALUE 替換為 PLAYER_ID + PLAYER_NAME。
    """
    df = resp.assisted_by.get_data_frame()
    if df.empty:
        print(f"── {label}助攻來源（AssistedBy）：無資料\n")
        return

    df = _clean(df, id_col="PLAYER_NAME")
    ast_by_cols = ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
                   "EFG_PCT", "PCT_AST_2PM", "PCT_AST_3PM", "PCT_AST_FGM"]
    avail = ["PLAYER_NAME"] + [c for c in ast_by_cols if c in df.columns]

    # 依 FGM 降序排列，最多助攻的隊友在前
    df_sorted = df.sort_values("FGM", ascending=False) if "FGM" in df.columns else df

    print(f"── {label}助攻來源（各隊友助攻命中統計）" + "─" * 22)
    print(df_sorted[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 8. 常規賽 vs 季後賽整體投籃對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashboardByShootingSplits,
    po_resp: PlayerDashboardByShootingSplits,
) -> None:
    """常規賽與季後賽 Overall 投籃數據並排，計算差值。"""
    reg_df = _clean(reg_resp.overall_player_dashboard.get_data_frame())
    po_df  = _clean(po_resp.overall_player_dashboard.get_data_frame())

    if reg_df.empty or po_df.empty:
        return

    compare_cols = ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
                    "EFG_PCT", "BLKA",
                    "PCT_AST_FGM", "PCT_UAST_FGM",
                    "PCT_AST_2PM", "PCT_UAST_2PM",
                    "PCT_AST_3PM", "PCT_UAST_3PM"]
    avail = [c for c in compare_cols if c in reg_df.columns and c in po_df.columns]

    reg_row = reg_df.iloc[0]
    po_row  = po_df.iloc[0]

    cmp = pd.DataFrame({
        "常規賽": {c: reg_row[c] for c in avail},
        "季後賽": {c: po_row[c] for c in avail},
    }).round(3)
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽整體投籃對比 " + "─" * 30)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 9. 季後賽各輪次投籃分析（po_round_nullable）
# ---------------------------------------------------------------------------

def show_playoff_by_round(player_id: int, season: str, per_mode: str) -> None:
    """逐輪抓取 Overall 投籃數據，比較各輪投籃效率變化。"""
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
        for col in ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
                    "EFG_PCT", "BLKA", "PCT_AST_FGM", "PCT_UAST_FGM"]:
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
# 10. 全場景整體投籃對比（常規賽多種 per_mode）
# ---------------------------------------------------------------------------

def show_per_mode_comparison(player_id: int, season: str, season_type: str) -> None:
    """
    同一球季相同賽季類型，比較 PerGame / Per36 / Per100Possessions 的整體投籃數據。
    因為投籃效率指標（FG_PCT、EFG_PCT 等）不受 per_mode 影響，
    差異主要體現在 FGM/FGA 的頻率（Totals vs PerGame 等）。
    """
    rows = []
    for mode, mode_lbl in [
        ("PerGame",          "場均"),
        ("Per36",            "Per36"),
        ("Per100Possessions","Per100Poss"),
    ]:
        try:
            resp = fetch_dashboard(
                player_id=player_id,
                season=season,
                season_type=season_type,
                per_mode=mode,
            )
        except Exception as e:
            print(f"  {mode_lbl} 取得失敗: {e}")
            continue

        df = _clean(resp.overall_player_dashboard.get_data_frame())
        if df.empty:
            continue

        row   = df.iloc[0]
        entry = {"模式": mode_lbl}
        for col in ["FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "EFG_PCT"]:
            if col in df.columns:
                v = row.get(col)
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return
    result = pd.DataFrame(rows).set_index("模式")
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"
PER_MODE = PerModeDetailed.per_game   # "PerGame"

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

    show_overall(reg_resp,            label="常規賽 ")
    show_shot_type_summary(reg_resp,  label="常規賽 ")
    show_shot_type(reg_resp,          label="常規賽 ")
    show_shot_area(reg_resp,          label="常規賽 ")
    show_distance_splits(reg_resp,    label="常規賽 ")
    show_assisted_shot(reg_resp,      label="常規賽 ")
    show_assisted_by(reg_resp,        label="常規賽 ")

    print("=" * 65)
    print("  常規賽 PerGame / Per36 / Per100Possessions 對比")
    print("=" * 65 + "\n")
    show_per_mode_comparison(player_id, season, SEASON_TYPE_REGULAR)

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
            show_overall(po_resp,            label="季後賽 ")
            show_shot_type_summary(po_resp,  label="季後賽 ")
            show_shot_type(po_resp,          label="季後賽 ")
            show_shot_area(po_resp,          label="季後賽 ")
            show_distance_splits(po_resp,    label="季後賽 ")
            show_assisted_shot(po_resp,      label="季後賽 ")
            show_assisted_by(po_resp,        label="季後賽 ")

            print("=" * 65)
            print("  季後賽各輪次投籃分析（po_round_nullable）")
            print("=" * 65 + "\n")
            show_playoff_by_round(player_id, season, PER_MODE)

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 常規賽 vs 季後賽並排對比 ─────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽整體投籃對比")
        print("=" * 65 + "\n")
        show_regular_vs_playoffs_comparison(reg_resp, po_resp)
