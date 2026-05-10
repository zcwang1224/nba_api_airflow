"""
nba_api.stats.endpoints.playerdashboardbygeneralsplits 範例程式

執行方式：
  python example/stats/player_dashboard_by_general_splits.py                    # 預設球員，本季
  python example/stats/player_dashboard_by_general_splits.py "Stephen Curry"    # 依姓名查詢
  python example/stats/player_dashboard_by_general_splits.py 2544               # 依 player_id
  python example/stats/player_dashboard_by_general_splits.py 2544 2023-24       # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽（各自完整 7 個 DataSet），最後並排對比 ──

與 PlayerGameLog / PlayerGameLogs 的差異：
  - 回傳「聚合數據」（各維度加總/場均），而非逐場記錄
  - 一次請求即可取得 7 種維度的分組統計，不需自行 groupby

PlayerDashboardByGeneralSplits 參數：
  player_id              — 球員 ID（必填）
  season                 — 球季（預設當季）
  season_type_playoffs   — "Regular Season"（預設）/ "Playoffs" /
                           "Pre Season" / "PlayIn"
  measure_type_detailed  — "Base"（預設）/ "Advanced" / "Misc" /
                           "Scoring" / "Usage" / "Opponent" / "Four Factors"
  per_mode_detailed      — "Totals"（預設）/ "PerGame" / "Per36" /
                           "Per100Possessions" / "PerPossession" / "PerPlay"
  last_n_games           — 最近 N 場（0=全部）
  month                  — 月份（0=全部，1-12=指定月）
  opponent_team_id       — 對手球隊 ID（0=全部）
  period                 — 節次（0=全場，1-4=指定節）
  pace_adjust            — "Y" / "N"（預設 "N"）
  plus_minus             — "Y" / "N"（預設 "N"）
  rank                   — "Y" / "N"（預設 "N"）
  po_round_nullable      — 季後賽輪次（"1"/"2"/"3"/"4"，留空=全部）
  date_from_nullable     — 起始日期 "YYYY-MM-DD"
  date_to_nullable       — 結束日期 "YYYY-MM-DD"
  location_nullable      — "Home" / "Road"
  outcome_nullable       — "W" / "L"
  season_segment_nullable — "Pre All-Star" / "Post All-Star"
  vs_conference_nullable — "East" / "West"
  vs_division_nullable   — 各分區名稱

DataSet 總覽（共 7 個，每個欄位結構相同）：
  overall_player_dashboard           — 整體（Overall）
  location_player_dashboard          — 主客場（Home / Road）
  month_player_dashboard             — 逐月（October ~ June）
  wins_losses_player_dashboard       — 勝敗場（Win / Loss）
  days_rest_player_dashboard         — 休息天數（0 / 1 / 2 / 3+ Days Rest）
  pre_post_all_star_player_dashboard — 全明星賽前後（Pre/Post All-Star）
  starting_position                  — 先發 vs 替補（Starter / Reserve）

共用欄位（每個 DataSet 相同）：
  GROUP_SET     — 維度標籤（如 "Location", "Month"）
  GROUP_VALUE   — 分組值（如 "Home", "Road", "October"）
  GP / W / L / W_PCT        — 出賽 / 勝 / 敗 / 勝率
  MIN           — 上場時間
  FGM/FGA/FG_PCT / FG3M/FG3A/FG3_PCT / FTM/FTA/FT_PCT
  OREB/DREB/REB / AST/TOV/STL/BLK/BLKA/PF/PFD
  PTS / PLUS_MINUS / NBA_FANTASY_PTS / DD2 / TD3
  *_RANK        — 各項目全聯盟排名
  CFID / CFPARAMS — NBA.com 條件過濾 ID（可忽略）
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashboardbygeneralsplits import (
    PlayerDashboardByGeneralSplits,
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

SEASON_TYPE_REGULAR  = SeasonType.regular      # "Regular Season"
SEASON_TYPE_PLAYOFFS = "Playoffs"              # SeasonType 不含此值，直接傳字串

STAT_COLS = [
    "GP", "W", "L", "W_PCT", "MIN",
    "PTS", "REB", "AST", "STL", "BLK", "BLKA", "PFD", "TOV",
    "FG_PCT", "FG3_PCT", "FT_PCT", "PLUS_MINUS", "NBA_FANTASY_PTS",
    "DD2", "TD3",
]
RANK_COLS = [
    "PTS_RANK", "REB_RANK", "AST_RANK", "STL_RANK", "BLK_RANK",
    "FG_PCT_RANK", "FG3_PCT_RANK", "PLUS_MINUS_RANK",
]
SKIP_COLS = {"CFID", "CFPARAMS", "GROUP_SET"}

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


def fetch_dashboard(
    player_id: int,
    season: str = Season.default,
    season_type: str = SEASON_TYPE_REGULAR,
    measure_type: str = MeasureTypeDetailed.default,   # "Base"
    per_mode: str = PerModeDetailed.per_game,          # "PerGame"（場均最直觀）
    last_n_games: int = 0,
    month: int = 0,
    opponent_team_id: int = 0,
    period: int = 0,
    pace_adjust: str = "N",
    plus_minus: str = "N",
    rank: str = "N",
    date_from: str = "",
    date_to: str = "",
    location: str = "",
    outcome: str = "",
    season_segment: str = "",
    vs_conference: str = "",
    po_round: str = "",
) -> PlayerDashboardByGeneralSplits:
    """取得 Dashboard response 物件，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashboardByGeneralSplits(
                player_id=player_id,
                season=season,
                season_type_playoffs=season_type,
                measure_type_detailed=measure_type,
                per_mode_detailed=per_mode,
                last_n_games=str(last_n_games),
                month=str(month),
                opponent_team_id=opponent_team_id,
                period=str(period),
                pace_adjust=pace_adjust,
                plus_minus=plus_minus,
                rank=rank,
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
    """數值欄位轉型，移除輔助欄位。"""
    if df.empty:
        return df
    num_cols = [c for c in df.columns if c not in SKIP_COLS and c != "GROUP_VALUE"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


def _show_split(
    df: pd.DataFrame,
    title: str,
    key_cols: list[str] | None = None,
    show_rank: bool = False,
) -> None:
    """通用：印出任一 DataSet 的 GROUP_VALUE + 指定欄位。"""
    if df.empty:
        print(f"── {title}：無資料\n")
        return

    df = _clean(df)
    display = key_cols or STAT_COLS
    avail = ["GROUP_VALUE"] + [c for c in display if c in df.columns]

    print(f"── {title} " + "─" * max(0, 56 - len(title)))
    print(df[avail].to_string(index=False))

    if show_rank:
        rank_avail = ["GROUP_VALUE"] + [c for c in RANK_COLS if c in df.columns]
        if len(rank_avail) > 1:
            print(f"\n  排名（數字越小 = 全聯盟越前）：")
            print(df[rank_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 1. 整體（Overall）
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashboardByGeneralSplits,
                 label: str, per_mode: str) -> None:
    df = resp.overall_player_dashboard.get_data_frame()
    mode_lbl = {"PerGame": "場均", "Totals": "累計", "Per36": "Per36",
                "Per100Possessions": "Per100Poss"}.get(per_mode, per_mode)
    _show_split(df, f"{label}整體數據（{mode_lbl}）", show_rank=True)


# ---------------------------------------------------------------------------
# 2. 主客場（Location）
# ---------------------------------------------------------------------------

def show_location_split(resp: PlayerDashboardByGeneralSplits, label: str) -> None:
    df = resp.location_player_dashboard.get_data_frame()
    _show_split(df, f"{label}主客場分析（Home / Road）")


# ---------------------------------------------------------------------------
# 3. 逐月（Month）
# ---------------------------------------------------------------------------

def show_monthly_split(resp: PlayerDashboardByGeneralSplits, label: str) -> None:
    df = resp.month_player_dashboard.get_data_frame()
    if df.empty:
        print(f"── {label}逐月：無資料\n")
        return
    df = _clean(df)
    cols = ["GROUP_VALUE", "GP", "W", "L", "W_PCT",
            "PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "PLUS_MINUS"]
    avail = [c for c in cols if c in df.columns]
    print(f"── {label}逐月數據 " + "─" * 48)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 勝敗場（Wins / Losses）
# ---------------------------------------------------------------------------

def show_win_loss_split(resp: PlayerDashboardByGeneralSplits, label: str) -> None:
    df = resp.wins_losses_player_dashboard.get_data_frame()
    _show_split(df, f"{label}勝場 vs 敗場")


# ---------------------------------------------------------------------------
# 5. 休息天數（Days Rest）
# ---------------------------------------------------------------------------

def show_days_rest_split(resp: PlayerDashboardByGeneralSplits, label: str) -> None:
    df = resp.days_rest_player_dashboard.get_data_frame()
    if df.empty:
        print(f"── {label}休息天數：無資料\n")
        return
    df = _clean(df)
    cols = ["GROUP_VALUE", "GP", "W_PCT", "MIN",
            "PTS", "REB", "AST", "FG_PCT", "PLUS_MINUS"]
    avail = [c for c in cols if c in df.columns]
    print(f"── {label}休息天數分析 " + "─" * 46)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 全明星賽前後（Pre / Post All-Star）
# ---------------------------------------------------------------------------

def show_pre_post_allstar(resp: PlayerDashboardByGeneralSplits, label: str) -> None:
    df = resp.pre_post_all_star_player_dashboard.get_data_frame()
    _show_split(df, f"{label}全明星賽前 vs 後")


# ---------------------------------------------------------------------------
# 7. 先發 vs 替補（Starting Position）
# ---------------------------------------------------------------------------

def show_starting_position(resp: PlayerDashboardByGeneralSplits, label: str) -> None:
    df = resp.starting_position.get_data_frame()
    _show_split(df, f"{label}先發 vs 替補（Starter / Reserve）")


# ---------------------------------------------------------------------------
# 8. 多種 MeasureType 並排（僅 Overall 行）
# ---------------------------------------------------------------------------

MEASURE_DISPLAY = {
    "Base":         ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FG_PCT", "PLUS_MINUS"],
    "Advanced":     ["MIN", "PTS", "REB", "AST", "PLUS_MINUS"],
    "Scoring":      ["PTS", "FG_PCT", "FG3_PCT", "FT_PCT"],
    "Usage":        ["PTS", "REB", "AST", "TOV"],
    "Opponent":     ["PTS", "REB", "AST", "FG_PCT"],
}


def show_multi_measure(
    player_id: int,
    season: str,
    season_type: str,
    per_mode: str,
) -> None:
    """跑 Base / Advanced / Scoring / Usage 四種 MeasureType，比較 Overall 行。"""
    rows = []
    for measure in ["Base", "Advanced", "Scoring", "Usage"]:
        try:
            resp = fetch_dashboard(
                player_id=player_id,
                season=season,
                season_type=season_type,
                measure_type=measure,
                per_mode=per_mode,
            )
        except Exception as e:
            print(f"  [{measure}] 取得失敗: {e}")
            continue

        df = resp.overall_player_dashboard.get_data_frame()
        if df.empty:
            continue
        df = _clean(df)
        row = df.iloc[0]
        entry = {"MeasureType": measure}
        for col in MEASURE_DISPLAY.get(measure, STAT_COLS[:8]):
            if col in df.columns:
                v = row.get(col)
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        print("  無資料\n")
        return

    result = pd.DataFrame(rows).set_index("MeasureType")
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 9. 常規賽 vs 季後賽 Overall 並排對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashboardByGeneralSplits,
    po_resp: PlayerDashboardByGeneralSplits,
) -> None:
    """常規賽與季後賽 Overall 數據並排，計算差值。"""
    reg_df = _clean(reg_resp.overall_player_dashboard.get_data_frame())
    po_df  = _clean(po_resp.overall_player_dashboard.get_data_frame())

    if reg_df.empty or po_df.empty:
        return

    stat_cols = ["GP", "W_PCT", "MIN", "PTS", "REB", "AST",
                 "STL", "BLK", "TOV", "FG_PCT", "FG3_PCT", "FT_PCT",
                 "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3"]
    avail = [c for c in stat_cols if c in reg_df.columns and c in po_df.columns]

    reg_row = reg_df.iloc[0]
    po_row  = po_df.iloc[0]

    cmp = pd.DataFrame({
        "常規賽": {c: reg_row[c] for c in avail},
        "季後賽": {c: po_row[c] for c in avail},
    }).round(3)
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽 Overall 對比（PerGame）" + "─" * 20)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 10. 季後賽各輪次 Overall 分析（po_round_nullable）
# ---------------------------------------------------------------------------

def show_playoff_by_round(
    player_id: int,
    season: str,
    per_mode: str,
) -> None:
    """利用 po_round_nullable 逐輪抓取，比較各輪次整體數據。"""
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
                    "STL", "BLK", "FG_PCT", "FG3_PCT", "PLUS_MINUS",
                    "NBA_FANTASY_PTS", "DD2", "TD3"]:
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
PER_MODE = PerModeDetailed.per_game   # "PerGame"（場均最直觀）

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

    show_overall(reg_resp,          label="常規賽 ", per_mode=PER_MODE)
    show_location_split(reg_resp,   label="常規賽 ")
    show_monthly_split(reg_resp,    label="常規賽 ")
    show_win_loss_split(reg_resp,   label="常規賽 ")
    show_days_rest_split(reg_resp,  label="常規賽 ")
    show_pre_post_allstar(reg_resp, label="常規賽 ")
    show_starting_position(reg_resp,label="常規賽 ")

    print("=" * 65)
    print("  常規賽多種 MeasureType 對比（各需一次額外請求）")
    print("=" * 65 + "\n")

    show_multi_measure(player_id, season, SEASON_TYPE_REGULAR, PER_MODE)

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    try:
        po_resp = fetch_dashboard(player_id, season=season,
                                  season_type=SEASON_TYPE_PLAYOFFS,
                                  per_mode=PER_MODE)

        po_overall = po_resp.overall_player_dashboard.get_data_frame()
        if po_overall.empty:
            print(f"  {full_name} 本季無季後賽出賽記錄（或尚未進入季後賽）。\n")
            po_resp = None
        else:
            show_overall(po_resp,          label="季後賽 ", per_mode=PER_MODE)
            show_location_split(po_resp,   label="季後賽 ")
            show_win_loss_split(po_resp,   label="季後賽 ")
            show_days_rest_split(po_resp,  label="季後賽 ")
            show_starting_position(po_resp,label="季後賽 ")

            print("=" * 65)
            print("  季後賽各輪次分析（po_round_nullable）")
            print("=" * 65 + "\n")
            show_playoff_by_round(player_id, season, PER_MODE)

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")
        po_resp = None

    # ── 常規賽 vs 季後賽並排對比 ─────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽 Overall 並排對比")
        print("=" * 65 + "\n")
        show_regular_vs_playoffs_comparison(reg_resp, po_resp)
