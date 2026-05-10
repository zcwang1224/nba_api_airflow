"""
nba_api.stats.endpoints.playerdashboardbyclutch 範例程式

執行方式：
  python example/stats/player_dashboard_by_clutch.py                   # 預設球員，本季
  python example/stats/player_dashboard_by_clutch.py "Stephen Curry"   # 依姓名查詢
  python example/stats/player_dashboard_by_clutch.py 2544              # 依 player_id
  python example/stats/player_dashboard_by_clutch.py 2544 2023-24      # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashboardByClutch 聚焦「關鍵時刻」表現：
  每個 DataSet 定義不同的時間視窗 × 比分差距條件，
  讓你看見球員在越來越緊張的情境下，數據如何變化。

DataSet 定義說明：
  ┌──────────────────────────────────────────────────────────────────┐
  │  DataSet 名稱                  時間視窗    分差條件               │
  ├──────────────────────────────────────────────────────────────────┤
  │  OverallPlayerDashboard        整體（全場比較基準）               │
  │  Last5Min5PointPlayerDashboard     最後 5 分鐘  差距 ≤ 5 分      │
  │  Last5MinPlusMinus5PointPlayerDashboard 最後 5 分鐘 ±5 分以內    │
  │  Last3Min5PointPlayerDashboard     最後 3 分鐘  差距 ≤ 5 分      │
  │  Last3MinPlusMinus5PointPlayerDashboard 最後 3 分鐘 ±5 分以內    │
  │  Last1Min5PointPlayerDashboard     最後 1 分鐘  差距 ≤ 5 分      │
  │  Last1MinPlusMinus5PointPlayerDashboard 最後 1 分鐘 ±5 分以內    │
  │  Last30Sec3PointPlayerDashboard    最後 30 秒   差距 ≤ 3 分      │
  │  Last30Sec3Point2PlayerDashboard   最後 30 秒   差距 ≤ 2 分      │
  │  Last10Sec3PointPlayerDashboard    最後 10 秒   差距 ≤ 3 分      │
  │  Last10Sec3Point2PlayerDashboard   最後 10 秒   差距 ≤ 2 分      │
  └──────────────────────────────────────────────────────────────────┘

共用欄位（同 GeneralSplits）：
  GROUP_VALUE / GP / W / L / W_PCT / MIN
  FGM/FGA/FG_PCT / FG3M/FG3A/FG3_PCT / FTM/FTA/FT_PCT
  OREB/DREB/REB / AST/TOV/STL/BLK/BLKA/PF/PFD
  PTS / PLUS_MINUS / NBA_FANTASY_PTS / DD2 / TD3 / *_RANK

參數（同 GeneralSplits）：
  player_id / season / season_type_playoffs / per_mode_detailed /
  measure_type_detailed / last_n_games / po_round_nullable 等
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashboardbyclutch import PlayerDashboardByClutch
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
    "FG_PCT", "FG3_PCT", "FT_PCT",
    "PLUS_MINUS", "NBA_FANTASY_PTS",
]
RANK_COLS = [
    "PTS_RANK", "REB_RANK", "AST_RANK", "STL_RANK",
    "FG_PCT_RANK", "FG3_PCT_RANK", "PLUS_MINUS_RANK",
]
PO_ROUND_LABEL = {
    "1": "第一輪",
    "2": "第二輪",
    "3": "分區決賽",
    "4": "總冠軍賽",
}

# 所有 Clutch DataSet 的定義順序（從最寬鬆到最嚴格）
CLUTCH_DATASETS = [
    ("整體",                  "overall_player_dashboard"),
    ("最後5分鐘 ≤5分差",      "last5_min5_point_player_dashboard"),
    ("最後5分鐘 ±5分",        "last5_min_plus_minus5_point_player_dashboard"),
    ("最後3分鐘 ≤5分差",      "last3_min5_point_player_dashboard"),
    ("最後3分鐘 ±5分",        "last3_min_plus_minus5_point_player_dashboard"),
    ("最後1分鐘 ≤5分差",      "last1_min5_point_player_dashboard"),
    ("最後1分鐘 ±5分",        "last1_min_plus_minus5_point_player_dashboard"),
    ("最後30秒  ≤3分差",      "last30_sec3_point_player_dashboard"),
    ("最後30秒  ≤2分差",      "last30_sec3_point2_player_dashboard"),
    ("最後10秒  ≤3分差",      "last10_sec3_point_player_dashboard"),
    ("最後10秒  ≤2分差",      "last10_sec3_point2_player_dashboard"),
]


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
) -> PlayerDashboardByClutch:
    """取得 Clutch Dashboard，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashboardByClutch(
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


def _extract_row(df: pd.DataFrame) -> pd.Series | None:
    if df.empty:
        return None
    return _clean(df).iloc[0]


# ---------------------------------------------------------------------------
# 1. 整體（Overall）含全聯盟排名
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashboardByClutch, label: str) -> None:
    df = _clean(resp.overall_player_dashboard.get_data_frame())
    if df.empty:
        print(f"── {label}整體數據：無資料\n")
        return
    avail = ["GROUP_VALUE"] + [c for c in STAT_COLS if c in df.columns]
    r_avail = ["GROUP_VALUE"] + [c for c in RANK_COLS if c in df.columns]
    print(f"── {label}整體數據 " + "─" * 50)
    print(df[avail].to_string(index=False))
    if len(r_avail) > 1:
        print(f"\n  全聯盟排名（數字越小 = 排名越前）：")
        print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 核心：關鍵時刻升溫曲線（所有 DataSet 並排）
# ---------------------------------------------------------------------------

def show_clutch_heatmap(resp: PlayerDashboardByClutch, label: str) -> None:
    """
    將所有 11 個 Clutch DataSet 的 key stats 並排，
    呈現「時間越短、分差越小 → 數據如何變化」的升溫曲線。
    這是本 endpoint 最有價值的分析視角。
    """
    key_cols = ["GP", "W_PCT", "PTS", "FG_PCT", "FG3_PCT", "FT_PCT",
                "AST", "TOV", "PLUS_MINUS"]

    rows = []
    for clutch_label, attr_name in CLUTCH_DATASETS:
        dataset = getattr(resp, attr_name, None)
        if dataset is None:
            continue
        row_data = _extract_row(dataset.get_data_frame())
        if row_data is None:
            continue

        entry = {"情境": clutch_label}
        for col in key_cols:
            if col in row_data.index:
                v = row_data[col]
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        print(f"── {label}關鍵時刻升溫曲線：無資料\n")
        return

    result = pd.DataFrame(rows).set_index("情境")
    avail  = [c for c in key_cols if c in result.columns]

    print(f"── {label}關鍵時刻升溫曲線（整體 → 最後10秒 ≤2分差）" + "─" * 14)
    print("  ※ 由上至下：定義越嚴格（時間越短 + 分差越小）")
    print()
    print(result[avail].to_string())
    print()


# ---------------------------------------------------------------------------
# 3. 全聯盟排名在各關鍵時刻的變化
# ---------------------------------------------------------------------------

def show_clutch_rank_trend(resp: PlayerDashboardByClutch, label: str) -> None:
    """
    各關鍵時刻定義下的全聯盟排名，
    排名越小代表球員在該情境下越突出。
    """
    rank_key = ["PTS_RANK", "FG_PCT_RANK", "PLUS_MINUS_RANK", "W_PCT_RANK"]

    rows = []
    for clutch_label, attr_name in CLUTCH_DATASETS:
        dataset = getattr(resp, attr_name, None)
        if dataset is None:
            continue
        row_data = _extract_row(dataset.get_data_frame())
        if row_data is None:
            continue

        entry = {"情境": clutch_label}
        for col in rank_key:
            if col in row_data.index:
                v = row_data[col]
                entry[col.replace("_RANK", "")] = int(v) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("情境")
    print(f"── {label}關鍵時刻全聯盟排名走勢（數字越小 = 全聯盟越前）" + "─" * 10)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 勝率分析：關鍵時刻的球隊勝率
# ---------------------------------------------------------------------------

def show_clutch_win_rate(resp: PlayerDashboardByClutch, label: str) -> None:
    """
    各關鍵時刻定義下的球隊勝率，
    高勝率表示球員在此情境下常帶領球隊勝出。
    """
    rows = []
    for clutch_label, attr_name in CLUTCH_DATASETS:
        dataset = getattr(resp, attr_name, None)
        if dataset is None:
            continue
        row_data = _extract_row(dataset.get_data_frame())
        if row_data is None:
            continue

        gp  = row_data.get("GP")
        w   = row_data.get("W")
        wpt = row_data.get("W_PCT")
        if pd.isna(gp) or gp == 0:
            continue

        entry = {
            "情境": clutch_label,
            "GP":   int(gp) if pd.notna(gp) else 0,
            "W":    int(w)  if pd.notna(w)  else 0,
            "L":    int(gp - w) if pd.notna(gp) and pd.notna(w) else 0,
            "W_PCT": round(float(wpt), 3) if pd.notna(wpt) else None,
        }
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("情境")
    print(f"── {label}關鍵時刻球隊勝率" + "─" * 44)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 5. 三種核心關鍵時刻完整數據
# ---------------------------------------------------------------------------

CORE_CLUTCH = [
    ("最後5分鐘 ≤5分差", "last5_min5_point_player_dashboard"),
    ("最後1分鐘 ≤5分差", "last1_min5_point_player_dashboard"),
    ("最後30秒  ≤3分差", "last30_sec3_point_player_dashboard"),
]


def show_core_clutch_detail(resp: PlayerDashboardByClutch, label: str) -> None:
    """
    展示三個最常被引用的關鍵時刻定義的完整數據，
    含所有 stat 欄位與全聯盟排名。
    """
    for clutch_label, attr_name in CORE_CLUTCH:
        dataset = getattr(resp, attr_name, None)
        if dataset is None:
            continue
        df = _clean(dataset.get_data_frame())
        if df.empty:
            print(f"── {label}{clutch_label}：無出賽記錄\n")
            continue

        avail = ["GROUP_VALUE"] + [c for c in STAT_COLS if c in df.columns]
        r_avail = ["GROUP_VALUE"] + [c for c in RANK_COLS if c in df.columns]

        print(f"── {label}{clutch_label} " + "─" * max(0, 52 - len(clutch_label)))
        print(df[avail].to_string(index=False))
        if len(r_avail) > 1:
            print(f"\n  排名：")
            print(df[r_avail].to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 6. 常規賽 vs 季後賽關鍵時刻升溫曲線對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_clutch(
    reg_resp: PlayerDashboardByClutch,
    po_resp: PlayerDashboardByClutch,
) -> None:
    """
    並排對比常規賽與季後賽在各關鍵時刻定義下的 PTS / FG_PCT / W_PCT / PLUS_MINUS。
    季後賽的關鍵時刻壓力通常更高，數據變化更明顯。
    """
    compare_cols = ["GP", "W_PCT", "PTS", "FG_PCT", "PLUS_MINUS"]

    rows = []
    for clutch_label, attr_name in CLUTCH_DATASETS:
        reg_ds = getattr(reg_resp, attr_name, None)
        po_ds  = getattr(po_resp,  attr_name, None)
        if reg_ds is None or po_ds is None:
            continue

        reg_row = _extract_row(reg_ds.get_data_frame())
        po_row  = _extract_row(po_ds.get_data_frame())
        if reg_row is None and po_row is None:
            continue

        entry = {"情境": clutch_label}
        for col in compare_cols:
            r_v = reg_row[col] if reg_row is not None and col in reg_row.index else None
            p_v = po_row[col]  if po_row  is not None and col in po_row.index  else None
            entry[f"常規_{col}"] = round(float(r_v), 3) if pd.notna(r_v) else None
            entry[f"季後_{col}"] = round(float(p_v), 3) if pd.notna(p_v) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("情境")
    print("── 常規賽 vs 季後賽關鍵時刻對比 " + "─" * 30)
    print("  ※ 各欄前綴：常規_ = 常規賽，季後_ = 季後賽")
    print()
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 7. 季後賽各輪次關鍵時刻（po_round_nullable）
# ---------------------------------------------------------------------------

def show_playoff_by_round(player_id: int, season: str, per_mode: str) -> None:
    """
    逐輪抓取 Last5Min5Point（最常用的關鍵時刻定義），
    比較各輪次在壓力下的表現。
    """
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

        entry = {"輪次": round_lbl}
        for clutch_label, attr_name in [
            ("整體",           "overall_player_dashboard"),
            ("最後5分鐘≤5分", "last5_min5_point_player_dashboard"),
            ("最後1分鐘≤5分", "last1_min5_point_player_dashboard"),
            ("最後30秒≤3分",  "last30_sec3_point_player_dashboard"),
        ]:
            dataset  = getattr(resp, attr_name, None)
            row_data = _extract_row(dataset.get_data_frame()) if dataset else None
            if row_data is None:
                entry[f"{clutch_label}_PTS"]    = None
                entry[f"{clutch_label}_FG_PCT"] = None
                entry[f"{clutch_label}_W_PCT"]  = None
                continue
            entry[f"{clutch_label}_PTS"]    = round(float(row_data["PTS"]),    2) if pd.notna(row_data.get("PTS"))    else None
            entry[f"{clutch_label}_FG_PCT"] = round(float(row_data["FG_PCT"]), 3) if pd.notna(row_data.get("FG_PCT")) else None
            entry[f"{clutch_label}_W_PCT"]  = round(float(row_data["W_PCT"]),  3) if pd.notna(row_data.get("W_PCT"))  else None
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

    show_overall(reg_resp,              label="常規賽 ")
    show_clutch_heatmap(reg_resp,       label="常規賽 ")
    show_clutch_rank_trend(reg_resp,    label="常規賽 ")
    show_clutch_win_rate(reg_resp,      label="常規賽 ")
    show_core_clutch_detail(reg_resp,   label="常規賽 ")

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
            show_clutch_heatmap(po_resp,     label="季後賽 ")
            show_clutch_rank_trend(po_resp,  label="季後賽 ")
            show_clutch_win_rate(po_resp,    label="季後賽 ")
            show_core_clutch_detail(po_resp, label="季後賽 ")

            print("=" * 65)
            print("  季後賽各輪次關鍵時刻分析（po_round_nullable）")
            print("=" * 65 + "\n")
            show_playoff_by_round(player_id, season, PER_MODE)

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 並排對比 ──────────────────────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽關鍵時刻對比")
        print("=" * 65 + "\n")
        show_regular_vs_playoffs_clutch(reg_resp, po_resp)
