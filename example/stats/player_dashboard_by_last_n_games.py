"""
nba_api.stats.endpoints.playerdashboardbylastngames 範例程式

執行方式：
  python example/stats/player_dashboard_by_last_n_games.py                   # 預設球員，本季
  python example/stats/player_dashboard_by_last_n_games.py "Stephen Curry"   # 依姓名查詢
  python example/stats/player_dashboard_by_last_n_games.py 2544              # 依 player_id
  python example/stats/player_dashboard_by_last_n_games.py 2544 2023-24      # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashboardByLastNGames 聚焦「近期狀態」：
  一次請求取得全季 / 近5 / 近10 / 近15 / 近20 場的聚合數據，
  以及以 5 場為一組的整季進度表，方便追蹤球員狀態趨勢。

與其他 Dashboard 的差異：
  - Last5/10/15/20 DataSet 每個只有「一列」聚合數據（無 GROUP_VALUE 細分）
  - GameNumberPlayerDashboard 以 5 場一組顯示整季進度
    （GROUP_VALUE：Game 1-5 / Game 6-10 / Game 11-15 …）
  - last_n_games 參數可縮短 GameNumber 的涵蓋範圍

DataSet 總覽（共 6 個）：
  overall_player_dashboard          — 整體（全季）
  last5_player_dashboard            — 最近 5 場
  last10_player_dashboard           — 最近 10 場
  last15_player_dashboard           — 最近 15 場
  last20_player_dashboard           — 最近 20 場
  game_number_player_dashboard      — 整季 5 場一組進度表

共用欄位（同 GeneralSplits / GameSplits）：
  GROUP_VALUE / GP / W / L / W_PCT / MIN
  FGM/FGA/FG_PCT / FG3M/FG3A/FG3_PCT / FTM/FTA/FT_PCT
  OREB/DREB/REB / AST/TOV/STL/BLK/BLKA/PF/PFD
  PTS / PLUS_MINUS / NBA_FANTASY_PTS / DD2 / TD3 / *_RANK
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashboardbylastngames import (
    PlayerDashboardByLastNGames,
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
    "FG_PCT", "FG3_PCT", "FT_PCT",
    "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3",
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
) -> PlayerDashboardByLastNGames:
    """取得 LastNGames Dashboard，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashboardByLastNGames(
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
    """取 DataFrame 第一列（Last5/10/15/20 及 Overall 都只有一列）。"""
    if df.empty:
        return None
    return _clean(df).iloc[0]


# ---------------------------------------------------------------------------
# 1. 整體（Overall）含全聯盟排名
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashboardByLastNGames, label: str) -> None:
    df = _clean(resp.overall_player_dashboard.get_data_frame())
    if df.empty:
        print(f"── {label}整體數據：無資料\n")
        return
    avail = ["GROUP_VALUE"] + [c for c in STAT_COLS if c in df.columns]
    r_avail = ["GROUP_VALUE"] + [c for c in RANK_COLS if c in df.columns]
    print(f"── {label}整體數據（全季）" + "─" * 42)
    print(df[avail].to_string(index=False))
    if len(r_avail) > 1:
        print(f"\n  全聯盟排名（數字越小 = 排名越前）：")
        print(df[r_avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 近期狀態走勢：全季 / Last5 / Last10 / Last15 / Last20 並排
# ---------------------------------------------------------------------------

def show_recent_trend(resp: PlayerDashboardByLastNGames, label: str) -> None:
    """
    一次呈現所有近期時段，讓近期狀態與全季平均的差異一目了然。
    每列為一個時段，每欄為一項統計。
    """
    sources = [
        ("全季",    resp.overall_player_dashboard),
        ("近 20 場", resp.last20_player_dashboard),
        ("近 15 場", resp.last15_player_dashboard),
        ("近 10 場", resp.last10_player_dashboard),
        ("近 5 場",  resp.last5_player_dashboard),
    ]

    rows = []
    for seg_label, dataset in sources:
        row_data = _extract_row(dataset.get_data_frame())
        if row_data is None:
            continue
        entry = {"時段": seg_label}
        for col in STAT_COLS:
            if col in row_data.index:
                v = row_data[col]
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        print(f"── {label}近期走勢：無資料\n")
        return

    result = pd.DataFrame(rows).set_index("時段")
    avail  = [c for c in STAT_COLS if c in result.columns]
    print(f"── {label}近期狀態走勢（全季 vs Last20/15/10/5）" + "─" * 16)
    print(result[avail].to_string())
    print()

    # 趨勢分析：近5場 vs 全季的差值
    if len(rows) >= 2:
        season_row = rows[0]
        last5_row  = rows[-1]
        diffs = {}
        for col in ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FG_PCT", "FG3_PCT", "PLUS_MINUS"]:
            if col in season_row and col in last5_row:
                sv = season_row.get(col)
                lv = last5_row.get(col)
                if sv is not None and lv is not None:
                    diffs[col] = round(lv - sv, 3)

        if diffs:
            print(f"  近5場 vs 全季差值（+ = 近況優於全季均值）：")
            for col, diff in diffs.items():
                arrow = "↑" if diff > 0 else ("↓" if diff < 0 else "─")
                print(f"    {col:<12}: {diff:+.3f}  {arrow}")
            print()


# ---------------------------------------------------------------------------
# 3. 整季 5 場一組進度表（GameNumber）
# ---------------------------------------------------------------------------

def show_game_number_progression(
    resp: PlayerDashboardByLastNGames, label: str
) -> None:
    """
    GameNumberPlayerDashboard：以每 5 場為一組顯示整季進度，
    可觀察球員在整季中的狀態變化曲線。
    GROUP_VALUE：Game 1-5 / Game 6-10 / Game 11-15 / …
    """
    df = _clean(resp.game_number_player_dashboard.get_data_frame())
    if df.empty:
        print(f"── {label}整季 5 場一組進度：無資料\n")
        return

    cols = ["GROUP_VALUE", "GP", "W_PCT", "PTS", "REB", "AST",
            "FG_PCT", "FG3_PCT", "PLUS_MINUS", "NBA_FANTASY_PTS"]
    avail = [c for c in cols if c in df.columns]

    print(f"── {label}整季 5 場一組進度（GameNumber）" + "─" * 24)
    print(df[avail].to_string(index=False))
    print()

    # 找出最佳與最差 5 場組
    if "PTS" in df.columns and len(df) > 1:
        best_idx  = df["PTS"].idxmax()
        worst_idx = df["PTS"].idxmin()
        best_grp  = df.loc[best_idx, "GROUP_VALUE"]
        worst_grp = df.loc[worst_idx, "GROUP_VALUE"]
        print(f"  得分最佳 5 場組 : {best_grp}  "
              f"（{df.loc[best_idx,'PTS']:.1f} pts, "
              f"FG {df.loc[best_idx,'FG_PCT']:.3f}）")
        print(f"  得分最差 5 場組 : {worst_grp}  "
              f"（{df.loc[worst_idx,'PTS']:.1f} pts, "
              f"FG {df.loc[worst_idx,'FG_PCT']:.3f}）")
        print()


# ---------------------------------------------------------------------------
# 4. 近期各時段全聯盟排名走勢
# ---------------------------------------------------------------------------

def show_rank_trend(resp: PlayerDashboardByLastNGames, label: str) -> None:
    """
    比較全季 / 近10場 / 近5場的全聯盟排名，
    確認球員近況是否在聯盟中整體上升或下滑。
    """
    sources = [
        ("全季",    resp.overall_player_dashboard),
        ("近 10 場", resp.last10_player_dashboard),
        ("近 5 場",  resp.last5_player_dashboard),
    ]

    rows = []
    for seg_label, dataset in sources:
        row_data = _extract_row(dataset.get_data_frame())
        if row_data is None:
            continue
        entry = {"時段": seg_label}
        for col in RANK_COLS:
            if col in row_data.index:
                v = row_data[col]
                entry[col.replace("_RANK", "")] = int(v) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("時段")
    print(f"── {label}全聯盟排名走勢（數字越小 = 排名越前）" + "─" * 18)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 5. 季後賽各輪次近期狀態（po_round_nullable）
# ---------------------------------------------------------------------------

def show_playoff_by_round(player_id: int, season: str, per_mode: str) -> None:
    """
    對季後賽而言，各輪次場次較少（最多 7 場），
    LastN DataSet 的意義在於：
      - Last5 ≈ 本輪最近 5 場（可能跨系列賽）
      - GameNumber ≈ 各輪次進度
    利用 po_round 逐輪抓取 Overall，比較各輪整體表現。
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

        row_data = _extract_row(resp.overall_player_dashboard.get_data_frame())
        if row_data is None:
            continue

        entry = {"輪次": round_lbl}
        for col in ["GP", "W", "L", "W_PCT", "PTS", "REB", "AST",
                    "STL", "BLK", "TOV", "FG_PCT", "FG3_PCT",
                    "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3"]:
            if col in row_data.index:
                v = row_data[col]
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        print("  無各輪次資料\n")
        return

    result = pd.DataFrame(rows).set_index("輪次")
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 6. 常規賽 vs 季後賽整體並排對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashboardByLastNGames,
    po_resp: PlayerDashboardByLastNGames,
) -> None:
    reg_row = _extract_row(reg_resp.overall_player_dashboard.get_data_frame())
    po_row  = _extract_row(po_resp.overall_player_dashboard.get_data_frame())

    if reg_row is None or po_row is None:
        return

    avail = [c for c in STAT_COLS if c in reg_row.index and c in po_row.index]
    cmp = pd.DataFrame({
        "常規賽": {c: reg_row[c] for c in avail},
        "季後賽": {c: po_row[c] for c in avail},
    }).round(3)
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽 Overall 對比 " + "─" * 30)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 7. 常規賽 vs 季後賽近期走勢並排
# ---------------------------------------------------------------------------

def show_trend_comparison(
    reg_resp: PlayerDashboardByLastNGames,
    po_resp: PlayerDashboardByLastNGames,
) -> None:
    """
    常規賽與季後賽各時段（全季/Last10/Last5）場均並排，
    觀察球員在兩種賽事中的近期狀態差異。
    """
    sources = [
        ("全季",    reg_resp.overall_player_dashboard,  po_resp.overall_player_dashboard),
        ("近 10 場", reg_resp.last10_player_dashboard,   po_resp.last10_player_dashboard),
        ("近 5 場",  reg_resp.last5_player_dashboard,    po_resp.last5_player_dashboard),
    ]

    key_cols = ["PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "PLUS_MINUS", "NBA_FANTASY_PTS"]
    rows = []
    for seg_label, reg_ds, po_ds in sources:
        reg_row = _extract_row(reg_ds.get_data_frame())
        po_row  = _extract_row(po_ds.get_data_frame())
        entry   = {"時段": seg_label}
        for col in key_cols:
            r_v = reg_row[col] if reg_row is not None and col in reg_row.index else None
            p_v = po_row[col]  if po_row  is not None and col in po_row.index  else None
            entry[f"常規_{col}"] = round(float(r_v), 2) if pd.notna(r_v) else None
            entry[f"季後_{col}"] = round(float(p_v), 2) if pd.notna(p_v) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("時段")
    print("── 常規賽 vs 季後賽近期走勢並排 " + "─" * 30)
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

    show_overall(reg_resp,                  label="常規賽 ")
    show_recent_trend(reg_resp,             label="常規賽 ")
    show_game_number_progression(reg_resp,  label="常規賽 ")
    show_rank_trend(reg_resp,               label="常規賽 ")

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
            show_overall(po_resp,                 label="季後賽 ")
            show_recent_trend(po_resp,            label="季後賽 ")
            show_game_number_progression(po_resp, label="季後賽 ")
            show_rank_trend(po_resp,              label="季後賽 ")

            print("=" * 65)
            print("  季後賽各輪次 Overall 分析（po_round_nullable）")
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
        show_trend_comparison(reg_resp, po_resp)
