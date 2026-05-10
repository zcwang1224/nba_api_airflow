"""
nba_api.stats.endpoints.playerdashptshots 範例程式

執行方式：
  python example/stats/player_dash_pt_shots.py                   # 預設球員，本季
  python example/stats/player_dash_pt_shots.py "Stephen Curry"   # 依姓名查詢
  python example/stats/player_dash_pt_shots.py 2544              # 依 player_id
  python example/stats/player_dash_pt_shots.py 2544 2023-24      # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashPtShots 與其他 Dashboard 的關鍵差異：
  1. 需要額外傳入 team_id（傳 0 = 不限球隊，取整季所有場次）
  2. 使用 PerModeSimple（"Totals" / "PerGame"），非 PerModeDetailed
  3. 使用 SeasonTypeAllStar（支援 "Playoffs" / "All Star"）
  4. 欄位完全不同：以「出手頻率 + 有效命中率」為主軸，無 RANK 欄
  5. 分離呈現 FG2 / FG3（2分球 / 3分球各自的頻率與命中率）

DataSet 總覽（共 7 個）：
  overall                              — 整體（SHOT_TYPE = All）
  general_shooting                     — 一般投籃類型（Catch and Shoot / Pull Up / Less Than 10 ft）
  closest_defender_shooting            — 最近防守者距離（0-2ft 緊逼 / 2-4ft 緊湊 / 4-6ft 開放 / 6+ft 無人防守）
  closest_defender10ft_plus_shooting   — 最近防守者 10 英尺以上的細分帶
  dribble_shooting                     — 出手前運球次數（0 / 1 / 2 / 3-6 / 7+）
  shot_clock_shooting                  — 出手時進攻時鐘剩餘時間
  touch_time_shooting                  — 接球後持球時間（<2秒 / 2-6秒 / 6+秒）

共用欄位（所有 DataSet）：
  PLAYER_ID / PLAYER_NAME_LAST_FIRST / SORT_ORDER / GP / G
  分組欄（各 DataSet 不同）：
    SHOT_TYPE / CLOSE_DEF_DIST_RANGE / DRIBBLE_RANGE /
    SHOT_CLOCK_RANGE / TOUCH_TIME_RANGE
  投籃欄：
    FGA_FREQUENCY  — 該分類占總出手比例（%）
    FGM / FGA / FG_PCT / EFG_PCT（有效命中率）
    FG2A_FREQUENCY / FG2M / FG2A / FG2_PCT  — 2 分球
    FG3A_FREQUENCY / FG3M / FG3A / FG3_PCT  — 3 分球

特別說明：
  team_id = 0  → 不限球隊，涵蓋整季所有場次（包含換隊後的統計）
  team_id = 具體球隊 ID → 只算在特定球隊出賽的場次
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashptshots import PlayerDashPtShots
from nba_api.stats.library.parameters import (
    PerModeSimple,
    Season,
    SeasonTypeAllStar,
)
from nba_api.stats.static import players

TIMEOUT     = 60
RETRIES     = 3
RETRY_DELAY = 5

SEASON_TYPE_REGULAR  = SeasonTypeAllStar.regular
SEASON_TYPE_PLAYOFFS = SeasonTypeAllStar.playoffs

SKIP_COLS = {"PLAYER_ID", "PLAYER_NAME_LAST_FIRST", "SORT_ORDER", "GP", "G"}

# 所有 DataSet 共用的投籃指標欄
SHOT_COLS = [
    "FGA_FREQUENCY",
    "FGM", "FGA", "FG_PCT", "EFG_PCT",
    "FG2A_FREQUENCY", "FG2M", "FG2A", "FG2_PCT",
    "FG3A_FREQUENCY", "FG3M", "FG3A", "FG3_PCT",
]

# 各 DataSet 的分組欄（GROUP_VALUE 的替代）
RANGE_COL = {
    "overall":                             "SHOT_TYPE",
    "general_shooting":                    "SHOT_TYPE",
    "closest_defender_shooting":           "CLOSE_DEF_DIST_RANGE",
    "closest_defender10ft_plus_shooting":  "CLOSE_DEF_DIST_RANGE",
    "dribble_shooting":                    "DRIBBLE_RANGE",
    "shot_clock_shooting":                 "SHOT_CLOCK_RANGE",
    "touch_time_shooting":                 "TOUCH_TIME_RANGE",
}

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
    team_id: int = 0,
    season: str = Season.default,
    season_type: str = SEASON_TYPE_REGULAR,
    per_mode: str = PerModeSimple.per_game,
    last_n_games: int = 0,
    month: int = 0,
    period: int = 0,
    date_from: str = "",
    date_to: str = "",
    location: str = "",
    outcome: str = "",
    season_segment: str = "",
    vs_conference: str = "",
) -> PlayerDashPtShots:
    """
    取得 PtShots Dashboard，失敗時自動重試。
    team_id=0 → 不限球隊，涵蓋整季所有場次。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashPtShots(
                player_id=player_id,
                team_id=team_id,
                season=season,
                season_type_all_star=season_type,
                per_mode_simple=per_mode,
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


def _clean(df: pd.DataFrame, range_col: str) -> pd.DataFrame:
    """數值欄位轉型，保留 range_col 為字串。"""
    if df.empty:
        return df
    num_cols = [c for c in df.columns
                if c not in SKIP_COLS and c != range_col and c != "SORT_ORDER"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    if "SORT_ORDER" in df.columns:
        df["SORT_ORDER"] = pd.to_numeric(df["SORT_ORDER"], errors="coerce")
    return df


def _show_pt_split(
    df: pd.DataFrame,
    title: str,
    range_col: str,
    gp_col: bool = False,
) -> None:
    """通用：印出 PtShots 任一 DataSet。"""
    if df.empty:
        print(f"── {title}：無資料\n")
        return

    df = _clean(df, range_col)
    if "SORT_ORDER" in df.columns:
        df = df.sort_values("SORT_ORDER")

    base = [range_col]
    if gp_col and "G" in df.columns:
        base.append("G")
    avail = base + [c for c in SHOT_COLS if c in df.columns]

    print(f"── {title} " + "─" * max(0, 58 - len(title)))
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 1. 整體（Overall）
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashPtShots, label: str) -> None:
    df = resp.overall.get_data_frame()
    _show_pt_split(df, f"{label}整體投籃（Overall）", range_col="SHOT_TYPE", gp_col=True)


# ---------------------------------------------------------------------------
# 2. 一般投籃類型（GeneralShooting）
# ---------------------------------------------------------------------------

def show_general_shooting(resp: PlayerDashPtShots, label: str) -> None:
    """
    SHOT_TYPE：
      Catch and Shoot      — 接球即出手（無運球）
      Pull Up              — 運球後急停跳投
      Less Than 10 ft      — 禁區 10 英尺內出手
    可分析球員的出手方式分布。
    """
    df = resp.general_shooting.get_data_frame()
    _show_pt_split(df, f"{label}一般投籃類型（GeneralShooting）",
                   range_col="SHOT_TYPE", gp_col=True)


# ---------------------------------------------------------------------------
# 3. 最近防守者距離（ClosestDefenderShooting）
# ---------------------------------------------------------------------------

def show_closest_defender(resp: PlayerDashPtShots, label: str) -> None:
    """
    CLOSE_DEF_DIST_RANGE（防守者距離）：
      0-2 Feet - Very Tight  — 緊貼（≤2英尺）
      2-4 Feet - Tight       — 緊湊（2-4英尺）
      4-6 Feet - Open        — 開放（4-6英尺）
      6+ Feet - Wide Open    — 完全無人（6+英尺）
    EFG_PCT 在各防守距離下的表現，是球員自主創造能力的關鍵指標。
    """
    df = resp.closest_defender_shooting.get_data_frame()
    _show_pt_split(df, f"{label}最近防守者距離（ClosestDefender）",
                   range_col="CLOSE_DEF_DIST_RANGE")


# ---------------------------------------------------------------------------
# 4. 最近防守者 10 英尺以上細分（ClosestDefender10ftPlus）
# ---------------------------------------------------------------------------

def show_closest_defender_10ft(resp: PlayerDashPtShots, label: str) -> None:
    """
    10 英尺以上無人防守時的進一步細分，
    可看出球員在完全空檔下的投籃效率分布。
    """
    df = resp.closest_defender10ft_plus_shooting.get_data_frame()
    _show_pt_split(df, f"{label}防守者 10ft+ 細分（ClosestDefender10ftPlus）",
                   range_col="CLOSE_DEF_DIST_RANGE")


# ---------------------------------------------------------------------------
# 5. 運球次數（DribbleShooting）
# ---------------------------------------------------------------------------

def show_dribble_shooting(resp: PlayerDashPtShots, label: str) -> None:
    """
    DRIBBLE_RANGE（出手前運球次數）：
      0 Dribbles    — 接球即出手（Catch & Shoot）
      1 Dribble     — 一次運球後出手
      2 Dribbles    — 兩次運球後出手
      3-6 Dribbles  — 中等運球（半隔離）
      7+ Dribbles   — 大量運球（完全隔離）
    可分析球員的持球進攻 vs 無球進攻比例。
    """
    df = resp.dribble_shooting.get_data_frame()
    _show_pt_split(df, f"{label}運球次數分布（DribbleShooting）",
                   range_col="DRIBBLE_RANGE")


# ---------------------------------------------------------------------------
# 6. 進攻時鐘剩餘時間（ShotClockShooting）
# ---------------------------------------------------------------------------

def show_shot_clock(resp: PlayerDashPtShots, label: str) -> None:
    """
    SHOT_CLOCK_RANGE（出手時進攻時鐘剩餘秒數）：
      24-22            — 早起手（拿到球馬上出手）
      22-18 Very Early — 非常提前
      18-15 Early      — 較早出手
      15-7 Average     — 正常時段
      7-4 Late         — 偏晚出手
      4-0 Very Late    — 時鐘快到最後才出手（被迫出手）
    可分析球員是否傾向早出手（catch-and-shoot）或是 ISO 到最後。
    """
    df = resp.shot_clock_shooting.get_data_frame()
    _show_pt_split(df, f"{label}進攻時鐘剩餘時間（ShotClockShooting）",
                   range_col="SHOT_CLOCK_RANGE")


# ---------------------------------------------------------------------------
# 7. 持球時間（TouchTimeShooting）
# ---------------------------------------------------------------------------

def show_touch_time(resp: PlayerDashPtShots, label: str) -> None:
    """
    TOUCH_TIME_RANGE（接球後出手前的持球秒數）：
      Touch < 2 Seconds  — 快速出手（典型 Catch & Shoot）
      Touch 2-6 Seconds  — 中等持球（評估後出手）
      Touch 6+ Seconds   — 長時間持球（多次假動作 / ISO）
    與 DribbleShooting 結合，可完整刻畫球員的進攻模式。
    """
    df = resp.touch_time_shooting.get_data_frame()
    _show_pt_split(df, f"{label}持球時間（TouchTimeShooting）",
                   range_col="TOUCH_TIME_RANGE")


# ---------------------------------------------------------------------------
# 8. 投籃側寫摘要（從多個 DataSet 萃取關鍵數字）
# ---------------------------------------------------------------------------

def show_shooting_profile(resp: PlayerDashPtShots, label: str) -> None:
    """
    綜合多個 DataSet 萃取球員投籃側寫：
    - 自主創造 vs 無球出手比例
    - 緊逼防守下的命中率
    - 時鐘壓力下的出手效率
    """
    print(f"── {label}投籃側寫摘要 " + "─" * 46)

    # 自主創造 vs Catch & Shoot（from DribbleShooting）
    drib_df = _clean(resp.dribble_shooting.get_data_frame(), "DRIBBLE_RANGE")
    if not drib_df.empty and "DRIBBLE_RANGE" in drib_df.columns:
        c_and_s = drib_df[drib_df["DRIBBLE_RANGE"].str.contains("0 Dribble", na=False)]
        heavy   = drib_df[drib_df["DRIBBLE_RANGE"].str.contains("7\+", regex=True, na=False)]
        cs_freq = float(c_and_s["FGA_FREQUENCY"].iloc[0]) if not c_and_s.empty and "FGA_FREQUENCY" in c_and_s.columns else None
        hv_freq = float(heavy["FGA_FREQUENCY"].iloc[0]) if not heavy.empty and "FGA_FREQUENCY" in heavy.columns else None
        print(f"  Catch & Shoot 出手比例 : {cs_freq*100:.1f}%" if cs_freq is not None else "  Catch & Shoot: N/A")
        print(f"  隔離進攻 (7+運球) 比例: {hv_freq*100:.1f}%" if hv_freq is not None else "  隔離進攻: N/A")

    # 緊逼防守效率（from ClosestDefenderShooting）
    def_df = _clean(resp.closest_defender_shooting.get_data_frame(), "CLOSE_DEF_DIST_RANGE")
    if not def_df.empty and "CLOSE_DEF_DIST_RANGE" in def_df.columns:
        tight    = def_df[def_df["CLOSE_DEF_DIST_RANGE"].str.contains("0-2|Very Tight", na=False)]
        wide_open = def_df[def_df["CLOSE_DEF_DIST_RANGE"].str.contains("6\+|Wide Open", regex=True, na=False)]
        for sub, lbl in [(tight, "緊逼下（0-2ft）EFG%"), (wide_open, "空檔（6+ft）EFG%")]:
            if not sub.empty and "EFG_PCT" in sub.columns:
                val = float(sub["EFG_PCT"].iloc[0])
                print(f"  {lbl:<24}: {val*100:.1f}%")

    # 時鐘壓力（from ShotClockShooting）
    clk_df = _clean(resp.shot_clock_shooting.get_data_frame(), "SHOT_CLOCK_RANGE")
    if not clk_df.empty and "SHOT_CLOCK_RANGE" in clk_df.columns:
        early = clk_df[clk_df["SHOT_CLOCK_RANGE"].str.contains("24-22|Very Early", na=False)]
        late  = clk_df[clk_df["SHOT_CLOCK_RANGE"].str.contains("4-0|Very Late", na=False)]
        for sub, lbl in [(early, "早出手 EFG%"), (late, "時鐘逼迫 EFG%")]:
            if not sub.empty and "EFG_PCT" in sub.columns:
                val = float(sub["EFG_PCT"].iloc[0])
                print(f"  {lbl:<24}: {val*100:.1f}%")

    print()


# ---------------------------------------------------------------------------
# 9. 常規賽 vs 季後賽 Overall 投籃對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashPtShots,
    po_resp: PlayerDashPtShots,
) -> None:
    reg_df = _clean(reg_resp.overall.get_data_frame(), "SHOT_TYPE")
    po_df  = _clean(po_resp.overall.get_data_frame(),  "SHOT_TYPE")

    if reg_df.empty or po_df.empty:
        return

    compare_cols = ["FGA_FREQUENCY", "FGM", "FGA", "FG_PCT", "EFG_PCT",
                    "FG2A_FREQUENCY", "FG2_PCT", "FG3A_FREQUENCY", "FG3_PCT"]

    # 取 SHOT_TYPE == "Overall" 或第一列
    reg_all = reg_df[reg_df["SHOT_TYPE"].str.upper().str.contains("OVERALL|ALL", na=False)]
    po_all  = po_df[po_df["SHOT_TYPE"].str.upper().str.contains("OVERALL|ALL", na=False)]
    reg_row = reg_all.iloc[0] if not reg_all.empty else reg_df.iloc[0]
    po_row  = po_all.iloc[0]  if not po_all.empty  else po_df.iloc[0]

    avail = [c for c in compare_cols if c in reg_df.columns and c in po_df.columns]
    cmp = pd.DataFrame({
        "常規賽": {c: reg_row[c] for c in avail},
        "季後賽": {c: po_row[c]  for c in avail},
    }).round(3)
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽 Overall 投籃對比 " + "─" * 24)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 10. 常規賽 vs 季後賽防守距離對比
# ---------------------------------------------------------------------------

def show_defender_comparison(
    reg_resp: PlayerDashPtShots,
    po_resp: PlayerDashPtShots,
) -> None:
    """常規賽 vs 季後賽在各防守距離下的 EFG% 對比。"""
    reg_df = _clean(reg_resp.closest_defender_shooting.get_data_frame(), "CLOSE_DEF_DIST_RANGE")
    po_df  = _clean(po_resp.closest_defender_shooting.get_data_frame(),  "CLOSE_DEF_DIST_RANGE")

    if reg_df.empty:
        return

    po_map: dict[str, pd.Series] = {}
    if not po_df.empty:
        for _, row in po_df.iterrows():
            po_map[str(row.get("CLOSE_DEF_DIST_RANGE", ""))] = row

    if "SORT_ORDER" in reg_df.columns:
        reg_df = reg_df.sort_values("SORT_ORDER")

    rows = []
    for _, reg_row in reg_df.iterrows():
        rng = str(reg_row.get("CLOSE_DEF_DIST_RANGE", "")).strip()
        entry = {"防守距離": rng}
        for col in ["FGA_FREQUENCY", "FG_PCT", "EFG_PCT", "FG3_PCT"]:
            r_v = reg_row.get(col)
            entry[f"常規_{col}"] = round(float(r_v), 3) if pd.notna(r_v) else None
            po_row = po_map.get(rng)
            if po_row is not None:
                p_v = po_row.get(col)
                entry[f"季後_{col}"] = round(float(p_v), 3) if pd.notna(p_v) else None
            else:
                entry[f"季後_{col}"] = None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("防守距離")
    print("── 常規賽 vs 季後賽防守距離 EFG% 對比 " + "─" * 22)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 11. 季後賽各輪次投籃側寫（po_round 無直接支援，改用日期範圍間接實現）
# ---------------------------------------------------------------------------

def show_playoff_shooting_profile(resp: PlayerDashPtShots, label: str) -> None:
    """季後賽完整投籃側寫（與常規賽側寫做對比用）。"""
    show_shooting_profile(resp, label)


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"
PER_MODE = PerModeSimple.per_game

if __name__ == "__main__":
    query  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    season = sys.argv[2] if len(sys.argv) > 2 else Season.default

    player_id, full_name = _find_player_id(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}  |  PerMode: {PER_MODE}")
    print(f"team_id  : 0（不限球隊，涵蓋整季所有場次）\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    reg_resp = fetch_dashboard(player_id, team_id=0, season=season,
                               season_type=SEASON_TYPE_REGULAR,
                               per_mode=PER_MODE)

    show_overall(reg_resp,              label="常規賽 ")
    show_general_shooting(reg_resp,     label="常規賽 ")
    show_closest_defender(reg_resp,     label="常規賽 ")
    show_closest_defender_10ft(reg_resp,label="常規賽 ")
    show_dribble_shooting(reg_resp,     label="常規賽 ")
    show_shot_clock(reg_resp,           label="常規賽 ")
    show_touch_time(reg_resp,           label="常規賽 ")
    show_shooting_profile(reg_resp,     label="常規賽 ")

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    po_resp = None
    try:
        po_resp = fetch_dashboard(player_id, team_id=0, season=season,
                                  season_type=SEASON_TYPE_PLAYOFFS,
                                  per_mode=PER_MODE)

        po_overall = po_resp.overall.get_data_frame()
        if po_overall.empty:
            print(f"  {full_name} 本季無季後賽出賽記錄（或尚未進入季後賽）。\n")
            po_resp = None
        else:
            show_overall(po_resp,               label="季後賽 ")
            show_general_shooting(po_resp,      label="季後賽 ")
            show_closest_defender(po_resp,      label="季後賽 ")
            show_closest_defender_10ft(po_resp, label="季後賽 ")
            show_dribble_shooting(po_resp,      label="季後賽 ")
            show_shot_clock(po_resp,            label="季後賽 ")
            show_touch_time(po_resp,            label="季後賽 ")
            show_playoff_shooting_profile(po_resp, label="季後賽 ")

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 並排對比 ──────────────────────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽投籃對比")
        print("=" * 65 + "\n")

        show_regular_vs_playoffs_comparison(reg_resp, po_resp)
        show_defender_comparison(reg_resp, po_resp)
