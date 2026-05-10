"""
nba_api.stats.endpoints.playerdashptreb 範例程式

執行方式：
  python example/stats/player_dash_pt_reb.py                      # 預設球員，本季
  python example/stats/player_dash_pt_reb.py "Nikola Jokic"       # 依姓名查詢
  python example/stats/player_dash_pt_reb.py 203999               # 依 player_id
  python example/stats/player_dash_pt_reb.py 203999 2023-24       # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashPtReb 聚焦「籃板追蹤」數據（Play Type Rebounding）：
  所有 DataSet 均以籃板為核心，依不同維度分組：
  - 爭奪籃板的對手人數（NumContested）
  - 球員搶板時的跑位距離（RebDistance）
  - 未命中球的出手距離（ShotDistance）
  - 未命中球的出手類型（ShotType）

與其他 Dashboard 的關鍵差異：
  1. 需要 team_id（傳 0 = 不限球隊）
  2. 使用 PerModeSimple / SeasonTypeAllStar（支援 Playoffs）
  3. 欄位完全以「籃板」為主軸，無投籃相關欄位
  4. 獨有的 C_REB / UC_REB 欄：
       C_REB   — Contested 爭奪籃板（有對手競爭）
       UC_REB  — Uncontested 無爭奪籃板（輕鬆到手）
       C_REB_PCT  — 爭奪板占比（愈高 = 愈積極）
       UC_REB_PCT — 無爭奪板占比

DataSet 總覽（共 5 個）：
  overall_rebounding          — 整體（單列）
  num_contested_rebounding    — 爭奪對手人數帶（1 人 / 2 人 / 3+ 人）
  reb_distance_rebounding     — 球員跑位距離帶（籃框附近 vs 外線）
  shot_distance_rebounding    — 未命中球的出手距離帶
  shot_type_rebounding        — 未命中球的出手類型（Dunk / Layup / Jump Shot…）

共用欄位（所有 DataSet）：
  G               — 場次
  分組欄：OVERALL / REB_NUM_CONTESTING_RANGE / REB_DIST_RANGE /
          SHOT_DIST_RANGE / SHOT_TYPE_RANGE
  REB_FREQUENCY   — 籃板頻率（此分組占總籃板的比例）
  OREB / DREB / REB           — 進攻 / 防守 / 總籃板
  C_OREB / C_DREB / C_REB / C_REB_PCT    — 爭奪板
  UC_OREB / UC_DREB / UC_REB / UC_REB_PCT — 無爭奪板
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashptreb import PlayerDashPtReb
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

STR_COLS = {
    "PLAYER_ID", "PLAYER_NAME_LAST_FIRST", "SORT_ORDER",
    "OVERALL", "REB_NUM_CONTESTING_RANGE", "REB_DIST_RANGE",
    "SHOT_DIST_RANGE", "SHOT_TYPE_RANGE",
}
REB_COLS = [
    "G", "REB_FREQUENCY",
    "OREB", "DREB", "REB",
    "C_OREB", "C_DREB", "C_REB", "C_REB_PCT",
    "UC_OREB", "UC_DREB", "UC_REB", "UC_REB_PCT",
]
# 各 DataSet 的分組欄
RANGE_COLS = {
    "overall":               "OVERALL",
    "num_contested":         "REB_NUM_CONTESTING_RANGE",
    "reb_distance":          "REB_DIST_RANGE",
    "shot_distance":         "SHOT_DIST_RANGE",
    "shot_type":             "SHOT_TYPE_RANGE",
}


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def _find_player(query: str) -> tuple[int, str]:
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


def _find_team_id(player_id: int, season: str) -> int:
    """發一次 team_id=0 的請求，從回傳資料自動取得 TEAM_ID。"""
    try:
        resp = PlayerDashPtReb(
            team_id=0, player_id=player_id,
            season=season,
            season_type_all_star=SEASON_TYPE_REGULAR,
            timeout=TIMEOUT,
        )
        df = resp.overall_rebounding.get_data_frame()
        # OverallRebounding 沒有 TEAM_ID，改查 NumContested
        df2 = resp.num_contested_rebounding.get_data_frame()
        # 這些 DataSet 也沒有 TEAM_ID...
        # 回傳 0 即可（不分球隊）
    except Exception:
        pass
    return 0


def fetch_reb_data(
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
) -> PlayerDashPtReb:
    """取得 PtReb Dashboard，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashPtReb(
                team_id=team_id,
                player_id=player_id,
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


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    num_cols = [c for c in df.columns if c not in STR_COLS]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


def _show_reb_split(
    df: pd.DataFrame,
    title: str,
    range_col: str,
) -> None:
    """通用：印出任一 PtReb DataSet。"""
    if df.empty:
        print(f"── {title}：無資料\n")
        return

    df = _clean(df)
    if "SORT_ORDER" in df.columns:
        df = df.sort_values("SORT_ORDER")

    avail = [range_col] + [c for c in REB_COLS if c in df.columns]
    print(f"── {title} " + "─" * max(0, 58 - len(title)))
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 1. 整體籃板（OverallRebounding）
# ---------------------------------------------------------------------------

def show_overall(resp: PlayerDashPtReb, label: str) -> None:
    """
    單列整體數據，含 C_REB_PCT（爭奪板占比）與 UC_REB_PCT（無爭奪板占比）。
    爭奪板占比高 → 積極搶板；無爭奪板占比高 → 多為輕鬆到手的板。
    """
    df = _clean(resp.overall_rebounding.get_data_frame())
    if df.empty:
        print(f"── {label}整體籃板：無資料\n")
        return

    avail = ["OVERALL"] + [c for c in REB_COLS if c in df.columns]
    print(f"── {label}整體籃板數據 " + "─" * 46)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 爭奪對手人數（NumContestedRebounding）
# ---------------------------------------------------------------------------

def show_num_contested(resp: PlayerDashPtReb, label: str) -> None:
    """
    REB_NUM_CONTESTING_RANGE：爭奪籃板時對手人數帶。
    常見值：1 Rebounders / 2 Rebounders / 3 Rebounders / 4-5 Rebounders / 6+ Rebounders
    人數越多代表在競爭越激烈的情況下仍能搶到板。
    """
    df = resp.num_contested_rebounding.get_data_frame()
    _show_reb_split(df, f"{label}爭奪對手人數（NumContested）",
                    range_col="REB_NUM_CONTESTING_RANGE")


# ---------------------------------------------------------------------------
# 3. 球員跑位距離（RebDistanceRebounding）
# ---------------------------------------------------------------------------

def show_reb_distance(resp: PlayerDashPtReb, label: str) -> None:
    """
    REB_DIST_RANGE：球員距籃框的跑位距離帶。
    距離越遠代表球員會往外線跑位搶板（外線籃板），
    距離越近代表主要在禁區搶板（內線籃板）。
    """
    df = resp.reb_distance_rebounding.get_data_frame()
    _show_reb_split(df, f"{label}球員跑位距離（RebDistance）",
                    range_col="REB_DIST_RANGE")


# ---------------------------------------------------------------------------
# 4. 出手距離（ShotDistanceRebounding）
# ---------------------------------------------------------------------------

def show_shot_distance(resp: PlayerDashPtReb, label: str) -> None:
    """
    SHOT_DIST_RANGE：未命中球的出手距離帶。
    出手距離遠（如三分線外）的 miss，籃板通常落點更不確定。
    可分析球員在搶哪種出手距離 miss 的籃板上最活躍。
    """
    df = resp.shot_distance_rebounding.get_data_frame()
    _show_reb_split(df, f"{label}出手距離（ShotDistance）",
                    range_col="SHOT_DIST_RANGE")


# ---------------------------------------------------------------------------
# 5. 出手類型（ShotTypeRebounding）
# ---------------------------------------------------------------------------

def show_shot_type(resp: PlayerDashPtReb, label: str) -> None:
    """
    SHOT_TYPE_RANGE：未命中球的出手類型。
    常見值：Dunk / Hook Shot / Jump Shot / Layup / Tip Shot
    Jump Shot 的 miss 籃板通常反彈更遠，Dunk/Layup 的 miss 多落在禁區。
    """
    df = resp.shot_type_rebounding.get_data_frame()
    _show_reb_split(df, f"{label}出手類型（ShotType）",
                    range_col="SHOT_TYPE_RANGE")


# ---------------------------------------------------------------------------
# 6. 籃板側寫摘要
# ---------------------------------------------------------------------------

def show_rebounding_profile(resp: PlayerDashPtReb, label: str) -> None:
    """
    從多個 DataSet 萃取球員籃板側寫：
    - 整體爭奪板 vs 無爭奪板比例
    - 在競爭最激烈條件下的效率
    - 外線 vs 禁區籃板比例
    """
    print(f"── {label}籃板側寫摘要 " + "─" * 46)

    # 整體 C/UC 比
    ov_df = _clean(resp.overall_rebounding.get_data_frame())
    if not ov_df.empty:
        row = ov_df.iloc[0]
        reb      = row.get("REB", 0)
        c_pct    = row.get("C_REB_PCT",  None)
        uc_pct   = row.get("UC_REB_PCT", None)
        oreb     = row.get("OREB", 0)
        dreb     = row.get("DREB", 0)
        g        = row.get("G", 0)
        print(f"  場均籃板 : {float(reb):.1f}  （進攻 {float(oreb):.1f} / 防守 {float(dreb):.1f}，{int(g)} 場）")
        if pd.notna(c_pct):
            print(f"  爭奪板占比: {float(c_pct)*100:.1f}%   無爭奪板占比: {float(uc_pct)*100:.1f}%" if pd.notna(uc_pct) else f"  爭奪板占比: {float(c_pct)*100:.1f}%")

    # 最多對手爭奪下的籃板
    nc_df = _clean(resp.num_contested_rebounding.get_data_frame())
    if not nc_df.empty and "SORT_ORDER" in nc_df.columns:
        nc_df = nc_df.sort_values("SORT_ORDER", ascending=False)
        hardest = nc_df.iloc[0]
        rng = hardest.get("REB_NUM_CONTESTING_RANGE", "")
        reb_h = hardest.get("REB", 0)
        freq_h = hardest.get("REB_FREQUENCY", None)
        print(f"  最激烈競爭（{rng}）: 場均 {float(reb_h):.1f} 板", end="")
        if pd.notna(freq_h):
            print(f"  占比 {float(freq_h)*100:.1f}%")
        else:
            print()

    # 外線（跑位距離 > 18ft）籃板比例
    rd_df = _clean(resp.reb_distance_rebounding.get_data_frame())
    if not rd_df.empty and "REB_DIST_RANGE" in rd_df.columns:
        perimeter = rd_df[rd_df["REB_DIST_RANGE"].str.contains(r"19|25\+|2\d ft", regex=True, na=False)]
        rim       = rd_df[rd_df["REB_DIST_RANGE"].str.contains(r"[<≤]?\s*6|0-6", regex=True, na=False)]
        if not perimeter.empty and "REB_FREQUENCY" in perimeter.columns:
            peri_freq = perimeter["REB_FREQUENCY"].sum()
            print(f"  外線籃板占比（19ft+）: {float(peri_freq)*100:.1f}%")
        if not rim.empty and "REB_FREQUENCY" in rim.columns:
            rim_freq = rim["REB_FREQUENCY"].sum()
            print(f"  禁區籃板占比（≤6ft）: {float(rim_freq)*100:.1f}%")

    print()


# ---------------------------------------------------------------------------
# 7. 常規賽 vs 季後賽整體籃板對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashPtReb,
    po_resp: PlayerDashPtReb,
) -> None:
    reg_df = _clean(reg_resp.overall_rebounding.get_data_frame())
    po_df  = _clean(po_resp.overall_rebounding.get_data_frame())

    if reg_df.empty or po_df.empty:
        return

    compare_cols = ["G", "REB_FREQUENCY", "OREB", "DREB", "REB",
                    "C_REB", "C_REB_PCT", "UC_REB", "UC_REB_PCT"]
    avail = [c for c in compare_cols if c in reg_df.columns and c in po_df.columns]

    reg_row = reg_df.iloc[0]
    po_row  = po_df.iloc[0]

    cmp = pd.DataFrame({
        "常規賽": {c: reg_row[c] for c in avail},
        "季後賽": {c: po_row[c]  for c in avail},
    }).round(3)
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽整體籃板對比 " + "─" * 30)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 8. 常規賽 vs 季後賽出手類型籃板對比
# ---------------------------------------------------------------------------

def show_shot_type_comparison(
    reg_resp: PlayerDashPtReb,
    po_resp: PlayerDashPtReb,
) -> None:
    """比較常規賽與季後賽在各出手類型下的籃板頻率與爭奪板占比。"""
    reg_df = _clean(reg_resp.shot_type_rebounding.get_data_frame())
    po_df  = _clean(po_resp.shot_type_rebounding.get_data_frame())

    if reg_df.empty:
        return

    po_map: dict[str, pd.Series] = {}
    if not po_df.empty and "SHOT_TYPE_RANGE" in po_df.columns:
        for _, row in po_df.iterrows():
            po_map[str(row.get("SHOT_TYPE_RANGE", ""))] = row

    if "SORT_ORDER" in reg_df.columns:
        reg_df = reg_df.sort_values("SORT_ORDER")

    rows = []
    for _, reg_row in reg_df.iterrows():
        shot_type = str(reg_row.get("SHOT_TYPE_RANGE", "")).strip()
        entry = {"出手類型": shot_type}
        for col in ["REB_FREQUENCY", "REB", "C_REB_PCT"]:
            r_v = reg_row.get(col)
            entry[f"常規_{col}"] = round(float(r_v), 3) if pd.notna(r_v) else None
            po_row = po_map.get(shot_type)
            if po_row is not None:
                p_v = po_row.get(col)
                entry[f"季後_{col}"] = round(float(p_v), 3) if pd.notna(p_v) else None
            else:
                entry[f"季後_{col}"] = None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("出手類型")
    print("── 常規賽 vs 季後賽出手類型籃板對比 " + "─" * 26)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "Nikola Jokic"   # 聯盟最佳全能籃板中鋒，資料最具代表性
PER_MODE = PerModeSimple.per_game

if __name__ == "__main__":
    query  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    season = sys.argv[2] if len(sys.argv) > 2 else Season.default

    player_id, full_name = _find_player(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}  |  PerMode: {PER_MODE}")
    print(f"team_id  : 0（不限球隊，涵蓋整季所有場次）\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    reg_resp = fetch_reb_data(
        player_id=player_id, team_id=0,
        season=season,
        season_type=SEASON_TYPE_REGULAR,
        per_mode=PER_MODE,
    )

    show_overall(reg_resp,          label="常規賽 ")
    show_rebounding_profile(reg_resp, label="常規賽 ")
    show_num_contested(reg_resp,    label="常規賽 ")
    show_reb_distance(reg_resp,     label="常規賽 ")
    show_shot_distance(reg_resp,    label="常規賽 ")
    show_shot_type(reg_resp,        label="常規賽 ")

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    po_resp = None
    try:
        po_resp = fetch_reb_data(
            player_id=player_id, team_id=0,
            season=season,
            season_type=SEASON_TYPE_PLAYOFFS,
            per_mode=PER_MODE,
        )

        po_overall = po_resp.overall_rebounding.get_data_frame()
        if po_overall.empty:
            print(f"  {full_name} 本季無季後賽出賽記錄（或尚未進入季後賽）。\n")
            po_resp = None
        else:
            show_overall(po_resp,             label="季後賽 ")
            show_rebounding_profile(po_resp,  label="季後賽 ")
            show_num_contested(po_resp,       label="季後賽 ")
            show_reb_distance(po_resp,        label="季後賽 ")
            show_shot_distance(po_resp,       label="季後賽 ")
            show_shot_type(po_resp,           label="季後賽 ")

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 並排對比 ──────────────────────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽籃板對比")
        print("=" * 65 + "\n")

        show_regular_vs_playoffs_comparison(reg_resp, po_resp)
        show_shot_type_comparison(reg_resp, po_resp)
