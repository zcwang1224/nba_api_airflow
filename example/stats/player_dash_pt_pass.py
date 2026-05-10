"""
nba_api.stats.endpoints.playerdashptpass 範例程式

執行方式：
  python example/stats/player_dash_pt_pass.py                   # 預設球員，本季
  python example/stats/player_dash_pt_pass.py "Nikola Jokic"   # 依姓名查詢
  python example/stats/player_dash_pt_pass.py 203999            # 依 player_id
  python example/stats/player_dash_pt_pass.py 203999 2023-24    # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashPtPass 聚焦「傳球助攻」數據，分為兩個視角：
  PassesMade     — 球員「傳出」後，隊友的出手與得分情況
  PassesReceived — 球員「接收」後，自己的出手與得分情況

DataSet 欄位說明：
  ┌───────────────────────────────────────────────────────────────┐
  │  欄位名稱             說明                                     │
  ├───────────────────────────────────────────────────────────────┤
  │  PASS_TYPE            傳球類型（如 "2PT" / "3PT"）             │
  │  G                    場次                                     │
  │  PASS_TO / PASS_FROM  接球隊友 / 傳球隊友姓名                  │
  │  PASS_TEAMMATE_PLAYER_ID  隊友 player_id                       │
  │  FREQUENCY            佔全隊傳球比例                           │
  │  PASS                 傳球總次數                               │
  │  AST                  助攻次數                                 │
  │  FGM / FGA / FG_PCT   出手命中 / 出手次數 / 命中率             │
  │  FG2M / FG2A / FG2_PCT  兩分球                                 │
  │  FG3M / FG3A / FG3_PCT  三分球                                 │
  └───────────────────────────────────────────────────────────────┘

主要參數：
  team_id / player_id / season / season_type_all_star /
  per_mode_simple / last_n_games / opponent_team_id /
  month / location_nullable / outcome_nullable 等
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashptpass import PlayerDashPtPass
from nba_api.stats.library.parameters import (
    PerModeSimple,
    Season,
    SeasonTypeAllStar,
)
from nba_api.stats.static import players, teams

TIMEOUT     = 60
RETRIES     = 3
RETRY_DELAY = 5

SEASON_TYPE_REGULAR  = SeasonTypeAllStar.regular
SEASON_TYPE_PLAYOFFS = SeasonTypeAllStar.playoffs

PASS_COLS = [
    "PASS_TYPE", "G", "FREQUENCY", "PASS", "AST",
    "FGM", "FGA", "FG_PCT",
    "FG2M", "FG2A", "FG2_PCT",
    "FG3M", "FG3A", "FG3_PCT",
]
TOP_N = 10


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def _find_player(query: str) -> tuple[int, str]:
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


def _find_team_id(player_id: int, season: str) -> int:
    """
    從 PlayerDashPtPass 的 PassesMade 取出 TEAM_ID。
    先用 team_id=0 發送一次請求，從回傳資料中取得實際 TEAM_ID。
    若無資料則回傳 0（讓後續查詢用 0 代替）。
    """
    try:
        resp = PlayerDashPtPass(
            team_id=0,
            player_id=player_id,
            season=season,
            season_type_all_star=SEASON_TYPE_REGULAR,
            timeout=TIMEOUT,
        )
        df = resp.passes_made.get_data_frame()
        if not df.empty and "TEAM_ID" in df.columns:
            return int(df["TEAM_ID"].iloc[0])
    except Exception:
        pass
    return 0


def fetch_pass_data(
    team_id: int,
    player_id: int,
    season: str = Season.default,
    season_type: str = SEASON_TYPE_REGULAR,
    per_mode: str = PerModeSimple.per_game,
    last_n_games: int = 0,
    month: int = 0,
    opponent_team_id: int = 0,
    date_from: str = "",
    date_to: str = "",
    location: str = "",
    outcome: str = "",
    season_segment: str = "",
    vs_conference: str = "",
    vs_division: str = "",
) -> PlayerDashPtPass:
    """取得 PlayerDashPtPass response，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashPtPass(
                team_id=team_id,
                player_id=player_id,
                season=season,
                season_type_all_star=season_type,
                per_mode_simple=per_mode,
                last_n_games=str(last_n_games),
                month=str(month),
                opponent_team_id=opponent_team_id,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                location_nullable=location,
                outcome_nullable=outcome,
                season_segment_nullable=season_segment,
                vs_conference_nullable=vs_conference,
                vs_division_nullable=vs_division,
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
    num_cols = [c for c in df.columns
                if c not in {"PLAYER_ID", "PLAYER_NAME_LAST_FIRST", "TEAM_NAME",
                              "TEAM_ID", "TEAM_ABBREVIATION", "PASS_TYPE",
                              "PASS_TO", "PASS_FROM", "PASS_TEAMMATE_PLAYER_ID"}]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 1. 傳出（PassesMade）：助攻給哪些隊友
# ---------------------------------------------------------------------------

def show_passes_made(resp: PlayerDashPtPass, label: str) -> None:
    df = _clean(resp.passes_made.get_data_frame())
    if df.empty:
        print(f"── {label}傳出助攻：無資料\n")
        return

    teammate_col = "PASS_TO"
    avail = [teammate_col] + [c for c in PASS_COLS if c in df.columns and c != teammate_col]

    total_passes = df["PASS"].sum() if "PASS" in df.columns else 0
    total_ast    = df["AST"].sum()  if "AST"  in df.columns else 0

    print(f"── {label}傳出助攻（PassesMade）" + "─" * 42)
    print(f"  傳球總次數: {int(total_passes)}  |  助攻總次數: {int(total_ast)}")
    print()

    top = df.nlargest(TOP_N, "PASS") if "PASS" in df.columns else df.head(TOP_N)
    print(f"  傳球次數最多前 {TOP_N} 位隊友：")
    print(top[avail].to_string(index=False))
    print()

    if "AST" in df.columns:
        top_ast = df.nlargest(TOP_N, "AST")
        print(f"  助攻次數最多前 {TOP_N} 位隊友：")
        print(top_ast[avail].to_string(index=False))
        print()

    if "FG3_PCT" in df.columns:
        three_pass = df[df["FG3A"] >= 5].copy() if "FG3A" in df.columns else pd.DataFrame()
        if not three_pass.empty:
            top_3p = three_pass.nlargest(5, "FG3_PCT")
            print("  傳球後隊友三分命中率最高（FG3A ≥ 5）：")
            print(top_3p[[teammate_col, "FG3A", "FG3M", "FG3_PCT"]].to_string(index=False))
            print()


# ---------------------------------------------------------------------------
# 2. 接收（PassesReceived）：從哪些隊友接球後的出手效率
# ---------------------------------------------------------------------------

def show_passes_received(resp: PlayerDashPtPass, label: str) -> None:
    df = _clean(resp.passes_received.get_data_frame())
    if df.empty:
        print(f"── {label}接收傳球：無資料\n")
        return

    teammate_col = "PASS_FROM"
    avail = [teammate_col] + [c for c in PASS_COLS if c in df.columns and c != teammate_col]

    total_passes = df["PASS"].sum() if "PASS" in df.columns else 0
    total_ast    = df["AST"].sum()  if "AST"  in df.columns else 0

    print(f"── {label}接收傳球（PassesReceived）" + "─" * 40)
    print(f"  接球總次數: {int(total_passes)}  |  接球後獲得助攻次數: {int(total_ast)}")
    print()

    top = df.nlargest(TOP_N, "PASS") if "PASS" in df.columns else df.head(TOP_N)
    print(f"  傳球次數最多前 {TOP_N} 位隊友：")
    print(top[avail].to_string(index=False))
    print()

    if "FG_PCT" in df.columns:
        fga_filter = df[df["FGA"] >= 5].copy() if "FGA" in df.columns else pd.DataFrame()
        if not fga_filter.empty:
            top_eff = fga_filter.nlargest(5, "FG_PCT")
            print("  接球後命中率最高（FGA ≥ 5）：")
            print(top_eff[[teammate_col, "FGA", "FGM", "FG_PCT"]].to_string(index=False))
            print()


# ---------------------------------------------------------------------------
# 3. 傳出 vs 接收綜合摘要
# ---------------------------------------------------------------------------

def show_summary(resp: PlayerDashPtPass, label: str) -> None:
    made = _clean(resp.passes_made.get_data_frame())
    recv = _clean(resp.passes_received.get_data_frame())

    print(f"── {label}傳球綜合摘要" + "─" * 48)

    def _sum(df: pd.DataFrame, col: str) -> float:
        if df.empty or col not in df.columns:
            return 0.0
        return float(df[col].sum())

    def _wavg(df: pd.DataFrame, val_col: str, weight_col: str) -> float | None:
        if df.empty or val_col not in df.columns or weight_col not in df.columns:
            return None
        mask = df[weight_col].notna() & (df[weight_col] > 0)
        sub  = df[mask]
        if sub.empty:
            return None
        total_w = sub[weight_col].sum()
        if total_w == 0:
            return None
        return float((sub[val_col] * sub[weight_col]).sum() / total_w)

    rows = [
        {"項目": "傳出（PassesMade）",
         "傳球次數":  int(_sum(made, "PASS")),
         "助攻次數":  int(_sum(made, "AST")),
         "隊友FG%":   round(_wavg(made, "FG_PCT", "FGA") or 0, 3),
         "隊友FG3%":  round(_wavg(made, "FG3_PCT", "FG3A") or 0, 3)},
        {"項目": "接收（PassesReceived）",
         "傳球次數":  int(_sum(recv, "PASS")),
         "助攻次數":  int(_sum(recv, "AST")),
         "隊友FG%":   round(_wavg(recv, "FG_PCT", "FGA") or 0, 3),
         "隊友FG3%":  round(_wavg(recv, "FG3_PCT", "FG3A") or 0, 3)},
    ]

    summary_df = pd.DataFrame(rows).set_index("項目")
    print(summary_df.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 常規賽 vs 季後賽傳球對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs(
    reg_resp: PlayerDashPtPass,
    po_resp: PlayerDashPtPass,
) -> None:
    print("── 常規賽 vs 季後賽傳球對比" + "─" * 40)

    for dataset_name, attr, teammate_col in [
        ("傳出（PassesMade）",   "passes_made",     "PASS_TO"),
        ("接收（PassesReceived）", "passes_received", "PASS_FROM"),
    ]:
        reg_df = _clean(getattr(reg_resp, attr).get_data_frame())
        po_df  = _clean(getattr(po_resp,  attr).get_data_frame())

        rows = []
        for season_label, df in [("常規賽", reg_df), ("季後賽", po_df)]:
            if df.empty:
                rows.append({"賽事": season_label, "傳球次數": None,
                              "助攻次數": None, "出手FG%": None, "FG3%": None})
                continue
            rows.append({
                "賽事":     season_label,
                "傳球次數": int(df["PASS"].sum())   if "PASS" in df.columns else None,
                "助攻次數": int(df["AST"].sum())    if "AST"  in df.columns else None,
                "出手FG%":  round(float(df["FG_PCT"].mean()),  3) if "FG_PCT"  in df.columns else None,
                "FG3%":     round(float(df["FG3_PCT"].mean()), 3) if "FG3_PCT" in df.columns else None,
            })

        cmp = pd.DataFrame(rows).set_index("賽事")
        print(f"\n  {dataset_name}")
        print(cmp.to_string())

    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "Nikola Jokic"
PER_MODE = PerModeSimple.per_game

if __name__ == "__main__":
    query  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    season = sys.argv[2] if len(sys.argv) > 2 else Season.default

    player_id, full_name = _find_player(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}  |  PerMode: {PER_MODE}")
    print(f"取得所屬球隊 ID 中…")

    team_id = _find_team_id(player_id, season)
    print(f"TEAM_ID  : {team_id}\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    reg_resp = fetch_pass_data(
        team_id=team_id,
        player_id=player_id,
        season=season,
        season_type=SEASON_TYPE_REGULAR,
        per_mode=PER_MODE,
    )

    show_summary(reg_resp,         label="常規賽 ")
    show_passes_made(reg_resp,     label="常規賽 ")
    show_passes_received(reg_resp, label="常規賽 ")

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    po_resp = None
    try:
        po_resp = fetch_pass_data(
            team_id=team_id,
            player_id=player_id,
            season=season,
            season_type=SEASON_TYPE_PLAYOFFS,
            per_mode=PER_MODE,
        )

        po_made = po_resp.passes_made.get_data_frame()
        if po_made.empty:
            print(f"  {full_name} 本季無季後賽出賽記錄（或尚未進入季後賽）。\n")
            po_resp = None
        else:
            show_summary(po_resp,         label="季後賽 ")
            show_passes_made(po_resp,     label="季後賽 ")
            show_passes_received(po_resp, label="季後賽 ")

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 常規賽 vs 季後賽對比 ──────────────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽傳球對比")
        print("=" * 65 + "\n")
        show_regular_vs_playoffs(reg_resp, po_resp)
