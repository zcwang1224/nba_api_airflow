"""
nba_api.stats.endpoints.playercompare 範例程式

執行方式：
  python example/stats/player_compare.py                              # 預設示範對決
  python example/stats/player_compare.py "LeBron James" "Kevin Durant"          # 1v1
  python example/stats/player_compare.py "LeBron James" "Kevin Durant" 2023-24  # 指定球季

PlayerCompare 必填參數：
  player_id_list      — 主方球員 ID，多人以逗號分隔，如 "2544,201939"
  vs_player_id_list   — 對照方球員 ID，多人以逗號分隔

PlayerCompare 選填參數：
  season                      — 球季（預設當季）
  season_type_playoffs        — "Regular Season"（預設）/ "Pre Season" /
                                "Playoffs" / "PlayIn"
  measure_type_detailed_defense — "Base"（預設）/ "Advanced" / "Misc" /
                                  "Scoring" / "Usage" / "Four Factors" /
                                  "Opponent" / "Defense"
  per_mode_detailed           — "Totals"（預設）/ "PerGame" / "Per36" /
                                "Per48" / "Per100Possessions" / "PerPossession"
  last_n_games                — 最近 N 場（0=全部）
  month                       — 月份（0=全部）
  period                      — 節次（0=全場，1-4=指定節）
  pace_adjust                 — "Y" / "N"（預設 "N"）
  plus_minus                  — "Y" / "N"（預設 "N"）
  rank                        — "Y" / "N"（預設 "N"）
  date_from_nullable          — 起始日期 "YYYY-MM-DD"
  date_to_nullable            — 結束日期 "YYYY-MM-DD"
  location_nullable           — "Home" / "Road"
  outcome_nullable            — "W" / "L"
  season_segment_nullable     — "Pre All-Star" / "Post All-Star"
  opponent_team_id            — 對手球隊 ID（0=全部）

DataSet：
  resp.individual        — 各方球員在不同情境下的細分數據（by GROUP_SET / DESCRIPTION）
  resp.overall_compare   — 雙方整體數據對比（by GROUP_SET / DESCRIPTION）

欄位說明：
  GROUP_SET   — 球員姓名或群組標籤
  DESCRIPTION — 統計維度描述（如 "Overall", "On Court", "Off Court"）
  MIN         — 上場時間
  FGM/FGA/FG_PCT  — 投籃
  FG3M/FG3A/FG3_PCT — 三分
  FTM/FTA/FT_PCT  — 罰球
  OREB/DREB/REB   — 籃板
  AST / TOV / STL / BLK / BLKA / PF / PFD
  PTS         — 得分
  PLUS_MINUS  — 正負值
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playercompare import PlayerCompare
from nba_api.stats.library.parameters import (
    MeasureTypeDetailedDefense,
    PerModeDetailed,
    Season,
    SeasonType,
)
from nba_api.stats.static import players

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5

STAT_COLS = [
    "MIN", "PTS", "REB", "AST", "STL", "BLK",
    "FG_PCT", "FG3_PCT", "FT_PCT", "TOV", "PLUS_MINUS",
]
ALL_STAT_COLS = [
    "MIN", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB",
    "AST", "TOV", "STL", "BLK", "BLKA", "PF", "PFD",
    "PTS", "PLUS_MINUS",
]


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


def _ids_to_str(id_list: list[int]) -> str:
    """將 player_id list 轉為逗號分隔字串"""
    return ",".join(str(i) for i in id_list)


def fetch_compare(
    player_ids: list[int],
    vs_player_ids: list[int],
    season: str = Season.default,
    season_type: str = SeasonType.default,
    measure_type: str = MeasureTypeDetailedDefense.default,
    per_mode: str = PerModeDetailed.default,
    last_n_games: int = 0,
    month: int = 0,
    period: int = 0,
    pace_adjust: str = "N",
    plus_minus: str = "N",
    rank: str = "N",
    date_from: str = "",
    date_to: str = "",
    location: str = "",
    outcome: str = "",
    season_segment: str = "",
    opponent_team_id: int = 0,
) -> PlayerCompare:
    """
    取得 PlayerCompare response 物件，失敗時自動重試。
    player_ids / vs_player_ids 為 player_id 的 list，多人用逗號合併後送出。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerCompare(
                player_id_list=_ids_to_str(player_ids),
                vs_player_id_list=_ids_to_str(vs_player_ids),
                season=season,
                season_type_playoffs=season_type,
                measure_type_detailed_defense=measure_type,
                per_mode_detailed=per_mode,
                last_n_games=str(last_n_games),
                month=str(month),
                period=str(period),
                pace_adjust=pace_adjust,
                plus_minus=plus_minus,
                rank=rank,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                location_nullable=location,
                outcome_nullable=outcome,
                season_segment_nullable=season_segment,
                opponent_team_id=opponent_team_id,
                timeout=TIMEOUT,
            )
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)


def _to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    num_cols = [c for c in df.columns if c not in ("GROUP_SET", "DESCRIPTION")]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 1. OverallCompare — 整體數據對比
# ---------------------------------------------------------------------------

def show_overall_compare(resp: PlayerCompare, per_mode: str) -> None:
    """印出 OverallCompare DataSet：雙方各條件下的整體數據"""
    df = resp.overall_compare.get_data_frame()
    if df.empty:
        print("── OverallCompare：無資料\n")
        return

    df = _to_numeric(df)
    label = {"Totals": "累計", "PerGame": "場均", "Per36": "Per36",
             "Per100Possessions": "Per100Poss"}.get(per_mode, per_mode)

    # 依 DESCRIPTION 分組，每個 group 並排雙方
    descriptions = df["DESCRIPTION"].unique()
    for desc in descriptions:
        sub = df[df["DESCRIPTION"] == desc]
        avail = [c for c in ["GROUP_SET"] + STAT_COLS if c in sub.columns]
        print(f"── OverallCompare  [{desc}]（{label}）" + "─" * 30)
        print(sub[avail].to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 2. Individual — 各方細分情境數據
# ---------------------------------------------------------------------------

def show_individual(resp: PlayerCompare, per_mode: str) -> None:
    """印出 Individual DataSet：各方球員在不同情境下的數據"""
    df = resp.individual.get_data_frame()
    if df.empty:
        print("── Individual：無資料\n")
        return

    df = _to_numeric(df)
    label = {"Totals": "累計", "PerGame": "場均", "Per36": "Per36",
             "Per100Possessions": "Per100Poss"}.get(per_mode, per_mode)

    groups = df["GROUP_SET"].unique()
    for grp in groups:
        sub = df[df["GROUP_SET"] == grp]
        avail = [c for c in ["DESCRIPTION"] + STAT_COLS if c in sub.columns]
        print(f"── Individual  [{grp}]（{label}）" + "─" * 36)
        print(sub[avail].to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 3. 快速對比摘要（從 OverallCompare 提取 Overall 行）
# ---------------------------------------------------------------------------

METRIC_LABEL = {
    "MIN":        "上場時間",
    "PTS":        "得分",
    "REB":        "籃板",
    "AST":        "助攻",
    "STL":        "抄截",
    "BLK":        "火鍋",
    "FG_PCT":     "投籃%",
    "FG3_PCT":    "三分%",
    "FT_PCT":     "罰球%",
    "TOV":        "失誤",
    "PLUS_MINUS": "正負值",
    "BLKA":       "被封蓋",
    "PFD":        "被犯規",
}
LOWER_IS_BETTER = {"TOV", "BLKA"}


def show_head_to_head(resp: PlayerCompare, names: list[str]) -> None:
    """
    從 OverallCompare 取 Overall 行，製作清楚的 head-to-head 對比表。
    每行一個統計項目，欄位為兩名球員，標記哪方佔優。
    """
    df = resp.overall_compare.get_data_frame()
    if df.empty:
        return

    df = _to_numeric(df)
    overall = df[df["DESCRIPTION"].str.upper().str.contains("OVERALL", na=False)]
    if overall.empty:
        overall = df  # fallback: 取第一筆

    if len(overall) < 2:
        print("── Head-to-Head：資料不足（少於 2 方）\n")
        return

    row_a = overall.iloc[0]
    row_b = overall.iloc[1]
    name_a = names[0] if names else str(row_a.get("GROUP_SET", "Player A"))
    name_b = names[1] if len(names) > 1 else str(row_b.get("GROUP_SET", "Player B"))

    rows = []
    for col in ALL_STAT_COLS:
        if col not in df.columns:
            continue
        label = METRIC_LABEL.get(col, col)
        val_a = row_a.get(col)
        val_b = row_b.get(col)
        if pd.isna(val_a) and pd.isna(val_b):
            continue

        if pd.notna(val_a) and pd.notna(val_b):
            if col in LOWER_IS_BETTER:
                winner = name_a if val_a < val_b else (name_b if val_b < val_a else "平手")
            else:
                winner = name_a if val_a > val_b else (name_b if val_b > val_a else "平手")
        else:
            winner = "—"

        rows.append({
            "統計項目":  label,
            name_a:      round(float(val_a), 3) if pd.notna(val_a) else "—",
            name_b:      round(float(val_b), 3) if pd.notna(val_b) else "—",
            "領先方":    winner,
        })

    if not rows:
        return

    result = pd.DataFrame(rows)
    print(f"── Head-to-Head 對比：{name_a}  vs  {name_b} " + "─" * 18)
    print(result.to_string(index=False))

    # 勝負統計
    wins_a = sum(1 for r in rows if r["領先方"] == name_a)
    wins_b = sum(1 for r in rows if r["領先方"] == name_b)
    ties   = sum(1 for r in rows if r["領先方"] == "平手")
    print(f"\n  各項佔優：{name_a} {wins_a}  /  {name_b} {wins_b}  /  平手 {ties}")
    print()


# ---------------------------------------------------------------------------
# 4. 多種 MeasureType 對比（Base / Advanced / Scoring / Defense）
# ---------------------------------------------------------------------------

MEASURE_TYPES = [
    ("Base",         "基礎數據"),
    ("Advanced",     "進階效率"),
    ("Scoring",      "得分方式"),
    ("Defense",      "防守數據"),
]

MEASURE_KEY_COLS = {
    "Base":     ["PTS", "REB", "AST", "FG_PCT", "PLUS_MINUS"],
    "Advanced": ["MIN", "PTS", "REB", "AST", "PLUS_MINUS"],
    "Scoring":  ["PTS", "FG_PCT", "FG3_PCT", "FT_PCT"],
    "Defense":  ["STL", "BLK", "BLKA", "PFD"],
}


def show_multi_measure(
    player_ids: list[int],
    vs_player_ids: list[int],
    names: list[str],
    season: str,
    season_type: str,
) -> None:
    """跑多個 MeasureType，摘要每類型的關鍵指標"""
    for measure, measure_label in MEASURE_TYPES:
        try:
            resp = fetch_compare(
                player_ids=player_ids,
                vs_player_ids=vs_player_ids,
                season=season,
                season_type=season_type,
                measure_type=measure,
                per_mode="PerGame",
            )
        except Exception as e:
            print(f"  [{measure_label}] 取得失敗: {e}\n")
            continue

        df = resp.overall_compare.get_data_frame()
        if df.empty:
            print(f"  [{measure_label}] 無資料\n")
            continue

        df = _to_numeric(df)
        overall = df[df["DESCRIPTION"].str.upper().str.contains("OVERALL", na=False)]
        if overall.empty:
            overall = df

        key_cols = MEASURE_KEY_COLS.get(measure, STAT_COLS)
        show_cols = [c for c in ["GROUP_SET"] + key_cols if c in overall.columns]

        print(f"── {measure_label}（{measure}，PerGame）" + "─" * 40)
        print(overall[show_cols].to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

# 預設示範：LeBron James vs Stephen Curry
DEFAULT_PAIRS = [
    ("LeBron James",  2544),
    ("Stephen Curry", 201939),
]

if __name__ == "__main__":
    # 解析命令列參數
    if len(sys.argv) >= 3:
        name_a, pid_a = sys.argv[1], _find_player_id(sys.argv[1])[0]
        name_b, pid_b = sys.argv[2], _find_player_id(sys.argv[2])[0]
        # re-fetch full name
        pid_a, name_a = _find_player_id(sys.argv[1])
        pid_b, name_b = _find_player_id(sys.argv[2])
        season = sys.argv[3] if len(sys.argv) > 3 else Season.default
    else:
        name_a, pid_a = DEFAULT_PAIRS[0]
        name_b, pid_b = DEFAULT_PAIRS[1]
        season = Season.default

    season_type = SeasonType.default

    player_ids    = [pid_a]
    vs_player_ids = [pid_b]
    names         = [name_a, name_b]

    print(f"主方  : {name_a}  (player_id={pid_a})")
    print(f"對照方: {name_b}  (player_id={pid_b})")
    print(f"球季  : {season}  |  賽季類型: {season_type}\n")

    # --- Base / PerGame 基本對比 ---
    print("=" * 65)
    print(f"  Base 數據對比（PerGame）")
    print("=" * 65 + "\n")

    resp_pg = fetch_compare(
        player_ids=player_ids,
        vs_player_ids=vs_player_ids,
        season=season,
        season_type=season_type,
        measure_type="Base",
        per_mode="PerGame",
    )

    # 1. Head-to-Head 摘要
    show_head_to_head(resp_pg, names)

    # 2. OverallCompare 完整欄位
    show_overall_compare(resp_pg, per_mode="PerGame")

    # 3. Individual 情境細分
    show_individual(resp_pg, per_mode="PerGame")

    # --- 多種 MeasureType ---
    print("=" * 65)
    print("  多維度對比（Base / Advanced / Scoring / Defense，各需一次請求）")
    print("=" * 65 + "\n")

    show_multi_measure(player_ids, vs_player_ids, names, season, season_type)

    # --- Per100Possessions（消除上場時間差異）---
    print("=" * 65)
    print("  Per100Possessions 對比（消除上場時間差異）")
    print("=" * 65 + "\n")

    try:
        resp_100 = fetch_compare(
            player_ids=player_ids,
            vs_player_ids=vs_player_ids,
            season=season,
            season_type=season_type,
            measure_type="Base",
            per_mode="Per100Possessions",
        )
        show_head_to_head(resp_100, names)
    except Exception as e:
        print(f"  Per100Possessions 取得失敗: {e}\n")
