"""
nba_api.stats.endpoints.boxscoretraditionalv3 範例程式

執行方式：
  python example/stats/box_score_traditional_v3.py                      # 本季常規賽最近場次
  python example/stats/box_score_traditional_v3.py 2024-25              # 指定球季
  python example/stats/box_score_traditional_v3.py 2024-25 Playoffs     # 季後賽
  python example/stats/box_score_traditional_v3.py 2024-25 Regular 0022401234  # 指定 game_id

  ── 預設行為：自動取得最近一場，同時附加季後賽示範 ──

BoxScoreTraditionalV3 是 V2 的現行版本（2025-26 起 V2 已廢棄）：
  - 欄位改用 camelCase（如 points / reboundsTotal / assists）
  - 新增球員專屬欄位：jerseyNum / nameI / playerSlug
  - 解析器走 V3 Parser（boxScoreTraditional JSON 結構）

BoxScoreTraditionalV3 參數：
  game_id      — 比賽 ID（必填，10 位數字字串）
                 格式：0022XYYYYY = 常規賽（20XX-XX 球季，第 YYYYY 場）
                       0042XYYYYY = 季後賽
  start_period — 起始節次（0=全場，1-4=指定節，預設 0）
  end_period   — 結束節次（預設 0=全場）
  range_type   — 0=整節/整場（預設）
  start_range  — 時間範圍起點（0=節次起點，1/10 秒單位）
  end_range    — 時間範圍終點（0=節次終點）

DataSet 總覽（共 3 個）：
  player_stats              — 球員個人數據
  team_stats                — 球隊整場累計數據（兩隊各一列）
  team_starter_bench_stats  — 先發 / 替補分組數據

PlayerStats 欄位（V3 camelCase）：
  gameId / teamId / teamCity / teamName / teamTricode / teamSlug
  personId / firstName / familyName / nameI / playerSlug
  position       — 位置（C/F/G；空白 = 替補未出場）
  comment        — 備註（DNP 原因等）
  jerseyNum      — 球衣號碼（V2 無此欄）
  minutes        — 上場時間
  fieldGoalsMade/Attempted/Percentage
  threePointersMade/Attempted/Percentage
  freeThrowsMade/Attempted/Percentage
  reboundsOffensive / reboundsDefensive / reboundsTotal
  assists / steals / blocks / turnovers / foulsPersonal
  points / plusMinusPoints

TeamStarterBenchStats 特殊說明：
  startersBench  — "Starters" / "Bench"
  ※ 此 DataSet 無 plusMinusPoints（TeamStats 和 PlayerStats 有）
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.boxscoretraditionalv3 import BoxScoreTraditionalV3
from nba_api.stats.endpoints.leaguegamefinder import LeagueGameFinder
from nba_api.stats.library.parameters import Season

TIMEOUT     = 60
RETRIES     = 3
RETRY_DELAY = 5

# V3 PlayerStats 顯示欄位
PLAYER_COLS = [
    "teamTricode", "jerseyNum", "nameI", "position",
    "minutes", "points", "reboundsTotal", "assists",
    "steals", "blocks", "turnovers",
    "fieldGoalsMade", "fieldGoalsAttempted", "fieldGoalsPercentage",
    "threePointersMade", "threePointersAttempted", "threePointersPercentage",
    "freeThrowsMade", "freeThrowsAttempted", "freeThrowsPercentage",
    "plusMinusPoints",
]
TEAM_COLS = [
    "teamTricode",
    "minutes", "points", "reboundsTotal", "assists",
    "steals", "blocks", "turnovers",
    "fieldGoalsMade", "fieldGoalsAttempted", "fieldGoalsPercentage",
    "threePointersMade", "threePointersAttempted", "threePointersPercentage",
    "freeThrowsMade", "freeThrowsAttempted", "freeThrowsPercentage",
    "plusMinusPoints",
]
SB_COLS = [
    "teamTricode", "startersBench",
    "minutes", "points", "reboundsTotal", "assists",
    "fieldGoalsMade", "fieldGoalsAttempted", "fieldGoalsPercentage",
    "threePointersMade", "threePointersAttempted", "threePointersPercentage",
]
STR_COLS = {
    "gameId", "teamId", "teamCity", "teamName", "teamTricode", "teamSlug",
    "personId", "firstName", "familyName", "nameI", "playerSlug",
    "position", "comment", "jerseyNum", "startersBench", "minutes",
}


# ---------------------------------------------------------------------------
# Game ID 取得
# ---------------------------------------------------------------------------

def find_game_id(
    season: str = Season.default,
    season_type: str = "Regular Season",
) -> str | None:
    """透過 LeagueGameFinder 取得指定球季、賽季類型的最近一場 game_id。"""
    for attempt in range(1, RETRIES + 1):
        try:
            finder = LeagueGameFinder(
                player_or_team_abbreviation="T",
                season_nullable=season,
                season_type_nullable=season_type,
                timeout=TIMEOUT,
            )
            df = finder.league_game_finder_results.get_data_frame()
            if df.empty:
                return None
            df = df.sort_values("GAME_DATE", ascending=False)
            unique_ids = df["GAME_ID"].unique()
            return str(unique_ids[0]) if len(unique_ids) >= 1 else None
        except Exception as e:
            if attempt == RETRIES:
                print(f"  LeagueGameFinder 失敗: {e}")
                return None
            time.sleep(RETRY_DELAY)


# ---------------------------------------------------------------------------
# BoxScoreTraditionalV3 取得
# ---------------------------------------------------------------------------

def fetch_boxscore_v3(
    game_id: str,
    start_period: int = 0,
    end_period: int = 0,
    range_type: int = 0,
    start_range: int = 0,
    end_range: int = 0,
) -> BoxScoreTraditionalV3:
    """取得 BoxScoreTraditionalV3，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return BoxScoreTraditionalV3(
                game_id=str(game_id),
                start_period=str(start_period),
                end_period=str(end_period),
                range_type=str(range_type),
                start_range=str(start_range),
                end_range=str(end_range),
                timeout=TIMEOUT,
            )
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    num_cols = [c for c in df.columns if c not in STR_COLS]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 1. 球員個人數據（PlayerStats）
# ---------------------------------------------------------------------------

def show_player_stats(resp: BoxScoreTraditionalV3, label: str = "") -> None:
    """
    印出兩隊所有球員的數據。
    position：G=後衛 / F=前鋒 / C=中鋒；空白=替補或未出場。
    nameI：縮寫姓名（如 "L. James"），適合表格顯示。
    jerseyNum：球衣號碼（V2 無此欄）。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        print(f"── {label}PlayerStats：無資料\n")
        return

    avail = [c for c in PLAYER_COLS if c in df.columns]
    teams = df["teamTricode"].unique()

    print(f"── {label}球員個人數據（PlayerStats）" + "─" * 30)
    for team in teams:
        team_df = df[df["teamTricode"] == team].copy()
        # 先發（position 非空）在前，替補在後
        starters = team_df[team_df["position"].notna() & (team_df["position"] != "")]
        bench    = team_df[team_df["position"].isna()  | (team_df["position"] == "")]
        ordered  = pd.concat([starters, bench])
        print(f"\n  {team}（先發: {len(starters)} / 替補: {len(bench)}）")
        print(ordered[avail].to_string(index=False))

    print()


# ---------------------------------------------------------------------------
# 2. 球隊總數據（TeamStats）
# ---------------------------------------------------------------------------

def show_team_stats(resp: BoxScoreTraditionalV3, label: str = "") -> None:
    """印出兩隊整場累計數據，並列出各項領先方。"""
    df = _clean(resp.team_stats.get_data_frame())
    if df.empty:
        print(f"── {label}TeamStats：無資料\n")
        return

    stat_cols = [c for c in TEAM_COLS if c not in ("teamTricode", "minutes")]

    if len(df) == 2:
        row_a  = df.iloc[0]
        row_b  = df.iloc[1]
        team_a = row_a["teamTricode"]
        team_b = row_b["teamTricode"]

        rows = []
        for col in stat_cols:
            v_a = row_a.get(col)
            v_b = row_b.get(col)
            if pd.isna(v_a) and pd.isna(v_b):
                continue
            winner = ""
            if pd.notna(v_a) and pd.notna(v_b):
                lower_better = col == "turnovers"
                if lower_better:
                    winner = team_a if v_a < v_b else (team_b if v_b < v_a else "—")
                else:
                    winner = team_a if v_a > v_b else (team_b if v_b > v_a else "—")
            rows.append({
                "統計項目": col,
                team_a:    round(float(v_a), 3) if pd.notna(v_a) else None,
                team_b:    round(float(v_b), 3) if pd.notna(v_b) else None,
                "領先方":   winner,
            })

        result = pd.DataFrame(rows)
        print(f"── {label}球隊總數據對比（TeamStats）" + "─" * 32)
        print(result.to_string(index=False))
    else:
        avail = [c for c in TEAM_COLS if c in df.columns]
        print(f"── {label}球隊總數據（TeamStats）" + "─" * 36)
        print(df[avail].to_string(index=False))

    print()


# ---------------------------------------------------------------------------
# 3. 先發 / 替補分組（TeamStarterBenchStats）
# ---------------------------------------------------------------------------

def show_starter_bench(resp: BoxScoreTraditionalV3, label: str = "") -> None:
    """先發五虎 vs 替補陣容累計數據對比。startersBench = "Starters" / "Bench"。"""
    df = _clean(resp.team_starter_bench_stats.get_data_frame())
    if df.empty:
        print(f"── {label}TeamStarterBenchStats：無資料\n")
        return

    avail = [c for c in SB_COLS if c in df.columns]
    print(f"── {label}先發 vs 替補（TeamStarterBenchStats）" + "─" * 22)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 節次 Boxscore（start_period / end_period 示範）
# ---------------------------------------------------------------------------

def show_quarter_boxscore(game_id: str, quarter: int, label: str = "") -> None:
    """
    取得單節球員數據。
    quarter：1-4 = 各節，5-10 = 加時（OT1-OT6）
    """
    try:
        resp = fetch_boxscore_v3(
            game_id=game_id,
            start_period=quarter,
            end_period=quarter,
            range_type=0,
        )
    except Exception as e:
        print(f"  第 {quarter} 節 boxscore 取得失敗: {e}\n")
        return

    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        print(f"── {label}第 {quarter} 節：無資料（可能無加時）\n")
        return

    period_label = f"Q{quarter}" if quarter <= 4 else f"OT{quarter - 4}"
    q_cols = ["teamTricode", "jerseyNum", "nameI", "minutes",
              "points", "reboundsTotal", "assists",
              "fieldGoalsMade", "fieldGoalsAttempted", "fieldGoalsPercentage",
              "turnovers", "plusMinusPoints"]
    avail = [c for c in q_cols if c in df.columns]

    # 只顯示有上場的球員
    played = df[df["minutes"].notna() & (~df["minutes"].isin(["PT00M00.00S", "0:00", ""]))]
    target = played if not played.empty else df

    print(f"── {label}{period_label} 節次 Boxscore " + "─" * 40)
    print(target[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 頂尖表現者快速摘要
# ---------------------------------------------------------------------------

def show_top_performers(resp: BoxScoreTraditionalV3, label: str = "") -> None:
    """從 PlayerStats 快速找出得分 / 籃板 / 助攻最高的球員。"""
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    # 過濾未出場球員（minutes 為空或 PT00M00.00S）
    played = df[df["minutes"].notna() & (~df["minutes"].isin(["PT00M00.00S", "0:00", "00:00", ""]))]
    if played.empty:
        played = df

    print(f"── {label}比賽頂尖表現者 " + "─" * 46)
    for stat, lbl in [("points","得分"), ("reboundsTotal","籃板"), ("assists","助攻"),
                       ("steals","抄截"), ("blocks","火鍋"), ("plusMinusPoints","正負值")]:
        if stat not in played.columns:
            continue
        sub = played[played[stat].notna()]
        if sub.empty:
            continue
        idx  = sub[stat].idxmax()
        row  = sub.loc[idx]
        name = row.get("nameI", row.get("firstName", ""))
        team = row.get("teamTricode", "")
        val  = row[stat]
        print(f"  {lbl:<8}: {val:>6.1f}  {name}  ({team})")
    print()


# ---------------------------------------------------------------------------
# 6. 常規賽 vs 季後賽球隊數據並排
# ---------------------------------------------------------------------------

def show_season_type_comparison(
    reg_resp: BoxScoreTraditionalV3,
    po_resp: BoxScoreTraditionalV3,
    team_tricode: str,
) -> None:
    """
    指定球隊在常規賽場次 vs 季後賽場次的累計數據並排。
    兩場比賽通常不是同一場，僅為示範如何比對兩種賽事的 BoxScore 結構。
    """
    compare_cols = ["fieldGoalsPercentage", "threePointersPercentage",
                    "freeThrowsPercentage", "reboundsTotal", "assists",
                    "turnovers", "points", "plusMinusPoints"]

    rows = []
    for season_lbl, resp in [("常規賽（示範場次）", reg_resp),
                              ("季後賽（示範場次）", po_resp)]:
        df = _clean(resp.team_stats.get_data_frame())
        if df.empty:
            continue
        team_row = df[df["teamTricode"] == team_tricode]
        if team_row.empty:
            team_row = df.iloc[[0]]
        r = team_row.iloc[0]
        entry = {"賽事": season_lbl, "球隊": r.get("teamTricode", "")}
        for col in compare_cols:
            v = r.get(col)
            entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("賽事")
    print(f"── 常規賽 vs 季後賽球隊數據並排（{team_tricode}）" + "─" * 20)
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    season      = sys.argv[1] if len(sys.argv) > 1 else Season.default
    st_arg      = sys.argv[2] if len(sys.argv) > 2 else "Regular"
    manual_id   = sys.argv[3] if len(sys.argv) > 3 else None
    season_type = "Playoffs" if "Playoff" in st_arg else "Regular Season"

    print(f"球季     : {season}")
    print(f"賽季類型 : {season_type}\n")

    # ── 取得常規賽 game_id ────────────────────────────────────────────────
    if manual_id:
        game_id = manual_id
        print(f"使用指定 game_id: {game_id}\n")
    else:
        print(f"透過 LeagueGameFinder 尋找 {season} {season_type} 場次…")
        game_id = find_game_id(season=season, season_type=season_type)
        if not game_id:
            print("找不到場次，請手動指定 game_id。")
            sys.exit(1)
        print(f"找到 game_id: {game_id}\n")

    # ── 全場 Boxscore ─────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  全場 Boxscore  game_id={game_id}  ({season_type})")
    print("=" * 65 + "\n")

    resp = fetch_boxscore_v3(game_id=game_id)

    show_player_stats(resp)
    show_team_stats(resp)
    show_starter_bench(resp)
    show_top_performers(resp)

    # ── 節次 Boxscore（Q1 / Q4）─────────────────────────────────────────
    print("=" * 65)
    print("  節次 Boxscore（start_period / end_period 參數示範）")
    print("=" * 65 + "\n")

    for q in [1, 4]:
        show_quarter_boxscore(game_id, quarter=q)

    # ── 季後賽（另找一場） ────────────────────────────────────────────────
    if season_type == "Regular Season":
        print("=" * 65)
        print(f"  季後賽 Boxscore 示範  ({season} Playoffs)")
        print("=" * 65 + "\n")

        print(f"透過 LeagueGameFinder 尋找 {season} Playoffs 場次…")
        po_game_id = find_game_id(season=season, season_type="Playoffs")

        if po_game_id:
            print(f"找到 game_id: {po_game_id}\n")
            po_resp = fetch_boxscore_v3(game_id=po_game_id)

            show_player_stats(po_resp,  label="季後賽 ")
            show_team_stats(po_resp,    label="季後賽 ")
            show_starter_bench(po_resp, label="季後賽 ")
            show_top_performers(po_resp, label="季後賽 ")

            # 節次 Boxscore
            for q in [1, 4]:
                show_quarter_boxscore(po_game_id, quarter=q, label="季後賽 ")

            # 常規賽 vs 季後賽並排（取兩場主隊對比）
            reg_df = _clean(resp.team_stats.get_data_frame())
            if not reg_df.empty:
                home_team = reg_df.iloc[0]["teamTricode"]
                print("=" * 65)
                print(f"  常規賽 vs 季後賽球隊數據並排示範（{home_team}）")
                print("=" * 65 + "\n")
                show_season_type_comparison(resp, po_resp, home_team)

        else:
            print(f"  {season} 無季後賽記錄（可能球季尚未結束）。\n")
