"""
nba_api.stats.endpoints.boxscoreadvancedv3 範例程式

執行方式：
  python example/stats/box_score_advanced_v3.py                      # 本季常規賽最近場次
  python example/stats/box_score_advanced_v3.py 2024-25              # 指定球季
  python example/stats/box_score_advanced_v3.py 2024-25 Playoffs     # 季後賽
  python example/stats/box_score_advanced_v3.py 2024-25 Regular 0022401234  # 指定 game_id

  ── 預設行為：自動取得最近一場，同時附加季後賽示範 ──

BoxScoreAdvancedV3 與 BoxScoreTraditionalV3 的差異：
  - 只有 2 個 DataSet（無 TeamStarterBenchStats）
  - 欄位完全為進階效率指標，無基礎的 FGM/FGA/PTS 等計數欄
  - TeamStats 比 PlayerStats 多一個：estimatedTeamTurnoverPercentage

DataSet 總覽（共 2 個）：
  player_stats — 球員進階指標（每列一名球員）
  team_stats   — 球隊進階指標（兩隊各一列）

欄位說明（全 camelCase，以下為解釋）：
  ─── 效率評分 ───────────────────────────────────────────────────────
  estimatedOffensiveRating  — 估算進攻效率（每100回合得分，基於機率模型）
  offensiveRating           — 實際進攻效率（每100回合得分）
  estimatedDefensiveRating  — 估算防守效率（每100回合失分，越低越好）
  defensiveRating           — 實際防守效率（越低越好）
  estimatedNetRating        — 估算淨效率 = e進攻 - e防守
  netRating                 — 實際淨效率（正值 = 上場時球隊有利）

  ─── 傳球 / 失誤 ────────────────────────────────────────────────────
  assistPercentage          — 助攻率（此球員助攻占球隊出手的比例）
  assistToTurnover          — 助攻 / 失誤比（越高越好）
  assistRatio               — 助攻比率（每100回合助攻次數）
  turnoverRatio             — 失誤比率（每100回合失誤次數，越低越好）
  estimatedTeamTurnoverPercentage  — 估算球隊整體失誤率（僅 TeamStats）

  ─── 籃板 ───────────────────────────────────────────────────────────
  offensiveReboundPercentage — 進攻籃板率（%）
  defensiveReboundPercentage — 防守籃板率（%）
  reboundPercentage          — 總籃板率（%）

  ─── 投籃效率 ────────────────────────────────────────────────────────
  effectiveFieldGoalPercentage — eFG%（考量三分額外價值的命中率）
  trueShootingPercentage       — TS%（真實命中率，含罰球，越高越好）

  ─── 使用率 / 節奏 ───────────────────────────────────────────────────
  usagePercentage           — 使用率（球員持球進攻佔球隊回合的比例）
  estimatedUsagePercentage  — 估算使用率
  estimatedPace             — 估算節奏（每48分鐘回合數）
  pace                      — 實際節奏
  pacePer40                 — 每40分鐘節奏
  possessions               — 回合數

  ─── 綜合評估 ────────────────────────────────────────────────────────
  PIE                       — Player Impact Estimate（球員整體影響力估算）
                              > 10% = 正向影響；< 5% = 低影響
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.boxscoreadvancedv3 import BoxScoreAdvancedV3
from nba_api.stats.endpoints.leaguegamefinder import LeagueGameFinder
from nba_api.stats.library.parameters import Season

TIMEOUT     = 60
RETRIES     = 3
RETRY_DELAY = 5

STR_COLS = {
    "gameId", "teamId", "teamCity", "teamName", "teamTricode", "teamSlug",
    "personId", "firstName", "familyName", "nameI", "playerSlug",
    "position", "comment", "jerseyNum", "minutes",
}

# PlayerStats 核心顯示欄位（分組顯示）
PLAYER_CORE = [
    "nameI", "teamTricode", "position", "minutes",
    "offensiveRating", "defensiveRating", "netRating",
    "trueShootingPercentage", "effectiveFieldGoalPercentage",
    "usagePercentage", "PIE",
]
PLAYER_PASS = [
    "nameI", "teamTricode",
    "assistPercentage", "assistToTurnover", "assistRatio", "turnoverRatio",
]
PLAYER_REB = [
    "nameI", "teamTricode",
    "offensiveReboundPercentage", "defensiveReboundPercentage", "reboundPercentage",
]
PLAYER_PACE = [
    "nameI", "teamTricode",
    "pace", "pacePer40", "estimatedPace", "possessions",
]

# TeamStats 核心顯示欄位
TEAM_CORE = [
    "teamTricode",
    "offensiveRating", "defensiveRating", "netRating",
    "trueShootingPercentage", "effectiveFieldGoalPercentage",
    "assistToTurnover", "turnoverRatio",
    "reboundPercentage", "pace", "possessions", "PIE",
]


# ---------------------------------------------------------------------------
# Game ID 取得
# ---------------------------------------------------------------------------

def find_game_id(
    season: str = Season.default,
    season_type: str = "Regular Season",
) -> str | None:
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
# BoxScoreAdvancedV3 取得
# ---------------------------------------------------------------------------

def fetch_boxscore_adv(
    game_id: str,
    start_period: int = 0,
    end_period: int = 0,
    range_type: int = 0,
    start_range: int = 0,
    end_range: int = 0,
) -> BoxScoreAdvancedV3:
    for attempt in range(1, RETRIES + 1):
        try:
            return BoxScoreAdvancedV3(
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


def _played(df: pd.DataFrame) -> pd.DataFrame:
    """過濾未出場球員。"""
    if df.empty:
        return df
    return df[df["minutes"].notna() & (~df["minutes"].isin(
        ["PT00M00.00S", "0:00", "00:00", ""]
    ))]


# ---------------------------------------------------------------------------
# 1. 球員進階數據（PlayerStats）
# ---------------------------------------------------------------------------

def show_player_stats(resp: BoxScoreAdvancedV3, label: str = "") -> None:
    """
    按球隊分組印出所有球員的核心進階指標。
    先發（position 非空）在前，替補在後。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        print(f"── {label}PlayerStats：無資料\n")
        return

    avail = [c for c in PLAYER_CORE if c in df.columns]
    teams = df["teamTricode"].unique()

    print(f"── {label}球員進階數據（核心效率指標）" + "─" * 32)
    for team in teams:
        team_df  = df[df["teamTricode"] == team].copy()
        starters = team_df[team_df["position"].notna() & (team_df["position"] != "")]
        bench    = team_df[team_df["position"].isna()  | (team_df["position"] == "")]
        ordered  = pd.concat([starters, bench])
        print(f"\n  {team}（先發: {len(starters)} / 替補: {len(bench)}）")
        print(ordered[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 球隊進階數據對比（TeamStats）
# ---------------------------------------------------------------------------

def show_team_stats(resp: BoxScoreAdvancedV3, label: str = "") -> None:
    """兩隊進階指標並排，標示哪方領先（含 estimatedTeamTurnoverPercentage）。"""
    df = _clean(resp.team_stats.get_data_frame())
    if df.empty:
        print(f"── {label}TeamStats：無資料\n")
        return

    # 較低越好的指標
    lower_better = {
        "defensiveRating", "estimatedDefensiveRating",
        "turnoverRatio", "estimatedTeamTurnoverPercentage",
    }

    stat_cols = [c for c in df.columns
                 if c not in STR_COLS and c not in ("gameId",)]

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
                if col in lower_better:
                    winner = team_a if v_a < v_b else (team_b if v_b < v_a else "—")
                else:
                    winner = team_a if v_a > v_b else (team_b if v_b > v_a else "—")
            rows.append({
                "進階指標": col,
                team_a:    round(float(v_a), 4) if pd.notna(v_a) else None,
                team_b:    round(float(v_b), 4) if pd.notna(v_b) else None,
                "領先方":   winner,
            })

        result = pd.DataFrame(rows)
        print(f"── {label}球隊進階數據對比（TeamStats）" + "─" * 28)
        print(result.to_string(index=False))
    else:
        avail = [c for c in TEAM_CORE if c in df.columns]
        print(f"── {label}球隊進階數據（TeamStats）" + "─" * 34)
        print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 頂尖影響力球員（PIE / netRating / usagePercentage）
# ---------------------------------------------------------------------------

def show_impact_players(resp: BoxScoreAdvancedV3, label: str = "",
                        min_min_threshold: str = "PT05M") -> None:
    """
    找出本場各進階指標最突出的球員。
    min_min_threshold：最低上場時間過濾（預設 5 分鐘）。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    played = _played(df)
    if played.empty:
        played = df

    print(f"── {label}本場進階影響力最佳球員 " + "─" * 38)
    metrics = [
        ("PIE",                   "球員影響力（PIE）",     False),
        ("netRating",             "淨效率",               False),
        ("trueShootingPercentage","真實命中率（TS%）",     False),
        ("usagePercentage",       "使用率",               False),
        ("assistToTurnover",      "助攻/失誤比",          False),
        ("defensiveRating",       "防守效率（越低越好）",  True),
    ]
    for col, lbl, lower in metrics:
        if col not in played.columns:
            continue
        sub = played[played[col].notna()]
        if sub.empty:
            continue
        idx  = sub[col].idxmin() if lower else sub[col].idxmax()
        row  = sub.loc[idx]
        name = row.get("nameI", "")
        team = row.get("teamTricode", "")
        val  = row[col]
        print(f"  {lbl:<24}: {val:>8.4f}  {name}  ({team})")
    print()


# ---------------------------------------------------------------------------
# 4. 傳球 / 失誤 進階數據
# ---------------------------------------------------------------------------

def show_passing_turnover(resp: BoxScoreAdvancedV3, label: str = "") -> None:
    """assistToTurnover / assistRatio / turnoverRatio 詳細分析。"""
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return
    played = _played(df)
    avail  = [c for c in PLAYER_PASS if c in played.columns]
    if len(avail) <= 2:
        return
    # 依 assistToTurnover 降序排列
    played_sorted = played.dropna(subset=["assistToTurnover"]).sort_values(
        "assistToTurnover", ascending=False
    )
    print(f"── {label}傳球 / 失誤進階數據（依助攻失誤比排序）" + "─" * 18)
    print(played_sorted[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 籃板進階數據
# ---------------------------------------------------------------------------

def show_rebounding_advanced(resp: BoxScoreAdvancedV3, label: str = "") -> None:
    """各球員籃板率（進攻 / 防守 / 總計）。"""
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return
    played = _played(df)
    avail  = [c for c in PLAYER_REB if c in played.columns]
    if len(avail) <= 2:
        return
    played_sorted = played.dropna(subset=["reboundPercentage"]).sort_values(
        "reboundPercentage", ascending=False
    )
    print(f"── {label}籃板率（依總籃板率排序）" + "─" * 40)
    print(played_sorted[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 節次進階數據（start_period / end_period）
# ---------------------------------------------------------------------------

def show_quarter_advanced(game_id: str, quarter: int, label: str = "") -> None:
    """取得單節的進階數據。"""
    try:
        resp = fetch_boxscore_adv(
            game_id=game_id,
            start_period=quarter,
            end_period=quarter,
            range_type=0,
        )
    except Exception as e:
        print(f"  第 {quarter} 節進階數據取得失敗: {e}\n")
        return

    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        print(f"── {label}第 {quarter} 節進階：無資料\n")
        return

    period_label = f"Q{quarter}" if quarter <= 4 else f"OT{quarter - 4}"
    played = _played(df)
    target = played if not played.empty else df

    q_cols = ["nameI", "teamTricode", "minutes",
              "offensiveRating", "defensiveRating", "netRating",
              "trueShootingPercentage", "usagePercentage", "PIE"]
    avail = [c for c in q_cols if c in target.columns]

    print(f"── {label}{period_label} 節次進階數據 " + "─" * 40)
    print(target[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 常規賽 vs 季後賽球隊進階對比
# ---------------------------------------------------------------------------

def show_season_type_comparison(
    reg_resp: BoxScoreAdvancedV3,
    po_resp: BoxScoreAdvancedV3,
    team_tricode: str,
) -> None:
    """
    指定球隊在常規賽場次 vs 季後賽場次的進階數據並排。
    兩場為不同場次，僅示範結構差異。
    """
    compare_cols = [
        "offensiveRating", "defensiveRating", "netRating",
        "trueShootingPercentage", "effectiveFieldGoalPercentage",
        "assistToTurnover", "turnoverRatio", "reboundPercentage",
        "pace", "possessions", "PIE",
    ]

    rows = []
    for lbl, resp in [("常規賽（示範場次）", reg_resp),
                      ("季後賽（示範場次）", po_resp)]:
        df = _clean(resp.team_stats.get_data_frame())
        if df.empty:
            continue
        team_row = df[df["teamTricode"] == team_tricode]
        if team_row.empty:
            team_row = df.iloc[[0]]
        r = team_row.iloc[0]
        entry = {"賽事": lbl, "球隊": r.get("teamTricode", "")}
        for col in compare_cols:
            v = r.get(col)
            entry[col] = round(float(v), 4) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("賽事")
    print(f"── 常規賽 vs 季後賽球隊進階對比（{team_tricode}）" + "─" * 18)
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

    # ── 取得 game_id ──────────────────────────────────────────────────────
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

    # ── 全場進階 Boxscore ─────────────────────────────────────────────────
    print("=" * 65)
    print(f"  全場進階 Boxscore  game_id={game_id}  ({season_type})")
    print("=" * 65 + "\n")

    resp = fetch_boxscore_adv(game_id=game_id)

    show_player_stats(resp)
    show_team_stats(resp)
    show_impact_players(resp)
    show_passing_turnover(resp)
    show_rebounding_advanced(resp)

    # ── 節次進階（Q1 / Q4）───────────────────────────────────────────────
    print("=" * 65)
    print("  節次進階數據（start_period / end_period 參數示範）")
    print("=" * 65 + "\n")

    for q in [1, 4]:
        show_quarter_advanced(game_id, quarter=q)

    # ── 季後賽 ────────────────────────────────────────────────────────────
    if season_type == "Regular Season":
        print("=" * 65)
        print(f"  季後賽進階 Boxscore 示範  ({season} Playoffs)")
        print("=" * 65 + "\n")

        print(f"透過 LeagueGameFinder 尋找 {season} Playoffs 場次…")
        po_game_id = find_game_id(season=season, season_type="Playoffs")

        if po_game_id:
            print(f"找到 game_id: {po_game_id}\n")
            po_resp = fetch_boxscore_adv(game_id=po_game_id)

            show_player_stats(po_resp,       label="季後賽 ")
            show_team_stats(po_resp,         label="季後賽 ")
            show_impact_players(po_resp,     label="季後賽 ")
            show_passing_turnover(po_resp,   label="季後賽 ")
            show_rebounding_advanced(po_resp, label="季後賽 ")

            for q in [1, 4]:
                show_quarter_advanced(po_game_id, quarter=q, label="季後賽 ")

            # 常規賽 vs 季後賽球隊進階並排
            reg_df = _clean(resp.team_stats.get_data_frame())
            if not reg_df.empty:
                home_team = reg_df.iloc[0]["teamTricode"]
                print("=" * 65)
                print(f"  常規賽 vs 季後賽球隊進階數據並排（{home_team}）")
                print("=" * 65 + "\n")
                show_season_type_comparison(resp, po_resp, home_team)

        else:
            print(f"  {season} 無季後賽記錄（可能球季尚未結束）。\n")
