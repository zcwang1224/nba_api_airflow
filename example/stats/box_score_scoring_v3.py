"""
nba_api.stats.endpoints.boxscorescoringv3 範例程式

執行方式：
  python example/stats/box_score_scoring_v3.py                      # 本季常規賽最近場次
  python example/stats/box_score_scoring_v3.py 2024-25              # 指定球季
  python example/stats/box_score_scoring_v3.py 2024-25 Playoffs     # 季後賽
  python example/stats/box_score_scoring_v3.py 2024-25 Regular 0022401234  # 指定 game_id

  ── 預設行為：自動取得最近一場，同時附加季後賽示範 ──

BoxScoreScoringV3 與其他 V3 BoxScore 的差異：
  - 只有 2 個 DataSet（無 TeamStarterBenchStats）
  - 欄位全為「得分來源百分比」，不含計數型數據（如 FGM、PTS）
  - 適合分析球員/球隊的得分結構（偏禁區 or 三分 or 快攻 etc.）

DataSet 總覽（共 2 個）：
  player_stats — 球員得分分布（每列一名球員）
  team_stats   — 球隊得分分布（兩隊各一列）

欄位說明（全 camelCase，值為 0.0–1.0 的小數）：

  ─── 出手分布 ────────────────────────────────────────────────────────
  percentageFieldGoalsAttempted2pt  — 二分球出手佔全部出手的比例
  percentageFieldGoalsAttempted3pt  — 三分球出手佔全部出手的比例

  ─── 得分來源比例（各項佔總得分的百分比）───────────────────────────
  percentagePoints2pt               — 二分球得分佔比
  percentagePointsMidrange2pt       — 中距離二分得分佔比
  percentagePoints3pt               — 三分球得分佔比（含三分加成）
  percentagePointsFastBreak         — 快攻得分佔比
  percentagePointsFreeThrow         — 罰球得分佔比
  percentagePointsOffTurnovers      — 對手失誤後得分佔比
  percentagePointsPaint             — 禁區得分佔比

  ─── 助攻依賴度 ──────────────────────────────────────────────────────
  percentageAssisted2pt             — 二分球命中中有助攻的比例
  percentageUnassisted2pt           — 二分球命中中無助攻的比例（個人創造）
  percentageAssisted3pt             — 三分球命中中有助攻的比例
  percentageUnassisted3pt           — 三分球命中中無助攻的比例（個人創造）
  percentageAssistedFGM             — 所有命中中有助攻的比例
  percentageUnassistedFGM           — 所有命中中無助攻的比例（自創得分）
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.boxscorescoringv3 import BoxScoreScoringV3
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

# PlayerStats 核心顯示欄位
PLAYER_SHOT_DIST = [
    "nameI", "teamTricode", "position", "minutes",
    "percentageFieldGoalsAttempted2pt", "percentageFieldGoalsAttempted3pt",
]
PLAYER_SCORE_SRC = [
    "nameI", "teamTricode",
    "percentagePoints2pt", "percentagePointsMidrange2pt",
    "percentagePoints3pt", "percentagePointsFastBreak",
    "percentagePointsFreeThrow", "percentagePointsOffTurnovers",
    "percentagePointsPaint",
]
PLAYER_ASSIST_DEP = [
    "nameI", "teamTricode",
    "percentageAssistedFGM", "percentageUnassistedFGM",
    "percentageAssisted2pt",  "percentageUnassisted2pt",
    "percentageAssisted3pt",  "percentageUnassisted3pt",
]

# TeamStats 核心顯示欄位
TEAM_CORE = [
    "teamTricode",
    "percentageFieldGoalsAttempted2pt", "percentageFieldGoalsAttempted3pt",
    "percentagePoints2pt", "percentagePointsMidrange2pt",
    "percentagePoints3pt", "percentagePointsFastBreak",
    "percentagePointsFreeThrow", "percentagePointsOffTurnovers",
    "percentagePointsPaint",
    "percentageAssistedFGM", "percentageUnassistedFGM",
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
# BoxScoreScoringV3 取得
# ---------------------------------------------------------------------------

def fetch_boxscore_scoring(
    game_id: str,
    start_period: int = 0,
    end_period: int = 0,
    range_type: int = 0,
    start_range: int = 0,
    end_range: int = 0,
) -> BoxScoreScoringV3:
    for attempt in range(1, RETRIES + 1):
        try:
            return BoxScoreScoringV3(
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


def _pct(val: float) -> str:
    """將 0-1 小數格式化為百分比字串。"""
    if pd.isna(val):
        return "  N/A"
    return f"{val * 100:5.1f}%"


# ---------------------------------------------------------------------------
# 1. 球員出手分布（二分 vs 三分出手佔比）
# ---------------------------------------------------------------------------

def show_player_shot_distribution(resp: BoxScoreScoringV3, label: str = "") -> None:
    """
    印出每位球員的出手分布（二分 vs 三分佔比）。
    兩者加總應接近 100%（差值為其他情況，如未出手）。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        print(f"── {label}PlayerStats：無資料\n")
        return

    played = _played(df)
    if played.empty:
        played = df

    avail = [c for c in PLAYER_SHOT_DIST if c in played.columns]
    teams = played["teamTricode"].unique()

    print(f"── {label}球員出手分布（二分 / 三分出手佔比）" + "─" * 26)
    for team in teams:
        team_df  = played[played["teamTricode"] == team].copy()
        starters = team_df[team_df["position"].notna() & (team_df["position"] != "")]
        bench    = team_df[team_df["position"].isna()  | (team_df["position"] == "")]
        ordered  = pd.concat([starters, bench])
        print(f"\n  {team}（先發: {len(starters)} / 替補: {len(bench)}）")
        print(ordered[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 球員得分來源（Points by Source）
# ---------------------------------------------------------------------------

def show_player_scoring_sources(resp: BoxScoreScoringV3, label: str = "") -> None:
    """
    印出每位球員的得分來源比例，各項加總等於 100%。
    欄位含義：
      Paint         — 禁區得分（靠近籃框）
      Midrange2pt   — 中距離跳投
      3pt           — 三分球
      FastBreak     — 快攻
      FreeThrow     — 罰球
      OffTurnovers  — 對手失誤後得分（反擊）
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    played = _played(df)
    if played.empty:
        played = df

    avail = [c for c in PLAYER_SCORE_SRC if c in played.columns]

    print(f"── {label}球員得分來源分布（各項佔總得分 %）" + "─" * 26)
    print(played[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 球員助攻依賴度（Assisted vs Unassisted）
# ---------------------------------------------------------------------------

def show_player_assist_dependency(resp: BoxScoreScoringV3, label: str = "") -> None:
    """
    分析每位球員的自創得分能力。
    percentageUnassistedFGM 越高 → 越能自主創造出手（球星特質）。
    percentageAssistedFGM   越高 → 越依賴隊友傳球創造機會（終結者/射手特質）。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    played = _played(df)
    if played.empty:
        played = df

    avail = [c for c in PLAYER_ASSIST_DEP if c in played.columns]

    # 依「無助攻命中佔比」降序：越高代表越能自主創造
    col_sort = "percentageUnassistedFGM"
    if col_sort in played.columns:
        played = played.dropna(subset=[col_sort]).sort_values(col_sort, ascending=False)

    print(f"── {label}球員助攻依賴度（依自創得分佔比排序）" + "─" * 22)
    print(played[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 球隊得分結構對比（TeamStats）
# ---------------------------------------------------------------------------

def show_team_stats(resp: BoxScoreScoringV3, label: str = "") -> None:
    """
    兩隊得分結構並排，標示各項領先方。
    禁區得分佔比高 → 靠近籃框進攻；三分佔比高 → 外線主導。
    """
    df = _clean(resp.team_stats.get_data_frame())
    if df.empty:
        print(f"── {label}TeamStats：無資料\n")
        return

    stat_cols = [c for c in df.columns if c not in STR_COLS and c != "gameId"]

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
                winner = team_a if v_a > v_b else (team_b if v_b > v_a else "—")
            rows.append({
                "得分指標":  col,
                team_a:      _pct(v_a),
                team_b:      _pct(v_b),
                "較高方":    winner,
            })

        result = pd.DataFrame(rows)
        print(f"── {label}球隊得分結構對比（TeamStats）" + "─" * 30)
        print(result.to_string(index=False))
    else:
        avail = [c for c in TEAM_CORE if c in df.columns]
        print(f"── {label}球隊得分結構（TeamStats）" + "─" * 36)
        print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 得分結構亮點（最偏重某種得分方式的球員）
# ---------------------------------------------------------------------------

def show_scoring_highlights(resp: BoxScoreScoringV3, label: str = "") -> None:
    """找出本場各得分方式比例最突出的球員。"""
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    played = _played(df)
    if played.empty:
        played = df

    metrics = [
        ("percentagePointsPaint",        "禁區得分佔比最高"),
        ("percentagePoints3pt",          "三分得分佔比最高"),
        ("percentagePointsFastBreak",    "快攻得分佔比最高"),
        ("percentagePointsFreeThrow",    "罰球得分佔比最高"),
        ("percentagePointsOffTurnovers", "反擊得分佔比最高"),
        ("percentageUnassistedFGM",      "自創命中佔比最高（最能單打）"),
        ("percentageAssistedFGM",        "有助攻命中佔比最高（最依賴傳球）"),
    ]

    print(f"── {label}得分結構亮點 " + "─" * 48)
    for col, lbl in metrics:
        if col not in played.columns:
            continue
        sub = played[played[col].notna()]
        if sub.empty:
            continue
        idx  = sub[col].idxmax()
        row  = sub.loc[idx]
        name = row.get("nameI", "")
        team = row.get("teamTricode", "")
        val  = row[col]
        print(f"  {lbl:<28}: {_pct(val)}  {name}  ({team})")
    print()


# ---------------------------------------------------------------------------
# 6. 節次得分分布（start_period / end_period 示範）
# ---------------------------------------------------------------------------

def show_quarter_scoring(game_id: str, quarter: int, label: str = "") -> None:
    """取得單節的得分分布數據。"""
    try:
        resp = fetch_boxscore_scoring(
            game_id=game_id,
            start_period=quarter,
            end_period=quarter,
            range_type=0,
        )
    except Exception as e:
        print(f"  第 {quarter} 節得分分布取得失敗: {e}\n")
        return

    df = _clean(resp.team_stats.get_data_frame())
    if df.empty:
        print(f"── {label}第 {quarter} 節：無資料\n")
        return

    period_label = f"Q{quarter}" if quarter <= 4 else f"OT{quarter - 4}"
    avail = [c for c in TEAM_CORE if c in df.columns]

    print(f"── {label}{period_label} 節次球隊得分結構 " + "─" * 38)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 常規賽 vs 季後賽球隊得分結構對比
# ---------------------------------------------------------------------------

def show_season_type_comparison(
    reg_resp: BoxScoreScoringV3,
    po_resp:  BoxScoreScoringV3,
    team_tricode: str,
) -> None:
    """
    指定球隊在常規賽場次 vs 季後賽場次的得分結構並排。
    示範如何比對兩種賽事球隊的進攻模式差異。
    """
    compare_cols = [
        "percentageFieldGoalsAttempted2pt", "percentageFieldGoalsAttempted3pt",
        "percentagePoints2pt", "percentagePointsMidrange2pt",
        "percentagePoints3pt", "percentagePointsFastBreak",
        "percentagePointsFreeThrow", "percentagePointsOffTurnovers",
        "percentagePointsPaint",
        "percentageAssistedFGM", "percentageUnassistedFGM",
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
            entry[col] = _pct(v) if pd.notna(v) else "N/A"
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("賽事")
    print(f"── 常規賽 vs 季後賽得分結構對比（{team_tricode}）" + "─" * 16)
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

    # ── 全場得分分布 ──────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  全場得分分布 Boxscore  game_id={game_id}  ({season_type})")
    print("=" * 65 + "\n")

    resp = fetch_boxscore_scoring(game_id=game_id)

    show_player_shot_distribution(resp)
    show_player_scoring_sources(resp)
    show_player_assist_dependency(resp)
    show_team_stats(resp)
    show_scoring_highlights(resp)

    # ── 節次得分結構（Q1 / Q4）───────────────────────────────────────────
    print("=" * 65)
    print("  節次得分結構（start_period / end_period 參數示範）")
    print("=" * 65 + "\n")

    for q in [1, 4]:
        show_quarter_scoring(game_id, quarter=q)

    # ── 季後賽 ────────────────────────────────────────────────────────────
    if season_type == "Regular Season":
        print("=" * 65)
        print(f"  季後賽得分分布示範  ({season} Playoffs)")
        print("=" * 65 + "\n")

        print(f"透過 LeagueGameFinder 尋找 {season} Playoffs 場次…")
        po_game_id = find_game_id(season=season, season_type="Playoffs")

        if po_game_id:
            print(f"找到 game_id: {po_game_id}\n")
            po_resp = fetch_boxscore_scoring(game_id=po_game_id)

            show_player_shot_distribution(po_resp,   label="季後賽 ")
            show_player_scoring_sources(po_resp,     label="季後賽 ")
            show_player_assist_dependency(po_resp,   label="季後賽 ")
            show_team_stats(po_resp,                 label="季後賽 ")
            show_scoring_highlights(po_resp,         label="季後賽 ")

            for q in [1, 4]:
                show_quarter_scoring(po_game_id, quarter=q, label="季後賽 ")

            # 常規賽 vs 季後賽球隊得分結構並排
            reg_df = _clean(resp.team_stats.get_data_frame())
            if not reg_df.empty:
                home_team = reg_df.iloc[0]["teamTricode"]
                print("=" * 65)
                print(f"  常規賽 vs 季後賽得分結構並排示範（{home_team}）")
                print("=" * 65 + "\n")
                show_season_type_comparison(resp, po_resp, home_team)

        else:
            print(f"  {season} 無季後賽記錄（可能球季尚未結束）。\n")
