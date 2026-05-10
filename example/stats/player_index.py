"""
nba_api.stats.endpoints.playerindex 範例程式

執行方式：
  python example/stats/player_index.py                 # 預設：本季所有球員
  python example/stats/player_index.py 2023-24         # 指定球季

PlayerIndex 參數：
  season                                  — 球季字串，如 "2024-25"（預設當季）
  league_id                               — "00"=NBA, "10"=WNBA, "20"=G-League（預設 "00"）
  active_nullable                         — "0"=全部, "1"=僅現役（預設 "0"）
  allstar_nullable                        — "0"=全部, "1"=明星賽球員（預設 "0"）
  historical_nullable                     — "0"=僅本季, "1"=歷史全部（預設 "0"）
  player_position_abbreviation_nullable   — "G"=後衛, "F"=前鋒, "C"=中鋒,
                                            "G-F", "F-G", "F-C", "C-F"
  college_nullable                        — 學校名稱（模糊比對，如 "Duke"）
  country_nullable                        — 國籍（如 "USA", "France"）
  draft_year_nullable                     — 選秀年（如 "2003"）
  draft_pick_nullable                     — 選秀順位（如 "1"）
  team_id_nullable                        — 球隊 ID
  height_nullable                         — 身高（如 "6-10"）
  weight_nullable                         — 體重磅數（如 "250"）

DataSet：
  resp.player_index          — 唯一 DataSet，含球員基本資料與本季平均數據

欄位說明：
  PERSON_ID          — 球員唯一 ID
  PLAYER_LAST_NAME   — 姓
  PLAYER_FIRST_NAME  — 名
  PLAYER_SLUG        — URL slug
  TEAM_ID            — 所屬球隊 ID
  TEAM_ABBREVIATION  — 球隊縮寫
  TEAM_CITY          — 球隊城市
  TEAM_NAME          — 球隊名稱
  IS_DEFUNCT         — 1=球隊已解散
  JERSEY_NUMBER      — 背號
  POSITION           — 位置
  HEIGHT             — 身高（英尺-英寸格式）
  WEIGHT             — 體重（磅）
  COLLEGE            — 學校
  COUNTRY            — 國籍
  DRAFT_YEAR         — 選秀年（undrafted 為空）
  DRAFT_ROUND        — 選秀輪次
  DRAFT_NUMBER       — 選秀順位
  ROSTER_STATUS      — 0=非現役, 1=現役
  PTS                — 本季場均得分
  REB                — 本季場均籃板
  AST                — 本季場均助攻
  STATS_TIMEFRAME    — 統計時間框架標記
  FROM_YEAR          — 生涯起始年
  TO_YEAR            — 生涯結束年
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerindex import PlayerIndex
from nba_api.stats.library.parameters import (
    ActiveNullable,
    AllStarNullable,
    HistoricalNullable,
    LeagueID,
    PlayerPositionAbbreviationNullable,
    Season,
)

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5


# ---------------------------------------------------------------------------
# 資料取得
# ---------------------------------------------------------------------------

def fetch_player_index(
    season: str = Season.default,
    league_id: str = LeagueID.nba,
    active_nullable: str = ActiveNullable.default,
    allstar_nullable: str = AllStarNullable.default,
    historical_nullable: str = HistoricalNullable.default,
    player_position_abbreviation_nullable: str = "",
    college_nullable: str = "",
    country_nullable: str = "",
    draft_year_nullable: str = "",
    draft_pick_nullable: str = "",
    team_id_nullable: str = "",
    height_nullable: str = "",
    weight_nullable: str = "",
) -> pd.DataFrame:
    """
    取得球員索引，回傳整理好的 DataFrame，失敗時自動重試。
    所有 nullable 參數留空字串表示不篩選。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            resp = PlayerIndex(
                season=season,
                league_id=league_id,
                active_nullable=active_nullable,
                allstar_nullable=allstar_nullable,
                historical_nullable=historical_nullable,
                player_position_abbreviation_nullable=player_position_abbreviation_nullable,
                college_nullable=college_nullable,
                country_nullable=country_nullable,
                draft_year_nullable=draft_year_nullable,
                draft_pick_nullable=draft_pick_nullable,
                team_id_nullable=team_id_nullable,
                height_nullable=height_nullable,
                weight_nullable=weight_nullable,
                timeout=TIMEOUT,
            )
            break
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)
    df = resp.player_index.get_data_frame()
    df["ROSTER_STATUS"] = pd.to_numeric(df["ROSTER_STATUS"], errors="coerce")
    df["FROM_YEAR"]     = pd.to_numeric(df["FROM_YEAR"],     errors="coerce")
    df["TO_YEAR"]       = pd.to_numeric(df["TO_YEAR"],       errors="coerce")
    df["PTS"]           = pd.to_numeric(df["PTS"],           errors="coerce")
    df["REB"]           = pd.to_numeric(df["REB"],           errors="coerce")
    df["AST"]           = pd.to_numeric(df["AST"],           errors="coerce")
    df["FULL_NAME"]     = df["PLAYER_FIRST_NAME"] + " " + df["PLAYER_LAST_NAME"]
    return df


# ---------------------------------------------------------------------------
# 1. 球員總覽
# ---------------------------------------------------------------------------

def show_overview(df: pd.DataFrame) -> None:
    """印出球員總覽：人數、現役比例、位置分佈"""
    total   = len(df)
    active  = int(df["ROSTER_STATUS"].eq(1).sum())
    no_team = int(df["TEAM_ID"].eq(0).sum())

    print(f"── 球員總覽 " + "─" * 52)
    print(f"  總人數      : {total:,}")
    print(f"  現役球員    : {active:,}  ({active/total*100:.1f}%)")
    print(f"  無隊球員    : {no_team}")

    pos_dist = (
        df[df["ROSTER_STATUS"] == 1]["POSITION"]
        .fillna("(未知)")
        .value_counts()
    )
    print("\n  現役球員位置分佈：")
    for pos, cnt in pos_dist.items():
        print(f"    {pos:<8}: {cnt}")
    print()


# ---------------------------------------------------------------------------
# 2. 本季得分 / 籃板 / 助攻排行
# ---------------------------------------------------------------------------

def show_stat_leaders(df: pd.DataFrame, top_n: int = 10) -> None:
    """印出本季場均得分、籃板、助攻前 N 名"""
    active = df[df["ROSTER_STATUS"] == 1].copy()

    for stat, label in [("PTS", "得分"), ("REB", "籃板"), ("AST", "助攻")]:
        leaders = (
            active[active[stat].notna()]
            .nlargest(top_n, stat)[["FULL_NAME", "TEAM_ABBREVIATION", "POSITION", stat]]
        )
        print(f"── 本季場均{label}前 {top_n} 名 " + "─" * 40)
        print(leaders.to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 3. 國籍分佈
# ---------------------------------------------------------------------------

def show_country_distribution(df: pd.DataFrame, top_n: int = 15) -> None:
    """現役球員的國籍分佈"""
    active  = df[df["ROSTER_STATUS"] == 1]
    dist    = (
        active["COUNTRY"]
        .fillna("(未知)")
        .value_counts()
        .head(top_n)
    )
    print(f"── 現役球員國籍分佈（前 {top_n} 名）" + "─" * 36)
    print(dist.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 學校 / 大學球員分析
# ---------------------------------------------------------------------------

def show_college_distribution(df: pd.DataFrame, top_n: int = 10) -> None:
    """現役球員產出最多的學校"""
    active   = df[df["ROSTER_STATUS"] == 1]
    has_col  = active[active["COLLEGE"].notna() & (active["COLLEGE"] != "")]
    dist     = has_col["COLLEGE"].value_counts().head(top_n)
    no_col   = active["COLLEGE"].isna().sum() + (active["COLLEGE"] == "").sum()

    print(f"── 現役球員學校分佈（前 {top_n} 所）" + "─" * 36)
    print(dist.to_string())
    print(f"\n  (無大學紀錄球員：{no_col} 人)")
    print()


# ---------------------------------------------------------------------------
# 5. 篩選示範：按位置
# ---------------------------------------------------------------------------

def show_by_position(df: pd.DataFrame, position: str = "C") -> None:
    """列出指定位置的現役球員"""
    sub = df[(df["ROSTER_STATUS"] == 1) & (df["POSITION"] == position)].copy()
    cols = ["FULL_NAME", "TEAM_ABBREVIATION", "HEIGHT", "WEIGHT", "PTS", "REB", "AST"]
    print(f"── 現役 {position} 球員（共 {len(sub)} 人）" + "─" * 40)
    print(sub[cols].sort_values("PTS", ascending=False).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 篩選示範：按國籍
# ---------------------------------------------------------------------------

def show_by_country(df: pd.DataFrame, country: str = "France") -> None:
    """列出指定國籍的球員"""
    sub = df[df["COUNTRY"].str.upper() == country.upper()].copy()
    cols = ["FULL_NAME", "TEAM_ABBREVIATION", "POSITION", "FROM_YEAR", "TO_YEAR",
            "PTS", "REB", "AST", "ROSTER_STATUS"]
    print(f"── 國籍為 {country} 的球員（共 {len(sub)} 人）" + "─" * 36)
    if sub.empty:
        print("  無資料")
    else:
        print(sub[cols].sort_values("ROSTER_STATUS", ascending=False).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 篩選示範：特定選秀年
# ---------------------------------------------------------------------------

def show_draft_class(df: pd.DataFrame, draft_year: str = "2003") -> None:
    """列出特定選秀年的球員，依選秀順位排序"""
    sub = df[df["DRAFT_YEAR"].astype(str) == str(draft_year)].copy()
    sub["DRAFT_NUMBER"] = pd.to_numeric(sub["DRAFT_NUMBER"], errors="coerce")
    cols = ["DRAFT_NUMBER", "FULL_NAME", "TEAM_ABBREVIATION", "POSITION",
            "FROM_YEAR", "TO_YEAR", "PTS"]
    print(f"── {draft_year} 屆選秀球員（共 {len(sub)} 人）" + "─" * 36)
    print(
        sub[cols]
        .sort_values("DRAFT_NUMBER")
        .to_string(index=False)
    )
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    season = sys.argv[1] if len(sys.argv) > 1 else Season.default
    print(f"球季: {season}\n")

    # --- 本季現役球員索引 ---
    print("=" * 65)
    print(f"  本季球員索引  (season={season})")
    print("=" * 65 + "\n")

    df = fetch_player_index(season=season, active_nullable=ActiveNullable.active_player)

    show_overview(df)
    show_stat_leaders(df, top_n=10)
    show_country_distribution(df, top_n=15)
    show_college_distribution(df, top_n=10)

    # --- 按位置篩選：中鋒 ---
    show_by_position(df, position="C")

    # --- 按國籍篩選：法國 ---
    show_by_country(df, country="France")

    # --- 選秀年篩選（需重新 fetch，帶 historical=1）---
    print("=" * 65)
    print("  選秀年篩選示範  (2003 屆, historical=1)")
    print("=" * 65 + "\n")

    draft_df = fetch_player_index(
        season=season,
        historical_nullable=HistoricalNullable.all_time,
        draft_year_nullable="2003",
    )
    show_draft_class(draft_df, draft_year="2003")

    # --- 明星賽球員名單 ---
    print("=" * 65)
    print(f"  本季明星賽球員  (allstar=1, season={season})")
    print("=" * 65 + "\n")

    try:
        allstar_df = fetch_player_index(
            season=season,
            allstar_nullable=AllStarNullable.all_star,
        )
        cols = ["FULL_NAME", "TEAM_ABBREVIATION", "POSITION", "PTS", "REB", "AST"]
        available = [c for c in cols if c in allstar_df.columns]
        print(f"明星賽球員數: {len(allstar_df)}\n")
        print(allstar_df[available].to_string(index=False))
    except Exception as e:
        print(f"明星賽球員資料取得失敗: {e}")
    print()
