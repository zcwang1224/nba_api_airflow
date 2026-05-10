"""
nba_api.stats.endpoints.commonallplayers 範例程式

執行方式：
  python example/stats/common_all_players.py
  python example/stats/common_all_players.py 2023-24   # 指定球季

CommonAllPlayers 參數：
  is_only_current_season  — 0 = 歷史全部球員，1 = 僅本季名單（預設 0）
  league_id               — "00"=NBA, "10"=WNBA, "20"=G-League（預設 "00"）
  season                  — 球季字串，如 "2024-25"（預設當季）

DataSet：
  resp.common_all_players  — 唯一填充的 DataSet
  resp.get_data_frames()[0] — 同上，直接取 DataFrame

欄位說明：
  PERSON_ID                — 球員唯一 ID
  DISPLAY_LAST_COMMA_FIRST — 姓名（Last, First 格式）
  DISPLAY_FIRST_LAST       — 姓名（First Last 格式）
  ROSTERSTATUS             — 0=非現役, 1=現役
  FROM_YEAR                — 生涯起始年（如 "1984"）
  TO_YEAR                  — 生涯結束年（如 "2003"）
  PLAYERCODE               — NBA.com 球員代碼
  PLAYER_SLUG              — URL slug
  TEAM_ID                  — 所屬球隊 ID（0 = 無隊）
  TEAM_CITY                — 球隊城市
  TEAM_NAME                — 球隊名稱
  TEAM_ABBREVIATION        — 球隊縮寫
  TEAM_CODE                — 球隊代碼
  TEAM_SLUG                — 球隊 slug
  GAMES_PLAYED_FLAG        — "Y"=本季有出賽記錄
  OTHERLEAGUE_EXPERIENCE_CH — 非 NBA 聯盟經驗標記（"G"=G-League, "I"=國際等）
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.commonallplayers import CommonAllPlayers
from nba_api.stats.library.parameters import LeagueID, Season

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5


# ---------------------------------------------------------------------------
# 資料取得
# ---------------------------------------------------------------------------

def fetch_players(
    is_only_current_season: int = 0,
    league_id: str = LeagueID.nba,
    season: str = Season.default,
) -> pd.DataFrame:
    """
    取得球員名單，回傳整理好的 DataFrame，失敗時自動重試。
    is_only_current_season=1 時只回傳本季名單；=0 回傳歷史所有球員。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            resp = CommonAllPlayers(
                is_only_current_season=is_only_current_season,
                league_id=league_id,
                season=season,
                timeout=TIMEOUT,
            )
            break
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)
    df = resp.common_all_players.get_data_frame()
    df["ROSTERSTATUS"] = df["ROSTERSTATUS"].astype(int)
    df["FROM_YEAR"]    = pd.to_numeric(df["FROM_YEAR"], errors="coerce")
    df["TO_YEAR"]      = pd.to_numeric(df["TO_YEAR"], errors="coerce")
    df["CAREER_YEARS"] = df["TO_YEAR"] - df["FROM_YEAR"] + 1
    return df


# ---------------------------------------------------------------------------
# 1. 本季名單概覽
# ---------------------------------------------------------------------------

def show_current_roster(df: pd.DataFrame) -> None:
    """本季現役球員清單（ROSTERSTATUS=1），依球隊排序"""
    active = df[df["ROSTERSTATUS"] == 1].copy()
    print(f"── 本季現役球員（共 {len(active)} 人）" + "─" * 38)
    cols = ["DISPLAY_FIRST_LAST", "TEAM_ABBREVIATION", "TEAM_CITY", "TEAM_NAME",
            "FROM_YEAR", "TO_YEAR", "GAMES_PLAYED_FLAG"]
    print(
        active.sort_values(["TEAM_ABBREVIATION", "DISPLAY_LAST_COMMA_FIRST"])[cols]
        .to_string(index=False)
    )
    print()


# ---------------------------------------------------------------------------
# 2. 各球隊人數統計
# ---------------------------------------------------------------------------

def show_team_roster_count(df: pd.DataFrame) -> None:
    """各球隊人數，只計 ROSTERSTATUS=1 且有球隊歸屬的球員"""
    active = df[(df["ROSTERSTATUS"] == 1) & (df["TEAM_ID"] != 0)].copy()
    counts = (
        active.groupby(["TEAM_ABBREVIATION", "TEAM_CITY", "TEAM_NAME"])
        .size()
        .reset_index(name="人數")
        .sort_values("TEAM_ABBREVIATION")
    )
    print(f"── 各球隊本季名單人數（共 {len(counts)} 支球隊）" + "─" * 30)
    print(counts.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 自由球員 / 無隊球員
# ---------------------------------------------------------------------------

def show_free_agents(df: pd.DataFrame) -> None:
    """ROSTERSTATUS=1 但 TEAM_ID=0 的球員（本季仍在聯盟卻無隊）"""
    fa = df[(df["ROSTERSTATUS"] == 1) & (df["TEAM_ID"] == 0)]
    print(f"── 自由球員 / 無隊現役球員（共 {len(fa)} 人）" + "─" * 32)
    cols = ["DISPLAY_FIRST_LAST", "FROM_YEAR", "TO_YEAR", "GAMES_PLAYED_FLAG",
            "OTHERLEAGUE_EXPERIENCE_CH"]
    if fa.empty:
        print("  無")
    else:
        print(fa[cols].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 歷史全部球員 — 生涯長度分析
# ---------------------------------------------------------------------------

def show_career_length_stats(df: pd.DataFrame) -> None:
    """生涯年數統計（含非現役）"""
    valid = df[df["CAREER_YEARS"].notna()].copy()

    print("── 生涯年數統計 " + "─" * 48)
    print(valid["CAREER_YEARS"].describe().to_string())
    print()

    bins   = [0, 1, 3, 5, 10, 15, 20, 100]
    labels = ["1年", "2-3年", "4-5年", "6-10年", "11-15年", "16-20年", "21年+"]
    valid["career_bucket"] = pd.cut(valid["CAREER_YEARS"], bins=bins, labels=labels, right=True)
    dist = valid["career_bucket"].value_counts().sort_index()
    print("── 生涯長度分佈 " + "─" * 48)
    print(dist.to_string())
    print()

    print("── 生涯最長 10 人 " + "─" * 46)
    top10 = valid.nlargest(10, "CAREER_YEARS")[
        ["DISPLAY_FIRST_LAST", "FROM_YEAR", "TO_YEAR", "CAREER_YEARS"]
    ]
    print(top10.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 按首個出賽年分析球員產出潮
# ---------------------------------------------------------------------------

def show_draft_year_wave(df: pd.DataFrame, top_n: int = 10) -> None:
    """各年新進球員人數（以 FROM_YEAR 計）"""
    wave = (
        df[df["FROM_YEAR"].notna()]
        .groupby("FROM_YEAR")
        .size()
        .reset_index(name="新進人數")
        .sort_values("FROM_YEAR", ascending=False)
    )
    print(f"── 各年新進球員人數（最近 {top_n} 年）" + "─" * 36)
    print(wave.head(top_n).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 搜尋球員
# ---------------------------------------------------------------------------

def search_players(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    """依姓名（模糊）搜尋球員"""
    result = df[df["DISPLAY_FIRST_LAST"].str.contains(keyword, case=False, na=False)]
    return result[["PERSON_ID", "DISPLAY_FIRST_LAST", "ROSTERSTATUS",
                   "TEAM_ABBREVIATION", "FROM_YEAR", "TO_YEAR", "CAREER_YEARS"]]


def show_search(df: pd.DataFrame, keyword: str) -> None:
    result = search_players(df, keyword)
    print(f"── 搜尋「{keyword}」（共 {len(result)} 筆）" + "─" * 38)
    print(result.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 非 NBA 聯盟經驗標記分佈
# ---------------------------------------------------------------------------

def show_other_league_exp(df: pd.DataFrame) -> None:
    """OTHERLEAGUE_EXPERIENCE_CH 欄位值分佈"""
    dist = df["OTHERLEAGUE_EXPERIENCE_CH"].fillna("(空)").value_counts()
    print("── OTHERLEAGUE_EXPERIENCE_CH 分佈 " + "─" * 30)
    print(dist.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    season = sys.argv[1] if len(sys.argv) > 1 else Season.default
    print(f"球季: {season}\n")

    # --- 本季名單（is_only_current_season=1）---
    print("=" * 65)
    print(f"  本季名單  (is_only_current_season=1, season={season})")
    print("=" * 65 + "\n")

    current_df = fetch_players(is_only_current_season=1, season=season)
    show_current_roster(current_df)
    show_team_roster_count(current_df)
    show_free_agents(current_df)

    # --- 歷史全部球員（is_only_current_season=0）---
    print("=" * 65)
    print(f"  歷史全部球員  (is_only_current_season=0, season={season})")
    print("=" * 65 + "\n")

    all_df = fetch_players(is_only_current_season=0, season=season)
    print(f"歷史累計球員數: {len(all_df):,}\n")

    show_career_length_stats(all_df)
    show_draft_year_wave(all_df, top_n=10)
    show_other_league_exp(all_df)
    show_search(all_df, "James")

    # --- G-League（league_id="20"）---
    print("=" * 65)
    print(f"  G-League 本季名單  (league_id=20, season={season})")
    print("=" * 65 + "\n")

    try:
        gleague_df = fetch_players(is_only_current_season=1, league_id=LeagueID.g_league, season=season)
        active_g = gleague_df[gleague_df["ROSTERSTATUS"] == 1]
        print(f"G-League 本季球員數: {len(active_g)}\n")
        cols = ["DISPLAY_FIRST_LAST", "TEAM_ABBREVIATION", "FROM_YEAR",
                "OTHERLEAGUE_EXPERIENCE_CH"]
        print(active_g[cols].head(15).to_string(index=False))
    except Exception as e:
        print(f"G-League 資料取得失敗: {e}")
    print()
