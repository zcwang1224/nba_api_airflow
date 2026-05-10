"""
nba_api.stats.endpoints.playerawards 範例程式

執行方式：
  python example/stats/player_awards.py                   # 預設示範球員
  python example/stats/player_awards.py "LeBron James"    # 依姓名查詢
  python example/stats/player_awards.py 2544              # 依 player_id 查詢

PlayerAwards 參數：
  player_id  — 球員 ID（必填，唯一參數）

DataSet：
  resp.player_awards  — 球員所有獎項記錄，每筆為一次得獎

欄位說明：
  PERSON_ID           — 球員 ID
  FIRST_NAME          — 名
  LAST_NAME           — 姓
  TEAM                — 得獎時所屬球隊縮寫
  DESCRIPTION         — 獎項名稱（如 "NBA Champion"、"Most Valuable Player"）
  ALL_NBA_TEAM_NUMBER — All-NBA 隊伍編號（1=一隊, 2=二隊, 3=三隊；其他獎項為空）
  SEASON              — 球季（如 "2015-16"）
  MONTH               — 月份（月獎項有值，其他為空）
  WEEK                — 週次（週獎項有值，其他為空）
  CONFERENCE          — 所屬聯盟區（東／西，適用月獎、週獎）
  TYPE                — 獎項大類（如 "Award", "All-NBA"）
  SUBTYPE1            — 子分類 1
  SUBTYPE2            — 子分類 2
  SUBTYPE3            — 子分類 3
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerawards import PlayerAwards
from nba_api.stats.static import players

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def _find_player_id(query: str) -> tuple[int, str]:
    """
    將字串 query 解析為 (player_id, full_name)。
    query 可以是數字 ID 或球員姓名（模糊比對）。
    """
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


def fetch_awards(player_id: int) -> pd.DataFrame:
    """取得球員所有獎項，回傳整理好的 DataFrame，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            resp = PlayerAwards(player_id=player_id, timeout=TIMEOUT)
            break
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)
    df = resp.player_awards.get_data_frame()
    if df.empty:
        return df
    df["FULL_NAME"] = df["FIRST_NAME"] + " " + df["LAST_NAME"]
    df["ALL_NBA_TEAM_NUMBER"] = pd.to_numeric(df["ALL_NBA_TEAM_NUMBER"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 1. 完整獎項清單
# ---------------------------------------------------------------------------

def show_all_awards(df: pd.DataFrame, full_name: str) -> None:
    """依球季排序，印出所有獎項"""
    print(f"── {full_name} 完整獎項清單（共 {len(df)} 筆）" + "─" * 28)
    if df.empty:
        print("  無獎項記錄\n")
        return

    cols = ["SEASON", "DESCRIPTION", "TEAM", "ALL_NBA_TEAM_NUMBER",
            "CONFERENCE", "MONTH", "WEEK"]
    available = [c for c in cols if c in df.columns]
    print(df.sort_values("SEASON")[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 按獎項類型彙整
# ---------------------------------------------------------------------------

def show_awards_by_type(df: pd.DataFrame) -> None:
    """各獎項 DESCRIPTION 出現次數統計"""
    if df.empty:
        print("── 獎項統計：無資料\n")
        return

    # 區分年度大獎與月 / 週獎
    annual = df[df["MONTH"].isna() & df["WEEK"].isna()]
    monthly = df[df["MONTH"].notna()]
    weekly  = df[df["WEEK"].notna()]

    print("── 年度獎項統計 " + "─" * 48)
    if annual.empty:
        print("  無年度獎項")
    else:
        dist = annual["DESCRIPTION"].value_counts()
        for desc, cnt in dist.items():
            seasons = annual[annual["DESCRIPTION"] == desc]["SEASON"].tolist()
            print(f"  {desc:<45} × {cnt:>2}  {seasons}")
    print()

    print("── 月份最佳球員次數 " + "─" * 44)
    if monthly.empty:
        print("  無月獎紀錄\n")
    else:
        print(f"  共 {len(monthly)} 次")
        conf_dist = monthly["CONFERENCE"].value_counts()
        for conf, cnt in conf_dist.items():
            print(f"    {conf:<10}: {cnt} 次")
    print()

    print("── 每週最佳球員次數 " + "─" * 44)
    if weekly.empty:
        print("  無週獎紀錄\n")
    else:
        print(f"  共 {len(weekly)} 次")
        conf_dist = weekly["CONFERENCE"].value_counts()
        for conf, cnt in conf_dist.items():
            print(f"    {conf:<10}: {cnt} 次")
    print()


# ---------------------------------------------------------------------------
# 3. 冠軍與 MVP 記錄
# ---------------------------------------------------------------------------

HIGHLIGHT_KEYWORDS = [
    "Champion",
    "Most Valuable Player",
    "Finals Most Valuable Player",
    "Defensive Player",
    "Rookie of the Year",
    "Sixth Man",
    "Most Improved Player",
]


def show_highlights(df: pd.DataFrame) -> None:
    """列出冠軍、MVP 等重要年度大獎"""
    if df.empty:
        print("── 重要獎項：無資料\n")
        return

    matched = df[
        df["DESCRIPTION"].str.contains("|".join(HIGHLIGHT_KEYWORDS), case=False, na=False)
    ].copy()

    print("── 重要年度獎項 " + "─" * 48)
    if matched.empty:
        print("  無符合記錄\n")
        return

    cols = ["SEASON", "DESCRIPTION", "TEAM"]
    print(matched.sort_values("SEASON")[cols].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. All-NBA / All-Defensive 選拔記錄
# ---------------------------------------------------------------------------

def show_all_nba(df: pd.DataFrame) -> None:
    """列出所有 All-NBA、All-Defensive、All-Rookie 等年度球隊入選記錄"""
    if df.empty:
        return

    team_awards = df[
        df["DESCRIPTION"].str.contains(r"All-NBA|All-Defensive|All-Rookie|All-Star", case=False, na=False)
    ].copy()

    if team_awards.empty:
        print("── All-NBA / All-Defensive / All-Star 記錄：無\n")
        return

    print("── 年度球隊入選記錄 " + "─" * 44)
    cols = ["SEASON", "DESCRIPTION", "ALL_NBA_TEAM_NUMBER", "TEAM"]
    available = [c for c in cols if c in team_awards.columns]
    print(team_awards.sort_values("SEASON")[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 多位球員獎項數比較
# ---------------------------------------------------------------------------

DEMO_PLAYERS = [
    (2544,  "LeBron James"),
    (201939, "Stephen Curry"),
    (201142, "Kevin Durant"),
    (203954, "Joel Embiid"),
    (1629029, "Luka Doncic"),
]


def compare_players_awards(player_list: list[tuple[int, str]]) -> None:
    """並排比較多位球員的獎項總數（年度大獎 / 月獎 / 週獎）"""
    rows = []
    for pid, name in player_list:
        try:
            df = fetch_awards(pid)
            if df.empty:
                rows.append({"球員": name, "年度大獎": 0, "月獎": 0, "週獎": 0, "冠軍": 0, "MVP": 0})
                continue

            annual  = df[df["MONTH"].isna() & df["WEEK"].isna()]
            monthly = df[df["MONTH"].notna()]
            weekly  = df[df["WEEK"].notna()]
            champs  = annual["DESCRIPTION"].str.contains("Champion",          case=False, na=False).sum()
            mvps    = annual["DESCRIPTION"].str.contains("Most Valuable Player", case=False, na=False).sum()

            rows.append({
                "球員":   name,
                "年度大獎": len(annual),
                "月獎":   len(monthly),
                "週獎":   len(weekly),
                "冠軍":   int(champs),
                "MVP":    int(mvps),
            })
            print(f"  ✓ {name} 獎項取得完成")
        except Exception as e:
            print(f"  ✗ {name} (player_id={pid}) 取得失敗: {e}")

    if not rows:
        print("── 比較：無資料\n")
        return

    cmp_df = (
        pd.DataFrame(rows)
        .sort_values("年度大獎", ascending=False)
        .reset_index(drop=True)
    )
    print()
    print("── 多位球員獎項比較 " + "─" * 44)
    print(cmp_df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    player_id, full_name = _find_player_id(query)
    print(f"查詢球員: {full_name}  (player_id={player_id})\n")

    df = fetch_awards(player_id)

    # 1. 完整獎項清單
    show_all_awards(df, full_name)

    # 2. 按類型彙整
    show_awards_by_type(df)

    # 3. 重要年度大獎
    show_highlights(df)

    # 4. All-NBA / All-Defensive
    show_all_nba(df)

    # 5. 多人比較示範
    print("=" * 65)
    print("  多位球員獎項比較示範")
    print("=" * 65 + "\n")
    compare_players_awards(DEMO_PLAYERS)
