"""
nba_api.stats.endpoints.commonplayerinfo 範例程式

執行方式：
  python example/stats/common_player_info.py                   # 預設示範球員
  python example/stats/common_player_info.py "LeBron James"    # 依姓名查詢
  python example/stats/common_player_info.py 2544              # 依 player_id 查詢

CommonPlayerInfo 參數：
  player_id          — 球員 ID（必填）
  league_id_nullable — 聯盟 ID，預設 "00"（NBA）

DataSet：
  resp.common_player_info     — 球員個人資料（1 row）
  resp.player_headline_stats  — 本季標題數據（PTS / AST / REB / PIE）
  resp.available_seasons      — 球員有統計資料的所有球季清單

CommonPlayerInfo 欄位：
  PERSON_ID, FIRST_NAME, LAST_NAME, DISPLAY_FIRST_LAST
  BIRTHDATE, SCHOOL, COUNTRY, LAST_AFFILIATION
  HEIGHT, WEIGHT, SEASON_EXP
  JERSEY, POSITION
  ROSTERSTATUS                — "Active" / "Inactive"
  TEAM_ID, TEAM_NAME, TEAM_ABBREVIATION, TEAM_CITY
  FROM_YEAR, TO_YEAR
  DRAFT_YEAR, DRAFT_ROUND, DRAFT_NUMBER
  DLEAGUE_FLAG, NBA_FLAG, GAMES_PLAYED_FLAG

PlayerHeadlineStats 欄位：
  PLAYER_ID, PLAYER_NAME, TimeFrame, PTS, AST, REB, PIE

AvailableSeasons 欄位：
  SEASON_ID
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
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


# ---------------------------------------------------------------------------
# 1. 個人資料卡
# ---------------------------------------------------------------------------

PROFILE_FIELDS = [
    ("DISPLAY_FIRST_LAST",     "姓名"),
    ("JERSEY",                 "背號"),
    ("POSITION",               "位置"),
    ("HEIGHT",                 "身高"),
    ("WEIGHT",                 "體重(磅)"),
    ("BIRTHDATE",              "生日"),
    ("COUNTRY",                "國籍"),
    ("SCHOOL",                 "學校"),
    ("LAST_AFFILIATION",       "最後所屬"),
    ("SEASON_EXP",             "資歷(年)"),
    ("ROSTERSTATUS",           "現役狀態"),
    ("TEAM_ABBREVIATION",      "球隊"),
    ("TEAM_CITY",              "城市"),
    ("FROM_YEAR",              "生涯起始"),
    ("TO_YEAR",                "生涯結束"),
    ("DRAFT_YEAR",             "選秀年"),
    ("DRAFT_ROUND",            "選秀輪"),
    ("DRAFT_NUMBER",           "選秀順位"),
    ("DLEAGUE_FLAG",           "G-League經歷"),
    ("NBA_FLAG",               "NBA經歷"),
    ("GAMES_PLAYED_FLAG",      "本季出賽"),
]


def show_profile(resp: CommonPlayerInfo) -> None:
    """以資料卡格式印出球員個人資訊"""
    info = resp.common_player_info.get_data_frame()
    if info.empty:
        print("無個人資料\n")
        return

    row = info.iloc[0]
    print("── 球員個人資料 " + "─" * 48)
    for field, label in PROFILE_FIELDS:
        val = row.get(field, "—")
        if pd.isna(val) or val == "":
            val = "—"
        print(f"  {label:<14}: {val}")
    print()


# ---------------------------------------------------------------------------
# 2. 標題數據（本季 / 生涯）
# ---------------------------------------------------------------------------

def show_headline_stats(resp: CommonPlayerInfo) -> None:
    """印出 PlayerHeadlineStats：本季與生涯 PTS / AST / REB / PIE"""
    df = resp.player_headline_stats.get_data_frame()
    if df.empty:
        print("── 標題數據：無資料\n")
        return

    print("── 標題數據（PTS / AST / REB / PIE）" + "─" * 28)
    cols = [c for c in ["TimeFrame", "PTS", "AST", "REB", "PIE"] if c in df.columns]
    print(df[cols].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 可用球季清單
# ---------------------------------------------------------------------------

def show_available_seasons(resp: CommonPlayerInfo) -> None:
    """列出球員有資料的所有球季"""
    df = resp.available_seasons.get_data_frame()
    if df.empty:
        print("── 可用球季：無資料\n")
        return

    seasons = df["SEASON_ID"].tolist()
    # 將 season list 每 10 個一行排版
    lines = [seasons[i:i+10] for i in range(0, len(seasons), 10)]
    print(f"── 可用球季（共 {len(seasons)} 季）" + "─" * 40)
    for line in lines:
        print("  " + "  ".join(str(s) for s in line))
    print()


# ---------------------------------------------------------------------------
# 4. 多位球員比較
# ---------------------------------------------------------------------------

COMPARE_FIELDS = [
    "DISPLAY_FIRST_LAST", "POSITION", "HEIGHT", "WEIGHT",
    "TEAM_ABBREVIATION", "FROM_YEAR", "TO_YEAR", "SEASON_EXP",
    "DRAFT_YEAR", "DRAFT_NUMBER",
]


def _fetch_player_info(player_id: int) -> CommonPlayerInfo:
    """取得 CommonPlayerInfo，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return CommonPlayerInfo(player_id=player_id, timeout=TIMEOUT)
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)


def compare_players(player_ids: list[int]) -> None:
    """並排比較多位球員的基本資料"""
    rows = []
    for pid in player_ids:
        try:
            resp = _fetch_player_info(pid)
            info = resp.common_player_info.get_data_frame()
            if not info.empty:
                rows.append(info.iloc[0])
        except Exception as e:
            print(f"  player_id={pid} 取得失敗: {e}")

    if not rows:
        print("── 比較：無資料\n")
        return

    df = pd.DataFrame(rows)
    available = [c for c in COMPARE_FIELDS if c in df.columns]
    print(f"── 球員比較（{len(rows)} 人）" + "─" * 44)
    print(df[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    player_id, full_name = _find_player_id(query)
    print(f"查詢球員: {full_name}  (player_id={player_id})\n")

    resp = _fetch_player_info(player_id)

    # 1. 個人資料卡
    show_profile(resp)

    # 2. 標題數據
    show_headline_stats(resp)

    # 3. 可用球季
    show_available_seasons(resp)

    # 4. 多人比較示範（LeBron James / Stephen Curry / Kevin Durant）
    DEMO_IDS = [2544, 201939, 201142]
    print("=" * 65)
    print("  多人比較示範（LeBron James / Stephen Curry / Kevin Durant）")
    print("=" * 65 + "\n")
    compare_players(DEMO_IDS)
