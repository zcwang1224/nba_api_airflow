"""
nba_api.stats.endpoints.playercareerstats 範例程式

執行方式：
  python example/stats/player_career_stats.py                    # 預設示範球員
  python example/stats/player_career_stats.py "Stephen Curry"    # 依姓名查詢
  python example/stats/player_career_stats.py 2544              # 依 player_id 查詢
  python example/stats/player_career_stats.py 2544 PerGame      # 指定 per_mode

PlayerCareerStats 參數：
  player_id          — 球員 ID（必填）
  per_mode36         — "Totals"（預設）/ "PerGame" / "Per36"
  league_id_nullable — "00"=NBA（預設）/ "10"=WNBA / "20"=G-League

DataSet 總覽（共 10 個）：
  season_totals_regular_season    — 逐季常規賽數據
  career_totals_regular_season    — 生涯常規賽累計
  season_totals_post_season       — 逐季季後賽數據
  career_totals_post_season       — 生涯季後賽累計
  season_totals_all_star_season   — 明星賽逐季數據
  career_totals_all_star_season   — 明星賽生涯累計
  season_totals_college_season    — 大學時期逐季數據
  career_totals_college_season    — 大學時期累計
  season_rankings_regular_season  — 逐季常規賽全聯盟排名
  season_rankings_post_season     — 逐季季後賽全聯盟排名

SeasonTotals 共用欄位：
  SEASON_ID         — 球季（如 "2003-04"）
  TEAM_ABBREVIATION — 所屬球隊縮寫
  PLAYER_AGE        — 當季球員年齡
  GP / GS           — 出賽 / 先發場次
  MIN               — 上場時間（Totals=總分鐘, PerGame=場均）
  FGM/FGA/FG_PCT    — 投籃
  FG3M/FG3A/FG3_PCT — 三分
  FTM/FTA/FT_PCT    — 罰球
  OREB/DREB/REB     — 進攻籃板 / 防守籃板 / 總籃板
  AST / STL / BLK / TOV / PF / PTS

CareerTotals 欄位同上，但無 SEASON_ID / TEAM_ABBREVIATION / PLAYER_AGE

SeasonRankings 欄位：
  RANK_MIN / RANK_FGM / RANK_FGA / RANK_FG_PCT
  RANK_FG3M / RANK_FG3A / RANK_FG3_PCT
  RANK_FTM / RANK_FTA / RANK_FT_PCT
  RANK_OREB / RANK_DREB / RANK_REB
  RANK_AST / RANK_STL / RANK_BLK / RANK_TOV / RANK_PTS / RANK_EFF
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playercareerstats import PlayerCareerStats
from nba_api.stats.library.parameters import PerMode36
from nba_api.stats.static import players

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5


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


def _to_numeric_cols(df: pd.DataFrame, exclude: list[str]) -> pd.DataFrame:
    """將非 exclude 欄位轉為 numeric，方便後續計算。"""
    num_cols = [c for c in df.columns if c not in exclude]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


STAT_COLS = ["GP", "GS", "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
             "FG_PCT", "FG3_PCT", "FT_PCT"]
RANK_COLS = ["RANK_PTS", "RANK_REB", "RANK_AST", "RANK_STL", "RANK_BLK",
             "RANK_FG_PCT", "RANK_EFF"]
STR_COLS  = ["SEASON_ID", "TEAM_ABBREVIATION", "PLAYER_AGE", "SCHOOL_NAME"]


def fetch_career_stats(
    player_id: int,
    per_mode: str = PerMode36.per_game,
) -> PlayerCareerStats:
    """取得 PlayerCareerStats response 物件，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerCareerStats(player_id=player_id, per_mode36=per_mode, timeout=TIMEOUT)
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)


# ---------------------------------------------------------------------------
# 1. 逐季常規賽場均數據
# ---------------------------------------------------------------------------

def show_regular_season(resp: PlayerCareerStats, per_mode: str) -> None:
    """印出逐季常規賽數據（依 per_mode 顯示）"""
    df = resp.season_totals_regular_season.get_data_frame()
    if df.empty:
        print("── 常規賽逐季：無資料\n")
        return

    df = _to_numeric_cols(df, STR_COLS)
    cols = ["SEASON_ID", "TEAM_ABBREVIATION", "PLAYER_AGE", "GP", "GS",
            "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
            "FG_PCT", "FG3_PCT", "FT_PCT"]
    available = [c for c in cols if c in df.columns]
    label = {"Totals": "累計", "PerGame": "場均", "Per36": "Per36"}.get(per_mode, per_mode)
    print(f"── 常規賽逐季數據（{label}）" + "─" * 42)
    print(df[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 逐季季後賽場均數據
# ---------------------------------------------------------------------------

def show_post_season(resp: PlayerCareerStats, per_mode: str) -> None:
    """印出逐季季後賽數據"""
    df = resp.season_totals_post_season.get_data_frame()
    if df.empty:
        print("── 季後賽逐季：無季後賽出賽記錄\n")
        return

    df = _to_numeric_cols(df, STR_COLS)
    cols = ["SEASON_ID", "TEAM_ABBREVIATION", "PLAYER_AGE", "GP", "GS",
            "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
            "FG_PCT", "FG3_PCT", "FT_PCT"]
    available = [c for c in cols if c in df.columns]
    label = {"Totals": "累計", "PerGame": "場均", "Per36": "Per36"}.get(per_mode, per_mode)
    print(f"── 季後賽逐季數據（{label}）" + "─" * 42)
    print(df[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 生涯累計對比：常規賽 vs 季後賽
# ---------------------------------------------------------------------------

def show_career_totals(resp: PlayerCareerStats) -> None:
    """印出常規賽與季後賽生涯累計對比"""
    reg = resp.career_totals_regular_season.get_data_frame()
    post = resp.career_totals_post_season.get_data_frame()

    num_cols = ["GP", "GS", "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
                "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
                "FTM", "FTA", "FT_PCT"]

    rows = []
    for label, df in [("常規賽", reg), ("季後賽", post)]:
        if not df.empty:
            df = _to_numeric_cols(df, [])
            row = df.iloc[0]
            available = {c: row.get(c) for c in num_cols if c in df.columns}
            available["類型"] = label
            rows.append(available)

    if not rows:
        print("── 生涯累計：無資料\n")
        return

    cmp = pd.DataFrame(rows).set_index("類型")
    show_cols = [c for c in num_cols if c in cmp.columns]
    print("── 生涯累計對比（Totals 模式下最有意義）" + "─" * 28)
    print(cmp[show_cols].T.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 生涯巔峰球季（PerGame 模式下）
# ---------------------------------------------------------------------------

def show_peak_seasons(resp: PlayerCareerStats) -> None:
    """找出得分 / 籃板 / 助攻各項生涯最佳球季"""
    df = resp.season_totals_regular_season.get_data_frame()
    if df.empty:
        return

    df = _to_numeric_cols(df, STR_COLS)
    print("── 生涯各項巔峰球季（PerGame 數據）" + "─" * 36)
    for stat, label in [("PTS", "得分"), ("REB", "籃板"), ("AST", "助攻"),
                        ("STL", "抄截"), ("BLK", "火鍋"), ("FG3_PCT", "三分%")]:
        if stat not in df.columns:
            continue
        sub = df[df[stat].notna()]
        if sub.empty:
            continue
        idx   = sub[stat].idxmax()
        row   = sub.loc[idx]
        team  = row.get("TEAM_ABBREVIATION", "—")
        season = row.get("SEASON_ID", "—")
        val   = row[stat]
        print(f"  {label:<8}: {val:>6.1f}  ({season}, {team})")
    print()


# ---------------------------------------------------------------------------
# 5. 全聯盟排名走勢（常規賽）
# ---------------------------------------------------------------------------

def show_season_rankings(resp: PlayerCareerStats) -> None:
    """逐季常規賽全聯盟排名（數字越小越好）"""
    df = resp.season_rankings_regular_season.get_data_frame()
    if df.empty:
        print("── 全聯盟排名：無資料\n")
        return

    df = _to_numeric_cols(df, STR_COLS)
    cols  = ["SEASON_ID", "TEAM_ABBREVIATION", "GP"] + [c for c in RANK_COLS if c in df.columns]
    print("── 逐季全聯盟排名（數字越小 = 排名越前）" + "─" * 28)
    print(df[cols].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 明星賽數據
# ---------------------------------------------------------------------------

def show_all_star(resp: PlayerCareerStats) -> None:
    """明星賽逐季數據"""
    df = resp.season_totals_all_star_season.get_data_frame()
    if df.empty:
        print("── 明星賽：無出賽記錄\n")
        return

    df = _to_numeric_cols(df, STR_COLS)
    cols = ["SEASON_ID", "TEAM_ABBREVIATION", "GP", "MIN",
            "PTS", "REB", "AST", "FG_PCT"]
    available = [c for c in cols if c in df.columns]
    print(f"── 明星賽出賽記錄（共 {len(df)} 次）" + "─" * 40)
    print(df[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 大學時期數據
# ---------------------------------------------------------------------------

def show_college(resp: PlayerCareerStats) -> None:
    """大學時期逐季數據"""
    df = resp.season_totals_college_season.get_data_frame()
    if df.empty:
        print("── 大學時期：無記錄（或直接加入 NBA）\n")
        return

    df = _to_numeric_cols(df, STR_COLS + ["SCHOOL_NAME"])
    cols = ["SEASON_ID", "SCHOOL_NAME", "GP", "MIN",
            "PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "FT_PCT"]
    available = [c for c in cols if c in df.columns]
    print(f"── 大學時期數據（共 {len(df)} 季）" + "─" * 40)
    print(df[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"

if __name__ == "__main__":
    query    = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    per_mode = sys.argv[2] if len(sys.argv) > 2 else PerMode36.per_game

    if per_mode not in (PerMode36.totals, PerMode36.per_game, PerMode36.per_36):
        print(f"per_mode 必須為 Totals / PerGame / Per36，收到: {per_mode}")
        sys.exit(1)

    player_id, full_name = _find_player_id(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"Per Mode : {per_mode}\n")

    resp = fetch_career_stats(player_id, per_mode=per_mode)

    # 1. 逐季常規賽
    show_regular_season(resp, per_mode)

    # 2. 逐季季後賽
    show_post_season(resp, per_mode)

    # 3. 生涯累計對比（Totals 模式下數字最直觀）
    if per_mode == PerMode36.per_game:
        print("  ※ 以下生涯累計改以 Totals 模式重新請求...")
        resp_totals = fetch_career_stats(player_id, per_mode=PerMode36.totals)
        show_career_totals(resp_totals)
    else:
        show_career_totals(resp)

    # 4. 巔峰球季（PerGame 才有意義）
    if per_mode == PerMode36.per_game:
        show_peak_seasons(resp)
    else:
        print("  ※ 巔峰球季分析需以 PerGame 模式執行，略過。\n")

    # 5. 全聯盟排名走勢
    show_season_rankings(resp)

    # 6. 明星賽
    show_all_star(resp)

    # 7. 大學時期
    show_college(resp)
