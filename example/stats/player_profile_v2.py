"""
nba_api.stats.endpoints.playerprofilev2 範例程式

執行方式：
  python example/stats/player_profile_v2.py                    # 預設示範球員
  python example/stats/player_profile_v2.py "Kevin Durant"     # 依姓名查詢
  python example/stats/player_profile_v2.py 2544               # 依 player_id 查詢
  python example/stats/player_profile_v2.py 2544 PerGame       # 指定 per_mode

PlayerProfileV2 參數：
  player_id          — 球員 ID（必填）
  per_mode36         — "Totals"（預設）/ "PerGame" / "Per36"
  league_id_nullable — "00"=NBA（預設）/ "10"=WNBA / "20"=G-League

DataSet 總覽（共 15 個）：
  ┌─ PlayerProfileV2 獨有 ───────────────────────────────────────┐
  │  career_highs              生涯單場各項目最高（含日期、對手）  │
  │  season_highs              本季單場各項目最高                  │
  │  next_game                 下一場比賽資訊                      │
  │  career_totals_preseason   季前賽生涯累計                      │
  │  season_totals_preseason   季前賽逐季數據                      │
  └──────────────────────────────────────────────────────────────┘
  ┌─ 與 PlayerCareerStats 共有 ──────────────────────────────────┐
  │  season_totals_regular_season   逐季常規賽                    │
  │  career_totals_regular_season   生涯常規賽累計                 │
  │  season_totals_post_season      逐季季後賽                    │
  │  career_totals_post_season      生涯季後賽累計                 │
  │  season_totals_all_star_season  明星賽逐季                    │
  │  career_totals_all_star_season  明星賽生涯累計                 │
  │  season_totals_college_season   大學時期逐季                   │
  │  career_totals_college_season   大學時期累計                   │
  │  season_rankings_regular_season 逐季常規賽全聯盟排名           │
  │  season_rankings_post_season    逐季季後賽全聯盟排名           │
  └──────────────────────────────────────────────────────────────┘

CareerHighs 欄位：
  STAT / STAT_VALUE / STAT_ORDER / GAME_DATE / GAME_ID
  VS_TEAM_CITY / VS_TEAM_NAME / VS_TEAM_ABBREVIATION / DATE_EST

SeasonHighs 欄位（注意 STATS_VALUE 比 CareerHighs 多一個 S）：
  STAT / STATS_VALUE / STAT_ORDER / GAME_DATE
  VS_TEAM_CITY / VS_TEAM_NAME / VS_TEAM_ABBREVIATION / DATE_EST

NextGame 欄位：
  GAME_ID / GAME_DATE / GAME_TIME / LOCATION（"H"=主場, "A"=客場）
  PLAYER_TEAM_ID / PLAYER_TEAM_CITY / PLAYER_TEAM_NICKNAME / PLAYER_TEAM_ABBREVIATION
  VS_TEAM_ID / VS_TEAM_CITY / VS_TEAM_NICKNAME / VS_TEAM_ABBREVIATION

SeasonTotals / CareerTotals 共用欄位：
  SEASON_ID / TEAM_ABBREVIATION / PLAYER_AGE / GP / GS / MIN
  FGM / FGA / FG_PCT / FG3M / FG3A / FG3_PCT / FTM / FTA / FT_PCT
  OREB / DREB / REB / AST / STL / BLK / TOV / PF / PTS

SeasonRankings 欄位：
  SEASON_ID / TEAM_ABBREVIATION / PLAYER_AGE / GP / GS
  RANK_MIN / RANK_FGM / RANK_FGA / RANK_FG_PCT
  RANK_FG3M / RANK_FG3A / RANK_FG3_PCT
  RANK_FTM / RANK_FTA / RANK_FT_PCT
  RANK_OREB / RANK_DREB / RANK_REB / RANK_AST
  RANK_STL / RANK_BLK / RANK_TOV / RANK_PTS / RANK_EFF
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerprofilev2 import PlayerProfileV2
from nba_api.stats.library.parameters import PerMode36
from nba_api.stats.static import players

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5

STR_COLS = [
    "SEASON_ID", "TEAM_ABBREVIATION", "PLAYER_AGE", "SCHOOL_NAME",
    "GAME_ID", "GAME_DATE", "GAME_TIME", "LOCATION", "STAT", "DATE_EST",
]
SEASON_STAT_COLS = [
    "SEASON_ID", "TEAM_ABBREVIATION", "PLAYER_AGE", "GP", "GS",
    "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
    "FG_PCT", "FG3_PCT", "FT_PCT",
]
CAREER_STAT_COLS = [
    "GP", "GS", "MIN", "PTS", "REB", "AST",
    "STL", "BLK", "TOV", "FG_PCT", "FG3_PCT", "FT_PCT",
]
RANK_COLS = [
    "SEASON_ID", "TEAM_ABBREVIATION", "GP",
    "RANK_PTS", "RANK_REB", "RANK_AST", "RANK_STL", "RANK_BLK",
    "RANK_FG_PCT", "RANK_FG3_PCT", "RANK_EFF",
]
STAT_LABEL = {
    "PTS": "得分",   "REB": "籃板",  "AST": "助攻",  "STL": "抄截",
    "BLK": "火鍋",   "MIN": "上場時間", "FGM": "投籃命中", "FGA": "投籃出手",
    "FG3M": "三分命中", "FG3A": "三分出手", "FTM": "罰球命中", "FTA": "罰球出手",
    "OREB": "進攻籃板", "DREB": "防守籃板", "TOV": "失誤", "PF": "犯規",
    "PLUS_MINUS": "正負值",
}
PER_MODE_LABEL = {"Totals": "累計", "PerGame": "場均", "Per36": "Per36"}


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


def _to_numeric(df: pd.DataFrame, skip: list[str] = STR_COLS) -> pd.DataFrame:
    num_cols = [c for c in df.columns if c not in skip]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


def fetch_profile(player_id: int, per_mode: str = PerMode36.per_game) -> PlayerProfileV2:
    """取得 PlayerProfileV2 response 物件，失敗時自動重試。"""
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerProfileV2(player_id=player_id, per_mode36=per_mode, timeout=TIMEOUT)
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)


def _print_highs_table(df: pd.DataFrame, val_col: str) -> None:
    """共用：印出 CareerHighs / SeasonHighs 表格。"""
    for _, row in df.sort_values("STAT_ORDER").iterrows():
        stat    = row["STAT"]
        label   = STAT_LABEL.get(stat, stat)
        val     = row[val_col]
        date    = str(row.get("GAME_DATE", ""))[:10]
        opp     = row.get("VS_TEAM_ABBREVIATION", "—")
        game_id = str(row.get("GAME_ID", "")).strip()
        val_str = f"{int(val)}" if pd.notna(val) and float(val) == int(float(val)) else f"{val}"
        gid_str = f"  (GAME_ID: {game_id})" if game_id and game_id != "nan" else ""
        print(f"  {label:<10} {stat:<6}: {val_str:>5}    vs {opp:<5} {date}{gid_str}")


# ---------------------------------------------------------------------------
# 1. 生涯單場最高（PlayerProfileV2 獨有）
# ---------------------------------------------------------------------------

def show_career_highs(resp: PlayerProfileV2) -> None:
    df = resp.career_highs.get_data_frame()
    if df.empty:
        print("── 生涯單場最高：無資料\n")
        return
    df["STAT_VALUE"] = pd.to_numeric(df["STAT_VALUE"], errors="coerce")
    print("── 生涯單場最高記錄 " + "─" * 44)
    _print_highs_table(df, "STAT_VALUE")
    print()


# ---------------------------------------------------------------------------
# 2. 本季單場最高（PlayerProfileV2 獨有）
# ---------------------------------------------------------------------------

def show_season_highs(resp: PlayerProfileV2) -> None:
    df = resp.season_highs.get_data_frame()
    if df.empty:
        print("── 本季單場最高：無資料\n")
        return
    # SeasonHighs 的值欄名為 STATS_VALUE（多一個 S）
    val_col = "STATS_VALUE" if "STATS_VALUE" in df.columns else "STAT_VALUE"
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    print("── 本季單場最高記錄 " + "─" * 44)
    _print_highs_table(df, val_col)
    print()


# ---------------------------------------------------------------------------
# 3. 生涯最高 vs 本季最高對比
# ---------------------------------------------------------------------------

def show_highs_comparison(resp: PlayerProfileV2) -> None:
    career_df = resp.career_highs.get_data_frame()
    season_df = resp.season_highs.get_data_frame()
    if career_df.empty and season_df.empty:
        return

    c_col = "STAT_VALUE"
    s_col = "STATS_VALUE" if "STATS_VALUE" in season_df.columns else "STAT_VALUE"
    career_df[c_col] = pd.to_numeric(career_df[c_col], errors="coerce")
    season_df[s_col] = pd.to_numeric(season_df[s_col], errors="coerce")

    c_map = dict(zip(career_df["STAT"], career_df[c_col]))
    s_map = dict(zip(season_df["STAT"], season_df[s_col]))
    all_stats = list(dict.fromkeys(
        list(career_df.sort_values("STAT_ORDER")["STAT"]) +
        list(season_df.sort_values("STAT_ORDER")["STAT"])
    ))

    rows = []
    for stat in all_stats:
        c_val = c_map.get(stat)
        s_val = s_map.get(stat)
        label = STAT_LABEL.get(stat, stat)
        # 標記本季是否追平或超越生涯最高
        flag = ""
        if pd.notna(c_val) and pd.notna(s_val):
            if s_val >= c_val:
                flag = " ★生涯新高"
            elif s_val >= c_val * 0.9:
                flag = " (接近生涯最高)"
        rows.append({"項目": f"{label}({stat})", "生涯最高": c_val, "本季最高": s_val, "備註": flag})

    cmp = pd.DataFrame(rows)
    print("── 生涯最高 vs 本季最高對比 " + "─" * 36)
    print(cmp.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 下一場比賽（PlayerProfileV2 獨有）
# ---------------------------------------------------------------------------

def show_next_game(resp: PlayerProfileV2) -> None:
    df = resp.next_game.get_data_frame()
    if df.empty:
        print("── 下一場比賽：無排程（球季結束或球員未在名單中）\n")
        return
    row = df.iloc[0]
    location = str(row.get("LOCATION", "")).strip().upper()
    home_away = "主場" if location == "H" else "客場"
    print("── 下一場比賽 " + "─" * 50)
    print(f"  日期 / 時間 : {row.get('GAME_DATE', '—')}  {row.get('GAME_TIME', '')}")
    print(f"  主客場      : {home_away}")
    print(f"  我方球隊    : {row.get('PLAYER_TEAM_CITY', '')} "
          f"{row.get('PLAYER_TEAM_NICKNAME', '')} "
          f"({row.get('PLAYER_TEAM_ABBREVIATION', '')})")
    print(f"  對手球隊    : {row.get('VS_TEAM_CITY', '')} "
          f"{row.get('VS_TEAM_NICKNAME', '')} "
          f"({row.get('VS_TEAM_ABBREVIATION', '')})")
    print(f"  GAME_ID     : {row.get('GAME_ID', '—')}")
    print()


# ---------------------------------------------------------------------------
# 5. 逐季常規賽數據
# ---------------------------------------------------------------------------

def show_regular_season(resp: PlayerProfileV2, per_mode: str) -> None:
    df = resp.season_totals_regular_season.get_data_frame()
    if df.empty:
        print("── 常規賽逐季：無資料\n")
        return
    df = _to_numeric(df)
    avail = [c for c in SEASON_STAT_COLS if c in df.columns]
    label = PER_MODE_LABEL.get(per_mode, per_mode)
    print(f"── 常規賽逐季（{label}）" + "─" * 44)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 逐季季後賽數據
# ---------------------------------------------------------------------------

def show_post_season(resp: PlayerProfileV2, per_mode: str) -> None:
    df = resp.season_totals_post_season.get_data_frame()
    if df.empty:
        print("── 季後賽逐季：無季後賽出賽記錄\n")
        return
    df = _to_numeric(df)
    avail = [c for c in SEASON_STAT_COLS if c in df.columns]
    label = PER_MODE_LABEL.get(per_mode, per_mode)
    print(f"── 季後賽逐季（{label}）" + "─" * 44)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 季前賽逐季（PlayerProfileV2 獨有）
# ---------------------------------------------------------------------------

def show_preseason(resp: PlayerProfileV2, per_mode: str) -> None:
    df = resp.season_totals_preseason.get_data_frame()
    if df.empty:
        print("── 季前賽逐季：無資料\n")
        return
    df = _to_numeric(df)
    cols = ["SEASON_ID", "TEAM_ABBREVIATION", "PLAYER_AGE", "GP",
            "MIN", "PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "FT_PCT"]
    avail = [c for c in cols if c in df.columns]
    label = PER_MODE_LABEL.get(per_mode, per_mode)
    print(f"── 季前賽逐季（{label}）" + "─" * 44)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 8. 明星賽逐季
# ---------------------------------------------------------------------------

def show_all_star(resp: PlayerProfileV2, per_mode: str) -> None:
    df = resp.season_totals_all_star_season.get_data_frame()
    if df.empty:
        print("── 明星賽：無出賽記錄\n")
        return
    df = _to_numeric(df)
    cols = ["SEASON_ID", "TEAM_ABBREVIATION", "GP", "MIN",
            "PTS", "REB", "AST", "FG_PCT"]
    avail = [c for c in cols if c in df.columns]
    label = PER_MODE_LABEL.get(per_mode, per_mode)
    print(f"── 明星賽逐季（{len(df)} 次，{label}）" + "─" * 38)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 9. 大學時期逐季
# ---------------------------------------------------------------------------

def show_college(resp: PlayerProfileV2, per_mode: str) -> None:
    df = resp.season_totals_college_season.get_data_frame()
    if df.empty:
        print("── 大學時期：無記錄（或直接加入 NBA）\n")
        return
    df = _to_numeric(df, STR_COLS + ["SCHOOL_NAME"])
    cols = ["SEASON_ID", "SCHOOL_NAME", "GP", "MIN",
            "PTS", "REB", "AST", "FG_PCT", "FG3_PCT", "FT_PCT"]
    avail = [c for c in cols if c in df.columns]
    label = PER_MODE_LABEL.get(per_mode, per_mode)
    print(f"── 大學時期逐季（{len(df)} 季，{label}）" + "─" * 38)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 10. 逐季全聯盟排名（常規賽 + 季後賽）
# ---------------------------------------------------------------------------

def show_season_rankings(resp: PlayerProfileV2) -> None:
    for label, dataset in [("常規賽", resp.season_rankings_regular_season),
                            ("季後賽", resp.season_rankings_post_season)]:
        df = dataset.get_data_frame()
        if df.empty:
            print(f"── {label}逐季全聯盟排名：無資料\n")
            continue
        df = _to_numeric(df, STR_COLS)
        avail = [c for c in RANK_COLS if c in df.columns]
        print(f"── {label}逐季全聯盟排名（數字越小排名越前）" + "─" * 26)
        print(df[avail].to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 11. 生涯累計對比（常規賽 / 季後賽 / 季前賽 / 明星賽）
# ---------------------------------------------------------------------------

def show_career_totals_summary(resp: PlayerProfileV2) -> None:
    sources = [
        ("常規賽", resp.career_totals_regular_season),
        ("季後賽", resp.career_totals_post_season),
        ("季前賽", resp.career_totals_preseason),
        ("明星賽", resp.career_totals_all_star_season),
        ("大學",   resp.career_totals_college_season),
    ]
    rows = []
    for label, dataset in sources:
        df = dataset.get_data_frame()
        if df.empty:
            continue
        df = _to_numeric(df, [])
        row = df.iloc[0]
        entry = {"類型": label}
        for col in CAREER_STAT_COLS:
            if col in df.columns:
                v = row[col]
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        print("── 生涯累計摘要：無資料\n")
        return

    cmp  = pd.DataFrame(rows).set_index("類型")
    avail = [c for c in CAREER_STAT_COLS if c in cmp.columns]
    print("── 生涯各賽事累計對比（Totals 模式）" + "─" * 30)
    print(cmp[avail].T.to_string())
    print()


# ---------------------------------------------------------------------------
# 12. 生涯巔峰球季分析（PerGame 模式下）
# ---------------------------------------------------------------------------

def show_peak_seasons(resp: PlayerProfileV2) -> None:
    df = resp.season_totals_regular_season.get_data_frame()
    if df.empty:
        return
    df = _to_numeric(df)
    items = [("PTS", "得分"), ("REB", "籃板"), ("AST", "助攻"),
             ("STL", "抄截"), ("BLK", "火鍋"), ("FG3_PCT", "三分%")]
    print("── 生涯各項巔峰球季（常規賽 PerGame）" + "─" * 30)
    for stat, label in items:
        if stat not in df.columns:
            continue
        sub = df[df[stat].notna()]
        if sub.empty:
            continue
        idx    = sub[stat].idxmax()
        row    = sub.loc[idx]
        season = row.get("SEASON_ID", "—")
        team   = row.get("TEAM_ABBREVIATION", "—")
        val    = row[stat]
        print(f"  {label:<10}: {val:>6.2f}  ({season}, {team})")
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

    resp = fetch_profile(player_id, per_mode=per_mode)

    # ── PlayerProfileV2 獨有 ────────────────────────────────────────────────
    print("=" * 65)
    print("  PlayerProfileV2 獨有 DataSet")
    print("=" * 65 + "\n")

    show_career_highs(resp)
    show_season_highs(resp)
    show_highs_comparison(resp)
    show_next_game(resp)

    # ── 逐季數據 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  逐季數據（{PER_MODE_LABEL.get(per_mode, per_mode)}）")
    print("=" * 65 + "\n")

    show_regular_season(resp, per_mode)
    show_post_season(resp, per_mode)
    show_preseason(resp, per_mode)
    show_all_star(resp, per_mode)
    show_college(resp, per_mode)

    # ── 全聯盟排名走勢 ───────────────────────────────────────────────────────
    print("=" * 65)
    print("  逐季全聯盟排名走勢")
    print("=" * 65 + "\n")

    show_season_rankings(resp)

    # ── 生涯累計（Totals 模式最直觀，必要時重新請求）────────────────────────
    print("=" * 65)
    print("  生涯各賽事累計對比（Totals 模式）")
    print("=" * 65 + "\n")

    if per_mode == PerMode36.totals:
        show_career_totals_summary(resp)
    else:
        print("  ※ 生涯累計改以 Totals 模式重新請求...\n")
        resp_totals = fetch_profile(player_id, per_mode=PerMode36.totals)
        show_career_totals_summary(resp_totals)

    # ── 巔峰球季（PerGame 模式下才有意義）───────────────────────────────────
    if per_mode == PerMode36.per_game:
        print("=" * 65)
        print("  生涯各項巔峰球季")
        print("=" * 65 + "\n")
        show_peak_seasons(resp)
