"""
nba_api.stats.endpoints.playergamelog 範例程式

執行方式：
  python example/stats/player_game_log.py                        # 預設示範球員，本季
  python example/stats/player_game_log.py "Stephen Curry"        # 依姓名查詢
  python example/stats/player_game_log.py 2544                   # 依 player_id 查詢
  python example/stats/player_game_log.py 2544 2023-24           # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽（各自完整分析，最後並排對比）──

PlayerGameLog 參數：
  player_id              — 球員 ID（必填）
  season                 — 球季字串，如 "2024-25"（預設當季）
  season_type_all_star   — "Regular Season"（預設）/ "Playoffs" /
                           "Pre Season" / "PlayIn" / "All Star"
  date_from_nullable     — 起始日期，格式 "YYYY-MM-DD"（留空 = 整季）
  date_to_nullable       — 結束日期，格式 "YYYY-MM-DD"（留空 = 整季）
  league_id_nullable     — "00"=NBA（預設）/ "10"=WNBA / "20"=G-League

DataSet：
  resp.player_game_log   — 唯一 DataSet，每列為一場比賽記錄

欄位說明：
  SEASON_ID            — 球季（如 "22024"）
  Player_ID / Game_ID  — 球員 ID / 比賽 ID（注意：混合大小寫）
  GAME_DATE            — 比賽日期（MMM DD, YYYY 格式，讀入後轉 datetime）
  MATCHUP              — 對戰標記："LAL vs. BOS"=主場, "LAL @ BOS"=客場
  WL                   — 勝負："W" / "L"
  MIN                  — 上場分鐘數
  FGM / FGA / FG_PCT   — 投籃命中 / 出手 / 命中率
  FG3M / FG3A / FG3_PCT — 三分命中 / 出手 / 命中率
  FTM / FTA / FT_PCT   — 罰球命中 / 出手 / 命中率
  OREB / DREB / REB    — 進攻 / 防守 / 總籃板
  AST / STL / BLK / TOV / PF
  PTS                  — 得分
  PLUS_MINUS           — 正負值（上場期間球隊得失分差）
  VIDEO_AVAILABLE      — 1=有比賽影片
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playergamelog import PlayerGameLog
from nba_api.stats.library.parameters import Season, SeasonTypeAllStar
from nba_api.stats.static import players

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5

NUM_COLS = [
    "MIN", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB",
    "AST", "STL", "BLK", "TOV", "PF", "PTS", "PLUS_MINUS",
]
DISPLAY_COLS = [
    "GAME_DATE", "MATCHUP", "WL", "MIN", "PTS", "REB", "AST",
    "STL", "BLK", "FG_PCT", "FG3_PCT", "FT_PCT", "PLUS_MINUS",
]
STAT_ITEMS = [
    ("PTS",        "得分"),
    ("REB",        "籃板"),
    ("AST",        "助攻"),
    ("STL",        "抄截"),
    ("BLK",        "火鍋"),
    ("TOV",        "失誤"),
    ("MIN",        "上場時間"),
    ("FG_PCT",     "投籃%"),
    ("FG3_PCT",    "三分%"),
    ("FT_PCT",     "罰球%"),
    ("PLUS_MINUS", "正負值"),
]
MONTH_MAP = {
    10: "10月", 11: "11月", 12: "12月",
     1:  "1月",  2:  "2月",  3:  "3月",
     4:  "4月",  5:  "5月",  6:  "6月",
}


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


def fetch_game_log(
    player_id: int,
    season: str = Season.default,
    season_type: str = SeasonTypeAllStar.regular,
    date_from: str = "",
    date_to: str = "",
) -> pd.DataFrame:
    """
    取得球員比賽記錄，失敗時自動重試。
    回傳 DataFrame 依 GAME_DATE 升冪排序（最舊在前）。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            resp = PlayerGameLog(
                player_id=player_id,
                season=season,
                season_type_all_star=season_type,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                timeout=TIMEOUT,
            )
            break
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)

    df = resp.player_game_log.get_data_frame()
    if df.empty:
        return df

    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], format="%b %d, %Y", errors="coerce")
    df["HOME_AWAY"] = df["MATCHUP"].apply(lambda m: "主場" if "vs." in str(m) else "客場")
    df["MONTH"]     = df["GAME_DATE"].dt.month
    df["OPP"]       = df["MATCHUP"].str.extract(r"(?:vs\.|@)\s+(\w+)")
    return df.sort_values("GAME_DATE").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 1. 比賽記錄總表
# ---------------------------------------------------------------------------

def show_game_log(df: pd.DataFrame, label: str, last_n: int = 20) -> None:
    """印出最近 N 場比賽記錄（最新在前）。"""
    if df.empty:
        print(f"── {label}比賽記錄：無資料\n")
        return
    recent = df.tail(last_n).iloc[::-1].copy()
    recent["GAME_DATE"] = recent["GAME_DATE"].dt.strftime("%Y-%m-%d")
    available = [c for c in DISPLAY_COLS if c in recent.columns]
    print(f"── {label}最近 {min(last_n, len(df))} 場記錄（最新在前）" + "─" * 26)
    print(recent[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 數據總覽
# ---------------------------------------------------------------------------

def show_season_summary(df: pd.DataFrame, label: str) -> None:
    """場均 / 最高 / 最低 / 中位數，含勝負統計。"""
    if df.empty:
        print(f"── {label}總覽：無資料\n")
        return
    gp   = len(df)
    wins = int((df["WL"] == "W").sum())
    loss = gp - wins

    print(f"── {label}數據總覽（共 {gp} 場）" + "─" * 38)
    print(f"  出賽場次 : {gp}  |  球隊勝 {wins}  負 {loss}  （勝率 {wins/gp*100:.1f}%）")
    print()

    rows = []
    for col, lbl in STAT_ITEMS:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        if s.empty:
            continue
        rows.append({
            "項目": lbl, "場均": round(s.mean(), 2),
            "最高": s.max(), "最低": s.min(), "中位": round(s.median(), 2),
        })
    print(pd.DataFrame(rows).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 主客場分析
# ---------------------------------------------------------------------------

def show_home_away(df: pd.DataFrame, label: str) -> None:
    """主場 vs 客場場均數據比較。"""
    if df.empty or "HOME_AWAY" not in df.columns:
        return
    rows = []
    for venue, grp in df.groupby("HOME_AWAY"):
        gp   = len(grp)
        wins = int((grp["WL"] == "W").sum())
        row  = {"地點": venue, "場次": gp, "勝率%": round(wins / gp * 100, 1)}
        for col, stat_lbl in [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                               ("FG_PCT","投籃%"), ("FG3_PCT","三分%"),
                               ("PLUS_MINUS","正負值")]:
            if col in grp.columns:
                row[stat_lbl] = round(grp[col].mean(), 2)
        rows.append(row)
    cmp = pd.DataFrame(rows).set_index("地點")
    print(f"── {label}主場 vs 客場 " + "─" * 46)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 得分結構分析（eFG% / TS%）
# ---------------------------------------------------------------------------

def show_scoring_breakdown(df: pd.DataFrame, label: str) -> None:
    """分析得分來源組成及投籃效率指標。"""
    if df.empty:
        return
    avg_pts = df["PTS"].mean()
    if avg_pts == 0:
        return

    avg_2  = ((df["FGM"] - df["FG3M"]) * 2).mean()
    avg_3  = (df["FG3M"] * 3).mean()
    avg_ft = df["FTM"].mean()
    efg    = ((df["FGM"] + 0.5 * df["FG3M"]) / df["FGA"]).mean()
    ts     = (df["PTS"] / (2 * (df["FGA"] + 0.44 * df["FTA"]))).mean()

    print(f"── {label}得分結構 " + "─" * 48)
    print(f"  場均得分 : {avg_pts:.1f}")
    print(f"  ├ 2 分球 : {avg_2:.1f} 分  ({avg_2/avg_pts*100:.1f}%)")
    print(f"  ├ 3 分球 : {avg_3:.1f} 分  ({avg_3/avg_pts*100:.1f}%)  "
          f"({df['FG3M'].mean():.1f}/{df['FG3A'].mean():.1f}，"
          f"{df['FG3_PCT'].mean()*100:.1f}%)")
    print(f"  └ 罰  球 : {avg_ft:.1f} 分  ({avg_ft/avg_pts*100:.1f}%)  "
          f"({df['FTM'].mean():.1f}/{df['FTA'].mean():.1f}，"
          f"{df['FT_PCT'].mean()*100:.1f}%)")
    print(f"  eFG%     : {efg*100:.1f}%  （有效命中率，3分額外加權）")
    print(f"  TS%      : {ts*100:.1f}%  （真實命中率，含罰球）")
    print()


# ---------------------------------------------------------------------------
# 5. 逐月統計（常規賽用）
# ---------------------------------------------------------------------------

def show_monthly_stats(df: pd.DataFrame) -> None:
    """逐月場均得分 / 籃板 / 助攻及球隊勝負。"""
    if df.empty or "MONTH" not in df.columns:
        return
    rows = []
    for month, grp in df.groupby("MONTH"):
        gp   = len(grp)
        wins = int((grp["WL"] == "W").sum())
        row  = {"月份": MONTH_MAP.get(month, f"{month}月"),
                "場次": gp, "勝": wins, "敗": gp - wins}
        for col, lbl in [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                          ("FG_PCT","投籃%"), ("PLUS_MINUS","正負值")]:
            if col in grp.columns:
                row[lbl] = round(grp[col].mean(), 2)
        rows.append(row)
    print("── 逐月統計 " + "─" * 52)
    print(pd.DataFrame(rows).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 近期狀態走勢（滑動場均）
# ---------------------------------------------------------------------------

def show_rolling_average(df: pd.DataFrame, label: str,
                         windows: list[int] = [5, 10]) -> None:
    """全季 vs 近 N 場滑動場均，追蹤近期狀態。"""
    if df.empty or len(df) < min(windows):
        return
    stat_cols = ["PTS", "REB", "AST", "STL", "BLK", "FG_PCT", "FG3_PCT", "PLUS_MINUS"]
    available = [c for c in stat_cols if c in df.columns]

    rows = []
    for seg_lbl, sub in [("全季", df)] + [(f"近 {w} 場", df.tail(w)) for w in windows]:
        gp   = len(sub)
        wins = int((sub["WL"] == "W").sum())
        row  = {"時段": seg_lbl, "場次": gp, "勝率%": round(wins/gp*100, 1)}
        for col in available:
            row[col] = round(sub[col].mean(), 2)
        rows.append(row)

    result = pd.DataFrame(rows).set_index("時段")
    avail_cols = ["場次", "勝率%"] + available
    print(f"── {label}近期狀態走勢（滑動場均）" + "─" * 34)
    print(result[[c for c in avail_cols if c in result.columns]].to_string())
    print()


# ---------------------------------------------------------------------------
# 7. 連勝 / 連敗分析
# ---------------------------------------------------------------------------

def show_streak_analysis(df: pd.DataFrame, label: str) -> None:
    """找出最長連勝與連敗的場次與日期範圍。"""
    if df.empty:
        return
    wl = df["WL"].tolist()
    best_w = best_l = cur_w = cur_l = 0
    best_w_end = best_l_end = 0
    for i, result in enumerate(wl):
        if result == "W":
            cur_w += 1; cur_l = 0
            if cur_w > best_w:
                best_w = cur_w; best_w_end = i
        else:
            cur_l += 1; cur_w = 0
            if cur_l > best_l:
                best_l = cur_l; best_l_end = i

    print(f"── {label}連勝 / 連敗分析 " + "─" * 42)
    if best_w:
        d1 = df.iloc[best_w_end - best_w + 1]["GAME_DATE"]
        d2 = df.iloc[best_w_end]["GAME_DATE"]
        print(f"  最長連勝 : {best_w} 場  "
              f"（{d1.strftime('%Y-%m-%d')} ～ {d2.strftime('%Y-%m-%d')}）")
    if best_l:
        d1 = df.iloc[best_l_end - best_l + 1]["GAME_DATE"]
        d2 = df.iloc[best_l_end]["GAME_DATE"]
        print(f"  最長連敗 : {best_l} 場  "
              f"（{d1.strftime('%Y-%m-%d')} ～ {d2.strftime('%Y-%m-%d')}）")
    print()


# ---------------------------------------------------------------------------
# 8. 單場最佳表現
# ---------------------------------------------------------------------------

def show_top_performances(df: pd.DataFrame, label: str, top_n: int = 5) -> None:
    """得分 / 籃板 / 助攻 / 正負值各自最高前 N 場。"""
    if df.empty:
        return
    items = [("PTS","得分"), ("REB","籃板"), ("AST","助攻"), ("PLUS_MINUS","正負值")]
    base  = ["GAME_DATE", "MATCHUP", "WL"]
    for col, lbl in items:
        if col not in df.columns:
            continue
        top = df.nlargest(top_n, col).copy()
        top["GAME_DATE"] = top["GAME_DATE"].dt.strftime("%Y-%m-%d")
        avail = [c for c in base + [col, "PTS", "REB", "AST"] if c in top.columns]
        print(f"── {label}{lbl}最高 {top_n} 場 " + "─" * 40)
        print(top[avail].to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 9. 季後賽系列賽分析（由比賽記錄推算）
# ---------------------------------------------------------------------------

def show_playoff_series_from_log(df: pd.DataFrame, full_name: str) -> None:
    """
    從 PlayerGameLog 的季後賽記錄推算各系列賽狀況。
    MATCHUP 相同的連續場次視為同一系列賽。
    """
    if df.empty:
        return

    # 從 MATCHUP 萃取對手縮寫，分組為系列賽
    df = df.copy()
    df["GAME_DATE_STR"] = df["GAME_DATE"].dt.strftime("%Y-%m-%d")

    # 按對手分群（季後賽每輪只對一個對手）
    if "OPP" not in df.columns:
        df["OPP"] = df["MATCHUP"].str.extract(r"(?:vs\.|@)\s+(\w+)")

    series_groups = df.groupby("OPP", sort=False)

    print("── 季後賽各系列賽分析（七戰四勝）" + "─" * 30)
    for opp, grp in series_groups:
        grp = grp.sort_values("GAME_DATE").reset_index(drop=True)
        wins = int((grp["WL"] == "W").sum())
        loss = int((grp["WL"] == "L").sum())
        total = len(grp)

        # 系列賽結果判斷
        if wins == 4:
            result = "晉級 ✓"
        elif loss == 4:
            result = "淘汰 ✗"
        elif wins > loss:
            result = f"領先 {wins}-{loss}（進行中）"
        elif wins < loss:
            result = f"落後 {wins}-{loss}（進行中）"
        else:
            result = f"平手 {wins}-{loss}（進行中）"

        date_start = grp.iloc[0]["GAME_DATE_STR"]
        date_end   = grp.iloc[-1]["GAME_DATE_STR"]

        print(f"\n  對手：{opp}  |  {result}  |  {date_start} ～ {date_end}")
        print(f"  {'場次':<4} {'日期':<12} {'主客':<5} {'WL':<3} "
              f"{'PTS':>5} {'REB':>5} {'AST':>5} {'FG_PCT':>7} {'PLUS_MINUS':>10}")
        print("  " + "─" * 56)

        for i, row in grp.iterrows():
            home_away = "主場" if "vs." in str(row["MATCHUP"]) else "客場"
            print(f"  {i+1:<4} {row['GAME_DATE_STR']:<12} {home_away:<5} "
                  f"{row['WL']:<3} {row['PTS']:>5.0f} {row['REB']:>5.0f} "
                  f"{row['AST']:>5.0f} {row['FG_PCT']:>7.3f} {row['PLUS_MINUS']:>10.0f}")

        print(f"\n  場均：得分 {grp['PTS'].mean():.1f}  籃板 {grp['REB'].mean():.1f}  "
              f"助攻 {grp['AST'].mean():.1f}  "
              f"投籃% {grp['FG_PCT'].mean()*100:.1f}%  "
              f"正負值 {grp['PLUS_MINUS'].mean():+.1f}")
    print()


# ---------------------------------------------------------------------------
# 10. 常規賽 vs 季後賽並排對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_df: pd.DataFrame, po_df: pd.DataFrame
) -> None:
    """常規賽與季後賽場均數據並排，計算差值。"""
    if reg_df.empty or po_df.empty:
        return

    stat_cols = ["PTS", "REB", "AST", "STL", "BLK", "TOV",
                 "FG_PCT", "FG3_PCT", "FT_PCT", "PLUS_MINUS"]
    avail = [c for c in stat_cols if c in reg_df.columns and c in po_df.columns]

    cmp = pd.DataFrame({
        "常規賽": reg_df[avail].mean().round(3),
        "季後賽": po_df[avail].mean().round(3),
    })
    cmp["差值（季後 - 常規）"] = (cmp["季後賽"] - cmp["常規賽"]).round(3)

    print("── 常規賽 vs 季後賽場均對比 " + "─" * 36)
    print(f"  常規賽：{len(reg_df)} 場  |  季後賽：{len(po_df)} 場")
    print()
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "LeBron James"

if __name__ == "__main__":
    query     = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    season    = sys.argv[2] if len(sys.argv) > 2 else Season.default

    player_id, full_name = _find_player_id(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    reg_df = fetch_game_log(player_id, season=season,
                            season_type=SeasonTypeAllStar.regular)

    if reg_df.empty:
        print("  此球季無常規賽出賽記錄。\n")
    else:
        show_game_log(reg_df,          label="常規賽 ")
        show_season_summary(reg_df,    label="常規賽 ")
        show_home_away(reg_df,         label="常規賽 ")
        show_scoring_breakdown(reg_df, label="常規賽 ")
        show_monthly_stats(reg_df)
        show_rolling_average(reg_df,   label="常規賽 ", windows=[5, 10])
        show_streak_analysis(reg_df,   label="常規賽 ")
        show_top_performances(reg_df,  label="常規賽 ", top_n=5)

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    po_df = fetch_game_log(player_id, season=season,
                           season_type=SeasonTypeAllStar.playoffs)

    if po_df.empty:
        print(f"  {full_name} 本季無季後賽出賽記錄（或尚未進入季後賽）。\n")
    else:
        show_game_log(po_df,          label="季後賽 ", last_n=len(po_df))
        show_season_summary(po_df,    label="季後賽 ")
        show_home_away(po_df,         label="季後賽 ")
        show_scoring_breakdown(po_df, label="季後賽 ")
        show_rolling_average(po_df,   label="季後賽 ", windows=[5, 10])
        show_streak_analysis(po_df,   label="季後賽 ")
        show_top_performances(po_df,  label="季後賽 ", top_n=3)
        show_playoff_series_from_log(po_df, full_name)

    # ── 常規賽 vs 季後賽並排對比 ─────────────────────────────────────────
    if not reg_df.empty and not po_df.empty:
        print("=" * 65)
        print("  常規賽 vs 季後賽並排對比")
        print("=" * 65 + "\n")
        show_regular_vs_playoffs_comparison(reg_df, po_df)
