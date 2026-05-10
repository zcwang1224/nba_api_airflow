"""
nba_api.stats.endpoints.playergamelogs 範例程式（複數版）

執行方式：
  python example/stats/player_game_logs.py                       # 預設示範球員，本季
  python example/stats/player_game_logs.py "Stephen Curry"       # 依姓名查詢
  python example/stats/player_game_logs.py 2544                  # 依 player_id 查詢
  python example/stats/player_game_logs.py 2544 2023-24          # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽（各自完整分析，最後並排對比）──

與 PlayerGameLog（單數）的關鍵差異：
  1. player_id 改為選填（可查詢全聯盟）
  2. 更多篩選維度：Location / Outcome / Month / GameSegment / LastNGames /
                   SeasonSegment / VsConference / VsDivision / OppTeamID /
                   Period / ShotClockRange / po_round_nullable（季後賽輪次）
  3. 獨有欄位：PLAYER_NAME, TEAM_ABBREVIATION, TEAM_NAME,
               BLKA（被封蓋）, PFD（被犯規）,
               NBA_FANTASY_PTS, DD2（雙十）, TD3（大三元）,
               全項目 RANK 欄（如 PTS_RANK, REB_RANK…）

重要注意事項：
  season_type / per_mode 必須傳非空字串；
  NBA API 對 playergamelogs 不接受空值，會回傳空 body → JSONDecodeError。
  season 必須用 Season.default（非 SeasonNullable.default = ""）。

PlayerGameLogs 參數：
  player_id_nullable          — 球員 ID（選填；留空抓全聯盟）
  season_nullable             — 球季，如 "2024-25"（必填非空）
  season_type_nullable        — "Regular Season"（必填非空）/ "Playoffs" /
                                "Pre Season" / "PlayIn"
  per_mode_simple_nullable    — "Totals"（必填非空）/ "PerGame"
  po_round_nullable           — 季後賽輪次："1"=首輪, "2"=次輪,
                                "3"=分區決賽, "4"=總冠軍賽（留空=全部）
  date_from_nullable          — 起始日期 "YYYY-MM-DD"
  date_to_nullable            — 結束日期 "YYYY-MM-DD"
  last_n_games_nullable       — 最近 N 場（"0"=全部）
  month_nullable              — 月份（"0"=全部，"1"-"12"=指定月）
  outcome_nullable            — "W" / "L"
  location_nullable           — "Home" / "Road"
  season_segment_nullable     — "Pre All-Star" / "Post All-Star"
  vs_conference_nullable      — "East" / "West"
  opp_team_id_nullable        — 對手球隊 ID

獨有欄位說明（PlayerGameLog 無此欄）：
  PLAYER_NAME / TEAM_ABBREVIATION / TEAM_NAME
  BLKA        — 被封蓋次數
  PFD         — 被犯規次數
  NBA_FANTASY_PTS — NBA 官方奇幻分數
  DD2 / TD3   — 雙十 / 大三元（當場是否達成）
  *_RANK      — 各項目當場全聯盟排名
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playergamelogs import PlayerGameLogs
from nba_api.stats.library.parameters import Season
from nba_api.stats.static import players

TIMEOUT      = 60
RETRIES      = 3
RETRY_DELAY  = 5

SEASON_TYPE_REGULAR  = "Regular Season"
SEASON_TYPE_PLAYOFFS = "Playoffs"
DEFAULT_PER_MODE     = "Totals"

NUM_COLS = [
    "MIN", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB",
    "AST", "TOV", "STL", "BLK", "BLKA", "PF", "PFD",
    "PTS", "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3",
]
DISPLAY_COLS = [
    "GAME_DATE", "MATCHUP", "WL", "MIN", "PTS", "REB", "AST",
    "STL", "BLK", "BLKA", "PFD", "FG_PCT", "FG3_PCT",
    "PLUS_MINUS", "NBA_FANTASY_PTS", "DD2", "TD3",
]
STAT_ITEMS = [
    ("PTS",           "得分"),
    ("REB",           "籃板"),
    ("AST",           "助攻"),
    ("STL",           "抄截"),
    ("BLK",           "火鍋"),
    ("BLKA",          "被封蓋"),
    ("PFD",           "被犯規"),
    ("TOV",           "失誤"),
    ("MIN",           "上場時間"),
    ("FG_PCT",        "投籃%"),
    ("FG3_PCT",       "三分%"),
    ("FT_PCT",        "罰球%"),
    ("PLUS_MINUS",    "正負值"),
    ("NBA_FANTASY_PTS","奇幻分"),
]
RANK_COLS = [
    "PTS_RANK", "REB_RANK", "AST_RANK", "STL_RANK", "BLK_RANK",
    "FG_PCT_RANK", "FG3_PCT_RANK", "PLUS_MINUS_RANK", "NBA_FANTASY_PTS_RANK",
]
PO_ROUND_LABEL = {
    "1": "第一輪（首輪）",
    "2": "第二輪",
    "3": "分區決賽",
    "4": "總冠軍賽",
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


def fetch_game_logs(
    player_id: int | str = "",
    season: str = Season.default,
    season_type: str = SEASON_TYPE_REGULAR,
    per_mode: str = DEFAULT_PER_MODE,
    date_from: str = "",
    date_to: str = "",
    last_n_games: str = "0",
    month: str = "0",
    outcome: str = "",
    location: str = "",
    season_segment: str = "",
    vs_conference: str = "",
    opp_team_id: str = "",
    team_id: str = "",
    po_round: str = "",
) -> pd.DataFrame:
    """
    取得比賽記錄，失敗時自動重試。
    season_type / per_mode 必須非空字串，season 必須用 Season.default。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            resp = PlayerGameLogs(
                player_id_nullable=player_id,
                season_nullable=season,
                season_type_nullable=season_type,
                per_mode_simple_nullable=per_mode,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                last_n_games_nullable=last_n_games,
                month_nullable=month,
                outcome_nullable=outcome,
                location_nullable=location,
                season_segment_nullable=season_segment,
                game_segment_nullable="",
                period_nullable="",
                shot_clock_range_nullable="",
                vs_conference_nullable=vs_conference,
                vs_division_nullable="",
                opp_team_id_nullable=opp_team_id,
                team_id_nullable=team_id,
                po_round_nullable=po_round,
                timeout=TIMEOUT,
            )
            break
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)

    df = resp.player_game_logs.get_data_frame()
    if df.empty:
        return df

    existing_num = [c for c in NUM_COLS if c in df.columns]
    df[existing_num] = df[existing_num].apply(pd.to_numeric, errors="coerce")
    df["GAME_DATE"]  = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    df["HOME_AWAY"]  = df["MATCHUP"].apply(
        lambda m: "主場" if "vs." in str(m) else "客場"
    )
    df["OPP"] = df["MATCHUP"].str.extract(r"(?:vs\.|@)\s+(\w+)")
    return df.sort_values("GAME_DATE").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 1. 比賽記錄總表（含獨有欄位）
# ---------------------------------------------------------------------------

def show_game_log(df: pd.DataFrame, label: str, last_n: int = 20) -> None:
    """印出最近 N 場記錄（最新在前），含 PlayerGameLogs 獨有欄位。"""
    if df.empty:
        print(f"── {label}比賽記錄：無資料\n")
        return
    recent = df.tail(last_n).iloc[::-1].copy()
    recent["GAME_DATE"] = recent["GAME_DATE"].dt.strftime("%Y-%m-%d")
    available = [c for c in DISPLAY_COLS if c in recent.columns]
    print(f"── {label}最近 {min(last_n, len(df))} 場記錄（最新在前）" + "─" * 24)
    print(recent[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 數據總覽（含 DD2 / TD3 / 奇幻分）
# ---------------------------------------------------------------------------

def show_season_summary(df: pd.DataFrame, label: str) -> None:
    """場均 / 最高 / 最低 / 中位，含雙十 / 大三元 / 奇幻分。"""
    if df.empty:
        print(f"── {label}總覽：無資料\n")
        return

    gp   = len(df)
    wins = int((df["WL"] == "W").sum())
    dd2  = int(df["DD2"].sum()) if "DD2" in df.columns else 0
    td3  = int(df["TD3"].sum()) if "TD3" in df.columns else 0

    print(f"── {label}數據總覽（共 {gp} 場）" + "─" * 38)
    print(f"  出賽場次 : {gp}  |  球隊勝 {wins}  負 {gp-wins}  "
          f"（勝率 {wins/gp*100:.1f}%）")
    print(f"  雙  十   : {dd2} 場  ({dd2/gp*100:.1f}%)")
    print(f"  大三元   : {td3} 場  ({td3/gp*100:.1f}%)")
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
# 3. 主客場分析（含 BLKA / PFD）
# ---------------------------------------------------------------------------

def show_home_away(df: pd.DataFrame, label: str) -> None:
    """主客場場均數據，含 BLKA（被封蓋）/ PFD（被犯規）。"""
    if df.empty or "HOME_AWAY" not in df.columns:
        return
    rows = []
    for venue, grp in df.groupby("HOME_AWAY"):
        gp   = len(grp)
        wins = int((grp["WL"] == "W").sum())
        row  = {"地點": venue, "場次": gp, "勝率%": round(wins / gp * 100, 1)}
        for col, stat_lbl in [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                               ("BLK","火鍋"), ("BLKA","被封蓋"), ("PFD","被犯規"),
                               ("FG_PCT","投籃%"), ("PLUS_MINUS","正負")]:
            if col in grp.columns:
                row[stat_lbl] = round(grp[col].mean(), 2)
        rows.append(row)
    cmp = pd.DataFrame(rows).set_index("地點")
    print(f"── {label}主客場分析（含 BLKA / PFD）" + "─" * 28)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 4. 全聯盟排名統計
# ---------------------------------------------------------------------------

def show_rank_summary(df: pd.DataFrame, label: str) -> None:
    """各場在全聯盟的排名中位數 / 最佳 / 最差。"""
    if df.empty:
        return
    available = [c for c in RANK_COLS if c in df.columns]
    if not available:
        return
    rank_df = df[available].apply(pd.to_numeric, errors="coerce")
    summary = pd.DataFrame({
        "中位排名": rank_df.median(),
        "最佳排名": rank_df.min(),
        "最差排名": rank_df.max(),
    }).rename(index=lambda c: c.replace("_RANK", ""))
    print(f"── {label}全聯盟場次排名（數字越小 = 排名越前）" + "─" * 20)
    print(summary.to_string())
    print()


# ---------------------------------------------------------------------------
# 5. 勝敗場對比
# ---------------------------------------------------------------------------

def show_win_loss_split(df: pd.DataFrame, label: str) -> None:
    """勝場 vs 敗場場均對比。"""
    if df.empty:
        return
    rows = []
    for wl_label, mask in [("勝場(W)", df["WL"] == "W"), ("敗場(L)", df["WL"] == "L")]:
        grp = df[mask]
        if grp.empty:
            continue
        row = {"勝負": wl_label, "場次": len(grp)}
        for col, stat_lbl in [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                               ("TOV","失誤"), ("FG_PCT","投籃%"),
                               ("PLUS_MINUS","正負"), ("NBA_FANTASY_PTS","奇幻分")]:
            if col in grp.columns:
                row[stat_lbl] = round(grp[col].mean(), 2)
        rows.append(row)
    if not rows:
        return
    cmp = pd.DataFrame(rows).set_index("勝負")
    print(f"── {label}勝場 vs 敗場場均對比 " + "─" * 36)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 6. 大三元明細（季後賽尤其珍貴）
# ---------------------------------------------------------------------------

def show_td3_detail(df: pd.DataFrame, label: str) -> None:
    """印出大三元場次明細。"""
    if df.empty or "TD3" not in df.columns:
        return
    td_games = df[df["TD3"] == 1].copy()
    if td_games.empty:
        return
    td_games["GAME_DATE"] = td_games["GAME_DATE"].dt.strftime("%Y-%m-%d")
    cols = ["GAME_DATE", "MATCHUP", "WL", "PTS", "REB", "AST", "STL", "BLK"]
    available = [c for c in cols if c in td_games.columns]
    print(f"── {label}大三元明細（共 {len(td_games)} 場）" + "─" * 34)
    print(td_games[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 季後賽各輪次分析（po_round_nullable，PlayerGameLogs 獨有）
# ---------------------------------------------------------------------------

def show_playoff_by_round(
    player_id: int, season: str
) -> None:
    """
    利用 po_round_nullable 分輪次抓取季後賽數據，
    逐輪呈現：比賽記錄 / 場均 / 雙十 / 奇幻分。
    """
    print("── 季後賽各輪次分析（po_round_nullable）" + "─" * 26)

    all_rows = []
    for round_num, round_label in PO_ROUND_LABEL.items():
        try:
            df = fetch_game_logs(
                player_id=player_id,
                season=season,
                season_type=SEASON_TYPE_PLAYOFFS,
                po_round=round_num,
            )
        except Exception as e:
            print(f"  {round_label} 取得失敗: {e}")
            continue

        if df.empty:
            continue

        gp   = len(df)
        wins = int((df["WL"] == "W").sum())
        dd2  = int(df["DD2"].sum()) if "DD2" in df.columns else 0
        td3  = int(df["TD3"].sum()) if "TD3" in df.columns else 0

        row = {
            "輪次": round_label,
            "場次": gp,
            "勝": wins,
            "敗": gp - wins,
            "DD2": dd2,
            "TD3": td3,
        }
        for col, stat_lbl in [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                               ("FG_PCT","投籃%"), ("FG3_PCT","三分%"),
                               ("PLUS_MINUS","正負"), ("NBA_FANTASY_PTS","奇幻分")]:
            if col in df.columns:
                row[stat_lbl] = round(df[col].mean(), 2)
        all_rows.append(row)

    if not all_rows:
        print("  無各輪次資料（可能季後賽未開打或輪次篩選無結果）\n")
        return

    result = pd.DataFrame(all_rows).set_index("輪次")
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 8. 常規賽專屬：上下半季 / 對東西區（api 篩選示範）
# ---------------------------------------------------------------------------

def show_regular_season_splits(player_id: int, season: str) -> None:
    """常規賽獨有篩選：全明星賽前後 + 對東西區場均對比。"""
    # 上下半季
    print("── 全明星賽前 vs 後場均對比（season_segment）" + "─" * 22)
    rows = []
    for seg, seg_lbl in [("Pre All-Star", "明星賽前"), ("Post All-Star", "明星賽後")]:
        try:
            df = fetch_game_logs(player_id=player_id, season=season,
                                 season_segment=seg)
            if df.empty:
                continue
            gp   = len(df)
            wins = int((df["WL"] == "W").sum())
            row  = {"時段": seg_lbl, "場次": gp, "勝率%": round(wins/gp*100, 1)}
            for col, lbl in [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                              ("FG_PCT","投籃%"), ("PLUS_MINUS","正負"),
                              ("NBA_FANTASY_PTS","奇幻分")]:
                if col in df.columns:
                    row[lbl] = round(df[col].mean(), 2)
            rows.append(row)
        except Exception as e:
            print(f"  {seg_lbl} 取得失敗: {e}")
    if rows:
        print(pd.DataFrame(rows).set_index("時段").to_string())
    print()

    # 對東西區
    print("── 對東 vs 對西區場均對比（vs_conference）" + "─" * 24)
    rows = []
    for conf, conf_lbl in [("East", "對東區"), ("West", "對西區")]:
        try:
            df = fetch_game_logs(player_id=player_id, season=season,
                                 vs_conference=conf)
            if df.empty:
                continue
            gp   = len(df)
            wins = int((df["WL"] == "W").sum())
            row  = {"對手": conf_lbl, "場次": gp, "勝率%": round(wins/gp*100, 1)}
            for col, lbl in [("PTS","得分"), ("REB","籃板"), ("AST","助攻"),
                              ("FG_PCT","投籃%"), ("PLUS_MINUS","正負")]:
                if col in df.columns:
                    row[lbl] = round(df[col].mean(), 2)
            rows.append(row)
        except Exception as e:
            print(f"  {conf_lbl} 取得失敗: {e}")
    if rows:
        print(pd.DataFrame(rows).set_index("對手").to_string())
    print()

    # 最近 10 場
    print("── 最近 10 場場均（last_n_games=10，PerGame 模式）" + "─" * 16)
    try:
        df = fetch_game_logs(player_id=player_id, season=season,
                             last_n_games="10", per_mode="PerGame")
        if not df.empty:
            cols = ["PTS", "REB", "AST", "STL", "BLK", "FG_PCT",
                    "FG3_PCT", "PLUS_MINUS", "NBA_FANTASY_PTS"]
            avail = [c for c in cols if c in df.columns]
            print(df[avail].mean().round(2).to_frame(name="近10場場均").to_string())
    except Exception as e:
        print(f"  取得失敗: {e}")
    print()


# ---------------------------------------------------------------------------
# 9. 常規賽 vs 季後賽並排對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_df: pd.DataFrame, po_df: pd.DataFrame
) -> None:
    """常規賽與季後賽場均數據並排，計算差值。"""
    if reg_df.empty or po_df.empty:
        return
    stat_cols = ["PTS", "REB", "AST", "STL", "BLK", "BLKA", "PFD", "TOV",
                 "FG_PCT", "FG3_PCT", "FT_PCT", "PLUS_MINUS", "NBA_FANTASY_PTS"]
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
    query  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    season = sys.argv[2] if len(sys.argv) > 2 else Season.default

    player_id, full_name = _find_player_id(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    reg_df = fetch_game_logs(player_id=player_id, season=season,
                             season_type=SEASON_TYPE_REGULAR)

    if reg_df.empty:
        print("  此球季無常規賽出賽記錄。\n")
    else:
        show_game_log(reg_df,        label="常規賽 ", last_n=20)
        show_season_summary(reg_df,  label="常規賽 ")
        show_home_away(reg_df,       label="常規賽 ")
        show_rank_summary(reg_df,    label="常規賽 ")
        show_win_loss_split(reg_df,  label="常規賽 ")
        show_td3_detail(reg_df,      label="常規賽 ")

        print("=" * 65)
        print("  常規賽進階篩選示範（各需一次額外請求）")
        print("=" * 65 + "\n")
        show_regular_season_splits(player_id, season)

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    po_df = fetch_game_logs(player_id=player_id, season=season,
                            season_type=SEASON_TYPE_PLAYOFFS)

    if po_df.empty:
        print(f"  {full_name} 本季無季後賽出賽記錄（或尚未進入季後賽）。\n")
    else:
        show_game_log(po_df,        label="季後賽 ", last_n=len(po_df))
        show_season_summary(po_df,  label="季後賽 ")
        show_home_away(po_df,       label="季後賽 ")
        show_rank_summary(po_df,    label="季後賽 ")
        show_win_loss_split(po_df,  label="季後賽 ")
        show_td3_detail(po_df,      label="季後賽 ")

        print("=" * 65)
        print("  季後賽各輪次分析（po_round_nullable，PlayerGameLogs 獨有）")
        print("=" * 65 + "\n")
        show_playoff_by_round(player_id, season)

    # ── 常規賽 vs 季後賽並排對比 ─────────────────────────────────────────
    if not reg_df.empty and not po_df.empty:
        print("=" * 65)
        print("  常規賽 vs 季後賽並排對比（含 BLKA / PFD / 奇幻分）")
        print("=" * 65 + "\n")
        show_regular_vs_playoffs_comparison(reg_df, po_df)
