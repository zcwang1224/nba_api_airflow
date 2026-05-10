"""
nba_api.stats.endpoints.boxscoremiscv3 範例程式

執行方式：
  python example/stats/box_score_misc_v3.py                      # 本季常規賽最近場次
  python example/stats/box_score_misc_v3.py 2024-25              # 指定球季
  python example/stats/box_score_misc_v3.py 2024-25 Playoffs     # 季後賽
  python example/stats/box_score_misc_v3.py 2024-25 Regular 0022401234  # 指定 game_id

  ── 預設行為：自動取得最近一場，同時附加季後賽示範 ──

BoxScoreMiscV3 與其他 V3 BoxScore 的差異：
  - 只有 2 個 DataSet（無 TeamStarterBenchStats）
  - 欄位為計數型整數，專注「情境得分」與「防守 / 犯規」面向
  - 包含對手相對數據（opp 前綴），可同時分析攻守兩端
  - 適合回答「誰靠快攻得分最多？」「誰最能造成犯規？」等問題

DataSet 總覽（共 2 個）：
  player_stats — 球員雜項數據（每列一名球員）
  team_stats   — 球隊雜項數據（兩隊各一列）

欄位說明（計數型整數）：

  ─── 己方情境得分 ─────────────────────────────────────────────────────
  pointsOffTurnovers    — 對手失誤後的得分（反擊得分）
  pointsSecondChance    — 進攻籃板後的二次進攻得分
  pointsFastBreak       — 快攻得分
  pointsPaint           — 禁區得分

  ─── 對手情境得分（越低越好，代表防守端表現）─────────────────────────
  oppPointsOffTurnovers — 對手從己方失誤後的得分
  oppPointsSecondChance — 對手二次進攻得分
  oppPointsFastBreak    — 對手快攻得分
  oppPointsPaint        — 對手禁區得分

  ─── 防守 / 犯規 ──────────────────────────────────────────────────────
  blocks                — 阻攻（蓋帽）數
  blocksAgainst         — 被蓋帽數
  foulsPersonal         — 個人犯規次數
  foulsDrawn            — 造成對手犯規次數（被犯規）
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.boxscoremiscv3 import BoxScoreMiscV3
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

# PlayerStats 顯示欄位
PLAYER_SITUATION = [
    "nameI", "teamTricode", "position", "minutes",
    "pointsOffTurnovers", "pointsSecondChance", "pointsFastBreak", "pointsPaint",
]
PLAYER_OPP = [
    "nameI", "teamTricode",
    "oppPointsOffTurnovers", "oppPointsSecondChance",
    "oppPointsFastBreak", "oppPointsPaint",
]
PLAYER_DEFENSE = [
    "nameI", "teamTricode", "position",
    "blocks", "blocksAgainst", "foulsPersonal", "foulsDrawn",
]

# TeamStats 顯示欄位
TEAM_CORE = [
    "teamTricode",
    "pointsOffTurnovers", "pointsSecondChance", "pointsFastBreak", "pointsPaint",
    "oppPointsOffTurnovers", "oppPointsSecondChance", "oppPointsFastBreak", "oppPointsPaint",
    "blocks", "blocksAgainst", "foulsPersonal", "foulsDrawn",
]

# 對手欄位（越低越好）
LOWER_BETTER = {
    "oppPointsOffTurnovers", "oppPointsSecondChance",
    "oppPointsFastBreak", "oppPointsPaint",
    "blocksAgainst", "foulsPersonal",
}


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
# BoxScoreMiscV3 取得
# ---------------------------------------------------------------------------

def fetch_boxscore_misc(
    game_id: str,
    start_period: int = 0,
    end_period: int = 0,
    range_type: int = 0,
    start_range: int = 0,
    end_range: int = 0,
) -> BoxScoreMiscV3:
    for attempt in range(1, RETRIES + 1):
        try:
            return BoxScoreMiscV3(
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
# 1. 球員情境得分（pointsOffTurnovers / SecondChance / FastBreak / Paint）
# ---------------------------------------------------------------------------

def show_player_situation_points(resp: BoxScoreMiscV3, label: str = "") -> None:
    """
    印出每位球員在各情境下的得分。
    先發（position 非空）在前，替補在後，按球隊分組。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        print(f"── {label}PlayerStats：無資料\n")
        return

    played = _played(df)
    if played.empty:
        played = df

    avail = [c for c in PLAYER_SITUATION if c in played.columns]
    teams = played["teamTricode"].unique()

    print(f"── {label}球員情境得分" + "─" * 50)
    for team in teams:
        team_df  = played[played["teamTricode"] == team].copy()
        starters = team_df[team_df["position"].notna() & (team_df["position"] != "")]
        bench    = team_df[team_df["position"].isna()  | (team_df["position"] == "")]
        ordered  = pd.concat([starters, bench])
        print(f"\n  {team}（先發: {len(starters)} / 替補: {len(bench)}）")
        print(ordered[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 球員防守數據（blocks / fouls）
# ---------------------------------------------------------------------------

def show_player_defense(resp: BoxScoreMiscV3, label: str = "") -> None:
    """
    印出每位球員的防守與犯規數據。
    blocks     — 主動蓋帽；blocksAgainst — 被蓋帽。
    foulsDrawn — 造成對手犯規的能力（越高代表越能吸引犯規）。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    played = _played(df)
    if played.empty:
        played = df

    avail = [c for c in PLAYER_DEFENSE if c in played.columns]

    # 依 blocks 降序
    col_sort = "blocks"
    if col_sort in played.columns:
        played = played.sort_values(col_sort, ascending=False)

    print(f"── {label}球員防守 / 犯規數據（依蓋帽數排序）" + "─" * 26)
    print(played[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 球員被防守端情境得分（opp 欄位，越低代表防守越好）
# ---------------------------------------------------------------------------

def show_player_opp_points(resp: BoxScoreMiscV3, label: str = "") -> None:
    """
    分析每位球員上場期間對手在各情境下得了多少分。
    oppPointsPaint 高 → 該球員上場時對手禁區得分多（護框弱）。
    oppPointsFastBreak 高 → 該球員上場時快攻失守多。
    """
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    played = _played(df)
    if played.empty:
        played = df

    avail = [c for c in PLAYER_OPP if c in played.columns]
    if len(avail) <= 2:
        return

    # 依 oppPointsPaint 降序（禁區防守壓力最大的球員在前）
    col_sort = "oppPointsPaint"
    if col_sort in played.columns:
        played = played.dropna(subset=[col_sort]).sort_values(col_sort, ascending=False)

    print(f"── {label}球員上場期間對手情境得分（越低代表防守越好）" + "─" * 14)
    print(played[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 球隊情境得分對比（TeamStats）
# ---------------------------------------------------------------------------

def show_team_stats(resp: BoxScoreMiscV3, label: str = "") -> None:
    """
    兩隊情境得分、防守、犯規數據並排，標示各項優勢方。
    opp 欄位代表被對手得分，越低越好；其餘越高越好。
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
                if col in LOWER_BETTER:
                    winner = team_a if v_a < v_b else (team_b if v_b < v_a else "—")
                else:
                    winner = team_a if v_a > v_b else (team_b if v_b > v_a else "—")
            rows.append({
                "雜項指標": col,
                team_a:    int(v_a) if pd.notna(v_a) else None,
                team_b:    int(v_b) if pd.notna(v_b) else None,
                "優勢方":   winner,
            })

        result = pd.DataFrame(rows)
        print(f"── {label}球隊雜項數據對比（TeamStats）" + "─" * 30)
        print(result.to_string(index=False))
    else:
        avail = [c for c in TEAM_CORE if c in df.columns]
        print(f"── {label}球隊雜項數據（TeamStats）" + "─" * 36)
        print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 情境得分亮點（各項最突出球員）
# ---------------------------------------------------------------------------

def show_misc_highlights(resp: BoxScoreMiscV3, label: str = "") -> None:
    """找出本場各雜項指標最突出的球員。"""
    df = _clean(resp.player_stats.get_data_frame())
    if df.empty:
        return

    played = _played(df)
    if played.empty:
        played = df

    metrics = [
        ("pointsPaint",           "禁區得分最多",         False),
        ("pointsFastBreak",       "快攻得分最多",         False),
        ("pointsSecondChance",    "二次進攻得分最多",     False),
        ("pointsOffTurnovers",    "反擊得分最多",         False),
        ("blocks",                "蓋帽最多",             False),
        ("foulsDrawn",            "造成犯規最多",         False),
        ("oppPointsPaint",        "被對手禁區得分最少",   True),
        ("oppPointsFastBreak",    "被對手快攻得分最少",   True),
        ("foulsPersonal",         "個人犯規最少",         True),
    ]

    print(f"── {label}雜項數據亮點 " + "─" * 48)
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
        val  = int(row[col]) if pd.notna(row[col]) else "N/A"
        print(f"  {lbl:<22}: {val:>4}  {name}  ({team})")
    print()


# ---------------------------------------------------------------------------
# 6. 節次雜項數據（start_period / end_period 示範）
# ---------------------------------------------------------------------------

def show_quarter_misc(game_id: str, quarter: int, label: str = "") -> None:
    """取得單節的雜項數據（球隊層級）。"""
    try:
        resp = fetch_boxscore_misc(
            game_id=game_id,
            start_period=quarter,
            end_period=quarter,
            range_type=0,
        )
    except Exception as e:
        print(f"  第 {quarter} 節雜項數據取得失敗: {e}\n")
        return

    df = _clean(resp.team_stats.get_data_frame())
    if df.empty:
        print(f"── {label}第 {quarter} 節：無資料\n")
        return

    period_label = f"Q{quarter}" if quarter <= 4 else f"OT{quarter - 4}"
    avail = [c for c in TEAM_CORE if c in df.columns]

    print(f"── {label}{period_label} 節次球隊雜項數據 " + "─" * 36)
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 常規賽 vs 季後賽球隊雜項對比
# ---------------------------------------------------------------------------

def show_season_type_comparison(
    reg_resp: BoxScoreMiscV3,
    po_resp:  BoxScoreMiscV3,
    team_tricode: str,
) -> None:
    """
    指定球隊在常規賽場次 vs 季後賽場次的雜項數據並排。
    可觀察球隊在季後賽是否更依賴禁區、是否減少快攻失誤等變化。
    """
    compare_cols = [
        "pointsOffTurnovers", "pointsSecondChance", "pointsFastBreak", "pointsPaint",
        "oppPointsOffTurnovers", "oppPointsSecondChance", "oppPointsFastBreak", "oppPointsPaint",
        "blocks", "blocksAgainst", "foulsPersonal", "foulsDrawn",
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
            entry[col] = int(v) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("賽事")
    print(f"── 常規賽 vs 季後賽雜項數據對比（{team_tricode}）" + "─" * 16)
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

    # ── 全場雜項 Boxscore ─────────────────────────────────────────────────
    print("=" * 65)
    print(f"  全場雜項 Boxscore  game_id={game_id}  ({season_type})")
    print("=" * 65 + "\n")

    resp = fetch_boxscore_misc(game_id=game_id)

    show_player_situation_points(resp)
    show_player_defense(resp)
    show_player_opp_points(resp)
    show_team_stats(resp)
    show_misc_highlights(resp)

    # ── 節次雜項（Q1 / Q4）───────────────────────────────────────────────
    print("=" * 65)
    print("  節次雜項數據（start_period / end_period 參數示範）")
    print("=" * 65 + "\n")

    for q in [1, 4]:
        show_quarter_misc(game_id, quarter=q)

    # ── 季後賽 ────────────────────────────────────────────────────────────
    if season_type == "Regular Season":
        print("=" * 65)
        print(f"  季後賽雜項 Boxscore 示範  ({season} Playoffs)")
        print("=" * 65 + "\n")

        print(f"透過 LeagueGameFinder 尋找 {season} Playoffs 場次…")
        po_game_id = find_game_id(season=season, season_type="Playoffs")

        if po_game_id:
            print(f"找到 game_id: {po_game_id}\n")
            po_resp = fetch_boxscore_misc(game_id=po_game_id)

            show_player_situation_points(po_resp, label="季後賽 ")
            show_player_defense(po_resp,          label="季後賽 ")
            show_player_opp_points(po_resp,       label="季後賽 ")
            show_team_stats(po_resp,              label="季後賽 ")
            show_misc_highlights(po_resp,         label="季後賽 ")

            for q in [1, 4]:
                show_quarter_misc(po_game_id, quarter=q, label="季後賽 ")

            # 常規賽 vs 季後賽球隊雜項並排
            reg_df = _clean(resp.team_stats.get_data_frame())
            if not reg_df.empty:
                home_team = reg_df.iloc[0]["teamTricode"]
                print("=" * 65)
                print(f"  常規賽 vs 季後賽雜項數據並排示範（{home_team}）")
                print("=" * 65 + "\n")
                show_season_type_comparison(resp, po_resp, home_team)

        else:
            print(f"  {season} 無季後賽記錄（可能球季尚未結束）。\n")
