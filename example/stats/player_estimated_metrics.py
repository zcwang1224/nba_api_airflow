"""
nba_api.stats.endpoints.playerestimatedmetrics 範例程式

執行方式：
  python example/stats/player_estimated_metrics.py                       # 本季常規賽
  python example/stats/player_estimated_metrics.py 2023-24               # 指定球季
  python example/stats/player_estimated_metrics.py 2024-25 Playoffs      # 季後賽
  python example/stats/player_estimated_metrics.py 2024-25 Regular LeBron James  # 查詢球員

PlayerEstimatedMetrics 參數：
  league_id    — "00"=NBA（預設）/ "10"=WNBA / "20"=G-League
  season       — 球季字串，如 "2024-25"（預設當季）
  season_type  — "Regular Season"（預設）/ "Pre Season" / "PlayIn"
                 注意：此 endpoint 不支援 "Playoffs"，請改用 PlayerGameLogs

DataSet：
  resp.player_estimated_metrics  — 全聯盟所有球員的估算進階指標，每列一名球員

欄位說明：
  PLAYER_ID         — 球員唯一 ID
  PLAYER_NAME       — 球員姓名
  GP                — 出賽場次
  W / L             — 勝場 / 敗場數
  W_PCT             — 出賽時球隊勝率
  MIN               — 總上場時間（分鐘）

  估算進階指標（Estimated，基於機率模型，與 Advanced 指標略有差異）：
  E_OFF_RATING      — 估算進攻效率（每 100 回合得分，越高越好）
  E_DEF_RATING      — 估算防守效率（每 100 回合失分，越低越好）
  E_NET_RATING      — 估算淨效率 = E_OFF_RATING - E_DEF_RATING
  E_AST_RATIO       — 估算助攻比率（每 100 回合助攻次數）
  E_OREB_PCT        — 估算進攻籃板率（%）
  E_DREB_PCT        — 估算防守籃板率（%）
  E_REB_PCT         — 估算總籃板率（%）
  E_TOV_PCT         — 估算失誤率（%，越低越好）
  E_USG_PCT         — 估算使用率（%，持球進攻時間佔比）
  E_PACE            — 估算攻防節奏（每 48 分鐘回合數）

  排名欄（數字越小排名越前）：
  GP_RANK / W_RANK / L_RANK / W_PCT_RANK / MIN_RANK
  E_OFF_RATING_RANK / E_DEF_RATING_RANK / E_NET_RATING_RANK
  E_AST_RATIO_RANK / E_OREB_PCT_RANK / E_DREB_PCT_RANK / E_REB_PCT_RANK
  E_TOV_PCT_RANK / E_USG_PCT_RANK / E_PACE_RANK
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerestimatedmetrics import PlayerEstimatedMetrics
from nba_api.stats.library.parameters import Season, SeasonType
from nba_api.stats.static import players

TIMEOUT = 60
RETRIES = 3
RETRY_DELAY = 5

METRIC_COLS = [
    "E_OFF_RATING", "E_DEF_RATING", "E_NET_RATING",
    "E_AST_RATIO", "E_OREB_PCT", "E_DREB_PCT", "E_REB_PCT",
    "E_TOV_PCT", "E_USG_PCT", "E_PACE",
]
METRIC_LABEL = {
    "E_OFF_RATING": "進攻效率",
    "E_DEF_RATING": "防守效率",
    "E_NET_RATING": "淨效率",
    "E_AST_RATIO":  "助攻比率",
    "E_OREB_PCT":   "進攻籃板率%",
    "E_DREB_PCT":   "防守籃板率%",
    "E_REB_PCT":    "總籃板率%",
    "E_TOV_PCT":    "失誤率%",
    "E_USG_PCT":    "使用率%",
    "E_PACE":       "節奏",
}
# 值越低越好的指標
LOWER_IS_BETTER = {"E_DEF_RATING", "E_TOV_PCT"}

STR_COLS = ["PLAYER_ID", "PLAYER_NAME"]


# ---------------------------------------------------------------------------
# 資料取得
# ---------------------------------------------------------------------------

def fetch_metrics(
    season: str = Season.default,
    season_type: str = SeasonType.default,
    league_id: str = "00",
    min_gp: int = 10,
) -> pd.DataFrame:
    """
    取得全聯盟球員估算指標，失敗時自動重試。
    min_gp：最低出賽場次門檻（過濾替補出賽極少的球員），預設 10 場。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            resp = PlayerEstimatedMetrics(
                season=season,
                season_type=season_type,
                league_id=league_id,
                timeout=TIMEOUT,
            )
            break
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)

    df = resp.player_estimated_metrics.get_data_frame()
    if df.empty:
        return df

    num_cols = [c for c in df.columns if c not in STR_COLS]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    if min_gp > 0:
        df = df[df["GP"] >= min_gp].copy()

    return df.reset_index(drop=True)


def _find_player_row(df: pd.DataFrame, query: str) -> pd.Series | None:
    """依姓名（模糊）或 player_id 在 DataFrame 中找到對應列。"""
    if query.isdigit():
        matched = df[df["PLAYER_ID"] == int(query)]
    else:
        matched = df[df["PLAYER_NAME"].str.contains(query, case=False, na=False)]
    return matched.iloc[0] if not matched.empty else None


# ---------------------------------------------------------------------------
# 1. 聯盟總覽
# ---------------------------------------------------------------------------

def show_overview(df: pd.DataFrame, season: str, season_type: str) -> None:
    """全聯盟球員數量與各指標分布摘要"""
    print(f"── 聯盟總覽  season={season}  season_type={season_type} " + "─" * 20)
    print(f"  符合門檻球員數 : {len(df)}")
    print(f"  平均出賽場次   : {df['GP'].mean():.1f}")
    print()

    rows = []
    for col in METRIC_COLS:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        rows.append({
            "指標":   METRIC_LABEL.get(col, col),
            "欄位":   col,
            "平均":   round(s.mean(), 2),
            "中位":   round(s.median(), 2),
            "最高":   round(s.max(), 2),
            "最低":   round(s.min(), 2),
            "標準差": round(s.std(), 2),
        })

    print("── 各估算指標分布 " + "─" * 46)
    print(pd.DataFrame(rows).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 各指標排行榜（前 N 名）
# ---------------------------------------------------------------------------

def show_leaderboard(df: pd.DataFrame, top_n: int = 10) -> None:
    """各指標前 N 名球員"""
    base_cols = ["PLAYER_NAME", "GP", "MIN"]

    for col in METRIC_COLS:
        if col not in df.columns:
            continue
        label = METRIC_LABEL.get(col, col)
        ascending = col in LOWER_IS_BETTER
        direction = "（越低越好）" if ascending else "（越高越好）"

        top = (
            df[df[col].notna()]
            .nsmallest(top_n, col) if ascending
            else df[df[col].notna()].nlargest(top_n, col)
        )
        rank_col = col + "_RANK"
        show_cols = base_cols + [col] + ([rank_col] if rank_col in df.columns else [])

        print(f"── {label} {direction} 前 {top_n} 名 " + "─" * 30)
        print(top[show_cols].to_string(index=False))
        print()


# ---------------------------------------------------------------------------
# 3. 特定球員完整指標卡
# ---------------------------------------------------------------------------

def show_player_card(df: pd.DataFrame, query: str) -> None:
    """列出指定球員的所有估算指標及全聯盟排名"""
    row = _find_player_row(df, query)
    if row is None:
        print(f"── 球員查詢「{query}」：找不到符合記錄（可能出賽不足門檻）\n")
        return

    total = len(df)
    print(f"── {row['PLAYER_NAME']} 估算指標卡（聯盟共 {total} 人符合門檻）" + "─" * 14)
    print(f"  出賽場次 : {int(row['GP'])}  |  上場時間 : {row['MIN']:.0f} 分鐘")
    print(f"  球隊勝率 : {row['W_PCT']:.3f}  (W={int(row['W'])} L={int(row['L'])})")
    print()

    for col in METRIC_COLS:
        if col not in df.columns:
            continue
        rank_col = col + "_RANK"
        val  = row.get(col)
        rank = row.get(rank_col)
        label = METRIC_LABEL.get(col, col)
        note  = " ↓佳" if col in LOWER_IS_BETTER else " ↑佳"
        rank_str = f"  #{int(rank)}/{total}" if pd.notna(rank) else ""
        print(f"  {label:<12} ({col:<18}): {val:>8.3f}{rank_str}{note}")
    print()


# ---------------------------------------------------------------------------
# 4. 雙向全能球員（高 NET_RATING + 低 TOV_PCT）
# ---------------------------------------------------------------------------

def show_two_way_players(df: pd.DataFrame, top_n: int = 15) -> None:
    """綜合進攻效率、防守效率、失誤率找出雙向全能球員"""
    required = {"E_NET_RATING", "E_TOV_PCT", "E_USG_PCT"}
    if not required.issubset(df.columns):
        return

    sub = df[df[list(required)].notna().all(axis=1)].copy()

    # 標準化排名（各指標 rank，防守與失誤取反使全部同向）
    sub["NET_RANK"]  = sub["E_NET_RATING"].rank(ascending=False)
    sub["DEF_RANK"]  = sub["E_DEF_RATING"].rank(ascending=True)   # 低分好
    sub["TOV_RANK"]  = sub["E_TOV_PCT"].rank(ascending=True)       # 低失誤好
    sub["COMPOSITE"] = sub["NET_RANK"] + sub["DEF_RANK"] + sub["TOV_RANK"]

    top = sub.nsmallest(top_n, "COMPOSITE")
    cols = ["PLAYER_NAME", "GP", "E_OFF_RATING", "E_DEF_RATING",
            "E_NET_RATING", "E_TOV_PCT", "E_USG_PCT"]
    available = [c for c in cols if c in top.columns]

    print(f"── 雙向全能球員（綜合淨效率 + 防守效率 + 失誤率，前 {top_n} 名）" + "─" * 12)
    print(top[available].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 高使用率球員分析（球隊主攻手）
# ---------------------------------------------------------------------------

def show_high_usage(df: pd.DataFrame, usg_min: float = 25.0, top_n: int = 15) -> None:
    """使用率 >= usg_min% 的球員，依淨效率排序"""
    if "E_USG_PCT" not in df.columns:
        return

    sub = df[df["E_USG_PCT"] >= usg_min].copy()
    cols = ["PLAYER_NAME", "GP", "E_USG_PCT", "E_OFF_RATING",
            "E_NET_RATING", "E_TOV_PCT", "W_PCT"]
    available = [c for c in cols if c in sub.columns]

    print(f"── 高使用率球員（E_USG_PCT ≥ {usg_min}%，共 {len(sub)} 人，依淨效率排序）" + "─" * 8)
    print(
        sub.nlargest(top_n, "E_NET_RATING")[available]
        .to_string(index=False)
    )
    print()


# ---------------------------------------------------------------------------
# 6. 賽季類型對比（常規賽 vs PlayIn）
# ---------------------------------------------------------------------------

def show_season_type_comparison(
    season: str,
    player_query: str,
    league_id: str = "00",
) -> None:
    """同一球季不同賽季類型的指標對比"""
    types = [
        ("Regular Season", "常規賽"),
        ("PlayIn",         "附加賽"),
    ]
    rows = []
    for st, label in types:
        try:
            df = fetch_metrics(season=season, season_type=st,
                               league_id=league_id, min_gp=1)
            if df.empty:
                continue
            row = _find_player_row(df, player_query)
            if row is None:
                continue
            entry = {"賽季類型": label}
            for col in ["GP", "W_PCT"] + METRIC_COLS:
                if col in df.columns:
                    entry[METRIC_LABEL.get(col, col)] = round(row[col], 3) if pd.notna(row.get(col)) else None
            rows.append(entry)
        except Exception as e:
            print(f"  {label} 取得失敗: {e}")

    if not rows:
        print(f"── 賽季類型對比「{player_query}」：無資料\n")
        return

    cmp = pd.DataFrame(rows).set_index("賽季類型")
    print(f"── {player_query} 不同賽季類型估算指標對比 " + "─" * 26)
    print(cmp.T.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

SEASON_TYPE_MAP = {
    "Regular":        SeasonType.regular,
    "RegularSeason":  SeasonType.regular,
    "Playoffs":       SeasonType.regular,   # 此 endpoint 不支援 Playoffs，fallback
    "PlayIn":         SeasonType.playin,
    "PreSeason":      SeasonType.preseason,
    "Pre":            SeasonType.preseason,
}

DEFAULT_PLAYER = "LeBron James"

if __name__ == "__main__":
    season      = sys.argv[1] if len(sys.argv) > 1 else Season.default
    st_arg      = sys.argv[2] if len(sys.argv) > 2 else "Regular"
    player_query = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else DEFAULT_PLAYER

    season_type = SEASON_TYPE_MAP.get(st_arg, SeasonType.regular)
    if st_arg == "Playoffs":
        print("  ※ PlayerEstimatedMetrics 不支援 Playoffs，改用 Regular Season。\n")

    print(f"球季     : {season}")
    print(f"賽季類型 : {season_type}")
    print(f"球員查詢 : {player_query}\n")

    df = fetch_metrics(season=season, season_type=season_type, min_gp=10)

    if df.empty:
        print("此球季無資料。")
        sys.exit(0)

    # 1. 聯盟總覽
    show_overview(df, season, season_type)

    # 2. 各指標排行榜
    show_leaderboard(df, top_n=10)

    # 3. 特定球員指標卡
    show_player_card(df, player_query)

    # 4. 雙向全能球員
    show_two_way_players(df, top_n=15)

    # 5. 高使用率主攻手
    show_high_usage(df, usg_min=25.0, top_n=15)

    # 6. 常規賽 vs PlayIn 對比（常規賽模式下才執行）
    if season_type == SeasonType.regular:
        print("=" * 65)
        print(f"  常規賽 vs PlayIn 指標對比  season={season}")
        print("=" * 65 + "\n")
        show_season_type_comparison(season, player_query)
