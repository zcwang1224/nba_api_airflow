"""
nba_api.stats.endpoints.playerdashptshotdefend 範例程式

執行方式：
  python example/stats/player_dash_pt_shot_defend.py                    # 預設球員，本季
  python example/stats/player_dash_pt_shot_defend.py "Bam Adebayo"      # 依姓名查詢
  python example/stats/player_dash_pt_shot_defend.py 1628389            # 依 player_id
  python example/stats/player_dash_pt_shot_defend.py 1628389 2023-24    # 指定球季

  ── 預設行為：同時顯示常規賽 + 季後賽，最後並排對比 ──

PlayerDashPtShotDefend 聚焦「防守對手出手」數據（Play Type Shot Defend）：
  回傳球員在「緊身防守」（Close Defender）情況下，
  對手各種出手類別的命中率，以及與全聯盟平均的差值。

與其他 PtStats 的關鍵差異：
  - 只有 1 個 DataSet（DefendingShots）
  - 欄位以「對手」數據為主（D_FGM / D_FGA / D_FG_PCT）
  - NORMAL_FG_PCT：對手在無此防守者時的正常命中率（基準線）
  - PCT_PLUSMINUS：D_FG_PCT - NORMAL_FG_PCT
                  「負值越大 = 防守越好」（壓低對手命中率）
                  「正值越大 = 被對手投穿」（對手命中率更高）

DataSet（共 1 個）：
  defending_shots — 防守者緊逼防守各類別的數據

欄位說明：
  CLOSE_DEF_PERSON_ID — 防守者 player_id（即查詢球員）
  GP                  — 出賽場次
  G                   — 場次
  DEFENSE_CATEGORY    — 防守類別（分組欄）：
                        Overall / 2-Pointers / 3-Pointers /
                        Less Than 6 ft / 6-10 ft / 10-16 ft / 16+ ft /
                        2PT Jump Shot / 3PT Jump Shot 等
  FREQ                — 頻率（此類別占對手對此球員出手的比例）
  D_FGM               — 對手命中數
  D_FGA               — 對手出手數
  D_FG_PCT            — 對手命中率（越低 = 防守越好）
  NORMAL_FG_PCT       — 對手正常（不被此球員防守）的命中率基準線
  PCT_PLUSMINUS       — D_FG_PCT - NORMAL_FG_PCT
                        負值 = 壓低對手命中率（好）
                        正值 = 對手命中率反而升高（差）

建議使用防守型球員（如 Bam Adebayo / Draymond Green / Rudy Gobert）
以呈現最具代表性的防守數據。
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints.playerdashptshotdefend import PlayerDashPtShotDefend
from nba_api.stats.library.parameters import (
    PerModeSimple,
    Season,
    SeasonTypeAllStar,
)
from nba_api.stats.static import players

TIMEOUT     = 60
RETRIES     = 3
RETRY_DELAY = 5

SEASON_TYPE_REGULAR  = SeasonTypeAllStar.regular
SEASON_TYPE_PLAYOFFS = SeasonTypeAllStar.playoffs

STR_COLS  = {"CLOSE_DEF_PERSON_ID", "DEFENSE_CATEGORY"}
CORE_COLS = [
    "DEFENSE_CATEGORY", "GP", "G", "FREQ",
    "D_FGM", "D_FGA", "D_FG_PCT",
    "NORMAL_FG_PCT", "PCT_PLUSMINUS",
]


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

def _find_player(query: str) -> tuple[int, str]:
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


def fetch_defend_data(
    player_id: int,
    team_id: int = 0,
    season: str = Season.default,
    season_type: str = SEASON_TYPE_REGULAR,
    per_mode: str = PerModeSimple.totals,
    last_n_games: int = 0,
    month: int = 0,
    period: int = 0,
    date_from: str = "",
    date_to: str = "",
    location: str = "",
    outcome: str = "",
    season_segment: str = "",
    vs_conference: str = "",
) -> PlayerDashPtShotDefend:
    """
    取得 PtShotDefend Dashboard，失敗時自動重試。
    team_id=0 → 不限球隊，涵蓋整季所有場次。
    ※ per_mode 預設 Totals（防守數據以累計次數更直觀）。
    """
    for attempt in range(1, RETRIES + 1):
        try:
            return PlayerDashPtShotDefend(
                team_id=team_id,
                player_id=player_id,
                season=season,
                season_type_all_star=season_type,
                per_mode_simple=per_mode,
                last_n_games=str(last_n_games),
                month=str(month),
                period=str(period),
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                location_nullable=location,
                outcome_nullable=outcome,
                season_segment_nullable=season_segment,
                vs_conference_nullable=vs_conference,
                timeout=TIMEOUT,
            )
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  [第 {attempt} 次嘗試失敗: {e}，{RETRY_DELAY}s 後重試]")
            time.sleep(RETRY_DELAY)


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    num_cols = [c for c in df.columns if c not in STR_COLS]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


def _get_df(resp: PlayerDashPtShotDefend) -> pd.DataFrame:
    """取得清理過的 DefendingShots DataFrame。"""
    return _clean(resp.defending_shots.get_data_frame())


# ---------------------------------------------------------------------------
# 1. 完整防守數據表
# ---------------------------------------------------------------------------

def show_defending_shots(resp: PlayerDashPtShotDefend, label: str) -> None:
    """
    印出全部 DEFENSE_CATEGORY 的防守數據。
    PCT_PLUSMINUS 欄：負值越大 = 防守越好，正值越大 = 被投穿越嚴重。
    """
    df = _get_df(resp)
    if df.empty:
        print(f"── {label}防守數據：無資料\n")
        return

    avail = [c for c in CORE_COLS if c in df.columns]
    print(f"── {label}防守各類別出手數據（DefendingShots）" + "─" * 18)
    print("  ※ PCT_PLUSMINUS：負值越大 = 防守越好（壓低對手命中率）")
    print()
    print(df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 防守效果摘要
# ---------------------------------------------------------------------------

def show_defense_summary(resp: PlayerDashPtShotDefend, label: str) -> None:
    """
    萃取 Overall / 2-Pointers / 3-Pointers 三列的關鍵數字，
    快速呈現球員整體防守效果。
    """
    df = _get_df(resp)
    if df.empty:
        print(f"── {label}防守效果摘要：無資料\n")
        return

    print(f"── {label}防守效果摘要 " + "─" * 46)

    target_cats = ["Overall", "2-Pointers", "3-Pointers"]
    for cat in target_cats:
        row = df[df["DEFENSE_CATEGORY"].str.contains(cat, case=False, na=False)]
        if row.empty:
            continue
        r = row.iloc[0]
        d_fga      = r.get("D_FGA", 0)
        d_fg_pct   = r.get("D_FG_PCT", None)
        normal_pct = r.get("NORMAL_FG_PCT", None)
        plusminus  = r.get("PCT_PLUSMINUS", None)
        freq       = r.get("FREQ", None)

        rating = ""
        if pd.notna(plusminus):
            if plusminus < -0.05:
                rating = "★★★ 優秀"
            elif plusminus < -0.02:
                rating = "★★  良好"
            elif plusminus < 0.02:
                rating = "★   普通"
            elif plusminus < 0.05:
                rating = "△   偏差"
            else:
                rating = "✗   較差"

        print(f"  {cat:<15}: ", end="")
        if pd.notna(d_fg_pct):
            print(f"對手 FG% {d_fg_pct:.3f}", end="")
        if pd.notna(normal_pct):
            print(f"  基準 {normal_pct:.3f}", end="")
        if pd.notna(plusminus):
            sign = "+" if plusminus > 0 else ""
            print(f"  ΔFG% {sign}{plusminus:.3f}", end="")
        if pd.notna(freq):
            print(f"  頻率 {freq:.1%}", end="")
        if pd.notna(d_fga):
            print(f"  (FGA={int(d_fga)})", end="")
        print(f"  {rating}")
    print()


# ---------------------------------------------------------------------------
# 3. 最佳 / 最差防守類別（按 PCT_PLUSMINUS 排序）
# ---------------------------------------------------------------------------

def show_best_worst_defense(resp: PlayerDashPtShotDefend,
                             label: str, min_fga: int = 10) -> None:
    """
    找出防守效果最好與最差的類別。
    min_fga 過濾出手次數過少的類別，避免小樣本誤導。
    """
    df = _get_df(resp)
    if df.empty or "PCT_PLUSMINUS" not in df.columns:
        return

    # 排除 Overall（整體）避免與細分類別重複
    sub = df[
        ~df["DEFENSE_CATEGORY"].str.contains("Overall", case=False, na=False) &
        (df["D_FGA"] >= min_fga)
    ].copy()

    if sub.empty:
        print(f"── {label}最佳/最差防守類別：符合門檻（FGA≥{min_fga}）場次不足\n")
        return

    sub_sorted = sub.sort_values("PCT_PLUSMINUS")
    avail = [c for c in CORE_COLS if c in sub_sorted.columns]

    print(f"── {label}防守效果最佳 3 類（PCT_PLUSMINUS 最小，D_FGA ≥ {min_fga}）" + "─" * 12)
    print(sub_sorted.head(3)[avail].to_string(index=False))
    print()

    print(f"── {label}防守效果最差 3 類（PCT_PLUSMINUS 最大，D_FGA ≥ {min_fga}）" + "─" * 12)
    print(sub_sorted.tail(3)[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 距離帶防守分析
# ---------------------------------------------------------------------------

def show_distance_defense(resp: PlayerDashPtShotDefend, label: str) -> None:
    """
    以距離帶（Less Than 6 ft / 6-10 ft / 10-16 ft / 16+ ft）
    分析球員在各距離的防守效果，判斷是禁區悍將還是外線防守者。
    """
    df = _get_df(resp)
    if df.empty:
        return

    dist_keywords = ["Less Than 6", "6-10", "10-16", "16+", "ft"]
    dist_df = df[df["DEFENSE_CATEGORY"].str.contains("|".join(dist_keywords),
                                                      case=False, na=False)]
    if dist_df.empty:
        print(f"── {label}距離帶防守：無資料\n")
        return

    avail = [c for c in CORE_COLS if c in dist_df.columns]
    print(f"── {label}各距離帶防守效果 " + "─" * 44)
    print("  ※ PCT_PLUSMINUS 負值 = 比基準值低（防守有效）")
    print()
    print(dist_df[avail].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 2PT vs 3PT 防守對比
# ---------------------------------------------------------------------------

def show_2pt_3pt_defense(resp: PlayerDashPtShotDefend, label: str) -> None:
    """
    2 分球 vs 3 分球防守效果對比，
    判斷球員是內線防守型（壓低 2PT）還是外線防守型（壓低 3PT）。
    """
    df = _get_df(resp)
    if df.empty:
        return

    rows = []
    for cat, cat_label in [("2-Pointer", "2 分球防守"), ("3-Pointer", "3 分球防守")]:
        row = df[df["DEFENSE_CATEGORY"].str.contains(cat, case=False, na=False)]
        if row.empty:
            continue
        r = row.iloc[0]
        entry = {"類別": cat_label}
        for col in ["FREQ", "D_FGM", "D_FGA", "D_FG_PCT", "NORMAL_FG_PCT", "PCT_PLUSMINUS"]:
            if col in df.columns:
                v = r.get(col)
                entry[col] = round(float(v), 3) if pd.notna(v) else None
        rows.append(entry)

    if not rows:
        return

    cmp = pd.DataFrame(rows).set_index("類別")
    print(f"── {label}2PT vs 3PT 防守對比 " + "─" * 38)
    print(cmp.to_string())
    print()


# ---------------------------------------------------------------------------
# 6. 常規賽 vs 季後賽防守對比
# ---------------------------------------------------------------------------

def show_regular_vs_playoffs_comparison(
    reg_resp: PlayerDashPtShotDefend,
    po_resp: PlayerDashPtShotDefend,
) -> None:
    """
    常規賽與季後賽在各 DEFENSE_CATEGORY 的防守效果並排，
    觀察球員是否在季後賽防守強度升高時仍維持防守水準。
    """
    reg_df = _get_df(reg_resp)
    po_df  = _get_df(po_resp)

    if reg_df.empty:
        return

    po_map: dict[str, pd.Series] = {}
    if not po_df.empty:
        for _, row in po_df.iterrows():
            po_map[str(row.get("DEFENSE_CATEGORY", ""))] = row

    rows = []
    for _, reg_row in reg_df.iterrows():
        cat = str(reg_row.get("DEFENSE_CATEGORY", "")).strip()
        entry = {"防守類別": cat}
        for col in ["D_FG_PCT", "NORMAL_FG_PCT", "PCT_PLUSMINUS", "FREQ"]:
            r_v = reg_row.get(col)
            entry[f"常規_{col}"] = round(float(r_v), 3) if pd.notna(r_v) else None
            po_row = po_map.get(cat)
            if po_row is not None:
                p_v = po_row.get(col)
                entry[f"季後_{col}"] = round(float(p_v), 3) if pd.notna(p_v) else None
            else:
                entry[f"季後_{col}"] = None
        rows.append(entry)

    if not rows:
        return

    result = pd.DataFrame(rows).set_index("防守類別")
    print("── 常規賽 vs 季後賽防守效果對比 " + "─" * 30)
    print("  ※ PCT_PLUSMINUS 負值越大 = 防守越好")
    print()
    print(result.to_string())
    print()


# ---------------------------------------------------------------------------
# 7. 季後賽各輪次防守效果
# ---------------------------------------------------------------------------

PO_ROUND_LABEL = {
    "1": "第一輪",
    "2": "第二輪",
    "3": "分區決賽",
    "4": "總冠軍賽",
}


def show_playoff_by_round(player_id: int, season: str) -> None:
    """
    逐輪抓取 Overall 防守效果，比較各輪次面對不同對手的防守表現。
    ※ PlayerDashPtShotDefend 無 po_round_nullable 參數，
       透過 last_n_games 近似各輪次（非精確，僅供參考）。
    """
    print("── 季後賽防守整體摘要 " + "─" * 44)
    print("  ※ 此 endpoint 無 po_round_nullable，以下為全季後賽彙總")

    try:
        resp = fetch_defend_data(
            player_id=player_id, team_id=0,
            season=season,
            season_type=SEASON_TYPE_PLAYOFFS,
        )
        df = _get_df(resp)
        if df.empty:
            print("  無資料\n")
            return

        overall = df[df["DEFENSE_CATEGORY"].str.contains("Overall", case=False, na=False)]
        if not overall.empty:
            r = overall.iloc[0]
            avail = [c for c in CORE_COLS if c in df.columns]
            print(overall[avail].to_string(index=False))
    except Exception as e:
        print(f"  取得失敗: {e}")
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

DEFAULT_PLAYER = "Bam Adebayo"   # 頂尖全能防守者，最具代表性
PER_MODE = PerModeSimple.totals  # 防守數據以累計次數最直觀

if __name__ == "__main__":
    query  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PLAYER
    season = sys.argv[2] if len(sys.argv) > 2 else Season.default

    player_id, full_name = _find_player(query)
    print(f"查詢球員 : {full_name}  (player_id={player_id})")
    print(f"球季     : {season}  |  PerMode: {PER_MODE}")
    print(f"team_id  : 0（不限球隊）")
    print(f"※ 建議使用防守型球員（如 Bam Adebayo / Draymond Green / Rudy Gobert）\n")

    # ── 常規賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  常規賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    reg_resp = fetch_defend_data(
        player_id=player_id, team_id=0,
        season=season, season_type=SEASON_TYPE_REGULAR,
        per_mode=PER_MODE,
    )

    show_defending_shots(reg_resp,       label="常規賽 ")
    show_defense_summary(reg_resp,       label="常規賽 ")
    show_2pt_3pt_defense(reg_resp,       label="常規賽 ")
    show_distance_defense(reg_resp,      label="常規賽 ")
    show_best_worst_defense(reg_resp,    label="常規賽 ", min_fga=10)

    # ── 季後賽 ────────────────────────────────────────────────────────────
    print("=" * 65)
    print(f"  季後賽  {full_name}  ({season})")
    print("=" * 65 + "\n")

    po_resp = None
    try:
        po_resp = fetch_defend_data(
            player_id=player_id, team_id=0,
            season=season, season_type=SEASON_TYPE_PLAYOFFS,
            per_mode=PER_MODE,
        )

        po_df = po_resp.defending_shots.get_data_frame()
        if po_df.empty:
            print(f"  {full_name} 本季無季後賽防守數據（或尚未進入季後賽）。\n")
            po_resp = None
        else:
            show_defending_shots(po_resp,     label="季後賽 ")
            show_defense_summary(po_resp,     label="季後賽 ")
            show_2pt_3pt_defense(po_resp,     label="季後賽 ")
            show_distance_defense(po_resp,    label="季後賽 ")
            show_best_worst_defense(po_resp,  label="季後賽 ", min_fga=5)

    except Exception as e:
        print(f"  季後賽資料取得失敗: {e}\n")

    # ── 常規賽 vs 季後賽防守效果並排 ─────────────────────────────────────
    if po_resp is not None:
        print("=" * 65)
        print("  常規賽 vs 季後賽防守效果對比")
        print("=" * 65 + "\n")

        show_regular_vs_playoffs_comparison(reg_resp, po_resp)
        show_playoff_by_round(player_id, season)
