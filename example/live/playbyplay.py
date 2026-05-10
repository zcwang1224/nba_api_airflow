"""
nba_api.live.nba.endpoints.playbyplay 範例程式

執行方式：
  python example/live/playbyplay.py              # 自動選取今日比賽
  python example/live/playbyplay.py 0042400306   # 指定 game_id

PlayByPlay 資料結構：
  pbp.game         — 比賽識別（gameId, gameEt, duration 等）
  pbp.home_team    — 主隊基本資訊（teamId, teamCity, teamName, teamTricode）
  pbp.away_team    — 客隊基本資訊
  pbp.officials    — 裁判名單
  pbp.actions      — DataSet，每筆為一個 action dict（list[dict]）

action dict 主要欄位：
  actionNumber     — 動作流水號（整場唯一）
  orderNumber      — 排序依據（比 actionNumber 更精確）
  clock            — 節次剩餘時間（"PT05M30.00S"）
  timeActual       — UTC 絕對時間（ISO 字串）
  period           — 節次（1–4；延長賽為 5+）
  periodType       — "REGULAR" / "OVERTIME"
  teamTricode      — 執行動作的隊伍縮寫
  playerNameI      — 球員縮寫姓名（"L. James"）
  actionType       — 動作大類（"2pt", "3pt", "freethrow", "rebound", "turnover" …）
  subType          — 動作細項（"jump shot", "layup", "missed" …）
  descriptor       — 附加描述（"driving", "cutting" …）
  qualifiers       — list[str]，額外修飾（["pointsinthepaint", "fromturnover"] …）
  description      — 完整文字描述（NBA 官方格式）
  isFieldGoal      — 1 = 投籃動作（含未進），0 = 非投籃
  scoreHome        — 執行後主隊累計分（字串；未得分時為空字串）
  scoreAway        — 執行後客隊累計分
  possession       — 持球隊伍 teamId
  x, y             — 場地座標（0–100 比例，可為 None）
  xLegacy, yLegacy — 舊版座標系（英吋）

常見 actionType：
  jumpball, 2pt, 3pt, freethrow, rebound, turnover,
  foul, violation, substitution, timeout, period, game
"""

import re
import sys
from collections import defaultdict

import pandas as pd
from nba_api.live.nba.endpoints import playbyplay
from nba_api.live.nba.endpoints.scoreboard import ScoreBoard


# ---------------------------------------------------------------------------
# 工具函式
# ---------------------------------------------------------------------------

STATUS = {1: "未開始", 2: "進行中", 3: "Final"}

SCORING_TYPES = {"2pt", "3pt", "freethrow"}


def _parse_clock(raw: str) -> str:
    """'PT05M30.00S' → '5:30'"""
    if not raw:
        return ""
    m = re.match(r"PT(\d+)M([\d.]+)S", raw)
    return f"{int(m.group(1))}:{int(float(m.group(2))):02d}" if m else raw


def _pick_game_id() -> tuple[str, str, str]:
    """從今日賽程選一場有資料的比賽（優先進行中，其次已結束）
    回傳 (game_id, away_tricode, home_tricode)
    """
    games = ScoreBoard().get_dict()["scoreboard"]["games"]
    if not games:
        print("今日無賽事，請手動指定 game_id。")
        sys.exit(0)
    target = (
        next((g for g in games if g["gameStatus"] == 2), None)
        or next((g for g in games if g["gameStatus"] == 3), None)
        or games[0]
    )
    away = target["awayTeam"]["teamTricode"]
    home = target["homeTeam"]["teamTricode"]
    status = STATUS.get(target["gameStatus"], "?")
    print(f"選取比賽: {away} @ {home}  ({status})  game_id={target['gameId']}\n")
    return target["gameId"], away, home


def _team_tricodes_from_scoreboard(game_id: str) -> tuple[str, str]:
    """透過 ScoreBoard 查詢指定 game_id 的隊伍縮寫，回傳 (away, home)"""
    games = ScoreBoard().get_dict()["scoreboard"]["games"]
    for g in games:
        if g["gameId"] == game_id:
            return g["awayTeam"]["teamTricode"], g["homeTeam"]["teamTricode"]
    return "AWAY", "HOME"


def _build_df(actions: list[dict]) -> pd.DataFrame:
    """將 actions list 轉為整理好的 DataFrame"""
    rows = []
    for a in actions:
        score_away = str(a.get("scoreAway", "")).strip()
        score_home = str(a.get("scoreHome", "")).strip()
        rows.append({
            "no":          a.get("actionNumber"),
            "period":      a.get("period"),
            "period_type": a.get("periodType", "REGULAR"),
            "clock":       _parse_clock(a.get("clock", "")),
            "team":        a.get("teamTricode", ""),
            "player":      a.get("playerNameI", ""),
            "action":      a.get("actionType", ""),
            "sub_type":    a.get("subType", ""),
            "descriptor":  a.get("descriptor", ""),
            "qualifiers":  ", ".join(a.get("qualifiers") or []),
            "description": a.get("description", ""),
            "is_fg":       bool(a.get("isFieldGoal")),
            "score_away":  score_away,
            "score_home":  score_home,
            "score":       f"{score_away}-{score_home}" if score_away and score_home else "",
            "x":           a.get("x"),
            "y":           a.get("y"),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. 完整逐球記錄
# ---------------------------------------------------------------------------

DISPLAY_COLS = ["no", "period", "clock", "team", "player", "action", "sub_type", "description", "score"]


def show_all_actions(df: pd.DataFrame, last_n: int | None = None) -> None:
    """印出全場逐球記錄（可指定最後 N 筆）"""
    out = df.tail(last_n).reset_index(drop=True) if last_n else df
    title = f"最後 {last_n} 筆" if last_n else f"全場共 {len(df)} 筆"
    print(f"── 逐球記錄（{title}）" + "─" * 40)
    print(out[DISPLAY_COLS].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 2. 依節次篩選
# ---------------------------------------------------------------------------

def show_by_period(df: pd.DataFrame, period: int) -> None:
    """印出指定節次的逐球記錄"""
    out = df[df["period"] == period]
    label = f"Q{period}" if period <= 4 else f"OT{period - 4}"
    print(f"── {label} 逐球記錄（共 {len(out)} 筆）" + "─" * 38)
    if out.empty:
        print(f"  {label} 無資料")
    else:
        print(out[DISPLAY_COLS].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 3. 得分事件
# ---------------------------------------------------------------------------

def show_scoring_plays(df: pd.DataFrame) -> None:
    """只顯示實際得分的動作（投進 + 罰球進）"""
    # 投籃進：isFieldGoal=True 且 subType 不含 "missed"
    # 罰球進：actionType=="freethrow" 且 subType 不含 "missed"
    made = df[
        df["action"].isin(SCORING_TYPES) &
        ~df["sub_type"].str.contains("missed", case=False, na=False)
    ].copy()

    print(f"── 得分事件（共 {len(made)} 筆）" + "─" * 42)
    if made.empty:
        print("  無得分記錄")
    else:
        cols = ["period", "clock", "team", "player", "action", "sub_type", "description", "score"]
        print(made[cols].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 4. 球員動作彙整
# ---------------------------------------------------------------------------

def show_player_actions(df: pd.DataFrame, player_name: str) -> None:
    """印出指定球員（模糊比對）的所有動作"""
    out = df[df["player"].str.contains(player_name, case=False, na=False)]
    print(f"── {player_name} 的所有動作（共 {len(out)} 筆）" + "─" * 35)
    if out.empty:
        print(f"  找不到包含 '{player_name}' 的球員")
    else:
        print(out[DISPLAY_COLS].to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 5. 各隊動作類型統計
# ---------------------------------------------------------------------------

def show_action_summary(df: pd.DataFrame) -> None:
    """各隊 actionType 次數統計"""
    team_df = df[df["team"] != ""].copy()
    summary = (
        team_df.groupby(["team", "action"])
        .size()
        .reset_index(name="count")
        .sort_values(["team", "count"], ascending=[True, False])
    )
    print("── 各隊動作類型統計 " + "─" * 44)
    print(summary.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 6. 領先變化追蹤
# ---------------------------------------------------------------------------

def show_lead_changes(df: pd.DataFrame, home: str, away: str) -> None:
    """列出每一次領先易主的時間點"""
    scored = df[df["score"] != ""].copy()
    if scored.empty:
        print("── 領先變化：無得分資料\n")
        return

    def _to_int(s: str) -> int:
        try:
            return int(s)
        except (ValueError, TypeError):
            return 0

    scored["s_away"] = scored["score_away"].apply(_to_int)
    scored["s_home"] = scored["score_home"].apply(_to_int)
    scored["diff"]   = scored["s_home"] - scored["s_away"]   # 正 = 主隊領先
    scored["leader"] = scored["diff"].apply(
        lambda d: home if d > 0 else (away if d < 0 else "TIED")
    )

    changes = []
    prev_leader = None
    for _, row in scored.iterrows():
        if row["leader"] != prev_leader:
            changes.append({
                "period":      row["period"],
                "clock":       row["clock"],
                "leader":      row["leader"],
                "score":       row["score"],
                "margin":      abs(int(row["diff"])),
                "description": row["description"],
            })
            prev_leader = row["leader"]

    print(f"── 領先變化（共 {len(changes)} 次）" + "─" * 42)
    if changes:
        print(pd.DataFrame(changes).to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# 7. 節次得分統計
# ---------------------------------------------------------------------------

def show_quarter_scores(df: pd.DataFrame, home: str, away: str) -> None:
    """各節得分（從得分事件反推每節得分差）"""
    made = df[
        df["action"].isin(SCORING_TYPES) &
        ~df["sub_type"].str.contains("missed", case=False, na=False) &
        (df["score"] != "")
    ].copy()

    if made.empty:
        print("── 節次得分：無資料\n")
        return

    def _to_int(s: str) -> int:
        try:
            return int(s)
        except (ValueError, TypeError):
            return 0

    made["s_away"] = made["score_away"].apply(_to_int)
    made["s_home"] = made["score_home"].apply(_to_int)

    rows = []
    prev_away, prev_home = 0, 0
    for period, grp in made.groupby("period"):
        last = grp.iloc[-1]
        q_away = last["s_away"] - prev_away
        q_home = last["s_home"] - prev_home
        label  = f"Q{period}" if period <= 4 else f"OT{period - 4}"
        rows.append({"period": label, away: q_away, home: q_home})
        prev_away, prev_home = last["s_away"], last["s_home"]

    # 加總行
    total_row = pd.DataFrame(rows).set_index("period")
    total_row.loc["TOTAL"] = total_row.sum()

    print(f"── 節次得分（{away} / {home}）" + "─" * 42)
    print(total_row.to_string())
    print()


# ---------------------------------------------------------------------------
# 主程式
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) > 1:
        game_id = sys.argv[1]
        away_tricode, home_tricode = _team_tricodes_from_scoreboard(game_id)
        print(f"選取比賽: {away_tricode} @ {home_tricode}  game_id={game_id}\n")
    else:
        game_id, away_tricode, home_tricode = _pick_game_id()

    pbp = playbyplay.PlayByPlay(game_id=game_id)

    # PlayByPlay 只有 actions DataSet 被實際填充；隊伍資訊由 ScoreBoard 提供
    actions = pbp.actions.get_dict()
    df = _build_df(actions)
    print(f"共載入 {len(df)} 筆 action\n")

    # 1. 全場最後 20 筆
    show_all_actions(df, last_n=20)

    # 2. 第一節完整記錄
    show_by_period(df, period=1)

    # 3. 得分事件
    show_scoring_plays(df)

    # 4. 各隊動作統計
    show_action_summary(df)

    # 5. 領先變化
    show_lead_changes(df, home=home_tricode, away=away_tricode)

    # 6. 節次得分
    show_quarter_scores(df, home=home_tricode, away=away_tricode)
