# nba_api 可爬取資料參考

## Live 端點（即時資料）

| 端點 | 類別 | 說明 |
|------|------|------|
| `live.nba.endpoints.scoreboard` | `ScoreBoard` | 今日所有比賽比分、節次、球隊戰績 |
| `live.nba.endpoints.boxscore` | `BoxScore` | 指定比賽即時 Box Score（球員/球隊數據、場館、裁判） |
| `live.nba.endpoints.playbyplay` | `PlayByPlay` | 指定比賽逐球記錄 |
| `live.nba.endpoints.odds` | `Odds` | 即時賠率資料 |

> Live 端點只提供今日比賽資料，歷史資料請使用 Stats 端點。

---

## Stats 端點（歷史／統計資料）

### 球員基本資料

| 端點 | 回傳資料 |
|------|---------|
| `CommonAllPlayers` | 所有球員清單（現役＋歷史），含球隊、賽季 |
| `CommonPlayerInfo` | 球員個人資訊、頭條數據 |
| `PlayerIndex` | 球員索引（位置、身高、體重、選秀等） |
| `PlayerAwards` | 球員獲獎紀錄 |
| `PlayerCareerStats` | 職業生涯各賽季累計數據 |
| `PlayerProfileV2` | 球員完整檔案（生涯各賽季、大學、季後賽） |

### 球員單場／賽季數據

| 端點 | 回傳資料 |
|------|---------|
| `PlayerGameLog` | 指定球員單賽季每場數據 |
| `PlayerGameLogs` | 多球員／多賽季每場數據（批量） |
| `PlayerEstimatedMetrics` | 估計效率指標（PACE、PIE 等） |
| `PlayerCompare` | 兩位球員數據對比 |
| `PlayerNextNGames` | 未來 N 場賽程 |

### 球員進階切分（Dashboard）

| 端點 | 說明 |
|------|-----|
| `PlayerDashboardByGeneralSplits` | 主客場、月份、勝負等分組數據 |
| `PlayerDashboardByShootingSplits` | 投籃位置分組 |
| `PlayerDashboardByGameSplits` | 分差、上下半場分組 |
| `PlayerDashboardByLastNGames` | 最近 N 場數據 |
| `PlayerDashboardByClutch` | 關鍵時刻數據 |
| `PlayerDashboardByYearOverYear` | 逐年對比 |
| `PlayerDashboardByTeamPerformance` | 依球隊勝負分組 |

### 球員追蹤數據（Player Tracking）

| 端點 | 回傳資料 |
|------|---------|
| `PlayerDashPtShots` | 出手類型、最近防守者距離 |
| `PlayerDashPtPass` | 助攻傳球（傳出／接收） |
| `PlayerDashPtReb` | 籃板爭搶詳情（距籃框距離、競爭者數） |
| `PlayerDashPtShotDefend` | 防守出手數據 |

### Box Score（單場詳細）

| 端點 | 說明 |
|------|-----|
| `BoxScoreTraditionalV2/V3` | 傳統數據（PTS／REB／AST 等） |
| `BoxScoreAdvancedV2/V3` | 進階數據（TS%、eFG%、Usage 等） |
| `BoxScoreScoringV2/V3` | 得分方式分析 |
| `BoxScoreMiscV2/V3` | 雜項（快攻、二波進攻、犯規等） |
| `BoxScoreFourFactorsV2/V3` | 四要素（eFG%、TOV%、OREB%、FT Rate） |
| `BoxScoreUsageV2/V3` | 使用率、上場時間 |
| `BoxScoreHustleV2` | 拼勁數據（loose balls、deflections 等） |
| `BoxScorePlayerTrackV3` | 球員追蹤（速度、距離） |
| `BoxScoreSummaryV2/V3` | 比賽摘要（最後對決、裁判、得分線等） |
| `HustleStatsBoxScore` | 拼勁 Box Score |
| `GameRotation` | 每節上場輪換紀錄 |

### 球隊資料

| 端點 | 回傳資料 |
|------|---------|
| `CommonTeamRoster` | 球隊名單（球員＋教練） |
| `TeamInfoCommon` | 球隊基本資訊、賽季排名 |
| `TeamDetails` | 球隊歷史、冠軍、退役號碼、名人堂成員 |
| `TeamGameLog` | 球隊單賽季每場數據 |
| `TeamGameLogs` | 多賽季批量 |
| `TeamYearByYearStats` | 球隊逐年數據 |
| `TeamHistoricalLeaders` | 球隊歷史各項目領先球員 |
| `TeamEstimatedMetrics` | 球隊估計效率指標 |
| `FranchiseHistory` | 隊史（含已解散球隊） |
| `FranchiseLeaders` | 隊史各項目領先球員 |

### 球隊進階切分（Dashboard）

| 端點 | 說明 |
|------|-----|
| `TeamDashboardByGeneralSplits` | 主客場、月份等分組 |
| `TeamDashboardByShootingSplits` | 投籃位置 |
| `TeamDashLineups` | 陣容數據（2～5 人組合） |
| `TeamDashPtPass` | 傳球助攻 |
| `TeamDashPtReb` | 籃板詳情 |
| `TeamDashPtShots` | 出手類型 |
| `TeamPlayerOnOffDetails/Summary` | 球員在場／離場影響 |
| `TeamVsPlayer` / `TeamAndPlayersVsPlayers` | 球隊對球員／陣容對比 |

### 聯盟整體數據

| 端點 | 說明 |
|------|-----|
| `LeagueDashPlayerStats` | 全聯盟球員數據排行（60+ 欄位） |
| `LeagueDashTeamStats` | 全聯盟球隊數據 |
| `LeagueDashPlayerClutch` / `LeagueDashTeamClutch` | 關鍵時刻排行 |
| `LeagueDashLineups` | 聯盟陣容效率 |
| `LeagueDashPlayerBioStats` | 球員身體數據統計 |
| `LeagueDashPtStats` | 追蹤數據排行 |
| `LeagueDashPtDefend` / `LeagueDashPtTeamDefend` | 防守追蹤 |
| `LeagueDashPlayerShotLocations` | 球員出手位置熱圖數據 |
| `LeagueDashTeamShotLocations` | 球隊出手位置 |
| `LeagueDashPlayerPtShot` / `LeagueDashTeamPtShot` / `LeagueDashOppPtShot` | 出手類型統計 |
| `LeagueLeaders` | 聯盟各項目領袖 |
| `LeagueStandingsV3` | 聯盟戰績排名 |
| `LeagueSeasonMatchups` | 賽季中球員對位數據 |

### 賽程 & 比賽查詢

| 端點 | 說明 |
|------|-----|
| `ScoreboardV2` / `ScoreboardV3` | 指定日期賽程與比分 |
| `ScheduleLeagueV2` | 整季賽程 |
| `LeagueGameLog` | 全聯盟比賽記錄 |
| `LeagueGameFinder` | 依條件查詢比賽 |
| `CommonPlayoffSeries` | 季後賽對戰組合 |
| `PlayoffPicture` | 季後賽晉級圖 |
| `WinProbabilityPBP` | 逐球勝率變化 |

### 逐球記錄

| 端點 | 說明 |
|------|-----|
| `PlayByPlay` | 指定比賽逐球（event type、分數） |
| `PlayByPlayV3` | 新版，含出手距離、結果、是否投籃 |

### 出手圖（Shot Chart）

| 端點 | 說明 |
|------|-----|
| `ShotChartDetail` | 球員／球隊每次出手座標、結果、距離 |
| `ShotChartLeagueWide` | 全聯盟出手熱圖 |
| `ShotChartLineupDetail` | 陣容出手圖 |

### 選秀資料

| 端點 | 說明 |
|------|-----|
| `DraftHistory` | 歷年選秀記錄 |
| `DraftBoard` | 選秀板 |
| `DraftCombineStats` | 選秀訓練營綜合數據 |
| `DraftCombinePlayerAnthro` | 身體測量（身高、臂展、體重等） |
| `DraftCombineDrillResults` | 體能測試（衝刺、垂跳等） |
| `DraftCombineSpotShooting` | 定點投籃測試 |

### 其他

| 端點 | 說明 |
|------|-----|
| `AllTimeLeadersGrids` | 歷史各項目 Top N（得分、籃板、助攻等 19 類） |
| `SynergyPlayTypes` | Synergy 戰術類型效率 |
| `AssistTracker` | 助攻傳球追蹤 |
| `VideoStatus` | 比賽影片是否可用 |

---

## 常用參數說明

| 參數 | 常見值 | 說明 |
|------|--------|------|
| `season` | `"2024-25"` | 賽季（Stats 端點必填） |
| `season_type_all_star` | `"Regular Season"` / `"Playoffs"` / `"Pre Season"` | 賽季類型 |
| `game_id` | `"0022400512"` | 比賽 ID，Stats 端點用 10 碼字串 |
| `player_id` | `203999` | 球員 ID（整數） |
| `team_id` | `1610612738` | 球隊 ID（整數） |
| `per_mode_simple` | `"PerGame"` / `"Totals"` / `"Per36"` | 數據計算方式 |
| `measure_type_simple` | `"Base"` / `"Advanced"` / `"Misc"` | 數據類型 |

---

## 推薦組合

| 用途 | 端點組合 |
|------|---------|
| 今日即時戰況 | `ScoreBoard` → `BoxScore` → `PlayByPlay` |
| 球員賽季分析 | `PlayerGameLogs` + `PlayerDashboardByGeneralSplits` + `PlayerDashPtShots` |
| 球隊效率分析 | `LeagueDashTeamStats` + `TeamDashLineups` + `TeamPlayerOnOffSummary` |
| 投籃熱圖 | `ShotChartDetail`（含 `LOC_X`、`LOC_Y` 座標） |
| 完整比賽重建 | `BoxScoreTraditionalV3` + `PlayByPlayV3` + `GameRotation` |
| 選秀研究 | `DraftHistory` + `DraftCombinePlayerAnthro` + `DraftCombineDrillResults` |
