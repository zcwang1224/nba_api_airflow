# CLAUDE.md

## 專案目標
爬取 NBA 數據，保存到資料庫做分析

## 技術堆疊
- 語言: Python 3.10+
- 套件管理: pip
- 排程管理: Airflow 2.9.3
- 資料庫: PostgreSQL/MongoDB
- 容器: Docker / DevContainer

## 專案結構
```
nba_airflow-devcontainer/
├── .devcontainer/
│   ├── devcontainer.json   # VSCode DevContainer 設定
│   └── Dockerfile          # 自訂 Airflow 映像
├── dags/                   # Airflow DAG 定義
├── history-data/           # 取得今日以前數據
├── example/                # nba_api 範例程式
├── logs/                   # Airflow 執行日誌（不提交）
├── plugins/                # Airflow 自訂 Plugin
├── config/                 # Airflow 設定檔
├── docker-compose.yml      # 服務編排
├── pyproject.toml          # Poetry 依賴管理
├── .env                    # 環境變數（不提交）
└── .gitignore
```

## 開發環境啟動

### 使用 DevContainer（推薦）
1. 安裝 VSCode + Dev Containers 擴充套件
2. 開啟專案，點選「Reopen in Container」
3. 等待 container 建置完成
4. 瀏覽器開啟 http://localhost:8080（帳密: airflow / airflow）

### 手動啟動
```bash
# 設定 UID（Linux 必要）
echo "AIRFLOW_UID=$(id -u)" >> .env

# 初始化並啟動
docker compose up airflow-init
docker compose up -d

# 停止
docker compose down
```

## Airflow 設定
- Executor: LocalExecutor
- Metastore: PostgreSQL
- Webserver: http://localhost:8080
- 預設帳密: airflow / airflow

## 常用指令
```bash
# 進入 airflow-webserver container
docker compose exec airflow-webserver bash

# 查看 DAG 清單
docker compose exec airflow-webserver airflow dags list

# 手動觸發 DAG
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```
