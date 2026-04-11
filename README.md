# congressTrack — 國會交易追蹤系統

即時 / 離線追蹤美國參眾議員、台灣立委與縣市議員的股票交易申報，以及知名機構 13F 持股。

---

## 目錄結構

```
congressTrack/
├── online/                         # ── 線上版（即時抓取）──
│   └── app.py                      #   Streamlit App 入口
├── offline/                        # ── 離線版（讀本地 data.db）──
│   ├── app.py                      #   Streamlit App 入口
│   └── sync_data.py                #   ETL 同步腳本：抓資料 → 寫入 data.db
├── confpath.py                     # 共用路徑常數（PROJECT_ROOT / DB 路徑）
├── senate_efd.py                   # 共用：美國參議院 eFD PTR 爬蟲
├── funds_13f.py                    # 共用：11 檔機構 13F 持股抓取
├── ticker_info.db                  # 共用：標的說明 SQLite
├── data.db                         # 離線版資料庫（同步後產生，不進 git）
├── requirements.txt                # Python 套件
├── .gitignore
├── .streamlit/
│   └── secrets.toml.example        # Streamlit Cloud Secrets 範本
├── scripts/
│   └── upload_data_db.sh           # 上傳 data.db 到 GitHub Release
├── DEPLOY.md                       # 部署到 Streamlit Cloud 的完整 SOP
├── README.md
├── 啟動指令.txt                     # 本機啟動快速參考
└── 1th old/                        # 舊版程式備份
```

---

## 程式說明

### online/app.py（線上版）

| 功能 | 說明 |
|---|---|
| 美國眾議院 | 抓 House Clerk PTR index → 下載 PDF → 解析交易明細 |
| 美國參議院 | 抓 Senate eFD 官方 PTR（senate_efd.py） |
| 台灣立委 | 監察院 API（priso.cy.gov.tw），Type=04, Value=立法委員 |
| 台灣縣市議員 | 同上，Value=議員，可依縣市篩選 |
| 機構 13F | SEC EDGAR 13F-HR（funds_13f.py），11 檔基金 |
| Copy-Trade 回測 | 以國會買入交易建構策略淨值曲線 vs 大盤 |
| 標的說明 | 使用者可在 UI 為任一標的新增中文說明，存 ticker_info.db |

### offline/app.py（離線版 / 雲端部署版）

與線上版功能完全相同，差別在資料來源改讀 `data.db`（本地 SQLite）：
- 啟動瞬間載入（原本數分鐘 → <1 秒）
- 雲端部署時自動從 GitHub Release 下載 data.db
- 側邊欄「📊 本地資料庫狀態」可查看各來源同步時間與筆數
- 美國國會時間範圍下拉：30天 / 90天 / 半年 / 1年 / 2年 / 3年
- 明細表支援依姓名篩選
- 13F 持股支援歷史季度切換（近 3 年）
- 一鍵「從 GitHub Release 重新下載 data.db」

### offline/sync_data.py（資料同步 ETL）

```bash
# 全部同步（眾議院 + 參議院 + 立委 + 13F）
python offline/sync_data.py --source all --days 1095

# 單一來源
python offline/sync_data.py --source us_house --days 1095
python offline/sync_data.py --source us_senate --days 1095
python offline/sync_data.py --source tw_legislator --from 2023-01 --to 2026-04
python offline/sync_data.py --source tw_councilor --cities 臺北市 新北市 --from 2023-01 --to 2026-04
python offline/sync_data.py --source funds_13f

# 全部縣市議員（22 縣市，約 60-90 分鐘）
python offline/sync_data.py --source tw_councilor --cities 臺北市 新北市 桃園市 臺中市 臺南市 高雄市 基隆市 新竹市 新竹縣 苗栗縣 彰化縣 南投縣 雲林縣 嘉義市 嘉義縣 屏東縣 宜蘭縣 花蓮縣 臺東縣 澎湖縣 金門縣 連江縣 --from 2023-01 --to 2026-04

# 修剪超過 10 年的舊資料
python offline/sync_data.py --source all --prune 10
```

### confpath.py（共用路徑常數）

```python
PROJECT_ROOT    = ...   # 專案根目錄
TICKER_INFO_DB  = ...   # ticker_info.db 絕對路徑
DATA_DB         = ...   # data.db 絕對路徑
```
online/ 與 offline/ 的 app 皆透過 `from confpath import ...` 取得統一路徑。

### senate_efd.py（共用模組）

針對美國參議院 Electronic Financial Disclosure (eFD) 系統：
- 自動接受 agreement → CSRF → DataTables 分頁抓取 PTR 列表
- 逐份解析 PTR 電子表格中的交易明細
- 使用 `curl_cffi` 繞過 Akamai bot 防護
- Paper PTR（PDF 掃描）會跳過

### funds_13f.py（共用模組）

追蹤 11 檔知名機構 / 家族辦公室的 13F 持股：

| 基金 | 經理人 | 風格簡述 |
|---|---|---|
| Berkshire Hathaway | Warren Buffett | 長期價值投資，重倉消費與金融 |
| Bridgewater Associates | Ray Dalio | 全天候宏觀策略，風險平價 |
| Scion Asset Management | Michael Burry | 極度逆向、集中持倉 |
| Pershing Square | Bill Ackman | 積極型 activist，高品質消費龍頭 |
| Renaissance Technologies | Jim Simons | 量化鼻祖，數學模型高頻 |
| Appaloosa Management | David Tepper | 不良債權與宏觀，危機入市 |
| Duquesne Family Office | Stanley Druckenmiller | 宏觀 + 集中成長股 |
| ARK Investment Mgmt | Cathie Wood | 破壞式創新主題 ETF |
| Oaktree Capital Mgmt | Howard Marks | 全球最大不良債權投資人 |
| Soros Fund Management | George Soros | 宏觀對沖，反射理論 |
| Citadel Advisors | Ken Griffin | 多策略避險 + 造市 |

每檔抓近 3 年所有季度快照（12–16 季），含 Top 20 持股與季度變動。

---

## 資料庫 data.db 結構

| Table | Primary Key | 說明 |
|---|---|---|
| `us_trades` | 議員+院+標的+交易日+操作+金額 | 美國參眾議員交易 |
| `tw_holdings` | 姓名+職稱+申報日+公司+持有人 | 台灣立委 / 縣市議員持股 |
| `funds_13f_cache` | key (='all') | 13F 整包 JSON（含歷史季度） |
| `sync_log` | source | 各來源最後同步時間與筆數 |

---

## 資料統計（2026-04-10 同步）

| 來源 | 筆數 | 涵蓋範圍 |
|---|---|---|
| 美國眾議院 | 3,656 (102 人) | 2017-02 ~ 2026-03 |
| 美國參議院 | 1,856 (37 人) | 2023-04 ~ 2026-03 |
| 台灣立委 | 1,000 | 近 3 年申報 |
| 台灣縣市議員 | 6,322 (22 縣市) | 2023-01 ~ 2026-04 |
| 機構 13F | 11 檔 × 12~16 季 | 近 3 年季度快照 |
| **data.db 大小** | **32 MB** | |

---

## 部署

### 本機執行

```bash
cd /Users/shawnclaw/autobot/congressTrack
source venv/bin/activate

# 線上版（即時抓取）
streamlit run online/app.py

# 離線版（讀本地 DB，需先 sync）
streamlit run offline/app.py
```

### Streamlit Community Cloud

詳見 [DEPLOY.md](DEPLOY.md)。摘要：

1. `python offline/sync_data.py --source all` 同步資料
2. `./scripts/upload_data_db.sh` 上傳 data.db 到 GitHub Release `data-latest`
3. share.streamlit.io → New app → `offline/app.py` → Secrets 填 `[data_release]`
4. 首次啟動自動下載 data.db → 即可使用

日常更新：本機跑 sync → `upload_data_db.sh` → 雲端 app 側邊欄按「重新下載」。

---

## 工作紀錄

### 2026-04-10

1. **13F 機構更新**
   - 新增 Soros Fund Management、Citadel Advisors（9 → 11 檔）
   - 每檔加入中文基本描述（風格、策略），顯示在 expander 上方
   - 擴展為近 3 年全季度歷史快照（12–16 季），離線版可用 selectbox 切換季度

2. **13F 納入離線 DB**
   - `offline/sync_data.py` 新增 `funds_13f_cache` 表、`sync_funds_13f()` 函式
   - `offline/app.py` 的 `_load_funds()` 改讀 DB JSON cache

3. **美國參議院資料來源升級**
   - 新建 `senate_efd.py`：改用 Senate eFD 官方 PTR 取代 CapitolTrades
   - 使用 `curl_cffi` + `impersonate="chrome124"` 繞過 Akamai bot 防護
   - 資料量：33 → 1,856 筆（56 倍成長）

4. **美國眾議院多年度抓取**
   - `load_us_trades` 改為依 `days` 回推年度，循環抓多年 PTR index
   - 資料量：238 → 3,656 筆（15 倍成長），102 位議員

5. **台灣縣市議員全數同步**
   - 22 縣市議員申報資料完整入庫：6,322 筆
   - 同步耗時約 60 分鐘

6. **離線版 UI 優化**
   - 美國國會「掃描天數」改為「時間範圍」下拉：30天/90天/半年/1年/2年/3年
   - 明細表新增「依議員/民代姓名篩選」文字輸入框（美/台各一）

7. **Streamlit Cloud 部署架構（方案 C：GitHub Release）**
   - `offline/app.py` 啟動時若無 data.db → 自動從 GitHub Release 下載至 /tmp
   - 新增 `scripts/upload_data_db.sh`（gh CLI 上傳腳本）
   - 新增 `.streamlit/secrets.toml.example`
   - 新增 `DEPLOY.md` 完整部署 SOP
   - 已上傳 data.db 至 release tag `data-latest`（32 MB）

8. **資料夾重構**
   - `app.py` → `online/app.py`
   - `app_offline.py` → `offline/app.py`
   - `sync_data.py` → `offline/sync_data.py`
   - 共用模組（`senate_efd.py`、`funds_13f.py`）留在根目錄
   - 新增 `confpath.py` 統一管理路徑常數
   - 所有 import 路徑與文件同步更新

### 既有功能（先前完成）

- 美國眾議院 PTR PDF 解析（House Clerk）
- 台灣立委監察院 API + PDF 解析
- 台灣縣市議員支援（API Value=議員，依縣市篩選，選取時大量資料警告）
- 13F 機構持股（SEC EDGAR）
- Copy-Trade 跟單策略回測
- 標的說明資料庫（ticker_info.db，UI 可即時新增/編輯）
- 離線版 data.db 架構（sync_data.py ETL + app_offline.py 讀取）
