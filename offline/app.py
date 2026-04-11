"""
國會議員交易追蹤器 — Streamlit Web App（💾 本地 DB 離線版）
🇺🇸 美國國會（眾議院 + 參議院）PTR  ×  🇹🇼 台灣民代財產申報
讀取本地 data.db，不走即時抓取。資料同步請跑：
    python offline/sync_data.py --source all
"""

import calendar
import io
import os
import re
import sqlite3
import subprocess
import sys
import time
import zipfile

# 將專案根目錄加入 sys.path，以便 import 共用模組
_PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, _PROJECT_ROOT)

import requests
import pdfplumber
from bs4 import BeautifulSoup
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import date, datetime, timedelta

# ── 頁面設定 ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="國會交易追蹤（離線版）",
    page_icon="💾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 本地資料庫 ──────────────────────────────────────────────────
# 在 Streamlit Cloud 上，專案目錄為 read-only 的 clone，我們把 data.db 放到
# 可寫目錄（tempfile），並在首次啟動時從 GitHub Release 下載。
# 本機執行則直接用 repo 內的 data.db。
import tempfile as _tempfile
from confpath import DATA_DB as _LOCAL_DB_PATH_ROOT
_LOCAL_DB_PATH = _LOCAL_DB_PATH_ROOT
_CLOUD_DB_DIR  = _tempfile.gettempdir()  # 跨平台取得可寫 temp 目錄
_CLOUD_DB_PATH = os.path.join(_CLOUD_DB_DIR, "data.db")

def _resolve_data_db_path() -> str:
    # 本機：優先用 repo 內已同步好的 data.db
    if os.path.exists(_LOCAL_DB_PATH):
        return _LOCAL_DB_PATH
    # 雲端 temp 已有快取
    if os.path.exists(_CLOUD_DB_PATH):
        return _CLOUD_DB_PATH
    # 雲端：回傳 temp 路徑以便稍後下載
    return _CLOUD_DB_PATH

DATA_DB_PATH = _resolve_data_db_path()

# ── 從 GitHub Release 下載 data.db ─────────────────────────────
# 使用 streamlit secrets 設定：
#   [data_release]
#   repo = "datadigshawn/congress_tracker"
#   tag  = "data-latest"       # release tag
#   asset = "data.db"          # asset 檔名
#   token = "ghp_xxx"          # 可選：private repo 才需要
def _download_data_db_from_release() -> tuple[bool, str]:
    """從 GitHub Release 下載 data.db → DATA_DB_PATH。回傳 (ok, message)。"""
    global DATA_DB_PATH
    try:
        cfg = st.secrets.get("data_release", {}) if hasattr(st, "secrets") else {}
    except Exception:
        cfg = {}
    repo  = cfg.get("repo")  or os.environ.get("DATA_REPO",  "datadigshawn/congress_tracker")
    tag   = cfg.get("tag")   or os.environ.get("DATA_TAG",   "data-latest")
    asset = cfg.get("asset") or os.environ.get("DATA_ASSET", "data.db")
    token = cfg.get("token") or os.environ.get("GITHUB_TOKEN") or os.environ.get("DATA_TOKEN")

    api = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "congressTrack-offline"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        # 1) 取得 release 資訊
        r = requests.get(api, headers=headers, timeout=30)
        r.raise_for_status()
        rel = r.json()
        target = next((a for a in rel.get("assets", []) if a["name"] == asset), None)
        if not target:
            return False, f"Release {tag} 找不到 asset `{asset}`"

        # 2) 用 browser_download_url（public repo 可直接下載，不需 token）
        dl_url = target.get("browser_download_url") or target["url"]
        dl_headers = {"User-Agent": "congressTrack-offline"}
        if not target.get("browser_download_url"):
            # 私有 repo fallback 到 API url + octet-stream
            dl_headers = dict(headers)
            dl_headers["Accept"] = "application/octet-stream"
            dl_url = target["url"]

        # 3) 下載到 temp 檔再 rename
        dest = _CLOUD_DB_PATH if not os.path.exists(_LOCAL_DB_PATH) else DATA_DB_PATH
        dest_dir = os.path.dirname(dest)
        os.makedirs(dest_dir, exist_ok=True)

        # 用 tempfile 確保寫在同一 filesystem（os.replace 要求同一 fs）
        fd, tmp_path = _tempfile.mkstemp(dir=dest_dir, suffix=".db.part")
        try:
            with requests.get(dl_url, headers=dl_headers, stream=True, timeout=600) as dr:
                dr.raise_for_status()
                total = 0
                with os.fdopen(fd, "wb") as f:
                    for chunk in dr.iter_content(chunk_size=1 << 20):
                        if chunk:
                            f.write(chunk)
                            total += len(chunk)
            if total == 0:
                os.unlink(tmp_path)
                return False, "下載內容為空（0 bytes），請確認 Release asset 是否正確"
            os.replace(tmp_path, dest)
        except Exception:
            # 清理殘留 temp 檔
            try: os.unlink(tmp_path)
            except OSError: pass
            raise

        DATA_DB_PATH = dest
        size_mb = total / (1024 * 1024)
        updated = target.get("updated_at", "?")
        return True, f"✅ 已下載 {asset} ({size_mb:.1f} MB)，release 更新時間 {updated}"
    except Exception as e:
        return False, f"下載失敗：{e}"


def _ensure_data_db() -> None:
    """啟動時確保 DATA_DB_PATH 存在；不存在則自動從 Release 下載。"""
    if os.path.exists(DATA_DB_PATH):
        return
    with st.spinner("首次啟動：從 GitHub Release 下載 data.db…"):
        ok, msg = _download_data_db_from_release()
    (st.success if ok else st.error)(msg)


_ensure_data_db()


def _local_db_ready() -> bool:
    if not os.path.exists(DATA_DB_PATH):
        return False
    try:
        with sqlite3.connect(DATA_DB_PATH) as c:
            c.execute("SELECT 1 FROM us_trades LIMIT 1")
            c.execute("SELECT 1 FROM tw_holdings LIMIT 1")
        return True
    except Exception:
        return False


@st.cache_data(ttl=60, show_spinner=False)
def _get_sync_log() -> pd.DataFrame:
    if not os.path.exists(DATA_DB_PATH):
        return pd.DataFrame()
    try:
        with sqlite3.connect(DATA_DB_PATH) as c:
            return pd.read_sql(
                "SELECT source, last_synced, row_count FROM sync_log ORDER BY source",
                c,
            )
    except Exception:
        return pd.DataFrame()

SESS = requests.Session()
SESS.headers.update({"User-Agent": "Mozilla/5.0"})

# ════════════════════════════════════════════════════════════════
# ── 標的說明資料庫（SQLite）──────────────────────────────────────
# ════════════════════════════════════════════════════════════════
from confpath import TICKER_INFO_DB as _DB_PATH


def _init_db() -> None:
    """建立資料表（若不存在）。"""
    with sqlite3.connect(_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ticker_info (
                ticker  TEXT PRIMARY KEY,
                market  TEXT NOT NULL DEFAULT 'US',   -- 'US' or 'TW'
                description TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.commit()


def _seed_defaults(defaults: dict[str, str], market: str) -> None:
    """將程式碼內建的預設說明寫入 DB（僅補缺，不覆蓋使用者已編輯的內容）。"""
    with sqlite3.connect(_DB_PATH) as conn:
        for key, desc in defaults.items():
            conn.execute(
                "INSERT OR IGNORE INTO ticker_info (ticker, market, description) VALUES (?, ?, ?)",
                (key, market, desc),
            )
        conn.commit()


def _get_description(ticker: str, market: str) -> str:
    """從 DB 讀取標的說明，找不到時回傳空字串。"""
    with sqlite3.connect(_DB_PATH) as conn:
        row = conn.execute(
            "SELECT description FROM ticker_info WHERE ticker = ? AND market = ?",
            (ticker, market),
        ).fetchone()
    return row[0] if row else ""


def _upsert_description(ticker: str, market: str, description: str) -> None:
    """新增或更新標的說明。"""
    with sqlite3.connect(_DB_PATH) as conn:
        conn.execute(
            "INSERT INTO ticker_info (ticker, market, description) VALUES (?, ?, ?) "
            "ON CONFLICT(ticker) DO UPDATE SET description = excluded.description, market = excluded.market",
            (ticker, market, description),
        )
        conn.commit()


def _get_all_descriptions(market: str) -> dict[str, str]:
    """批次讀取某市場所有標的說明。"""
    with sqlite3.connect(_DB_PATH) as conn:
        rows = conn.execute(
            "SELECT ticker, description FROM ticker_info WHERE market = ?",
            (market,),
        ).fetchall()
    return {r[0]: r[1] for r in rows if r[1]}


# 初始化 DB 並匯入預設值（僅首次）
_init_db()

# ════════════════════════════════════════════════════════════════
# ── 設定（Fix #8：集中管理，不再分散兩處）────────────────────────
# ════════════════════════════════════════════════════════════════
PORTFOLIO_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
    "META", "TSLA", "AMD", "INTC", "AVGO",
    "JPM", "BAC", "GS", "MS", "WFC",
    "XOM", "CVX", "COP", "SLB",
    "LMT", "RTX", "NOC", "BA",
    "SPY", "QQQ", "IWM",
]
PORTFOLIO_SET = set(PORTFOLIO_TICKERS)   # O(1) lookup

SECTOR_MAP = {
    "AAPL":"科技","MSFT":"科技","NVDA":"科技","GOOGL":"科技",
    "AMZN":"科技","META":"科技","AMD":"科技","INTC":"科技","AVGO":"科技",
    "TSLA":"汽車",
    "JPM":"金融","BAC":"金融","GS":"金融","MS":"金融","WFC":"金融",
    "XOM":"能源","CVX":"能源","COP":"能源","SLB":"能源",
    "LMT":"國防","RTX":"國防","NOC":"國防","BA":"國防",
    "SPY":"ETF","QQQ":"ETF","IWM":"ETF",
}

# ── 標的 / 公司簡易說明 ──────────────────────────────────────────
TICKER_INFO: dict[str, str] = {
    # ── 科技 / 軟體 ──
    "AAPL":  "蘋果公司 — iPhone、Mac、iPad、Apple Watch 等消費電子與軟體服務",
    "MSFT":  "微軟 — Windows、Office 365、Azure 雲端、Xbox 遊戲平台",
    "GOOGL": "Alphabet (Google) — 搜尋引擎、YouTube、Android、Google Cloud",
    "AMZN":  "亞馬遜 — 電商平台、AWS 雲端服務、Prime 影音串流",
    "META":  "Meta (Facebook) — Facebook、Instagram、WhatsApp 社群平台與 VR",
    "NFLX":  "Netflix — 全球最大影音串流平台，原創影集與電影",
    "ORCL":  "甲骨文 — 企業資料庫、雲端基礎設施（OCI）、企業軟體",
    "IBM":   "IBM — 企業 IT 服務、混合雲、AI（Watson）、大型主機",
    "ACN":   "埃森哲 — 全球最大 IT 顧問與外包服務公司",
    "INTU":  "Intuit — TurboTax 報稅、QuickBooks 會計、Mailchimp 行銷",
    "ADSK":  "Autodesk — AutoCAD、3D 設計軟體、建築與製造業 CAD/CAM",
    "PANW":  "Palo Alto Networks — 網路安全防火牆、雲端資安解決方案",
    "NET":   "Cloudflare — CDN 內容傳遞、DDoS 防護、邊緣運算平台",
    "SHOP":  "Shopify — 電商平台建置工具，協助商家建立線上商店",
    "TTD":   "The Trade Desk — 程序化廣告購買平台，數位廣告技術",
    "GDDY":  "GoDaddy — 網域註冊、網站託管、中小企業線上工具",
    "APPF":  "AppFolio — 不動產管理 SaaS 軟體平台",
    "MORN":  "Morningstar — 投資研究、基金評等、財務數據分析平台",
    "CDW":   "CDW — IT 解決方案與硬體經銷商，服務企業與政府機構",
    "FISV":  "Fiserv — 金融科技，支付處理、核心銀行系統",
    "FIS":   "Fidelity National — 金融科技，銀行支付與資本市場技術",
    "VRSK":  "Verisk Analytics — 保險、能源、金融業數據分析與風險評估",
    "VRSN":  "VeriSign — .com/.net 網域註冊管理、DNS 基礎設施營運",
    "APP":   "AppLovin — 行動應用程式廣告與變現平台，手遊推廣",
    "DSGX":  "Descartes Systems — 物流與供應鏈管理 SaaS 軟體",
    "TECH":  "Bio-Techne — 生物技術試劑、儀器與蛋白質分析工具",
    # ── 半導體 ──
    "NVDA":  "輝達 — GPU 顯示卡、AI 加速晶片、資料中心解決方案",
    "AMD":   "超微半導體 — CPU、GPU 處理器，伺服器與個人電腦晶片",
    "INTC":  "英特爾 — CPU 處理器製造、晶圓代工、資料中心晶片",
    "AVGO":  "博通 — 半導體、網路晶片、企業軟體（含 VMware）",
    "TSM":   "台積電 ADR — 全球最大晶圓代工廠，先進製程晶片製造",
    "MU":    "美光科技 — DRAM、NAND Flash 記憶體晶片製造",
    "SWKS":  "Skyworks — 射頻半導體，手機 5G 通訊晶片",
    "ENTG":  "Entegris — 半導體製程材料、化學品、過濾與純化系統",
    "FSLR":  "First Solar — 太陽能薄膜模組製造，美國最大太陽能公司",
    "BRCM":  "博通（舊代碼） — 同 AVGO，網路與無線通訊半導體",
    # ── 汽車 / 交通 ──
    "TSLA":  "特斯拉 — 電動車（Model 3/Y/S/X）、儲能系統、自動駕駛",
    "UBER":  "Uber — 共乘叫車、Uber Eats 外送、貨運物流平台",
    "UAL":   "聯合航空 — 美國大型航空公司，國內與國際客貨運",
    "CVNA":  "Carvana — 線上二手車買賣平台，提供到府交車服務",
    "CHRW":  "C.H. Robinson — 全球第三方物流（3PL）與貨運仲介服務",
    # ── 金融 ──
    "JPM":   "摩根大通 — 美國最大銀行，投資銀行、資產管理、零售銀行",
    "BAC":   "美國銀行 — 零售銀行、財富管理、投資銀行服務",
    "GS":    "高盛集團 — 投資銀行、證券交易、資產管理",
    "MS":    "摩根士丹利 — 投資銀行、財富管理、機構證券",
    "WFC":   "富國銀行 — 美國大型零售銀行、房貸與消費金融",
    "C":     "花旗集團 — 全球性銀行，消費金融、投資銀行、財富管理",
    "BK":    "紐約梅隆銀行 — 全球最大託管銀行，資產管理與證券服務",
    "COF":   "Capital One — 信用卡發行商、消費金融與數位銀行",
    "PNC":   "PNC 金融服務 — 美國大型區域銀行，零售與企業銀行業務",
    "AXP":   "美國運通 — 信用卡與簽帳卡網路、旅遊與商務支付服務",
    "PYPL":  "PayPal — 線上支付平台、Venmo 行動支付、跨境匯款",
    "SQ":    "Block (Square) — 行動支付、Cash App、商家收款終端",
    "BX":    "黑石集團 — 全球最大另類資產管理公司，私募股權與不動產",
    "PRU":   "保德信金融 — 壽險、年金、資產管理與退休金服務",
    "IVZ":   "景順投信 — 全球資產管理，ETF（QQQ 發行商）與共同基金",
    "NDAQ":  "那斯達克交易所 — 證券交易所營運、市場技術與數據服務",
    "HOOD":  "Robinhood — 零手續費股票交易 App，散戶投資平台",
    "COIN":  "Coinbase — 美國最大加密貨幣交易所，比特幣與以太幣交易",
    "PGR":   "Progressive — 美國大型汽車保險公司，財產與意外險",
    "ERIE":  "Erie Indemnity — 財產與意外保險管理公司",
    "AJG":   "Arthur J. Gallagher — 全球保險經紀與風險管理服務",
    "BRO":   "Brown & Brown — 保險經紀與代理服務公司",
    "FCNCA": "First Citizens BancShares — 美國區域銀行（收購矽谷銀行）",
    "FICO":  "Fair Isaac (FICO) — 信用評分系統、風險決策分析軟體",
    "FDS":   "FactSet — 金融數據與分析平台，投資研究工具",
    # ── 能源 ──
    "XOM":   "埃克森美孚 — 全球最大石油公司之一，上游開採與煉油",
    "CVX":   "雪佛龍 — 石油天然氣巨頭，上下游整合營運",
    "COP":   "康菲石油 — 獨立油氣探勘與生產公司",
    "SLB":   "斯倫貝謝 — 全球最大油田服務公司，鑽井與油藏技術",
    "OXY":   "西方石油 — 石油天然氣開採，二疊紀盆地主要營運商",
    "EOG":   "EOG Resources — 美國頁岩油氣探勘與生產公司",
    "KMI":   "Kinder Morgan — 北美最大天然氣管線營運商",
    "OKE":   "ONEOK — 天然氣液（NGL）管線與加工服務",
    "WMB":   "Williams Companies — 天然氣管線與中游處理設施",
    "TRGP":  "Targa Resources — 天然氣與 NGL 收集、加工、運輸",
    "EQT":   "EQT Corporation — 美國最大天然氣生產商",
    "CTRA":  "Coterra Energy — 石油與天然氣探勘生產（二疊紀+阿帕拉契）",
    # ── 國防 / 航太 ──
    "LMT":   "洛克希德馬丁 — F-35 戰鬥機、飛彈防禦系統、軍事航太",
    "RTX":   "雷神技術 — 飛彈系統、軍用雷達、普惠航空發動機",
    "NOC":   "諾斯洛普格魯曼 — B-21 轟炸機、無人機、國防電子系統",
    "BA":    "波音 — 商用客機（737/787）、軍用飛機、太空系統",
    "GD":    "通用動力 — 灣流商務機、核子潛艦、裝甲車輛製造",
    "GE":    "奇異電氣 — 航空發動機（GE Aerospace 為主）、能源設備",
    "GEV":   "GE Vernova — 從 GE 分拆的能源公司，風力與燃氣渦輪發電",
    "GEHC":  "GE HealthCare — 從 GE 分拆的醫療設備公司，MRI、CT、超音波",
    # ── 醫療 / 製藥 ──
    "UNH":   "聯合健康 — 美國最大健康保險公司，Optum 醫療服務",
    "LLY":   "禮來 — 製藥巨頭，GLP-1 減重藥（Mounjaro/Zepbound）、糖尿病藥",
    "PFE":   "輝瑞 — COVID 疫苗、抗癌藥、心血管藥物等大型製藥",
    "MRK":   "默克 — 免疫療法 Keytruda 抗癌藥、疫苗與動物保健",
    "ABT":   "亞培 — 醫療器材、營養品（亞培奶粉）、診斷檢測",
    "BSX":   "波士頓科學 — 心臟支架、微創手術醫療器材",
    "LH":    "Labcorp — 臨床實驗室檢測、藥物開發服務",
    "STE":   "STERIS — 手術室設備、醫療消毒滅菌系統",
    "ILMN":  "Illumina — 基因定序儀器與試劑，基因體學研究龍頭",
    "VCYT":  "Veracyte — 基因組診斷檢測，癌症與甲狀腺分子診斷",
    "RARE":  "Ultragenyx — 罕見疾病基因治療與酵素替代療法",
    "ZTS":   "碩騰 — 全球最大動物保健公司，寵物與畜牧用藥",
    "THC":   "Tenet Healthcare — 醫院與門診手術中心營運商",
    # ── 消費 / 零售 ──
    "DIS":   "迪士尼 — 主題樂園、Disney+ 串流、漫威、皮克斯影業",
    "BKNG":  "Booking Holdings — 全球線上旅遊預訂（Booking.com、Agoda）",
    "DASH":  "DoorDash — 美國最大外送平台，餐飲與生鮮雜貨配送",
    "MNST":  "Monster Beverage — Monster 能量飲料製造商",
    "DPZ":   "達美樂 — 全球最大披薩連鎖，外送與外帶為主",
    "BJ":    "BJ's Wholesale — 美國會員制倉儲量販店（類似 Costco）",
    "TSCO":  "Tractor Supply — 美國最大農村生活零售連鎖，農具與寵物用品",
    "NWL":   "Newell Brands — 消費品集團（Rubbermaid、Sharpie、Coleman）",
    "GIS":   "通用磨坊 — 食品大廠（Cheerios 麥片、哈根達斯、灣仔碼頭）",
    "CAG":   "Conagra Brands — 冷凍食品與零食（Marie Callender's、Slim Jim）",
    "KVUE":  "Kenvue — 從嬌生分拆的消費保健品（Tylenol、Listerine、Band-Aid）",
    "VIK":   "Viking Holdings — 維京遊輪，河輪與遠洋郵輪旅遊",
    # ── 工業 / 製造 ──
    "MMM":   "3M — 多元化工業集團（Post-it、膠帶、研磨材料、醫療用品）",
    "CMI":   "康明斯 — 柴油引擎、發電機、動力系統製造",
    "LIN":   "林德 — 全球最大工業氣體公司，氧氣、氮氣、氫氣供應",
    "EME":   "EMCOR Group — 機電工程承包、設施維護服務",
    "ROL":   "Rollins — 害蟲防治服務（Orkin 品牌）",
    "ROP":   "Roper Technologies — 多元化工業軟體與技術集團",
    "AME":   "AMETEK — 電子儀器與機電設備製造（航太、能源、工業）",
    "PRIM":  "Primoris Services — 基礎建設工程承包（電力、管線、太陽能）",
    "MIDD":  "Middleby — 商用廚房設備、食品加工機械製造",
    "WAT":   "Waters Corporation — 液相層析儀與質譜儀，分析化學儀器",
    "DAY":   "Dayforce (Ceridian) — 人力資源管理與薪資 SaaS 平台",
    "PAYX":  "Paychex — 中小企業薪資處理、人資外包服務",
    "JLL":   "仲量聯行 — 全球商業不動產服務、物業管理與投資顧問",
    "FSV":   "FirstService — 不動產服務（物業管理、房屋修繕品牌）",
    "CLH":   "Clean Harbors — 環境服務、有害廢棄物處理與回收",
    # ── 不動產 ──
    "INVH":  "Invitation Homes — 美國最大獨棟住宅出租 REIT",
    "WELL":  "Welltower — 醫療保健不動產 REIT（養護中心、醫療辦公室）",
    "DOC":   "Healthpeak Properties — 生命科學與醫療不動產 REIT",
    "SKY":   "Skyline Champion — 組合屋與預製房屋製造商",
    "TPH":   "Tri Pointe Homes — 美國住宅建設公司",
    # ── 電信 / 媒體 ──
    "T":     "AT&T — 美國大型電信商，行動通信、光纖寬頻、HBO Max",
    "WBD":   "Warner Bros. Discovery — HBO、CNN、Discovery 頻道、電影製作",
    "V":     "Visa — 全球最大支付網路，信用卡與簽帳卡交易處理",
    # ── 國際 ──
    "BABA":  "阿里巴巴 — 中國最大電商平台（淘寶、天貓）、阿里雲",
    "TEL":   "TE Connectivity — 連接器與感測器製造，車用與工業電子",
    # ── ETF ──
    "SPY":   "SPDR S&P 500 ETF — 追蹤標普 500 指數（500 大美國企業）的最大 ETF",
    "QQQ":   "Invesco QQQ ETF — 追蹤那斯達克 100 指數，以科技股為主（蘋果、微軟、NVIDIA 等）",
    "IWM":   "iShares Russell 2000 ETF — 追蹤美國小型股 Russell 2000 指數，涵蓋約 2000 家小型企業",
    # ── 其他常見 ──
    "B":     "Barnes Group — 航太零件與工業製造，精密彈簧與氣體渦輪零件",
    "PAR":   "PAR Technology — 餐飲業 POS 系統與雲端管理軟體",
    "SGI":   "Dine Brands (前 SGI) — 餐飲集團（Applebee's、IHOP 連鎖餐廳）",
    "FSS":   "Federal Signal — 市政清掃車、消防車、緊急警報系統",
    "EG":    "Everest Group — 再保險與特殊保險公司",
    "THR":   "Thermon Group — 工業加熱系統、管線保溫設備製造",
    "FBIN":  "Fortune Brands Innovations — 居家裝修（Moen 水龍頭、門鎖）",
    "EXE":   "Expand Energy — 天然氣探勘與生產公司",
}

TW_COMPANY_INFO: dict[str, str] = {
    # 半導體
    "台積電": "全球最大晶圓代工廠，為蘋果、NVIDIA 等製造先進製程晶片",
    "聯發科": "IC 設計公司，手機處理器（天璣系列）、WiFi / 藍牙晶片",
    "聯電": "晶圓代工廠，專注成熟製程（車用、IoT、面板驅動 IC）",
    "日月光": "全球最大半導體封測廠，IC 封裝與測試服務",
    # 電子製造
    "鴻海": "全球最大電子代工廠（富士康），組裝 iPhone、伺服器、電動車",
    "廣達": "筆電代工龍頭，AI 伺服器、雲端設備製造",
    "仁寶": "筆電代工大廠，消費電子與車用電子產品",
    "英業達": "筆電、伺服器代工，企業級運算設備製造",
    # 電子零件
    "台達電": "電源供應器、散熱方案、電動車充電樁、工業自動化",
    "光寶科": "電源供應、LED 照明、雲端運算周邊零組件",
    "研華": "工業電腦與物聯網（IoT）解決方案領導廠商",
    # 電信
    "中華電": "台灣最大電信商，固網、行動通信、MOD 數位內容",
    "台灣大": "電信服務、momo 電商平台、有線電視",
    "遠傳": "行動通信、企業數位轉型、friDay 影音串流",
    # 金融
    "富邦金": "金控集團 — 富邦人壽、台北富邦銀行、富邦證券",
    "國泰金": "金控集團 — 國泰人壽（壽險龍頭）、國泰世華銀行",
    "中信金": "金控集團 — 中國信託銀行（信用卡龍頭）、台灣人壽",
    "兆豐金": "金控集團 — 兆豐銀行（外匯龍頭）、兆豐證券",
    "玉山金": "金控集團 — 玉山銀行（數位金融）、玉山證券",
    # 石化
    "台塑": "台灣最大塑化集團，塑膠原料、化學品製造",
    "南亞": "台塑集團成員，塑膠加工、電子材料（銅箔基板）",
    "台化": "台塑集團成員，石化原料、紡織纖維",
    "台塑化": "台灣最大煉油廠，汽柴油、石化原料生產",
    # 鋼鐵
    "中鋼": "台灣最大鋼鐵公司，鋼板、鋼捲、特殊鋼製造",
    "中鴻": "中鋼集團成員，熱軋鋼捲與鋼板加工",
    # 航運
    "長榮": "全球前十大貨櫃航運公司，國際海運物流",
    "陽明": "貨櫃航運公司，亞洲-歐美航線為主",
    "萬海": "區域性貨櫃航運，亞洲近洋航線為主力",
    # 交通 / 航空
    "台灣高鐵": "台灣西部高速鐵路營運商，南北 345 公里路線",
    "中華航空": "台灣國籍航空公司，國際與兩岸客貨運",
    "長榮航": "台灣國籍航空公司，以服務品質著稱的國際航線",
}

# ── 將內建預設說明匯入 SQLite（僅補缺，不覆蓋使用者已編輯的內容）──
_seed_defaults(TICKER_INFO, "US")
_seed_defaults(TW_COMPANY_INFO, "TW")

US_BASE = "https://disclosures-clerk.house.gov/public_disc"

TW_API      = "https://priso.cy.gov.tw/api/Query"
TW_PAGE_TPL = {"PageNo": 1, "PageSize": 50, "TotalCount": 0, "OrderByNum": 0, "OrderBySort": ""}

TW_SECTOR_MAP = {
    "台積電":"半導體","聯發科":"半導體","聯電":"半導體","日月光":"半導體",
    "鴻海":"電子製造","廣達":"電子製造","仁寶":"電子製造","英業達":"電子製造",
    "台達電":"電子零件","光寶科":"電子零件","研華":"電子零件",
    "中華電":"電信","台灣大":"電信","遠傳":"電信",
    "富邦金":"金融","國泰金":"金融","中信金":"金融","兆豐金":"金融","玉山金":"金融",
    "台塑":"石化","南亞":"石化","台化":"石化","台塑化":"石化",
    "中鋼":"鋼鐵","中鴻":"鋼鐵",
    "長榮":"航運","陽明":"航運","萬海":"航運",
    "台灣高鐵":"交通","中華航空":"航空","長榮航":"航空",
}

# ════════════════════════════════════════════════════════════════
# ── 美國 US 資料函數 ──────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
def fetch_ptr_index(year: int) -> list[dict]:
    import xml.etree.ElementTree as ET
    resp = SESS.get(f"{US_BASE}/financial-pdfs/{year}FD.zip", timeout=30)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        xml_name = [n for n in z.namelist() if n.endswith(".xml")][0]
        root = ET.fromstring(z.read(xml_name))
    filings = []
    for m in root.findall(".//Member"):
        if (m.findtext("FilingType") or "") != "P":
            continue
        filings.append({
            "name":       f"{m.findtext('First','')} {m.findtext('Last','')}".strip(),
            "state":      m.findtext("StateDst", ""),
            "filingDate": m.findtext("FilingDate", ""),
            "docId":      m.findtext("DocID", ""),
            "year":       m.findtext("Year", str(year)),
        })
    return filings


def parse_filing_date(s: str):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def parse_amount(s: str) -> float:
    """
    Fix #3：對非數字格式（None / N/A / 空字串）安全回傳 0.0，
    並過濾掉明顯不合理的純年份數字（如 2024）。
    """
    if not s or not isinstance(s, str):
        return 0.0
    nums = [float(n.replace(",", "")) for n in re.findall(r"[\d,]+", s)
            if len(n.replace(",", "")) <= 10]   # 超過 10 位數不像金額
    if len(nums) >= 2:
        return (nums[0] + nums[1]) / 2
    elif len(nums) == 1:
        return nums[0]
    return 0.0


def parse_ptr_pdf(pdf_bytes: bytes) -> list[dict]:
    """
    Fix #1：加回 CUSIP 過濾（ticker 長度 > 5 排除）。
    """
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        lines = []
        for page in pdf.pages:
            lines.extend((page.extract_text() or "").splitlines())
    joined = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.search(r"\$[\d,]+ -\s*$", line) and i + 1 < len(lines):
            tail = re.sub(r"^\[[A-Z]{2}\]\s*", "", lines[i + 1]).strip()
            line = line.rstrip() + " " + tail
            i += 2
        else:
            i += 1
        joined.append(line)
    pat = re.compile(
        r"\(([A-Z]{1,5})\).*?([SP])\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})"
        r"\s+(\$[\d,]+(?:\s*-\s*\$[\d,]+)?)"
    )
    trades = []
    for line in joined:
        m = pat.search(line)
        if m:
            ticker, tx_type, tx_date, disc_date, amount = m.groups()
            if len(ticker) > 5:          # Fix #1：排除 CUSIP 碼
                continue
            trades.append({
                "ticker":         ticker,
                "type":           "Purchase" if tx_type == "P" else "Sale",
                "txDate":         tx_date,
                "disclosureDate": disc_date,
                "amount":         amount.strip(),
            })
    return trades


@st.cache_data(ttl=300, show_spinner=False)
def load_us_trades(days: int, today: str) -> pd.DataFrame:
    """[OFFLINE] 從 data.db 讀取眾議院資料，用 days 過濾交易日。"""
    if not _local_db_ready():
        return pd.DataFrame()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with sqlite3.connect(DATA_DB_PATH) as conn:
        df = pd.read_sql(
            """SELECT 議員,院,州,標的,操作,金額,交易日,揭露日,申報日,板塊,持倉
               FROM us_trades WHERE 院='眾議院'""",
            conn,
        )
    if df.empty:
        return df
    df["交易日_dt"] = pd.to_datetime(df["交易日"], format="%m/%d/%Y", errors="coerce")
    df["金額_數值"] = df["金額"].apply(parse_amount)
    df["持倉"] = df["持倉"].astype(bool)
    df = df[df["交易日_dt"] >= pd.Timestamp(since)]
    return df.sort_values("交易日_dt", ascending=False)


# ════════════════════════════════════════════════════════════════
# ── 美國參議院 Senate 資料函數（via CapitolTrades）──────────────────
# ════════════════════════════════════════════════════════════════
CAPITOL_TRADES_URL = "https://www.capitoltrades.com/trades"


def _parse_capitol_trades_page(html: str) -> list[dict]:
    """解析 CapitolTrades 頁面中的交易表格。"""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    trades = []
    for tr in table.select("tbody tr"):
        tds = tr.select("td")
        if len(tds) < 9:
            continue
        # 0: Politician, 1: Traded Issuer, 2: Published, 3: Traded, 4: Filed After,
        # 5: Owner, 6: Type, 7: Size, 8: Price
        politician_text = tds[0].get_text(" ", strip=True)
        issuer_text     = tds[1].get_text(strip=True)
        published_text  = tds[2].get_text(" ", strip=True)   # e.g. "5 Apr 2026" (Filed / Published)
        traded_text     = tds[3].get_text(" ", strip=True)   # e.g. "12 Mar 2026"
        tx_type_text    = tds[6].get_text(strip=True)        # buy / sell / exchange
        size_text       = tds[7].get_text(strip=True)        # e.g. "1K–15K"

        # 提取 ticker（格式：CompanyNameTICKER:US）
        ticker_match = re.search(r"([A-Z]{1,5}):US$", issuer_text)
        if not ticker_match:
            continue
        ticker = ticker_match.group(1)

        # 提取議員名稱（去掉黨派 / 院 / 州等尾巴）
        # 文字範例："Shelley Moore Capito Republican Senate WV" 或
        #          "Angus King Other Senate ME"（無黨籍在 CapitolTrades 顯示為 Other）
        name_match = re.match(
            r"^(.*?)\s+(?:Republican|Democrat|Independent|Other)\s+(?:Senate|House)\s+([A-Z]{2})$",
            politician_text,
        )
        if name_match:
            name  = name_match.group(1).strip()
            state = name_match.group(2)
        else:
            # fallback：舊的切法（以防格式變動）
            name = politician_text
            for party in ("Republican", "Democrat", "Independent", "Other"):
                idx = name.find(party)
                if idx > 0:
                    name = name[:idx].strip()
                    break
            state_match = re.search(r"([A-Z]{2})$", politician_text)
            state = state_match.group(1) if state_match else ""

        # 標準化交易類型
        if tx_type_text == "buy":
            op = "Purchase"
        elif tx_type_text == "sell":
            op = "Sale"
        elif tx_type_text == "exchange":
            op = "Exchange"
        else:
            continue

        # 解析日期（"12 Mar 2026" / "12 Mar2026" → "03/12/2026"）
        def _parse_ct_date(s: str) -> str:
            dm = re.match(r"(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})", s or "")
            if dm:
                try:
                    return datetime.strptime(
                        f"{dm.group(1)} {dm.group(2)} {dm.group(3)}", "%d %b %Y"
                    ).strftime("%m/%d/%Y")
                except ValueError:
                    return s
            return s

        tx_date        = _parse_ct_date(traded_text)
        published_date = _parse_ct_date(published_text)

        # 金額轉換：CapitolTrades 用縮寫（1K–15K → $1,001 - $15,000）
        amount = _normalize_senate_amount(size_text)

        trades.append({
            "name":      name,
            "state":     state,
            "ticker":    ticker,
            "type":      op,
            "txDate":    tx_date,
            "filedDate": published_date,
            "amount":    amount,
        })
    return trades


def _normalize_senate_amount(s: str) -> str:
    """將 CapitolTrades 的金額縮寫轉為完整格式，例如 '1K–15K' → '$1,001 - $15,000'。"""
    amount_map = {
        "1K–15K":    "$1,001 - $15,000",
        "15K–50K":   "$15,001 - $50,000",
        "50K–100K":  "$50,001 - $100,000",
        "100K–250K": "$100,001 - $250,000",
        "250K–500K": "$250,001 - $500,000",
        "500K–1M":   "$500,001 - $1,000,000",
        "1M–5M":     "$1,000,001 - $5,000,000",
        "5M–25M":    "$5,000,001 - $25,000,000",
        "25M–50M":   "$25,000,001 - $50,000,000",
        "Over 50M":  "Over $50,000,000",
    }
    return amount_map.get(s.strip(), s)


@st.cache_data(ttl=300, show_spinner=False)
def load_senate_trades(days: int, today: str) -> pd.DataFrame:
    """[OFFLINE] 從 data.db 讀取參議院資料。"""
    if not _local_db_ready():
        return pd.DataFrame()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with sqlite3.connect(DATA_DB_PATH) as conn:
        df = pd.read_sql(
            """SELECT 議員,院,州,標的,操作,金額,交易日,揭露日,申報日,板塊,持倉
               FROM us_trades WHERE 院='參議院'""",
            conn,
        )
    if df.empty:
        return df
    df["交易日_dt"] = pd.to_datetime(df["交易日"], format="%m/%d/%Y", errors="coerce")
    df["金額_數值"] = df["金額"].apply(parse_amount)
    df["持倉"] = df["持倉"].astype(bool)
    df = df[df["交易日_dt"] >= pd.Timestamp(since)]
    return df.sort_values("交易日_dt", ascending=False)


def _legacy_load_senate_disabled(days: int, today: str) -> pd.DataFrame:
    """原即時抓取邏輯（保留程式碼以利對照），不再被呼叫。"""
    since = datetime.now() - timedelta(days=days)
    rows: list[dict] = []
    page = 1
    max_pages = 100
    consecutive_failures = 0
    prog = st.progress(0, text="正在載入參議院交易資料...")
    try:
        while page <= max_pages:
            prog.progress(min(page / max_pages, 0.99),
                          text=f"載入參議院交易第 {page} 頁...")
            # 單頁抓取 + 重試 3 次，避免單次瞬斷就停掉整個分頁
            trades = None
            for attempt in range(3):
                try:
                    resp = SESS.get(
                        CAPITOL_TRADES_URL,
                        params={"chamber": "senate", "txDate": f"{days}d", "page": page},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        trades = _parse_capitol_trades_page(resp.text)
                        break
                except Exception:
                    pass
                time.sleep(1.0 * (attempt + 1))

            if trades is None:
                # 三次都失敗：跳過此頁，但不中止整個分頁流程
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    break
                page += 1
                continue
            consecutive_failures = 0

            if not trades:
                break

            stop = False
            for t in trades:
                # 檢查日期是否超出範圍
                try:
                    tx_dt = datetime.strptime(t["txDate"], "%m/%d/%Y")
                    if tx_dt < since:
                        stop = True
                        continue
                except ValueError:
                    pass
                rows.append({
                    "議員":   t["name"],
                    "州":    t["state"],
                    "院":    "參議院",
                    "標的":  t["ticker"],
                    "操作":  t["type"],
                    "金額":  t["amount"],
                    "交易日": t["txDate"],
                    "揭露日": t.get("filedDate") or t["txDate"],
                    "申報日": t.get("filedDate") or t["txDate"],
                    "板塊":  SECTOR_MAP.get(t["ticker"], "其他"),
                    "持倉":  t["ticker"] in PORTFOLIO_SET,
                })
            if stop:
                break
            page += 1
            time.sleep(0.5)
    finally:
        prog.empty()

    df = pd.DataFrame(rows)
    if not df.empty:
        df["交易日_dt"] = pd.to_datetime(df["交易日"], format="%m/%d/%Y", errors="coerce")
        df["金額_數值"] = df["金額"].apply(parse_amount)
        df = df.sort_values("交易日_dt", ascending=False)
    return df


# ════════════════════════════════════════════════════════════════
# ── Copy-Trade 跟單策略回測 ──────────────────────────────────────
# ════════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600, show_spinner="計算跟單策略績效…")
def compute_copytrade_strategy(
    purchases_df: pd.DataFrame,
    benchmark: str,
    today: str,          # cache key
) -> dict | None:
    """
    將 purchases_df 內每筆 Purchase 視為等額買入、持有至今日，
    建立策略淨值曲線並計算 Key Metrics（與 benchmark 對照）。

    purchases_df 需含欄位：標的、交易日_dt
    回傳 dict 含 metrics / bench_metrics / beta / alpha / curve / 樣本統計；
    若無法計算則回傳 None。
    """
    try:
        import yfinance as yf
        import numpy as np
    except ImportError:
        return None

    p = purchases_df[["標的", "交易日_dt"]].dropna()
    # 只保留看起來像 ticker 的代碼
    p = p[p["標的"].astype(str).str.match(r"^[A-Z][A-Z\.\-]{0,5}$", na=False)]
    if p.empty:
        return None

    tickers = sorted(p["標的"].unique().tolist())
    start = pd.Timestamp(p["交易日_dt"].min()) - pd.Timedelta(days=7)
    end   = pd.Timestamp.today() + pd.Timedelta(days=1)

    try:
        raw = yf.download(
            tickers + [benchmark],
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            progress=False, auto_adjust=True, threads=True,
        )
    except Exception:
        return None
    if raw is None or raw.empty:
        return None

    # 取 Close 欄；單一/多檔兩種返回結構都處理
    if isinstance(raw.columns, pd.MultiIndex):
        if "Close" not in raw.columns.get_level_values(0):
            return None
        prices = raw["Close"]
    else:
        prices = raw[["Close"]].rename(columns={"Close": tickers[0] if len(tickers) == 1 else benchmark})

    if isinstance(prices, pd.Series):
        prices = prices.to_frame()

    idx = pd.bdate_range(start, pd.Timestamp.today().normalize())
    prices = prices.reindex(idx).ffill()

    if benchmark not in prices.columns:
        return None

    strat    = pd.Series(0.0, index=idx)
    bench    = pd.Series(0.0, index=idx)
    n_active = pd.Series(0,   index=idx)
    skipped  = 0

    for _, row in p.iterrows():
        tk = row["標的"]
        d  = pd.Timestamp(row["交易日_dt"]).normalize()
        if tk not in prices.columns:
            skipped += 1; continue
        tk_s = prices[tk]
        valid_entry = tk_s.index[(tk_s.index >= d) & tk_s.notna()]
        if len(valid_entry) == 0:
            skipped += 1; continue
        entry_date = valid_entry[0]
        entry_px   = tk_s.loc[entry_date]
        bench_px   = prices[benchmark].loc[entry_date]
        if pd.isna(entry_px) or pd.isna(bench_px) or entry_px == 0 or bench_px == 0:
            skipped += 1; continue
        mask = idx >= entry_date
        strat.loc[mask]    += (tk_s.loc[mask] / entry_px).fillna(1.0)
        bench.loc[mask]    += (prices[benchmark].loc[mask] / bench_px).fillna(1.0)
        n_active.loc[mask] += 1

    active = n_active > 0
    if not active.any():
        return None

    strat_norm = strat[active] / n_active[active]
    bench_norm = bench[active] / n_active[active]

    def _metrics(s: pd.Series) -> dict:
        s = s.dropna()
        if len(s) < 2:
            return {}
        days = max((s.index[-1] - s.index[0]).days, 1)
        total = float(s.iloc[-1] / s.iloc[0] - 1)
        cagr  = float((s.iloc[-1] / s.iloc[0]) ** (365.25 / days) - 1)
        rets  = s.pct_change().dropna()
        sharpe = float((rets.mean() / rets.std()) * (252 ** 0.5)) if rets.std() else 0.0
        mdd   = float((s / s.cummax() - 1).min())
        r1d   = float(s.iloc[-1] / s.iloc[-2] - 1) if len(s) >= 2 else 0.0
        i30   = max(-22, -len(s))
        r30   = float(s.iloc[-1] / s.iloc[i30] - 1)
        one_y_cut = s.index[-1] - pd.Timedelta(days=365)
        idx_1y = s.index[s.index >= one_y_cut]
        r1y = float(s.iloc[-1] / s.loc[idx_1y[0]] - 1) if len(idx_1y) else total
        return dict(total=total, cagr=cagr, sharpe=sharpe, mdd=mdd,
                    r1d=r1d, r30=r30, r1y=r1y)

    m_strat = _metrics(strat_norm)
    m_bench = _metrics(bench_norm)

    # Beta / Alpha（對 benchmark，日報酬迴歸）
    rs = strat_norm.pct_change().dropna()
    rb = bench_norm.pct_change().dropna()
    common = rs.index.intersection(rb.index)
    beta = alpha = None
    if len(common) > 5:
        rsc, rbc = rs.loc[common], rb.loc[common]
        var = float(rbc.var())
        if var:
            beta  = float(rsc.cov(rbc) / var)
            alpha = float((rsc.mean() - beta * rbc.mean()) * 252)

    curve = pd.DataFrame({
        "date": strat_norm.index,
        "策略": strat_norm.values,
        "大盤": bench_norm.values,
    })

    return dict(
        metrics=m_strat, bench_metrics=m_bench,
        beta=beta, alpha=alpha,
        curve=curve,
        n_trades=int(len(p)), n_skipped=int(skipped), n_tickers=int(len(tickers)),
        benchmark=benchmark,
    )


# ════════════════════════════════════════════════════════════════
# ── 台灣 TW 資料函數 ──────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
def parse_roc_date(s: str):
    """Parse ROC calendar date string like '民國115年 03月 19日' → datetime."""
    m = re.search(r'民國\s*(\d+)年\s*(\d+)月\s*(\d+)日', s or "")
    if m:
        try:
            return datetime(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def parse_tw_stocks(pdf_bytes: bytes) -> list[dict]:
    """Parse stock holdings section from Taiwan financial disclosure PDF."""
    stocks = []
    seen   = set()
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text  = page.extract_text() or ""
                lines = text.split("\n")
                in_stock = False
                for line in lines:
                    s = line.strip()
                    if re.search(r'1\s*[.。]\s*股票', s):
                        in_stock = True
                        continue
                    if in_stock and (re.search(r'^2\s*[.。]', s) or
                                     re.search(r'^（[九十百]）', s) or
                                     re.search(r'本欄空白', s)):
                        in_stock = False
                        continue
                    if not in_stock or not s:
                        continue
                    if re.search(r'名\s*稱|所\s*有\s*人|股\s*數|票\s*面|外\s*幣|總\s*額', s):
                        continue
                    m = re.match(
                        r'^(\S{1,12})\s+([\u4e00-\u9fff]{2,4})\s+([\d,]+)\s+(\d+(?:\.\d+)?)\s*([\d,]*)\s*$',
                        s
                    )
                    if m:
                        company, owner, shares_s, face_s, total_s = m.groups()
                        key = (company, owner)
                        if key in seen:
                            continue
                        seen.add(key)
                        try:
                            stocks.append({
                                "company":    company,
                                "owner":      owner,
                                "shares":     int(shares_s.replace(",", "")),
                                "face_value": float(face_s),
                                "total_twd":  float(total_s.replace(",", "")) if total_s else 0.0,
                            })
                        except ValueError:
                            pass
    except Exception:
        pass
    return stocks


def _dept_to_city(dept: str | None) -> str:
    """'新竹縣議會' -> '新竹縣'；'高雄市議會' -> '高雄市'"""
    if not dept:
        return ""
    m = re.match(r"^(.+?[市縣])議會", dept)
    return m.group(1) if m else dept


@st.cache_data(ttl=300, show_spinner=False)
def load_tw_holdings(
    date_from_str: str,
    date_to_str: str,
    role: str = "立法委員",
    cities: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """[OFFLINE] 從 data.db 讀取 TW 民代持股資料。"""
    if not _local_db_ready():
        return pd.DataFrame()
    q = "SELECT * FROM tw_holdings WHERE 職稱 = ?"
    params: list = [role]
    if cities:
        q += f" AND 縣市 IN ({','.join(['?']*len(cities))})"
        params.extend(cities)
    with sqlite3.connect(DATA_DB_PATH) as conn:
        df = pd.read_sql(q, conn, params=params)
    if df.empty:
        return df
    # 用 ROC 民國年比對申報日：申報日格式 "民國115年 04月 09日"
    def _to_iso(roc: str) -> str:
        m = re.search(r"民國\s*(\d+)年\s*(\d+)月\s*(\d+)日", roc or "")
        if not m:
            return ""
        try:
            return f"{int(m.group(1))+1911:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        except Exception:
            return ""
    df["_申報日_iso"] = df["申報日"].apply(_to_iso)
    df = df[(df["_申報日_iso"] >= date_from_str) & (df["_申報日_iso"] <= date_to_str)]
    df = df.drop(columns=["_申報日_iso", "_synced_at"], errors="ignore")
    df["是否本人"] = df["是否本人"].astype(bool)
    return df.reset_index(drop=True)


def _legacy_load_tw_disabled(
    date_from_str: str,
    date_to_str: str,
    role: str = "立法委員",
    cities: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """原即時抓取邏輯（保留以利對照），不再被呼叫。"""
    date_from = datetime.strptime(date_from_str, "%Y-%m-%d")
    date_to   = datetime.strptime(date_to_str,   "%Y-%m-%d")
    api_value = "立法委員" if role == "立法委員" else "議員"
    city_set = set(cities) if cities else None

    filings = []
    page_no = 1
    done    = False
    while not done:
        page = dict(TW_PAGE_TPL)
        page["PageNo"] = page_no
        try:
            r = SESS.post(f"{TW_API}/QueryData",
                          json={"Data": {"Method": "", "Type": "04", "Value": api_value},
                                "Page": page},
                          timeout=30)
            data = r.json()
            if not data.get("Success"):
                break
            records = data["Data"]["Data"]
            if not records:
                break
            for rec in records:
                pub_dt = parse_roc_date(rec.get("PublishDate", ""))
                if pub_dt is None:
                    continue
                if pub_dt > date_to:
                    continue
                if pub_dt < date_from:
                    done = True
                    break
                if "01" not in rec.get("PublishType", ""):
                    continue
                if city_set is not None:
                    if _dept_to_city(rec.get("Dept", "")) not in city_set:
                        continue
                filings.append(rec)
            page_no += 1
            if page_no > 400:
                break
        except Exception:
            break

    rows = []
    prog = st.progress(0, text=f"正在解析{role}財產申報 PDF…")
    try:
        for idx, filing in enumerate(filings):
            prog.progress((idx + 1) / max(len(filings), 1),
                          text=f"解析 {filing['Name']} ({idx+1}/{len(filings)})")
            try:
                pdf_r = SESS.post(f"{TW_API}/getFile",
                                  json={"From": "base", "FileId": filing["Id"]},
                                  timeout=60)
                stocks = parse_tw_stocks(pdf_r.content)
                city = _dept_to_city(filing.get("Dept", ""))
                for stk in stocks:
                    rows.append({
                        "姓名":   filing["Name"],
                        "職稱":   role,
                        "縣市":   city,
                        "申報日": filing["PublishDate"],
                        "公司":   stk["company"],
                        "持有人": stk["owner"],
                        "股數":   stk["shares"],
                        "票面額": stk["face_value"],
                        "申報總額": stk["total_twd"],
                        "板塊":   TW_SECTOR_MAP.get(stk["company"], "其他"),
                        "是否本人": stk["owner"] == filing["Name"],
                    })
                time.sleep(0.3)
            except Exception:
                pass
    finally:
        prog.empty()

    return pd.DataFrame(rows)


TW_CITIES = [
    "臺北市", "新北市", "桃園市", "臺中市", "臺南市", "高雄市",
    "基隆市", "新竹市", "嘉義市",
    "新竹縣", "苗栗縣", "彰化縣", "南投縣", "雲林縣", "嘉義縣",
    "屏東縣", "宜蘭縣", "花蓮縣", "臺東縣", "澎湖縣", "金門縣", "連江縣",
]


# ════════════════════════════════════════════════════════════════
# ── Sunburst 輔助（Fix #2：O(n²) → O(n)）────────────────────────
# ════════════════════════════════════════════════════════════════
def build_sunburst(
    df: pd.DataFrame,
    group_col: str,
    leaf_col: str,
    value_col: str | None,
) -> tuple[list, list, list]:
    """
    Fix #2：用 pivot 取代巢狀迴圈，時間複雜度從 O(n²) 降至 O(n)。

    group_col : 中層分類欄位（板塊）
    leaf_col  : 葉節點欄位（標的 / 公司）
    value_col : None 代表計數；否則為加總欄位名稱
    """
    if value_col:
        root_val = df[value_col].sum()
        sec_agg  = df.groupby(group_col)[value_col].sum()
        lf_agg   = df.groupby([group_col, leaf_col])[value_col].sum()
    else:
        root_val = len(df)
        sec_agg  = df.groupby(group_col).size()
        lf_agg   = df.groupby([group_col, leaf_col]).size()

    parents, labels, values = [], [], []
    parents += ["",     "全部"]
    labels  += ["全部", "（其他）"]
    values  += [root_val, 0]

    for sector, s_val in sec_agg.items():
        parents.append("全部")
        labels.append(sector)
        values.append(s_val)

    # lf_agg index is MultiIndex (sector, leaf) — 直接迭代，無巢狀迴圈
    for (sector, leaf), lf_val in lf_agg.items():
        parents.append(sector)
        labels.append(leaf)
        values.append(lf_val)

    return parents, labels, values


# ════════════════════════════════════════════════════════════════
# ── 側邊欄 ───────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("💾 國會交易追蹤（離線）")

    # ── 資料同步狀態 ──
    with st.expander("📊 本地資料庫狀態", expanded=not _local_db_ready()):
        if not _local_db_ready():
            st.error("找不到 data.db 或資料表為空。\n\n"
                     "請先執行：\n```\npython sync_data.py --source all\n```")
        else:
            _log = _get_sync_log()
            if _log.empty:
                st.warning("data.db 存在但 sync_log 為空")
            else:
                st.dataframe(
                    _log.rename(columns={
                        "source": "來源", "last_synced": "上次同步", "row_count": "筆數",
                    }),
                    hide_index=True, use_container_width=True,
                )

        if st.button("⬇️ 從 GitHub Release 重新下載 data.db",
                     use_container_width=True, key="redl_db"):
            if os.path.exists(DATA_DB_PATH):
                try: os.remove(DATA_DB_PATH)
                except Exception: pass
            ok, msg = _download_data_db_from_release()
            (st.success if ok else st.error)(msg)
            st.cache_data.clear()
            st.rerun()

        st.caption("若要在背景重新同步資料：")
        _sync_target = st.selectbox(
            "同步範圍",
            ["all", "us_house", "us_senate", "tw_legislator"],
            key="sync_target",
        )
        if st.button("🔄 背景同步", use_container_width=True, key="bg_sync"):
            try:
                script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_data.py")
                subprocess.Popen(
                    ["python3", script, "--source", _sync_target],
                    cwd=os.path.dirname(script),
                )
                st.success(f"已在背景啟動同步：{_sync_target}\n稍後重新整理本頁即可看到新資料。")
                _get_sync_log.clear()
            except Exception as e:
                st.error(f"啟動同步失敗：{e}")

    country = st.radio("資料來源", ["🇺🇸 美國國會", "🇹🇼 台灣民代", "🏦 機構 13F"], horizontal=True)

    if country == "🇺🇸 美國國會":
        chambers = st.multiselect(
            "院別", ["眾議院", "參議院"], default=["眾議院", "參議院"],
        )
        st.caption("資料來源：House Clerk PTR ＋ Senate eFD（本地快取）")
        _range_opts = {
            "30 天": 30, "90 天": 90, "半年": 182,
            "1 年": 365, "2 年": 730, "3 年": 1095,
        }
        _range_label = st.selectbox("時間範圍", list(_range_opts.keys()), index=1)
        days = _range_opts[_range_label]
        if st.button("🔍 立即掃描", type="primary", use_container_width=True):
            st.cache_data.clear()
        st.divider()
        match_opt = st.radio("標的篩選", ["全部", "僅持倉標的", "非持倉標的"])
        type_opt  = st.radio("交易方向", ["全部", "只看買入", "只看賣出"])
        search    = st.text_input("搜尋議員 / 標的")
        st.divider()
        st.caption("你的持倉標的")
        st.caption(", ".join(PORTFOLIO_TICKERS))
    elif country == "🏦 機構 13F":
        st.caption("資料來源：SEC EDGAR 13F-HR（季末後 45 天內申報，免費官方）")
        if st.button("🔄 重新抓取", type="primary", use_container_width=True):
            st.cache_data.clear()
        st.divider()
        f13_search = st.text_input("搜尋標的（CUSIP / 公司名）")
        f13_change_filter = st.multiselect(
            "只顯示變動類型", ["NEW", "ADD", "REDUCE", "EXIT"],
            default=["NEW", "ADD", "REDUCE", "EXIT"],
        )
    else:
        st.caption("資料來源：監察院財產申報公示系統")
        tw_role = st.selectbox(
            "民代類別",
            ["立法委員", "縣市議員"],
            key="tw_role",
        )
        tw_cities_sel: list[str] = []
        if tw_role == "縣市議員":
            st.warning(
                "⚠️ 縣市議員全國共約 900 位、7,000+ 筆申報，資料量龐大。\n\n"
                "**建議**：選取單一縣市，或將時間範圍縮到單一年度，避免解析時間過長。",
                icon="📢",
            )
            tw_cities_sel = st.multiselect(
                "縣市（可多選）",
                options=TW_CITIES,
                default=["臺北市"],
                key="tw_cities",
            )
        _now = datetime.now()
        _ym_list: list[str] = []
        _y, _m = _now.year - 5, _now.month
        while (_y, _m) <= (_now.year, _now.month):
            _ym_list.append(f"{_y}-{_m:02d}")
            _m += 1
            if _m > 12:
                _m = 1
                _y += 1
        _default_from = _ym_list[max(0, len(_ym_list) - 13)]
        _default_to   = _ym_list[-1]
        tw_range = st.select_slider(
            "查詢時間範圍（月份）",
            options=_ym_list,
            value=(_default_from, _default_to),
            format_func=lambda s: f"{s[:4]}年{s[5:]}月",
        )
        tw_date_from = tw_range[0] + "-01"

        # Fix #5：用 calendar.monthrange 取得當月真實最後一天，取代硬編碼 -28
        _tw_end_y = int(tw_range[1][:4])
        _tw_end_m = int(tw_range[1][5:])
        _last_day = calendar.monthrange(_tw_end_y, _tw_end_m)[1]
        tw_date_to = f"{tw_range[1]}-{_last_day:02d}"

        if st.button("🔍 重新載入", type="primary", use_container_width=True):
            st.cache_data.clear()
        st.divider()
        tw_search = st.text_input("搜尋姓名 / 公司")
        tw_owner  = st.radio("持有人", ["全部", "僅本人", "配偶/子女"])


# ════════════════════════════════════════════════════════════════
# ── 美國國會 頁面 ─────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
if country == "🇺🇸 美國國會":
    _today_str = date.today().isoformat()   # Fix #7
    dfs = []
    with st.spinner("載入資料中…"):
        if "眾議院" in chambers:
            dfs.append(load_us_trades(days, _today_str))
        if "參議院" in chambers:
            dfs.append(load_senate_trades(days, _today_str))
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if df.empty:
        st.warning("無資料，請點擊「立即掃描」")
        st.stop()

    # 篩選
    dff = df.copy()
    if match_opt == "僅持倉標的":
        dff = dff[dff["持倉"]]
    elif match_opt == "非持倉標的":
        dff = dff[~dff["持倉"]]
    if type_opt == "只看買入":
        dff = dff[dff["操作"] == "Purchase"]
    elif type_opt == "只看賣出":
        dff = dff[dff["操作"] == "Sale"]
    if search:
        kw = search.lower()
        dff = dff[dff["議員"].str.lower().str.contains(kw) |
                  dff["標的"].str.lower().str.contains(kw)]

    # 統計卡
    hits  = df[df["持倉"]]
    buys  = df[df["操作"] == "Purchase"]
    sells = df[df["操作"] == "Sale"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("總交易筆數", len(df))
    c2.metric("買入", len(buys))
    c3.metric("賣出", len(sells))
    c4.metric("命中持倉", len(hits))

    if not hits.empty:
        hit_tickers = sorted(hits["標的"].unique())
        st.error(f"⚡ 命中你的持倉：**{', '.join(hit_tickers)}** ({len(hits)} 筆)")

    st.divider()

    # 圖表 Row 1
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("標的買賣次數（Top 20）")
        tk_buy  = df[df["操作"]=="Purchase"]["標的"].value_counts()
        tk_sell = df[df["操作"]=="Sale"]["標的"].value_counts()
        all_tks = (tk_buy.add(tk_sell, fill_value=0)
                   .sort_values(ascending=False).head(20).index.tolist())
        fig_tk = go.Figure()
        colors_buy  = ["#cc9900" if t in PORTFOLIO_SET else "#2a6fb5" for t in all_tks]
        colors_sell = ["#cc4400" if t in PORTFOLIO_SET else "#8b2222" for t in all_tks]
        fig_tk.add_bar(name="買入", x=all_tks,
                       y=[tk_buy.get(t, 0) for t in all_tks], marker_color=colors_buy)
        fig_tk.add_bar(name="賣出", x=all_tks,
                       y=[tk_sell.get(t, 0) for t in all_tks], marker_color=colors_sell)
        fig_tk.update_layout(barmode="stack", height=320,
                             margin=dict(t=10,b=10,l=10,r=10),
                             paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             legend=dict(orientation="h"), font=dict(color="#aaa"))
        fig_tk.update_xaxes(tickangle=45)
        st.plotly_chart(fig_tk, use_container_width=True)

    with col_r:
        st.subheader("板塊分佈（點選板塊可下鑽）")
        us_mode = st.radio("統計方式", ["項目統計", "數量統計（估算金額）"],
                           horizontal=True, key="us_sun_mode")

        # Fix #2：改用 build_sunburst()，O(n) 取代 O(n²)
        if us_mode == "項目統計":
            sun_p, sun_l, sun_v = build_sunburst(df, "板塊", "標的", None)
        else:
            sun_p, sun_l, sun_v = build_sunburst(df, "板塊", "標的", "金額_數值")

        fig_sec = go.Figure(go.Sunburst(
            labels=sun_l, parents=sun_p, values=sun_v,
            branchvalues="total", insidetextorientation="radial",
            marker=dict(colors=px.colors.qualitative.Set2 * 10),
        ))
        fig_sec.update_layout(height=360, margin=dict(t=10,b=10,l=10,r=10),
                               paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#aaa"))
        st.plotly_chart(fig_sec, use_container_width=True)

    # 圖表 Row 2
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        st.subheader("每日交易量")
        daily = df.groupby(["交易日_dt", "操作"]).size().unstack(fill_value=0).sort_index()
        fig_t = go.Figure()
        if "Purchase" in daily.columns:
            fig_t.add_bar(name="買入", x=daily.index, y=daily["Purchase"], marker_color="#2a6fb5")
        if "Sale" in daily.columns:
            fig_t.add_bar(name="賣出", x=daily.index, y=daily["Sale"], marker_color="#8b2222")
        fig_t.update_layout(barmode="stack", height=300,
                            margin=dict(t=10,b=10,l=10,r=10),
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            legend=dict(orientation="h"), font=dict(color="#aaa"))
        st.plotly_chart(fig_t, use_container_width=True)

    with col_r2:
        st.subheader("最活躍議員（Top 10）")
        st.caption("點選議員名稱可查看其交易標的明細")
        pol_ct    = df["議員"].value_counts().head(10)
        pol_names = pol_ct.index.tolist()
        fig_p = px.bar(x=pol_ct.values, y=pol_ct.index, orientation="h",
                       height=300, color_discrete_sequence=["#4a9eff"])
        fig_p.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                             paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             xaxis_title="", yaxis_title="", font=dict(color="#aaa"))
        sel_pol = st.plotly_chart(fig_p, use_container_width=True,
                                  on_select="rerun", key="us_pol_bar")

    # 議員下鑽
    selected_pol = None
    if sel_pol and sel_pol.selection and sel_pol.selection.points:
        pt  = sel_pol.selection.points[0]
        idx = pt.get("point_index")
        if idx is not None and idx < len(pol_names):
            selected_pol = pol_names[idx]
        elif pt.get("y") in pol_names:
            selected_pol = pt["y"]

    if selected_pol:
        st.markdown(f"#### 📊 {selected_pol} — 交易標的明細")
        pol_df = df[df["議員"] == selected_pol]

        # Fix #6：金額欄位判斷提前統一，避免重複條件
        use_amount = "金額_數值" in pol_df.columns
        if use_amount:
            pol_buy  = pol_df[pol_df["操作"] == "Purchase"].groupby("標的")["金額_數值"].sum()
            pol_sell = pol_df[pol_df["操作"] == "Sale"].groupby("標的")["金額_數值"].sum()
        else:
            pol_buy  = pol_df[pol_df["操作"] == "Purchase"]["標的"].value_counts().astype(float)
            pol_sell = pol_df[pol_df["操作"] == "Sale"]["標的"].value_counts().astype(float)

        all_pol_tks = sorted(
            set(pol_buy.index) | set(pol_sell.index),
            key=lambda t: pol_buy.get(t, 0.0) - pol_sell.get(t, 0.0),
            reverse=True,
        )
        net_vals = [(pol_buy.get(t, 0.0) - pol_sell.get(t, 0.0)) / 1e6
                    for t in all_pol_tks]
        bar_colors = [
            ("#cc9900" if v >= 0 else "#cc4400") if t in PORTFOLIO_SET
            else ("#2a6fb5" if v >= 0 else "#8b2222")
            for t, v in zip(all_pol_tks, net_vals)
        ]
        fig_pd = go.Figure()
        fig_pd.add_bar(name="累積買進量（買進－賣出）", x=all_pol_tks, y=net_vals,
                       marker_color=bar_colors)
        fig_pd.add_hline(y=0, line_color="#555", line_dash="dot")
        fig_pd.update_layout(height=280, showlegend=False,
                             margin=dict(t=10,b=10,l=10,r=10),
                             paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             yaxis_title="淨金額（百萬美元，買進－賣出）",
                             font=dict(color="#aaa"))
        fig_pd.update_xaxes(tickangle=45)
        st.plotly_chart(fig_pd, use_container_width=True)
        pol_detail = (pol_df[["標的","操作","金額","交易日","板塊"]]
                      .copy()
                      .assign(操作=pol_df["操作"].map({"Purchase":"買入 🔵","Sale":"賣出 🔴"}))
                      .sort_values("交易日", ascending=False))
        st.dataframe(pol_detail, use_container_width=True, height=260,
                     column_config={"金額": st.column_config.TextColumn("金額", width=160)})

    # 標的買賣量
    st.subheader("標的買賣量（Top 20，估算中位金額）")
    if "金額_數值" in df.columns:
        vol_buy  = df[df["操作"]=="Purchase"].groupby("標的")["金額_數值"].sum()
        vol_sell = df[df["操作"]=="Sale"].groupby("標的")["金額_數值"].sum()
        all_vol_tks = (vol_buy.add(vol_sell, fill_value=0)
                       .sort_values(ascending=False).head(20).index.tolist())
        fig_vol = go.Figure()
        fig_vol.add_bar(name="買入", x=all_vol_tks,
                        y=[vol_buy.get(t, 0)/1e6 for t in all_vol_tks],
                        marker_color=["#cc9900" if t in PORTFOLIO_SET else "#2a6fb5" for t in all_vol_tks])
        fig_vol.add_bar(name="賣出", x=all_vol_tks,
                        y=[vol_sell.get(t, 0)/1e6 for t in all_vol_tks],
                        marker_color=["#cc4400" if t in PORTFOLIO_SET else "#8b2222" for t in all_vol_tks])
        fig_vol.update_layout(barmode="stack", height=320,
                              margin=dict(t=10,b=10,l=10,r=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              legend=dict(orientation="h"), yaxis_title="金額（百萬美元）",
                              font=dict(color="#aaa"))
        fig_vol.update_xaxes(tickangle=45)
        st.plotly_chart(fig_vol, use_container_width=True)

    st.divider()

    # ── 跟單策略績效（Copy-Trade Backtest）──
    st.subheader("📈 跟單策略績效（Copy-Trade Backtest）")
    st.caption(
        "將期間內每筆 Purchase 視為等額買入、持有至今日，與 S&P 500（SPY）同日等額進場做對照。"
        "策略淨值＝所有進場部位的平均報酬。"
    )
    _strat_res = compute_copytrade_strategy(
        df[df["操作"] == "Purchase"][["標的", "交易日_dt"]],
        "SPY",
        _today_str,
    )
    if _strat_res is None:
        st.info("無法計算策略績效（樣本不足或 yfinance 無法取得股價）")
    else:
        _m  = _strat_res["metrics"]
        _mb = _strat_res["bench_metrics"]
        _total_pct = _m.get("total", 0) * 100
        _bench_pct = _mb.get("total", 0) * 100
        _delta_pct = _total_pct - _bench_pct
        _color = "#00d4aa" if _total_pct >= 0 else "#ff5c5c"
        st.markdown(
            f"### <span style='color:{_color}'>{_total_pct:+.2f}%</span>"
            f"　<span style='color:#888;font-size:0.7em'>期間總報酬　·　"
            f"SPY {_bench_pct:+.2f}%　·　超額 {_delta_pct:+.2f}%</span>",
            unsafe_allow_html=True,
        )

        _curve = _strat_res["curve"]
        _fig_eq = go.Figure()
        _fig_eq.add_scatter(x=_curve["date"], y=_curve["策略"],
                            name="國會跟單策略",
                            line=dict(color="#00d4aa", width=2),
                            fill="tozeroy", fillcolor="rgba(0,212,170,0.08)")
        _fig_eq.add_scatter(x=_curve["date"], y=_curve["大盤"],
                            name="SPY", line=dict(color="#888", width=1.5, dash="dot"))
        _fig_eq.update_layout(
            height=320, margin=dict(t=10, b=10, l=10, r=10),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.0),
            font=dict(color="#aaa"),
            yaxis_title="淨值（起始=1）", xaxis_title="",
        )
        st.plotly_chart(_fig_eq, use_container_width=True)

        # Key Metrics — 兩列四欄
        _c1, _c2, _c3, _c4 = st.columns(4)
        _c1.metric("Return (1d)",  f"{_m.get('r1d', 0)*100:+.2f}%")
        _c2.metric("Return (30d)", f"{_m.get('r30', 0)*100:+.2f}%")
        _c3.metric("Return (1Y)",  f"{_m.get('r1y', 0)*100:+.2f}%")
        _c4.metric("CAGR (Total)", f"{_m.get('cagr', 0)*100:+.2f}%")
        _d1, _d2, _d3, _d4 = st.columns(4)
        _d1.metric("Max Drawdown", f"{_m.get('mdd', 0)*100:.2f}%")
        _d2.metric("Beta",         f"{_strat_res['beta']:.2f}"    if _strat_res["beta"]  is not None else "—")
        _d3.metric("Alpha",        f"{_strat_res['alpha']*100:+.2f}%" if _strat_res["alpha"] is not None else "—")
        _d4.metric("Sharpe Ratio", f"{_m.get('sharpe', 0):.3f}")
        st.caption(
            f"樣本：{_strat_res['n_trades']} 筆買入、{_strat_res['n_tickers']} 檔標的"
            f"（略過 {_strat_res['n_skipped']} 筆無股價）　·　股價來源：yfinance"
        )

    st.divider()

    # 明細表
    st.subheader(f"明細（{len(dff)} 筆）")

    # ── 姓名篩選 ──
    _us_name_q = st.text_input(
        "🔎 依議員姓名篩選（支援部分字串，不分大小寫）",
        key="us_name_filter",
        placeholder="例：Pelosi / Tuberville",
    )
    if _us_name_q:
        _kw = _us_name_q.strip().lower()
        dff = dff[dff["議員"].str.lower().str.contains(_kw, na=False)]
        st.caption(f"篩選後：{len(dff)} 筆")

    # ── 議員統計 ──
    _us_legislators   = dff["議員"].nunique()
    _us_tickers       = dff["標的"].nunique()
    _us_house_count   = dff[dff["院"] == "眾議院"]["議員"].nunique()
    _us_senate_count  = dff[dff["院"] == "參議院"]["議員"].nunique()
    _chamber_parts = []
    if _us_house_count:
        _chamber_parts.append(f"眾議院 {_us_house_count} 人")
    if _us_senate_count:
        _chamber_parts.append(f"參議院 {_us_senate_count} 人")
    _chamber_str = "、".join(_chamber_parts) if _chamber_parts else ""
    st.markdown(
        f"共 **{_us_legislators}** 位議員（{_chamber_str}）交易了 "
        f"**{_us_tickers}** 檔標的"
    )

    # ── 標的說明查詢（下拉選單 + 編輯）──
    _us_traded_tickers = sorted(dff["標的"].unique())
    _us_db_descs = _get_all_descriptions("US")          # 批次讀取 DB
    _tk_selected = st.selectbox(
        "📖 查詢標的說明",
        options=["（請選擇標的）"] + _us_traded_tickers,
        index=0,
        key="us_ticker_info",
    )
    if _tk_selected != "（請選擇標的）":
        _tk_desc   = _us_db_descs.get(_tk_selected, "")
        _tk_sector = SECTOR_MAP.get(_tk_selected, "其他")
        _tk_count  = len(dff[dff["標的"] == _tk_selected])
        _tk_buys   = len(dff[(dff["標的"] == _tk_selected) & (dff["操作"] == "Purchase")])
        _tk_sells  = len(dff[(dff["標的"] == _tk_selected) & (dff["操作"] == "Sale")])
        _tk_pols   = dff[dff["標的"] == _tk_selected]["議員"].nunique()
        st.info(
            f"**{_tk_selected}**　｜　板塊：{_tk_sector}　｜　"
            f"共 {_tk_count} 筆交易（買入 {_tk_buys} / 賣出 {_tk_sells}）"
            f"、{_tk_pols} 位議員交易\n\n"
            f"{_tk_desc or '暫無說明'}",
            icon="📊",
        )
        # 編輯區
        with st.expander("✏️ 編輯此標的說明", expanded=False):
            _us_edit_val = st.text_area(
                "說明內容", value=_tk_desc, height=100,
                key=f"us_edit_{_tk_selected}",
                placeholder="輸入標的說明，例如：蘋果公司 — iPhone、Mac、iPad 等消費電子…",
            )
            if st.button("💾 更新", key=f"us_save_{_tk_selected}", type="primary"):
                _upsert_description(_tk_selected, "US", _us_edit_val.strip())
                st.success(f"已更新 {_tk_selected} 的說明！")
                st.rerun()

    # ── 手動新增標的說明 ──
    with st.expander("➕ 新增標的說明", expanded=False):
        _us_new_col1, _us_new_col2 = st.columns([1, 3])
        with _us_new_col1:
            _us_new_ticker = st.text_input(
                "標的代碼", key="us_new_ticker",
                placeholder="例如：TSLA",
            ).strip().upper()
        with _us_new_col2:
            _us_new_desc = st.text_input(
                "說明", key="us_new_desc",
                placeholder="例如：特斯拉 — 電動車、儲能系統、自動駕駛",
            ).strip()
        if st.button("💾 新增", key="us_add_new", type="primary"):
            if _us_new_ticker and _us_new_desc:
                _upsert_description(_us_new_ticker, "US", _us_new_desc)
                st.success(f"已新增 {_us_new_ticker} 的說明！")
                st.rerun()
            else:
                st.warning("請輸入標的代碼與說明")

    # ── 多欄位篩選 + 排序 ──
    with st.expander("🔧 進階篩選 / 排序", expanded=False):
        # 篩選
        _us_f_col1, _us_f_col2, _us_f_col3, _us_f_col4 = st.columns(4)
        with _us_f_col1:
            _us_f_chamber = st.multiselect("院別", dff["院"].unique().tolist(),
                                           default=dff["院"].unique().tolist(), key="us_f_chamber")
        with _us_f_col2:
            _us_f_op = st.multiselect("操作", dff["操作"].unique().tolist(),
                                      default=dff["操作"].unique().tolist(), key="us_f_op")
        with _us_f_col3:
            _us_f_sector = st.multiselect("板塊", sorted(dff["板塊"].unique()),
                                          default=sorted(dff["板塊"].unique()), key="us_f_sector")
        with _us_f_col4:
            _us_f_hold = st.selectbox("持倉", ["全部", "僅持倉 ⭐", "非持倉"], key="us_f_hold2")
        dff = dff[dff["院"].isin(_us_f_chamber) & dff["操作"].isin(_us_f_op) & dff["板塊"].isin(_us_f_sector)]
        if _us_f_hold == "僅持倉 ⭐":
            dff = dff[dff["持倉"] == True]
        elif _us_f_hold == "非持倉":
            dff = dff[dff["持倉"] != True]

        st.markdown("---")
        # 多層排序（最多 3 層）
        _us_sort_cols = ["交易日", "金額_數值", "標的", "議員", "操作", "板塊", "院"]
        _us_s_col1, _us_s_dir1, _us_s_col2, _us_s_dir2, _us_s_col3, _us_s_dir3 = st.columns(6)
        with _us_s_col1:
            _us_sk1 = st.selectbox("首要排序", _us_sort_cols, index=1, key="us_sk1")
        with _us_s_dir1:
            _us_sd1 = st.selectbox("方向", ["降冪 ↓", "升冪 ↑"], key="us_sd1")
        with _us_s_col2:
            _us_sk2 = st.selectbox("次要排序", ["（無）"] + _us_sort_cols, index=1, key="us_sk2")
        with _us_s_dir2:
            _us_sd2 = st.selectbox("方向", ["降冪 ↓", "升冪 ↑"], key="us_sd2")
        with _us_s_col3:
            _us_sk3 = st.selectbox("第三排序", ["（無）"] + _us_sort_cols, index=0, key="us_sk3")
        with _us_s_dir3:
            _us_sd3 = st.selectbox("方向", ["降冪 ↓", "升冪 ↑"], key="us_sd3")

        _sort_by, _sort_asc = [], []
        for sk, sd in [(_us_sk1, _us_sd1), (_us_sk2, _us_sd2), (_us_sk3, _us_sd3)]:
            if sk and sk != "（無）":
                # 交易日用 datetime 欄位排序
                col = "交易日_dt" if sk == "交易日" else sk
                if col in dff.columns and col not in _sort_by:
                    _sort_by.append(col)
                    _sort_asc.append(sd == "升冪 ↑")
        if _sort_by:
            dff = dff.sort_values(_sort_by, ascending=_sort_asc)

        st.caption(f"篩選後：{len(dff)} 筆")

    display = dff[["議員","院","州","標的","操作","金額","交易日","揭露日","板塊","持倉"]].copy()
    display["操作"] = display["操作"].map({"Purchase":"買入 🔵","Sale":"賣出 🔴"})
    display["持倉"] = display["持倉"].map({True:"⭐","":""}).fillna("")
    st.dataframe(display, use_container_width=True, height=480,
                 column_config={
                     "持倉": st.column_config.TextColumn("持倉", width=50),
                     "操作": st.column_config.TextColumn("操作", width=80),
                     "金額": st.column_config.TextColumn("金額", width=160),
                 })
    st.caption("⚠ 依法議員需在交易後 45 天內申報。⭐ 代表你目前的持倉標的。"
               " | 資料來源：House Clerk PTR ＋ CapitolTrades（參議院）")


# ════════════════════════════════════════════════════════════════
# ── 台灣立委 頁面 ─────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
elif country == "🇹🇼 台灣民代":
    _role_label = "立委" if tw_role == "立法委員" else "議員"
    _from_lbl = f"{tw_range[0][:4]}年{tw_range[0][5:]}月"
    _to_lbl   = f"{tw_range[1][:4]}年{tw_range[1][5:]}月"
    _api_role = "立法委員" if tw_role == "立法委員" else "議員"
    _city_tuple = tuple(tw_cities_sel) if tw_role == "縣市議員" else None
    _city_note = f"　｜　縣市：{'、'.join(tw_cities_sel) or '全部'}" if tw_role == "縣市議員" else ""
    st.info(
        f"📋 {tw_role}　｜　查詢期間：{_from_lbl} ～ {_to_lbl}{_city_note}　｜　資料來源：監察院財產申報公示系統",
        icon="🇹🇼",
    )
    if tw_role == "縣市議員" and not tw_cities_sel:
        st.warning("請在左側至少選擇一個縣市再載入，避免資料量過大。")
        st.stop()

    with st.spinner(f"載入{tw_role}持股資料（首次需數分鐘）…"):
        tw_df = load_tw_holdings(tw_date_from, tw_date_to, _api_role, _city_tuple)

    if tw_df.empty:
        st.warning("查無股票申報資料，可能該期別尚未有資料或解析失敗。")
        st.stop()

    # 篩選
    dff_tw = tw_df.copy()
    if tw_search:
        kw = tw_search.lower()
        dff_tw = dff_tw[dff_tw["姓名"].str.lower().str.contains(kw) |
                        dff_tw["公司"].str.lower().str.contains(kw)]
    if tw_owner == "僅本人":
        dff_tw = dff_tw[dff_tw["是否本人"]]
    elif tw_owner == "配偶/子女":
        dff_tw = dff_tw[~dff_tw["是否本人"]]

    # 統計卡
    total_legislators        = tw_df["姓名"].nunique()
    legislators_with_stocks  = tw_df[tw_df["股數"] > 0]["姓名"].nunique()
    total_companies          = tw_df["公司"].nunique()
    total_shares             = tw_df["股數"].sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"申報{_role_label}人數", total_legislators)
    c2.metric("持有股票人數", legislators_with_stocks)
    c3.metric("持股公司種類", total_companies)
    c4.metric("持股總股數", f"{total_shares:,.0f}")

    st.divider()

    # 圖表 Row 1
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader(f"{_role_label}持股數排行（Top 20）")
        st.caption(f"點選{_role_label}名稱可查看其持股明細")
        leg_shares = (tw_df.groupby("姓名")["股數"].sum()
                      .sort_values(ascending=False).head(20))
        leg_names  = leg_shares.index.tolist()
        fig_leg = px.bar(x=leg_shares.values / 1000, y=leg_shares.index,
                         orientation="h", height=400,
                         color_discrete_sequence=["#4a9eff"])
        fig_leg.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                               paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               xaxis_title="持股數（千股）", yaxis_title="",
                               font=dict(color="#aaa"))
        sel_leg = st.plotly_chart(fig_leg, use_container_width=True,
                                  on_select="rerun", key="tw_leg_bar")

    with col_r:
        st.subheader("板塊分佈（點選板塊可下鑽）")
        tw_mode = st.radio("統計方式", ["項目統計", "數量統計（股數）"],
                           horizontal=True, key="tw_sun_mode")

        # Fix #2：改用 build_sunburst()
        if tw_mode == "項目統計":
            tw_sun_p, tw_sun_l, tw_sun_v = build_sunburst(tw_df, "板塊", "公司", None)
        else:
            tw_sun_p, tw_sun_l, tw_sun_v = build_sunburst(tw_df, "板塊", "公司", "股數")

        fig_tw_sec = go.Figure(go.Sunburst(
            labels=tw_sun_l, parents=tw_sun_p, values=tw_sun_v,
            branchvalues="total", insidetextorientation="radial",
            marker=dict(colors=px.colors.qualitative.Set3 * 10),
        ))
        fig_tw_sec.update_layout(height=360, margin=dict(t=10,b=10,l=10,r=10),
                                  paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#aaa"))
        st.plotly_chart(fig_tw_sec, use_container_width=True)

    # 人員下鑽
    selected_leg = None
    if sel_leg and sel_leg.selection and sel_leg.selection.points:
        pt  = sel_leg.selection.points[0]
        idx = pt.get("point_index")
        if idx is not None and idx < len(leg_names):
            selected_leg = leg_names[idx]
        elif pt.get("y") in leg_names:
            selected_leg = pt["y"]

    if selected_leg:
        st.markdown(f"#### 📊 {selected_leg} — 持股明細")
        ld = tw_df[tw_df["姓名"] == selected_leg].copy()
        ld_grp = (ld.groupby("公司")["股數"].sum()
                    .sort_values(ascending=False)
                    .reset_index())
        fig_ld = px.bar(ld_grp, x="公司", y="股數",
                        height=300, color_discrete_sequence=["#4a9eff"],
                        labels={"公司": "", "股數": "股數"})
        fig_ld.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              showlegend=False, font=dict(color="#aaa"))
        fig_ld.update_xaxes(tickangle=45)
        st.plotly_chart(fig_ld, use_container_width=True)
        disp_ld = ld[["公司","持有人","股數","票面額","申報總額","板塊"]].sort_values("股數", ascending=False)
        st.dataframe(disp_ld, use_container_width=True, height=260,
                     column_config={
                         "股數":   st.column_config.NumberColumn("股數", format="%d"),
                         "票面額": st.column_config.NumberColumn("票面額(元)", format="%.0f"),
                         "申報總額": st.column_config.NumberColumn("申報總額(元)", format="%.0f"),
                     })

    # 圖表 Row 2
    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader(f"熱門持股（被最多{_role_label}持有）")
        co_cnt = (tw_df.groupby("公司")["姓名"].nunique()
                  .sort_values(ascending=False).head(20))
        fig_co = px.bar(x=co_cnt.index, y=co_cnt.values,
                        height=320, color_discrete_sequence=["#f0a500"])
        fig_co.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              xaxis_title="", yaxis_title=f"持有{_role_label}數",
                              font=dict(color="#aaa"))
        fig_co.update_xaxes(tickangle=45)
        st.plotly_chart(fig_co, use_container_width=True)

    with col_r2:
        st.subheader("本人 vs 配偶/子女持股（股數）")
        owner_grp = tw_df.groupby("是否本人")["股數"].sum()
        labels    = {True: "本人", False: "配偶/子女"}
        fig_own = px.pie(
            values=owner_grp.values,
            names=[labels.get(k, str(k)) for k in owner_grp.index],
            hole=0.4, height=320,
            color_discrete_sequence=["#4a9eff", "#f06080"])
        fig_own.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                               paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#aaa"))
        st.plotly_chart(fig_own, use_container_width=True)

    st.divider()

    # 明細表
    st.subheader(f"持股明細（{len(dff_tw)} 筆）")

    # ── 姓名篩選 ──
    _tw_name_q = st.text_input(
        f"🔎 依{_role_label}姓名篩選（支援部分字串）",
        key="tw_name_filter",
        placeholder="例：黃國昌",
    )
    if _tw_name_q:
        _kw = _tw_name_q.strip()
        dff_tw = dff_tw[dff_tw["姓名"].astype(str).str.contains(_kw, na=False)]
        st.caption(f"篩選後：{len(dff_tw)} 筆")

    # ── 人員統計 ──
    _tw_leg_count = dff_tw["姓名"].nunique()
    _tw_co_count  = dff_tw["公司"].nunique()
    st.markdown(
        f"共 **{_tw_leg_count}** 位{_role_label}持有 **{_tw_co_count}** 家公司股票"
    )

    # ── 公司說明查詢（下拉選單 + 編輯）──
    _tw_companies = sorted(dff_tw["公司"].unique())
    _tw_db_descs = _get_all_descriptions("TW")           # 批次讀取 DB
    _tw_selected = st.selectbox(
        "📖 查詢公司說明",
        options=["（請選擇公司）"] + _tw_companies,
        index=0,
        key="tw_company_info",
    )
    if _tw_selected != "（請選擇公司）":
        _tw_desc   = _tw_db_descs.get(_tw_selected, "")
        _tw_sector = TW_SECTOR_MAP.get(_tw_selected, "其他")
        _tw_legs   = dff_tw[dff_tw["公司"] == _tw_selected]["姓名"].nunique()
        _tw_shares = dff_tw[dff_tw["公司"] == _tw_selected]["股數"].sum()
        st.info(
            f"**{_tw_selected}**　｜　板塊：{_tw_sector}　｜　"
            f"共 {_tw_legs} 位{_role_label}持有、合計 {_tw_shares:,.0f} 股\n\n"
            f"{_tw_desc or '暫無說明'}",
            icon="📊",
        )
        # 編輯區
        with st.expander("✏️ 編輯此公司說明", expanded=False):
            _tw_edit_val = st.text_area(
                "說明內容", value=_tw_desc, height=100,
                key=f"tw_edit_{_tw_selected}",
                placeholder="輸入公司說明，例如：全球最大晶圓代工廠…",
            )
            if st.button("💾 更新", key=f"tw_save_{_tw_selected}", type="primary"):
                _upsert_description(_tw_selected, "TW", _tw_edit_val.strip())
                st.success(f"已更新 {_tw_selected} 的說明！")
                st.rerun()

    # ── 手動新增公司說明 ──
    with st.expander("➕ 新增公司說明", expanded=False):
        _tw_new_col1, _tw_new_col2 = st.columns([1, 3])
        with _tw_new_col1:
            _tw_new_company = st.text_input(
                "公司名稱", key="tw_new_company",
                placeholder="例如：台積電",
            ).strip()
        with _tw_new_col2:
            _tw_new_desc = st.text_input(
                "說明", key="tw_new_desc",
                placeholder="例如：全球最大晶圓代工廠，先進製程晶片製造",
            ).strip()
        if st.button("💾 新增", key="tw_add_new", type="primary"):
            if _tw_new_company and _tw_new_desc:
                _upsert_description(_tw_new_company, "TW", _tw_new_desc)
                st.success(f"已新增 {_tw_new_company} 的說明！")
                st.rerun()
            else:
                st.warning("請輸入公司名稱與說明")

    # ── 多欄位篩選 + 排序 ──
    with st.expander("🔧 進階篩選 / 排序", expanded=False):
        _tw_f_col1, _tw_f_col2, _tw_f_col3 = st.columns(3)
        with _tw_f_col1:
            _tw_f_holder = st.multiselect("持有人", sorted(dff_tw["持有人"].dropna().unique()),
                                          default=sorted(dff_tw["持有人"].dropna().unique()), key="tw_f_holder")
        with _tw_f_col2:
            _tw_f_sector = st.multiselect("板塊", sorted(dff_tw["板塊"].unique()),
                                          default=sorted(dff_tw["板塊"].unique()), key="tw_f_sector")
        with _tw_f_col3:
            if tw_role == "縣市議員" and "縣市" in dff_tw.columns:
                _tw_f_city = st.multiselect("縣市", sorted(dff_tw["縣市"].dropna().unique()),
                                            default=sorted(dff_tw["縣市"].dropna().unique()), key="tw_f_city")
                dff_tw = dff_tw[dff_tw["縣市"].isin(_tw_f_city)]
            else:
                st.empty()
        dff_tw = dff_tw[dff_tw["持有人"].isin(_tw_f_holder) & dff_tw["板塊"].isin(_tw_f_sector)]

        st.markdown("---")
        _tw_sort_cols = ["股數", "申報總額", "票面額", "申報日", "公司", "姓名", "板塊"]
        _tw_s_col1, _tw_s_dir1, _tw_s_col2, _tw_s_dir2, _tw_s_col3, _tw_s_dir3 = st.columns(6)
        with _tw_s_col1:
            _tw_sk1 = st.selectbox("首要排序", _tw_sort_cols, index=0, key="tw_sk1")
        with _tw_s_dir1:
            _tw_sd1 = st.selectbox("方向", ["降冪 ↓", "升冪 ↑"], key="tw_sd1")
        with _tw_s_col2:
            _tw_sk2 = st.selectbox("次要排序", ["（無）"] + _tw_sort_cols, index=4, key="tw_sk2")
        with _tw_s_dir2:
            _tw_sd2 = st.selectbox("方向", ["降冪 ↓", "升冪 ↑"], key="tw_sd2")
        with _tw_s_col3:
            _tw_sk3 = st.selectbox("第三排序", ["（無）"] + _tw_sort_cols, index=0, key="tw_sk3")
        with _tw_s_dir3:
            _tw_sd3 = st.selectbox("方向", ["降冪 ↓", "升冪 ↑"], key="tw_sd3")

        _tw_sort_by, _tw_sort_asc = [], []
        for sk, sd in [(_tw_sk1, _tw_sd1), (_tw_sk2, _tw_sd2), (_tw_sk3, _tw_sd3)]:
            if sk and sk != "（無）" and sk in dff_tw.columns and sk not in _tw_sort_by:
                _tw_sort_by.append(sk)
                _tw_sort_asc.append(sd == "升冪 ↑")
        if _tw_sort_by:
            dff_tw = dff_tw.sort_values(_tw_sort_by, ascending=_tw_sort_asc)

        st.caption(f"篩選後：{len(dff_tw)} 筆")

    _disp_cols = ["姓名","職稱","縣市","公司","持有人","股數","票面額","申報總額","板塊","申報日"] \
        if tw_role == "縣市議員" else \
        ["姓名","公司","持有人","股數","票面額","申報總額","板塊","申報日"]
    disp_tw = dff_tw[_disp_cols].copy()
    disp_tw["是否本人"] = dff_tw["是否本人"].map({True:"本人 ✅", False:"配偶/子女"})
    st.dataframe(disp_tw, use_container_width=True, height=480,
                 column_config={
                     "股數":   st.column_config.NumberColumn("股數", format="%d"),
                     "票面額": st.column_config.NumberColumn("票面額(元)", format="%.0f"),
                     "申報總額": st.column_config.NumberColumn("申報總額(元)", format="%.0f"),
                 })
    st.caption("⚠ 資料為年度財產申報，非即時交易。"
               " | 資料來源：監察院財產申報公示系統 priso.cy.gov.tw")


# ════════════════════════════════════════════════════════════════
# ── 機構 13F 頁面 ─────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
else:
    import funds_13f as f13

    st.title("🏦 機構 13F 持股追蹤")
    st.caption("Berkshire / Bridgewater / Scion / Pershing Square / Renaissance / Appaloosa / Duquesne / ARK / Oaktree")

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_funds():
        # 離線版：從 data.db 的 funds_13f_cache 讀取
        if not _local_db_ready():
            st.warning("⚠️ 尚未同步 13F 資料，請先執行 `python sync_data.py --source funds_13f`")
            return []
        try:
            with sqlite3.connect(DATA_DB_PATH) as c:
                row = c.execute(
                    "SELECT payload, _synced_at FROM funds_13f_cache WHERE key='all'"
                ).fetchone()
            if not row:
                st.warning("⚠️ data.db 無 13F 快取，請先執行 `python sync_data.py --source funds_13f`")
                return []
            import json as _json
            st.caption(f"📦 本地快取時間：{row[1]}")
            return _json.loads(row[0])
        except Exception as e:
            st.error(f"讀取 13F 快取失敗：{e}")
            return []

    funds = _load_funds()

    def _fmt_val(v):
        if v is None: return "—"
        if abs(v) >= 1e9: return f"${v/1e9:.2f}B"
        if abs(v) >= 1e6: return f"${v/1e6:.1f}M"
        if abs(v) >= 1e3: return f"${v/1e3:.1f}K"
        return f"${v:.0f}"

    # 概覽卡
    valid = [f for f in funds if not f.get("error")]
    cA, cB, cC = st.columns(3)
    cA.metric("追蹤基金數", f"{len(valid)} / {len(funds)}")
    cB.metric("總管理市值", _fmt_val(sum(f["total_value"] for f in valid)))
    cC.metric("總持股檔次", sum(f["holdings_count"] for f in valid))

    st.divider()

    # 跨基金交叉視圖：看哪些標的被多個大佬同時持有
    st.subheader("🌟 大佬共同持股（被 ≥2 檔基金持有）")
    cross = {}
    for f in valid:
        for h in f["top_holdings"]:
            k = h["name"]
            cross.setdefault(k, []).append((f["manager"], h["value"]))
    cross_rows = [
        {"標的": k, "持有基金數": len(v), "經理人": ", ".join(m for m, _ in v),
         "總市值": sum(val for _, val in v)}
        for k, v in cross.items() if len(v) >= 2
    ]
    if cross_rows:
        cross_df = pd.DataFrame(cross_rows).sort_values("總市值", ascending=False)
        cross_df["總市值"] = cross_df["總市值"].apply(_fmt_val)
        st.dataframe(cross_df, use_container_width=True, height=280, hide_index=True)
    else:
        st.caption("無共同持股")

    st.divider()

    # 每檔基金一個 expander
    for f in funds:
        if f.get("error"):
            with st.expander(f"⚠️ {f['name']} ({f['manager']}) — {f['error']}"):
                pass
            continue

        header = (f"**{f['name']}** · {f['manager']}　｜　"
                  f"{f['latest_form']} {f['latest_filed']}　｜　"
                  f"{f['holdings_count']} 檔　｜　{_fmt_val(f['total_value'])}")
        with st.expander(header, expanded=(f["manager"] in ("Warren Buffett", "Michael Burry"))):
            if f.get("desc"):
                st.caption(f"📖 {f['desc']}")

            # 歷史季度選擇（近 3 年）
            history = f.get("history") or []
            if len(history) > 1:
                labels = [f"{h['filed']} ({h['form']})" for h in history]
                sel_idx = st.selectbox(
                    "選擇季度",
                    range(len(labels)),
                    format_func=lambda i: labels[i],
                    key=f"hist_{f['cik']}",
                )
                snap = history[sel_idx]
                snap_top = snap["top_holdings"]
                snap_total = snap["total_value"]
                snap_changes = snap["changes"]
                snap_cnt = snap["holdings_count"]
                st.caption(f"📅 {snap['filed']}　｜　{snap_cnt} 檔　｜　{_fmt_val(snap_total)}")
            else:
                snap_top = f["top_holdings"]
                snap_total = f["total_value"]
                snap_changes = f["changes"]

            col1, col2 = st.columns(2)

            # Top 持股
            with col1:
                st.markdown("##### Top 20 持股")
                top_df = pd.DataFrame(snap_top)
                if not top_df.empty:
                    top_df["佔比"] = (top_df["value"] / max(snap_total, 1) * 100).round(2)
                    top_df = top_df[["name", "shares", "value", "佔比"]]
                    top_df.columns = ["標的", "股數", "市值", "佔比%"]
                    st.dataframe(
                        top_df, use_container_width=True, height=400, hide_index=True,
                        column_config={
                            "股數": st.column_config.NumberColumn(format="%d"),
                            "市值": st.column_config.NumberColumn(format="$%.0f"),
                        },
                    )

            # 季變動
            with col2:
                st.markdown("##### 本季變動")
                changes = snap_changes
                if f13_change_filter:
                    changes = [c for c in changes if c["change_type"] in f13_change_filter]
                if f13_search:
                    kw = f13_search.lower()
                    changes = [c for c in changes if kw in c["name"].lower() or kw in c["cusip"].lower()]
                if changes:
                    chg_df = pd.DataFrame(changes)[["name", "change_type", "shares_change", "curr_shares", "curr_value"]]
                    chg_df.columns = ["標的", "動作", "股數變動", "當前股數", "當前市值"]
                    st.dataframe(
                        chg_df, use_container_width=True, height=400, hide_index=True,
                        column_config={
                            "股數變動": st.column_config.NumberColumn(format="%+d"),
                            "當前股數": st.column_config.NumberColumn(format="%d"),
                            "當前市值": st.column_config.NumberColumn(format="$%.0f"),
                        },
                    )
                else:
                    st.caption("無變動或無前期資料")

    st.caption("⚠ 13F 申報延遲約 45 天，僅揭露多頭美股部位（不含放空、選擇權細節、海外資產）"
               " | 資料來源：SEC EDGAR sec.gov")
