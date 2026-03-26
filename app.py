"""
國會議員交易追蹤器 — Streamlit Web App
🇺🇸 美國眾議院 PTR  ×  🇹🇼 台灣立委財產申報
"""

import io
import re
import time
import zipfile
import requests
import pdfplumber
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta

# ── 頁面設定 ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="國會交易追蹤",
    page_icon="🏛",
    layout="wide",
    initial_sidebar_state="expanded",
)

SESS = requests.Session()
SESS.headers.update({"User-Agent": "Mozilla/5.0"})

# ════════════════════════════════════════════════════════════════
# ── 美國 US 設定 ─────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
PORTFOLIO_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
    "META", "TSLA", "AMD", "INTC", "AVGO",
    "JPM", "BAC", "GS", "MS", "WFC",
    "XOM", "CVX", "COP", "SLB",
    "LMT", "RTX", "NOC", "BA",
    "SPY", "QQQ", "IWM",
]
SECTOR_MAP = {
    "AAPL":"科技","MSFT":"科技","NVDA":"科技","GOOGL":"科技",
    "AMZN":"科技","META":"科技","AMD":"科技","INTC":"科技","AVGO":"科技",
    "TSLA":"汽車",
    "JPM":"金融","BAC":"金融","GS":"金融","MS":"金融","WFC":"金融",
    "XOM":"能源","CVX":"能源","COP":"能源","SLB":"能源",
    "LMT":"國防","RTX":"國防","NOC":"國防","BA":"國防",
    "SPY":"ETF","QQQ":"ETF","IWM":"ETF",
}
US_BASE = "https://disclosures-clerk.house.gov/public_disc"

# ════════════════════════════════════════════════════════════════
# ── 台灣 TW 設定 ─────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
TW_API = "https://priso.cy.gov.tw/api/Query"
TW_PAGE_TPL = {"PageNo": 1, "PageSize": 50, "TotalCount": 0, "OrderByNum": 0, "OrderBySort": ""}

# 台灣科技股板塊對照（可自行擴充）
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
    nums = [float(n.replace(",", "")) for n in re.findall(r"[\d,]+", s)]
    if len(nums) >= 2:
        return (nums[0] + nums[1]) / 2
    elif len(nums) == 1:
        return nums[0]
    return 0.0


def parse_ptr_pdf(pdf_bytes: bytes) -> list[dict]:
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
            trades.append({
                "ticker":         ticker,
                "type":           "Purchase" if tx_type == "P" else "Sale",
                "txDate":         tx_date,
                "disclosureDate": disc_date,
                "amount":         amount.strip(),
            })
    return trades


@st.cache_data(ttl=3600, show_spinner=False)
def load_us_trades(days: int) -> pd.DataFrame:
    year  = datetime.now().year
    since = datetime.now() - timedelta(days=days)
    all_ptrs = fetch_ptr_index(year)
    recent   = [p for p in all_ptrs
                if (d := parse_filing_date(p["filingDate"])) and d >= since]
    rows = []
    prog = st.progress(0, text="正在解析 PTR 申報...")
    for idx, ptr in enumerate(recent):
        prog.progress((idx + 1) / max(len(recent), 1),
                      text=f"解析 {ptr['name']} ({idx+1}/{len(recent)})")
        try:
            resp = SESS.get(f"{US_BASE}/ptr-pdfs/{ptr['year']}/{ptr['docId']}.pdf", timeout=30)
            resp.raise_for_status()
            for t in parse_ptr_pdf(resp.content):
                rows.append({
                    "議員":   ptr["name"],
                    "州":    ptr["state"],
                    "院":    "眾議院",
                    "標的":  t["ticker"],
                    "操作":  t["type"],
                    "金額":  t["amount"],
                    "交易日": t["txDate"],
                    "揭露日": t["disclosureDate"],
                    "申報日": ptr["filingDate"],
                    "板塊":  SECTOR_MAP.get(t["ticker"], "其他"),
                    "持倉":  t["ticker"] in PORTFOLIO_TICKERS,
                })
        except Exception:
            pass
    prog.empty()
    df = pd.DataFrame(rows)
    if not df.empty:
        df["交易日_dt"] = pd.to_datetime(df["交易日"], format="%m/%d/%Y", errors="coerce")
        df["金額_數值"] = df["金額"].apply(parse_amount)
        df = df.sort_values("交易日_dt", ascending=False)
    return df


# ════════════════════════════════════════════════════════════════
# ── 台灣 TW 資料函數 ──────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
def fetch_tw_period() -> str:
    """Get the most recent 廉政專刊 period number."""
    try:
        page = dict(TW_PAGE_TPL)
        page["PageSize"] = 1
        r = SESS.post(f"{TW_API}/QueryData",
                      json={"Data": {"Method": "", "Type": "04", "Value": "立法委員"}, "Page": page},
                      timeout=15)
        data = r.json()
        return data["Data"]["Data"][0]["Period"]
    except Exception:
        return "299"


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
                    # Enter stock sub-section
                    if re.search(r'1\s*[.。]\s*股票', s):
                        in_stock = True
                        continue
                    # Exit when hitting next sub-section or new major section
                    if in_stock and (re.search(r'^2\s*[.。]', s) or
                                     re.search(r'^（[九十百]）', s) or
                                     re.search(r'本欄空白', s)):
                        in_stock = False
                        continue
                    if not in_stock or not s:
                        continue
                    # Skip header rows
                    if re.search(r'名\s*稱|所\s*有\s*人|股\s*數|票\s*面|外\s*幣|總\s*額', s):
                        continue
                    # Parse data row: "公司名 所有人 股數 票面額 [外幣] 總額"
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


@st.cache_data(ttl=86400, show_spinner=False)
def load_tw_holdings(period: str) -> pd.DataFrame:
    """Download & parse stock holdings for all 立法委員 in the given 廉政專刊 period."""
    # ── Step 1: collect filing list ───────────────────────────
    filings = []
    page_no = 1
    while True:
        page = dict(TW_PAGE_TPL)
        page["PageNo"] = page_no
        try:
            r = SESS.post(f"{TW_API}/QueryData",
                          json={"Data": {"Method": "", "Type": "04", "Value": "立法委員"},
                                "Page": page},
                          timeout=30)
            data = r.json()
            if not data.get("Success"):
                break
            records = data["Data"]["Data"]
            if not records:
                break
            for rec in records:
                if rec["Period"] == period and "01" in rec["PublishType"]:
                    filings.append(rec)
            # Stop once records drift past the target period
            if records[-1]["Period"] < period:
                break
            page_no += 1
            if page_no > 20:
                break
        except Exception:
            break

    # ── Step 2: download & parse each PDF ─────────────────────
    rows = []
    prog = st.progress(0, text="正在解析立委財產申報 PDF…")
    for idx, filing in enumerate(filings):
        prog.progress((idx + 1) / max(len(filings), 1),
                      text=f"解析 {filing['Name']} ({idx+1}/{len(filings)})")
        try:
            pdf_r = SESS.post(f"{TW_API}/getFile",
                              json={"From": "base", "FileId": filing["Id"]},
                              timeout=60)
            stocks = parse_tw_stocks(pdf_r.content)
            for stk in stocks:
                rows.append({
                    "立委":   filing["Name"],
                    "申報日": filing["PublishDate"],
                    "公司":   stk["company"],
                    "持有人": stk["owner"],
                    "股數":   stk["shares"],
                    "票面額": stk["face_value"],
                    "申報總額": stk["total_twd"],
                    "板塊":   TW_SECTOR_MAP.get(stk["company"], "其他"),
                    "是否本人": stk["owner"] == filing["Name"],
                })
            time.sleep(0.3)   # polite rate-limit
        except Exception:
            pass
    prog.empty()
    df = pd.DataFrame(rows)
    return df


# ════════════════════════════════════════════════════════════════
# ── 側邊欄 ───────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("🏛 國會交易追蹤")

    # 國家切換
    country = st.radio("資料來源", ["🇺🇸 美國眾議院", "🇹🇼 台灣立委"], horizontal=True)

    if country == "🇺🇸 美國眾議院":
        st.caption("資料來源：House Clerk 官方 PTR")
        days = st.selectbox("掃描天數", [7, 14, 30, 60, 90], index=2)
        if st.button("🔍 立即掃描", type="primary", use_container_width=True):
            st.cache_data.clear()
        st.divider()
        match_opt = st.radio("標的篩選", ["全部", "僅持倉標的", "非持倉標的"])
        type_opt  = st.radio("交易方向", ["全部", "只看買入", "只看賣出"])
        search    = st.text_input("搜尋議員 / 標的")
        st.divider()
        st.caption("你的持倉標的")
        st.caption(", ".join(PORTFOLIO_TICKERS))
    else:
        st.caption("資料來源：監察院財產申報公示系統")
        tw_period = st.text_input("廉政專刊期別（留空=最新）", value="", placeholder="如 299")
        if st.button("🔍 重新載入", type="primary", use_container_width=True):
            st.cache_data.clear()
        st.divider()
        tw_search = st.text_input("搜尋立委 / 公司")
        tw_owner  = st.radio("持有人", ["全部", "僅本人", "配偶/子女"])


# ════════════════════════════════════════════════════════════════
# ── 美國眾議院 頁面 ───────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
if country == "🇺🇸 美國眾議院":
    with st.spinner("載入資料中…"):
        df = load_us_trades(days)

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
        colors_buy  = ["#cc9900" if t in PORTFOLIO_TICKERS else "#2a6fb5" for t in all_tks]
        colors_sell = ["#cc4400" if t in PORTFOLIO_TICKERS else "#8b2222" for t in all_tks]
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
        # Build sunburst data: sector → ticker
        sun_parents, sun_labels, sun_values = [], [], []
        if us_mode == "項目統計":
            root_val = len(df)
            sec_agg  = df.groupby("板塊").size()
            tk_agg   = df.groupby(["板塊", "標的"]).size()
            unit_lbl = lambda v: f"({v:.0f})"
        else:
            root_val = df["金額_數值"].sum() if "金額_數值" in df.columns else len(df)
            sec_agg  = df.groupby("板塊")["金額_數值"].sum()
            tk_agg   = df.groupby(["板塊", "標的"])["金額_數值"].sum()
            unit_lbl = lambda v: f"(${v/1e6:.1f}M)"
        sun_parents += ["",     "全部"]
        sun_labels  += ["全部", "（其他）"]
        sun_values  += [root_val, 0]
        for sector, s_val in sec_agg.items():
            sun_parents.append("全部");  sun_labels.append(sector);  sun_values.append(s_val)
            for (sec2, ticker), t_val in tk_agg.items():
                if sec2 != sector:
                    continue
                sun_parents.append(sector)
                sun_labels.append(f"{ticker}\n{unit_lbl(t_val)}")
                sun_values.append(t_val)
        fig_sec = go.Figure(go.Sunburst(
            labels=sun_labels, parents=sun_parents, values=sun_values,
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
        pol_ct = df["議員"].value_counts().head(10)
        fig_p = px.bar(x=pol_ct.values, y=pol_ct.index, orientation="h",
                       height=300, color_discrete_sequence=["#4a9eff"])
        fig_p.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                             paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             xaxis_title="", yaxis_title="", font=dict(color="#aaa"))
        st.plotly_chart(fig_p, use_container_width=True)

    # 標的買賣的量
    st.subheader("標的買賣的量（Top 20，估算中位金額）")
    if "金額_數值" in df.columns:
        vol_buy  = df[df["操作"]=="Purchase"].groupby("標的")["金額_數值"].sum()
        vol_sell = df[df["操作"]=="Sale"].groupby("標的")["金額_數值"].sum()
        all_vol_tks = (vol_buy.add(vol_sell, fill_value=0)
                       .sort_values(ascending=False).head(20).index.tolist())
        fig_vol = go.Figure()
        fig_vol.add_bar(name="買入", x=all_vol_tks,
                        y=[vol_buy.get(t, 0)/1e6 for t in all_vol_tks],
                        marker_color=["#cc9900" if t in PORTFOLIO_TICKERS else "#2a6fb5" for t in all_vol_tks])
        fig_vol.add_bar(name="賣出", x=all_vol_tks,
                        y=[vol_sell.get(t, 0)/1e6 for t in all_vol_tks],
                        marker_color=["#cc4400" if t in PORTFOLIO_TICKERS else "#8b2222" for t in all_vol_tks])
        fig_vol.update_layout(barmode="stack", height=320,
                              margin=dict(t=10,b=10,l=10,r=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              legend=dict(orientation="h"), yaxis_title="金額（百萬美元）",
                              font=dict(color="#aaa"))
        fig_vol.update_xaxes(tickangle=45)
        st.plotly_chart(fig_vol, use_container_width=True)

    st.divider()

    # 明細表
    st.subheader(f"明細（{len(dff)} 筆）")
    display = dff[["議員","州","標的","操作","金額","交易日","揭露日","板塊","持倉"]].copy()
    display["操作"] = display["操作"].map({"Purchase":"買入 🔵","Sale":"賣出 🔴"})
    display["持倉"] = display["持倉"].map({True:"⭐","":""}).fillna("")
    st.dataframe(display, use_container_width=True, height=480,
                 column_config={
                     "持倉": st.column_config.TextColumn("持倉", width=50),
                     "操作": st.column_config.TextColumn("操作", width=80),
                     "金額": st.column_config.TextColumn("金額", width=160),
                 })
    st.caption("⚠ 依法議員需在交易後 45 天內申報。⭐ 代表你目前的持倉標的。"
               " | 資料來源：House Clerk 官方 PTR（僅眾議院）")


# ════════════════════════════════════════════════════════════════
# ── 台灣立委 頁面 ─────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════
else:
    # 決定期別
    with st.spinner("取得最新期別…"):
        period = tw_period.strip() if tw_period.strip() else fetch_tw_period()
    st.info(f"📋 廉政專刊第 **{period}** 期　｜　資料來源：監察院財產申報公示系統", icon="🇹🇼")

    with st.spinner(f"載入第 {period} 期立委持股資料（首次需數分鐘）…"):
        tw_df = load_tw_holdings(period)

    if tw_df.empty:
        st.warning("查無股票申報資料，可能該期別尚未有資料或解析失敗。")
        st.stop()

    # 篩選
    dff_tw = tw_df.copy()
    if tw_search:
        kw = tw_search.lower()
        dff_tw = dff_tw[dff_tw["立委"].str.lower().str.contains(kw) |
                        dff_tw["公司"].str.lower().str.contains(kw)]
    if tw_owner == "僅本人":
        dff_tw = dff_tw[dff_tw["是否本人"]]
    elif tw_owner == "配偶/子女":
        dff_tw = dff_tw[~dff_tw["是否本人"]]

    # 統計卡
    total_legislators = tw_df["立委"].nunique()
    legislators_with_stocks = tw_df[tw_df["股數"] > 0]["立委"].nunique()
    total_companies = tw_df["公司"].nunique()
    total_shares    = tw_df["股數"].sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("申報立委人數", total_legislators)
    c2.metric("持有股票人數", legislators_with_stocks)
    c3.metric("持股公司種類", total_companies)
    c4.metric("持股總股數", f"{total_shares:,.0f}")

    st.divider()

    # 圖表 Row 1
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("立委持股數排行（Top 20）")
        leg_shares = (tw_df.groupby("立委")["股數"].sum()
                      .sort_values(ascending=False).head(20))
        fig_leg = px.bar(x=leg_shares.values / 1000, y=leg_shares.index,
                         orientation="h", height=400,
                         color_discrete_sequence=["#4a9eff"])
        fig_leg.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                               paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               xaxis_title="持股數（千股）", yaxis_title="",
                               font=dict(color="#aaa"))
        st.plotly_chart(fig_leg, use_container_width=True)

    with col_r:
        st.subheader("板塊分佈（點選板塊可下鑽）")
        tw_mode = st.radio("統計方式", ["項目統計", "數量統計（股數）"],
                           horizontal=True, key="tw_sun_mode")
        # Build sunburst: sector → company
        tw_sun_p, tw_sun_l, tw_sun_v = [], [], []
        if tw_mode == "項目統計":
            tw_root_val = len(tw_df)
            tw_sec_agg  = tw_df.groupby("板塊").size()
            tw_co_agg   = tw_df.groupby(["板塊", "公司"]).size()
            tw_unit     = lambda v: f"({v:.0f})"
        else:
            tw_root_val = tw_df["股數"].sum()
            tw_sec_agg  = tw_df.groupby("板塊")["股數"].sum()
            tw_co_agg   = tw_df.groupby(["板塊", "公司"])["股數"].sum()
            tw_unit     = lambda v: f"({v/1000:.0f}K股)"
        tw_sun_p += ["",     "全部"]
        tw_sun_l += ["全部", "（其他）"]
        tw_sun_v += [tw_root_val, 0]
        for sector, s_val in tw_sec_agg.items():
            tw_sun_p.append("全部");  tw_sun_l.append(sector);  tw_sun_v.append(s_val)
            for (sec2, co), c_val in tw_co_agg.items():
                if sec2 != sector:
                    continue
                tw_sun_p.append(sector)
                tw_sun_l.append(f"{co}\n{tw_unit(c_val)}")
                tw_sun_v.append(c_val)
        fig_tw_sec = go.Figure(go.Sunburst(
            labels=tw_sun_l, parents=tw_sun_p, values=tw_sun_v,
            branchvalues="total", insidetextorientation="radial",
            marker=dict(colors=px.colors.qualitative.Set3 * 10),
        ))
        fig_tw_sec.update_layout(height=360, margin=dict(t=10,b=10,l=10,r=10),
                                  paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#aaa"))
        st.plotly_chart(fig_tw_sec, use_container_width=True)

    # 圖表 Row 2
    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader("熱門持股（被最多立委持有）")
        co_cnt = (tw_df.groupby("公司")["立委"].nunique()
                  .sort_values(ascending=False).head(20))
        fig_co = px.bar(x=co_cnt.index, y=co_cnt.values,
                        height=320, color_discrete_sequence=["#f0a500"])
        fig_co.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              xaxis_title="", yaxis_title="持有立委數",
                              font=dict(color="#aaa"))
        fig_co.update_xaxes(tickangle=45)
        st.plotly_chart(fig_co, use_container_width=True)

    with col_r2:
        st.subheader("本人 vs 配偶/子女持股（股數）")
        owner_grp = tw_df.groupby("是否本人")["股數"].sum()
        labels = {True: "本人", False: "配偶/子女"}
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
    disp_tw = dff_tw[["立委","公司","持有人","股數","票面額","申報總額","板塊","申報日"]].copy()
    disp_tw["是否本人"] = dff_tw["是否本人"].map({True:"本人 ✅", False:"配偶/子女"})
    st.dataframe(disp_tw, use_container_width=True, height=480,
                 column_config={
                     "股數":   st.column_config.NumberColumn("股數", format="%d"),
                     "票面額": st.column_config.NumberColumn("票面額(元)", format="%.0f"),
                     "申報總額": st.column_config.NumberColumn("申報總額(元)", format="%.0f"),
                 })
    st.caption("⚠ 資料為年度財產申報，非即時交易。"
               " | 資料來源：監察院財產申報公示系統 priso.cy.gov.tw")
