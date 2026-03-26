"""
國會議員交易追蹤器 — Streamlit Web App
部署於 Streamlit Community Cloud，任何裝置皆可查看
"""

import io
import re
import zipfile
import requests
import pdfplumber
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta
from collections import Counter

# ── 頁面設定 ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="國會交易追蹤",
    page_icon="🏛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 你的持倉標的 ──────────────────────────────────────────────────
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

BASE = "https://disclosures-clerk.house.gov/public_disc"
SESS = requests.Session()
SESS.headers.update({"User-Agent": "Mozilla/5.0"})


# ── 資料抓取（含快取） ────────────────────────────────────────────
def fetch_ptr_index(year: int) -> list[dict]:
    import xml.etree.ElementTree as ET
    resp = SESS.get(f"{BASE}/financial-pdfs/{year}FD.zip", timeout=30)
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
def load_trades(days: int) -> pd.DataFrame:
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
            resp = SESS.get(f"{BASE}/ptr-pdfs/{ptr['year']}/{ptr['docId']}.pdf",
                            timeout=30)
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
        df = df.sort_values("交易日_dt", ascending=False)
    return df


# ── 側邊欄：篩選控制 ─────────────────────────────────────────────
with st.sidebar:
    st.title("🏛 國會交易追蹤")
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


# ── 載入資料 ──────────────────────────────────────────────────────
with st.spinner("載入資料中..."):
    df = load_trades(days)

if df.empty:
    st.warning("無資料，請點擊「立即掃描」")
    st.stop()

# ── 套用篩選 ──────────────────────────────────────────────────────
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

# ── 統計卡 ────────────────────────────────────────────────────────
hits = df[df["持倉"]]
buys = df[df["操作"] == "Purchase"]
sells= df[df["操作"] == "Sale"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("總交易筆數", len(df))
c2.metric("買入", len(buys))
c3.metric("賣出", len(sells))
c4.metric("命中持倉", len(hits))

# ── 持倉命中警示 ─────────────────────────────────────────────────
if not hits.empty:
    hit_tickers = sorted(hits["標的"].unique())
    st.error(f"⚡ 命中你的持倉：**{', '.join(hit_tickers)}** ({len(hits)} 筆)")

st.divider()

# ── 圖表區 ────────────────────────────────────────────────────────
col_l, col_r = st.columns(2)

# 標的買賣次數
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
                   y=[tk_buy.get(t, 0) for t in all_tks],
                   marker_color=colors_buy)
    fig_tk.add_bar(name="賣出", x=all_tks,
                   y=[tk_sell.get(t, 0) for t in all_tks],
                   marker_color=colors_sell)
    fig_tk.update_layout(barmode="stack", height=320,
                         margin=dict(t=10,b=10,l=10,r=10),
                         paper_bgcolor="rgba(0,0,0,0)",
                         plot_bgcolor="rgba(0,0,0,0)",
                         legend=dict(orientation="h"),
                         font=dict(color="#aaa"))
    fig_tk.update_xaxes(tickangle=45)
    st.plotly_chart(fig_tk, use_container_width=True)

# 板塊分佈
with col_r:
    st.subheader("板塊分佈")
    sector_ct = df["板塊"].value_counts()
    fig_sec = px.pie(values=sector_ct.values, names=sector_ct.index,
                     hole=0.4, height=320,
                     color_discrete_sequence=px.colors.qualitative.Set2)
    fig_sec.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                          paper_bgcolor="rgba(0,0,0,0)",
                          font=dict(color="#aaa"),
                          legend=dict(orientation="h", y=-0.1))
    st.plotly_chart(fig_sec, use_container_width=True)

col_l2, col_r2 = st.columns(2)

# 每日交易量
with col_l2:
    st.subheader("每日交易量")
    daily = df.groupby(["交易日_dt", "操作"]).size().unstack(fill_value=0)
    daily = daily.sort_index()
    fig_t = go.Figure()
    if "Purchase" in daily.columns:
        fig_t.add_bar(name="買入", x=daily.index,
                      y=daily["Purchase"], marker_color="#2a6fb5")
    if "Sale" in daily.columns:
        fig_t.add_bar(name="賣出", x=daily.index,
                      y=daily["Sale"], marker_color="#8b2222")
    fig_t.update_layout(barmode="stack", height=300,
                        margin=dict(t=10,b=10,l=10,r=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h"),
                        font=dict(color="#aaa"))
    st.plotly_chart(fig_t, use_container_width=True)

# 最活躍議員
with col_r2:
    st.subheader("最活躍議員（Top 10）")
    pol_ct = df["議員"].value_counts().head(10)
    fig_p = px.bar(x=pol_ct.values, y=pol_ct.index, orientation="h",
                   height=300, color_discrete_sequence=["#4a9eff"])
    fig_p.update_layout(margin=dict(t=10,b=10,l=10,r=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        xaxis_title="", yaxis_title="",
                        font=dict(color="#aaa"))
    st.plotly_chart(fig_p, use_container_width=True)

st.divider()

# ── 明細表格 ──────────────────────────────────────────────────────
st.subheader(f"明細（{len(dff)} 筆）")

display = dff[["議員","州","標的","操作","金額","交易日","揭露日","板塊","持倉"]].copy()
display["操作"] = display["操作"].map({"Purchase":"買入 🔵","Sale":"賣出 🔴"})
display["持倉"] = display["持倉"].map({True:"⭐","":""}).fillna("")

st.dataframe(
    display,
    use_container_width=True,
    height=480,
    column_config={
        "持倉": st.column_config.TextColumn("持倉", width=50),
        "操作": st.column_config.TextColumn("操作", width=80),
        "金額": st.column_config.TextColumn("金額", width=160),
    }
)

st.caption("⚠ 依法議員需在交易後 45 天內申報。⭐ 代表你目前的持倉標的。"
           " | 資料來源：House Clerk 官方 PTR（僅眾議院）")
