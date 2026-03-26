"""
美國國會議員交易追蹤器
資料來源：House Clerk 官方 PTR（Periodic Transaction Reports）
無需 API key，完全免費
"""

import io
import os
import re
import json
import zipfile
import requests
import pdfplumber
from datetime import datetime, timedelta

# ── 你的持倉標的（請自行修改） ────────────────────────────────────
PORTFOLIO_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
    "META", "TSLA", "AMD", "INTC", "AVGO",
    "JPM", "BAC", "GS", "MS", "WFC",
    "XOM", "CVX", "COP", "SLB",
    "LMT", "RTX", "NOC", "BA",
    "SPY", "QQQ", "IWM",
]

# ── 板塊分類 ──────────────────────────────────────────────────────
SECTOR_MAP = {
    "AAPL":"科技","MSFT":"科技","NVDA":"科技","GOOGL":"科技",
    "AMZN":"科技","META":"科技","AMD":"科技","INTC":"科技","AVGO":"科技",
    "TSLA":"汽車",
    "JPM":"金融","BAC":"金融","GS":"金融","MS":"金融","WFC":"金融",
    "XOM":"能源","CVX":"能源","COP":"能源","SLB":"能源",
    "LMT":"國防","RTX":"國防","NOC":"國防","BA":"國防",
    "SPY":"ETF","QQQ":"ETF","IWM":"ETF",
}

BASE  = "https://disclosures-clerk.house.gov/public_disc"
SESS  = requests.Session()
SESS.headers.update({"User-Agent": "Mozilla/5.0"})


# ── Step 1：抓 PTR 申報索引 ───────────────────────────────────────
def fetch_ptr_index(year: int) -> list[dict]:
    """下載當年 FD.zip，回傳 PTR（FilingType='P'）申報清單"""
    import xml.etree.ElementTree as ET

    url  = f"{BASE}/financial-pdfs/{year}FD.zip"
    resp = SESS.get(url, timeout=30)
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


def parse_filing_date(s: str) -> datetime | None:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


# ── Step 2：解析單份 PTR PDF ──────────────────────────────────────
def parse_ptr_pdf(pdf_bytes: bytes) -> list[dict]:
    """從 PDF 位元組提取所有股票交易列"""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        lines = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())

    # 先把「金額跨行」接合：行末 $X,XXX - \n[??] $Y,YYY → $X,XXX - $Y,YYY
    joined = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.search(r"\$[\d,]+ -\s*$", line) and i + 1 < len(lines):
            nxt = lines[i + 1]
            tail = re.sub(r"^\[[A-Z]{2}\]\s*", "", nxt).strip()
            line = line.rstrip() + " " + tail
            i += 2
        else:
            i += 1
        joined.append(line)

    trades = []
    # 交易行模式：含 (TICKER) [ST/OP/GS/...] 或無 bracket，緊跟 S/P、兩個日期、金額
    pat = re.compile(
        r"\(([A-Z]{1,5})\)"           # (TICKER)
        r".*?"
        r"\b([SP])\b"                  # S=Sale / P=Purchase
        r"\s+(\d{2}/\d{2}/\d{4})"     # tx date
        r"\s+(\d{2}/\d{2}/\d{4})"     # disclosure date
        r"\s+(\$[\d,]+(?:\s*-\s*\$[\d,]+)?)"  # amount
    )
    for line in joined:
        m = pat.search(line)
        if m:
            ticker, tx_type, tx_date, disc_date, amount = m.groups()
            if len(ticker) > 5:          # 排除 CUSIP
                continue
            trades.append({
                "ticker":         ticker.upper(),
                "type":           "Purchase" if tx_type == "P" else "Sale",
                "txDate":         tx_date,
                "disclosureDate": disc_date,
                "amount":         amount.strip(),
            })
    return trades


# ── Step 3：組合完整流程 ──────────────────────────────────────────
def fetch_trades(days: int = 30) -> list[dict]:
    year  = datetime.now().year
    since = datetime.now() - timedelta(days=days)

    print(f"正在下載 {year} 年 PTR 索引...")
    try:
        all_ptrs = fetch_ptr_index(year)
    except Exception as e:
        print(f"  ⚠ 索引下載失敗：{e}")
        return []

    # 篩選申報日期在近 N 天內
    recent = [
        p for p in all_ptrs
        if (d := parse_filing_date(p["filingDate"])) and d >= since
    ]
    print(f"  共 {len(all_ptrs)} 份 PTR，近 {days} 天有 {len(recent)} 份需解析")

    trades = []
    for idx, ptr in enumerate(recent, 1):
        doc_id = ptr["docId"]
        url    = f"{BASE}/ptr-pdfs/{ptr['year']}/{doc_id}.pdf"
        print(f"  [{idx}/{len(recent)}] 解析 {ptr['name']} ({ptr['filingDate']}) ...", end="\r")
        try:
            resp = SESS.get(url, timeout=30)
            resp.raise_for_status()
            pdf_trades = parse_ptr_pdf(resp.content)
        except Exception as e:
            print(f"\n  ⚠ {ptr['name']} ({doc_id}) 失敗：{e}")
            continue

        for t in pdf_trades:
            trades.append({
                "politician":     ptr["name"],
                "party":          "",          # House Clerk 不含黨籍
                "chamber":        "House",
                "state":          ptr["state"],
                "ticker":         t["ticker"],
                "type":           t["type"],
                "amount":         t["amount"],
                "txDate":         t["txDate"],
                "disclosureDate": t["disclosureDate"],
                "filingDate":     ptr["filingDate"],
                "sector":         SECTOR_MAP.get(t["ticker"], ""),
                "inPortfolio":    t["ticker"] in PORTFOLIO_TICKERS,
            })

    print(f"\n  解析完畢，共取得 {len(trades)} 筆交易")
    return trades


# ── 輸出：統計 ────────────────────────────────────────────────────
def print_stats(trades: list[dict], days: int) -> None:
    hits  = [t for t in trades if t["inPortfolio"]]
    buys  = [t for t in trades if "purchase" in t["type"].lower()]
    sells = [t for t in trades if "sale" in t["type"].lower()]

    print("\n" + "═" * 60)
    print(f"  統計摘要（近 {days} 天，僅眾議院）")
    print("═" * 60)
    print(f"  總交易筆數：{len(trades)}  │  買入：{len(buys)}  │  賣出：{len(sells)}")

    if hits:
        hit_tickers = sorted(set(t["ticker"] for t in hits))
        print(f"\n  ⚡ 命中你的持倉（{len(hits)} 筆）：{', '.join(hit_tickers)}")
    else:
        print("\n  ✓ 近期無持倉標的被交易")
    print("═" * 60)


# ── 輸出：明細表格 ────────────────────────────────────────────────
def print_table(
    trades: list[dict],
    search: str = "",
    match_filter: str = "all",
    type_filter: str = "all",
    limit: int = 50,
) -> None:
    filtered = trades

    if search:
        kw = search.lower()
        filtered = [t for t in filtered if kw in t["politician"].lower() or kw in t["ticker"].lower()]

    if match_filter == "portfolio":
        filtered = [t for t in filtered if t["inPortfolio"]]
    elif match_filter == "nonportfolio":
        filtered = [t for t in filtered if not t["inPortfolio"]]

    if type_filter == "buy":
        filtered = [t for t in filtered if "purchase" in t["type"].lower()]
    elif type_filter == "sell":
        filtered = [t for t in filtered if "sale" in t["type"].lower()]

    if not filtered:
        print("\n  無符合條件的交易\n")
        return

    # 持倉命中優先，再按交易日倒序
    filtered.sort(key=lambda t: (not t["inPortfolio"], t["txDate"]), reverse=False)
    filtered.sort(key=lambda t: t["txDate"], reverse=True)

    header = f"  {'議員':<22} {'州':<5} {'標的':<7} {'操作':<10} {'金額範圍':<22} {'交易日':<12} {'揭露日':<12} 板塊"
    sep    = "  " + "─" * (len(header) - 2)

    print(f"\n  共 {len(filtered)} 筆（顯示前 {min(limit, len(filtered))} 筆）")
    print(sep)
    print(header)
    print(sep)

    for t in filtered[:limit]:
        marker = "●" if t["inPortfolio"] else " "
        print(
            f"  {marker} {t['politician']:<21} {t['state']:<5} "
            f"{t['ticker']:<7} {t['type'][:9]:<10} {t['amount']:<22} "
            f"{t['txDate']:<12} {t['disclosureDate']:<12} {t['sector']}"
        )

    print(sep)
    if len(filtered) > limit:
        print(f"  … 還有 {len(filtered) - limit} 筆（調高 --limit 可顯示更多）")

    print("\n  ⚠ 依法議員需在交易後 45 天內申報。● 代表你目前的持倉標的。")
    print("  資料來源：House Clerk 官方 PTR（僅眾議院，不含參議院）\n")


# ── 儲存 JSON ─────────────────────────────────────────────────────
def save_json(trades: list[dict], path: str = "congress_trades.json") -> None:
    path = os.path.join(os.path.dirname(__file__), path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(trades, f, ensure_ascii=False, indent=2)
    print(f"  已儲存至 {path}")


# ── 主程式 ────────────────────────────────────────────────────────
def main(
    days: int = 30,
    search: str = "",
    match_filter: str = "all",
    type_filter: str = "all",
    limit: int = 50,
    save: bool = False,
) -> None:
    trades = fetch_trades(days=days)
    if not trades:
        return

    print_stats(trades, days)
    print_table(trades, search=search, match_filter=match_filter,
                type_filter=type_filter, limit=limit)

    if save:
        save_json(trades)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="美國眾議員交易追蹤器（House Clerk 官方資料）")
    parser.add_argument("--days",   type=int, default=30,
                        choices=[7, 14, 30, 60, 90], help="掃描天數")
    parser.add_argument("--search", type=str, default="",
                        help="搜尋關鍵字（議員 / 標的）")
    parser.add_argument("--match",  type=str, default="all",
                        choices=["all", "portfolio", "nonportfolio"],
                        help="all=全部 | portfolio=僅持倉 | nonportfolio=非持倉")
    parser.add_argument("--type",   type=str, default="all",
                        choices=["all", "buy", "sell"], help="交易方向篩選")
    parser.add_argument("--limit",  type=int, default=50, help="顯示筆數上限")
    parser.add_argument("--save",   action="store_true", help="儲存 JSON 檔案")

    args = parser.parse_args()
    main(
        days=args.days,
        search=args.search,
        match_filter=args.match,
        type_filter=args.type,
        limit=args.limit,
        save=args.save,
    )
