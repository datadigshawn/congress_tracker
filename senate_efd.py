"""美國參議院 eFD PTR 資料抓取模組。

從官方 Senate eFD (https://efdsearch.senate.gov/search/) 取得完整的
Periodic Transaction Report (PTR) 資料。流程：
1. GET /search/home/ 取得 csrftoken
2. POST /search/home/ 接受 prohibition_agreement 以授權 session
3. POST /search/report/data/ 分頁取得 PTR 清單
4. GET 每筆 report 詳細頁，解析交易表格
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from typing import Callable, Optional

from curl_cffi import requests
from bs4 import BeautifulSoup

BASE_URL     = "https://efdsearch.senate.gov"
HOME_URL     = f"{BASE_URL}/search/home/"
SEARCH_URL   = f"{BASE_URL}/search/home/"
REPORT_URL   = f"{BASE_URL}/search/report/data/"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# 金額對照：Senate eFD 的範圍字串 → 保留原樣（與 CapitolTrades 風格大致一致）
AMOUNT_PASS_THROUGH = True


def _new_session():
    """建立可繞過 Akamai 阻擋的 session（透過 curl_cffi 模擬 Chrome 指紋）。"""
    s = requests.Session(impersonate="chrome124")
    return s


def _get_csrf(sess: requests.Session) -> str:
    token = sess.cookies.get("csrftoken")
    if not token:
        raise RuntimeError("未取得 csrftoken cookie")
    return token


def _accept_agreement(sess: requests.Session) -> None:
    """接受 prohibition_agreement，授權 session。"""
    r = sess.get(HOME_URL, timeout=30)
    r.raise_for_status()
    csrf = _get_csrf(sess)
    r2 = sess.post(
        SEARCH_URL,
        data={
            "csrfmiddlewaretoken": csrf,
            "prohibition_agreement": "1",
        },
        headers={"Referer": HOME_URL},
        timeout=30,
    )
    r2.raise_for_status()


def _parse_office_state(office: str) -> str:
    """從 office 欄位（如 'Senator, MI'）擷取州代號。"""
    if not office:
        return ""
    m = re.search(r"\b([A-Z]{2})\b\s*$", office.strip())
    return m.group(1) if m else ""


def _fetch_report_list(
    sess: requests.Session,
    start_date: str,
    end_date: str,
    start: int,
    length: int,
) -> dict:
    csrf = _get_csrf(sess)
    payload = {
        "csrfmiddlewaretoken": csrf,
        "report_types": "[11]",
        "filer_types": "[]",
        "submitted_start_date": start_date,
        "submitted_end_date": end_date,
        "candidate_state": "",
        "senator_state": "",
        "office_id": "",
        "first_name": "",
        "last_name": "",
        "start": str(start),
        "length": str(length),
    }
    headers = {
        "Referer": f"{BASE_URL}/search/",
        "X-CSRFToken": csrf,
        "X-Requested-With": "XMLHttpRequest",
    }
    r = sess.post(REPORT_URL, data=payload, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


_AMOUNT_CLEAN_RE = re.compile(r"\s+")


def _clean_text(s: str) -> str:
    return _AMOUNT_CLEAN_RE.sub(" ", (s or "").replace("\xa0", " ")).strip()


def _parse_detail_page(html: str) -> list[dict]:
    """解析 PTR 詳細頁面，回傳原始交易清單。"""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    rows: list[dict] = []
    headers = [
        _clean_text(th.get_text()) for th in table.select("thead th")
    ]
    # 欄位：# | Transaction Date | Owner | Ticker | Asset Name | Asset Type |
    #       Type | Amount | Comment
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue
        # 依欄位數量靈活對齊（有的頁面沒有 #）
        offset = 0 if len(tds) < 9 else 1
        try:
            tx_date    = _clean_text(tds[offset + 0].get_text())
            owner      = _clean_text(tds[offset + 1].get_text())
            ticker_td  = tds[offset + 2]
            asset_name = _clean_text(tds[offset + 3].get_text())
            asset_type = _clean_text(tds[offset + 4].get_text())
            tx_type    = _clean_text(tds[offset + 5].get_text())
            amount     = _clean_text(tds[offset + 6].get_text())
        except IndexError:
            continue

        # ticker 通常是 <a>AAPL</a> 或 "--"
        ticker = _clean_text(ticker_td.get_text())
        if ticker in ("--", "", "N/A"):
            continue
        # 只保留像股票代號的字元（避免 "AAPL <br> Apple" 等雜訊）
        ticker = ticker.split()[0].strip().upper()
        if not re.fullmatch(r"[A-Z.\-]{1,6}", ticker):
            continue

        # 僅保留股票 / ETF（排除債券、選擇權等較難對應的標的？保留所有含 ticker 的）
        # 規範化交易類型
        t_low = tx_type.lower()
        if "purchase" in t_low:
            op = "Purchase"
        elif "sale" in t_low and "partial" in t_low:
            op = "Sale (Partial)"
        elif "sale" in t_low:
            op = "Sale"
        elif "exchange" in t_low:
            op = "Exchange"
        else:
            op = tx_type or "Unknown"

        # 日期格式化 MM/DD/YYYY
        tx_date_fmt = tx_date
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                tx_date_fmt = datetime.strptime(tx_date, fmt).strftime("%m/%d/%Y")
                break
            except ValueError:
                continue

        rows.append({
            "ticker":    ticker,
            "type":      op,
            "amount":    amount,
            "txDate":    tx_date_fmt,
            "assetType": asset_type,
            "owner":     owner,
            "assetName": asset_name,
        })
    return rows


def fetch_senate_ptrs(
    days: int,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> list[dict]:
    """從 Senate eFD 抓取過去 `days` 天內申報的 PTR 交易。

    回傳欄位與 load_senate_trades 原 schema 一致：
      {name, state, ticker, type, amount, txDate, filedDate}
    """
    sess = _new_session()
    _accept_agreement(sess)

    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    # Senate eFD 使用 MM/DD/YYYY HH:MM:SS（申報提交日期範圍）
    start_date = start_dt.strftime("%m/%d/%Y 00:00:00")
    end_date   = end_dt.strftime("%m/%d/%Y 23:59:59")

    # 先抓第一頁取得總筆數
    length = 100
    first = _fetch_report_list(sess, start_date, end_date, 0, length)
    total = int(first.get("recordsTotal") or first.get("recordsFiltered") or 0)
    all_reports: list[list] = list(first.get("data", []))

    start = length
    while start < total:
        if progress_cb:
            progress_cb(start, total, f"載入參議院 PTR 清單 {start}/{total}")
        try:
            page = _fetch_report_list(sess, start_date, end_date, start, length)
        except Exception:
            time.sleep(1.0)
            try:
                _accept_agreement(sess)
                page = _fetch_report_list(sess, start_date, end_date, start, length)
            except Exception:
                break
        data = page.get("data", [])
        if not data:
            break
        all_reports.extend(data)
        start += length
        time.sleep(0.3)

    # 解析每一筆 report 摘要列
    report_meta: list[dict] = []
    skipped_paper = 0
    for row in all_reports:
        # row: [first_html, last_html, office, report_link_html, date_str]
        if len(row) < 5:
            continue
        first_name = _clean_text(BeautifulSoup(row[0], "lxml").get_text())
        last_name  = _clean_text(BeautifulSoup(row[1], "lxml").get_text())
        office     = _clean_text(row[2] or "")
        link_html  = row[3] or ""
        filed_str  = _clean_text(row[4] or "")

        link_soup = BeautifulSoup(link_html, "lxml")
        a = link_soup.find("a")
        if not a or not a.get("href"):
            continue
        href = a["href"]
        if "/paper/" in href:
            skipped_paper += 1
            continue
        if not href.startswith("http"):
            href = BASE_URL + href

        # filed_str 可能是 "MM/DD/YYYY" 或含時間
        filed_fmt = filed_str
        for fmt in ("%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d"):
            try:
                filed_fmt = datetime.strptime(filed_str, fmt).strftime("%m/%d/%Y")
                break
            except ValueError:
                continue

        report_meta.append({
            "name":  f"{first_name} {last_name}".strip(),
            "state": _parse_office_state(office),
            "url":   href,
            "filed": filed_fmt,
        })

    # 逐筆抓取詳細頁
    results: list[dict] = []
    total_reports = len(report_meta)
    for idx, meta in enumerate(report_meta, 1):
        if progress_cb:
            progress_cb(idx, total_reports,
                        f"載入參議院 PTR 詳細 {idx}/{total_reports}")
        try:
            r = sess.get(meta["url"], timeout=30,
                         headers={"Referer": f"{BASE_URL}/search/"})
            if r.status_code != 200:
                continue
            txs = _parse_detail_page(r.text)
        except Exception:
            continue
        for t in txs:
            results.append({
                "name":      meta["name"],
                "state":     meta["state"],
                "ticker":    t["ticker"],
                "type":      t["type"],
                "amount":    t["amount"],
                "txDate":    t["txDate"],
                "filedDate": meta["filed"],
            })
        time.sleep(0.3)

    return results


if __name__ == "__main__":
    import sys
    d = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    rows = fetch_senate_ptrs(d)
    print(f"days={d} -> {len(rows)} rows")
    for r in rows[:3]:
        print(r)
