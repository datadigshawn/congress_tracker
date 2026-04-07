"""
SEC EDGAR 13F-HR 抓取模組（給 congressTrack Streamlit app 使用）
資料免費、官方來源，無需 API key。
"""
import re
import time
import requests
import xml.etree.ElementTree as ET
from collections import defaultdict

HEADERS = {
    "User-Agent": "congressTrack shawnclaw@example.com",
    "Accept-Encoding": "gzip, deflate",
}

FUNDS = [
    {"name": "Berkshire Hathaway",       "manager": "Warren Buffett",        "cik": "0001067983"},
    {"name": "Bridgewater Associates",   "manager": "Ray Dalio",             "cik": "0001350694"},
    {"name": "Scion Asset Management",   "manager": "Michael Burry",         "cik": "0001649339"},
    {"name": "Pershing Square",          "manager": "Bill Ackman",           "cik": "0001336528"},
    {"name": "Renaissance Technologies", "manager": "Jim Simons",            "cik": "0001037389"},
    {"name": "Appaloosa Management",     "manager": "David Tepper",          "cik": "0001656456"},
    {"name": "Duquesne Family Office",   "manager": "Stanley Druckenmiller", "cik": "0001536411"},
    {"name": "ARK Investment Mgmt",      "manager": "Cathie Wood",           "cik": "0001697748"},
    {"name": "Oaktree Capital Mgmt",     "manager": "Howard Marks",          "cik": "0000949509"},
]

NS = {"ns": "http://www.sec.gov/edgar/document/thirteenf/informationtable"}


def _get(url, host):
    h = dict(HEADERS)
    h["Host"] = host
    r = requests.get(url, headers=h, timeout=30)
    r.raise_for_status()
    return r


def list_13f_filings(cik):
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    data = _get(url, host="data.sec.gov").json()
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accs = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])
    out = []
    for i, f in enumerate(forms):
        if f in ("13F-HR", "13F-HR/A"):
            out.append({"form": f, "accession": accs[i], "filed": dates[i]})
    return out


def fetch_holdings(cik, accession):
    acc_nodash = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}"
    idx = _get(base + "/index.json", host="www.sec.gov").json()
    items = idx.get("directory", {}).get("item", [])
    xml_files = [it["name"] for it in items if it["name"].lower().endswith(".xml")]
    candidates = [n for n in xml_files if "primary" not in n.lower()]
    info_xml = None
    for n in candidates:
        ln = n.lower()
        if "infotable" in ln or "informationtable" in ln or "form13f" in ln:
            info_xml = n
            break
    if not info_xml and candidates:
        info_xml = candidates[0]
    if not info_xml:
        return []
    xml_text = _get(f"{base}/{info_xml}", host="www.sec.gov").text
    root = ET.fromstring(xml_text)
    rows = []
    entries = root.findall("ns:infoTable", NS) or root.findall("infoTable")
    for e in entries:
        def g(tag):
            el = e.find(f"ns:{tag}", NS)
            if el is None:
                el = e.find(tag)
            return el.text.strip() if el is not None and el.text else None

        name = g("nameOfIssuer")
        cusip = g("cusip")
        value = g("value")
        shrs_node = e.find("ns:shrsOrPrnAmt", NS)
        if shrs_node is None:
            shrs_node = e.find("shrsOrPrnAmt")
        shares = 0
        if shrs_node is not None:
            sh_el = shrs_node.find("ns:sshPrnamt", NS)
            if sh_el is None:
                sh_el = shrs_node.find("sshPrnamt")
            if sh_el is not None and sh_el.text:
                try:
                    shares = int(sh_el.text)
                except ValueError:
                    shares = 0
        try:
            value_f = float(value) if value else 0.0
        except ValueError:
            value_f = 0.0
        rows.append({"name": name or "", "cusip": cusip or "", "value": value_f, "shares": shares})
    return rows


def aggregate(rows):
    agg = defaultdict(lambda: {"name": "", "value": 0.0, "shares": 0})
    for r in rows:
        k = r["cusip"] or r["name"]
        agg[k]["name"] = r["name"]
        agg[k]["value"] += r["value"]
        agg[k]["shares"] += r["shares"]
    return agg


def diff_quarters(curr, prev):
    changes = []
    keys = set(curr.keys()) | set(prev.keys())
    for k in keys:
        c = curr.get(k)
        p = prev.get(k)
        if c and not p:
            ctype = "NEW"; chg = c["shares"]
        elif p and not c:
            ctype = "EXIT"; chg = -p["shares"]
        else:
            chg = c["shares"] - p["shares"]
            ctype = "HOLD" if chg == 0 else ("ADD" if chg > 0 else "REDUCE")
        name = (c or p)["name"]
        changes.append({
            "cusip": k, "name": name, "change_type": ctype,
            "shares_change": chg,
            "curr_shares": c["shares"] if c else 0,
            "curr_value": c["value"] if c else 0,
            "prev_shares": p["shares"] if p else 0,
        })
    return changes


def process_fund(fund):
    filings = list_13f_filings(fund["cik"])
    if not filings:
        return {"name": fund["name"], "manager": fund["manager"], "error": "無 13F 申報"}
    latest = filings[0]
    prev = filings[1] if len(filings) > 1 else None
    time.sleep(0.15)
    latest_rows = fetch_holdings(fund["cik"], latest["accession"])
    time.sleep(0.15)
    prev_rows = fetch_holdings(fund["cik"], prev["accession"]) if prev else []

    latest_agg = aggregate(latest_rows)
    prev_agg = aggregate(prev_rows)

    top = sorted(
        [{"cusip": k, **v} for k, v in latest_agg.items()],
        key=lambda x: x["value"], reverse=True,
    )[:20]
    total_value = sum(v["value"] for v in latest_agg.values())

    changes = diff_quarters(latest_agg, prev_agg)
    changes = [c for c in changes if c["change_type"] != "HOLD"]
    changes.sort(key=lambda c: max(c["curr_value"], 0) + abs(c["shares_change"]), reverse=True)

    return {
        "name": fund["name"],
        "manager": fund["manager"],
        "cik": fund["cik"],
        "latest_filed": latest["filed"],
        "latest_form": latest["form"],
        "prev_filed": prev["filed"] if prev else None,
        "total_value": total_value,
        "holdings_count": len(latest_agg),
        "top_holdings": top,
        "changes": changes,
    }


def fetch_all():
    out = []
    for f in FUNDS:
        try:
            out.append(process_fund(f))
        except Exception as e:
            out.append({"name": f["name"], "manager": f["manager"], "error": str(e)})
        time.sleep(0.25)
    return out
