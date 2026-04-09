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
    {"name": "Berkshire Hathaway",       "manager": "Warren Buffett",        "cik": "0001067983",
     "desc": "波克夏海瑟威：巴菲特的旗艦控股公司，長期價值投資的代表，重倉可口可樂、美國銀行、蘋果等，持股週期以數年至數十年計。"},
    {"name": "Bridgewater Associates",   "manager": "Ray Dalio",             "cik": "0001350694",
     "desc": "橋水：全球最大避險基金之一，以「全天候」(All Weather) 與「純阿爾法」(Pure Alpha) 宏觀策略著稱，跨股、債、商品、外匯做風險平價配置。"},
    {"name": "Scion Asset Management",   "manager": "Michael Burry",         "cik": "0001649339",
     "desc": "Scion：《大賣空》原型 Michael Burry 的基金，以極度逆向、集中持倉、經常放空大型科技股與被動 ETF 而聞名。"},
    {"name": "Pershing Square",          "manager": "Bill Ackman",           "cik": "0001336528",
     "desc": "Pershing Square：Bill Ackman 的積極型 (activist) 基金，集中持有少數幾檔高品質消費／服務業龍頭，經常介入公司治理。"},
    {"name": "Renaissance Technologies", "manager": "Jim Simons",            "cik": "0001037389",
     "desc": "文藝復興科技：Jim Simons 創立的量化鼻祖，旗下 Medallion 基金績效傳奇，靠數學模型高頻交易大量標的。"},
    {"name": "Appaloosa Management",     "manager": "David Tepper",          "cik": "0001656456",
     "desc": "Appaloosa：David Tepper 的不良債權與宏觀基金，擅長危機入市、在科技與金融股之間大膽輪動。"},
    {"name": "Duquesne Family Office",   "manager": "Stanley Druckenmiller", "cik": "0001536411",
     "desc": "Duquesne 家族辦公室：索羅斯前左右手 Druckenmiller 操盤，以宏觀判斷加集中成長股聞名，年化績效長期逾 30%。"},
    {"name": "ARK Investment Mgmt",      "manager": "Cathie Wood",           "cik": "0001697748",
     "desc": "ARK Invest：Cathie Wood 旗下主題型主動 ETF 發行商，聚焦人工智慧、基因體、自駕、區塊鏈等破壞式創新。"},
    {"name": "Oaktree Capital Mgmt",     "manager": "Howard Marks",          "cik": "0000949509",
     "desc": "橡樹資本：Howard Marks 創辦，全球最大不良債權／信用投資管理人之一，強調風險控管與景氣循環。"},
    {"name": "Soros Fund Management",    "manager": "George Soros",          "cik": "0001029160",
     "desc": "索羅斯基金管理：George Soros 的家族辦公室（前量子基金），以宏觀對沖與「反射理論」聞名，著名戰役包含 1992 年放空英鎊。"},
    {"name": "Citadel Advisors",         "manager": "Ken Griffin",           "cik": "0001423053",
     "desc": "Citadel：Ken Griffin 創立，全球頂尖多策略避險基金，橫跨股票、固收、商品、量化與造市，是華爾街流動性重要提供者。"},
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


def process_fund(fund, years_back: int = 3):
    """抓取近 N 年全部 13F 申報，逐季保存快照與季變動。"""
    from datetime import datetime, timedelta
    filings = list_13f_filings(fund["cik"])
    if not filings:
        return {"name": fund["name"], "manager": fund["manager"], "error": "無 13F 申報"}

    cutoff = (datetime.now() - timedelta(days=365 * years_back + 120)).strftime("%Y-%m-%d")
    # filings 由新到舊；多抓一季以便做 diff
    kept = [f for f in filings if f["filed"] >= cutoff]
    if not kept:
        kept = filings[:1]
    # 多保留一筆更舊的，以便最舊那季也能算 diff
    if len(filings) > len(kept):
        kept.append(filings[len(kept)])

    # 逐筆抓 holdings（由舊到新處理以便 diff）
    kept_sorted = list(reversed(kept))  # 舊 → 新
    aggs = []
    for filing in kept_sorted:
        try:
            rows = fetch_holdings(fund["cik"], filing["accession"])
        except Exception as e:
            rows = []
        aggs.append({"filing": filing, "agg": aggregate(rows)})
        time.sleep(0.2)

    # 建 history：每一季的 top + 相對前一季的 changes
    history = []  # 新 → 舊
    for i in range(len(aggs) - 1, -1, -1):
        curr = aggs[i]["agg"]
        prev = aggs[i - 1]["agg"] if i - 1 >= 0 else {}
        top = sorted(
            [{"cusip": k, **v} for k, v in curr.items()],
            key=lambda x: x["value"], reverse=True,
        )[:20]
        total_value = sum(v["value"] for v in curr.values())
        changes = [c for c in diff_quarters(curr, prev) if c["change_type"] != "HOLD"]
        changes.sort(key=lambda c: max(c["curr_value"], 0) + abs(c["shares_change"]), reverse=True)
        history.append({
            "filed": aggs[i]["filing"]["filed"],
            "form": aggs[i]["filing"]["form"],
            "accession": aggs[i]["filing"]["accession"],
            "total_value": total_value,
            "holdings_count": len(curr),
            "top_holdings": top,
            "changes": changes,
        })

    # 依 3 年 cutoff 過濾 history（UI 顯示用），但保留最新一季
    cutoff_strict = (datetime.now() - timedelta(days=365 * years_back)).strftime("%Y-%m-%d")
    history_trim = [h for h in history if h["filed"] >= cutoff_strict] or history[:1]

    latest = history_trim[0]
    prev = history_trim[1] if len(history_trim) > 1 else None

    return {
        "name": fund["name"],
        "manager": fund["manager"],
        "cik": fund["cik"],
        "desc": fund.get("desc", ""),
        "latest_filed": latest["filed"],
        "latest_form": latest["form"],
        "prev_filed": prev["filed"] if prev else None,
        "total_value": latest["total_value"],
        "holdings_count": latest["holdings_count"],
        "top_holdings": latest["top_holdings"],
        "changes": latest["changes"],
        "history": history_trim,  # 新 → 舊，近 N 年所有季度
    }


def fetch_all(years_back: int = 3):
    out = []
    for f in FUNDS:
        try:
            out.append(process_fund(f, years_back=years_back))
        except Exception as e:
            out.append({"name": f["name"], "manager": f["manager"], "desc": f.get("desc", ""), "error": str(e)})
        time.sleep(0.25)
    return out
