"""
國會交易儀表板生成器
讀取 congress_trades.json，輸出互動式 HTML 儀表板
"""

import json
import os
import sys
import webbrowser
from collections import Counter, defaultdict
from datetime import datetime

# ── 讀取資料 ──────────────────────────────────────────────────────
def load_trades(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def build_stats(trades: list[dict]) -> dict:
    buys  = [t for t in trades if t["type"] == "Purchase"]
    sells = [t for t in trades if t["type"] == "Sale"]
    hits  = [t for t in trades if t["inPortfolio"]]

    # 每個 ticker 的買賣次數
    ticker_buys  = Counter(t["ticker"] for t in buys)
    ticker_sells = Counter(t["ticker"] for t in sells)
    all_tickers  = sorted(set(ticker_buys) | set(ticker_sells),
                          key=lambda x: -(ticker_buys[x] + ticker_sells[x]))

    # 每位議員的交易次數
    pol_counts = Counter(t["politician"] for t in trades)
    top_pols   = pol_counts.most_common(10)

    # 板塊分佈
    sector_counts = Counter(t["sector"] or "其他" for t in trades)

    # 時間軸（依交易日）
    date_counts: dict[str, dict] = defaultdict(lambda: {"buy": 0, "sell": 0})
    for t in trades:
        d = t["txDate"]
        if t["type"] == "Purchase":
            date_counts[d]["buy"] += 1
        else:
            date_counts[d]["sell"] += 1
    sorted_dates = sorted(date_counts)

    return {
        "total": len(trades),
        "buys": len(buys),
        "sells": len(sells),
        "hits": len(hits),
        "hit_tickers": sorted(set(t["ticker"] for t in hits)),
        "all_tickers": all_tickers[:20],
        "ticker_buys":  [ticker_buys.get(tk, 0) for tk in all_tickers[:20]],
        "ticker_sells": [ticker_sells.get(tk, 0) for tk in all_tickers[:20]],
        "ticker_portfolio": [any(t["ticker"] == tk and t["inPortfolio"] for t in trades)
                             for tk in all_tickers[:20]],
        "top_pols":     [p for p, _ in top_pols],
        "top_pol_counts":[c for _, c in top_pols],
        "sectors":      list(sector_counts.keys()),
        "sector_vals":  list(sector_counts.values()),
        "dates":        sorted_dates,
        "date_buys":    [date_counts[d]["buy"]  for d in sorted_dates],
        "date_sells":   [date_counts[d]["sell"] for d in sorted_dates],
        "trades":       trades,
    }

# ── HTML 模板 ─────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>國會交易儀表板</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  background:#0f1117;color:#e0e0e0;padding:20px}
h1{font-size:20px;font-weight:600;margin-bottom:4px;color:#fff}
.sub{font-size:12px;color:#888;margin-bottom:24px}

/* stat cards */
.stats{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px}
.card{background:#1a1d27;border:1px solid #2a2d3d;border-radius:10px;
  padding:14px 20px;min-width:110px}
.card .val{font-size:26px;font-weight:700;color:#fff}
.card .lbl{font-size:11px;color:#888;margin-top:2px}
.card.buy .val{color:#4a9eff}
.card.sell .val{color:#ff6b6b}
.card.hit .val{color:#ffd166}

/* portfolio hits */
.hits{background:#1a1d27;border:1px solid #3d2a2a;border-radius:10px;
  padding:14px 20px;margin-bottom:24px}
.hits h2{font-size:13px;color:#ffd166;margin-bottom:10px}
.badge{display:inline-block;padding:3px 10px;border-radius:999px;
  font-size:12px;font-weight:600;margin:3px;
  background:#2d2000;color:#ffd166;border:1px solid #554400}

/* charts grid */
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
.grid.wide{grid-template-columns:1fr}
@media(max-width:800px){.grid{grid-template-columns:1fr}}
.chart-box{background:#1a1d27;border:1px solid #2a2d3d;border-radius:10px;
  padding:16px}
.chart-box h2{font-size:13px;color:#aaa;margin-bottom:12px}
.chart-wrap{position:relative;height:240px}
.chart-wrap.tall{height:300px}

/* filter bar */
.filters{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px;align-items:center}
.filters input,.filters select{
  background:#1a1d27;border:1px solid #2a2d3d;border-radius:6px;
  color:#e0e0e0;padding:6px 10px;font-size:13px;outline:none}
.filters input:focus,.filters select:focus{border-color:#4a9eff}

/* table */
.tbl-wrap{overflow-x:auto;border-radius:10px;border:1px solid #2a2d3d}
table{width:100%;border-collapse:collapse;font-size:12.5px}
thead tr{background:#1e2130}
th{padding:9px 12px;text-align:left;color:#888;font-weight:500;
   border-bottom:1px solid #2a2d3d;white-space:nowrap}
td{padding:8px 12px;border-bottom:1px solid #1e2130;white-space:nowrap}
tr.hit{background:#221800}
tr:hover td{background:#1e2130}
.buy{color:#4a9eff;font-weight:600}
.sell{color:#ff6b6b;font-weight:600}
.dot{color:#ffd166;margin-right:4px}
.badge-sm{display:inline-block;padding:1px 7px;border-radius:999px;
  font-size:11px;border:1px solid}
.footer{font-size:11px;color:#555;margin-top:20px;line-height:1.7}
</style>
</head>
<body>
<h1>🏛 國會議員交易儀表板</h1>
<div class="sub" id="subtitle"></div>

<!-- stat cards -->
<div class="stats">
  <div class="card"><div class="val" id="st-total">—</div><div class="lbl">總交易筆數</div></div>
  <div class="card buy"><div class="val" id="st-buy">—</div><div class="lbl">買入</div></div>
  <div class="card sell"><div class="val" id="st-sell">—</div><div class="lbl">賣出</div></div>
  <div class="card hit"><div class="val" id="st-hit">—</div><div class="lbl">命中持倉</div></div>
</div>

<!-- portfolio hits -->
<div class="hits" id="hits-box" style="display:none">
  <h2>⚡ 命中你的持倉標的</h2>
  <div id="hit-badges"></div>
</div>

<!-- charts -->
<div class="grid">
  <div class="chart-box">
    <h2>標的買賣次數（Top 20）</h2>
    <div class="chart-wrap tall"><canvas id="c-ticker"></canvas></div>
  </div>
  <div class="chart-box">
    <h2>板塊分佈</h2>
    <div class="chart-wrap"><canvas id="c-sector"></canvas></div>
  </div>
</div>
<div class="grid">
  <div class="chart-box">
    <h2>每日交易量</h2>
    <div class="chart-wrap"><canvas id="c-time"></canvas></div>
  </div>
  <div class="chart-box">
    <h2>最活躍議員（Top 10）</h2>
    <div class="chart-wrap"><canvas id="c-pol"></canvas></div>
  </div>
</div>

<!-- table -->
<div class="filters">
  <input id="f-search" placeholder="搜尋議員 / 標的" oninput="renderTable()">
  <select id="f-match" onchange="renderTable()">
    <option value="all">全部標的</option>
    <option value="portfolio">僅持倉</option>
    <option value="nonportfolio">非持倉</option>
  </select>
  <select id="f-type" onchange="renderTable()">
    <option value="all">買賣全部</option>
    <option value="buy">只看買入</option>
    <option value="sell">只看賣出</option>
  </select>
  <span id="f-count" style="font-size:12px;color:#666"></span>
</div>
<div class="tbl-wrap">
  <table>
    <thead><tr>
      <th>議員</th><th>州</th><th>標的</th><th>操作</th>
      <th>金額範圍</th><th>交易日</th><th>揭露日</th><th>板塊</th>
    </tr></thead>
    <tbody id="tbl-body"></tbody>
  </table>
</div>

<div class="footer">
  ⚠ 依法議員需在交易後 45 天內申報。● 代表你目前的持倉標的。<br>
  資料來源：House Clerk 官方 PTR（僅眾議院）
</div>

<script>
const RAW  = __RAW_DATA__;
const STAT = __STAT_DATA__;

// ── 統計卡 ──────────────────────────────────────────────────────
document.getElementById('st-total').textContent = STAT.total;
document.getElementById('st-buy').textContent   = STAT.buys;
document.getElementById('st-sell').textContent  = STAT.sells;
document.getElementById('st-hit').textContent   = STAT.hits;

const d0 = RAW.reduce((a,t)=>t.txDate<a?t.txDate:a, RAW[0]?.txDate||'');
const d1 = RAW.reduce((a,t)=>t.txDate>a?t.txDate:a, '');
document.getElementById('subtitle').textContent =
  `資料期間：${d0} ~ ${d1}　共 ${STAT.total} 筆`;

// 命中持倉
if (STAT.hits > 0) {
  document.getElementById('hits-box').style.display = '';
  document.getElementById('hit-badges').innerHTML =
    STAT.hit_tickers.map(tk =>
      `<span class="badge">${tk}</span>`).join('');
}

// ── Chart 共用設定 ───────────────────────────────────────────────
Chart.defaults.color = '#888';
Chart.defaults.borderColor = '#2a2d3d';
const font = {family:'-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif', size:11};

// ── 標的買賣圖 ───────────────────────────────────────────────────
const tickerColors = STAT.all_tickers.map((_,i) =>
  STAT.ticker_portfolio[i] ? '#ffd166' : '#4a9eff');
new Chart(document.getElementById('c-ticker'), {
  type: 'bar',
  data: {
    labels: STAT.all_tickers,
    datasets: [
      {label:'買入', data: STAT.ticker_buys,
       backgroundColor: STAT.all_tickers.map((_,i)=>
         STAT.ticker_portfolio[i]?'#cc9900':'#2a6fb5'), borderRadius:3},
      {label:'賣出', data: STAT.ticker_sells,
       backgroundColor: STAT.all_tickers.map((_,i)=>
         STAT.ticker_portfolio[i]?'#cc4400':'#8b2222'), borderRadius:3},
    ]
  },
  options: {
    responsive:true, maintainAspectRatio:false,
    plugins:{legend:{labels:{font}},
      tooltip:{callbacks:{afterLabel:ctx=>{
        const tk=STAT.all_tickers[ctx.dataIndex];
        return STAT.ticker_portfolio[ctx.dataIndex]?'★ 你的持倉':'';
      }}}},
    scales:{
      x:{stacked:true, ticks:{font, maxRotation:45}},
      y:{stacked:true, ticks:{font, stepSize:1}}
    }
  }
});

// ── 板塊圓餅圖 ───────────────────────────────────────────────────
const sectorColors = ['#4a9eff','#ff6b6b','#ffd166','#06d6a0','#a29bfe',
  '#fd79a8','#e17055','#74b9ff','#55efc4','#fdcb6e'];
new Chart(document.getElementById('c-sector'), {
  type: 'doughnut',
  data: {
    labels: STAT.sectors,
    datasets:[{data: STAT.sector_vals,
      backgroundColor: sectorColors, borderWidth:1, borderColor:'#0f1117'}]
  },
  options:{
    responsive:true, maintainAspectRatio:false,
    plugins:{legend:{position:'right', labels:{font, padding:12}}}
  }
});

// ── 時間軸折線圖 ─────────────────────────────────────────────────
new Chart(document.getElementById('c-time'), {
  type: 'bar',
  data: {
    labels: STAT.dates,
    datasets:[
      {label:'買入', data:STAT.date_buys,  backgroundColor:'#2a6fb5', borderRadius:2},
      {label:'賣出', data:STAT.date_sells, backgroundColor:'#8b2222', borderRadius:2},
    ]
  },
  options:{
    responsive:true, maintainAspectRatio:false,
    plugins:{legend:{labels:{font}}},
    scales:{
      x:{stacked:true, ticks:{font, maxRotation:45, maxTicksLimit:10}},
      y:{stacked:true, ticks:{font, stepSize:1}}
    }
  }
});

// ── 議員活躍度 ───────────────────────────────────────────────────
new Chart(document.getElementById('c-pol'), {
  type: 'bar',
  data: {
    labels: STAT.top_pols,
    datasets:[{label:'交易次數', data:STAT.top_pol_counts,
      backgroundColor:'#4a9eff', borderRadius:3}]
  },
  options:{
    indexAxis:'y',
    responsive:true, maintainAspectRatio:false,
    plugins:{legend:{display:false}},
    scales:{x:{ticks:{font, stepSize:1}}, y:{ticks:{font}}}
  }
});

// ── 明細表格 ─────────────────────────────────────────────────────
function renderTable() {
  const search = document.getElementById('f-search').value.toLowerCase();
  const match  = document.getElementById('f-match').value;
  const type   = document.getElementById('f-type').value;

  let data = RAW.filter(t => {
    if (search && !t.politician.toLowerCase().includes(search)
               && !t.ticker.toLowerCase().includes(search)) return false;
    if (match === 'portfolio'    && !t.inPortfolio) return false;
    if (match === 'nonportfolio' &&  t.inPortfolio) return false;
    if (type  === 'buy'  && t.type !== 'Purchase') return false;
    if (type  === 'sell' && t.type !== 'Sale')     return false;
    return true;
  });

  // 持倉命中優先，再按交易日倒序
  data.sort((a,b) => {
    if (a.inPortfolio !== b.inPortfolio) return a.inPortfolio ? -1 : 1;
    return b.txDate.localeCompare(a.txDate);
  });

  document.getElementById('f-count').textContent = `共 ${data.length} 筆`;

  const tbody = document.getElementById('tbl-body');
  tbody.innerHTML = data.map(t => {
    const isBuy = t.type === 'Purchase';
    return `<tr class="${t.inPortfolio?'hit':''}">
      <td>${t.inPortfolio?'<span class="dot">●</span>':''}${t.politician}</td>
      <td style="color:#666">${t.state}</td>
      <td><b>${t.ticker}</b></td>
      <td class="${isBuy?'buy':'sell'}">${isBuy?'買入':'賣出'}</td>
      <td style="color:#aaa">${t.amount}</td>
      <td style="color:#aaa">${t.txDate}</td>
      <td style="color:#666">${t.disclosureDate}</td>
      <td>${t.sector?`<span class="badge-sm" style="color:#888;border-color:#333">${t.sector}</span>`:''}</td>
    </tr>`;
  }).join('');
}

renderTable();
</script>
</body>
</html>
"""

# ── 主程式 ────────────────────────────────────────────────────────
def generate(json_path: str, open_browser: bool = True) -> str:
    trades = load_trades(json_path)
    stats  = build_stats(trades)

    # 移除 trades 大陣列（table 直接用 RAW）
    stat_out = {k: v for k, v in stats.items() if k != "trades"}

    html = HTML.replace("__RAW_DATA__",  json.dumps(trades,    ensure_ascii=False))
    html = html.replace("__STAT_DATA__", json.dumps(stat_out,  ensure_ascii=False))

    out_path = os.path.join(os.path.dirname(json_path), "dashboard.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✓ 儀表板已生成：{out_path}")
    if open_browser:
        webbrowser.open(f"file://{out_path}")
    return out_path


if __name__ == "__main__":
    default = os.path.join(os.path.dirname(__file__), "congress_trades.json")
    path = sys.argv[1] if len(sys.argv) > 1 else default

    if not os.path.exists(path):
        print(f"⚠ 找不到 {path}")
        print("  請先執行：./run.sh --days 30 --save")
        sys.exit(1)

    generate(path)
