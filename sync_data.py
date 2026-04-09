#!/usr/bin/env python3
"""
sync_data.py ── 資料同步腳本（本地 SQLite 版本）

將 app.py 內的 load_* 函式結果寫入 data.db，讓 app_offline.py 可秒開。
本腳本不依賴 Streamlit runtime，透過 mock streamlit 模組後再 import app。

用法：
  # 一次同步全部（US 3 年 + 立委 3 年）
  python sync_data.py --source all

  # 只同步 US 眾議院 / 參議院（days = 交易日往前回溯天數）
  python sync_data.py --source us_house  --days 1095
  python sync_data.py --source us_senate --days 1095

  # 同步立委（依月份範圍）
  python sync_data.py --source tw_legislator --from 2023-01 --to 2026-04

  # 同步縣市議員（需指定縣市，否則全國 7000+ PDF 會跑很久）
  python sync_data.py --source tw_councilor --cities 臺北市 新北市 --from 2024-01 --to 2026-04

  # 清掉超過 10 年的舊資料
  python sync_data.py --prune 10
"""
import argparse
import contextlib
import json
import os
import sqlite3
import sys
import types
from datetime import date, datetime, timedelta


# ══════════════════════════════════════════════════════════════════
# 1. 在 import app 前 mock streamlit，讓 @st.cache_data / st.progress 變 no-op
# ══════════════════════════════════════════════════════════════════
def _install_fake_streamlit() -> None:
    class _FakeProg:
        def progress(self, *a, **k): pass
        def empty(self): pass

    class _FakeSt:
        def set_page_config(self, *a, **k): pass
        def progress(self, *a, **k): return _FakeProg()
        def empty(self, *a, **k): return _FakeProg()

        def cache_data(self, *a, **k):
            # 支援 @st.cache_data 與 @st.cache_data(ttl=..., show_spinner=...)
            if len(a) == 1 and callable(a[0]) and not k:
                return a[0]
            def deco(fn): return fn
            return deco

        cache_resource = cache_data

        def spinner(self, *a, **k):
            @contextlib.contextmanager
            def cm(): yield
            return cm()

        def warning(self, *a, **k): pass
        def info(self, *a, **k): pass
        def error(self, *a, **k): pass
        def success(self, *a, **k): pass
        def caption(self, *a, **k): pass
        def write(self, *a, **k): pass
        def markdown(self, *a, **k): pass
        def stop(self): raise SystemExit

        # 用到的其他 attribute 直接回傳 no-op
        def __getattr__(self, name):
            def _noop(*a, **k): return None
            return _noop

    fake = _FakeSt()
    # column_config 是個命名空間，給個空 namespace 即可
    fake.column_config = types.SimpleNamespace(
        TextColumn=lambda *a, **k: None,
        NumberColumn=lambda *a, **k: None,
    )
    sys.modules["streamlit"] = fake


_install_fake_streamlit()

# app.py 在模組層執行 UI 程式碼 (with st.sidebar: ...)，
# 因此無法直接 import。改為 compile + exec 前半部（定義區），
# 切點在 "# ── 側邊欄" 之前，僅載入函式定義。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def _load_app_functions() -> dict:
    app_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py")
    with open(app_path, encoding="utf-8") as f:
        src = f.read()
    cut = src.find("# ── 側邊欄")
    if cut == -1:
        cut = src.find("with st.sidebar:")
    if cut == -1:
        cut = len(src)
    code = compile(src[:cut], app_path, "exec")
    ns: dict = {"__name__": "app_partial", "__file__": app_path}
    exec(code, ns)
    return ns

app = types.SimpleNamespace(**_load_app_functions())  # noqa: E402


# ══════════════════════════════════════════════════════════════════
# 2. DB schema
# ══════════════════════════════════════════════════════════════════
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS us_trades (
  議員 TEXT NOT NULL,
  院   TEXT NOT NULL,
  州   TEXT,
  標的 TEXT,
  操作 TEXT,
  金額 TEXT,
  交易日 TEXT,
  揭露日 TEXT,
  申報日 TEXT,
  板塊 TEXT,
  持倉 INTEGER,
  _synced_at TEXT,
  PRIMARY KEY (議員, 院, 標的, 交易日, 操作, 金額)
);
CREATE INDEX IF NOT EXISTS idx_us_tx      ON us_trades(交易日);
CREATE INDEX IF NOT EXISTS idx_us_disc    ON us_trades(揭露日);
CREATE INDEX IF NOT EXISTS idx_us_chamber ON us_trades(院);

CREATE TABLE IF NOT EXISTS tw_holdings (
  姓名 TEXT NOT NULL,
  職稱 TEXT,
  縣市 TEXT,
  申報日 TEXT,
  公司   TEXT,
  持有人 TEXT,
  股數   INTEGER,
  票面額 REAL,
  申報總額 REAL,
  板塊 TEXT,
  是否本人 INTEGER,
  _synced_at TEXT,
  PRIMARY KEY (姓名, 職稱, 申報日, 公司, 持有人)
);
CREATE INDEX IF NOT EXISTS idx_tw_name ON tw_holdings(姓名);
CREATE INDEX IF NOT EXISTS idx_tw_city ON tw_holdings(縣市);
CREATE INDEX IF NOT EXISTS idx_tw_date ON tw_holdings(申報日);
CREATE INDEX IF NOT EXISTS idx_tw_role ON tw_holdings(職稱);

CREATE TABLE IF NOT EXISTS funds_13f_cache (
  key TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  _synced_at TEXT
);

CREATE TABLE IF NOT EXISTS sync_log (
  source TEXT PRIMARY KEY,
  last_synced TEXT,
  row_count INTEGER,
  params TEXT
);
"""


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(SCHEMA)


def _mark_log(source: str, n: int, params: dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO sync_log VALUES (?,?,?,?)",
            (source, datetime.now().isoformat(timespec="seconds"),
             n, json.dumps(params, ensure_ascii=False)),
        )


# ══════════════════════════════════════════════════════════════════
# 3. Upsert
# ══════════════════════════════════════════════════════════════════
def upsert_us(df) -> int:
    if df is None or df.empty:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            str(r.get("議員", "")), str(r.get("院", "")), str(r.get("州", "")),
            str(r.get("標的", "")), str(r.get("操作", "")), str(r.get("金額", "")),
            str(r.get("交易日", "")), str(r.get("揭露日", "")),
            str(r.get("申報日", "")), str(r.get("板塊", "")),
            int(bool(r.get("持倉", False))), now,
        ))
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO us_trades
               (議員,院,州,標的,操作,金額,交易日,揭露日,申報日,板塊,持倉,_synced_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
    return len(rows)


def upsert_tw(df) -> int:
    if df is None or df.empty:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            str(r.get("姓名", "")), str(r.get("職稱", "")), str(r.get("縣市", "")),
            str(r.get("申報日", "")), str(r.get("公司", "")), str(r.get("持有人", "")),
            int(r.get("股數", 0) or 0),
            float(r.get("票面額", 0) or 0),
            float(r.get("申報總額", 0) or 0),
            str(r.get("板塊", "")),
            int(bool(r.get("是否本人", False))), now,
        ))
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO tw_holdings
               (姓名,職稱,縣市,申報日,公司,持有人,股數,票面額,申報總額,板塊,是否本人,_synced_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
    return len(rows)


# ══════════════════════════════════════════════════════════════════
# 4. Sync entry points
# ══════════════════════════════════════════════════════════════════
def sync_us_house(days: int) -> None:
    print(f"🇺🇸 Sync US House: 過去 {days} 天 ...")
    df = app.load_us_trades(days, date.today().isoformat())
    n = upsert_us(df)
    _mark_log("us_house", n, {"days": days})
    print(f"   → {n} 筆")


def sync_us_senate(days: int) -> None:
    print(f"🇺🇸 Sync US Senate: 過去 {days} 天 ...")
    df = app.load_senate_trades(days, date.today().isoformat())
    n = upsert_us(df)
    _mark_log("us_senate", n, {"days": days})
    print(f"   → {n} 筆")


def sync_tw_legislator(date_from: str, date_to: str) -> None:
    print(f"🇹🇼 Sync 立委: {date_from} ~ {date_to} ...")
    df = app.load_tw_holdings(date_from, date_to, "立法委員", None)
    n = upsert_tw(df)
    _mark_log("tw_legislator", n, {"from": date_from, "to": date_to})
    print(f"   → {n} 筆")


def sync_funds_13f() -> None:
    print("🏦 Sync 機構 13F ...")
    import funds_13f as f13
    data = f13.fetch_all()
    payload = json.dumps(data, ensure_ascii=False, default=str)
    now = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO funds_13f_cache VALUES (?,?,?)",
            ("all", payload, now),
        )
    n = len(data) if isinstance(data, list) else 0
    _mark_log("funds_13f", n, {})
    print(f"   → {n} 檔基金")


def sync_tw_councilor(date_from: str, date_to: str, cities: list[str]) -> None:
    if not cities:
        print("⚠️  未指定 --cities，為避免誤抓全國改為跳過。"); return
    for city in cities:
        print(f"🇹🇼 Sync 議員 {city}: {date_from} ~ {date_to} ...")
        df = app.load_tw_holdings(date_from, date_to, "議員", (city,))
        n = upsert_tw(df)
        _mark_log(f"tw_councilor_{city}", n,
                  {"from": date_from, "to": date_to, "city": city})
        print(f"   → {n} 筆")


# ══════════════════════════════════════════════════════════════════
# 5. Prune：保留最近 N 年
# ══════════════════════════════════════════════════════════════════
def prune_old(years: int) -> None:
    cutoff = datetime.now() - timedelta(days=365 * years)
    cutoff_iso = cutoff.date().isoformat()          # YYYY-MM-DD
    cutoff_us  = cutoff.strftime("%m/%d/%Y")        # MM/DD/YYYY
    cutoff_roc = f"民國{cutoff.year - 1911:03d}年"
    print(f"🧹 Prune: 保留最近 {years} 年（cutoff={cutoff_iso}）")
    with sqlite3.connect(DB_PATH) as conn:
        # US 交易日是 MM/DD/YYYY 字串，轉 date 比對用 substr
        before_us = conn.execute("SELECT COUNT(*) FROM us_trades").fetchone()[0]
        conn.execute("""
            DELETE FROM us_trades
            WHERE 交易日 != '' AND substr(交易日,7,4)||'-'||substr(交易日,1,2)||'-'||substr(交易日,4,2) < ?
        """, (cutoff_iso,))
        after_us = conn.execute("SELECT COUNT(*) FROM us_trades").fetchone()[0]

        before_tw = conn.execute("SELECT COUNT(*) FROM tw_holdings").fetchone()[0]
        # 台灣申報日格式 "民國115年 04月 09日" → 取年份比對
        conn.execute("""
            DELETE FROM tw_holdings
            WHERE 申報日 != '' AND 申報日 < ?
        """, (cutoff_roc,))
        after_tw = conn.execute("SELECT COUNT(*) FROM tw_holdings").fetchone()[0]
    print(f"   US: {before_us} → {after_us}  (removed {before_us-after_us})")
    print(f"   TW: {before_tw} → {after_tw}  (removed {before_tw-after_tw})")


# ══════════════════════════════════════════════════════════════════
# 6. CLI
# ══════════════════════════════════════════════════════════════════
def _default_date_from(years: int = 3) -> str:
    return (date.today() - timedelta(days=365 * years)).strftime("%Y-%m")


def main() -> None:
    ap = argparse.ArgumentParser(description="同步 app.py 抓取結果到 data.db")
    ap.add_argument(
        "--source",
        choices=["all", "us_house", "us_senate", "us", "tw_legislator", "tw_councilor", "tw", "funds_13f"],
        default="all",
    )
    ap.add_argument("--days", type=int, default=365 * 3,
                    help="US 回溯交易日天數（預設 3 年 = 1095）")
    ap.add_argument("--from", dest="date_from", default=None,
                    help="TW 起始月份 YYYY-MM，預設 3 年前")
    ap.add_argument("--to", dest="date_to", default=None,
                    help="TW 結束月份 YYYY-MM，預設本月")
    ap.add_argument("--cities", nargs="*", default=None,
                    help="TW 縣市議員要同步的縣市（可多選）")
    ap.add_argument("--prune", type=int, default=None,
                    help="清掉超過 N 年的舊資料（建議 10）")
    args = ap.parse_args()

    init_db()

    # 預設時間 = 過去 3 年
    if args.date_from is None:
        args.date_from = _default_date_from(3)
    if args.date_to is None:
        args.date_to = date.today().strftime("%Y-%m")

    tw_from = args.date_from + "-01"
    _y, _m = int(args.date_to[:4]), int(args.date_to[5:7])
    import calendar
    tw_to = f"{args.date_to}-{calendar.monthrange(_y, _m)[1]:02d}"

    src = args.source
    if src in ("all", "us", "us_house"):
        try: sync_us_house(args.days)
        except Exception as e: print(f"   ❌ us_house 失敗: {e}")
    if src in ("all", "us", "us_senate"):
        try: sync_us_senate(args.days)
        except Exception as e: print(f"   ❌ us_senate 失敗: {e}")
    if src in ("all", "tw", "tw_legislator"):
        try: sync_tw_legislator(tw_from, tw_to)
        except Exception as e: print(f"   ❌ tw_legislator 失敗: {e}")
    if src in ("all", "funds_13f"):
        try: sync_funds_13f()
        except Exception as e: print(f"   ❌ funds_13f 失敗: {e}")
    if src in ("tw", "tw_councilor"):
        try: sync_tw_councilor(tw_from, tw_to, args.cities or [])
        except Exception as e: print(f"   ❌ tw_councilor 失敗: {e}")

    if args.prune is not None:
        prune_old(args.prune)

    # 顯示最終 log
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT source, last_synced, row_count FROM sync_log ORDER BY source"
        ).fetchall()
    if rows:
        print("\n📊 sync_log:")
        for s, t, n in rows:
            print(f"   {s:<30} {t}  {n} rows")


if __name__ == "__main__":
    main()
