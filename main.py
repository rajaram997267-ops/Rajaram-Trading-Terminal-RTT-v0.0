from __future__ import annotations

import csv
import gzip
import io
import json
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras

from flask import Flask, jsonify, render_template, request, redirect, url_for, make_response

BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.environ.get("DATABASE_URL", "")
IST_OFFSET = timedelta(hours=5, minutes=30)

app = Flask(__name__)

SECTOR_MAP = {
    "360ONE": "Financials",
    "ABB": "Industrials",
    "ABCAPITAL": "Financials",
    "ADANIENSOL": "Power & Utilities",
    "ADANIENT": "Services",
    "ADANIGREEN": "Power & Utilities",
    "ADANIPORTS": "Transportation",
    "ADANIPOWER": "Power & Utilities",
    "ALKEM": "Healthcare",
    "AMBER": "Consumer Discretionary",
    "AMBUJACEM": "Building Materials",
    "ANGELONE": "Financials",
    "APLAPOLLO": "Industrials",
    "APOLLOHOSP": "Healthcare",
    "ASHOKLEY": "Auto",
    "ASIANPAINT": "Building Materials",
    "ASTRAL": "Plastic Products",
    "AUBANK": "Bank",
    "AUROPHARMA": "Healthcare",
    "AXISBANK": "Bank",
    "BAJAJ-AUTO": "Auto",
    "BAJAJFINSV": "Financials",
    "BAJAJHLDNG": "Financials",
    "BAJFINANCE": "Financials",
    "BANDHANBNK": "Bank",
    "BANKBARODA": "Bank",
    "BANKINDIA": "Bank",
    "BANKNIFTY": "Indices",
    "BDL": "Aerospace & Defence",
    "BEL": "Aerospace & Defence",
    "BHARATFORG": "Industrials",
    "BHARTIARTL": "Telecom-Service",
    "BHEL": "Industrials",
    "BIOCON": "Healthcare",
    "BLUESTARCO": "Consumer Discretionary",
    "BOSCHLTD": "Auto",
    "BPCL": "Energy",
    "BRITANNIA": "Fmcg",
    "BSE": "Financials",
    "CAMS": "Financials",
    "CANBK": "Bank",
    "CDSL": "Financials",
    "CGPOWER": "Industrials",
    "CHOLAFIN": "Financials",
    "CIPLA": "Healthcare",
    "CNXMIDCAP": "Indices",
    "COALINDIA": "Metals & Mining",
    "COCHINSHIP": "Aerospace & Defence",
    "COFORGE": "I.T",
    "COLPAL": "Fmcg",
    "CONCOR": "Transportation",
    "CROMPTON": "Consumer Discretionary",
    "CUMMINSIND": "Industrials",
    "DABUR": "Fmcg",
    "DALBHARAT": "Building Materials",
    "DELHIVERY": "Transportation",
    "DIVISLAB": "Healthcare",
    "DIXON": "Consumer Discretionary",
    "DLF": "Realty",
    "DMART": "Consumer Discretionary",
    "DRREDDY": "Healthcare",
    "EICHERMOT": "Auto",
    "ETERNAL": "I.T",
    "EXIDEIND": "Auto",
    "FEDERALBNK": "Bank",
    "FORCEMOT": "Auto",
    "FORTIS": "Healthcare",
    "GAIL": "Energy",
    "GLENMARK": "Healthcare",
    "GMRAIRPORT": "Miscellaneous",
    "GODFRYPHLP": "Fmcg",
    "GODREJCP": "Fmcg",
    "GODREJPROP": "Realty",
    "GRASIM": "Textiles",
    "GVT&D": "Industrials",
    "HAL": "Aerospace & Defence",
    "HAVELLS": "Consumer Discretionary",
    "HCLTECH": "I.T",
    "HDFCAMC": "Financials",
    "HDFCBANK": "Bank",
    "HDFCLIFE": "Financials",
    "HEROMOTOCO": "Auto",
    "HINDALCO": "Metals & Mining",
    "HINDPETRO": "Energy",
    "HINDUNILVR": "Fmcg",
    "HINDZINC": "Metals & Mining",
    "HYUNDAI": "Auto",
    "ICICIBANK": "Bank",
    "ICICIGI": "Financials",
    "ICICIPRULI": "Financials",
    "IDEA": "Telecom-Service",
    "IDFCFIRSTB": "Bank",
    "IEX": "Financials",
    "INDHOTEL": "Services",
    "INDIANB": "Bank",
    "INDIGO": "Transportation",
    "INDUSINDBK": "Bank",
    "INDUSTOWER": "Telecom",
    "INFY": "I.T",
    "INOXWIND": "Industrials",
    "IOC": "Energy",
    "IREDA": "Financials",
    "IRFC": "Financials",
    "ITC": "Fmcg",
    "JINDALSTEL": "Metals & Mining",
    "JIOFIN": "Financials",
    "JSWENERGY": "Power & Utilities",
    "JSWSTEEL": "Metals & Mining",
    "JUBLFOOD": "Consumer Discretionary",
    "KALYANKJIL": "Consumer Discretionary",
    "KAYNES": "Consumer Discretionary",
    "KEI": "Industrials",
    "KFINTECH": "Financials",
    "KOTAKBANK": "Bank",
    "KPITTECH": "I.T",
    "LAURUSLABS": "Healthcare",
    "LICHSGFIN": "Financials",
    "LICI": "Financials",
    "LODHA": "Realty",
    "LT": "Realty",
    "LTF": "Financials",
    "LTM": "I.T",
    "LUPIN": "Healthcare",
    "M&M": "Auto",
    "MANAPPURAM": "Financials",
    "MANKIND": "Healthcare",
    "MARICO": "Fmcg",
    "MARUTI": "Auto",
    "MAXHEALTH": "Healthcare",
    "MAZDOCK": "Aerospace & Defence",
    "MCX": "Financials",
    "MFSL": "Miscellaneous",
    "MOTHERSON": "Auto",
    "MOTILALOFS": "Financials",
    "MPHASIS": "I.T",
    "MUTHOOTFIN": "Financials",
    "NAM-INDIA": "Financials",
    "NATIONALUM": "Metals & Mining",
    "NAUKRI": "I.T",
    "NBCC": "Miscellaneous",
    "NESTLEIND": "Fmcg",
    "NHPC": "Power & Utilities",
    "NIFTY": "Indices",
    "NMDC": "Metals & Mining",
    "NTPC": "Power & Utilities",
    "NUVAMA": "Financials",
    "NYKAA": "I.T",
    "OBEROIRLTY": "Realty",
    "OFSS": "I.T",
    "OIL": "Energy",
    "ONGC": "Energy",
    "PAGEIND": "Consumer Discretionary",
    "PATANJALI": "Fmcg",
    "PAYTM": "I.T",
    "PERSISTENT": "I.T",
    "PETRONET": "Energy",
    "PFC": "Financials",
    "PGEL": "Consumer Discretionary",
    "PHOENIXLTD": "Realty",
    "PIDILITIND": "Chemicals",
    "PIIND": "Chemicals",
    "PNB": "Bank",
    "PNBHOUSING": "Financials",
    "POLICYBZR": "I.T",
    "POLYCAB": "Industrials",
    "POWERGRID": "Power & Utilities",
    "POWERINDIA": "Industrials",
    "PREMIERENE": "Services",
    "PRESTIGE": "Realty",
    "RADICO": "Fmcg",
    "RBLBANK": "Bank",
    "RECLTD": "Financials",
    "RELIANCE": "Energy",
    "RVNL": "Realty",
    "SAIL": "Metals & Mining",
    "SBICARD": "Financials",
    "SBILIFE": "Financials",
    "SBIN": "Bank",
    "SHREECEM": "Building Materials",
    "SHRIRAMFIN": "Financials",
    "SIEMENS": "Industrials",
    "SOLARINDS": "Aerospace & Defence",
    "SONACOMS": "Auto",
    "SRF": "Chemicals",
    "SUNPHARMA": "Healthcare",
    "SUPREMEIND": "Plastic Products",
    "SUZLON": "Industrials",
    "SWIGGY": "I.T",
    "TATACONSUM": "Fmcg",
    "TATAELXSI": "I.T",
    "TATAPOWER": "Power & Utilities",
    "TATASTEEL": "Metals & Mining",
    "TCS": "I.T",
    "TECHM": "I.T",
    "TIINDIA": "Industrials",
    "TITAN": "Consumer Discretionary",
    "TMPV": "Auto",
    "TORNTPHARM": "Healthcare",
    "TRENT": "Consumer Discretionary",
    "TVSMOTOR": "Auto",
    "ULTRACEMCO": "Building Materials",
    "UNIONBANK": "Bank",
    "UNITDSPR": "Fmcg",
    "UNOMINDA": "Auto",
    "UPL": "Chemicals",
    "VBL": "Fmcg",
    "VEDL": "Metals & Mining",
    "VMM": "Consumer Discretionary",
    "VOLTAS": "Consumer Discretionary",
    "WAAREEENER": "Industrials",
    "WIPRO": "I.T",
    "YESBANK": "Bank",
    "ZYDUSLIFE": "Healthcare",
}


def get_sector(symbol: str) -> str:
    """Look up the sector for a stock symbol from the F&O watchlist mapping.
    Any symbol not in the list (new listings, non-F&O stocks, test data)
    falls back to 'Unclassified' rather than breaking the page."""
    return SECTOR_MAP.get((symbol or "").strip().upper(), "Unclassified")


# The full, static universe of sectors this app knows about - independent of
# which sectors happen to have an alert today. Used to show every sector in
# the sidebar (with today's % change) rather than only sectors that alerted.
ALL_SECTOR_NAMES = sorted(set(SECTOR_MAP.values()))


def ist_date_str(created_at_iso: str) -> str:
    """Convert a stored UTC timestamp to its IST calendar date (YYYY-MM-DD),
    so 'today' matches the Indian trading day rather than the UTC day."""
    try:
        dt = datetime.fromisoformat(created_at_iso)
    except (ValueError, TypeError):
        return ""
    return (dt + IST_OFFSET).strftime("%Y-%m-%d")


def categorize(alert: dict) -> str:
    """Group an alert into Sell / Buy / Others based on the scan name (and
    alert name as a fallback) Chartink assigned to it."""
    text = f"{alert.get('scan_name', '')} {alert.get('alert_name', '')}".lower()
    if "sell" in text:
        return "Sell"
    if "buy" in text:
        return "Buy"
    return "Others"


CATEGORY_ORDER = ["Sell", "Buy", "Others"]


def merge_duplicate_symbols(items: list[dict]) -> list[dict]:
    """Merge alerts for the same stock (within one category) into a single
    card. When two different scans both flag the same stock - e.g.
    'Rajaram369-Buy' and the RSI-combined scan - that's one stronger signal,
    not two separate things to show, so it becomes one card listing both
    scan names instead of a duplicate card per scan."""
    groups: dict[str, list[dict]] = {}
    for a in items:
        groups.setdefault(a.get("symbol", ""), []).append(a)

    merged = []
    for symbol, group in groups.items():
        seen_scans = set()
        scan_names = []
        alert_name = ""
        for a in group:
            sn = a.get("scan_name", "")
            if sn not in seen_scans:
                seen_scans.add(sn)
                scan_names.append(sn)
            if not alert_name and a.get("alert_name"):
                alert_name = a["alert_name"]

        newest = group[0]  # group[0] is newest since input is newest-first
        combined = dict(newest)
        combined["alert_name"] = alert_name
        combined["scan_names"] = scan_names
        combined["confirmed_count"] = len(scan_names)
        merged.append(combined)
    return merged


def group_by_category(alerts: list[dict]) -> list[tuple[str, list[dict]]]:
    """Split alerts into Sell / Buy / Others sections, in that fixed order,
    merging duplicate stocks within each section, skipping any section that
    has no alerts."""
    buckets = {name: [] for name in CATEGORY_ORDER}
    for alert in alerts:
        buckets[categorize(alert)].append(alert)
    result = []
    for name in CATEGORY_ORDER:
        if buckets[name]:
            result.append((name, merge_duplicate_symbols(buckets[name])))
    return result


class PGConnWrapper:
    """Makes a psycopg2 connection usable the same way the rest of this
    file already uses sqlite3 connections: conn.execute(sql, params) returns
    a cursor with .fetchall()/.fetchone(), and rows support row["column"]
    access via RealDictCursor. This lets every existing query in this file
    work unchanged - only this class and init_db()'s schema needed to
    change for the Postgres move."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, query, params=None):
        pg_query = query.replace("?", "%s")  # sqlite-style -> psycopg2-style
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(pg_query, params or ())
        return cur

    def commit(self):
        self._conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._conn.close()
        return False


def get_db():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set - add your Neon "
            "connection string in Render's Environment settings."
        )
    conn = psycopg2.connect(DATABASE_URL)
    return PGConnWrapper(conn)


def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                batch_id TEXT,
                symbol TEXT,
                trigger_price TEXT,
                scan_name TEXT,
                scan_url TEXT,
                alert_name TEXT,
                triggered_at TEXT,
                raw_payload TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_trades (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                direction TEXT,
                entry_price DOUBLE PRECISION,
                entry_time TEXT,
                status TEXT DEFAULT 'OPEN',
                exit_price DOUBLE PRECISION,
                exit_time TEXT,
                pnl DOUBLE PRECISION,
                exit_reason TEXT,
                last_checked_price DOUBLE PRECISION,
                last_checked_time TEXT,
                last_error TEXT,
                quantity INTEGER DEFAULT 0,
                pnl_pct DOUBLE PRECISION,
                live_status TEXT DEFAULT 'NONE',
                live_entry_order_id TEXT,
                live_exit_order_id TEXT,
                live_error TEXT,
                live_quantity INTEGER,
                live_instrument_key TEXT,
                live_option_label TEXT,
                paper_instrument_key TEXT,
                paper_option_label TEXT,
                strategy TEXT DEFAULT 'EMA',
                entry_reason TEXT,
                original_quantity INTEGER,
                target1_hit_time TEXT,
                target1_exit_price DOUBLE PRECISION,
                target1_qty INTEGER,
                target1_pnl DOUBLE PRECISION,
                trail_high_pct DOUBLE PRECISION,
                live_trail_high_pct DOUBLE PRECISION,
                live_exit_reason TEXT,
                live_exit_price DOUBLE PRECISION,
                live_entry_price DOUBLE PRECISION,
                live_exit_time TEXT,
                live_sl_order_id TEXT,
                live_sl_trigger_price DOUBLE PRECISION,
                atr_trail_peak_price DOUBLE PRECISION
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sector_prev_close (
                symbol TEXT PRIMARY KEY,
                close DOUBLE PRECISION,
                date TEXT
            )
            """
        )
        # ADD COLUMN IF NOT EXISTS so existing Render/Neon databases (created
        # before live trading existed) pick up the new columns without
        # needing a manual migration.
        for stmt in (
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_status TEXT DEFAULT 'NONE'",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_entry_order_id TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_exit_order_id TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_error TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_quantity INTEGER",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_instrument_key TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_option_label TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS paper_instrument_key TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS paper_option_label TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS strategy TEXT DEFAULT 'EMA'",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS entry_reason TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS original_quantity INTEGER",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS target1_hit_time TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS target1_exit_price DOUBLE PRECISION",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS target1_qty INTEGER",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS target1_pnl DOUBLE PRECISION",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS trail_high_pct DOUBLE PRECISION",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_trail_high_pct DOUBLE PRECISION",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_exit_reason TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_exit_price DOUBLE PRECISION",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_entry_price DOUBLE PRECISION",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_exit_time TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_sl_order_id TEXT",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS live_sl_trigger_price DOUBLE PRECISION",
            "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS atr_trail_peak_price DOUBLE PRECISION",
        ):
            conn.execute(stmt)
        conn.commit()


DEFAULT_CAPITAL = 100000.0


def get_capital() -> float:
    value = get_setting("paper_trade_capital", str(DEFAULT_CAPITAL))
    try:
        return float(value)
    except (ValueError, TypeError):
        return DEFAULT_CAPITAL


def get_live_trading_enabled() -> bool:
    """Master safety switch for placing real orders on Upstox. Defaults to
    OFF - live orders only fire once this is explicitly turned on via
    /api/paper-trading/live-toggle, so the app can never start placing real
    trades just because the code was deployed."""
    return get_setting("live_trading_enabled", "false") == "true"


def get_exit_strategy() -> str:
    """Which exit strategy new trades use. Defaults to TRAIL_FROM_2 (hard
    -2% stop-loss, full quantity trails from +2% in 0.2% steps) since
    'EMA' was retired from selection - it let losses run too deep waiting
    for a pattern reversal to confirm, instead of a hard stop."""
    return get_setting("exit_strategy", "TRAIL_FROM_2")


def get_sector_filter_enabled() -> bool:
    """Off by default - when on, an alert only opens a trade if its sector
    is currently among the top/bottom N performing sectors (Buy needs a
    top-performing sector, Sell needs a bottom-performing one)."""
    return get_setting("sector_filter_enabled", "false") == "true"


def get_buy_alerts_enabled() -> bool:
    """On by default - when off, Buy alerts still show up on the dashboard
    (the raw alert feed is untouched) but never open a paper or live
    trade. Lets the user favor one side based on market sentiment without
    losing visibility into what's firing."""
    return get_setting("buy_alerts_enabled", "true") == "true"


def get_sell_alerts_enabled() -> bool:
    """Same as get_buy_alerts_enabled() but for Sell alerts."""
    return get_setting("sell_alerts_enabled", "true") == "true"


def get_poc_filter_enabled() -> bool:
    """Off by default - when on, an alert only opens a trade if the
    option's own price has closed on the correct side of its Volume
    Profile POC (above for a Call/Buy alert, below for a Put/Sell alert),
    computed over today's session so far."""
    return get_setting("poc_filter_enabled", "false") == "true"


def get_sector_filter_top_n() -> int:
    try:
        return int(get_setting("sector_filter_top_n", "5"))
    except (TypeError, ValueError):
        return 5


def get_entry_time_filter() -> str | None:
    """Earliest clock time (IST, 'HH:MM') new trades are allowed to open,
    e.g. '09:45' - alerts before this are simply skipped, same as if the
    sector filter or a single-position lock rejected them. Empty/unset
    means no filter (any time is fine). Stored as plain 'HH:MM' text so
    it sorts/compares as a string against the current IST time cleanly."""
    value = (get_setting("entry_time_filter", "") or "").strip()
    return value or None


def is_after_entry_time_filter() -> bool:
    """True if either no entry-time filter is set, or the current IST
    clock time is at/after the configured cutoff."""
    cutoff = get_entry_time_filter()
    if not cutoff:
        return True
    now_hhmm = (datetime.utcnow() + IST_OFFSET).strftime("%H:%M")
    return now_hhmm >= cutoff


def get_atr_period() -> int:
    """Lookback period (in 5-min candles) for the ATR_TRAIL exit
    strategy's Average True Range calculation. Defaults to 10."""
    try:
        return max(2, int(get_setting("atr_period", "10")))
    except (TypeError, ValueError):
        return 10


def get_atr_multiplier() -> float:
    """Multiplier applied to ATR for the ATR_TRAIL exit strategy's
    Chandelier-style trailing stop: stop = highest price since entry -
    (ATR * multiplier). Defaults to 2.0."""
    try:
        return max(0.1, float(get_setting("atr_multiplier", "2")))
    except (TypeError, ValueError):
        return 2.0


def sector_qualifies(symbol: str, category: str, access_token: str | None) -> bool:
    """True if this alert's sector currently ranks strongly enough to take
    the trade: a Buy alert needs its sector in the top N by % change, a
    Sell alert needs its sector in the bottom N (most negative). Returns
    False (skip the alert) if sector performance data isn't available yet
    or ranks aren't computable - never opens a trade blind when the filter
    is on but the data isn't ready."""
    if not access_token:
        return False
    perf = get_sector_performance_cached(access_token)
    if not perf:
        return False
    sector = get_sector(symbol)
    ranked = sorted(perf.items(), key=lambda kv: kv[1]["pct_change"], reverse=True)
    top_n = get_sector_filter_top_n()
    top_sectors = {name for name, _ in ranked[:top_n]}
    bottom_sectors = {name for name, _ in ranked[-top_n:]}
    if category == "Buy":
        return sector in top_sectors
    else:
        return sector in bottom_sectors


def get_setting(key: str, default: str | None = None) -> str | None:
    with get_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()


def save_alert_batch(data: dict):
    """Chartink sends one alert per scan trigger, but it can list several
    stocks at once as comma-separated strings. Split them into one row per
    stock so each shows as its own card."""
    stocks = [s.strip() for s in str(data.get("stocks", "")).split(",") if s.strip()]
    prices = [p.strip() for p in str(data.get("trigger_prices", "")).split(",") if p.strip()]

    if not stocks:
        # Fall back gracefully for a hand-sent test payload that only has
        # a single "symbol" field instead of Chartink's real format.
        stocks = [data.get("symbol", "UNKNOWN")]
        prices = [str(data.get("price", ""))]

    batch_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat()
    raw_payload = json.dumps(data, ensure_ascii=False)

    with get_db() as conn:
        for i, symbol in enumerate(stocks):
            price = prices[i] if i < len(prices) else ""
            conn.execute(
                """
                INSERT INTO alerts
                    (batch_id, symbol, trigger_price, scan_name, scan_url,
                     alert_name, triggered_at, raw_payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    symbol,
                    price,
                    data.get("scan_name", ""),
                    data.get("scan_url", ""),
                    data.get("alert_name", ""),
                    data.get("triggered_at", ""),
                    raw_payload,
                    created_at,
                ),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# Paper trading
# ---------------------------------------------------------------------------

def get_current_capital() -> float:
    """The account balance available for the next trade: the starting
    capital plus every closed trade's P&L so far, compounding - this is
    what actually answers 'how would 1 lakh grow over the month'."""
    base = get_capital()
    with get_db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) AS total FROM paper_trades WHERE status = 'CLOSED'"
        ).fetchone()
    return base + (row["total"] or 0)


def _mark_live_failed(trade_id: int, error: str, live_quantity: int | None = None) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE paper_trades SET live_status = 'FAILED', live_error = ?, "
            "live_quantity = COALESCE(?, live_quantity) WHERE id = ?",
            (error, live_quantity, trade_id),
        )
        conn.commit()


def create_paper_trades_for_batch(data: dict) -> None:
    """Single-position paper trading: only one trade is ever open at a
    time, using the full current account balance. A new alert only starts
    a trade if nothing is currently open - it does NOT stack multiple
    simultaneous positions. This simulates actually growing one pool of
    capital sequentially, trade after trade, rather than evaluating every
    signal independently."""
    category = categorize(data)
    if category not in ("Buy", "Sell"):
        return

    # Per-side kill switch - lets the user favor Buy-only or Sell-only
    # based on market sentiment. The alert itself still shows on the
    # dashboard (saved separately in the webhook handler) - only trade
    # creation is skipped here.
    if category == "Buy" and not get_buy_alerts_enabled():
        return
    if category == "Sell" and not get_sell_alerts_enabled():
        return

    # Entry-time filter - lets the user skip the noisier opening minutes
    # entirely and only let the system open trades from a chosen clock
    # time onward (e.g. 09:45). Off by default (no filter).
    if not is_after_entry_time_filter():
        return

    with get_db() as conn:
        already_open = conn.execute(
            "SELECT id FROM paper_trades WHERE status = 'OPEN' LIMIT 1"
        ).fetchone()
    if already_open:
        return  # a trade is already running - wait for it to exit first

    stocks = [s.strip() for s in str(data.get("stocks", "")).split(",") if s.strip()]
    prices = [p.strip() for p in str(data.get("trigger_prices", "")).split(",") if p.strip()]
    if not stocks:
        stocks = [data.get("symbol", "UNKNOWN")]
        prices = [str(data.get("price", ""))]

    # Only the first valid stock in this alert becomes the trade - capital
    # is fully committed to one position at a time, not split further.
    symbol = None
    price_val = None
    for i, s in enumerate(stocks):
        p = prices[i] if i < len(prices) else ""
        try:
            candidate = float(p)
        except (ValueError, TypeError):
            continue
        if candidate > 0:
            symbol, price_val = s, candidate
            break
    if symbol is None:
        return

    open_trade_for_symbol(symbol, category, price_val)


def open_trade_for_symbol(symbol: str, category: str, price_val: float) -> None:
    """Opens exactly one paper trade (and, if live trading is on, a
    matching live order) for a single already-resolved symbol/category/
    price. This is the shared core used by both the normal webhook
    alert flow (create_paper_trades_for_batch) and the manual 'missed
    alert' Buy button on the dashboard (api_manual_enter_alert).
    Callers are responsible for their own already-open-trade / entry-
    time / buy-sell-enabled checks first - this function only applies
    the per-trade qualification checks (sector filter, POC filter)."""
    # Paper trading now simulates the actual ATM OPTION this alert would
    # buy live (Call for Buy, Put for Sell) - not the equity - so it's a
    # true preview of the live strategy: premium as entry price, quantity
    # in whole lots sized off the paper capital pool. Falls back to the
    # old equity-based simulation only if option data genuinely isn't
    # available (no token, no chain, no premium), so paper trading never
    # just silently does nothing.
    opt_type = "CE" if category == "Buy" else "PE"
    access_token = get_setting("upstox_access_token")

    # Optional sector-momentum filter: only take this alert if its sector
    # currently ranks strongly enough (Buy needs a top-N sector, Sell needs
    # a bottom-N one). Off by default; when on, a non-qualifying alert is
    # skipped entirely - no paper trade opens, next alert gets a chance.
    if get_sector_filter_enabled() and not sector_qualifies(symbol, category, access_token):
        return

    option = get_atm_option(symbol, opt_type, price_val) if access_token else None
    premium = get_ltp(option["instrument_key"], access_token) if option else None

    paper_instrument_key = None
    paper_option_label = None
    fallback_note = None
    entry_reason = None
    if option and premium and premium > 0:
        capital = get_current_capital()
        lot_size = option["lot_size"]
        lots = max(1, int(capital // (premium * lot_size)))
        quantity = lots * lot_size
        entry_price = premium
        paper_instrument_key = option["instrument_key"]
        paper_option_label = f"{symbol} {option['strike']:g} {opt_type} exp {option['expiry']}"

        if get_poc_filter_enabled():
            poc_ok, poc_reason = poc_qualifies(option["instrument_key"], category, premium, access_token)
            if not poc_ok:
                return  # doesn't qualify - skip this alert entirely, no paper or live trade
            entry_reason = poc_reason
    else:
        capital = get_current_capital()
        quantity = int(capital // price_val) or 1
        entry_price = price_val
        if not access_token:
            fallback_note = "No Upstox token saved - fell back to equity paper trading"
        elif not option:
            fallback_note = f"No {opt_type} option chain data for {symbol} - fell back to equity paper trading"
        else:
            fallback_note = f"Could not fetch option premium for {symbol} {opt_type} - fell back to equity paper trading"

    now = datetime.utcnow().isoformat()
    entry_strategy = get_exit_strategy()
    with get_db() as conn:
        row = conn.execute(
            """
            INSERT INTO paper_trades
                (symbol, direction, entry_price, entry_time, status, quantity,
                 paper_instrument_key, paper_option_label, last_error,
                 strategy, original_quantity)
            VALUES (?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (symbol, category, entry_price, now, quantity, paper_instrument_key, paper_option_label,
             fallback_note, entry_strategy, quantity),
        ).fetchone()
        conn.commit()

    if access_token:
        ensure_websocket_started(access_token)
    if paper_instrument_key:
        update_ws_subscriptions(_ws_subscribed_keys | {paper_instrument_key})

    # Live trading buys ATM OPTIONS, never the underlying equity: a Buy
    # alert buys the ATM Call, a Sell alert buys the ATM Put - both are
    # always a BUY transaction (never writes/shorts an option), only the
    # option TYPE differs by alert direction. Any failure here is recorded
    # on the trade and never blocks the paper trade itself.
    if get_live_trading_enabled():
        opt_type = "CE" if category == "Buy" else "PE"
        trade_id = row["id"]
        access_token = get_setting("upstox_access_token")
        if not access_token:
            _mark_live_failed(trade_id, "Live trading is on but no Upstox access token is saved")
        else:
            option = get_atm_option(symbol, opt_type, price_val)
            if not option:
                _mark_live_failed(trade_id, f"No {opt_type} option chain data found for {symbol} (nearest monthly expiry)")
            else:
                premium = get_ltp(option["instrument_key"], access_token)
                if not premium or premium <= 0:
                    _mark_live_failed(trade_id, f"Could not fetch option premium for {symbol} {opt_type}")
                else:
                    available_funds = get_upstox_available_funds(access_token)
                    if available_funds is None:
                        _mark_live_failed(trade_id, "Could not fetch available Upstox funds - live order skipped")
                    else:
                        lot_size = option["lot_size"]
                        lots = int(available_funds // (premium * lot_size))
                        label = f"{symbol} {option['strike']:g} {opt_type} exp {option['expiry']}"
                        if lots < 1:
                            _mark_live_failed(
                                trade_id,
                                f"Available funds (Rs {available_funds:.2f}) not enough for 1 lot "
                                f"({lot_size} qty) of {label} at premium Rs {premium}",
                                live_quantity=0,
                            )
                        else:
                            live_quantity = lots * lot_size
                            result = place_live_order(option["instrument_key"], "BUY", live_quantity, access_token)
                            if result["ok"]:
                                # Fetch the REAL fill price - this is what
                                # live P&L must be based on, not the
                                # theoretical premium we used to size the
                                # order (see get_order_average_price).
                                live_entry_price = get_order_average_price(result["order_id"], access_token)

                                # Immediately place a broker-side SL-M stop
                                # at -2% from the real fill price. This is
                                # what actually protects the position if
                                # this app, the server, or the network goes
                                # down - Upstox's own engine fires it, not
                                # our polling loop. Best-effort: a failure
                                # here doesn't fail the trade (the position
                                # is already live and open), it's just
                                # recorded so it's visible that this
                                # position is running without a broker-side
                                # net and needs the app's own monitoring.
                                live_sl_order_id = None
                                live_sl_trigger_price = None
                                sl_note = None
                                if live_entry_price:
                                    live_sl_trigger_price = round(live_entry_price * 0.98, 1)
                                    sl_result = place_live_order(
                                        option["instrument_key"], "SELL", live_quantity, access_token,
                                        order_type="SL-M", trigger_price=live_sl_trigger_price,
                                    )
                                    if sl_result["ok"]:
                                        live_sl_order_id = sl_result["order_id"]
                                    else:
                                        sl_note = f"Entry filled but broker SL-M stop failed to place: {sl_result['error']}"
                                else:
                                    sl_note = "Entry filled but no fill price yet - broker SL-M stop skipped, app-side monitoring only"

                                with get_db() as conn:
                                    conn.execute(
                                        """
                                        UPDATE paper_trades
                                        SET live_status = 'OPEN', live_entry_order_id = ?, live_quantity = ?,
                                            live_instrument_key = ?, live_option_label = ?, live_entry_price = ?,
                                            live_sl_order_id = ?, live_sl_trigger_price = ?, live_error = ?
                                        WHERE id = ?
                                        """,
                                        (result["order_id"], live_quantity, option["instrument_key"], label,
                                         live_entry_price, live_sl_order_id, live_sl_trigger_price, sl_note, trade_id),
                                    )
                                    conn.commit()
                            else:
                                with get_db() as conn:
                                    conn.execute(
                                        """
                                        UPDATE paper_trades
                                        SET live_status = 'FAILED', live_error = ?, live_quantity = ?,
                                            live_option_label = ?
                                        WHERE id = ?
                                        """,
                                        (result["error"], live_quantity, label, trade_id),
                                    )
                                    conn.commit()


def calculate_ema(values: list[float], period: int = 5) -> list[float | None]:
    """Seeded with a simple average of the first `period` values, then the
    usual exponential smoothing after that. Same length as `values`, with
    None for the seeding gap at the start."""
    n = len(values)
    if n < period:
        return [None] * n
    k = 2 / (period + 1)
    ema: list[float | None] = [None] * (period - 1)
    seed = sum(values[:period]) / period
    ema.append(seed)
    for price in values[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema


def calculate_atr(candles: list[tuple[float, float, float, float]], period: int = 10) -> list[float | None]:
    """Average True Range using Wilder's smoothing (the standard
    definition - a plain moving average of True Range understates
    volatility spikes). candles: list of (open, high, low, close), oldest
    first. Same length as `candles`, with None for the seeding gap at the
    start (needs `period` True Range values before the first ATR reading
    exists, which itself needs one prior candle for the first True Range -
    so `period + 1` candles minimum)."""
    n = len(candles)
    if n < period + 1:
        return [None] * n

    true_ranges: list[float | None] = [None]
    for i in range(1, n):
        high, low = candles[i][1], candles[i][2]
        prev_close = candles[i - 1][3]
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))

    atr: list[float | None] = [None] * period
    seed = sum(true_ranges[1:period + 1]) / period
    atr.append(seed)
    for i in range(period + 1, n):
        atr.append((atr[-1] * (period - 1) + true_ranges[i]) / period)
    return atr


def _floor_to_step(value: float, step: float) -> float:
    """Floor `value` down to the nearest multiple of `step`. Uses a tiny
    epsilon so binary float imprecision doesn't clip a value that should
    land exactly on a step (e.g. 0.6 landing on 2 steps of 0.2 instead of
    3, because 0.6 / 0.2 comes out as 2.9999999999999996 in float math)."""
    return round(math.floor(value / step + 1e-9) * step, 2)


def check_exit(direction: str, candles: list[tuple[float, float, float, float]]):
    """candles: list of (open, high, low, close), oldest first.
    Buy exit: 2 consecutive red candles (close < open) closing below the
              EMA(5) of the LOW price series.
    Sell exit: 2 consecutive green candles (close > open) closing above the
               EMA(5) of the HIGH price series.
    Returns (exited: bool, exit_price: float | None)."""
    if len(candles) < 7:  # EMA(5) needs 5 to seed, plus 2 confirming candles
        return False, None

    opens = [c[0] for c in candles]
    highs = [c[1] for c in candles]
    lows = [c[2] for c in candles]
    closes = [c[3] for c in candles]

    if direction == "Buy":
        ema_line = calculate_ema(lows, 5)
        for i in (-1, -2):
            if ema_line[i] is None:
                return False, None
            if not (closes[i] < opens[i] and closes[i] < ema_line[i]):
                return False, None
        return True, closes[-1]
    else:
        ema_line = calculate_ema(highs, 5)
        for i in (-1, -2):
            if ema_line[i] is None:
                return False, None
            if not (closes[i] > opens[i] and closes[i] > ema_line[i]):
                return False, None
        return True, closes[-1]


_instrument_cache: dict[str, str] = {}
_instrument_cache_date: str | None = None
_instrument_debug: dict = {}

# underlying symbol -> list of {"strike", "opt_type", "expiry" (YYYY-MM-DD),
# "instrument_key", "lot_size"} for every CE/PE contract on that underlying.
_option_chain_cache: dict[str, list[dict]] = {}
_option_debug: dict = {}

UPSTOX_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"


def _parse_expiry(raw) -> str | None:
    """Upstox's CSV has been seen to store expiry either as an epoch-ms
    timestamp or as a plain date string, depending on export version -
    handle both rather than assuming one."""
    if not raw:
        return None
    raw = str(raw).strip()
    if not raw:
        return None
    if raw.isdigit():
        try:
            ms = int(raw)
            # heuristic: 13-digit numbers are ms, 10-digit are seconds
            seconds = ms / 1000 if len(raw) >= 13 else ms
            return datetime.utcfromtimestamp(seconds).strftime("%Y-%m-%d")
        except Exception:
            return None
    # already a date-like string, e.g. "2026-08-27"
    return raw[:10]


def _load_instrument_master() -> None:
    """Downloads Upstox's public instrument master file and builds two
    lookups in a single pass: a trading-symbol -> instrument_key map for
    NSE equities, and an underlying-symbol -> option-contract-list map for
    NSE F&O options (CE/PE). Cached for the day since it's a large file and
    doesn't change intraday.

    NOTE: the F&O column names (name/strike/expiry/lot_size) are Upstox's
    documented ones but haven't been confirmed against the live file yet -
    check /api/paper-trading/debug-instruments after the first real fetch
    and fix the column names there if option_matched_count is 0."""
    global _instrument_cache, _instrument_cache_date, _instrument_debug
    global _option_chain_cache, _option_debug
    try:
        req = urllib.request.Request(UPSTOX_INSTRUMENTS_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            content_type = resp.headers.get("Content-Type", "")
            raw_bytes = resp.read()

        # Try gzip first (expected), but fall back to plain text in case
        # the URL now serves an uncompressed file.
        try:
            raw = gzip.decompress(raw_bytes)
        except OSError:
            raw = raw_bytes

        text = raw.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        fieldnames = reader.fieldnames or []
        sample_rows = []
        fo_sample_rows = []
        mapping = {}
        name_to_ticker: dict[str, str] = {}
        pending_options = []
        row_count = 0
        for i, row in enumerate(reader):
            row_count = i + 1
            if i < 3:
                sample_rows.append(dict(row))
            exch = (row.get("exchange") or "").upper()
            itype = (row.get("instrument_type") or "").upper()
            opt_type = (row.get("option_type") or "").upper()
            tsym = (row.get("tradingsymbol") or row.get("trading_symbol") or "").upper()
            ikey = row.get("instrument_key") or ""
            if exch == "NSE_EQ" and itype == "EQUITY" and tsym and ikey:
                mapping[tsym] = ikey
                name = (row.get("name") or "").upper()
                if name:
                    name_to_ticker[name] = tsym
            elif exch == "NSE_FO" and opt_type in ("CE", "PE") and ikey:
                if len(fo_sample_rows) < 3:
                    fo_sample_rows.append(dict(row))
                pending_options.append({
                    "name": (row.get("name") or row.get("asset_symbol") or "").upper(),
                    "strike_raw": row.get("strike") or row.get("strike_price"),
                    "expiry_raw": row.get("expiry"),
                    "lot_raw": row.get("lot_size"),
                    "opt_type": opt_type,
                    "instrument_key": ikey,
                })

        # Option rows store the underlying under its descriptive company
        # name (e.g. "ORACLE FIN SERV SOFT LTD."), not its trading symbol -
        # so resolve each one against the name->ticker map just built from
        # the equity rows. Index options (e.g. "MIDCPNIFTY") won't have a
        # matching equity name, so those fall back to using the raw name
        # as-is, which works as long as it matches how that index is keyed
        # elsewhere (e.g. in SECTOR_MAP).
        option_chains: dict[str, list[dict]] = {}
        option_row_count = 0
        unresolved_names_sample = []
        for opt in pending_options:
            underlying = name_to_ticker.get(opt["name"], opt["name"])
            if opt["name"] and opt["name"] not in name_to_ticker and len(unresolved_names_sample) < 5:
                unresolved_names_sample.append(opt["name"])
            try:
                strike = float(opt["strike_raw"]) if opt["strike_raw"] not in (None, "") else None
            except (TypeError, ValueError):
                strike = None
            try:
                lot_size = int(float(opt["lot_raw"])) if opt["lot_raw"] not in (None, "") else None
            except (TypeError, ValueError):
                lot_size = None
            expiry = _parse_expiry(opt["expiry_raw"])
            if underlying and strike and expiry and lot_size and opt["instrument_key"]:
                option_row_count += 1
                option_chains.setdefault(underlying, []).append({
                    "strike": strike,
                    "opt_type": opt["opt_type"],
                    "expiry": expiry,
                    "instrument_key": opt["instrument_key"],
                    "lot_size": lot_size,
                })

        _instrument_cache = mapping
        _option_chain_cache = option_chains
        _instrument_cache_date = (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d")
        _instrument_debug = {
            "ok": True,
            "content_type": content_type,
            "raw_bytes_len": len(raw_bytes),
            "decompressed_len": len(raw),
            "fieldnames": fieldnames,
            "sample_rows": sample_rows,
            "total_rows_scanned": row_count,
            "matched_count": len(mapping),
            "error": None,
        }
        _option_debug = {
            "ok": True,
            "fo_sample_rows": fo_sample_rows,
            "option_rows_matched": option_row_count,
            "underlyings_with_options": len(option_chains),
            "sample_underlyings": list(option_chains.keys())[:5],
            "name_to_ticker_size": len(name_to_ticker),
            "unresolved_names_sample": unresolved_names_sample,
            "reliance_resolved": "RELIANCE" in option_chains,
        }
    except Exception as e:
        import traceback
        _instrument_cache = {}
        _option_chain_cache = {}
        _instrument_debug = {
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }
        _option_debug = {"ok": False, "error": str(e)}


def get_instrument_key(symbol: str) -> str | None:
    today = (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d")
    if _instrument_cache_date != today or not _instrument_cache:
        _load_instrument_master()
    return _instrument_cache.get((symbol or "").upper())


def get_theta_switch_enabled() -> bool:
    """On by default - when on, once the nearest monthly expiry crosses
    into the back (higher-decay) half of its own trading-day cycle,
    get_atm_option rolls new trades to the NEXT monthly expiry instead.
    Rationale: theta decay is non-linear - the back half of a monthly
    cycle bleeds extrinsic value far faster than the front half, so
    staying in the current month during that window means theta (not
    just being wrong on direction) eats the position. See the analysis
    that led to this feature for the full reasoning."""
    return get_setting("theta_switch_enabled", "true") == "true"


def _count_weekdays_inclusive(start_date, end_date) -> int:
    """Counts Mon-Fri days in [start_date, end_date], inclusive of both
    ends. Used as a practical stand-in for NSE trading days, since this
    app doesn't maintain a full exchange holiday calendar - this can be
    off by a day or two around a holiday, which matters far less than
    which half of the expiry cycle a trade actually sits in."""
    if end_date < start_date:
        return 0
    days = 0
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:
            days += 1
        d += timedelta(days=1)
    return days


def get_atm_option(symbol: str, opt_type: str, reference_price: float) -> dict | None:
    """Finds the ATM (closest-strike) contract for this underlying's
    nearest MONTHLY expiry - or the NEXT monthly expiry instead, once
    get_theta_switch_enabled() is on and the nearest one has crossed into
    the back (high-decay) half of its own trading-day cycle. A monthly
    expiry is identified as the latest expiry date that falls within a
    given calendar month among all this underlying's expiries (matches
    how NSE's monthly contracts work: the last expiry of the month is
    the monthly one, the rest are weeklies). Returns None if no option
    chain data is available for this symbol."""
    today_str = (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d")
    today_date = (datetime.utcnow() + IST_OFFSET).date()
    if _instrument_cache_date != today_str or not _option_chain_cache:
        _load_instrument_master()
    contracts = _option_chain_cache.get((symbol or "").upper())
    if not contracts:
        return None

    # Monthly expiries computed from the FULL contract list (not just
    # expiries >= today) - the cycle-start calculation below needs the
    # PREVIOUS monthly expiry too, which is necessarily in the past.
    monthly_by_month: dict[str, str] = {}
    for c in contracts:
        exp = c["expiry"]
        month_key = exp[:7]  # "YYYY-MM"
        if month_key not in monthly_by_month or exp > monthly_by_month[month_key]:
            monthly_by_month[month_key] = exp
    all_monthlies = sorted(monthly_by_month.values())
    upcoming_monthlies = [m for m in all_monthlies if m >= today_str]
    if not upcoming_monthlies:
        return None

    nearest_monthly = upcoming_monthlies[0]
    target_expiry = nearest_monthly

    if get_theta_switch_enabled():
        idx = all_monthlies.index(nearest_monthly)
        prev_monthly = all_monthlies[idx - 1] if idx > 0 else None
        next_monthly = all_monthlies[idx + 1] if idx + 1 < len(all_monthlies) else None

        if next_monthly:
            nearest_date = datetime.strptime(nearest_monthly, "%Y-%m-%d").date()
            if prev_monthly:
                cycle_start = datetime.strptime(prev_monthly, "%Y-%m-%d").date() + timedelta(days=1)
            else:
                # No earlier monthly expiry on record (e.g. right at the
                # start of this instrument's data) - fall back to a
                # ~22-trading-day cycle length assumption (~30 calendar
                # days), rather than skip the check entirely.
                cycle_start = nearest_date - timedelta(days=30)

            total_trading_days = _count_weekdays_inclusive(cycle_start, nearest_date)
            trading_days_remaining = _count_weekdays_inclusive(today_date, nearest_date)

            if total_trading_days > 0 and trading_days_remaining <= total_trading_days / 2:
                target_expiry = next_monthly

    candidates = [c for c in contracts if c["expiry"] == target_expiry and c["opt_type"] == opt_type]
    if not candidates:
        return None
    best = min(candidates, key=lambda c: abs(c["strike"] - reference_price))
    return best


_sector_perf_cache: dict = {"data": None, "fetched_at": None}
_sector_perf_debug: dict = {"ok": None, "error": None}
SECTOR_PERF_CACHE_SECONDS = 5

# Upstox's live quote/OHLC endpoints only return TODAY's still-in-progress
# OHLC for a 1d interval (confirmed in their own docs: "For a time interval
# of 1d, the API returns only the live_ohlc... Previous day OHLC data is
# available in Historical Candle Data"). So previous close has to come from
# the separate historical-candle endpoint instead - and since that's one
# call per instrument (not batchable), it's fetched once per calendar day
# in a background thread and cached, not on every poll.
_prev_close_cache: dict = {"date": None, "closes": {}}
_prev_close_loading = False
IST_OFFSET = timedelta(hours=5, minutes=30)


def _ist_today_str() -> str:
    return (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d")


def _load_prev_closes_background(access_token: str) -> None:
    """Runs in a background thread: loops every mapped symbol and fetches
    its most recent completed daily candle's close via the Historical
    Candle Data API. This is ~180 sequential calls, so it's kept off the
    request/response path entirely - callers just see stale/partial data
    in _prev_close_cache until this finishes. Each symbol's close is
    written to the DB as soon as it's fetched (not just at the end), so a
    Render cold-start or redeploy mid-load never throws away progress
    already made today."""
    global _prev_close_cache, _prev_close_loading
    today = _ist_today_str()
    from_date = (datetime.utcnow() + IST_OFFSET - timedelta(days=10)).strftime("%Y-%m-%d")
    closes: dict[str, float] = dict(_prev_close_cache["closes"])  # keep anything already loaded today
    for symbol in SECTOR_MAP:
        if symbol in closes:
            continue  # already have it (from DB or an earlier partial run today)
        key = get_instrument_key(symbol)
        if not key:
            continue
        url = (
            f"https://api.upstox.com/v2/historical-candle/"
            f"{urllib.parse.quote(key, safe='|')}/day/{today}/{from_date}"
        )
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "User-Agent": BROWSER_USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())
            candles = (payload.get("data") or {}).get("candles") or []
            # Candles come back newest-first; the first entry is the most
            # recent COMPLETED trading day (today's still-open session
            # isn't included here), so its close is what we want.
            if candles:
                close = candles[0][4]
                closes[symbol] = close
                _prev_close_cache = {"date": today, "closes": dict(closes)}
                with get_db() as conn:
                    conn.execute(
                        "INSERT INTO sector_prev_close (symbol, close, date) VALUES (?, ?, ?) "
                        "ON CONFLICT(symbol) DO UPDATE SET close = excluded.close, date = excluded.date",
                        (symbol, close, today),
                    )
                    conn.commit()
        except Exception:
            continue
    _prev_close_cache = {"date": today, "closes": closes}
    _prev_close_loading = False


def _load_prev_closes_from_db() -> dict[str, float]:
    """Loads today's already-persisted previous closes from Postgres - this
    is what makes the data survive a Render cold-start or redeploy, since
    it doesn't depend on any process having stayed alive since the load."""
    today = _ist_today_str()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT symbol, close FROM sector_prev_close WHERE date = ?", (today,)
        ).fetchall()
    return {row["symbol"]: row["close"] for row in rows}


def _get_prev_closes(access_token: str) -> dict[str, float]:
    """Returns today's previous-close map. Checks the DB first (covers a
    fresh process picking up progress an earlier process already made
    today), then kicks off a background refresh (non-blocking) for
    whatever's still missing."""
    global _prev_close_loading, _prev_close_cache
    today = _ist_today_str()
    if _prev_close_cache["date"] != today:
        # New process, or a new day - seed from whatever's already in the
        # DB from today before deciding whether a background load is needed.
        _prev_close_cache = {"date": today, "closes": _load_prev_closes_from_db()}
    if len(_prev_close_cache["closes"]) < len(SECTOR_MAP) and not _prev_close_loading:
        _prev_close_loading = True
        threading.Thread(target=_load_prev_closes_background, args=(access_token,), daemon=True).start()
    return _prev_close_cache["closes"]


def _fetch_sector_performance(access_token: str) -> dict | None:
    """Computes each sector's today's % change as the average % change of
    its own constituent stocks (from SECTOR_MAP): previous close comes from
    the cached once-daily historical-candle load, current price from one
    batched LTP call. Returns {sector: {"pct_change": float, "count": int}}
    or None on failure/still-loading - callers must treat None as 'unknown
    for now', not zero."""
    global _sector_perf_debug
    prev_closes = _get_prev_closes(access_token)
    if not prev_closes:
        _sector_perf_debug = {"ok": False, "error": "Previous closes still loading in the background - try again shortly"}
        return None

    symbol_to_sector = SECTOR_MAP
    instrument_key_to_symbol: dict[str, str] = {}
    for symbol in prev_closes:
        key = get_instrument_key(symbol)
        if key:
            instrument_key_to_symbol[key] = symbol
    if not instrument_key_to_symbol:
        _sector_perf_debug = {"ok": False, "error": "No instrument keys resolved for symbols with a cached previous close"}
        return None

    instrument_keys = list(instrument_key_to_symbol.keys())
    # v2 LTP supports up to 500 instruments in a single call - our whole
    # F&O universe (~180 symbols) fits in one request.
    url = (
        "https://api.upstox.com/v2/market-quote/ltp?instrument_key="
        + urllib.parse.quote(",".join(instrument_keys), safe=",|")
    )
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            body_text = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            body_text = ""
        _sector_perf_debug = {"ok": False, "error": f"HTTP {e.code}: {body_text}"}
        return None
    except Exception as e:
        _sector_perf_debug = {"ok": False, "error": str(e)}
        return None

    data = payload.get("data") or {}
    sector_changes: dict[str, list[float]] = {}
    unmatched_tokens = []
    for entry in data.values():
        instrument_token = entry.get("instrument_token")
        symbol = instrument_key_to_symbol.get(instrument_token)
        if not symbol:
            if len(unmatched_tokens) < 3:
                unmatched_tokens.append(instrument_token)
            continue
        last_price = entry.get("last_price")
        prev_close = prev_closes.get(symbol)
        if not last_price or not prev_close:
            continue
        pct = (last_price - prev_close) / prev_close * 100
        sector = symbol_to_sector.get(symbol, "Unclassified")
        sector_changes.setdefault(sector, []).append(pct)

    result = {
        sector: {"pct_change": round(sum(vals) / len(vals), 2), "count": len(vals)}
        for sector, vals in sector_changes.items()
    }
    _sector_perf_debug = {
        "ok": True,
        "error": None,
        "prev_closes_cached": len(prev_closes),
        "instrument_keys_sent": len(instrument_keys),
        "data_entries_received": len(data),
        "unmatched_instrument_tokens_sample": unmatched_tokens,
        "sectors_matched": len(result),
    }
    return result


def get_sector_performance_cached(access_token: str | None) -> dict | None:
    """Same data as _fetch_sector_performance(), cached for a short window
    since this covers the app's whole ~180-symbol universe in one call and
    the sidebar polls independently of the 5s alert refresh."""
    global _sector_perf_cache
    if not access_token:
        return None
    now = datetime.utcnow()
    fetched_at = _sector_perf_cache["fetched_at"]
    if fetched_at and (now - fetched_at).total_seconds() < SECTOR_PERF_CACHE_SECONDS:
        return _sector_perf_cache["data"]
    value = _fetch_sector_performance(access_token)
    _sector_perf_cache = {"data": value, "fetched_at": now}
    return value


def resample_1min_to_5min(candles_1min: list) -> list[tuple[float, float, float, float]]:
    """candles_1min: list of (timestamp_str, open, high, low, close, volume, oi),
    oldest first. Groups consecutive 1-min candles into 5-min buckets aligned
    to the clock (e.g. 09:15-09:19), the way real 5-min candles work."""
    buckets: dict = {}
    order = []
    for c in candles_1min:
        ts_str, o, h, l, cl = c[0], c[1], c[2], c[3], c[4]
        dt = datetime.fromisoformat(ts_str)
        bucket_minute = (dt.minute // 5) * 5
        bucket_key = dt.replace(minute=bucket_minute, second=0, microsecond=0)
        if bucket_key not in buckets:
            buckets[bucket_key] = [o, h, l, cl]
            order.append(bucket_key)
        else:
            b = buckets[bucket_key]
            b[1] = max(b[1], h)
            b[2] = min(b[2], l)
            b[3] = cl
    return [tuple(buckets[k]) for k in order]


def resample_1min_to_5min_with_time(candles_1min: list) -> list[tuple[str, float, float, float, float]]:
    """Same bucketing as resample_1min_to_5min, but keeps each bucket's
    start time (as 'HH:MM') - needed for charting, where the plain OHLC
    tuples elsewhere in this file don't carry enough to label an x-axis."""
    buckets: dict = {}
    order = []
    for c in candles_1min:
        ts_str, o, h, l, cl = c[0], c[1], c[2], c[3], c[4]
        dt = datetime.fromisoformat(ts_str)
        bucket_minute = (dt.minute // 5) * 5
        bucket_key = dt.replace(minute=bucket_minute, second=0, microsecond=0)
        if bucket_key not in buckets:
            buckets[bucket_key] = [o, h, l, cl]
            order.append(bucket_key)
        else:
            b = buckets[bucket_key]
            b[1] = max(b[1], h)
            b[2] = min(b[2], l)
            b[3] = cl
    return [(k.strftime("%H:%M"), *buckets[k]) for k in order]


def fetch_previous_day_high_low(instrument_key: str, access_token: str) -> tuple[float | None, float | None]:
    """The most recent COMPLETED trading day's high and low, for the
    dashboard's per-alert chart PDH/PDL reference lines. Same day-candle
    endpoint the sector prev-close loader already uses - just reading
    high/low instead of close."""
    today = _ist_today_str()
    from_date = (datetime.utcnow() + IST_OFFSET - timedelta(days=10)).strftime("%Y-%m-%d")
    url = (
        f"https://api.upstox.com/v2/historical-candle/"
        f"{urllib.parse.quote(instrument_key, safe='|')}/day/{today}/{from_date}"
    )
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
        candles = (payload.get("data") or {}).get("candles") or []
        # Newest-first; the first entry is the most recent COMPLETED day.
        if candles:
            return candles[0][2], candles[0][3]
    except Exception:
        pass
    return None, None


def fetch_5min_candles_with_time(instrument_key: str, access_token: str) -> list[tuple[str, float, float, float, float]]:
    """Same data source as fetch_5min_candles, but keeps each candle's
    time label - used only by the dashboard's on-demand per-alert chart
    (see /api/chart/<symbol>), fetched lazily on click, never for the
    whole alert list at once."""
    url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode())
    raw_candles = payload.get("data", {}).get("candles", [])
    raw_candles = sorted(raw_candles, key=lambda c: c[0])
    return resample_1min_to_5min_with_time(raw_candles)


def fetch_5min_candles(instrument_key: str, access_token: str) -> list[tuple[float, float, float, float]]:
    """Fetches today's 1-minute candles from Upstox (their intraday API only
    supports 1minute or 30minute - not 5minute directly) and combines them
    into 5-minute candles ourselves. Returns (open, high, low, close),
    oldest first."""
    url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode())
    raw_candles = payload.get("data", {}).get("candles", [])
    # Upstox candle format: [timestamp, open, high, low, close, volume, oi]
    raw_candles = sorted(raw_candles, key=lambda c: c[0])
    return resample_1min_to_5min(raw_candles)


def fetch_intraday_candles_with_volume(instrument_key: str, access_token: str) -> list[tuple[float, float, float, float, float]]:
    """Same endpoint as fetch_5min_candles, but keeps volume and returns
    raw 1-minute candles (not resampled) - needed for the Volume Profile
    POC calculation, which fetch_5min_candles' OHLC-only tuples don't
    carry. Returns (open, high, low, close, volume), oldest first."""
    url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode())
    raw_candles = payload.get("data", {}).get("candles", [])
    # Upstox candle format: [timestamp, open, high, low, close, volume, oi]
    raw_candles = sorted(raw_candles, key=lambda c: c[0])
    return [(c[1], c[2], c[3], c[4], c[5]) for c in raw_candles]


def get_poc_price(instrument_key: str, access_token: str) -> float | None:
    """Computes the Point of Control (the price level with the highest
    traded volume) for this option contract's own price action, over
    today's session so far - a simplified Volume Profile Fixed Range: each
    1-minute candle's full volume is attributed to the bucket containing
    its close price (not split across the candle's high-low range), across
    20 buckets spanning today's close-price range. Returns None if there's
    not enough data yet to compute a meaningful profile."""
    try:
        candles = fetch_intraday_candles_with_volume(instrument_key, access_token)
    except Exception:
        return None
    if not candles or len(candles) < 3:
        return None
    closes = [c[3] for c in candles]
    lo, hi = min(closes), max(closes)
    if hi <= lo:
        return closes[-1]
    bucket_count = 20
    bucket_size = (hi - lo) / bucket_count
    volume_by_bucket: dict[int, float] = {}
    for _, _, _, close, volume in candles:
        idx = min(int((close - lo) / bucket_size), bucket_count - 1)
        volume_by_bucket[idx] = volume_by_bucket.get(idx, 0) + (volume or 0)
    if not volume_by_bucket:
        return None
    poc_bucket = max(volume_by_bucket, key=volume_by_bucket.get)
    poc_price = lo + (poc_bucket + 0.5) * bucket_size
    return round(poc_price, 2)


def poc_qualifies(instrument_key: str, category: str, current_price: float, access_token: str) -> tuple[bool, str]:
    """Entry filter: a Buy alert (buying a Call) needs that Call's own
    price to have closed ABOVE its Point of Control; a Sell alert (buying
    a Put) needs that Put's own price to have closed BELOW its own POC.
    Returns (qualifies, reason) - reason is stored as the trade's entry
    reason on success, or used for debugging on failure."""
    poc = get_poc_price(instrument_key, access_token)
    if poc is None:
        return False, "POC unavailable (not enough intraday data yet)"
    if category == "Buy":
        if current_price > poc:
            return True, "Price close above POC line"
        return False, f"Price {current_price} not above POC {poc}"
    else:
        if current_price < poc:
            return True, "Price close below POC line"
        return False, f"Price {current_price} not below POC {poc}"


UPSTOX_FUNDS_URL = "https://api.upstox.com/v3/user/get-funds-and-margin"

# Upstox's API sits behind Cloudflare, which blocks requests without a
# browser-like User-Agent - this bit us on the candle-fetching code earlier,
# so every Upstox call here uses the same header to avoid it happening again.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


_funds_debug: dict = {"ok": None, "error": None, "raw": None}


def get_upstox_available_funds(access_token: str) -> float | None:
    """Fetches the user's actual available-to-trade balance from Upstox, so
    live order quantity is sized off real account funds rather than the
    paper-trading capital setting. Returns None (never raises) if the call
    fails for any reason - callers must treat that as 'funds unknown' and
    skip placing the live order rather than guessing. The failure reason is
    saved to _funds_debug (see /api/paper-trading/debug-funds) instead of
    just disappearing, since a silent '-' on the page isn't debuggable."""
    global _funds_debug
    req = urllib.request.Request(
        UPSTOX_FUNDS_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Api-Version": "3.0",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode())
        available = (payload.get("data") or {}).get("available_to_trade") or {}
        total = available.get("total")
        if total is None:
            # fall back to the cash-only breakdown if 'total' isn't present
            total = (available.get("cash_available_to_trade") or {}).get("total")
        if total is None:
            _funds_debug = {"ok": False, "error": f"No 'total' field in response: {payload}", "raw": payload}
            return None
        _funds_debug = {"ok": True, "error": None, "raw": payload}
        return float(total)
    except urllib.error.HTTPError as e:
        try:
            body_text = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            body_text = ""
        _funds_debug = {"ok": False, "error": f"HTTP {e.code}: {body_text}", "raw": None}
        return None
    except Exception as e:
        _funds_debug = {"ok": False, "error": str(e), "raw": None}
        return None


_funds_display_cache: dict = {"value": None, "fetched_at": None}
FUNDS_DISPLAY_CACHE_SECONDS = 20


def get_upstox_available_funds_for_display(access_token: str) -> float | None:
    """Same data as get_upstox_available_funds(), but cached for a short
    window - this is only for showing a number on the page (which polls
    every 60s and can have multiple tabs open), not for sizing a live order.
    Order placement always calls get_upstox_available_funds() directly so
    it never uses a stale balance."""
    global _funds_display_cache
    now = datetime.utcnow()
    fetched_at = _funds_display_cache["fetched_at"]
    if fetched_at and (now - fetched_at).total_seconds() < FUNDS_DISPLAY_CACHE_SECONDS:
        return _funds_display_cache["value"]
    value = get_upstox_available_funds(access_token)
    _funds_display_cache = {"value": value, "fetched_at": now}
    return value


def compute_live_stats(open_trades: list[dict], closed_trades: list[dict]) -> dict:
    """Live trading runs its own exit rule now, fully decoupled from the
    paper trade's strategy - so P&L uses live_entry_price/live_exit_price
    (the REAL order fill prices), not the paper trade's theoretical
    entry_price/exit_price, since a market order can fill meaningfully
    differently from whatever price we used to decide to place it."""
    live_open = sum(1 for t in open_trades if t.get("live_status") == "OPEN")
    live_closed_trades = [t for t in closed_trades if t.get("live_status") == "CLOSED"]
    live_pnl = 0.0
    for t in live_closed_trades:
        qty = t.get("live_quantity") or 0
        entry = t.get("live_entry_price") if t.get("live_entry_price") is not None else (t.get("entry_price") or 0)
        exit_p = t.get("live_exit_price") if t.get("live_exit_price") is not None else (t.get("exit_price") or 0)
        live_pnl += (exit_p - entry) * qty
    return {
        "live_open": live_open,
        "live_closed": len(live_closed_trades),
        "live_pnl": round(live_pnl, 2),
    }


UPSTOX_ORDER_URL = "https://api.upstox.com/v3/order/place"


def get_ltp(instrument_key: str, access_token: str) -> float | None:
    """Fetches the current last-traded price for a single instrument (used
    to price an option's premium before sizing lots). Returns None (never
    raises) on any failure."""
    url = "https://api.upstox.com/v2/market-quote/ltp?instrument_key=" + urllib.parse.quote(instrument_key, safe="|")
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
        data = payload.get("data") or {}
        for entry in data.values():
            if entry.get("instrument_token") == instrument_key:
                return entry.get("last_price")
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Real-time price feed via Upstox's V3 WebSocket (MarketDataStreamerV3 from
# the official upstox-python-sdk package). This exists purely for SPEED:
# the exit-check loop can react to price-LEVEL triggers (Target %
# thresholds, trailing-stop pullbacks) using the latest streamed tick
# instead of waiting on a REST candle fetch. The 5-EMA rule still uses
# fetch_5min_candles (it needs actual OHLC history, not a single current
# price) - this doesn't replace that, it only supplements the current
# price used for percentage-based decisions.
#
# This is a SOFT dependency end to end: if upstox-python-sdk isn't
# installed, or the connection can't be established, or a message doesn't
# parse - everything falls back to the existing REST-only behavior. The
# app must never crash or block because of anything in this section.
# ---------------------------------------------------------------------------
_ws_price_cache: dict[str, dict] = {}  # instrument_key -> {"ltp": float, "updated_at": datetime}
_ws_lock = threading.Lock()
_ws_streamer = None
_ws_subscribed_keys: set[str] = set()
_ws_thread_started = False
_ws_debug: dict = {"status": "not_started", "error": None, "last_message_at": None, "sdk_available": None, "connect_attempts": 0}

WS_PRICE_MAX_AGE_SECONDS = 5  # a cached tick older than this is treated as stale, not used


def get_ws_price(instrument_key: str) -> float | None:
    """Returns the latest WebSocket-streamed LTP for this instrument, or
    None if we don't have a fresh one (not subscribed yet, feed not
    connected, or the last tick is too old to trust). Callers must fall
    back to a REST fetch in that case - None here means 'use REST', not
    'price is unavailable'."""
    with _ws_lock:
        entry = _ws_price_cache.get(instrument_key)
    if not entry:
        return None
    age = (datetime.utcnow() - entry["updated_at"]).total_seconds()
    if age > WS_PRICE_MAX_AGE_SECONDS:
        return None
    return entry["ltp"]


def _ws_on_message(message) -> None:
    """Callback registered with MarketDataStreamerV3 - fires on the
    streamer's own thread for every tick. Handles both the plain 'ltpc'
    mode shape and the 'full' mode's nested marketFF.ltpc shape, since the
    exact shape returned by the SDK's decoded message wasn't verifiable
    from this build environment (no live connection was possible here) -
    kept defensive so an unexpected shape just gets skipped, never crashes
    the feed thread."""
    global _ws_debug
    try:
        feeds = message.get("feeds") if isinstance(message, dict) else None
        if not feeds:
            return
        now = datetime.utcnow()
        with _ws_lock:
            for instrument_key, feed in feeds.items():
                if not isinstance(feed, dict):
                    continue
                ltpc = feed.get("ltpc")
                if not ltpc:
                    full = feed.get("fullFeed") or {}
                    market_ff = full.get("marketFF") or full.get("indexFF") or {}
                    ltpc = market_ff.get("ltpc")
                if not ltpc:
                    continue
                ltp = ltpc.get("ltp")
                if ltp:
                    _ws_price_cache[instrument_key] = {"ltp": float(ltp), "updated_at": now}
        _ws_debug["last_message_at"] = now.isoformat()
        _ws_debug["status"] = "streaming"
    except Exception as e:
        _ws_debug["error"] = f"on_message error: {e}"


def _ws_run(access_token: str) -> None:
    """Runs for the lifetime of the process on a background thread -
    connects to Upstox's V3 feed and automatically reconnects (after a
    pause) if the connection drops for any reason: network blip, Render
    spinning the app down and back up, Upstox-side restart, etc."""
    global _ws_streamer, _ws_debug
    try:
        import upstox_client
        _ws_debug["sdk_available"] = True
    except ImportError:
        _ws_debug["sdk_available"] = False
        _ws_debug["status"] = "sdk_not_installed"
        _ws_debug["error"] = "upstox-python-sdk not installed - add it to requirements.txt"
        return

    while True:
        try:
            # Always use the freshest saved token, not the one this
            # thread happened to be started with. If the DB already had
            # a stale/expired token when this process booted (e.g. it
            # spun up before today's token was pasted in), the token
            # passed into this function at thread-start is permanently
            # wrong - updating Settings afterward otherwise wouldn't do
            # anything, since the process itself doesn't restart just
            # because a setting changed. Re-reading on every reconnect
            # attempt is what makes a fresh token actually take effect
            # without needing a manual redeploy.
            current_token = get_setting("upstox_access_token") or access_token
            configuration = upstox_client.Configuration()
            configuration.access_token = current_token
            initial_keys = list(_ws_subscribed_keys) or ["NSE_INDEX|Nifty 50"]
            streamer = upstox_client.MarketDataStreamerV3(
                upstox_client.ApiClient(configuration), initial_keys, "ltpc"
            )
            streamer.on("message", _ws_on_message)
            # Best-effort: register open/close/error events too, if the
            # SDK exposes them - wrapped individually since it's unverified
            # which events this particular SDK version supports.
            try:
                streamer.on("open", lambda *a: _ws_debug.update({"status": "streaming", "error": None}))
            except Exception:
                pass
            try:
                streamer.on("error", lambda *a: _ws_debug.update({"status": "error", "error": f"ws error event: {a}"}))
            except Exception:
                pass
            try:
                streamer.on("close", lambda *a: _ws_debug.update({"status": "disconnected"}))
            except Exception:
                pass
            _ws_streamer = streamer
            _ws_debug["status"] = "connecting"
            _ws_debug["connect_attempts"] += 1
            streamer.connect()
            # connect() may block for the life of the connection (a
            # typical run_forever pattern), or may return quickly after
            # starting the feed on its own internal thread - handled
            # either way below via a staleness monitor, instead of
            # treating "connect() returned" as "it disconnected" and
            # immediately retrying in a tight loop (which is what produced
            # a connect/reconnect flap in testing - status kept landing on
            # 'disconnected' with no actual error).
        except Exception as e:
            _ws_debug["status"] = "error"
            _ws_debug["error"] = str(e)
            _ws_streamer = None
            time.sleep(5)
            continue

        # Monitor loop: only treat the feed as dead (and reconnect) once
        # it's genuinely gone quiet for a while with subscriptions active -
        # not the instant connect() happens to return.
        quiet_checks = 0
        while True:
            time.sleep(10)
            if _ws_debug["status"] == "error":
                break  # the "error" event fired above - go reconnect
            last_msg = _ws_debug.get("last_message_at")
            if last_msg:
                age = (datetime.utcnow() - datetime.fromisoformat(last_msg)).total_seconds()
                if age < 60:
                    quiet_checks = 0
                    continue
            if not _ws_subscribed_keys:
                continue  # nothing subscribed yet - silence is expected, not staleness
            quiet_checks += 1
            if quiet_checks >= 3:  # ~30s of silence despite active subscriptions
                _ws_debug["status"] = "stale_reconnecting"
                break
        _ws_streamer = None
        time.sleep(5)  # brief pause before reconnecting, avoid a hot retry loop


def ensure_websocket_started(access_token: str) -> None:
    """Starts the background WebSocket thread once per process. Safe to
    call from any request handler - a no-op if it's already running or if
    there's no token yet."""
    global _ws_thread_started
    if _ws_thread_started or not access_token:
        return
    _ws_thread_started = True
    threading.Thread(target=_ws_run, args=(access_token,), daemon=True).start()


def update_ws_subscriptions(instrument_keys: set[str]) -> None:
    """Keeps the WebSocket subscription list in sync with whatever
    instruments currently matter (every open trade's option contract).
    Safe to call even before the streamer connects - the next connect
    picks up the current _ws_subscribed_keys set from scratch."""
    global _ws_subscribed_keys
    added = instrument_keys - _ws_subscribed_keys
    removed = _ws_subscribed_keys - instrument_keys
    _ws_subscribed_keys = set(instrument_keys)
    if _ws_streamer is None or (not added and not removed):
        return
    try:
        if added:
            _ws_streamer.subscribe(list(added), "ltpc")
        if removed:
            _ws_streamer.unsubscribe(list(removed))
    except Exception as e:
        _ws_debug["error"] = f"subscribe error: {e}"


def _get_order_proxy_opener():
    """Builds a urllib opener routed through the static-IP proxy (StaticIP.in
    or similar), used ONLY for order placement since Upstox's static-IP
    requirement applies to order APIs, not quotes/funds/candles. Credentials
    come from environment variables (set in Render's dashboard) - NEVER
    hardcoded here, since this repo is public. Returns None if the env vars
    aren't set, so order placement falls back to a direct connection
    (which Upstox will then reject with a static-IP error, surfaced as a
    normal order failure - not a crash) until they're configured."""
    host = os.environ.get("STATICIP_HOST")
    port = os.environ.get("STATICIP_PORT")
    user = os.environ.get("STATICIP_USER")
    password = os.environ.get("STATICIP_PASS")
    if not all([host, port, user, password]):
        return None
    proxy_url = f"https://{urllib.parse.quote(user)}:{urllib.parse.quote(password)}@{host}:{port}"
    proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    return urllib.request.build_opener(proxy_handler)


def place_live_order(
    instrument_key: str,
    transaction_type: str,
    quantity: int,
    access_token: str,
    order_type: str = "MARKET",
    trigger_price: float = 0.0,
) -> dict:
    """Places a real order on Upstox: CNC (Delivery) product, DAY validity -
    matching the decisions for this app (live trading only ever goes long,
    so transaction_type is effectively always 'BUY' to open and 'SELL' to
    close the same delivery position). order_type defaults to 'MARKET' (the
    original behavior); pass order_type='SL-M' with a trigger_price to
    place a stop-loss-market order instead - used to protect a fresh
    position with a broker-side stop that fires even if this app/server is
    down, rather than relying only on our own polling.

    Never raises - a live-order failure must not be able to crash paper
    trade creation or the exit-check loop. Returns:
      {"ok": True, "order_id": "..."} on success
      {"ok": False, "error": "..."} on failure
    """
    body = {
        "quantity": quantity,
        "product": "D",  # CNC / Delivery
        "validity": "DAY",
        "price": 0,  # ignored by Upstox for MARKET/SL-M orders
        "tag": "chartink-auto",
        "instrument_token": instrument_key,
        "order_type": order_type,
        "transaction_type": transaction_type,  # "BUY" or "SELL"
        "disclosed_quantity": 0,
        "trigger_price": trigger_price,
        "is_amo": False,
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        UPSTOX_ORDER_URL,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Api-Version": "3.0",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        opener = _get_order_proxy_opener()
        opener_fn = opener.open if opener else urllib.request.urlopen
        with opener_fn(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode())
        order_ids = (payload.get("data") or {}).get("order_ids") or []
        order_id = order_ids[0] if order_ids else None
        if not order_id:
            return {"ok": False, "error": f"No order_id in response: {payload}"}
        return {"ok": True, "order_id": order_id}
    except urllib.error.HTTPError as e:
        try:
            body_text = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            body_text = ""
        return {"ok": False, "error": f"HTTP {e.code}: {body_text}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_order_average_price(order_id: str, access_token: str) -> float | None:
    """Fetches the REAL average fill price for a placed order via Upstox's
    Order Details API. This is what live P&L must be computed from -
    a market order can fill meaningfully differently from whatever candle
    price we used to decide to place it (illiquid option, wide bid-ask
    spread, fast-moving price) - using our own theoretical price instead of
    the actual fill was silently producing wrong (sometimes even
    wrong-signed) live P&L. Retries briefly since a market order may take a
    moment to settle to 'complete' status. Returns None (never raises) if
    it can't get a real fill price - callers must NOT fall back to a
    theoretical price in that case, since that's the exact bug this fixes."""
    url = "https://api.upstox.com/v2/order/details?order_id=" + urllib.parse.quote(order_id)
    for attempt in range(4):
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "User-Agent": BROWSER_USER_AGENT,
            },
        )
        try:
            opener = _get_order_proxy_opener()
            opener_fn = opener.open if opener else urllib.request.urlopen
            with opener_fn(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode())
            data = payload.get("data") or {}
            status = data.get("status")
            avg_price = data.get("average_price")
            if status == "complete" and avg_price:
                return float(avg_price)
        except Exception:
            pass
        if attempt < 3:
            time.sleep(1.5)
    return None


def get_order_status(order_id: str, access_token: str) -> dict | None:
    """Fetches an order's current status/fields from Upstox (status,
    average_price, filled_quantity, trigger_price, etc.) without the
    retry-and-wait behavior of get_order_average_price - used to poll
    whether a resting SL-M stop order has fired on its own. Returns None
    (never raises) on any failure."""
    url = "https://api.upstox.com/v2/order/details?order_id=" + urllib.parse.quote(order_id)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        opener = _get_order_proxy_opener()
        opener_fn = opener.open if opener else urllib.request.urlopen
        with opener_fn(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
        return payload.get("data") or None
    except Exception:
        return None


def cancel_order(order_id: str, access_token: str) -> bool:
    """Cancels a resting order on Upstox (used to pull the protective SL-M
    stop the moment software decides to exit the position some other way -
    a trailing stop firing, a target hit, etc. - so the two exit paths can
    never both fill and double-sell the position). Returns True on a
    successful cancel request; never raises. Treats 'already
    filled/cancelled' errors as a non-fatal no-op, since that's exactly
    what happens when the SL-M itself already fired first."""
    url = "https://api.upstox.com/v2/order/cancel?order_id=" + urllib.parse.quote(order_id)
    req = urllib.request.Request(
        url,
        method="DELETE",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        opener = _get_order_proxy_opener()
        opener_fn = opener.open if opener else urllib.request.urlopen
        with opener_fn(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception:
        return False


def _close_live_position_if_any(trade: dict, access_token: str | None) -> tuple[str | None, str | None, float | None]:
    """If this paper trade has a live position open (live_status == 'OPEN'),
    places the closing SELL order on Upstox for the EXACT option contract
    that was bought at entry (live_instrument_key) - not a freshly
    recomputed ATM strike, since the ATM strike at entry time is what's
    actually held, regardless of where the underlying has moved since.
    Returns (order_id, error, fill_price) - all None if there was no live
    position to close, which is the normal case for any trade from before
    live trading was turned on. fill_price is the REAL average price the
    order executed at (fetched via get_order_average_price), not a
    theoretical price - callers must use this for live P&L, not last_price."""
    if trade.get("live_status") != "OPEN":
        return None, None, None
    if not access_token:
        return None, "Live position open but no Upstox access token saved - could not close it", None

    instrument_key = trade.get("live_instrument_key")
    if not instrument_key:
        return None, "No live_instrument_key recorded on this trade - could not close live position", None

    # Pull the resting broker-side SL-M stop first, if one exists - a
    # software-driven exit (trailing stop, target hit, manual close) and
    # the SL-M firing on its own must never both go through, or the
    # position gets double-sold. Best-effort: if the cancel fails because
    # the SL-M already fired, the subsequent SELL below will simply fail
    # or oversell-protect at the broker, and get picked up as an error.
    sl_order_id = trade.get("live_sl_order_id")
    if sl_order_id:
        cancel_order(sl_order_id, access_token)

    qty = trade.get("live_quantity") or 1
    result = place_live_order(instrument_key, "SELL", qty, access_token)
    if result["ok"]:
        fill_price = get_order_average_price(result["order_id"], access_token)
        return result["order_id"], None, fill_price
    return None, f"Failed to close live position: {result['error']}", None


EXIT_CHECK_INTERVAL_SECONDS = 5
_exit_check_thread_started = False


def _exit_check_loop() -> None:
    """Runs for the life of the process on its own background thread,
    calling run_paper_trade_check() on a short fixed cadence - this is
    what makes every % based exit (the -2% stoploss included) a real
    stop instead of one that only gets evaluated once a minute, and only
    when a browser tab happens to be open. The frontend's own 60s poll
    (in paper_trading.html / live_trading.html) still runs too, purely to
    refresh what's on screen - this loop is what keeps the actual checking
    tight in between, tab or no tab, as long as the server process is
    alive. Never lets one bad check kill the loop."""
    while True:
        try:
            run_paper_trade_check()
        except Exception as e:
            print(f"[exit-check-loop] error: {e}")
        time.sleep(EXIT_CHECK_INTERVAL_SECONDS)


def ensure_exit_check_loop_started() -> None:
    """Starts the background exit-check thread once per process. Safe to
    call from anywhere - a no-op if it's already running. Unlike the
    WebSocket starter, this doesn't need a token up front: it's called
    once at import time (see bottom of file) and run_paper_trade_check()
    itself already handles the no-token case cleanly every pass."""
    global _exit_check_thread_started
    if _exit_check_thread_started:
        return
    _exit_check_thread_started = True
    threading.Thread(target=_exit_check_loop, daemon=True).start()


def run_paper_trade_check() -> dict:
    """Checks every open paper trade against its exit strategy, closing
    any that qualify. Returns a summary dict for the UI. Runs both from
    the frontend's periodic poll AND continuously from the background
    _exit_check_loop thread (see above) - the latter is what keeps this
    from only firing once a minute with a tab open."""
    access_token = get_setting("upstox_access_token")
    if not access_token:
        return {"checked": 0, "closed": 0, "error": "No Upstox access token saved yet."}

    with get_db() as conn:
        open_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE status = 'OPEN' OR live_status = 'OPEN'"
        ).fetchall()

    ensure_websocket_started(access_token)
    active_keys = {
        t["paper_instrument_key"] for t in open_trades if t["paper_instrument_key"]
    } | {
        t["live_instrument_key"] for t in open_trades if t["live_instrument_key"]
    }
    update_ws_subscriptions(active_keys)

    checked = 0
    closed = 0
    errors = []
    now = datetime.utcnow().isoformat()

    for trade in open_trades:
        checked += 1
        symbol = trade["symbol"]
        try:
            instrument_key = trade["paper_instrument_key"] or get_instrument_key(symbol)
            if not instrument_key:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE paper_trades SET last_error = ?, last_checked_time = ? WHERE id = ?",
                        (f"No instrument_key found for {symbol}", now, trade["id"]),
                    )
                    conn.commit()
                continue

            strategy = trade["strategy"] or "EMA"
            # Candles are only fetched when actually needed: strategies
            # 2-5 (the current ones) decide purely off a % move against
            # entry, using the WebSocket tick price - no candle series
            # required. Only the legacy EMA/TARGETS paths (kept for
            # backward compatibility on trades opened before the strategy
            # picklist changed) need the 5-min candle series for the
            # 5-EMA rule. Skipping the fetch for the common case is what
            # makes it safe to run this check every few seconds instead of
            # once a minute, without hammering Upstox's candle endpoint.
            needs_candles = strategy in ("EMA", "TARGETS", "ATR_TRAIL")
            ws_price = get_ws_price(instrument_key)
            candles = fetch_5min_candles(instrument_key, access_token) if (needs_candles or ws_price is None) else None
            last_price = ws_price if ws_price is not None else (candles[-1][3] if candles else None)
            entry_price = trade["entry_price"]

            # EMA_SPOT_TRAIL is the one strategy that needs a candle
            # series from the UNDERLYING stock, not the option premium -
            # the whole point of it is trailing the same clean 5-EMA
            # pattern a trader would watch on the spot chart, since
            # option premiums are far noisier than the stock itself.
            # Fetched lazily, only for trades actually running this
            # strategy - it's a second Upstox call on top of the option's
            # own, so no reason to pay for it otherwise.
            spot_candles = None
            if strategy in ("EMA_SPOT_TRAIL", "EMA_SPOT_PURE"):
                spot_key = get_instrument_key(symbol)
                if spot_key:
                    spot_candles = fetch_5min_candles(spot_key, access_token)

            # Paper and live each resolve their own ATM contract at entry
            # time (both from the same alert price, moments apart) - in
            # practice that's almost always the same strike, but if it
            # ever isn't, live's own price checks must use live's own
            # contract, not paper's. Only does the extra work when they
            # actually differ - the common case reuses last_price with no
            # extra call. Live no longer needs a candle series at all
            # (its downside is now a broker-side SL-M order, not a
            # software 5-EMA check - see run_paper_trade_check's live
            # section below), so this is just a price lookup.
            live_instrument_key = trade["live_instrument_key"]
            if live_instrument_key and live_instrument_key != instrument_key:
                live_ws_price = get_ws_price(live_instrument_key)
                if live_ws_price is not None:
                    live_last_price = live_ws_price
                else:
                    live_candles_fallback = fetch_5min_candles(live_instrument_key, access_token)
                    live_last_price = live_candles_fallback[-1][3] if live_candles_fallback else None
            else:
                live_last_price = last_price

            exited = False
            exit_price = None
            exit_reason = None
            partial = None  # set if a half-booking target books this pass, without fully closing
            trail_update = None  # set if the trailing stop advances this pass, without closing

            pct_change = None
            if last_price is not None and entry_price:
                pct_change = (last_price - entry_price) / entry_price * 100

            if trade["status"] != "OPEN":
                pass  # paper side already closed - only live-side logic below applies

            elif strategy == "EMA":
                # #1: 5-EMA only, let it run - kept only for backward
                # compatibility with any trade that opened under this
                # strategy before it was retired from the picklist (5-EMA
                # alone let losses run too deep - see strategies below).
                exited, exit_price = check_exit("Buy", candles or [])
                if exited:
                    exit_reason = "5-EMA exit rule"

            elif strategy == "HALF_HALF_HARD4" and pct_change is not None:
                # #2: hard -2% stop-loss protects until +2% (immediate,
                # not pattern-based - the 5-EMA rule let losses run too
                # deep waiting for a reversal to confirm). At +2%, book
                # half the qty (Target-1). The remaining half is protected
                # at breakeven until +4%, where it exits in full
                # (Target-2) - a hard target, no trailing beyond it.
                if trade["target1_hit_time"] is None:
                    if pct_change >= 2.0:
                        original_qty = trade["original_quantity"] or trade["quantity"] or 1
                        half_qty = max(1, original_qty // 2)
                        remaining_qty = max(0, original_qty - half_qty)
                        partial = {
                            "exit_price": last_price,
                            "qty": half_qty,
                            "pnl": round((last_price - entry_price) * half_qty, 2),
                            "remaining_qty": remaining_qty,
                        }
                    elif pct_change <= -2.0:
                        exited, exit_price = True, last_price
                        exit_reason = "Stop-loss (-2%)"
                else:
                    if pct_change >= 4.0:
                        exited, exit_price = True, last_price
                        exit_reason = "Target-2 (4%) - full exit"
                    elif last_price <= entry_price:
                        exited, exit_price = True, last_price
                        exit_reason = "Breakeven stop after Target-1"

            elif strategy == "TRAIL_FROM_2" and pct_change is not None:
                # #3: hard -2% stop-loss protects until +2% (immediate,
                # not pattern-based). No half-booking - the FULL quantity
                # starts trailing from +2%, ratcheting up in 0.2% steps as
                # price climbs, always sitting one step behind the peak.
                if trade["trail_high_pct"] is None:
                    if pct_change >= 2.0:
                        trail_update = {"trail_high_pct": round(int(pct_change / 0.2) * 0.2, 2)}
                    elif pct_change <= -2.0:
                        exited, exit_price = True, last_price
                        exit_reason = "Stop-loss (-2%)"
                else:
                    trail_high = trade["trail_high_pct"]
                    current_milestone = round(int(pct_change / 0.2) * 0.2, 2)
                    if current_milestone > trail_high:
                        trail_update = {"trail_high_pct": current_milestone}
                    else:
                        stop_pct = round(trail_high - 0.2, 2)
                        if pct_change <= stop_pct:
                            exited, exit_price = True, last_price
                            exit_reason = f"Trailing stop ({stop_pct:g}%)"

            elif strategy == "FULL_AT_2" and pct_change is not None:
                # #4: hard -2% stop-loss protects until +2%, then the FULL
                # quantity exits at +2% - a single fixed target, no
                # trailing, symmetric 1:1 risk-reward.
                if pct_change >= 2.0:
                    exited, exit_price = True, last_price
                    exit_reason = "Target (2%) - full exit"
                elif pct_change <= -2.0:
                    exited, exit_price = True, last_price
                    exit_reason = "Stop-loss (-2%)"

            elif strategy == "FULL_AT_4" and pct_change is not None:
                # #5: hard -2% stop-loss protects until +4%, then the FULL
                # quantity exits at +4% - a single fixed target, no
                # trailing, 1:2 risk-reward.
                if pct_change >= 4.0:
                    exited, exit_price = True, last_price
                    exit_reason = "Target (4%) - full exit"
                elif pct_change <= -2.0:
                    exited, exit_price = True, last_price
                    exit_reason = "Stop-loss (-2%)"

            elif strategy == "ATR_TRAIL" and pct_change is not None:
                # #6 (hybrid): ATR (Chandelier-style) protects the
                # downside only until +2% profit - stop distance scales
                # with how much the premium is actually moving, so it
                # sits wider in choppy/volatile stretches and tighter once
                # things calm down, instead of a fixed %. A -2% floor
                # covers the trade until there's enough candle history for
                # the first ATR reading. Once +2% is reached, ATR hands
                # off entirely to a plain 0.5%-step trailing stop on the %
                # gain (same mechanism as strategy #3, wider steps) - this
                # is what actually locks in profit mechanically, rather
                # than risk ATR suddenly going quiet on a winning trade
                # and loosening the stop right when it should be tightest.
                if trade["trail_high_pct"] is None:
                    if pct_change >= 2.0:
                        trail_update = {"trail_high_pct": _floor_to_step(pct_change, 0.5)}
                    else:
                        atr_period = get_atr_period()
                        atr_multiplier = get_atr_multiplier()
                        atr_series = calculate_atr(candles or [], atr_period)
                        current_atr = atr_series[-1] if atr_series else None
                        if current_atr is None:
                            if pct_change <= -2.0:
                                exited, exit_price = True, last_price
                                exit_reason = "Stop-loss (-2%, ATR not ready yet)"
                        else:
                            peak_price = trade["atr_trail_peak_price"]
                            peak_price = max(peak_price, last_price) if peak_price is not None else max(entry_price, last_price)
                            stop_price = peak_price - (current_atr * atr_multiplier)
                            # Never let the ATR stop sit LOOSER than the
                            # -2% floor - thin/illiquid option premiums
                            # can produce 5-min candles with huge true
                            # range relative to price (bid-ask noise, not
                            # real movement), which can otherwise make the
                            # ATR stop far wider than intended. ATR can
                            # only tighten the stop below -2%, never widen
                            # it past that.
                            stop_price = max(stop_price, entry_price * 0.98)
                            if last_price <= stop_price:
                                exited, exit_price = True, last_price
                                exit_reason = f"ATR stop ({atr_period}p x{atr_multiplier:g}, stop {stop_price:.2f})"
                            else:
                                trail_update = {"atr_trail_peak_price": peak_price}
                else:
                    trail_high = trade["trail_high_pct"]
                    current_milestone = _floor_to_step(pct_change, 0.5)
                    if current_milestone > trail_high:
                        trail_update = {"trail_high_pct": current_milestone}
                    else:
                        stop_pct = round(trail_high - 0.5, 2)
                        if pct_change <= stop_pct:
                            exited, exit_price = True, last_price
                            exit_reason = f"Trailing stop ({stop_pct:g}%) after ATR phase"

            elif strategy == "EMA_SPOT_TRAIL" and pct_change is not None:
                # #7: a hard -2% stop-loss protects the trade throughout.
                # On top of that, the full quantity exits the moment the
                # UNDERLYING stock (not the option premium) confirms a
                # reversal against the position - 2 consecutive candles
                # closing beyond its own 5-EMA:
                #   Buy:  2 red candles closing below EMA(5) of the LOWS
                #   Sell: 2 green candles closing above EMA(5) of the HIGHS
                # This trails the same clean pattern a trader would watch
                # on the spot chart, rather than the option premium's own
                # noisier candles.
                if pct_change <= -2.0:
                    exited, exit_price = True, last_price
                    exit_reason = "Stop-loss (-2%)"
                else:
                    spot_exited, _ = check_exit(trade["direction"], spot_candles or [])
                    if spot_exited:
                        exited, exit_price = True, last_price
                        exit_reason = "5-EMA (spot) reversal"

            elif strategy == "EMA_SPOT_PURE" and last_price is not None:
                # #8: pure 5-EMA (spot) trail - NO -2% floor, no fixed
                # target. The full quantity only exits when the
                # UNDERLYING stock confirms a reversal against the
                # position (same rule as #7, just without a stop
                # underneath it):
                #   Buy:  2 red candles closing below EMA(5) of the LOWS
                #   Sell: 2 green candles closing above EMA(5) of the HIGHS
                # Deliberately unprotected on the downside - the whole
                # point is to never cut a trade off before the EMA itself
                # ever gets a chance to trail it. A losing trade can run
                # further than any other strategy here before this fires.
                spot_exited, _ = check_exit(trade["direction"], spot_candles or [])
                if spot_exited:
                    exited, exit_price = True, last_price
                    exit_reason = "5-EMA (spot) reversal"

            elif strategy == "TARGETS" and pct_change is not None:
                # Legacy hybrid (half at 2%, breakeven, trail from 4%) -
                # kept only so trades that opened under the old single
                # "TARGETS" option (before this 5-way split) keep working
                # correctly. Not offered as a choice for new trades anymore.
                if trade["target1_hit_time"] is None:
                    if pct_change >= 2.0:
                        original_qty = trade["original_quantity"] or trade["quantity"] or 1
                        half_qty = max(1, original_qty // 2)
                        remaining_qty = max(0, original_qty - half_qty)
                        partial = {
                            "exit_price": last_price,
                            "qty": half_qty,
                            "pnl": round((last_price - entry_price) * half_qty, 2),
                            "remaining_qty": remaining_qty,
                        }
                    else:
                        exited, exit_price = check_exit("Buy", candles or [])
                        if exited:
                            exit_reason = "5-EMA exit (before Target-1)"
                elif trade["trail_high_pct"] is None:
                    if pct_change >= 4.0:
                        trail_update = {"trail_high_pct": int(pct_change // 0.5) * 0.5}
                    elif last_price <= entry_price:
                        exited, exit_price = True, last_price
                        exit_reason = "Breakeven stop after Target-1"
                else:
                    trail_high = trade["trail_high_pct"]
                    current_milestone = int(pct_change // 0.5) * 0.5
                    if current_milestone > trail_high:
                        trail_update = {"trail_high_pct": current_milestone}
                    else:
                        stop_pct = round(trail_high - 0.5, 2)
                        if pct_change <= stop_pct:
                            exited, exit_price = True, last_price
                            exit_reason = f"Trailing stop ({stop_pct:g}%) after Target-2"

            elif trade["status"] == "OPEN":
                # Both Buy and Sell alerts result in BUYING an option (Call
                # or Put respectively) - the position is always long the
                # premium, so the exit rule always uses the "Buy" (long)
                # logic here, regardless of trade["direction"] (which only
                # reflects which alert category triggered entry, not the
                # position's own side). Fallback for an unrecognized
                # strategy value or missing price data.
                exited, exit_price = check_exit("Buy", candles or [])
                if exited:
                    exit_reason = "5-EMA exit rule"

            # Live trading runs its OWN, independent exit rule from the
            # paper strategy chosen in Settings - full quantity, no half
            # booking. Downside is now protected by a REAL broker-side
            # SL-M stop order placed at entry (see place_live_order call
            # above) instead of a software-checked rule, so it fires from
            # Upstox's own engine even if this app/server is down. This
            # loop's job on the downside is just to notice when that SL-M
            # has already fired and reconcile our own status - never to
            # place a second closing order on top of it. From +2%, the
            # stop trails the whole position in 0.5% steps, closed via a
            # normal software-driven market SELL when it's hit (which also
            # cancels the now-unneeded SL-M first, see
            # _close_live_position_if_any).
            live_exited = False
            live_exit_reason_val = None
            live_trail_update = None
            live_sl_fired_price = None

            # Check whether the resting SL-M stop already fired on its own
            # since the last pass - if so, the broker closed the position
            # for us; reconcile status instead of treating it as still open.
            if trade["live_status"] == "OPEN" and trade["live_sl_order_id"]:
                sl_status = get_order_status(trade["live_sl_order_id"], access_token)
                if sl_status and sl_status.get("status") == "complete":
                    live_exited = True
                    live_exit_reason_val = "Stoploss (-2%, broker SL-M order)"
                    live_sl_fired_price = sl_status.get("average_price")

            # Must use the REAL fill price (live_entry_price), not the
            # paper trade's theoretical entry_price - a market order can
            # fill at a genuinely different price, so using paper's number
            # here would make live's own +2%/trailing checkpoints wrong
            # relative to what was actually paid.
            live_entry_ref = trade["live_entry_price"] if trade["live_entry_price"] is not None else entry_price
            if not live_exited and trade["live_status"] == "OPEN" and live_last_price is not None and live_entry_ref:
                live_pct_change = (live_last_price - live_entry_ref) / live_entry_ref * 100
                if trade["live_trail_high_pct"] is None:
                    if live_pct_change >= 2.0:
                        live_trail_update = int(live_pct_change // 0.5) * 0.5
                else:
                    live_trail_high = trade["live_trail_high_pct"]
                    live_milestone = int(live_pct_change // 0.5) * 0.5
                    if live_milestone > live_trail_high:
                        live_trail_update = live_milestone
                    else:
                        live_stop_pct = round(live_trail_high - 0.5, 2)
                        if live_pct_change <= live_stop_pct:
                            live_exited = True
                            live_exit_reason_val = f"Trailing stop ({live_stop_pct:g}%) after +2%"

            if live_exited and live_sl_fired_price is not None:
                # The SL-M already filled at the broker - nothing left to
                # place, just record it. Skip _close_live_position_if_any
                # entirely (it would try to cancel/sell an already-closed
                # position).
                with get_db() as conn:
                    conn.execute(
                        """
                        UPDATE paper_trades
                        SET live_status = 'CLOSED', live_exit_reason = ?,
                            live_exit_price = ?, live_exit_time = ?
                        WHERE id = ?
                        """,
                        (live_exit_reason_val, live_sl_fired_price, now, trade["id"]),
                    )
                    conn.commit()
            elif live_exited:
                live_exit_order_id, live_exit_error, live_fill_price = _close_live_position_if_any(trade, access_token)
                with get_db() as conn:
                    conn.execute(
                        """
                        UPDATE paper_trades
                        SET live_status = CASE WHEN ? THEN 'CLOSED' ELSE live_status END,
                            live_exit_order_id = COALESCE(?, live_exit_order_id),
                            live_error = COALESCE(?, live_error),
                            live_exit_reason = ?,
                            live_exit_price = COALESCE(?, live_exit_price),
                            live_exit_time = CASE WHEN ? THEN ? ELSE live_exit_time END
                        WHERE id = ?
                        """,
                        (live_exit_order_id is not None, live_exit_order_id, live_exit_error,
                         live_exit_reason_val, live_fill_price,
                         live_exit_order_id is not None, now, trade["id"]),
                    )
                    conn.commit()
            elif live_trail_update is not None:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE paper_trades SET live_trail_high_pct = ? WHERE id = ?",
                        (live_trail_update, trade["id"]),
                    )
                    conn.commit()

            with get_db() as conn:
                if partial:
                    conn.execute(
                        """
                        UPDATE paper_trades
                        SET target1_hit_time = ?, target1_exit_price = ?, target1_qty = ?,
                            target1_pnl = ?, quantity = ?, last_checked_price = ?,
                            last_checked_time = ?, last_error = NULL
                        WHERE id = ?
                        """,
                        (now, partial["exit_price"], partial["qty"], partial["pnl"],
                         partial["remaining_qty"], last_price, now, trade["id"]),
                    )
                elif trail_update and "trail_high_pct" in trail_update:
                    conn.execute(
                        """
                        UPDATE paper_trades
                        SET trail_high_pct = ?, last_checked_price = ?,
                            last_checked_time = ?, last_error = NULL
                        WHERE id = ?
                        """,
                        (trail_update["trail_high_pct"], last_price, now, trade["id"]),
                    )
                elif trail_update and "atr_trail_peak_price" in trail_update:
                    conn.execute(
                        """
                        UPDATE paper_trades
                        SET atr_trail_peak_price = ?, last_checked_price = ?,
                            last_checked_time = ?, last_error = NULL
                        WHERE id = ?
                        """,
                        (trail_update["atr_trail_peak_price"], last_price, now, trade["id"]),
                    )
                elif exited:
                    qty = trade["quantity"] or 1
                    # Always a long option position (Call or Put bought,
                    # never written) - profit is always exit minus entry,
                    # regardless of alert direction. Combines this final
                    # leg with whatever Target-1 already booked, if any.
                    leg_pnl = round((exit_price - entry_price) * qty, 2)
                    total_pnl = round(leg_pnl + (trade["target1_pnl"] or 0), 2)
                    original_qty = trade["original_quantity"] or qty
                    capital_used = entry_price * original_qty
                    pnl_pct = round((total_pnl / capital_used) * 100, 2) if capital_used else 0
                    conn.execute(
                        """
                        UPDATE paper_trades
                        SET status = 'CLOSED', exit_price = ?, exit_time = ?,
                            pnl = ?, pnl_pct = ?, exit_reason = ?, last_checked_price = ?,
                            last_checked_time = ?, last_error = NULL
                        WHERE id = ?
                        """,
                        (exit_price, now, total_pnl, pnl_pct, exit_reason, last_price, now, trade["id"]),
                    )
                    closed += 1
                elif trade["status"] == "OPEN":
                    conn.execute(
                        "UPDATE paper_trades SET last_checked_price = ?, last_checked_time = ?, last_error = NULL WHERE id = ?",
                        (last_price, now, trade["id"]),
                    )
                conn.commit()
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                body = ""
            msg = f"Upstox API error {e.code} for {symbol}: {body}"
            errors.append(msg)
            with get_db() as conn:
                conn.execute(
                    "UPDATE paper_trades SET last_error = ?, last_checked_time = ? WHERE id = ?",
                    (msg, now, trade["id"]),
                )
                conn.commit()
        except Exception as e:
            msg = f"{symbol}: {e}"
            errors.append(msg)
            with get_db() as conn:
                conn.execute(
                    "UPDATE paper_trades SET last_error = ?, last_checked_time = ? WHERE id = ?",
                    (msg, now, trade["id"]),
                )
                conn.commit()

    return {"checked": checked, "closed": closed, "errors": errors}


@app.route("/api/sectors")
def api_sectors():
    """Full sector list with today's % change (average of that sector's own
    constituent stocks). % change is null for any sector where it couldn't
    be computed (no token saved, or the batch quote call failed) - the
    sidebar still shows every sector name in that case, just without a
    number yet."""
    access_token = get_setting("upstox_access_token")
    perf = get_sector_performance_cached(access_token)
    result = [
        {
            "sector": sector,
            "pct_change": (perf or {}).get(sector, {}).get("pct_change"),
        }
        for sector in ALL_SECTOR_NAMES
    ]
    return jsonify(result)


@app.route("/api/paper-trading/debug-sector-perf")
def debug_sector_perf():
    """Diagnostic-only: forces a fresh sector performance fetch and shows
    the real error if it fails, instead of the sidebar just showing '-'.
    Note: previous closes load once per day in the background (~180 calls),
    so right after a redeploy or at the start of a new day this may show
    'still loading' for up to a minute or two before real numbers appear."""
    access_token = get_setting("upstox_access_token")
    if not access_token:
        return jsonify({"ok": False, "error": "No Upstox access token saved"})
    global _sector_perf_cache
    _sector_perf_cache = {"data": None, "fetched_at": None}  # force a fresh fetch
    value = get_sector_performance_cached(access_token)
    return jsonify({
        "value": value,
        "prev_close_cache_date": _prev_close_cache["date"],
        "prev_closes_loaded": len(_prev_close_cache["closes"]),
        "prev_close_still_loading": _prev_close_loading,
        **_sector_perf_debug,
    })


@app.route("/")
def index():
    try:
        selected_date = request.args.get("date", "")
        today_str = (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d")
        if not selected_date:
            selected_date = today_str

        with get_db() as conn:
            all_alerts = conn.execute(
                "SELECT * FROM alerts ORDER BY id DESC LIMIT 2000"
            ).fetchall()

        # Group every stored alert by its IST calendar date, so "today" and
        # past days both work correctly regardless of server (UTC) time.
        available_dates = sorted(
            {ist_date_str(a["created_at"]) for a in all_alerts if a["created_at"]},
            reverse=True,
        )
        if selected_date not in available_dates and available_dates:
            selected_date = available_dates[0]

        alerts = [
            dict(a) for a in all_alerts if ist_date_str(a["created_at"]) == selected_date
        ]
        for a in alerts:
            a["category"] = categorize(a)
            a["sector"] = get_sector(a.get("symbol", ""))
        all_sectors = sorted({a["sector"] for a in alerts})
        grouped = group_by_category(alerts)
        merged_count = sum(len(items) for _, items in grouped)

        html = render_template(
            "index.html",
            active_tab="dashboard",
            alerts_count=merged_count,
            grouped=grouped,
            all_sectors=all_sectors,
            all_sector_names=ALL_SECTOR_NAMES,
            available_dates=available_dates,
            selected_date=selected_date,
            today_str=today_str,
            buy_alerts_enabled=get_buy_alerts_enabled(),
            sell_alerts_enabled=get_sell_alerts_enabled(),
        )
    except Exception:
        import traceback
        # Surface the real error instead of ever returning a silent blank
        # page - this makes any future problem immediately visible.
        return f"<pre>{traceback.format_exc()}</pre>", 500

    # Build the response manually and set Content-Length explicitly. Some
    # proxies mishandle chunked responses (no Content-Length) for HTML
    # pages, which can result in the browser receiving an empty body even
    # though the server rendered the page correctly. Setting this directly
    # avoids relying on chunked transfer-encoding at all.
    body = html.encode("utf-8")
    response = make_response(body)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Length"] = str(len(body))
    return response


@app.route("/api/alerts")
def api_alerts():
    selected_date = request.args.get("date", "")
    today_str = (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d")
    if not selected_date:
        selected_date = today_str

    with get_db() as conn:
        all_alerts = conn.execute(
            "SELECT * FROM alerts ORDER BY id DESC LIMIT 2000"
        ).fetchall()

    alerts = [
        dict(a) for a in all_alerts if ist_date_str(a["created_at"]) == selected_date
    ]
    for a in alerts:
        a["category"] = categorize(a)
        a["sector"] = get_sector(a.get("symbol", ""))

    by_category: dict[str, list[dict]] = {}
    for a in alerts:
        by_category.setdefault(a["category"], []).append(a)

    merged_alerts: list[dict] = []
    for name in CATEGORY_ORDER:
        if name in by_category:
            merged_alerts.extend(merge_duplicate_symbols(by_category[name]))

    return jsonify(merged_alerts)


@app.route("/webhook/chartink", methods=["POST"])
def chartink_webhook():
    data = request.get_json(silent=True)
    if data is None:
        data = request.form.to_dict()

    save_alert_batch(data)
    create_paper_trades_for_batch(data)
    return jsonify({"status": "ok"}), 200


@app.route("/api/alerts/<int:alert_id>/enter", methods=["POST"])
def api_manual_enter_alert(alert_id):
    """Manually opens a trade for one specific alert picked off the
    dashboard - typically a signal that was missed because another trade
    was already open at the time it fired, and has since closed. Uses the
    exact same entry logic as the normal webhook flow
    (open_trade_for_symbol), just triggered by a button instead of
    matching a live incoming alert. Still respects the single-position
    rule - refuses if a trade is already open, same as the automatic
    path would."""
    with get_db() as conn:
        alert = conn.execute("SELECT * FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    if not alert:
        return jsonify({"status": "error", "message": "Alert not found"}), 404

    alert_dict = dict(alert)
    category = categorize(alert_dict)
    if category not in ("Buy", "Sell"):
        return jsonify({"status": "error", "message": "Could not determine Buy/Sell for this alert"}), 400

    with get_db() as conn:
        already_open = conn.execute(
            "SELECT id FROM paper_trades WHERE status = 'OPEN' LIMIT 1"
        ).fetchone()
    if already_open:
        return jsonify({"status": "error", "message": "A trade is already open - exit it first before manually entering another"}), 409

    symbol = alert_dict.get("symbol")
    try:
        price_val = float(alert_dict.get("trigger_price") or 0)
    except (TypeError, ValueError):
        price_val = 0
    if not symbol or price_val <= 0:
        return jsonify({"status": "error", "message": "This alert doesn't have a usable symbol/price"}), 400

    open_trade_for_symbol(symbol, category, price_val)
    return jsonify({"status": "ok", "symbol": symbol, "category": category})


@app.route("/api/chart/<symbol>")
def api_chart(symbol):
    """On-demand candle data for the dashboard's per-alert Chart button -
    fetched only when a user actually clicks Chart on a specific alert,
    never automatically for the whole alert list. Loading candles for
    every alert on the page at once would genuinely slow the dashboard
    and hammer Upstox's API for no reason - this keeps that cost at
    exactly one API call per click, nothing more (two, counting the
    separate previous-day high/low lookup).

    Also returns:
    - A 5-EMA overlay of the underlying's own lows (for a Buy position)
      or highs (for a Sell position) - the same line the EMA_SPOT
      strategies (#7/#8) trail.
    - Which 5-min candles are "inside bars" (high <= previous candle's
      high AND low >= previous candle's low) - the frontend highlights
      these in dark blue, since the inside-bar pattern is part of the
      user's own strategy.
    - The previous COMPLETED trading day's high and low, for the PDH/PDL
      reference lines - also part of the strategy."""
    access_token = get_setting("upstox_access_token")
    if not access_token:
        return jsonify({"status": "error", "message": "No Upstox access token saved yet."}), 400

    instrument_key = get_instrument_key(symbol)
    if not instrument_key:
        return jsonify({"status": "error", "message": f"Could not resolve an instrument key for {symbol}"}), 404

    try:
        candles = fetch_5min_candles_with_time(instrument_key, access_token)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to fetch candles: {e}"}), 502

    if not candles:
        return jsonify({"status": "error", "message": "No candle data available yet for today"}), 404

    direction = request.args.get("direction", "Buy")
    series = [c[3] for c in candles] if direction == "Buy" else [c[2] for c in candles]  # lows or highs
    ema_series = calculate_ema(series, 5)

    # Inside bar: current candle's whole range sits within the PRIOR
    # candle's range. The first candle of the day has no prior candle to
    # compare against, so it's never flagged.
    is_inside_bar = [False]
    for i in range(1, len(candles)):
        _, _, prev_h, prev_l, _ = candles[i - 1]
        _, _, h, l, _ = candles[i]
        is_inside_bar.append(h <= prev_h and l >= prev_l)

    prev_day_high, prev_day_low = fetch_previous_day_high_low(instrument_key, access_token)

    return jsonify({
        "status": "ok",
        "symbol": symbol,
        "labels": [c[0] for c in candles],
        "ohlc": [{"o": c[1], "h": c[2], "l": c[3], "c": c[4]} for c in candles],
        "is_inside_bar": is_inside_bar,
        "ema": ema_series,
        "ema_label": "EMA5(low)" if direction == "Buy" else "EMA5(high)",
        "prev_day_high": prev_day_high,
        "prev_day_low": prev_day_low,
    })


@app.route("/clear", methods=["POST"])
def clear_alerts():
    with get_db() as conn:
        conn.execute("DELETE FROM alerts")
        conn.commit()
    return redirect(url_for("index"))


def attach_stop_info(open_trades: list[dict], access_token: str | None) -> None:
    """Adds a human-readable 'stop_info' string (and 'atr_value' when
    relevant) to each open trade, showing exactly where its stop actually
    sits right now - not just the exit_reason that appears after the fact
    once it's hit. Strategy-aware: reads the same fields/logic
    run_paper_trade_check uses to decide exits, so this is a live view of
    the real stop, not an approximation. For ATR_TRAIL specifically, this
    also does a fresh ATR calculation off the latest candles (one extra
    Upstox call per open trade using that strategy - never more than one,
    since only a single position is open at a time in this app)."""
    for t in open_trades:
        strategy = t.get("strategy") or "EMA"
        entry = t.get("entry_price")
        if not entry:
            t["stop_info"] = None
            t["atr_value"] = None
            continue

        if strategy == "HALF_HALF_HARD4":
            stop_pct = 0.0 if t.get("target1_hit_time") else -2.0
            stop_price = entry * (1 + stop_pct / 100)
            t["stop_info"] = f"{'Breakeven' if stop_pct == 0 else 'Stop'}: {stop_pct:+.1f}% ({stop_price:.2f})"
            t["atr_value"] = None

        elif strategy == "TRAIL_FROM_2":
            trail_high = t.get("trail_high_pct")
            stop_pct = -2.0 if trail_high is None else round(trail_high - 0.2, 2)
            stop_price = entry * (1 + stop_pct / 100)
            t["stop_info"] = f"{'Stop' if trail_high is None else 'Trail stop'}: {stop_pct:+.2f}% ({stop_price:.2f})"
            t["atr_value"] = None

        elif strategy in ("FULL_AT_2", "FULL_AT_4"):
            stop_price = entry * 0.98
            t["stop_info"] = f"Stop: -2.0% ({stop_price:.2f})"
            t["atr_value"] = None

        elif strategy == "ATR_TRAIL":
            trail_high = t.get("trail_high_pct")
            if trail_high is not None:
                # Past +2% - in the plain 0.5%-step trailing phase.
                stop_pct = round(trail_high - 0.5, 2)
                stop_price = entry * (1 + stop_pct / 100)
                t["stop_info"] = f"Trail stop: {stop_pct:+.2f}% ({stop_price:.2f})"
                t["atr_value"] = None
            else:
                # Still in the ATR phase - recompute live off fresh candles.
                current_atr = None
                instrument_key = t.get("paper_instrument_key")
                if instrument_key and access_token:
                    try:
                        candles = fetch_5min_candles(instrument_key, access_token)
                        atr_series = calculate_atr(candles, get_atr_period())
                        current_atr = atr_series[-1] if atr_series else None
                    except Exception:
                        current_atr = None
                if current_atr is None:
                    t["stop_info"] = "Stop: -2.0% (ATR warming up)"
                    t["atr_value"] = None
                else:
                    last_price = t.get("last_checked_price")
                    peak_price = t.get("atr_trail_peak_price") or entry
                    if last_price is not None:
                        peak_price = max(peak_price, last_price)
                    atr_mult = get_atr_multiplier()
                    stop_price = peak_price - (current_atr * atr_mult)
                    stop_price = max(stop_price, entry * 0.98)  # never display looser than the -2% floor
                    stop_pct = round((stop_price / entry - 1) * 100, 2)
                    t["atr_value"] = round(current_atr, 2)
                    t["stop_info"] = f"ATR stop: {stop_price:.2f} ({stop_pct:+.2f}%)"

        elif strategy in ("EMA_SPOT_TRAIL", "EMA_SPOT_PURE"):
            # Informational only - unlike the others, this isn't a single
            # stop price. Exit needs 2 consecutive spot candles closing
            # beyond the current 5-EMA, so show the live 5-EMA(low/high)
            # value as context, not something that fires the instant
            # price touches it.
            spot_ema_note = ""
            spot_key = t.get("symbol")
            if spot_key and access_token:
                try:
                    resolved_key = get_instrument_key(spot_key)
                    if resolved_key:
                        spot_candles = fetch_5min_candles(resolved_key, access_token)
                        if spot_candles:
                            series = [c[2] for c in spot_candles] if t.get("direction") == "Buy" else [c[1] for c in spot_candles]
                            ema_series = calculate_ema(series, 5)
                            current_ema = ema_series[-1] if ema_series else None
                            if current_ema is not None:
                                label = "EMA5(low)" if t.get("direction") == "Buy" else "EMA5(high)"
                                spot_ema_note = f" | spot {label}: {current_ema:.2f}"
                except Exception:
                    spot_ema_note = ""
            if strategy == "EMA_SPOT_TRAIL":
                stop_price = entry * 0.98
                t["stop_info"] = f"Stop: -2.0% ({stop_price:.2f}){spot_ema_note}"
            else:
                t["stop_info"] = f"No floor - pattern exit only{spot_ema_note}"
            t["atr_value"] = None

        else:
            t["stop_info"] = None
            t["atr_value"] = None


def attach_unrealized_pnl(open_trades: list[dict]) -> None:
    """Adds 'unrealized_pnl' and 'unrealized_pnl_pct' to each open trade.
    The percentage is against the capital actually deployed for that
    specific trade (entry_price * quantity), not the account's total
    current capital - so it answers 'what % gain/loss is this trade at',
    useful for deciding whether a target has been hit."""
    for t in open_trades:
        last_price = t.get("last_checked_price")
        qty = t.get("quantity") or 1
        entry = t["entry_price"]
        capital_used = entry * qty
        if last_price is None:
            t["unrealized_pnl"] = None
            t["unrealized_pnl_pct"] = None
            continue
        # Always a long option position (Call or Put bought, never
        # written) - profit is always current price minus entry,
        # regardless of alert direction.
        pnl = (last_price - entry) * qty
        t["unrealized_pnl"] = round(pnl, 2)
        t["unrealized_pnl_pct"] = round((pnl / capital_used) * 100, 2) if capital_used else 0


def attach_live_unrealized_pnl(open_trades: list[dict]) -> None:
    """Same as attach_unrealized_pnl but sized off live_quantity and priced
    off live_entry_price (the REAL fill price) - not the paper trade's own
    quantity/entry_price, since the two are sized/priced from different
    sources (paper capital + theoretical premium vs real Upstox funds +
    real order fill)."""
    for t in open_trades:
        last_price = t.get("last_checked_price")
        qty = t.get("live_quantity") or 0
        entry = t.get("live_entry_price") if t.get("live_entry_price") is not None else t.get("entry_price")
        if last_price is None or not qty or entry is None:
            t["unrealized_pnl"] = None
            t["unrealized_pnl_pct"] = None
            continue
        capital_used = entry * qty
        pnl = (last_price - entry) * qty
        t["unrealized_pnl"] = round(pnl, 2)
        t["unrealized_pnl_pct"] = round((pnl / capital_used) * 100, 2) if capital_used else 0


def attach_running_balance(closed_trades_desc: list[dict], starting_capital: float) -> None:
    """closed_trades_desc: newest first (as queried). Adds 'balance_after'
    to each trade - the account balance right after that trade closed,
    computed by walking the trades oldest-to-newest and accumulating P&L.
    This is what actually shows the compounding progression over time."""
    running = starting_capital
    for t in reversed(closed_trades_desc):  # oldest first for the running sum
        running += t["pnl"] or 0
        t["balance_after"] = round(running, 2)


def group_trades_by_date(trades: list[dict], date_field: str = "exit_time", pnl_field: str = "pnl") -> list[dict]:
    """Groups closed trades by exit date into [{"date", "total_pnl",
    "trades"}, ...], most recent date first - powers the collapsible
    per-day view (like a trading diary). date_field/pnl_field let this
    serve both paper trades (exit_time/pnl) and live trades
    (live_exit_time/live_pnl_value), which can now close at different
    times/prices since the two run independent exit rules. Rows aren't
    guaranteed to already be in date order here (unlike the paper-only
    case, since live_exit_time isn't tied to insertion order the way
    exit_time was), so this sorts by the date field first."""
    trades_sorted = sorted(trades, key=lambda t: t.get(date_field) or "", reverse=True)
    groups: list[dict] = []
    current_date = None
    for t in trades_sorted:
        exit_date = (t.get(date_field) or "")[:10] or "Unknown"
        if exit_date != current_date:
            groups.append({"date": exit_date, "total_pnl": 0.0, "trades": []})
            current_date = exit_date
        groups[-1]["trades"].append(t)
        groups[-1]["total_pnl"] += t.get(pnl_field) or 0
    for g in groups:
        g["total_pnl"] = round(g["total_pnl"], 2)
    return groups


@app.route("/paper-trading")
def paper_trading():
    with get_db() as conn:
        open_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE status = 'OPEN' ORDER BY id DESC"
        ).fetchall()
        closed_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE status = 'CLOSED' ORDER BY id DESC LIMIT 200"
        ).fetchall()

    open_trades = [dict(t) for t in open_trades]
    closed_trades = [dict(t) for t in closed_trades]
    attach_unrealized_pnl(open_trades)
    attach_stop_info(open_trades, get_setting("upstox_access_token"))

    total_pnl = sum(t["pnl"] for t in closed_trades if t["pnl"] is not None)
    wins = sum(1 for t in closed_trades if (t["pnl"] or 0) > 0)
    total_closed = len(closed_trades)
    win_rate = round((wins / total_closed) * 100, 1) if total_closed else 0

    token_saved = bool(get_setting("upstox_access_token"))
    capital = get_capital()
    attach_running_balance(closed_trades, capital)
    closed_by_date = group_trades_by_date(closed_trades)
    current_capital = get_current_capital()
    live_trading_enabled = get_live_trading_enabled()
    exit_strategy = get_exit_strategy()
    sector_filter_enabled = get_sector_filter_enabled()
    sector_filter_top_n = get_sector_filter_top_n()

    live_stats = compute_live_stats(open_trades, closed_trades)
    live_available_funds = None
    if token_saved:
        live_available_funds = get_upstox_available_funds_for_display(get_setting("upstox_access_token"))

    html = render_template(
        "paper_trading.html",
        active_tab="paper",
        open_trades=open_trades,
        closed_trades=closed_trades,
        closed_by_date=closed_by_date,
        total_pnl=round(total_pnl, 2),
        win_rate=win_rate,
        total_closed=total_closed,
        wins=wins,
        token_saved=token_saved,
        capital=capital,
        current_capital=round(current_capital, 2),
        live_trading_enabled=live_trading_enabled,
        exit_strategy=exit_strategy,
        sector_filter_enabled=sector_filter_enabled,
        sector_filter_top_n=sector_filter_top_n,
        live_open=live_stats["live_open"],
        live_closed=live_stats["live_closed"],
        live_pnl=live_stats["live_pnl"],
        live_available_funds=live_available_funds,
    )
    body = html.encode("utf-8")
    response = make_response(body)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Length"] = str(len(body))
    return response


@app.route("/stats")
def stats_page():
    html = render_template("stats.html", active_tab="stats")
    body = html.encode("utf-8")
    response = make_response(body)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Length"] = str(len(body))
    return response


@app.route("/api/stats/pnl-curve")
def api_stats_pnl_curve():
    """Two independent equity/P&L curves - paper and live are fully
    decoupled (different exit rules, different money), so they're built
    and returned separately rather than combined into one line.

    Paper: starting capital + cumulative P&L, walked oldest-to-newest -
    the same running-balance math attach_running_balance uses for the
    Paper Trading page's closed-trades table, just returned as a time
    series instead of attached per-row.

    Live: cumulative REAL P&L (live_entry_price/live_exit_price, the
    actual fill prices - see compute_live_stats), starting at 0 rather
    than a seeded capital figure, since live trading draws on the
    Upstox account's real funds rather than a seeded pool this app
    tracks."""
    with get_db() as conn:
        paper_rows = conn.execute(
            "SELECT exit_time, pnl FROM paper_trades WHERE status = 'CLOSED' AND exit_time IS NOT NULL ORDER BY exit_time ASC"
        ).fetchall()
        live_rows = conn.execute(
            "SELECT live_exit_time, live_entry_price, live_exit_price, live_quantity, entry_price, exit_price "
            "FROM paper_trades WHERE live_status = 'CLOSED' AND live_exit_time IS NOT NULL ORDER BY live_exit_time ASC"
        ).fetchall()

    paper_points = []
    running = get_capital()
    for r in paper_rows:
        running += r["pnl"] or 0
        paper_points.append({"time": r["exit_time"], "balance": round(running, 2)})

    live_points = []
    running_live = 0.0
    for r in live_rows:
        qty = r["live_quantity"] or 0
        entry = r["live_entry_price"] if r["live_entry_price"] is not None else (r["entry_price"] or 0)
        exit_p = r["live_exit_price"] if r["live_exit_price"] is not None else (r["exit_price"] or 0)
        running_live += (exit_p - entry) * qty
        live_points.append({"time": r["live_exit_time"], "pnl": round(running_live, 2)})

    return jsonify({"paper": paper_points, "live": live_points})


@app.route("/settings")
def settings_page():
    token_saved = bool(get_setting("upstox_access_token"))
    capital = get_capital()
    html = render_template(
        "settings.html",
        active_tab="settings",
        token_saved=token_saved,
        capital=capital,
        live_trading_enabled=get_live_trading_enabled(),
        exit_strategy=get_exit_strategy(),
        strategy_descriptions=STRATEGY_DESCRIPTIONS,
        sector_filter_enabled=get_sector_filter_enabled(),
        sector_filter_top_n=get_sector_filter_top_n(),
        atr_period=get_atr_period(),
        atr_multiplier=get_atr_multiplier(),
        entry_time_filter=get_entry_time_filter() or "",
        theta_switch_enabled=get_theta_switch_enabled(),
    )
    body = html.encode("utf-8")
    response = make_response(body)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Length"] = str(len(body))
    return response


@app.route("/live-trading")
def live_trading_page():
    with get_db() as conn:
        open_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE live_status = 'OPEN' ORDER BY id DESC"
        ).fetchall()
        closed_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE live_status = 'CLOSED' ORDER BY id DESC LIMIT 200"
        ).fetchall()

    open_trades = [dict(t) for t in open_trades]
    closed_trades = [dict(t) for t in closed_trades]
    attach_live_unrealized_pnl(open_trades)
    for t in closed_trades:
        qty = t.get("live_quantity") or 0
        entry = t.get("live_entry_price") if t.get("live_entry_price") is not None else (t.get("entry_price") or 0)
        exit_p = t.get("live_exit_price") if t.get("live_exit_price") is not None else (t.get("exit_price") or 0)
        t["live_pnl_value"] = round((exit_p - entry) * qty, 2)

    live_stats = compute_live_stats(open_trades, closed_trades)
    closed_by_date = group_trades_by_date(closed_trades, date_field="live_exit_time", pnl_field="live_pnl_value")
    token_saved = bool(get_setting("upstox_access_token"))
    live_available_funds = None
    if token_saved:
        live_available_funds = get_upstox_available_funds_for_display(get_setting("upstox_access_token"))

    html = render_template(
        "live_trading.html",
        active_tab="live",
        open_trades=open_trades,
        closed_trades=closed_trades,
        closed_by_date=closed_by_date,
        live_trading_enabled=get_live_trading_enabled(),
        live_open=live_stats["live_open"],
        live_closed=live_stats["live_closed"],
        live_pnl=live_stats["live_pnl"],
        live_available_funds=live_available_funds,
    )
    body = html.encode("utf-8")
    response = make_response(body)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Length"] = str(len(body))
    return response


@app.route("/api/live-trading/data")
def live_trading_data():
    """Dedicated data endpoint for the Live Trading page - deliberately
    separate from /api/paper-trading/data since paper and live now run
    independent exit rules and can be open/closed in different states at
    the same time. This one is always keyed off live_status, never the
    paper trade's own status."""
    with get_db() as conn:
        open_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE live_status = 'OPEN' ORDER BY id DESC"
        ).fetchall()
        closed_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE live_status = 'CLOSED' ORDER BY id DESC LIMIT 200"
        ).fetchall()
    open_trades = [dict(t) for t in open_trades]
    closed_trades = [dict(t) for t in closed_trades]
    attach_live_unrealized_pnl(open_trades)
    for t in closed_trades:
        qty = t.get("live_quantity") or 0
        entry = t.get("live_entry_price") if t.get("live_entry_price") is not None else (t.get("entry_price") or 0)
        exit_p = t.get("live_exit_price") if t.get("live_exit_price") is not None else (t.get("exit_price") or 0)
        t["live_pnl_value"] = round((exit_p - entry) * qty, 2)

    live_stats = compute_live_stats(open_trades, closed_trades)
    access_token = get_setting("upstox_access_token")
    live_available_funds = get_upstox_available_funds_for_display(access_token) if access_token else None

    return jsonify({
        "open": open_trades,
        "closed": closed_trades,
        "live_open": live_stats["live_open"],
        "live_closed": live_stats["live_closed"],
        "live_pnl": live_stats["live_pnl"],
        "live_available_funds": live_available_funds,
    })


@app.route("/api/paper-trading/data")
def paper_trading_data():
    with get_db() as conn:
        open_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE status = 'OPEN' ORDER BY id DESC"
        ).fetchall()
        closed_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE status = 'CLOSED' ORDER BY id DESC LIMIT 200"
        ).fetchall()
    open_trades = [dict(t) for t in open_trades]
    closed_trades = [dict(t) for t in closed_trades]
    attach_unrealized_pnl(open_trades)
    attach_running_balance(closed_trades, get_capital())

    live_stats = compute_live_stats(open_trades, closed_trades)
    access_token = get_setting("upstox_access_token")
    attach_stop_info(open_trades, access_token)
    live_available_funds = get_upstox_available_funds_for_display(access_token) if access_token else None

    return jsonify({
        "open": open_trades,
        "closed": closed_trades,
        "current_capital": round(get_current_capital(), 2),
        "live_trading_enabled": get_live_trading_enabled(),
        "exit_strategy": get_exit_strategy(),
        "live_open": live_stats["live_open"],
        "live_closed": live_stats["live_closed"],
        "live_pnl": live_stats["live_pnl"],
        "live_available_funds": live_available_funds,
    })


@app.route("/api/paper-trading/settings", methods=["POST"])
def paper_trading_settings():
    data = request.get_json(silent=True) or {}
    token = (data.get("access_token") or "").strip()
    if not token:
        return jsonify({"status": "error", "message": "No token provided"}), 400
    set_setting("upstox_access_token", token)
    return jsonify({"status": "ok"})


@app.route("/api/paper-trading/capital", methods=["POST"])
def paper_trading_capital():
    data = request.get_json(silent=True) or {}
    try:
        capital = float(data.get("capital"))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Invalid amount"}), 400
    if capital <= 0:
        return jsonify({"status": "error", "message": "Amount must be positive"}), 400
    set_setting("paper_trade_capital", str(capital))
    return jsonify({"status": "ok", "capital": capital})


@app.route("/api/paper-trading/live-toggle", methods=["POST"])
def paper_trading_live_toggle():
    """Master on/off switch for placing real Upstox orders. Off by default -
    turning this on means the NEXT Buy alert (and its eventual exit) will
    place real CNC market orders with real money."""
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))
    set_setting("live_trading_enabled", "true" if enabled else "false")
    return jsonify({"status": "ok", "live_trading_enabled": enabled})


VALID_STRATEGIES = ("HALF_HALF_HARD4", "TRAIL_FROM_2", "FULL_AT_2", "FULL_AT_4", "ATR_TRAIL", "EMA_SPOT_TRAIL", "EMA_SPOT_PURE")

STRATEGY_DESCRIPTIONS = {
    "EMA": "1) 5-EMA only, let it run - retired from selection (let losses run too deep waiting for a pattern reversal to confirm). Kept only for trades that already opened under it.",
    "HALF_HALF_HARD4": "2) Hard -2% stop-loss protects until +2% (immediate, not pattern-based). At +2%, half the qty books (Target-1) and the rest is protected at breakeven. At +4%, the remaining half exits in full - a hard target, no trailing beyond it.",
    "TRAIL_FROM_2": "3) Hard -2% stop-loss protects until +2%. No half-booking - the full quantity starts trailing from +2%, in 0.2% steps, always one step behind the peak.",
    "FULL_AT_2": "4) Hard -2% stop-loss protects until +2%, then the full quantity exits at +2% - one fixed target, 1:1 risk-reward.",
    "FULL_AT_4": "5) Hard -2% stop-loss protects until +4%, then the full quantity exits at +4% - one fixed target, 1:2 risk-reward.",
    "ATR_TRAIL": "6) Hybrid ATR + trail: an ATR (Chandelier-style) stop protects the downside until +2% profit - stop = highest price since entry minus (ATR x multiplier), widening automatically in choppy conditions and tightening when calm. A -2% floor covers the trade until enough candle history exists for the first ATR reading. Once +2% is reached, hands off entirely to a plain 0.5% step trailing stop on the % gain, to lock in profit mechanically. Uses the ATR period/multiplier set below.",
    "EMA_SPOT_TRAIL": "7) 5-EMA (spot) trail with -2% floor: a hard -2% stop-loss protects the trade throughout. On top of that, the full quantity exits the moment the UNDERLYING stock (not the option premium) closes 2 consecutive candles beyond its own 5-EMA - below EMA(5) of the lows for a Buy, above EMA(5) of the highs for a Sell. Tracks the same clean trend line you'd watch on the spot chart, instead of the option premium's noisier candles.",
    "EMA_SPOT_PURE": "8) Pure 5-EMA (spot) trail, NO floor: same underlying-based 5-EMA rule as #7 (2 consecutive candles closing beyond EMA(5) of lows/highs), but with no -2% stop underneath it. The trade is never cut off before the EMA itself gets a chance to trail - deliberately unprotected on the downside, so a losing trade can run further than any other strategy here before this fires.",
}


@app.route("/api/paper-trading/strategy-toggle", methods=["POST"])
def paper_trading_strategy_toggle():
    """Switches which exit strategy the NEXT new trade uses. Trades already
    open keep whatever strategy was active when they opened (stored per
    trade), so switching mid-day never changes the rules for a position
    already in flight."""
    data = request.get_json(silent=True) or {}
    strategy = data.get("strategy")
    if strategy not in VALID_STRATEGIES:
        return jsonify({"status": "error", "message": f"strategy must be one of {VALID_STRATEGIES}"}), 400
    set_setting("exit_strategy", strategy)
    return jsonify({"status": "ok", "exit_strategy": strategy, "description": STRATEGY_DESCRIPTIONS.get(strategy, "")})


@app.route("/api/paper-trading/atr-settings", methods=["POST"])
def paper_trading_atr_settings():
    """Sets the ATR period and multiplier used by the ATR_TRAIL exit
    strategy. Takes effect immediately for any open trade already running
    that strategy (the check reads these settings fresh every pass), not
    just new trades - unlike exit_strategy, which is locked in per-trade
    at entry."""
    data = request.get_json(silent=True) or {}
    try:
        period = max(2, int(data.get("atr_period")))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "atr_period must be a whole number >= 2"}), 400
    try:
        multiplier = max(0.1, float(data.get("atr_multiplier")))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "atr_multiplier must be a number >= 0.1"}), 400
    set_setting("atr_period", str(period))
    set_setting("atr_multiplier", str(multiplier))
    return jsonify({"status": "ok", "atr_period": period, "atr_multiplier": multiplier})


@app.route("/api/paper-trading/entry-time-filter", methods=["POST"])
def paper_trading_entry_time_filter():
    """Sets (or clears) the earliest clock time new trades are allowed to
    open - lets the noisier opening minutes be skipped entirely. Takes
    effect on the very next alert; doesn't touch anything already open."""
    data = request.get_json(silent=True) or {}
    raw = (data.get("entry_time_filter") or "").strip()
    if not raw:
        set_setting("entry_time_filter", "")
        return jsonify({"status": "ok", "entry_time_filter": None})
    try:
        parsed = datetime.strptime(raw, "%H:%M")
    except ValueError:
        return jsonify({"status": "error", "message": "entry_time_filter must be HH:MM (24-hour), e.g. 09:45"}), 400
    value = parsed.strftime("%H:%M")
    set_setting("entry_time_filter", value)
    return jsonify({"status": "ok", "entry_time_filter": value})


@app.route("/api/paper-trading/sector-filter-toggle", methods=["POST"])
def paper_trading_sector_filter_toggle():
    """Turns the sector-momentum filter on/off and sets the top-N cutoff.
    When on, an alert only opens a trade if its sector currently ranks
    among the top N (Buy) or bottom N (Sell) by today's % change."""
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))
    top_n = data.get("top_n")
    set_setting("sector_filter_enabled", "true" if enabled else "false")
    if top_n is not None:
        try:
            top_n = max(1, int(top_n))
            set_setting("sector_filter_top_n", str(top_n))
        except (TypeError, ValueError):
            pass
    return jsonify({
        "status": "ok",
        "sector_filter_enabled": enabled,
        "sector_filter_top_n": get_sector_filter_top_n(),
    })


@app.route("/api/paper-trading/theta-switch-toggle", methods=["POST"])
def paper_trading_theta_switch_toggle():
    """Turns the next-month expiry auto-roll on/off. When on, new trades
    whose nearest monthly expiry has crossed into the back (high-decay)
    half of its own trading-day cycle open in the NEXT monthly expiry
    instead - see get_atm_option for the actual trading-day math."""
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))
    set_setting("theta_switch_enabled", "true" if enabled else "false")
    return jsonify({"status": "ok", "theta_switch_enabled": enabled})


@app.route("/api/paper-trading/category-toggle", methods=["POST"])
def paper_trading_category_toggle():
    """Turns Buy-side or Sell-side trade creation on/off. The alert feed on
    the dashboard is unaffected either way - this only controls whether a
    paper/live trade gets created from that side's alerts."""
    data = request.get_json(silent=True) or {}
    category = data.get("category")
    enabled = bool(data.get("enabled"))
    if category not in ("Buy", "Sell"):
        return jsonify({"status": "error", "message": "category must be 'Buy' or 'Sell'"}), 400
    set_setting(f"{category.lower()}_alerts_enabled", "true" if enabled else "false")
    return jsonify({"status": "ok", "category": category, "enabled": enabled})


@app.route("/api/paper-trading/reset", methods=["POST"])
def paper_trading_reset():
    """Clears all paper trades so single-position sequential trading can
    start clean - needed once when switching from the old model (many
    simultaneous trades) to this one (one at a time)."""
    with get_db() as conn:
        conn.execute("DELETE FROM paper_trades")
        conn.commit()
    return jsonify({"status": "ok"})


@app.route("/api/paper-trading/manual-exit/<int:trade_id>", methods=["POST"])
def paper_trading_manual_exit(trade_id):
    """Manually closes a trade right now, at the freshest price we can get -
    since the exit rules don't cover every situation you might want to
    exit on (e.g. hitting a target %, end of day, news, etc.). Handles two
    cases since paper and live now run independent exit rules: the normal
    case (paper still open - closes both together), and a live-only case
    (paper already closed via its own rule, but the live leg is still
    running its own separate exit logic - closes just that)."""
    with get_db() as conn:
        trade = conn.execute(
            "SELECT * FROM paper_trades WHERE id = ? AND (status = 'OPEN' OR live_status = 'OPEN')",
            (trade_id,),
        ).fetchone()
    if not trade:
        return jsonify({"status": "error", "message": "Trade not found or already fully closed"}), 404

    exit_price = None
    access_token = get_setting("upstox_access_token")
    if access_token:
        try:
            instrument_key = trade["paper_instrument_key"] or get_instrument_key(trade["symbol"])
            if instrument_key:
                ws_price = get_ws_price(instrument_key)
                if ws_price is not None:
                    exit_price = ws_price
                else:
                    candles = fetch_5min_candles(instrument_key, access_token)
                    if candles:
                        exit_price = candles[-1][3]
        except Exception:
            pass  # fall back to last_checked_price below

    if exit_price is None:
        exit_price = trade["last_checked_price"]
    if exit_price is None:
        return jsonify({"status": "error", "message": "No price available - save a token and run Check Exits Now at least once first"}), 400

    now = datetime.utcnow().isoformat()

    if trade["live_status"] == "OPEN":
        live_exit_order_id, live_exit_error, live_fill_price = _close_live_position_if_any(trade, access_token)
    else:
        live_exit_order_id, live_exit_error, live_fill_price = None, None, None

    if trade["status"] == "OPEN":
        entry_price = trade["entry_price"]
        qty = trade["quantity"] or 1
        # Always a long option position (Call or Put bought, never
        # written) - profit is always exit minus entry, regardless of
        # alert direction. Combines this leg with whatever Target-1
        # already booked, if any.
        leg_pnl = round((exit_price - entry_price) * qty, 2)
        pnl = round(leg_pnl + (trade["target1_pnl"] or 0), 2)
        original_qty = trade["original_quantity"] or qty
        capital_used = entry_price * original_qty
        pnl_pct = round((pnl / capital_used) * 100, 2) if capital_used else 0
        with get_db() as conn:
            conn.execute(
                """
                UPDATE paper_trades
                SET status = 'CLOSED', exit_price = ?, exit_time = ?,
                    pnl = ?, pnl_pct = ?, exit_reason = ?, last_checked_time = ?,
                    live_exit_order_id = COALESCE(?, live_exit_order_id),
                    live_status = CASE WHEN ? THEN 'CLOSED' ELSE live_status END,
                    live_error = COALESCE(?, live_error),
                    live_exit_reason = CASE WHEN ? THEN 'Manual exit' ELSE live_exit_reason END,
                    live_exit_price = CASE WHEN ? THEN COALESCE(?, live_exit_price) ELSE live_exit_price END,
                    live_exit_time = CASE WHEN ? THEN ? ELSE live_exit_time END
                WHERE id = ?
                """,
                (
                    exit_price, now, pnl, pnl_pct, "Manual exit", now,
                    live_exit_order_id,
                    live_exit_order_id is not None,
                    live_exit_error,
                    live_exit_order_id is not None,
                    live_exit_order_id is not None,
                    live_fill_price,
                    live_exit_order_id is not None,
                    now,
                    trade_id,
                ),
            )
            conn.commit()
        return jsonify({"status": "ok", "exit_price": exit_price, "pnl": pnl, "pnl_pct": pnl_pct})
    else:
        # Paper already closed via its own rule - this manual exit only
        # closes the still-running live leg, leaving the paper trade's
        # own result untouched.
        with get_db() as conn:
            conn.execute(
                """
                UPDATE paper_trades
                SET live_exit_order_id = COALESCE(?, live_exit_order_id),
                    live_status = CASE WHEN ? THEN 'CLOSED' ELSE live_status END,
                    live_error = COALESCE(?, live_error),
                    live_exit_reason = 'Manual exit',
                    live_exit_price = COALESCE(?, live_exit_price),
                    live_exit_time = ?
                WHERE id = ?
                """,
                (live_exit_order_id, live_exit_order_id is not None, live_exit_error, live_fill_price, now, trade_id),
            )
            conn.commit()
        return jsonify({"status": "ok", "exit_price": exit_price, "note": "Closed the live leg only (paper side had already closed)"})


@app.route("/api/paper-trading/check", methods=["POST"])
def paper_trading_check():
    result = run_paper_trade_check()
    return jsonify(result)


@app.route("/api/paper-trading/debug-instruments")
def debug_instruments():
    """Diagnostic-only: forces a fresh download of Upstox's instrument
    master and shows exactly what came back - real column names, sample
    rows, and any error - so a mismatch in the parsing code can be spotted
    and fixed precisely. Includes the option-chain parsing results too."""
    _load_instrument_master()
    return jsonify({"equities": _instrument_debug, "options": _option_debug})


@app.route("/api/paper-trading/debug-proxy-ip")
def debug_proxy_ip():
    """Diagnostic-only: confirms the static-IP proxy is configured and
    check what outbound IP it actually produces, before trusting it with a
    real order. Hits a plain IP-echo service (not Upstox) through the same
    proxy path place_live_order() uses, so this is a safe, order-free way
    to verify the proxy itself works end-to-end."""
    opener = _get_order_proxy_opener()
    if not opener:
        return jsonify({
            "ok": False,
            "error": "Proxy env vars not set (need STATICIP_HOST, STATICIP_PORT, STATICIP_USER, STATICIP_PASS on Render)",
        })
    try:
        req = urllib.request.Request("https://api.ipify.org?format=json", headers={"User-Agent": BROWSER_USER_AGENT})
        with opener.open(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode())
        return jsonify({"ok": True, "outbound_ip_seen": payload.get("ip")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/paper-trading/debug-websocket")
def debug_websocket():
    """Diagnostic-only: shows the WebSocket feed's connection status,
    whether upstox-python-sdk is installed, what's currently subscribed,
    and how many prices are cached - so a connection problem can be
    diagnosed precisely instead of just seeing slower-than-expected
    reactions with no visible cause."""
    access_token = get_setting("upstox_access_token")
    if access_token:
        ensure_websocket_started(access_token)
    with _ws_lock:
        cache_size = len(_ws_price_cache)
        sample_prices = dict(list(_ws_price_cache.items())[:5])
    return jsonify({
        **_ws_debug,
        "thread_started": _ws_thread_started,
        "subscribed_keys": list(_ws_subscribed_keys),
        "cached_price_count": cache_size,
        "sample_prices": {
            k: {"ltp": v["ltp"], "age_seconds": round((datetime.utcnow() - v["updated_at"]).total_seconds(), 1)}
            for k, v in sample_prices.items()
        },
    })


@app.route("/api/paper-trading/debug-atm")
def debug_atm():
    """Diagnostic-only: looks up the ATM CE and PE for a given symbol and
    reference price, e.g. /api/paper-trading/debug-atm?symbol=RELIANCE&price=1400
    - shows the resolved contract or explains why none was found."""
    symbol = request.args.get("symbol", "")
    try:
        price = float(request.args.get("price", ""))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Pass ?symbol=X&price=Y in the URL"})
    _load_instrument_master()
    return jsonify({
        "symbol": symbol.upper(),
        "reference_price": price,
        "chain_length": len(_option_chain_cache.get(symbol.upper(), [])),
        "atm_call": get_atm_option(symbol, "CE", price),
        "atm_put": get_atm_option(symbol, "PE", price),
    })


@app.route("/api/paper-trading/debug-funds")
def debug_funds():
    """Diagnostic-only: forces a fresh call to Upstox's funds-and-margin
    API and shows the real response or error - so a failed fetch (shown as
    just '-' on the page) can be diagnosed precisely."""
    access_token = get_setting("upstox_access_token")
    if not access_token:
        return jsonify({"ok": False, "error": "No Upstox access token saved"})
    value = get_upstox_available_funds(access_token)
    return jsonify({"value": value, **_funds_debug})


@app.route("/api/paper-trading/debug-profile")
def debug_profile():
    """Diagnostic-only: calls Upstox's basic v2 user profile endpoint, which
    needs only the Authorization header (no Api-Version, no static IP
    requirement). If THIS also 401s with the saved token, the token itself
    is the problem, not anything specific to the v3 funds/order code."""
    access_token = get_setting("upstox_access_token")
    if not access_token:
        return jsonify({"ok": False, "error": "No Upstox access token saved"})
    req = urllib.request.Request(
        "https://api.upstox.com/v2/user/profile",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode())
        return jsonify({"ok": True, "raw": payload})
    except urllib.error.HTTPError as e:
        try:
            body_text = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            body_text = ""
        return jsonify({"ok": False, "error": f"HTTP {e.code}: {body_text}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


init_db()  # runs on import too, so gunicorn (used in production) creates the table
ensure_exit_check_loop_started()  # same: starts on import, tab-independent from here on

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
