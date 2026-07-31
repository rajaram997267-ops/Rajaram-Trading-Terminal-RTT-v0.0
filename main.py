from __future__ import annotations

import csv
import gzip
import io
import json
import os
import threading
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
                live_quantity INTEGER
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

    capital = get_current_capital()
    quantity = int(capital // price_val)
    if quantity < 1:
        quantity = 1

    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        row = conn.execute(
            """
            INSERT INTO paper_trades
                (symbol, direction, entry_price, entry_time, status, quantity)
            VALUES (?, ?, ?, ?, 'OPEN', ?)
            RETURNING id
            """,
            (symbol, category, price_val, now, quantity),
        ).fetchone()
        conn.commit()

    # Live trading only ever goes long via CNC (Delivery) - CNC can't open a
    # fresh short, so Sell-direction alerts stay paper-trade only, same as
    # before. Buy alerts also go live, but only when the safety switch is on
    # and a token is saved; any failure here is recorded on the trade and
    # never blocks the paper trade itself.
    if category == "Buy" and get_live_trading_enabled():
        trade_id = row["id"]
        access_token = get_setting("upstox_access_token")
        if not access_token:
            with get_db() as conn:
                conn.execute(
                    "UPDATE paper_trades SET live_status = 'FAILED', live_error = ? WHERE id = ?",
                    ("Live trading is on but no Upstox access token is saved", trade_id),
                )
                conn.commit()
        else:
            try:
                instrument_key = get_instrument_key(symbol)
            except Exception:
                instrument_key = None
            if not instrument_key:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE paper_trades SET live_status = 'FAILED', live_error = ? WHERE id = ?",
                        (f"No instrument_key found for {symbol}", trade_id),
                    )
                    conn.commit()
            else:
                available_funds = get_upstox_available_funds(access_token)
                if available_funds is None:
                    with get_db() as conn:
                        conn.execute(
                            "UPDATE paper_trades SET live_status = 'FAILED', live_error = ? WHERE id = ?",
                            ("Could not fetch available Upstox funds - live order skipped", trade_id),
                        )
                        conn.commit()
                else:
                    live_quantity = int(available_funds // price_val)
                    if live_quantity < 1:
                        with get_db() as conn:
                            conn.execute(
                                """
                                UPDATE paper_trades
                                SET live_status = 'FAILED', live_error = ?, live_quantity = ?
                                WHERE id = ?
                                """,
                                (
                                    f"Available funds (Rs {available_funds:.2f}) not enough for "
                                    f"1 share at Rs {price_val}",
                                    live_quantity,
                                    trade_id,
                                ),
                            )
                            conn.commit()
                    else:
                        result = place_live_order(instrument_key, "BUY", live_quantity, access_token)
                        with get_db() as conn:
                            if result["ok"]:
                                conn.execute(
                                    """
                                    UPDATE paper_trades
                                    SET live_status = 'OPEN', live_entry_order_id = ?, live_quantity = ?
                                    WHERE id = ?
                                    """,
                                    (result["order_id"], live_quantity, trade_id),
                                )
                            else:
                                conn.execute(
                                    """
                                    UPDATE paper_trades
                                    SET live_status = 'FAILED', live_error = ?, live_quantity = ?
                                    WHERE id = ?
                                    """,
                                    (result["error"], live_quantity, trade_id),
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

UPSTOX_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"


def _load_instrument_master() -> None:
    """Downloads Upstox's public instrument master file and builds a
    trading-symbol -> instrument_key lookup for NSE equities. Cached for
    the day since it's a large file and doesn't change intraday.

    NOTE: this was written from Upstox's documented CSV format but could
    not be tested against the live file (no internet access in the build
    sandbox) - if this fails, the actual column names may differ slightly
    and will need a small fix once you see the real error."""
    global _instrument_cache, _instrument_cache_date, _instrument_debug
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
        mapping = {}
        row_count = 0
        for i, row in enumerate(reader):
            row_count = i + 1
            if i < 3:
                sample_rows.append(dict(row))
            exch = (row.get("exchange") or "").upper()
            itype = (row.get("instrument_type") or "").upper()
            tsym = (row.get("tradingsymbol") or row.get("trading_symbol") or "").upper()
            ikey = row.get("instrument_key") or ""
            if exch == "NSE_EQ" and itype == "EQUITY" and tsym and ikey:
                mapping[tsym] = ikey

        _instrument_cache = mapping
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
    except Exception as e:
        import traceback
        _instrument_cache = {}
        _instrument_debug = {
            "ok": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


def get_instrument_key(symbol: str) -> str | None:
    today = (datetime.utcnow() + IST_OFFSET).strftime("%Y-%m-%d")
    if _instrument_cache_date != today or not _instrument_cache:
        _load_instrument_master()
    return _instrument_cache.get((symbol or "").upper())


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
    """Live trading only ever goes long (Buy alerts, CNC), so P&L for a
    live-closed trade is just (exit - entry) * live_quantity - the same
    entry/exit prices the paper trade recorded, since the live order is a
    market order placed alongside it."""
    live_open = sum(1 for t in open_trades if t.get("live_status") == "OPEN")
    live_closed_trades = [t for t in closed_trades if t.get("live_status") == "CLOSED"]
    live_pnl = 0.0
    for t in live_closed_trades:
        qty = t.get("live_quantity") or 0
        entry = t.get("entry_price") or 0
        exit_p = t.get("exit_price") or 0
        live_pnl += (exit_p - entry) * qty
    return {
        "live_open": live_open,
        "live_closed": len(live_closed_trades),
        "live_pnl": round(live_pnl, 2),
    }


UPSTOX_ORDER_URL = "https://api.upstox.com/v3/order/place"


def place_live_order(instrument_key: str, transaction_type: str, quantity: int, access_token: str) -> dict:
    """Places a real order on Upstox: CNC (Delivery) product, Market order,
    DAY validity - matching the decisions for this app (live trading only
    ever goes long, so transaction_type is effectively always 'BUY' to open
    and 'SELL' to close the same delivery position).

    Never raises - a live-order failure must not be able to crash paper
    trade creation or the exit-check loop. Returns:
      {"ok": True, "order_id": "..."} on success
      {"ok": False, "error": "..."} on failure
    """
    body = {
        "quantity": quantity,
        "product": "D",  # CNC / Delivery
        "validity": "DAY",
        "price": 0,  # ignored by Upstox for MARKET orders
        "tag": "chartink-auto",
        "instrument_token": instrument_key,
        "order_type": "MARKET",
        "transaction_type": transaction_type,  # "BUY" or "SELL"
        "disclosed_quantity": 0,
        "trigger_price": 0,
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
        with urllib.request.urlopen(req, timeout=15) as resp:
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


def _close_live_position_if_any(trade: dict, access_token: str | None) -> tuple[str | None, str | None]:
    """If this paper trade has a live position open (live_status == 'OPEN'),
    places the closing SELL order on Upstox. Returns (order_id, error) - both
    None if there was no live position to close, since that's the normal
    case for Sell-direction trades and for any trade from before live
    trading was turned on."""
    if trade.get("live_status") != "OPEN":
        return None, None
    if not access_token:
        return None, "Live position open but no Upstox access token saved - could not close it"

    try:
        instrument_key = get_instrument_key(trade["symbol"])
    except Exception as e:
        return None, f"Could not look up instrument_key to close live position: {e}"
    if not instrument_key:
        return None, f"No instrument_key found for {trade['symbol']} - could not close live position"

    qty = trade.get("live_quantity") or trade["quantity"] or 1
    result = place_live_order(instrument_key, "SELL", qty, access_token)
    if result["ok"]:
        return result["order_id"], None
    return None, f"Failed to close live position: {result['error']}"


def run_paper_trade_check() -> dict:
    """Checks every open paper trade against the live 5-min candle exit
    rule, closing any that qualify. Returns a summary dict for the UI."""
    access_token = get_setting("upstox_access_token")
    if not access_token:
        return {"checked": 0, "closed": 0, "error": "No Upstox access token saved yet."}

    with get_db() as conn:
        open_trades = conn.execute(
            "SELECT * FROM paper_trades WHERE status = 'OPEN'"
        ).fetchall()

    checked = 0
    closed = 0
    errors = []
    now = datetime.utcnow().isoformat()

    for trade in open_trades:
        checked += 1
        symbol = trade["symbol"]
        try:
            instrument_key = get_instrument_key(symbol)
            if not instrument_key:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE paper_trades SET last_error = ?, last_checked_time = ? WHERE id = ?",
                        (f"No instrument_key found for {symbol}", now, trade["id"]),
                    )
                    conn.commit()
                continue

            candles = fetch_5min_candles(instrument_key, access_token)
            exited, exit_price = check_exit(trade["direction"], candles)
            last_price = candles[-1][3] if candles else None

            if exited:
                live_exit_order_id, live_exit_error = _close_live_position_if_any(trade, access_token)

            with get_db() as conn:
                if exited:
                    entry_price = trade["entry_price"]
                    qty = trade["quantity"] or 1
                    if trade["direction"] == "Buy":
                        pnl = (exit_price - entry_price) * qty
                    else:
                        pnl = (entry_price - exit_price) * qty
                    pnl = round(pnl, 2)
                    capital_used = entry_price * qty
                    pnl_pct = round((pnl / capital_used) * 100, 2) if capital_used else 0
                    conn.execute(
                        """
                        UPDATE paper_trades
                        SET status = 'CLOSED', exit_price = ?, exit_time = ?,
                            pnl = ?, pnl_pct = ?, exit_reason = ?, last_checked_price = ?,
                            last_checked_time = ?, last_error = NULL,
                            live_exit_order_id = COALESCE(?, live_exit_order_id),
                            live_status = CASE WHEN ? THEN 'CLOSED' ELSE live_status END,
                            live_error = COALESCE(?, live_error)
                        WHERE id = ?
                        """,
                        (
                            exit_price, now, pnl, pnl_pct, "5-EMA exit rule", last_price, now,
                            live_exit_order_id,
                            live_exit_order_id is not None,
                            live_exit_error,
                            trade["id"],
                        ),
                    )
                    closed += 1
                else:
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
            alerts_count=merged_count,
            grouped=grouped,
            all_sectors=all_sectors,
            all_sector_names=ALL_SECTOR_NAMES,
            available_dates=available_dates,
            selected_date=selected_date,
            today_str=today_str,
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


@app.route("/clear", methods=["POST"])
def clear_alerts():
    with get_db() as conn:
        conn.execute("DELETE FROM alerts")
        conn.commit()
    return redirect(url_for("index"))


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
        if t["direction"] == "Buy":
            pnl = (last_price - entry) * qty
        else:
            pnl = (entry - last_price) * qty
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

    total_pnl = sum(t["pnl"] for t in closed_trades if t["pnl"] is not None)
    wins = sum(1 for t in closed_trades if (t["pnl"] or 0) > 0)
    total_closed = len(closed_trades)
    win_rate = round((wins / total_closed) * 100, 1) if total_closed else 0

    token_saved = bool(get_setting("upstox_access_token"))
    capital = get_capital()
    attach_running_balance(closed_trades, capital)
    current_capital = get_current_capital()
    live_trading_enabled = get_live_trading_enabled()

    live_stats = compute_live_stats(open_trades, closed_trades)
    live_available_funds = None
    if token_saved:
        live_available_funds = get_upstox_available_funds_for_display(get_setting("upstox_access_token"))

    html = render_template(
        "paper_trading.html",
        open_trades=open_trades,
        closed_trades=closed_trades,
        total_pnl=round(total_pnl, 2),
        win_rate=win_rate,
        total_closed=total_closed,
        wins=wins,
        token_saved=token_saved,
        capital=capital,
        current_capital=round(current_capital, 2),
        live_trading_enabled=live_trading_enabled,
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
    live_available_funds = get_upstox_available_funds_for_display(access_token) if access_token else None

    return jsonify({
        "open": open_trades,
        "closed": closed_trades,
        "current_capital": round(get_current_capital(), 2),
        "live_trading_enabled": get_live_trading_enabled(),
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
    """Manually closes an open trade right now, at the freshest price we
    can get - since the 5-EMA rule doesn't cover every situation you might
    want to exit on (e.g. hitting a target %, end of day, news, etc.)."""
    with get_db() as conn:
        trade = conn.execute(
            "SELECT * FROM paper_trades WHERE id = ? AND status = 'OPEN'", (trade_id,)
        ).fetchone()
    if not trade:
        return jsonify({"status": "error", "message": "Trade not found or already closed"}), 404

    exit_price = None
    access_token = get_setting("upstox_access_token")
    if access_token:
        try:
            instrument_key = get_instrument_key(trade["symbol"])
            if instrument_key:
                candles = fetch_5min_candles(instrument_key, access_token)
                if candles:
                    exit_price = candles[-1][3]
        except Exception:
            pass  # fall back to last_checked_price below

    if exit_price is None:
        exit_price = trade["last_checked_price"]
    if exit_price is None:
        return jsonify({"status": "error", "message": "No price available - save a token and run Check Exits Now at least once first"}), 400

    entry_price = trade["entry_price"]
    qty = trade["quantity"] or 1
    if trade["direction"] == "Buy":
        pnl = (exit_price - entry_price) * qty
    else:
        pnl = (entry_price - exit_price) * qty
    pnl = round(pnl, 2)
    capital_used = entry_price * qty
    pnl_pct = round((pnl / capital_used) * 100, 2) if capital_used else 0
    now = datetime.utcnow().isoformat()

    live_exit_order_id, live_exit_error = _close_live_position_if_any(trade, access_token)

    with get_db() as conn:
        conn.execute(
            """
            UPDATE paper_trades
            SET status = 'CLOSED', exit_price = ?, exit_time = ?,
                pnl = ?, pnl_pct = ?, exit_reason = ?, last_checked_time = ?,
                live_exit_order_id = COALESCE(?, live_exit_order_id),
                live_status = CASE WHEN ? THEN 'CLOSED' ELSE live_status END,
                live_error = COALESCE(?, live_error)
            WHERE id = ?
            """,
            (
                exit_price, now, pnl, pnl_pct, "Manual exit", now,
                live_exit_order_id,
                live_exit_order_id is not None,
                live_exit_error,
                trade_id,
            ),
        )
        conn.commit()

    return jsonify({"status": "ok", "exit_price": exit_price, "pnl": pnl, "pnl_pct": pnl_pct})


@app.route("/api/paper-trading/check", methods=["POST"])
def paper_trading_check():
    result = run_paper_trade_check()
    return jsonify(result)


@app.route("/api/paper-trading/debug-instruments")
def debug_instruments():
    """Diagnostic-only: forces a fresh download of Upstox's instrument
    master and shows exactly what came back - real column names, sample
    rows, and any error - so a mismatch in the parsing code can be spotted
    and fixed precisely."""
    _load_instrument_master()
    return jsonify(_instrument_debug)


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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
