from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, render_template, request, redirect, url_for, make_response

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "alerts.db"
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


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    return jsonify({"status": "ok"}), 200


@app.route("/clear", methods=["POST"])
def clear_alerts():
    with get_db() as conn:
        conn.execute("DELETE FROM alerts")
        conn.commit()
    return redirect(url_for("index"))


init_db()  # runs on import too, so gunicorn (used in production) creates the table

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
