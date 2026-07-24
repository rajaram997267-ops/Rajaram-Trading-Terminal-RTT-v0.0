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


def group_by_category(alerts: list[dict]) -> list[tuple[str, list[dict]]]:
    """Split alerts into Sell / Buy / Others sections, in that fixed order,
    skipping any section that has no alerts."""
    buckets = {name: [] for name in CATEGORY_ORDER}
    for alert in alerts:
        buckets[categorize(alert)].append(alert)
    return [(name, buckets[name]) for name in CATEGORY_ORDER if buckets[name]]


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
        grouped = group_by_category(alerts)

        html = render_template(
            "index.html",
            alerts=alerts,
            grouped=grouped,
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
    return jsonify(alerts)


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
