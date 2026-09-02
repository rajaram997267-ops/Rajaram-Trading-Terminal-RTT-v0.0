from __future__ import annotations

import csv
import gzip
import io
import json
import math
import os
import threading
from collections import deque
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

# [SECTOR_MAP is omitted here for brevity, but keep your full 180+ symbol map in your local file]
SECTOR_MAP = {"RELIANCE": "Energy", "BHARTIARTL": "Telecom-Service", "ICICIBANK": "Bank"} # etc...

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    conn = psycopg2.connect(DATABASE_URL)
    return PGConnWrapper(conn)

class PGConnWrapper:
    def __init__(self, conn):
        self._conn = conn
    def execute(self, query, params=None):
        pg_query = query.replace("?", "%s")
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(pg_query, params or ())
        return cur
    def commit(self):
        self._conn.commit()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None: self._conn.commit()
            else: self._conn.rollback()
        finally: self._conn.close()
        return False

# --- DYNAMIC MOMENTUM SETTINGS ---
def get_setting(key: str, default: str | None = None) -> str | None:
    with get_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default

def set_setting(key: str, value: str):
    with get_db() as conn:
        conn.execute("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
        conn.commit()

def get_momo_rsi_period() -> int: return int(get_setting("momo_rsi_period", "14"))
def get_momo_rsi_upper() -> int: return int(get_setting("momo_rsi_upper", "70"))
def get_momo_rsi_lower() -> int: return int(get_setting("momo_rsi_lower", "45"))
def get_momo_ema_period() -> int: return int(get_setting("momo_ema_period", "9"))
def get_momo_emergency_enabled() -> bool: return get_setting("momo_emergency_enabled", "false") == "true"

# --- MATH UTILITIES ---
def calculate_ema(values: list[float], period: int = 5) -> list[float | None]:
    n = len(values)
    if n < period: return [None] * n
    k = 2 / (period + 1)
    ema: list[float | None] = [None] * (period - 1)
    seed = sum(values[:period]) / period
    ema.append(seed)
    for price in values[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema

def calculate_rsi(prices: list[float], period: int = 14) -> list[float | None]:
    n = len(prices)
    if n < period + 1: return [None] * n
    deltas = [prices[i] - prices[i-1] for i in range(1, n)]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    rsi: list[float | None] = [None] * period
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0: rsi.append(100.0)
    else: rsi.append(100 - (100 / (1 + (avg_gain / avg_loss))))
    for i in range(period, n - 1):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0: rsi.append(100.0)
        else: rsi.append(100 - (100 / (1 + (avg_gain / avg_loss))))
    return rsi

# --- SYSTEM CORE: CHECK EXITS ---
VALID_STRATEGIES = ("HALF_HALF_HARD4", "TRAIL_FROM_2", "FULL_AT_2", "FULL_AT_4", "ATR_TRAIL", "EMA_SPOT_TRAIL", "EMA_SPOT_PURE", "JOAT_HYBRID", "JOAT_TEST_B", "JOAT_TEST_C", "MOMENTUM_CONFIRMED")

def run_paper_trade_check() -> dict:
    access_token = get_setting("upstox_access_token")
    if not access_token: return {"checked": 0, "closed": 0}

    # Strategy Parameters
    rsi_p = get_momo_rsi_period(); rsi_up = get_momo_rsi_upper(); rsi_lo = get_momo_rsi_lower()
    ema_p = get_momo_ema_period(); emerg_on = get_momo_emergency_enabled()

    with get_db() as conn:
        open_trades = conn.execute("SELECT * FROM paper_trades WHERE status = 'OPEN' OR live_status = 'OPEN'").fetchall()
    
    now = datetime.utcnow().isoformat(); checked = 0; closed = 0
    for trade in open_trades:
        checked += 1; symbol = trade["symbol"]
        try:
            instrument_key = trade["paper_instrument_key"] or get_instrument_key(symbol)
            strategy = trade["strategy"] or "EMA"
            needs_candles = strategy in ("EMA", "MOMENTUM_CONFIRMED", "ATR_TRAIL")
            
            ws_price = get_ws_price(instrument_key)
            candles = fetch_5min_candles(instrument_key, access_token) if needs_candles else None
            last_price = ws_price if ws_price is not None else (candles[-1][3] if candles else None)
            
            if last_price is None: continue

            exited = False; exit_reason = None
            
            # --- RSI-EMA DUAL EXIT ENGINE (Works for Paper & Live) ---
            if strategy == "MOMENTUM_CONFIRMED":
                if len(candles or []) >= rsi_p + 5:
                    closes = [c[3] for c in candles]
                    rsi_series = calculate_rsi(closes, rsi_p)
                    ema9_series = calculate_ema(closes, ema_p)
                    ema20_series = calculate_ema(closes, 20)
                    
                    curr_rsi = rsi_series[-1]; prev_rsi = rsi_series[-2]
                    curr_ema9 = ema9_series[-1]; curr_ema20 = ema20_series[-1]

                    if trade["direction"] == "Buy": # CALLS
                        if emerg_on and (curr_rsi < 50 or last_price < curr_ema20):
                            exited, exit_reason = True, "Emergency RSI50/EMA20"
                        elif curr_rsi < rsi_up and prev_rsi < rsi_up and last_price < curr_ema9:
                            exited, exit_reason = True, f"Momo Loss (RSI<{rsi_up} + EMA{ema_p})"
                    else: # PUTS
                        if emerg_on and (curr_rsi > 55 or last_price > curr_ema20):
                            exited, exit_reason = True, "Emergency RSI55/EMA20"
                        elif curr_rsi > rsi_lo and prev_rsi > rsi_lo and last_price > curr_ema9:
                            exited, exit_reason = True, f"Momo Loss (RSI>{rsi_lo} + EMA{ema_p})"

            # [Other elif strategy branches from original main.py here...]

            # EXECUTE EXIT
            if exited:
                # 1. Handle Live Exit if applicable
                if trade["live_status"] == "OPEN":
                    l_order_id, l_error, l_fill = _close_live_position_if_any(trade, access_token)
                    with get_db() as conn:
                        conn.execute("UPDATE paper_trades SET live_status='CLOSED', live_exit_price=?, live_exit_reason=?, live_exit_time=? WHERE id=?", (l_fill or last_price, exit_reason, now, trade["id"]))
                
                # 2. Handle Paper Exit
                if trade["status"] == "OPEN":
                    with get_db() as conn:
                        conn.execute("UPDATE paper_trades SET status='CLOSED', exit_price=?, exit_time=?, pnl=?, exit_reason=? WHERE id=?", (last_price, now, (last_price - trade["entry_price"]) * trade["quantity"], exit_reason, trade["id"]))
                    closed += 1
                    
        except Exception as e: print(f"Error checking {symbol}: {e}")
    return {"checked": checked, "closed": closed}

# --- ATTACH UI INFO ---
def attach_stop_info(open_trades: list[dict], access_token: str | None) -> None:
    for t in open_trades:
        strategy = t.get("strategy")
        if strategy == "MOMENTUM_CONFIRMED":
            try:
                instrument_key = t.get("paper_instrument_key")
                candles = fetch_5min_candles(instrument_key, access_token)
                closes = [c[3] for c in candles]
                rsi_val = calculate_rsi(closes, get_momo_rsi_period())[-1]
                ema_val = calculate_ema(closes, get_momo_ema_period())[-1]
                t["stop_info"] = f"RSI: {rsi_val:.1f} | EMA: {ema_val:.2f}"
            except: t["stop_info"] = "Calculating..."
        # [Other strategy stop_info logic here...]

# --- API ROUTES ---
@app.route("/api/paper-trading/momo-settings", methods=["POST"])
def save_momo_settings():
    data = request.get_json(silent=True) or {}
    set_setting("momo_rsi_period", str(data.get("rsi_period", 14)))
    set_setting("momo_rsi_upper", str(data.get("rsi_upper", 70)))
    set_setting("momo_rsi_lower", str(data.get("rsi_lower", 45)))
    set_setting("momo_ema_period", str(data.get("ema_period", 9)))
    set_setting("momo_emergency_enabled", "true" if data.get("emergency_enabled") else "false")
    return jsonify({"status": "ok"})

@app.route("/settings")
def settings_page():
    return render_template("settings.html", 
        active_tab="settings",
        token_saved=bool(get_setting("upstox_access_token")),
        capital=get_capital(),
        live_trading_enabled=get_live_trading_enabled(),
        exit_strategy=get_exit_strategy(),
        momo_rsi_period=get_momo_rsi_period(),
        momo_rsi_upper=get_momo_rsi_upper(),
        momo_rsi_lower=get_momo_rsi_lower(),
        momo_ema_period=get_momo_ema_period(),
        momo_emergency_enabled=get_momo_emergency_enabled(),
        strategy_descriptions=STRATEGY_DESCRIPTIONS
    )

# [Include your existing Flask setup and WebSocket logic from your previous main.py here]

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)