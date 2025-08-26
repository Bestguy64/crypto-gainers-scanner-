# scanner_coingecko_sqlite.py
import os
import time
import sqlite3
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)

DB_PATH = "alerts.db"
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
TOP_N = 100
MIN_24H_PCT = 15.0
MIN_VOLUME_USD = 100000
ALERT_DEDUP_HOURS = 6  # do not resend same coin within this window
SLEEP_BETWEEN_RUNS = 300  # seconds

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# --- DB helpers
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT NOT NULL,
            symbol TEXT,
            alert_type TEXT,
            alert_time INTEGER NOT NULL,
            pct REAL,
            volume REAL
        )
        """
    )
    conn.commit()
    conn.close()

def was_alerted_recent(coin_id, hours=ALERT_DEDUP_HOURS):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cutoff = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
    cur.execute("SELECT 1 FROM alerts WHERE coin_id = ? AND alert_time >= ? LIMIT 1", (coin_id, cutoff))
    found = cur.fetchone() is not None
    conn.close()
    return found

def record_alert(coin_id, symbol, alert_type, pct, volume):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cur.execute(
        "INSERT INTO alerts (coin_id, symbol, alert_type, alert_time, pct, volume) VALUES (?, ?, ?, ?, ?, ?)",
        (coin_id, symbol, alert_type, now_ts, pct, volume)
    )
    conn.commit()
    conn.close()

# --- Telegram
def send_telegram(text):
    try:
        r = requests.post(TELEGRAM_API_BASE + "/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram send error:", e)
        return False

# --- CoinGecko
def fetch_top_markets(limit=TOP_N):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": min(limit, 250),
        "page": 1,
        "price_change_percentage": "24h"
    }
    r = requests.get(COINGECKO_MARKETS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def scan_once():
    print(f"[{datetime.now().isoformat()}] Fetching CoinGecko top {TOP_N} markets...")
    markets = fetch_top_markets(TOP_N)
    alerts = []
    for m in markets:
        try:
            coin_id = m.get("id")
            symbol = (m.get("symbol") or "").upper()
            pct = float(m.get("price_change_percentage_24h") or 0.0)
            vol = float(m.get("total_volume") or 0.0)
            if pct >= MIN_24H_PCT and vol >= MIN_VOLUME_USD:
                if was_alerted_recent(coin_id):
                    # skip duplicate
                    print("SKIP (recent):", symbol, pct, "% vol:", vol)
                    continue
                text = (
                    f"COINGECKO ALERT: {symbol} / {m.get('name')}\n"
                    f"Price: ${m.get('current_price')}\n"
                    f"24h Change: {pct:.2f}%\n"
                    f"24h Volume: ${vol:,.0f}\n"
                    f"MarketCap: ${m.get('market_cap',0):,}\n"
                    f"Link: https://www.coingecko.com/en/coins/{coin_id}"
                )
                ok = send_telegram(text)
                if ok:
                    record_alert(coin_id, symbol, "24h_pct", pct, vol)
                    alerts.append(coin_id)
                    print("ALERT SENT:", symbol, pct, "% vol:", vol)
        except Exception as e:
            print("entry error", e)
    print(f"[{datetime.now().isoformat()}] Done. Alerts:", alerts)
    return alerts

if __name__ == "__main__":
    init_db()
    scan_once()
