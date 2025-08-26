# scanner_coingecko_only.py
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"

# Config
TOP_N = 100
MIN_24H_PCT = 15.0   # alert if 24h price change >= this percent
MIN_VOLUME_USD = 100000   # minimum 24h volume to avoid tiny illiquid coins
SLEEP_BETWEEN_RUNS = 300  # seconds between full scans

def send_telegram(text):
    try:
        r = requests.post(TELEGRAM_API_BASE + "/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram send error:", e)
        return False

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
    print("Fetching CoinGecko top markets...")
    markets = fetch_top_markets(TOP_N)
    alerts = []
    for m in markets:
        try:
            pct = m.get("price_change_percentage_24h") or 0.0
            vol = m.get("total_volume") or 0.0
            if pct >= MIN_24H_PCT and vol >= MIN_VOLUME_USD:
                text = (
                    f"COINGECKO ALERT: {m.get('symbol','').upper()} / {m.get('name')}\n"
                    f"Price: ${m.get('current_price')}\n"
                    f"24h Change: {pct:.2f}%\n"
                    f"24h Volume: ${vol:,.0f}\n"
                    f"MarketCap: ${m.get('market_cap',0):,}\n"
                    f"Link: https://www.coingecko.com/en/coins/{m.get('id')}"
                )
                print("ALERT ->", m.get('symbol'), pct, "% vol:", vol)
                ok = send_telegram(text)
                if ok:
                    alerts.append(m.get('id'))
        except Exception as e:
            print("entry error", e)
    return alerts

if __name__ == "__main__":
    alerts = scan_once()
    print("Done. Alerts:", alerts)
