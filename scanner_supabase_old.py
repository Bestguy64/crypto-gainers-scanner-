# scanner_supabase.py
# Supabase-backed crypto scanner
# - prefers IPv4 when connecting to Postgres to avoid IPv6 routing failures on CI/runners
# - tries exchange OHLCV via CCXT (Binance) and falls back to CoinGecko-only alerts
# - dedupes alerts by inserting into a Postgres table in Supabase

import os
import time
import requests
import ccxt
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
import socket
import sys

load_dotenv()

# Env / config
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")  # required
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not SUPABASE_DB_URL:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and SUPABASE_DB_URL in environment")

TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Scanner config (tune as needed)
COINGECKO_TOP = 200
MIN_24H_PCT = 10.0
MIN_VOLUME_USD = 100000.0
VOL_GAIN_WINDOW = 10
VOL_GAIN_THRESHOLD_PCT = 150.0
PRICE_BREAKOUT_LOOKBACK = 20
PRICE_BREAKOUT_PCT = 1.5
RSI_LOWER = 20
RSI_UPPER = 85
OHLCV_LIMIT = 500
SLEEP_BETWEEN_SYMBOLS = 0.5
CONNECT_TIMEOUT = 10  # seconds for DB connect attempts
IPV4_CONNECT_ATTEMPTS = 3

# --- Postgres helpers
def parse_db_url(url):
    p = urlparse(url)
    username = p.username
    password = p.password
    hostname = p.hostname
    port = p.port or 5432
    dbname = p.path.lstrip('/') or "postgres"
    return username, password, hostname, port, dbname

def try_connect_ipv4(username, password, host, port, dbname):
    """
    Resolve IPv4 addresses for host and try to connect to each one.
    Returns a live connection or raises the last exception.
    """
    last_exc = None
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    except Exception as e:
        # no IPv4 addresses found
        infos = []

    # iterate unique IPv4 addresses
    seen = set()
    for info in infos:
        addr = info[4][0]
        if addr in seen:
            continue
        seen.add(addr)
        for attempt in range(IPV4_CONNECT_ATTEMPTS):
            try:
                print(f"[DB] Trying IPv4 connect to {addr}:{port} (attempt {attempt+1})", flush=True)
                conn = psycopg2.connect(
                    host=addr,
                    port=port,
                    dbname=dbname,
                    user=username,
                    password=password,
                    sslmode="require",
                    connect_timeout=CONNECT_TIMEOUT,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                )
                print(f"[DB] Connected to {addr}", flush=True)
                return conn
            except Exception as e:
                last_exc = e
                print(f"[DB] IPv4 connect to {addr} failed: {e}", flush=True)
                time.sleep(1)
    # If no IPv4 worked, raise last exception
    if last_exc:
        raise last_exc
    # No IPv4 addresses at all, raise to allow fallback
    raise RuntimeError("No IPv4 addresses found for host")

def get_conn():
    """
    Connect to Postgres using IPv4 if possible; fall back to original URL connect.
    """
    username, password, host, port, dbname = parse_db_url(SUPABASE_DB_URL)
    # first try IPv4-based connects
    try:
        return try_connect_ipv4(username, password, host, port, dbname)
    except Exception as e:
        print(f"[DB] IPv4 connection attempts failed: {e}. Falling back to direct DSN connect (may use IPv6).", flush=True)
        # fallback: try standard psycopg2.connect with DSN (this mirrors SUPABASE_DB_URL)
        try:
            conn = psycopg2.connect(SUPABASE_DB_URL, cursor_factory=psycopg2.extras.RealDictCursor, connect_timeout=CONNECT_TIMEOUT)
            print("[DB] Connected via DSN fallback", flush=True)
            return conn
        except Exception as e2:
            print(f"[DB] DSN fallback also failed: {e2}", flush=True)
            raise

def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS alerts (
      id SERIAL PRIMARY KEY,
      coin_id TEXT NOT NULL,
      symbol TEXT,
      alert_type TEXT,
      alert_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
      pct NUMERIC,
      volume NUMERIC
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
    print("[DB] ensured alerts table exists", flush=True)

def was_alerted_recent(coin_id, hours=6):
    sql = "SELECT 1 FROM alerts WHERE coin_id = %s AND alert_time >= (now() - (%s || ' hours')::interval) LIMIT 1"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (coin_id, hours))
            found = cur.fetchone() is not None
            return found

def record_alert(coin_id, symbol, alert_type, pct, volume):
    sql = "INSERT INTO alerts (coin_id, symbol, alert_type, pct, volume, alert_time) VALUES (%s, %s, %s, %s, %s, now())"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (coin_id, symbol, alert_type, pct, volume))
            conn.commit()

# --- Telegram
def send_telegram(text):
    url = TELEGRAM_API_BASE + "/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        r = requests.post(url, data=payload, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram send error:", e, flush=True)
        return False

# --- CoinGecko / exchange helpers
def coingecko_top_markets(limit=COINGECKO_TOP):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": min(limit, 250),
        "page": 1,
        "price_change_percentage": "24h"
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def map_to_exchange_symbols(exchange, coin_list):
    markets = exchange.load_markets()
    mapped = []
    for coin in coin_list:
        sym = (coin.get("symbol") or "").upper()
        candidates = [f"{sym}/USDT", f"{sym}/BUSD", f"{sym}/USD"]
        for c in candidates:
            if c in markets:
                mapped.append((coin, c))
                break
    print(f"Mapped {len(mapped)} symbols to exchange markets", flush=True)
    return mapped

def fetch_ohlcv_for_symbol(exchange, symbol, timeframe='1m', limit=OHLCV_LIMIT):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not ohlcv:
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('datetime', inplace=True)
        return df[['open','high','low','close','volume']]
    except Exception as e:
        print("fetch_ohlcv error for", symbol, e, flush=True)
        return None

def compute_indicators(df):
    df_15 = df.resample('15T').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
    df_1h = df.resample('1H').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
    if len(df_15) < VOL_GAIN_WINDOW + 2 or len(df_1h) < 2:
        return None
    # RSI using ta
    from ta.momentum import RSIIndicator
    df_15 = df_15.copy()
    df_1h = df_1h.copy()
    df_15['RSI'] = RSIIndicator(df_15['close'], window=14).rsi()
    df_1h['RSI'] = RSIIndicator(df_1h['close'], window=14).rsi()
    last_15 = df_15.iloc[-1]
    prev_15 = df_15.iloc[-(VOL_GAIN_WINDOW+1):-1]
    avg_vol = prev_15['volume'].mean()
    vol_gain_pct = (last_15['volume'] / avg_vol * 100) if avg_vol > 0 else 0
    last_close = last_15['close']
    recent_section = df_15['high'].iloc[-(PRICE_BREAKOUT_LOOKBACK+1):-1]
    recent_high = recent_section.max() if not recent_section.empty else float('nan')
    breakout_pct = ((last_close - recent_high) / recent_high * 100) if recent_high and recent_high > 0 else 0
    rsi_15 = last_15['RSI']
    rsi_1h = df_1h['RSI'].iloc[-1]
    return {
        "vol_gain_pct": float(vol_gain_pct),
        "breakout_pct": float(breakout_pct),
        "rsi_15": float(rsi_15) if not pd.isna(rsi_15) else None,
        "rsi_1h": float(rsi_1h) if not pd.isna(rsi_1h) else None,
        "last_close": float(last_close),
        "recent_high": float(recent_high) if not pd.isna(recent_high) else None
    }

def scan_once():
    print("Starting scan", flush=True)
    markets = coingecko_top_markets(COINGECKO_TOP)
    candidates = [m for m in markets if (m.get("price_change_percentage_24h") or 0) >= MIN_24H_PCT and (m.get("total_volume") or 0) >= MIN_VOLUME_USD]
    print("Candidates after CG filter:", len(candidates), flush=True)

    exchange = ccxt.binance({"enableRateLimit": True})
    mapped = []
    try:
        exchange.load_markets()
        mapped = map_to_exchange_symbols(exchange, candidates)
    except Exception as e:
        print("Exchange load error, will skip exchange symbol matching", e, flush=True)
        mapped = []

    alerts_sent = []
    for coin, symbol in mapped:
        time.sleep(SLEEP_BETWEEN_SYMBOLS)
        df = fetch_ohlcv_for_symbol(exchange, symbol, timeframe='1m', limit=OHLCV_LIMIT)
        if df is None:
            continue
        ind = compute_indicators(df)
        if not ind:
            continue
        vol_ok = ind['vol_gain_pct'] >= VOL_GAIN_THRESHOLD_PCT
        breakout_ok = ind['breakout_pct'] >= PRICE_BREAKOUT_PCT
        rsi_ok = (ind['rsi_15'] is not None and ind['rsi_1h'] is not None and ind['rsi_15'] > RSI_LOWER and ind['rsi_1h'] < RSI_UPPER)
        if vol_ok and breakout_ok and rsi_ok:
            coin_id = coin.get("id")
            if was_alerted_recent(coin_id):
                print("SKIP recent alert", coin_id, flush=True)
                continue
            text = (
                f"ALERT {symbol}\n"
                f"Name: {coin.get('name')} ({coin.get('symbol').upper()})\n"
                f"Price: ${ind['last_close']:.8f}\n"
                f"Vol gain% (15m vs avg): {ind['vol_gain_pct']:.1f}%\n"
                f"Breakout% vs {PRICE_BREAKOUT_LOOKBACK} bars: {ind['breakout_pct']:.2f}%\n"
                f"RSI 15m: {ind['rsi_15']:.1f}, RSI 1h: {ind['rsi_1h']:.1f}\n"
                f"CoinGecko 24h change: {coin.get('price_change_percentage_24h'):.2f}%\n"
                f"https://www.coingecko.com/en/coins/{coin_id}"
            )
            ok = send_telegram(text)
            if ok:
                record_alert(coin_id, coin.get("symbol").upper(), "indicator", ind['vol_gain_pct'], ind.get('last_close'))
                alerts_sent.append(coin_id)

    for coin in markets:
        coin_id = coin.get("id")
        if coin_id in alerts_sent:
            continue
        pct = float(coin.get("price_change_percentage_24h") or 0)
        vol = float(coin.get("total_volume") or 0)
        if pct >= MIN_24H_PCT and vol >= MIN_VOLUME_USD:
            if was_alerted_recent(coin_id):
                continue
            text = (
                f"COINGECKO ALERT: {coin.get('symbol','').upper()} / {coin.get('name')}\n"
                f"Price: ${coin.get('current_price')}\n"
                f"24h Change: {pct:.2f}%\n"
                f"24h Volume: ${vol:,.0f}\n"
                f"https://www.coingecko.com/en/coins/{coin_id}"
            )
            ok = send_telegram(text)
            if ok:
                record_alert(coin_id, coin.get("symbol","").upper(), "24h_pct", pct, vol)
                alerts_sent.append(coin_id)

    print("Done. Alerts sent:", alerts_sent, flush=True)
    return alerts_sent

if __name__ == "__main__":
    ensure_table()
    scan_once()
