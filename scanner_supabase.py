# scanner_supabase.py
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

# --- Postgres helpers
def get_conn():
    """
    Parse SUPABASE_DB_URL and connect using an IPv4 address if possible.
    This avoids flaky IPv6 routing on some runners.
    """
    # parse the URL
    parsed = urlparse(SUPABASE_DB_URL)
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port or 5432
    dbname = parsed.path.lstrip("/") or "postgres"

    # try to resolve an IPv4 address for the host; fall back to hostname if resolution fails
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        ipv4 = infos[0][4][0]  # first IPv4 address
    except Exception:
        ipv4 = host

    # connect by passing explicit connection parameters and forcing sslmode=require
    conn = psycopg2.connect(
        host=ipv4,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode="require",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return conn


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

def was_alerted_recent(coin_id, hours=6):
    sql = "SELECT 1 FROM alerts WHERE coin_id = %s AND alert_time >= (now() - (%s || ' hours')::interval) LIMIT 1"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (coin_id, hours))
            return cur.fetchone() is not None

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
        print("Telegram send error:", e)
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
        print("fetch_ohlcv error for", symbol, e)
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
    print("Starting scan")
    markets = coingecko_top_markets(COINGECKO_TOP)
    # filter by 24h pct and volume early to reduce load
    candidates = [m for m in markets if (m.get("price_change_percentage_24h") or 0) >= MIN_24H_PCT and (m.get("total_volume") or 0) >= MIN_VOLUME_USD]
    print("Candidates after CG filter:", len(candidates))
    # try to map to exchange markets via Binance where possible
    exchange = ccxt.binance({"enableRateLimit": True})
    try:
        exchange.load_markets()
        mapped = map_to_exchange_symbols(exchange, candidates)
    except Exception as e:
        print("Exchange load error, will skip exchange symbol matching", e)
        mapped = []  # fallback to CoinGecko-only path below

    alerts_sent = []
    # If we could map to exchange symbols, we will use them to compute indicators
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
                print("SKIP recent alert", coin_id)
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
    # fallback CoinGecko-only alerts for coins we could not map or compute indicators for
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
    print("Done. Alerts sent:", alerts_sent)
    return alerts_sent

if __name__ == "__main__":
    ensure_table()
    scan_once()
