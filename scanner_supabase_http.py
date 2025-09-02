# scanner_supabase_http.py
# REST-based Supabase scanner with TradingView-style filters:
# - Volume % change (24h) > 150%
# - Price % change (15m vs 1h) between 3% and 15%
# - RSI (1h) between 50 and 70
# - MACD (1h) bullish crossover
# - Prefers USDT pairs
# - Requires exchange OHLCV to compute indicators; if missing, coin is skipped.

import os
import time
import requests
import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# TA indicators
from ta.momentum import RSIIndicator
from ta.trend import MACD

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")               # e.g. https://<project-ref>.supabase.co
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SUPABASE_URL, SUPABASE_SERVICE_KEY in environment")

TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
REST_BASE = SUPABASE_URL.rstrip("/") + "/rest/v1"
HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# rules / tuning (from screenshot)
COINGECKO_TOP = 200
MIN_VOLUME_USD = 10000          # still require some liquidity (you can raise)
DEDUPE_HOURS = 6
SLEEP_BETWEEN_SYMBOLS = 0.3

# indicator params
OHLCV_LIMIT_1H = 72    # fetch 72 hours of 1h bars (if available)
OHLCV_LIMIT_15M = 8    # fetch 8 bars of 15m (~2 hours, enough to get latest)
VOL_CHANGE_THRESHOLD_PCT = 150.0
PRICE_MIN_PCT = 3.0
PRICE_MAX_PCT = 15.0
RSI_LOW = 50.0
RSI_HIGH = 70.0

# ----- Supabase REST helpers
def was_alerted_recent(coin_id, hours=DEDUPE_HOURS):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    url = f"{REST_BASE}/alerts"
    params = {
        "coin_id": f"eq.{coin_id}",
        "alert_time": f"gte.{cutoff}"
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return len(r.json()) > 0

def record_alert(coin_id, symbol, alert_type, pct, volume):
    url = f"{REST_BASE}/alerts"
    payload = {
        "coin_id": coin_id,
        "symbol": symbol,
        "alert_type": alert_type,
        "pct": pct,
        "volume": volume
    }
    r = requests.post(url, headers=HEADERS, json=payload, timeout=15)
    r.raise_for_status()
    return r.json()

# ----- Telegram
def send_telegram(text):
    try:
        r = requests.post(TELEGRAM_API_BASE + "/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram send error:", e)
        return False

# ----- CoinGecko helper
def coingecko_top_markets(limit=COINGECKO_TOP):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency":"usd","order":"market_cap_desc","per_page":min(limit,250),"page":1,"price_change_percentage":"24h"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

# ----- Exchange helpers
def map_to_exchange_symbols(exchange, coin_list):
    markets = exchange.load_markets()
    mapped = []
    for coin in coin_list:
        sym = (coin.get("symbol") or "").upper()
        # prefer USDT
        candidates = [f"{sym}/USDT", f"{sym}/BUSD", f"{sym}/USD"]
        for c in candidates:
            if c in markets:
                mapped.append((coin, c))
                break
    print(f"Mapped {len(mapped)} symbols to exchange markets")
    return mapped

def fetch_ohlcv(exchange, symbol, timeframe='1h', limit=OHLCV_LIMIT_1H):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not ohlcv:
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('datetime', inplace=True)
        return df[['open','high','low','close','volume']]
    except Exception as e:
        print(f"fetch_ohlcv error for {symbol} {timeframe}: {e}")
        return None

# ----- Indicator computations (expects 1h and 15m dataframes)
def compute_signals(df1h, df15m):
    """
    Returns dict:
     - vol_pct_24h (float)
     - price_pct_15_vs_1h (float)
     - rsi_1h (float)
     - macd_bull_cross (bool)
     - last_close (float)
    If the provided data is insufficient for a particular metric, returns None for that metric.
    """
    results = {
        "vol_pct_24h": None,
        "price_pct_15_vs_1h": None,
        "rsi_1h": None,
        "macd_bull_cross": False,
        "last_close": None
    }
    # last_close from most recent 1h close if available, else 15m
    try:
        if df1h is None or df1h.empty:
            return results
        last_close = float(df1h['close'].iloc[-1])
        results['last_close'] = last_close
    except Exception:
        pass

    # Volume % change (24h): need at least 48 hours of 1h bars (24 + previous 24)
    if df1h is not None and len(df1h) >= 48:
        try:
            vol_last24 = df1h['volume'].iloc[-24:].sum()
            vol_prev24 = df1h['volume'].iloc[-48:-24].sum()
            if vol_prev24 > 0:
                results['vol_pct_24h'] = (vol_last24 / vol_prev24 - 1.0) * 100.0
        except Exception:
            results['vol_pct_24h'] = None

    # Price % change (15m vs 1h): we compute ((close_15m_latest - close_1h_ago) / close_1h_ago)*100
    if df15m is not None and not df15m.empty and df1h is not None and len(df1h) >= 2:
        try:
            close_15m = float(df15m['close'].iloc[-1])
            # find the 1h bar from ~1 hour ago (previous 1h close)
            close_1h_ago = float(df1h['close'].iloc[-2])
            results['price_pct_15_vs_1h'] = (close_15m / close_1h_ago - 1.0) * 100.0
        except Exception:
            results['price_pct_15_vs_1h'] = None

    # RSI (1h)
    if df1h is not None and len(df1h) >= 20:
        try:
            rsi = RSIIndicator(df1h['close'], window=14).rsi()
            results['rsi_1h'] = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else None
        except Exception:
            results['rsi_1h'] = None

    # MACD (1h) bullish crossover: previous macd < prev signal AND current macd > current signal
    if df1h is not None and len(df1h) >= 35:  # need enough bars for MACD
        try:
            macd = MACD(df1h['close'], window_slow=26, window_fast=12, window_sign=9)
            macd_line = macd.macd()
            macd_signal = macd.macd_signal()
            if len(macd_line) >= 2 and len(macd_signal) >= 2:
                prev_macd = macd_line.iloc[-2]
                prev_sig = macd_signal.iloc[-2]
                cur_macd = macd_line.iloc[-1]
                cur_sig = macd_signal.iloc[-1]
                results['macd_bull_cross'] = (prev_macd < prev_sig) and (cur_macd > cur_sig)
        except Exception:
            results['macd_bull_cross'] = False

    return results

# ----- Main scan logic applying the filters
def scan_once():
    print("Starting scan", flush=True)
    markets = coingecko_top_markets(COINGECKO_TOP)
    # we will compute only for coins with some volume
    candidates = [m for m in markets if (m.get("total_volume") or 0) >= MIN_VOLUME_USD]
    print("Candidates after CG volume filter:", len(candidates), flush=True)

    exchange = ccxt.binance({"enableRateLimit": True})
    try:
        exchange.load_markets()
        mapped = map_to_exchange_symbols(exchange, candidates)
    except Exception as e:
        print("Exchange load error, cannot compute indicators. Exiting scan.", e, flush=True)
        return []

    alerts_sent = []
    for coin, symbol in mapped:
        time.sleep(SLEEP_BETWEEN_SYMBOLS)
        # ensure this is a USDT-like pair (map_to_exchange_symbols already prefers USDT)
        if not symbol.endswith("/USDT"):
            # prefer exact USDT pairs; if not, skip
            continue

        # fetch 1h and 15m bars
        df1h = fetch_ohlcv(exchange, symbol, timeframe='1h', limit=OHLCV_LIMIT_1H)
        df15m = fetch_ohlcv(exchange, symbol, timeframe='15m', limit=OHLCV_LIMIT_15M)
        if df1h is None or df15m is None:
            # insufficient data, skip coin
            print(f"Skipping {symbol}: missing OHLCV data", flush=True)
            continue

        sig = compute_signals(df1h, df15m)
        # Ensure we have all required metrics
        if sig['vol_pct_24h'] is None or sig['price_pct_15_vs_1h'] is None or sig['rsi_1h'] is None:
            print(f"Skipping {symbol}: insufficient metrics: {sig}", flush=True)
            continue

        # Apply the rules
        vol_ok = sig['vol_pct_24h'] >= VOL_CHANGE_THRESHOLD_PCT
        price_ok = PRICE_MIN_PCT <= sig['price_pct_15_vs_1h'] <= PRICE_MAX_PCT
        rsi_ok = (sig['rsi_1h'] is not None) and (RSI_LOW <= sig['rsi_1h'] <= RSI_HIGH)
        macd_ok = sig['macd_bull_cross']

        print(f"{symbol} signals vol%={sig['vol_pct_24h']:.1f}, price15vs1h={sig['price_pct_15_vs_1h']:.2f}, rsi1h={sig['rsi_1h']}, macd_cross={sig['macd_bull_cross']}", flush=True)

        if vol_ok and price_ok and rsi_ok and macd_ok:
            coin_id = coin.get("id")
            if was_alerted_recent(coin_id):
                print("SKIP recent alert", coin_id, flush=True)
                continue
            text = (
                f"ALERT {symbol}\n"
                f"Name: {coin.get('name')} ({coin.get('symbol').upper()})\n"
                f"Price: ${sig['last_close']:.8f}\n"
                f"Vol change 24h: {sig['vol_pct_24h']:.1f}%\n"
                f"Price 15m vs 1h: {sig['price_pct_15_vs_1h']:.2f}%\n"
                f"RSI 1h: {sig['rsi_1h']:.1f}\n"
                f"MACD bullish crossover: {sig['macd_bull_cross']}\n"
                f"https://www.coingecko.com/en/coins/{coin.get('id')}"
            )
            ok = send_telegram(text)
            if ok:
                record_alert(coin_id, coin.get("symbol").upper(), "tv_strategy", sig['price_pct_15_vs_1h'], sig['vol_pct_24h'])
                alerts_sent.append(coin_id)
    print("Done. Alerts sent:", alerts_sent, flush=True)
    return alerts_sent

if __name__ == "__main__":
    scan_once()
