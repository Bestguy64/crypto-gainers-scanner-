# scanner_supabase_http.py
# Multi-exchange REST-backed crypto scanner with TradingView-style filters
# - tries multiple exchanges (EXCHANGES env) to find USDT/BUSD/USD markets
# - computes indicators (24h vol change, 15m vs 1h price, RSI(1h), MACD(1h))
# - writes deduped alerts to Supabase via REST and sends Telegram messages
# - requires SUPABASE_URL and SUPABASE_SERVICE_KEY for REST access (service role key)

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

# --- Env / config
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")               # e.g. https://<project-ref>.supabase.co
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
EXCHANGES = os.getenv("EXCHANGES", "binance")         # comma-separated list, e.g. "binance,bitget,okx,bybit"

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

# ----- Scanner tuning (from your TradingView-style rules)
COINGECKO_TOP = 200
MIN_VOLUME_USD = 10000          # baseline liquidity; increase if needed
DEDUPE_HOURS = 6
SLEEP_BETWEEN_SYMBOLS = 0.4     # increase if you add many exchanges
OHLCV_LIMIT_1H = 72             # hours of 1h bars (72 = 3 days)
OHLCV_LIMIT_15M = 8             # ~2 hours of 15m bars
VOL_CHANGE_THRESHOLD_PCT = 150.0
PRICE_MIN_PCT = 3.0
PRICE_MAX_PCT = 15.0
RSI_LOW = 50.0
RSI_HIGH = 70.0

REQUEST_TIMEOUT = 15

# ----- Supabase REST helpers
def was_alerted_recent(coin_id, hours=DEDUPE_HOURS):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    url = f"{REST_BASE}/alerts"
    params = {
        "coin_id": f"eq.{coin_id}",
        "alert_time": f"gte.{cutoff}"
    }
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return len(r.json()) > 0
    except Exception as e:
        # if Supabase is temporarily unreachable, default to not alerted to avoid missing alerts later
        print("[DB] was_alerted_recent error:", e, flush=True)
        return False

def record_alert(coin_id, symbol, alert_type, pct, volume):
    url = f"{REST_BASE}/alerts"
    payload = {
        "coin_id": coin_id,
        "symbol": symbol,
        "alert_type": alert_type,
        "pct": pct,
        "volume": volume
    }
    try:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("[DB] record_alert error:", e, flush=True)
        return None

# ----- Telegram
def send_telegram(text):
    try:
        r = requests.post(TELEGRAM_API_BASE + "/sendMessage",
                          data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                          timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram send error:", e, flush=True)
        return False

# ----- CoinGecko
def coingecko_top_markets(limit=COINGECKO_TOP):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency":"usd","order":"market_cap_desc","per_page":min(limit,250),"page":1,"price_change_percentage":"24h"}
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

# ----- Multi-exchange setup and mapping
def build_exchange_instances():
    ex_names = [e.strip() for e in EXCHANGES.split(",") if e.strip()]
    instances = {}
    for name in ex_names:
        try:
            ex_cls = getattr(ccxt, name)
            instances[name] = ex_cls({"enableRateLimit": True})
            print(f"[EXCH] Prepared instance for {name}", flush=True)
        except Exception as e:
            print(f"[EXCH] Could not prepare ccxt exchange '{name}': {e}", flush=True)
    if not instances:
        # fallback to binance if nothing valid
        instances["binance"] = ccxt.binance({"enableRateLimit": True})
    return instances

def map_to_exchange_symbols_multi(exchange_instances, coin_list):
    """
    Return list of tuples: (coin, symbol, exchange_name)
    """
    markets_by_ex = {}
    for name, ex in exchange_instances.items():
        try:
            ex.load_markets()
            markets_by_ex[name] = ex.markets  # mapping of normalized symbol -> market info
            print(f"[EXCH] Loaded markets for {name}, markets={len(ex.markets)}", flush=True)
        except Exception as e:
            print(f"[EXCH] Failed loading markets for {name}: {e}", flush=True)

    mapped = []
    for coin in coin_list:
        sym = (coin.get("symbol") or "").upper()
        if not sym:
            continue
        candidates = [f"{sym}/USDT", f"{sym}/BUSD", f"{sym}/USD"]
        matched = False
        for ex_name, markets in markets_by_ex.items():
            for c in candidates:
                if c in markets:
                    mapped.append((coin, c, ex_name))
                    matched = True
                    break
            if matched:
                break
    print(f"[MAPPING] Mapped {len(mapped)} symbols across exchanges", flush=True)
    return mapped

# ----- OHLCV fetch
def fetch_ohlcv(exchange, symbol, timeframe='1h', limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not ohlcv:
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('datetime', inplace=True)
        return df[['open','high','low','close','volume']]
    except Exception as e:
        print(f"[EXCH] fetch_ohlcv error for {symbol} on {getattr(exchange, 'id', 'unknown')}: {e}", flush=True)
        return None

# ----- Indicator computation (same logic as before)
def compute_signals(df1h, df15m):
    results = {
        "vol_pct_24h": None,
        "price_pct_15_vs_1h": None,
        "rsi_1h": None,
        "macd_bull_cross": False,
        "last_close": None
    }
    try:
        if df1h is None or df1h.empty:
            return results
        results['last_close'] = float(df1h['close'].iloc[-1])
    except Exception:
        pass

    # Volume % change (24h): need at least 48 hours of 1h bars
    if df1h is not None and len(df1h) >= 48:
        try:
            vol_last24 = df1h['volume'].iloc[-24:].sum()
            vol_prev24 = df1h['volume'].iloc[-48:-24].sum()
            if vol_prev24 > 0:
                results['vol_pct_24h'] = (vol_last24 / vol_prev24 - 1.0) * 100.0
        except Exception:
            results['vol_pct_24h'] = None

    # Price % change (15m vs 1h), use last 15m close vs previous 1h close
    if df15m is not None and not df15m.empty and df1h is not None and len(df1h) >= 2:
        try:
            close_15m = float(df15m['close'].iloc[-1])
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

    # MACD (1h) bullish crossover
    if df1h is not None and len(df1h) >= 35:
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

# ----- Main scanning logic
def scan_once():
    print(f"[SCAN] Starting scan {datetime.now(timezone.utc).isoformat()}", flush=True)
    markets = coingecko_top_markets(COINGECKO_TOP)
    candidates = [m for m in markets if (m.get("total_volume") or 0) >= MIN_VOLUME_USD]
    print(f"[SCAN] Candidates after CG volume filter: {len(candidates)}", flush=True)

    exch_instances = build_exchange_instances()
    mapped = map_to_exchange_symbols_multi(exch_instances, candidates)
    if not mapped:
        print("[SCAN] No mapped exchange symbols found. Exiting.", flush=True)
        return []

    alerts_sent = []
    for coin, symbol, ex_name in mapped:
        time.sleep(SLEEP_BETWEEN_SYMBOLS)
        exchange = exch_instances.get(ex_name)
        if exchange is None:
            print(f"[SCAN] Skipping {symbol}: no exchange instance for {ex_name}", flush=True)
            continue

        # prefer strict USDT pairs (you can allow BUSD/USD by removing this check)
        if not symbol.endswith("/USDT"):
            # optional: continue to include BUSD/USD pairs; here we still accept them
            pass

        df1h = fetch_ohlcv(exchange, symbol, timeframe='1h', limit=OHLCV_LIMIT_1H)
        df15m = fetch_ohlcv(exchange, symbol, timeframe='15m', limit=OHLCV_LIMIT_15M)
        if df1h is None or df15m is None:
            print(f"[SCAN] Skipping {symbol}: missing OHLCV data", flush=True)
            continue

        sig = compute_signals(df1h, df15m)
        # must have core metrics
        if sig['vol_pct_24h'] is None or sig['price_pct_15_vs_1h'] is None or sig['rsi_1h'] is None:
            print(f"[SCAN] Skipping {symbol}: insufficient metrics {sig}", flush=True)
            continue

        vol_ok = sig['vol_pct_24h'] >= VOL_CHANGE_THRESHOLD_PCT
        price_ok = PRICE_MIN_PCT <= sig['price_pct_15_vs_1h'] <= PRICE_MAX_PCT
        rsi_ok = (sig['rsi_1h'] is not None) and (RSI_LOW <= sig['rsi_1h'] <= RSI_HIGH)
        macd_ok = sig['macd_bull_cross']

        print(f"[SIGNAL] {symbol}@{ex_name} vol%={sig['vol_pct_24h']:.1f} price15vs1h={sig['price_pct_15_vs_1h']:.2f} rsi1h={sig['rsi_1h']} macd={sig['macd_bull_cross']}", flush=True)

        if vol_ok and price_ok and rsi_ok and macd_ok:
            coin_id = coin.get("id")
            if was_alerted_recent(coin_id):
                print(f"[SCAN] SKIP recent alert {coin_id}", flush=True)
                continue
            text = (
                f"ALERT {symbol} ({ex_name})\n"
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
                record_alert(coin_id, coin.get("symbol").upper(), "multi_ex_tv", sig['price_pct_15_vs_1h'], sig['vol_pct_24h'])
                alerts_sent.append(coin_id)

    print(f"[SCAN] Done. Alerts sent: {alerts_sent}", flush=True)
    return alerts_sent

if __name__ == "__main__":
    scan_once()
