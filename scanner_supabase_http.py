# scanner_supabase_http.py
# Multi-exchange, exchange-sourced candidate scanner
# - scans all USDT/BUSD/USD spot markets across exchanges listed in EXCHANGES env
# - computes TradingView-style filters: vol%24h, price 15m vs 1h, RSI(1h), MACD(1h)
# - writes deduped alerts to Supabase via REST (service role key) and sends Telegram messages
# - coin_id used for dedupe: "<exchange>:<base>"

import os
import time
import requests
import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from ta.momentum import RSIIndicator
from ta.trend import MACD

load_dotenv()

# --- Env / config (required)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
EXCHANGES = os.getenv("EXCHANGES", "binance")  # repo variable recommended

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SUPABASE_URL, SUPABASE_SERVICE_KEY in environment or repo variables")

TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
REST_BASE = SUPABASE_URL.rstrip("/") + "/rest/v1"
HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# ----- Scanner tuning (changeable)
DEDUPE_HOURS = 6
SLEEP_BETWEEN_SYMBOLS = 0.4
MIN_VOLUME_USD = 10000           # baseline liquidity for candidate markets
OHLCV_LIMIT_1H = 72              # hours of 1h bars
OHLCV_LIMIT_15M = 8              # 15m bars
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

# ----- Exchange helpers
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
        instances["binance"] = ccxt.binance({"enableRateLimit": True})
    return instances

def build_candidates_from_exchanges(exchange_instances):
    """
    Build candidate list from exchange markets: returns list of dicts:
    { "exchange": ex_name, "market": market_symbol, "base": base, "quote": quote, "volume": 24h_volume_estimate }
    """
    candidates = []
    for name, ex in exchange_instances.items():
        try:
            ex.load_markets()
        except Exception as e:
            print(f"[EXCH] Failed load_markets for {name}: {e}", flush=True)
            continue
        # markets is dict: 'BASE/QUOTE' -> market
        for m_sym, m in ex.markets.items():
            # some markets have .get('spot') flag; we only want spot markets
            # prefer quote USDT/BUSD/USD
            quote = m_sym.split("/")[-1] if "/" in m_sym else None
            if quote not in ("USDT", "BUSD", "USD"):
                continue
            base = m_sym.split("/")[0]
            # estimate volume if market info provides it (not guaranteed)
            vol = None
            try:
                info = m.get("info", {}) or {}
                # try to get 24h volume in quote or base depending on exchange
                vol = info.get("quoteVolume") or info.get("volume") or info.get("quoteVolume24h") or None
            except Exception:
                vol = None
            candidates.append({"exchange": name, "market": m_sym, "base": base, "quote": quote, "volume": vol})
    print(f"[CAND] Built {len(candidates)} exchange-sourced candidates", flush=True)
    return candidates

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

# ----- Indicators
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

    if df1h is not None and len(df1h) >= 48:
        try:
            vol_last24 = df1h['volume'].iloc[-24:].sum()
            vol_prev24 = df1h['volume'].iloc[-48:-24].sum()
            if vol_prev24 > 0:
                results['vol_pct_24h'] = (vol_last24 / vol_prev24 - 1.0) * 100.0
        except Exception:
            results['vol_pct_24h'] = None

    if df15m is not None and not df15m.empty and df1h is not None and len(df1h) >= 2:
        try:
            close_15m = float(df15m['close'].iloc[-1])
            close_1h_ago = float(df1h['close'].iloc[-2])
            results['price_pct_15_vs_1h'] = (close_15m / close_1h_ago - 1.0) * 100.0
        except Exception:
            results['price_pct_15_vs_1h'] = None

    if df1h is not None and len(df1h) >= 20:
        try:
            rsi = RSIIndicator(df1h['close'], window=14).rsi()
            results['rsi_1h'] = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else None
        except Exception:
            results['rsi_1h'] = None

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

# ----- Main scanning logic (exchange-sourced candidates)
def scan_once():
    print(f"[SCAN] Starting scan {datetime.now(timezone.utc).isoformat()}", flush=True)
    exch_instances = build_exchange_instances()
    candidates = build_candidates_from_exchanges(exch_instances)

    # quick filter by baseline 24h volume estimate if available
    filtered = []
    for c in candidates:
        vol = c.get("volume")
        if vol is None:
            filtered.append(c)  # keep unknown-volume markets (we will compute from OHLCV)
            continue
        try:
            v = float(vol)
            if v >= MIN_VOLUME_USD:
                filtered.append(c)
        except Exception:
            filtered.append(c)
    print(f"[SCAN] Candidates after baseline filter: {len(filtered)}", flush=True)

    alerts_sent = []
    for c in filtered:
        ex_name = c['exchange']
        symbol = c['market']
        base = c['base']
        exchange = exch_instances.get(ex_name)
        if exchange is None:
            continue
        time.sleep(SLEEP_BETWEEN_SYMBOLS)

        # fetch OHLCV
        df1h = fetch_ohlcv(exchange, symbol, timeframe='1h', limit=OHLCV_LIMIT_1H)
        df15m = fetch_ohlcv(exchange, symbol, timeframe='15m', limit=OHLCV_LIMIT_15M)
        if df1h is None or df15m is None:
            print(f"[SCAN] Skipping {symbol}@{ex_name}: missing OHLCV", flush=True)
            continue

        sig = compute_signals(df1h, df15m)
        if sig['vol_pct_24h'] is None or sig['price_pct_15_vs_1h'] is None or sig['rsi_1h'] is None:
            print(f"[SCAN] Skipping {symbol}@{ex_name}: insufficient metrics {sig}", flush=True)
            continue

        vol_ok = sig['vol_pct_24h'] >= VOL_CHANGE_THRESHOLD_PCT
        price_ok = PRICE_MIN_PCT <= sig['price_pct_15_vs_1h'] <= PRICE_MAX_PCT
        rsi_ok = (sig['rsi_1h'] is not None) and (RSI_LOW <= sig['rsi_1h'] <= RSI_HIGH)
        macd_ok = sig['macd_bull_cross']

        print(f"[SIGNAL] {symbol}@{ex_name} vol%={sig['vol_pct_24h']:.1f} price15vs1h={sig['price_pct_15_vs_1h']:.2f} rsi1h={sig['rsi_1h']} macd={sig['macd_bull_cross']}", flush=True)

        if vol_ok and price_ok and rsi_ok and macd_ok:
            coin_id = f"{ex_name}:{base}"
            if was_alerted_recent(coin_id):
                print(f"[SCAN] SKIP recent alert {coin_id}", flush=True)
                continue
            text = (
                f"ALERT {symbol} ({ex_name})\n"
                f"Pair: {symbol}\n"
                f"Price: ${sig['last_close']:.8f}\n"
                f"Vol change 24h: {sig['vol_pct_24h']:.1f}%\n"
                f"Price 15m vs 1h: {sig['price_pct_15_vs_1h']:.2f}%\n"
                f"RSI 1h: {sig['rsi_1h']:.1f}\n"
                f"MACD bullish crossover: {sig['macd_bull_cross']}\n"
                f"Exchange: {ex_name}\n"
                f"Symbol: {symbol}"
            )
            ok = send_telegram(text)
            if ok:
                record_alert(coin_id, symbol, "exchange_multi_tv", sig['price_pct_15_vs_1h'], sig['vol_pct_24h'])
                alerts_sent.append(coin_id)

    print(f"[SCAN] Done. Alerts sent: {alerts_sent}", flush=True)
    return alerts_sent

if __name__ == "__main__":
    scan_once()
