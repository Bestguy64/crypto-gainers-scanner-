# scanner_mvp.py (patched to use 'ta' instead of pandas_ta)
import os
import time
import math
import requests
import ccxt
import pandas as pd
from dotenv import load_dotenv
from ta.momentum import RSIIndicator

load_dotenv()

# Read secrets
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")

TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
EXCHANGES = os.getenv("EXCHANGES", "binance").split(",")

# Config - tune these numbers for first run
COINGECKO_TOP = 100
VOL_GAIN_WINDOW = 10
VOL_GAIN_THRESHOLD_PCT = 150.0
PRICE_BREAKOUT_LOOKBACK = 20
PRICE_BREAKOUT_PCT = 1.5
RSI_LOWER = 20
RSI_UPPER = 85
OHLCV_LIMIT = 500
SLEEP_BETWEEN_SYMBOLS = 0.5
SLEEP_BETWEEN_LOOPS = 300

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

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

def coingecko_top_gainers(limit=COINGECKO_TOP):
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
    data = r.json()
    sorted_data = sorted(data, key=lambda x: (x.get("price_change_percentage_24h") or 0), reverse=True)
    return sorted_data

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

    # Compute RSI using 'ta' library
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
    coins = coingecko_top_gainers(limit=COINGECKO_TOP)
    candidates = coins[:COINGECKO_TOP]

    exchange = ccxt.binance({"enableRateLimit": True})
    exchange.load_markets()

    mapped = map_to_exchange_symbols(exchange, candidates)
    print(f"Mapped {len(mapped)} symbols to exchange markets")

    alerts = []
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
        rsi_ok = (ind['rsi_15'] is not None and ind['rsi_1h'] is not None and
                  ind['rsi_15'] > RSI_LOWER and ind['rsi_1h'] < RSI_UPPER)

        if vol_ok and breakout_ok and rsi_ok:
            text = (
                f"ALERT {symbol}\n"
                f"Name: {coin.get('name')} ({coin.get('symbol').upper()})\n"
                f"Price: ${ind['last_close']:.8f}\n"
                f"Vol gain% (15m vs avg): {ind['vol_gain_pct']:.1f}%\n"
                f"Breakout% vs {PRICE_BREAKOUT_LOOKBACK} bars: {ind['breakout_pct']:.2f}%\n"
                f"RSI 15m: {ind['rsi_15']:.1f}, RSI 1h: {ind['rsi_1h']:.1f}\n"
                f"CoinGecko 24h change: {coin.get('price_change_percentage_24h'):.2f}%\n"
            )
            print("ALERT MATCH:", symbol)
            ok = send_telegram(text)
            if ok:
                alerts.append(symbol)

    return alerts

if __name__ == "__main__":
    alerts = scan_once()
    print("Done. Alerts sent for:", alerts)
