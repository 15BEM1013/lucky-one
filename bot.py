import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import json
import os
import logging

# === CONFIG ===
BOT_TOKEN = '8320917334:AAEFyQAgLk-elAxF8kFpydHvch7dqyLKGo0'
CHAT_ID = '655537138'
TIMEFRAMES = ['5m', '30m']
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 20.0
LEVERAGE = 5                      # ← Updated to 5x
SL_PCT = 3.0 / 100
TP_PCT = 1.0 / 100
TP_SL_CHECK_INTERVAL = 30
TRADE_FILE = 'open_trades.json'
BODY_SIZE_THRESHOLD = 0.1

# === PROXY CONFIGURATION ===
PROXY_LIST = [
    # your proxies here if any
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === THREAD LOCK ===
trade_lock = threading.Lock()

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
        print(f"Trades saved to {TRADE_FILE}")
    except Exception as e:
        print(f"Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                loaded = json.load(f)
                open_trades = {k: v for k, v in loaded.items()}
            print(f"Loaded {len(open_trades)} trades from {TRADE_FILE}")
    except Exception as e:
        print(f"Error loading trades: {e}")
        open_trades = {}

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    try:
        response = requests.post(url, data=data, timeout=5, proxies=proxies).json()
        print(f"Telegram sent: {msg}")
        return response.get('result', {}).get('message_id')
    except Exception as e:
        print(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    try:
        requests.post(url, data=data, timeout=5, proxies=proxies)
        print(f"Telegram updated: {new_text}")
    except Exception as e:
        print(f"Edit error: {e}")

# === INIT EXCHANGE ===
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            logging.info(f"Trying proxy: {proxy['host']}:{proxy['port']}")
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(pool_maxsize=20, max_retries=retries))
            exchange = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
            })
            exchange.load_markets()
            logging.info(f"Connected using proxy: {proxy['host']}:{proxy['port']}")
            return exchange, proxies
        except Exception as e:
            logging.error(f"Proxy failed {proxy['host']}:{proxy['port']}: {e}")
            continue

    logging.error("All proxies failed. Trying direct connection.")
    try:
        exchange = ccxt.binance({
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })
        exchange.load_markets()
        logging.info("Connected directly.")
        return exchange, None
    except Exception as e:
        logging.error(f"Direct connection failed: {e}")
        raise Exception("Connection failed.")

app = Flask(__name__)
sent_signals = {}
open_trades = {}
try:
    exchange, proxies = initialize_exchange()
except Exception as e:
    logging.error(f"Exchange init failed: {e}")
    exit(1)

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0
def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

def upper_wick_pct(c):
    if is_bullish(c) and (c[4] - c[1]) != 0:
        return (c[2] - c[4]) / (c[4] - c[1]) * 100
    elif is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[2] - c[1]) / (c[1] - c[4]) * 100
    return 0

def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100 if candle[1] != 0 else 0
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100 if candle[1] != 0 else 0
    wick_ratio = upper_wick / lower_wick if lower_wick != 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick != 0 else float('inf')

    if pattern_type == 'rising':
        if wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}

    elif pattern_type == 'falling':
        if wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}

# === PRICE ROUNDING ===
def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][0]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except Exception as e:
        print(f"Error rounding price for {symbol}: {e}")
        return price

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5 if len(candles) >= 6 else 0
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = (
        is_bearish(c1) and
        body_pct(c1) < MAX_SMALL_BODY_PCT and
        lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and
        c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    )
    small_red_0 = (
        is_bearish(c0) and
        body_pct(c0) < MAX_SMALL_BODY_PCT and
        lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and
        c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    )
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5 if len(candles) >= 6 else 0
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = (
        is_bullish(c1) and
        body_pct(c1) < MAX_SMALL_BODY_PCT and
        c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    )
    small_green_0 = (
        is_bullish(c0) and
        body_pct(c0) < MAX_SMALL_BODY_PCT and
        c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    )
    return big_red and small_green_1 and small_green_0

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']

# === NEXT CANDLE CLOSE ===
def get_next_candle_close(tf_minutes):
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    cycle = tf_minutes * 60
    seconds_to_next = cycle - (seconds % cycle)
    if seconds_to_next < 5:
        seconds_to_next += cycle
    return time.time() + seconds_to_next

# === TP/SL MONITOR ===
def check_tp_sl():
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        hit = ""
                        hit_price = None
                        entry_time = trade.get('entry_time')
                        if entry_time:
                            candles_1m = exchange.fetch_ohlcv(sym, '1m', since=entry_time, limit=2880)
                            for c in candles_1m:
                                high, low = c[2], c[3]
                                if trade['side'] == 'buy':
                                    if high >= trade['tp']:
                                        hit = "✅ TP hit"
                                        hit_price = trade['tp']
                                        break
                                    if low <= trade['sl']:
                                        hit = "❌ SL hit"
                                        hit_price = trade['sl']
                                        break
                                else:
                                    if low <= trade['tp']:
                                        hit = "✅ TP hit"
                                        hit_price = trade['tp']
                                        break
                                    if high >= trade['sl']:
                                        hit = "❌ SL hit"
                                        hit_price = trade['sl']
                                        break

                        if not hit:
                            ticker = exchange.fetch_ticker(sym)
                            last = round_price(sym, ticker['last'])
                            if trade['side'] == 'buy':
                                if last >= trade['tp']:
                                    hit = "✅ TP hit"
                                    hit_price = trade['tp']
                                elif last <= trade['sl']:
                                    hit = "❌ SL hit"
                                    hit_price = trade['sl']
                            else:
                                if last <= trade['tp']:
                                    hit = "✅ TP hit"
                                    hit_price = trade['tp']
                                elif last >= trade['sl']:
                                    hit = "❌ SL hit"
                                    hit_price = trade['sl']

                        if hit:
                            if trade['side'] == 'buy':
                                pnl = (hit_price - trade['entry']) / trade['entry'] * 100
                            else:
                                pnl = (trade['entry'] - hit_price) / trade['entry'] * 100
                            leveraged_pnl_pct = pnl * LEVERAGE
                            profit = CAPITAL * leveraged_pnl_pct / 100
                            logging.info(f"Closed {sym}: {hit} | PnL: {leveraged_pnl_pct:.2f}%")

                            new_msg = (
                                f"{sym} - {'REVERSED SELL' if trade['side'] == 'sell' else 'REVERSED BUY'} PATTERN\n"
                                f"entry - {trade['entry']}\n"
                                f"tp - {trade['tp']}\n"
                                f"sl - {trade['sl']}\n"
                                f"Profit/Loss: {leveraged_pnl_pct:.2f}% (${profit:.2f})\n"
                                f"{hit}"
                            )
                            trade['msg'] = new_msg
                            trade['hit'] = hit
                            edit_telegram_message(trade['msg_id'], new_msg)
                            del open_trades[sym]
                            save_trades()
                            logging.info(f"Trade closed: {sym}")
                    except Exception as e:
                        logging.error(f"TP/SL check error {sym}: {e}")
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL loop error: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, timeframe, alert_queue):
    try:
        for attempt in range(3):
            try:
                candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=10)
                if len(candles) < 6:
                    return
                if attempt < 2 and candles[-1][0] > candles[-2][0]:
                    break
                time.sleep(0.5)
            except ccxt.NetworkError as e:
                print(f"Network error {symbol}: {e}")
                time.sleep(2 ** attempt)
                continue

        signal_time = candles[-2][0]
        first_small_close = round_price(symbol, candles[-3][4])
        second_small_close = round_price(symbol, candles[-2][4])

        if detect_rising_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'rising')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            key = (symbol, 'rising', timeframe)
            if sent_signals.get(key) == signal_time:
                return
            sent_signals[key] = signal_time

            side = 'buy'
            entry_price = second_small_close
            tp = round_price(symbol, first_small_close * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))
            tp_distance = (tp - entry_price) / entry_price * 100
            pattern = 'rising'

            msg = (
                f"{symbol} ({timeframe}) - REVERSED BUY (Rising Three tiny bodies)\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"TP Distance: {tp_distance:.2f}%\n"
                f"sl - {sl}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, {}, 'signal', side, entry_price, tp, sl,
                             first_candle_analysis['text'], first_candle_analysis['status'],
                             first_candle_analysis['body_pct'], pattern))

        elif detect_falling_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'falling')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            key = (symbol, 'falling', timeframe)
            if sent_signals.get(key) == signal_time:
                return
            sent_signals[key] = signal_time

            side = 'sell'
            entry_price = second_small_close
            tp = round_price(symbol, first_small_close * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))
            tp_distance = (entry_price - tp) / entry_price * 100
            pattern = 'falling'

            msg = (
                f"{symbol} ({timeframe}) - REVERSED SELL (Falling Three tiny bodies)\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"TP Distance: {tp_distance:.2f}%\n"
                f"sl - {sl}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, {}, 'signal', side, entry_price, tp, sl,
                             first_candle_analysis['text'], first_candle_analysis['status'],
                             first_candle_analysis['body_pct'], pattern))

    except ccxt.RateLimitExceeded:
        time.sleep(5)
    except Exception as e:
        logging.error(f"Error {symbol} ({timeframe}): {e}")

# === BATCH PROCESSING ===
def process_batch(symbols, timeframe, alert_queue):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol, symbol, timeframe, alert_queue): symbol for symbol in symbols}
        for future in as_completed(futures):
            future.result()

# === SCAN LOOP ===
def scan_loop(timeframe):
    tf_minutes = int(timeframe[:-1])
    load_trades()
    symbols = get_symbols()
    print(f"Scanning {len(symbols)} symbols on {timeframe}...")
    alert_queue = queue.Queue()
    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    def send_alerts():
        while True:
            try:
                item = alert_queue.get(timeout=1)
                symbol, msg, _, _, side, entry_price, tp, sl, first_text, pressure, body_pct, pattern = item
                with trade_lock:
                    mid = send_telegram(msg)
                    if mid and symbol not in open_trades:
                        trade = {
                            'side': side,
                            'entry': entry_price,
                            'tp': tp,
                            'sl': sl,
                            'msg': msg,
                            'msg_id': mid,
                            'entry_time': int(time.time() * 1000),
                            'pattern': pattern
                        }
                        open_trades[symbol] = trade
                        save_trades()
                        logging.info(f"Opened trade: {symbol}")
                alert_queue.task_done()
            except queue.Empty:
                time.sleep(1)
            except Exception as e:
                logging.error(f"Alert thread error: {e}")

    threading.Thread(target=send_alerts, daemon=True).start()

    while True:
        next_close = get_next_candle_close(tf_minutes)
        wait = max(0, next_close - time.time())
        print(f"Waiting {wait:.1f}s for next {timeframe} close...")
        time.sleep(wait)

        for i, chunk in enumerate(symbol_chunks):
            print(f"Batch {i+1}/{NUM_CHUNKS} ({timeframe})")
            process_batch(chunk, timeframe, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)

        print(f"Scan finished ({timeframe})")
        print(f"Tracking {len(open_trades)} open positions")

# === FLASK ===
@app.route('/')
def home():
    return "Rising & Falling Three Bot (5m + 30m) - Running"

# === MAIN ===
def run_bot():
    load_trades()
    num_open = len(open_trades)
    startup_msg = f"BOT STARTED\nTracking {num_open} open positions\nLeverage: {LEVERAGE}x\nNo max trades"
    send_telegram(startup_msg)

    threading.Thread(target=check_tp_sl, daemon=True).start()

    for tf in TIMEFRAMES:
        threading.Thread(target=scan_loop, args=(tf,), daemon=True).start()

    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
