import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import json
import os
import talib
import numpy as np
import logging

# === CONFIG ===
BOT_TOKEN = '7402265241:AAHRDxd12LRizl1qTsQggEEoJ-BeWME3ERo'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 10.0
SL_PCT = 1.5 / 100
TP_PCT = 0.7 / 100
TP_SL_CHECK_INTERVAL = 30
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
MAX_OPEN_TRADES = 5
CATEGORY_PRIORITY = {
    'two_green': 3,
    'one_green': 2,
    'two_cautions': 1
}
RSI_PERIOD = 14
RSI_OVERBOUGHT = 80
RSI_OVERSOLD = 30
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600

# === PROXY CONFIGURATION ===
PROXY_LIST = [
    {'host': '23.95.150.145', 'port': '6114', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '198.23.239.134', 'port': '6540', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '45.38.107.97', 'port': '6014', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '107.172.163.27', 'port': '6543', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '64.137.96.74', 'port': '6641', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '45.43.186.39', 'port': '6257', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '154.203.43.247', 'port': '5536', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '216.10.27.159', 'port': '6837', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '136.0.207.84', 'port': '6661', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
    {'host': '142.147.128.93', 'port': '6593', 'username': 'ihpzjkrb', 'password': '4s5y5kaq34cs'},
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# === CONFIGURE LOGGING ===
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

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                all_closed_trades = json.load(f)
        all_closed_trades.append(closed_trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(all_closed_trades, f, default=str)
        print(f"Closed trade saved to {CLOSED_TRADE_FILE}")
    except Exception as e:
        print(f"Error saving closed trades: {e}")

def load_closed_trades():
    try:
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        print(f"Error loading closed trades: {e}")
        return []

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

# === INIT ===
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
            logging.info(f"Successfully connected using proxy: {proxy['host']}:{proxy['port']}")
            return exchange, proxies
        except Exception as e:
            logging.error(f"Failed to connect with proxy {proxy['host']}:{proxy['port']}: {e}")
            continue
    logging.error("All proxies failed. Falling back to direct connection.")
    try:
        exchange = ccxt.binance({
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })
        exchange.load_markets()
        logging.info("Successfully connected using direct connection.")
        return exchange, None
    except Exception as e:
        logging.error(f"Direct connection failed: {e}")
        raise Exception("All proxies and direct connection failed.")

app = Flask(__name__)

sent_signals = {}
open_trades = {}
closed_trades = []
last_summary_time = 0

try:
    exchange, proxies = initialize_exchange()
except Exception as e:
    logging.error(f"Failed to initialize exchange: {e}")
    exit(1)

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100
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
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    wick_ratio = upper_wick / lower_wick if lower_wick != 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick != 0 else float('inf')

    if pattern_type == 'rising':
        if wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
    elif pattern_type == 'falling':
        if wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
        elif wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure', 'body_pct': body}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure ❌\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure', 'body_pct': body}
            else:
                return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}
        else:
            return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral', 'body_pct': body}

# === EMA ===
def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

# === RSI ===
def calculate_rsi(candles, period=14):
    closes = np.array([c[4] for c in candles])
    if len(closes) < period:
        return None
    return talib.RSI(closes, timeperiod=period)[-1]

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

# === PATTERN DETECTION - COMPLETELY REWRITTEN ===
def detect_rising_three(candles):
    """
    Detect Rising Three pattern at the FIRST small candle close
    Pattern: Big Green -> First Small Red (THIS is when we alert)
    """
    if len(candles) < 5:
        return False
    
    # candles[-1] = current forming candle (ignore this)
    # candles[-2] = previous candle (should be the first small red candle that just closed)
    # candles[-3] = big green candle
    # candles[-4] = candle before big green
    
    big_candle = candles[-3]  # The big green candle
    first_small_candle = candles[-2]  # The first small red candle that JUST closed
    
    # Verify the big candle is indeed big and green
    if not (is_bullish(big_candle) and body_pct(big_candle) >= MIN_BIG_BODY_PCT):
        return False
    
    # Verify the first small candle is small and red
    if not (is_bearish(first_small_candle) and body_pct(first_small_candle) <= MAX_SMALL_BODY_PCT):
        return False
    
    # Check if first small candle is within the range of big candle
    if first_small_candle[4] > big_candle[3] + (big_candle[2] - big_candle[3]) * 0.3:
        return True
    
    return False

def detect_falling_three(candles):
    """
    Detect Falling Three pattern at the FIRST small candle close
    Pattern: Big Red -> First Small Green (THIS is when we alert)
    """
    if len(candles) < 5:
        return False
    
    # candles[-1] = current forming candle (ignore this)
    # candles[-2] = previous candle (should be the first small green candle that just closed)
    # candles[-3] = big red candle
    # candles[-4] = candle before big red
    
    big_candle = candles[-3]  # The big red candle
    first_small_candle = candles[-2]  # The first small green candle that JUST closed
    
    # Verify the big candle is indeed big and red
    if not (is_bearish(big_candle) and body_pct(big_candle) >= MIN_BIG_BODY_PCT):
        return False
    
    # Verify the first small candle is small and green
    if not (is_bullish(first_small_candle) and body_pct(first_small_candle) <= MAX_SMALL_BODY_PCT):
        return False
    
    # Check if first small candle is within the range of big candle
    if first_small_candle[4] < big_candle[2] - (big_candle[2] - big_candle[3]) * 0.3:
        return True
    
    return False

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']

# === CANDLE CLOSE TIMING ===
def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    return time.time() + seconds_to_next

def is_candle_close_time():
    """Check if we're at a 15-minute candle close time (:00, :15, :30, :45)"""
    now = get_ist_time()
    return now.minute % 15 == 0 and now.second >= 0

# === TP/SL CHECK ===
def check_tp_sl():
    global closed_trades
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        hit = ""
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
                            
                            logging.info(f"TP/SL hit for {sym}: {hit}, PnL: {pnl:.2f}%")
                            profit = CAPITAL * pnl / 100
                            closed_trade = {
                                'symbol': sym,
                                'pnl': profit,
                                'pnl_pct': pnl,
                                'category': trade['category'],
                                'ema_status': trade['ema_status'],
                                'pressure_status': trade['pressure_status'],
                                'hit': hit,
                                'body_pct': trade['body_pct']
                            }
                            closed_trades.append(closed_trade)
                            save_closed_trades(closed_trade)
                            
                            new_msg = f"{trade['msg']}\nProfit/Loss: {pnl:.2f}% (${profit:.2f})\n{hit}"
                            edit_telegram_message(trade['msg_id'], new_msg)
                            del open_trades[sym]
                            save_trades()
                            
                    except Exception as e:
                        logging.error(f"TP/SL check error on {sym}: {e}")
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL loop error: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        # Get candles
        candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=10)
        if len(candles) < 5:
            return

        current_time = get_ist_time()
        print(f"🔍 {symbol} - Processing at {current_time.strftime('%H:%M:%S')} IST")
        
        # Debug candle times
        for i, candle in enumerate(candles[-5:]):
            candle_time = datetime.fromtimestamp(candle[0] / 1000).astimezone(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
            print(f"  Candle {-5+i}: {candle_time} - O: {candle[1]}, H: {candle[2]}, L: {candle[3]}, C: {candle[4]}")

        # Calculate indicators
        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        rsi = calculate_rsi(candles, period=RSI_PERIOD)
        if ema21 is None or ema9 is None or rsi is None:
            return

        # Use the candle that just closed (candles[-2])
        signal_time = candles[-2][0]  # This is the first small candle timestamp
        first_small_candle_close = round_price(symbol, candles[-2][4])

        if detect_rising_three(candles):
            print(f"🚨 RISING THREE DETECTED for {symbol}!")
            first_candle_analysis = analyze_first_small_candle(candles[-2], 'rising')
            
            if sent_signals.get((symbol, 'rising')) == signal_time:
                return
            sent_signals[(symbol, 'rising')] = signal_time
            
            price_above_ema21 = first_small_candle_close > ema21
            ema9_above_ema21 = ema9 > ema21
            ema_status = {
                'price_ema21': '✅' if price_above_ema21 else '❌',
                'ema9_ema21': '✅' if ema9_above_ema21 else '❌'
            }
            
            green_count = sum(1 for v in ema_status.values() if v == '✅')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'
            
            side = 'sell'
            entry_price = first_small_candle_close
            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))
            
            alert_time = datetime.fromtimestamp(signal_time / 1000).astimezone(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
            msg = (
                f"{symbol} - RISING PATTERN\n"
                f"Alert Time: {alert_time} (First Small Candle Close)\n"
                f"Above 21 ema - {ema_status['price_ema21']}\n"
                f"EMA9 above EMA21 - {ema_status['ema9_ema21']}\n"
                f"RSI: {rsi:.2f}\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"Entry: {entry_price}\n"
                f"TP: {tp} (0.7%)\n"
                f"SL: {sl}\n"
                f"Trade active..."
            )
            print(f"📤 Queueing alert for {symbol}")
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], 'rising'))

        elif detect_falling_three(candles):
            print(f"🚨 FALLING THREE DETECTED for {symbol}!")
            first_candle_analysis = analyze_first_small_candle(candles[-2], 'falling')
            
            if sent_signals.get((symbol, 'falling')) == signal_time:
                return
            sent_signals[(symbol, 'falling')] = signal_time
            
            price_below_ema21 = first_small_candle_close < ema21
            ema9_below_ema21 = ema9 < ema21
            ema_status = {
                'price_ema21': '✅' if price_below_ema21 else '❌',
                'ema9_ema21': '✅' if ema9_below_ema21 else '❌'
            }
            
            green_count = sum(1 for v in ema_status.values() if v == '✅')
            category = 'two_green' if green_count == 2 else 'one_green' if green_count == 1 else 'two_cautions'
            
            side = 'buy'
            entry_price = first_small_candle_close
            tp = round_price(symbol, entry_price * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))
            
            alert_time = datetime.fromtimestamp(signal_time / 1000).astimezone(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
            msg = (
                f"{symbol} - FALLING PATTERN\n"
                f"Alert Time: {alert_time} (First Small Candle Close)\n"
                f"Below 21 ema - {ema_status['price_ema21']}\n"
                f"EMA9 below EMA21 - {ema_status['ema9_ema21']}\n"
                f"RSI: {rsi:.2f}\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"Entry: {entry_price}\n"
                f"TP: {tp} (0.7%)\n"
                f"SL: {sl}\n"
                f"Trade active..."
            )
            print(f"📤 Queueing alert for {symbol}")
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], 'falling'))

    except Exception as e:
        logging.error(f"Error on {symbol}: {e}")

# === PROCESS BATCH ===
def process_batch(symbols, alert_queue):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol, alert_queue): symbol for symbol in symbols}
        for future in as_completed(future_to_symbol):
            future.result()

# === SCAN LOOP ===
def scan_loop():
    global closed_trades, last_summary_time
    load_trades()
    symbols = get_symbols()
    print(f"🔍 Scanning {len(symbols)} Binance Futures symbols...")
    alert_queue = queue.Queue()

    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    def send_alerts():
        while True:
            try:
                symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis, pressure_status, body_pct, pattern = alert_queue.get(timeout=1)
                with trade_lock:
                    if len(open_trades) < MAX_OPEN_TRADES:
                        mid = send_telegram(msg)
                        if mid and symbol not in open_trades:
                            trade = {
                                'side': side,
                                'entry': entry_price,
                                'tp': tp,
                                'sl': sl,
                                'msg': msg,
                                'msg_id': mid,
                                'ema_status': ema_status,
                                'category': category,
                                'first_candle_analysis': first_candle_analysis,
                                'pressure_status': pressure_status,
                                'body_pct': body_pct,
                                'entry_time': int(time.time() * 1000),
                                'pattern': pattern
                            }
                            open_trades[symbol] = trade
                            save_trades()
                            print(f"✅ Trade opened for {symbol}")
                    alert_queue.task_done()
            except queue.Empty:
                time.sleep(1)
            except Exception as e:
                logging.error(f"Alert thread error: {e}")
                time.sleep(1)

    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp_sl, daemon=True).start()

    # Wait for first proper candle close
    print("⏳ Waiting for next 15-minute candle close...")
    next_close = get_next_candle_close()
    time.sleep(max(0, next_close - time.time()))

    while True:
        current_time = get_ist_time()
        print(f"\n🎯 SCANNING at {current_time.strftime('%H:%M:%S')} IST (Candle just closed)")
        
        # Process all symbols
        for i, chunk in enumerate(symbol_chunks):
            print(f"Processing batch {i+1}/{NUM_CHUNKS}...")
            process_batch(chunk, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)

        print("✅ Scan complete. Waiting for next candle close...")
        
        # Wait for next candle close
        next_close = get_next_candle_close()
        wait_time = max(0, next_close - time.time())
        print(f"⏰ Next scan in {wait_time:.1f} seconds...")
        time.sleep(wait_time)

# === FLASK ===
@app.route('/')
def home():
    return "✅ Rising & Falling Three Pattern Bot is Live!"

# === RUN ===
def run_bot():
    global last_summary_time
    load_trades()
    num_open = len(open_trades)
    last_summary_time = time.time()
    startup_msg = f"BOT STARTED\nNumber of open trades: {num_open}"
    send_telegram(startup_msg)
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
