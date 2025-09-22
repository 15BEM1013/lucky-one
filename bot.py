
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
import talib
import numpy as np
import logging
from dotenv import load_dotenv
import redis

# Load environment variables
load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

# Initialize Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# === CONFIG ===
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 10.0  # $10 per trade
LEVERAGE = 5  # 5x leverage
SL_PCT = 1.5 / 100
TP_PCT = 0.7 / 100  # Take-profit set to 0.7%
TP_SL_CHECK_INTERVAL = 30
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
        redis_client.set('open_trades', json.dumps(open_trades, default=str))
        logging.info("Trades saved to Redis")
    except Exception as e:
        logging.error(f"Error saving trades to Redis: {e}")

def load_trades():
    global open_trades
    try:
        data = redis_client.get('open_trades')
        if data:
            open_trades = json.loads(data)
            logging.info(f"Loaded {len(open_trades)} trades from Redis")
        else:
            open_trades = {}
    except Exception as e:
        logging.error(f"Error loading trades from Redis: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = load_closed_trades()
        all_closed_trades.append(closed_trade)
        redis_client.set('closed_trades', json.dumps(all_closed_trades, default=str))
        logging.info("Closed trade saved to Redis")
    except Exception as e:
        logging.error(f"Error saving closed trades to Redis: {e}")

def load_closed_trades():
    try:
        data = redis_client.get('closed_trades')
        if data:
            return json.loads(data)
        return []
    except Exception as e:
        logging.error(f"Error loading closed trades from Redis: {e}")
        return []

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    try:
        response = requests.post(url, data=data, timeout=5, proxies=proxies).json()
        logging.info(f"Telegram sent: {msg}")
        return response.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    try:
        requests.post(url, data=data, timeout=5, proxies=proxies)
        logging.info(f"Telegram updated: {new_text}")
    except Exception as e:
        logging.error(f"Edit error: {e}")

# === INIT ===
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange(testnet=False):
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            logging.info(f"Trying proxy: {proxy['host']}:{proxy['port']}")
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(pool_maxsize=20, max_retries=retries))
            exchange = ccxt.binance({
                'apiKey': API_KEY,
                'secret': API_SECRET,
                'enableRateLimit': True,
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'session': session,
                'urls': {'api': {'public': 'https://testnet.binancefuture.com', 'private': 'https://testnet.binancefuture.com'}} if testnet else {}
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
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
            'urls': {'api': {'public': 'https://testnet.binancefuture.com', 'private': 'https://testnet.binancefuture.com'}} if testnet else {}
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
    exchange, proxies = initialize_exchange(testnet=False)  # Live trading
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
        logging.error(f"Error rounding price for {symbol}: {e}")
        return price

# === ORDER PLACEMENT ===
def set_leverage(symbol, leverage):
    try:
        exchange.fapiPrivate_post_leverage({
            'symbol': exchange.market(symbol)['id'],
            'leverage': leverage
        })
        logging.info(f"Set leverage {leverage}x for {symbol}")
    except Exception as e:
        logging.error(f"Error setting leverage for {symbol}: {e}")

def place_order(symbol, side, entry_price, tp, sl):
    try:
        # Calculate quantity based on capital and leverage
        market = exchange.market(symbol)
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        quantity = (CAPITAL * LEVERAGE) / current_price
        quantity = exchange.amount_to_precision(symbol, quantity)

        # Set leverage
        set_leverage(symbol, LEVERAGE)

        # Place market order
        order = exchange.create_market_order(symbol, side, quantity)
        logging.info(f"Placed {side} market order for {symbol}: {quantity} @ {current_price}")

        # Set TP/SL orders
        if side == 'buy':
            tp_order = exchange.create_order(
                symbol, 'limit', 'sell', quantity, tp,
                {'type': 'TAKE_PROFIT', 'stopPrice': tp}
            )
            sl_order = exchange.create_order(
                symbol, 'stop_market', 'sell', quantity, None,
                {'type': 'STOP_MARKET', 'stopPrice': sl}
            )
        else:
            tp_order = exchange.create_order(
                symbol, 'limit', 'buy', quantity, tp,
                {'type': 'TAKE_PROFIT', 'stopPrice': tp}
            )
            sl_order = exchange.create_order(
                symbol, 'stop_market', 'buy', quantity, None,
                {'type': 'STOP_MARKET', 'stopPrice': sl}
            )
        logging.info(f"Set TP {tp} and SL {sl} for {symbol}")
        return order['id'], tp_order['id'], sl_order['id']
    except Exception as e:
        logging.error(f"Error placing order for {symbol}: {e}")
        return None, None, None

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    # Check for big green candle followed by one small red candle
    c2, c1 = candles[-3], candles[-2]  # Adjusted to check one candle earlier
    avg_volume = sum(c[5] for c in candles[-5:-1]) / 4
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = (
        is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and
        c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    return big_green and small_red_1

def detect_falling_three(candles):
    # Check for big red candle followed by one small green candle
    c2, c1 = candles[-3], candles[-2]  # Adjusted to check one candle earlier
    avg_volume = sum(c[5] for c in candles[-5:-1]) / 4
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = (
        is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    return big_red and small_green_1

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']

# === CANDLE CLOSE ===
def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === TP/SL CHECK ===
def check_tp_sl():
    global closed_trades
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        hit = ""
                        pnl = 0
                        hit_price = None

                        # Check if TP or SL order was filled
                        if trade.get('tp_order_id'):
                            tp_order = exchange.fetch_order(trade['tp_order_id'], sym)
                            if tp_order['status'] == 'closed':
                                hit = "✅ TP hit"
                                hit_price = trade['tp']
                        if not hit and trade.get('sl_order_id'):
                            sl_order = exchange.fetch_order(trade['sl_order_id'], sym)
                            if sl_order['status'] == 'closed':
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
                            ema_status = trade['ema_status']
                            new_msg = (
                                f"{sym} - {'REVERSED SELL' if trade['side'] == 'sell' and trade['pattern'] == 'rising' else 'REVERSED BUY' if trade['side'] == 'buy' and trade['pattern'] == 'falling' else trade['pattern'].upper()} PATTERN\n"
                                f"{'Above' if trade['pattern'] == 'rising' else 'Below'} 21 ema - {ema_status['price_ema21']}\n"
                                f"ema 9 {'above' if trade['pattern'] == 'rising' else 'below'} 21 - {ema_status['ema9_ema21']}\n"
                                f"First small candle: {trade['first_candle_analysis']}\n"
                                f"entry - {trade['entry']}\n"
                                f"tp - {trade['tp']}\n"
                                f"sl - {trade['sl']}\n"
                                f"Profit/Loss: {pnl:.2f}% (${profit:.2f})\n{hit}"
                            )
                            trade['msg'] = new_msg
                            trade['hit'] = hit
                            logging.info(f"Updating Telegram message for {sym}: {hit}")
                            edit_telegram_message(trade['msg_id'], new_msg)
                            # Cancel any remaining orders
                            try:
                                if trade.get('order_id'):
                                    exchange.cancel_order(trade['order_id'], sym)
                                if trade.get('tp_order_id') and not hit.startswith("✅"):
                                    exchange.cancel_order(trade['tp_order_id'], sym)
                                if trade.get('sl_order_id') and not hit.startswith("❌"):
                                    exchange.cancel_order(trade['sl_order_id'], sym)
                            except Exception as e:
                                logging.error(f"Error canceling orders for {sym}: {e}")
                            del open_trades[sym]
                            save_trades()
                            logging.info(f"Trade closed for {sym}")
                    except Exception as e:
                        logging.error(f"TP/SL check error on {sym}: {e}")
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL loop error: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        for attempt in range(3):
            try:
                candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=30)
                if len(candles) < 25:
                    return
                if attempt < 2 and candles[-1][0] > candles[-2][0]:
                    break
                time.sleep(0.5)
            except ccxt.NetworkError as e:
                logging.error(f"Network error on {symbol}: {e}")
                time.sleep(2 ** attempt)
                continue

        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        rsi = calculate_rsi(candles, period=RSI_PERIOD)
        if ema21 is None or ema9 is None or rsi is None:
            return

        signal_time = candles[-3][0]  # Use first small candle for signal time
        first_small_candle_close = round_price(symbol, candles[-3][4])  # Entry at first small candle close

        if detect_rising_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'rising')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            if sent_signals.get((symbol, 'rising')) == signal_time:
                return
            sent_signals[(symbol, 'rising')] = signal_time
            price_above_ema21 = first_small_candle_close > ema21
            ema9_above_ema21 = ema9 > ema21
            ema_status = {
                'price_ema21': '✅' if price_above_ema21 else '⚠️',
                'ema9_ema21': '✅' if ema9_above_ema21 else '⚠️'
            }
            green_count = sum(1 for v in ema_status.values() if v == '✅')
            if green_count == 2:
                category = 'two_green'
            elif green_count == 1:
                category = 'one_green'
            else:
                category = 'two_cautions'
            side = 'sell'
            entry_price = first_small_candle_close
            tp = round_price(symbol, entry_price * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))
            pattern = 'rising'
            msg = (
                f"{symbol} - {'REVERSED SELL' if side == 'sell' else 'RISING'} PATTERN\n"
                f"Above 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 above 21 - {ema_status['ema9_ema21']}\n"
                f"RSI: {rsi:.2f}\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"sl - {sl}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern))

        elif detect_falling_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'falling')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                return
            if sent_signals.get((symbol, 'falling')) == signal_time:
                return
            sent_signals[(symbol, 'falling')] = signal_time
            price_below_ema21 = first_small_candle_close < ema21
            ema9_below_ema21 = ema9 < ema21
            ema_status = {
                'price_ema21': '✅' if price_below_ema21 else '⚠️',
                'ema9_ema21': '✅' if ema9_below_ema21 else '⚠️'
            }
            green_count = sum(1 for v in ema_status.values() if v == '✅')
            if green_count == 2:
                category = 'two_green'
            elif green_count == 1:
                category = 'one_green'
            else:
                category = 'two_cautions'
            side = 'buy'
            entry_price = first_small_candle_close
            tp = round_price(symbol, entry_price * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))
            pattern = 'falling'
            msg = (
                f"{symbol} - {'REVERSED BUY' if side == 'buy' else 'FALLING'} PATTERN\n"
                f"Below 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 below 21 - {ema_status['ema9_ema21']}\n"
                f"RSI: {rsi:.2f}\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"sl - {sl}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern))

    except ccxt.RateLimitExceeded:
        time.sleep(5)
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
    logging.info(f"🔍 Scanning {len(symbols)} Binance Futures symbols...")
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
                            order_id, tp_order_id, sl_order_id = place_order(symbol, side, entry_price, tp, sl)
                            if order_id:
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
                                    'pattern': pattern,
                                    'order_id': order_id,
                                    'tp_order_id': tp_order_id,
                                    'sl_order_id': sl_order_id
                                }
                                open_trades[symbol] = trade
                                save_trades()
                                logging.info(f"New trade opened for {symbol}")
                    else:
                        lowest_priority = min(
                            (CATEGORY_PRIORITY[trade['category']] for trade in open_trades.values()),
                            default=0
                        )
                        if CATEGORY_PRIORITY[category] > lowest_priority:
                            for sym, trade in list(open_trades.items()):
                                if CATEGORY_PRIORITY[trade['category']] == lowest_priority:
                                    try:
                                        if trade.get('order_id'):
                                            exchange.cancel_order(trade['order_id'], sym)
                                        if trade.get('tp_order_id'):
                                            exchange.cancel_order(trade['tp_order_id'], sym)
                                        if trade.get('sl_order_id'):
                                            exchange.cancel_order(trade['sl_order_id'], sym)
                                    except Exception as e:
                                        logging.error(f"Error canceling orders for {sym}: {e}")
                                    edit_telegram_message(
                                        trade['msg_id'],
                                        f"{sym} - Trade canceled for higher-priority signal."
                                    )
                                    del open_trades[sym]
                                    save_trades()
                                    mid = send_telegram(msg)
                                    if mid and symbol not in open_trades:
                                        order_id, tp_order_id, sl_order_id = place_order(symbol, side, entry_price, tp, sl)
                                        if order_id:
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
                                                'pattern': pattern,
                                                'order_id': order_id,
                                                'tp_order_id': tp_order_id,
                                                'sl_order_id': sl_order_id
                                            }
                                            open_trades[symbol] = trade
                                            save_trades()
                                            logging.info(f"Replaced trade with higher priority for {symbol}")
                                    break
                    alert_queue.task_done()
            except queue.Empty:
                with trade_lock:
                    for sym, trade in list(open_trades.items()):
                        if 'hit' in trade:
                            edit_telegram_message(trade['msg_id'], trade['msg'])
                            logging.info(f"Safety net update for {sym}: {trade['hit']}")
                time.sleep(1)
                continue
            except Exception as e:
                logging.error(f"Alert thread error: {e}")
                time.sleep(1)

    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp_sl, daemon=True).start()

    while True:
        next_close = get_next_candle_close()
        wait_time = max(0, next_close - time.time())
        logging.info(f"⏳ Waiting {wait_time:.1f} seconds for next 15m candle close...")
        time.sleep(wait_time)

        for i, chunk in enumerate(symbol_chunks):
            logging.info(f"Processing batch {i+1}/{NUM_CHUNKS}...")
            process_batch(chunk, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)

        logging.info("✅ Scan complete.")
        num_open = len(open_trades)
        logging.info(f"📊 Number of open trades: {num_open}")

        current_time = time.time()
        if current_time - last_summary_time >= SUMMARY_INTERVAL:
            all_closed_trades = load_closed_trades()
            two_green_trades = [t for t in all_closed_trades if t['category'] == 'two_green']
            one_green_trades = [t for t in all_closed_trades if t['category'] == 'one_green']
            two_cautions_trades = [t for t in all_closed_trades if t['category'] == 'two_cautions']

            def get_pressure_metrics(trades):
                small_body_trades = [t for t in trades if t.get('body_pct', float('inf')) <= BODY_SIZE_THRESHOLD]

                small_neutral_trades = [t for t in small_body_trades if t.get('pressure_status') == 'neutral']
                small_selling_trades = [t for t in small_body_trades if t.get('pressure_status') == 'selling_pressure']
                small_buying_trades = [t for t in small_body_trades if t.get('pressure_status') == 'buying_pressure']

                def calc_metrics(trade_list):
                    count = len(trade_list)
                    wins = sum(1 for t in trade_list if t.get('pnl', 0) > 0)
                    losses = sum(1 for t in trade_list if t.get('pnl', 0) < 0)
                    tp_hits = sum(1 for t in trade_list if t.get('hit') == '✅ TP hit')
                    sl_hits = sum(1 for t in trade_list if t.get('hit') == '❌ SL hit')
                    pnl = sum(t.get('pnl', 0) for t in trade_list)
                    pnl_pct = sum(t.get('pnl_pct', 0) for t in trade_list)
                    win_rate = (wins / count * 100) if count > 0 else 0.00
                    return count, wins, losses, tp_hits, sl_hits, pnl, pnl_pct, win_rate

                small_neutral_metrics = calc_metrics(small_neutral_trades)
                small_selling_metrics = calc_metrics(small_selling_trades)
                small_buying_metrics = calc_metrics(small_buying_trades)
                small_total_metrics = calc_metrics(small_body_trades)
                total_metrics = calc_metrics(trades)

                return {
                    'small_body': {
                        'neutral': small_neutral_metrics,
                        'selling': small_selling_metrics,
                        'buying': small_buying_metrics,
                        'total': small_total_metrics
                    },
                    'total': total_metrics
                }

            two_green_metrics = get_pressure_metrics(two_green_trades)
            one_green_metrics = get_pressure_metrics(one_green_trades)
            two_cautions_metrics = get_pressure_metrics(two_cautions_trades)

            total_pnl = sum(t.get('pnl', 0) for t in all_closed_trades)
            total_pnl_pct = sum(t.get('pnl_pct', 0) for t in all_closed_trades)
            cumulative_pnl = total_pnl
            cumulative_pnl_pct = total_pnl_pct

            if all_closed_trades:
                symbol_pnl = {}
                for trade in all_closed_trades:
                    sym = trade.get('symbol', '')
                    symbol_pnl[sym] = symbol_pnl.get(sym, 0) + trade.get('pnl', 0)
                top_symbol = max(symbol_pnl.items(), key=lambda x: x[1], default=(None, 0))
                top_symbol_name, top_symbol_pnl = top_symbol
                top_symbol_pnl_pct = sum(t.get('pnl_pct', 0) for t in all_closed_trades if t.get('symbol') == top_symbol_name)
            else:
                top_symbol_name, top_symbol_pnl, top_symbol_pnl_pct = None, 0, 0

            timestamp = get_ist_time().strftime("%I:%M %p IST, %B %d, %Y")
            summary_msg = (
                f"🔍 Hourly Summary at {timestamp}\n"
                f"📊 Trade Summary (Closed Trades):\n"
                f"- ✅✅ Two Green Ticks:\n"
                f"  - Neutral ✅: {two_green_metrics['small_body']['neutral'][0]} trades (W: {two_green_metrics['small_body']['neutral'][1]}, L: {two_green_metrics['small_body']['neutral'][2]}, TP: {two_green_metrics['small_body']['neutral'][3]}, SL: {two_green_metrics['small_body']['neutral'][4]}), PnL: ${two_green_metrics['small_body']['neutral'][5]:.2f} ({two_green_metrics['small_body']['neutral'][6]:.2f}%), Win Rate: {two_green_metrics['small_body']['neutral'][7]:.2f}%\n"
                f"  - Selling Pressure ⚠️: {two_green_metrics['small_body']['selling'][0]} trades (W: {two_green_metrics['small_body']['selling'][1]}, L: {two_green_metrics['small_body']['selling'][2]}, TP: {two_green_metrics['small_body']['selling'][3]}, SL: {two_green_metrics['small_body']['selling'][4]}), PnL: ${two_green_metrics['small_body']['selling'][5]:.2f} ({two_green_metrics['small_body']['selling'][6]:.2f}%), Win Rate: {two_green_metrics['small_body']['selling'][7]:.2f}%\n"
                f"  - Buying Pressure ⚠️: {two_green_metrics['small_body']['buying'][0]} trades (W: {two_green_metrics['small_body']['buying'][1]}, L: {two_green_metrics['small_body']['buying'][2]}, TP: {two_green_metrics['small_body']['buying'][3]}, SL: {two_green_metrics['small_body']['buying'][4]}), PnL: ${two_green_metrics['small_body']['buying'][5]:.2f} ({two_green_metrics['small_body']['buying'][6]:.2f}%), Win Rate: {two_green_metrics['small_body']['buying'][7]:.2f}%\n"
                f"  - Total: {two_green_metrics['small_body']['total'][0]} trades (W: {two_green_metrics['small_body']['total'][1]}, L: {two_green_metrics['small_body']['total'][2]}, TP: {two_green_metrics['small_body']['total'][3]}, SL: {two_green_metrics['small_body']['total'][4]}), PnL: ${two_green_metrics['small_body']['total'][5]:.2f} ({two_green_metrics['small_body']['total'][6]:.2f}%), Win Rate: {two_green_metrics['small_body']['total'][7]:.2f}%\n"
                f"  - Overall Total: {two_green_metrics['total'][0]} trades (W: {two_green_metrics['total'][1]}, L: {two_green_metrics['total'][2]}, TP: {two_green_metrics['total'][3]}, SL: {two_green_metrics['total'][4]}), PnL: ${two_green_metrics['total'][5]:.2f} ({two_green_metrics['total'][6]:.2f}%), Win Rate: {two_green_metrics['total'][7]:.2f}%\n"
                f"- ✅⚠️ One Green One Caution:\n"
                f"  - Neutral ✅: {one_green_metrics['small_body']['neutral'][0]} trades (W: {one_green_metrics['small_body']['neutral'][1]}, L: {one_green_metrics['small_body']['neutral'][2]}, TP: {one_green_metrics['small_body']['neutral'][3]}, SL: {one_green_metrics['small_body']['neutral'][4]}), PnL: ${one_green_metrics['small_body']['neutral'][5]:.2f} ({one_green_metrics['small_body']['neutral'][6]:.2f}%), Win Rate: {one_green_metrics['small_body']['neutral'][7]:.2f}%\n"
                f"  - Selling Pressure ⚠️: {one_green_metrics['small_body']['selling'][0]} trades (W: {one_green_metrics['small_body']['selling'][1]}, L: {one_green_metrics['small_body']['selling'][2]}, TP: {one_green_metrics['small_body']['selling'][3]}, SL: {one_green_metrics['small_body']['selling'][4]}), PnL: ${one_green_metrics['small_body']['selling'][5]:.2f} ({one_green_metrics['small_body']['selling'][6]:.2f}%), Win Rate: {one_green_metrics['small_body']['selling'][7]:.2f}%\n"
                f"  - Buying Pressure ⚠️: {one_green_metrics['small_body']['buying'][0]} trades (W: {one_green_metrics['small_body']['buying'][1]}, L: {one_green_metrics['small_body']['buying'][2]}, TP: {one_green_metrics['small_body']['buying'][3]}, SL: {one_green_metrics['small_body']['buying'][4]}), PnL: ${one_green_metrics['small_body']['buying'][5]:.2f} ({one_green_metrics['small_body']['buying'][6]:.2f}%), Win Rate: {one_green_metrics['small_body']['buying'][7]:.2f}%\n"
                f"  - Total: {one_green_metrics['small_body']['total'][0]} trades (W: {one_green_metrics['small_body']['total'][1]}, L: {one_green_metrics['small_body']['total'][2]}, TP: {one_green_metrics['small_body']['total'][3]}, SL: {one_green_metrics['small_body']['total'][4]}), PnL: ${one_green_metrics['small_body']['total'][5]:.2f} ({one_green_metrics['small_body']['total'][6]:.2f}%), Win Rate: {one_green_metrics['small_body']['total'][7]:.2f}%\n"
                f"  - Overall Total: {one_green_metrics['total'][0]} trades (W: {one_green_metrics['total'][1]}, L: {one_green_metrics['total'][2]}, TP: {one_green_metrics['total'][3]}, SL: {one_green_metrics['total'][4]}), PnL: ${one_green_metrics['total'][5]:.2f} ({one_green_metrics['total'][6]:.2f}%), Win Rate: {one_green_metrics['total'][7]:.2f}%\n"
                f"- ⚠️⚠️ Two Cautions:\n"
                f"  - Neutral ✅: {two_cautions_metrics['small_body']['neutral'][0]} trades (W: {two_cautions_metrics['small_body']['neutral'][1]}, L: {two_cautions_metrics['small_body']['neutral'][2]}, TP: {two_cautions_metrics['small_body']['neutral'][3]}, SL: {two_cautions_metrics['small_body']['neutral'][4]}), PnL: ${two_cautions_metrics['small_body']['neutral'][5]:.2f} ({two_cautions_metrics['small_body']['neutral'][6]:.2f}%), Win Rate: {two_cautions_metrics['small_body']['neutral'][7]:.2f}%\n"
                f"  - Selling Pressure ⚠️: {two_cautions_metrics['small_body']['selling'][0]} trades (W: {two_cautions_metrics['small_body']['selling'][1]}, L: {two_cautions_metrics['small_body']['selling'][2]}, TP: {two_cautions_metrics['small_body']['selling'][3]}, SL: {two_cautions_metrics['small_body']['selling'][4]}), PnL: ${two_cautions_metrics['small_body']['selling'][5]:.2f} ({two_cautions_metrics['small_body']['selling'][6]:.2f}%), Win Rate: {two_cautions_metrics['small_body']['selling'][7]:.2f}%\n"
                f"  - Buying Pressure ⚠️: {two_cautions_metrics['small_body']['buying'][0]} trades (W: {two_cautions_metrics['small_body']['buying'][1]}, L: {two_cautions_metrics['small_body']['buying'][2]}, TP: {two_cautions_metrics['small_body']['buying'][3]}, SL: {two_cautions_metrics['small_body']['buying'][4]}), PnL: ${two_cautions_metrics['small_body']['buying'][5]:.2f} ({two_cautions_metrics['small_body']['buying'][6]:.2f}%), Win Rate: {two_cautions_metrics['small_body']['buying'][7]:.2f}%\n"
                f"  - Total: {two_cautions_metrics['small_body']['total'][0]} trades (W: {two_cautions_metrics['small_body']['total'][1]}, L: {two_cautions_metrics['small_body']['total'][2]}, TP: {two_cautions_metrics['small_body']['total'][3]}, SL: {two_cautions_metrics['small_body']['total'][4]}), PnL: ${two_cautions_metrics['small_body']['total'][5]:.2f} ({two_cautions_metrics['small_body']['total'][6]:.2f}%), Win Rate: {two_cautions_metrics['small_body']['total'][7]:.2f}%\n"
                f"  - Overall Total: {two_cautions_metrics['total'][0]} trades (W: {two_cautions_metrics['total'][1]}, L: {two_cautions_metrics['total'][2]}, TP: {two_cautions_metrics['total'][3]}, SL: {two_cautions_metrics['total'][4]}), PnL: ${two_cautions_metrics['total'][5]:.2f} ({two_cautions_metrics['total'][6]:.2f}%), Win Rate: {two_cautions_metrics['total'][7]:.2f}%\n"
                f"💰 Total Executed PnL: ${total_pnl:.2f} ({total_pnl_pct:.2f}%)\n"
                f"📈 Cumulative Executed PnL: ${cumulative_pnl:.2f} ({cumulative_pnl_pct:.2f}%)\n"
                f"🏆 Top Symbol: {top_symbol_name or 'None'} with ${top_symbol_pnl:.2f} ({top_symbol_pnl_pct:.2f}%)\n"
                f"🔄 Open Trades: {num_open}"
            )
            send_telegram(summary_msg)
            send_telegram(f"Number of open trades after scan: {num_open}")
            last_summary_time = current_time
            closed_trades = []

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
