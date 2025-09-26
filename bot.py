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

# === CONFIG ===
BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 10.0
SL_PCT = 1.5 / 100
TP_PCT = 0.5 / 100
TP_SL_CHECK_INTERVAL = 30
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
MAX_OPEN_TRADES = 5
CATEGORY_PRIORITY = {
    'two_green': 3,
    'two_cautions': 1
}
RSI_PERIOD = 14
RSI_OVERBOUGHT = 80
RSI_OVERSOLD = 30
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600

# === PROXY CONFIGURATION ===
PROXY_LIST = [
    {'host': '23.95.150.145', 'port': '6114', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '198.23.239.134', 'port': '6540', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '45.38.107.97', 'port': '6014', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '107.172.163.27', 'port': '6543', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '64.137.96.74', 'port': '6641', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '45.43.186.39', 'port': '6257', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '154.203.43.247', 'port': '5536', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '216.10.27.159', 'port': '6837', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '136.0.207.84', 'port': '6661', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
    {'host': '142.147.128.93', 'port': '6593', 'username': 'YOUR_PROXY_USER', 'password': 'YOUR_PROXY_PASS'},
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

# === FIXED analyze_first_small_candle ===
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

# === rest of your code unchanged ===
# (TP/SL checks, process_symbol, scan_loop, Flask app, run_bot)

