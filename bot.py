import ccxt
import time
import threading
import requests
from flask import Flask
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import numpy as np
import logging
from datetime import datetime
import pytz

app = Flask(__name__)

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
CAPITAL = 8.0
LEVERAGE = 5
SL_PCT = 0.03
TP_PCT = 0.01
MAX_OPEN_TRADES = 5
BODY_SIZE_THRESHOLD = 0.1

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    try:
        requests.post(url, data=data, timeout=5).json()
        print(f"Telegram: {msg}")
    except Exception as e:
        print(f"Telegram error: {e}")

# === INIT EXCHANGE ===
def initialize_exchange():
    exchange = ccxt.binance({
        'apiKey': 'T9inhK51iTAStVD0td5vZQwbB1OtX54AhYx12Q7rM1VdgfpYnklaUP2gTF0mGj3Z',
        'secret': 'H1GkoQm8HeIKCYZSVhbtkVn2DUmUKdHXBjqH5vUEwLXRmRnJ29CXoXnz66eFplNZ',
        'options': {'defaultType': 'future'},
        'enableRateLimit': True,
    })
    exchange.load_markets()
    try:
        exchange.fapiPrivate_post_positionside_dual({'dualSidePosition': False})
        send_telegram("Bot started - One-way mode set")
    except Exception as e:
        send_telegram(f"Position mode error: {e}")
    return exchange

exchange = initialize_exchange()

# === HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100
def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period: return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

def round_price(symbol, price):
    return exchange.price_to_precision(symbol, price)

def round_quantity(symbol, qty):
    return exchange.amount_to_precision(symbol, qty)

# === FIRST SMALL CANDLE ANALYSIS (RESTORED) ===
def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    wick_ratio = upper_wick / lower_wick if lower_wick != 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick != 0 else float('inf')

    if pattern_type == 'rising':
        if wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure'}
        elif wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure'}
        elif body >= 0.1:
            if wick_ratio_reverse >= 2.5:
                return {'text': f"Buying pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'buying_pressure'}
            elif wick_ratio >= 2.5:
                return {'text': f"Selling pressure ⚠️\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'selling_pressure'}
            else:
                return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral'}
        else:
            return {'text': f"Neutral ✅\nUpper wick: {upper_wick:.2f}%\nLower wick: {lower_wick:.2f}%\nBody: {body:.2f}%", 'status': 'neutral'}
    else:  # falling
        similar logic...

# === LEVERAGE & QUANTITY ===
def set_leverage(symbol):
    try:
        sym = symbol.replace('/', '')
        exchange.fapiPrivate_post_leverage({'symbol': sym, 'leverage': LEVERAGE})
    except Exception as e:
        send_telegram(f"Leverage error {symbol}: {e}")

def calculate_quantity(symbol, entry_price):
    notional = CAPITAL * LEVERAGE
    qty = notional / entry_price
    market = exchange.market(symbol)
    min_qty = float(market['limits']['amount']['min'])
    if qty < min_qty:
        send_telegram(f"Skip {symbol}: Qty {qty:.6f} < min {min_qty}")
        return None
    return round_quantity(symbol, qty)

# === OPEN POSITION WITH TP/SL ===
def open_position(symbol, side, entry_price, tp_price, sl_price, analysis_text):
    send_telegram(f"Executing {side.upper()} on {symbol}\nFirst small candle: {analysis_text}\nEntry ~{entry_price}")

    try:
        balance = exchange.fetch_balance()['USDT']['free']
        if balance < CAPITAL * 1.1:
            send_telegram(f"Insufficient balance: {balance:.2f} USDT")
            return
    except Exception as e:
        send_telegram(f"Balance check error: {e}")
        return

    qty = calculate_quantity(symbol, entry_price)
    if not qty: return

    set_leverage(symbol)

    try:
        order = exchange.create_order(symbol, 'market', side, qty, params={'positionSide': 'BOTH'})
        actual_entry = float(order.get('average') or order.get('price') or entry_price)
        send_telegram(f"Opened {side.upper()} {symbol} at {actual_entry} (Qty: {qty})")

        close_side = 'sell' if side == 'buy' else 'buy'
        exchange.create_order(symbol, 'stop_market', close_side, qty,
                              params={'stopPrice': sl_price, 'reduceOnly': True, 'positionSide': 'BOTH'})
        send_telegram(f"SL placed at {sl_price}")

        exchange.create_order(symbol, 'take_profit_market', close_side, qty,
                              params={'stopPrice': tp_price, 'reduceOnly': True, 'positionSide': 'BOTH'})
        send_telegram(f"TP placed at {tp_price}")

    except Exception as e:
        send_telegram(f"Order error {symbol}: {e}")

# === PATTERN DETECTION (with restored analysis) ===
def detect_rising_three(candles):
    # ... same as before ...

def detect_falling_three(candles):
    # ... same as before ...

sent_signals = {}

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    # ... fetch candles, ema ...
    if detect_rising_three(candles):
        analysis = analyze_first_small_candle(candles[-3], 'rising')
        if body_pct(candles[-3]) > BODY_SIZE_THRESHOLD: return
        if sent_signals.get((symbol, 'rising')) == signal_time: return
        sent_signals[(symbol, 'rising')] = signal_time

        if first_close > ema21 and ema9 > ema21:
            side = 'sell'
            entry = second_close
            tp = round_price(symbol, first_close * (1 - TP_PCT))
            sl = round_price(symbol, entry * (1 + SL_PCT))
            alert_queue.put((symbol, side, entry, tp, sl, analysis['text']))

    elif detect_falling_three(candles):
        analysis = analyze_first_small_candle(candles[-3], 'falling')
        # ... similar for falling ...

# === SCAN LOOP (same as minimal version) ===
# ... (with open_position call including analysis_text)

# === FLASK (RESTORED) ===
@app.route('/')
def home():
    return "✅ Rising & Falling Three Pattern Bot is Live!"

if __name__ == "__main__":
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)
