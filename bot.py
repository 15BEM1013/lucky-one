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
BOT_TOKEN = '7402265241:AAHRDxd12LRizl1qTsQggEEoJ-BeWME3ERo'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 8.0  # USDT margin per trade
LEVERAGE = 5
SL_PCT = 3.0 / 100
TP_PCT = 1.0 / 100
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
    global proxies
    proxies = None
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            logging.info(f"Trying proxy: {proxy['host']}:{proxy['port']}")
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(pool_maxsize=20, max_retries=retries))
            exchange = ccxt.binance({
                'apiKey': 'T9inhK51iTAStVD0td5vZQwbB1OtX54AhYx12Q7rM1VdgfpYnklaUP2gTF0mGj3Z',  # INSERT YOUR REAL API KEY
                'secret': 'H1GkoQm8HeIKCYZSVhbtkVn2DUmUKdHXBjqH5vUEwLXRmRnJ29CXoXnz66eFplNZ',  # INSERT YOUR REAL API SECRET
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
                # For testnet: uncomment below and use testnet keys
                # 'urls': {'api': {'fapi': 'https://testnet.binancefuture.com/fapi'}},
            })
            exchange.load_markets()
            logging.info(f"Successfully connected using proxy: {proxy['host']}:{proxy['port']}")
            return exchange
        except Exception as e:
            logging.error(f"Failed to connect with proxy {proxy['host']}:{proxy['port']}: {e}")
            continue
    logging.error("All proxies failed. Falling back to direct connection.")
    try:
        exchange = ccxt.binance({
            'apiKey': 'T9inhK51iTAStVD0td5vZQwbB1OtX54AhYx12Q7rM1VdgfpYnklaUP2gTF0mGj3Z',  # INSERT YOUR REAL API KEY
            'secret': 'H1GkoQm8HeIKCYZSVhbtkVn2DUmUKdHXBjqH5vUEwLXRmRnJ29CXoXnz66eFplNZ',  # INSERT YOUR REAL API SECRET
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
            # For testnet: uncomment below and use testnet keys
            # 'urls': {'api': {'fapi': 'https://testnet.binancefuture.com/fapi'}},
        })
        exchange.load_markets()
        logging.info("Successfully connected using direct connection.")
        return exchange
    except Exception as e:
        logging.error(f"Direct connection failed: {e}")
        raise Exception("All proxies and direct connection failed.")

app = Flask(__name__)

sent_signals = {}
open_trades = {}
closed_trades = []
last_summary_time = 0

try:
    exchange = initialize_exchange()
except Exception as e:
    logging.error(f"Failed to initialize exchange: {e}")
    exit(1)

# Set position mode to one-way (run once at startup)
try:
    exchange.fapiPrivate_post_positionside_dual({'dualSidePosition': False})
    logging.info("Set position mode to one-way.")
    send_telegram("Bot started: Position mode set to one-way.")
except Exception as e:
    logging.error(f"Failed to set position mode: {e}")
    send_telegram(f"Error setting position mode: {e}")

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

# === PRICE/QUANTITY ROUNDING ===
def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['precision']['price'])
        return exchange.price_to_precision(symbol, price)
    except Exception as e:
        print(f"Error rounding price for {symbol}: {e}")
        return price

def round_quantity(symbol, qty):
    market = exchange.market(symbol)
    step_size = float(market['precision']['amount'])
    return exchange.amount_to_precision(symbol, qty)

# === SET LEVERAGE ===
def set_leverage(symbol):
    try:
        sym = symbol.replace('/', '')
        exchange.fapiPrivate_post_leverage({'symbol': sym, 'leverage': LEVERAGE})
        logging.info(f"Set leverage {LEVERAGE}x for {symbol}")
        send_telegram(f"Step: Leverage set to {LEVERAGE}x for {symbol}")
    except Exception as e:
        logging.error(f"Failed to set leverage for {symbol}: {e}")
        send_telegram(f"Error setting leverage for {symbol}: {e}")

# === CALCULATE QUANTITY ===
def calculate_quantity(symbol, entry_price):
    notional = CAPITAL * LEVERAGE
    qty = notional / entry_price
    market = exchange.market(symbol)
    min_qty = float(market['limits']['amount']['min'])
    if qty < min_qty:
        logging.warning(f"Qty {qty} < min {min_qty} for {symbol}. Skipping trade.")
        send_telegram(f"Step: Skipping trade for {symbol} - Quantity {qty} below minimum {min_qty}")
        return None
    return round_quantity(symbol, qty)

# === OPEN POSITION ===
def open_position(symbol, side, entry_price, tp, sl, pattern):
    send_telegram(f"Step: Attempting to open {side.upper()} position on {symbol} at approx {entry_price}")
    try:
        balance = exchange.fetch_balance()
        free_usdt = balance.get('USDT', {}).get('free', 0)
        required = CAPITAL * 1.1  # 10% buffer for fees
        if free_usdt < required:
            logging.warning(f"Insufficient balance for {symbol}: {free_usdt} < {required}")
            send_telegram(f"Error: Insufficient balance for {symbol} - Available: {free_usdt} USDT, Required: {required} USDT")
            return None, None
        send_telegram(f"Step: Balance check passed for {symbol} - Available: {free_usdt} USDT")
    except Exception as e:
        logging.error(f"Balance check failed for {symbol}: {e}")
        send_telegram(f"Error: Balance check failed for {symbol}: {e}")
        return None, None

    qty = calculate_quantity(symbol, entry_price)
    if not qty:
        return None, None

    set_leverage(symbol)

    try:
        # Market order to open
        order = exchange.create_order(
            symbol=symbol,
            type='market',
            side=side,
            amount=qty,
            params={'positionSide': 'BOTH'}  # One-way mode
        )
        actual_entry = float(order['average']) if 'average' in order else entry_price  # Get fill price
        logging.info(f"Opened {side} position for {symbol} at {actual_entry}, qty: {qty}")
        send_telegram(f"Step: Position opened for {symbol} - {side.upper()} at {actual_entry}, Quantity: {qty}")

        # Place SL
        close_side = 'sell' if side == 'buy' else 'buy'
        sl_order = exchange.create_order(
            symbol=symbol,
            type='stop_market',
            side=close_side,
            amount=qty,
            params={
                'stopPrice': sl,
                'reduceOnly': True,
                'positionSide': 'BOTH'
            }
        )
        logging.info(f"Placed SL at {sl} for {symbol}")
        send_telegram(f"Step: SL placed at {sl} for {symbol}")

        # Place TP
        tp_order = exchange.create_order(
            symbol=symbol,
            type='take_profit_market',
            side=close_side,
            amount=qty,
            params={
                'stopPrice': tp,
                'reduceOnly': True,
                'positionSide': 'BOTH'
            }
        )
        logging.info(f"Placed TP at {tp} for {symbol}")
        send_telegram(f"Step: TP placed at {tp} for {symbol}")

        return actual_entry, order['id']
    except Exception as e:
        logging.error(f"Failed to open position or place TP/SL for {symbol}: {e}")
        send_telegram(f"Error: Failed to open position or place TP/SL for {symbol}: {e}")
        return None, None

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = (
        is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and
        c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    small_red_0 = (
        is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and
        c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    )
    volume_decreasing = c1[5] > c0[5]
    return big_green and small_red_1 and small_red_0 and volume_decreasing

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = (
        is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    small_green_0 = (
        is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    )
    volume_decreasing = c1[5] > c0[5]
    return big_red and small_green_1 and small_green_0 and volume_decreasing

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

# === TP/SL CHECK (keep for logging/Telegram updates, exchange handles real close) ===
def check_tp_sl():
    global closed_trades
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        # Check if position still open via API
                        positions = exchange.fapiPrivate_get_positionrisk({'symbol': sym.replace('/', '')})
                        position = next((p for p in positions if float(p['positionAmt']) != 0), None)
                        if not position:
                            # Closed by exchange
                            hit = trade.get('hit', 'Closed')
                            pnl_pct = float(position['unRealizedProfit']) / CAPITAL * 100 if position else 0
                            profit = CAPITAL * pnl_pct / 100
                            closed_trade = {
                                'symbol': sym,
                                'pnl': profit,
                                'pnl_pct': pnl_pct,
                                'category': trade['category'],
                                'ema_status': trade['ema_status'],
                                'pressure_status': trade['pressure_status'],
                                'hit': hit,
                                'body_pct': trade['body_pct']
                            }
                            closed_trades.append(closed_trade)
                            save_closed_trades(closed_trade)
                            new_msg = trade['msg'] + f"\nProfit/Loss: {pnl_pct:.2f}% (${profit:.2f})\n{hit}"
                            edit_telegram_message(trade['msg_id'], new_msg)
                            del open_trades[sym]
                            save_trades()
                            send_telegram(f"Step: Trade closed for {sym} - {hit}")
                            continue

                        # Sim check as before for precise hit detection
                        hit = ""
                        pnl = 0
                        hit_price = None
                        entry_time = trade.get('entry_time')
                        if entry_time:
                            candles_1m = exchange.fetch_ohlcv(sym, '1m', since=entry_time, limit=2880)
                            for c in candles_1m:
                                high = c[2]
                                low = c[3]
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
                            logging.info(f"TP/SL hit for {sym}: {hit}, Leveraged PnL: {leveraged_pnl_pct:.2f}%")
                            closed_trade = {
                                'symbol': sym,
                                'pnl': profit,
                                'pnl_pct': leveraged_pnl_pct,
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
                                f"Profit/Loss: {leveraged_pnl_pct:.2f}% (${profit:.2f})\n{hit}"
                            )
                            trade['msg'] = new_msg
                            trade['hit'] = hit
                            logging.info(f"Updating Telegram message for {sym}: {hit}")
                            edit_telegram_message(trade['msg_id'], new_msg)
                            del open_trades[sym]
                            save_trades()
                            logging.info(f"Trade closed for {sym}")
                            send_telegram(f"Step: Trade closed for {sym} - {hit}")
                    except Exception as e:
                        logging.error(f"TP/SL check error on {sym}: {e}")
                        send_telegram(f"Error in TP/SL check for {sym}: {e}")
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL loop error: {e}")
            send_telegram(f"Error in TP/SL loop: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    send_telegram(f"Step: Processing symbol {symbol}")
    try:
        for attempt in range(3):
            try:
                candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=30)
                if len(candles) < 25:
                    send_telegram(f"Step: Insufficient candles for {symbol} - skipping")
                    return
                if attempt < 2 and candles[-1][0] > candles[-2][0]:
                    break
                time.sleep(0.5)
            except ccxt.NetworkError as e:
                print(f"Network error on {symbol}: {e}")
                send_telegram(f"Error: Network error fetching candles for {symbol}: {e}")
                time.sleep(2 ** attempt)
                continue

        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        rsi = calculate_rsi(candles, period=RSI_PERIOD)
        if ema21 is None or ema9 is None or rsi is None:
            send_telegram(f"Step: EMA/RSI calculation failed for {symbol} - skipping")
            return

        signal_time = candles[-2][0]
        first_small_candle_close = round_price(symbol, candles[-3][4])
        second_small_candle_close = round_price(symbol, candles[-2][4])
        big_candle_close = round_price(symbol, candles[-4][4])

        if detect_rising_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'rising')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                send_telegram(f"Step: Body size threshold exceeded for {symbol} rising - skipping")
                return
            if sent_signals.get((symbol, 'rising')) == signal_time:
                send_telegram(f"Step: Signal already sent for {symbol} rising - skipping")
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
            entry_price = second_small_candle_close
            tp = round_price(symbol, first_small_candle_close * (1 - TP_PCT))
            sl = round_price(symbol, entry_price * (1 + SL_PCT))
            tp_distance = (entry_price - tp) / entry_price * 100
            pattern = 'rising'
            msg = (
                f"{symbol} - {'REVERSED SELL' if side == 'sell' else 'RISING'} PATTERN\n"
                f"Above 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 above 21 - {ema_status['ema9_ema21']}\n"
                f"RSI: {rsi:.2f}\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"TP Distance: {tp_distance:.2f}%\n"
                f"sl - {sl}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern))
            send_telegram(f"Step: Rising pattern detected for {symbol} - added to queue")

        elif detect_falling_three(candles):
            first_candle_analysis = analyze_first_small_candle(candles[-3], 'falling')
            if first_candle_analysis['body_pct'] > BODY_SIZE_THRESHOLD:
                send_telegram(f"Step: Body size threshold exceeded for {symbol} falling - skipping")
                return
            if sent_signals.get((symbol, 'falling')) == signal_time:
                send_telegram(f"Step: Signal already sent for {symbol} falling - skipping")
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
            entry_price = second_small_candle_close
            tp = round_price(symbol, first_small_candle_close * (1 + TP_PCT))
            sl = round_price(symbol, entry_price * (1 - SL_PCT))
            tp_distance = (tp - entry_price) / entry_price * 100
            pattern = 'falling'
            msg = (
                f"{symbol} - {'REVERSED BUY' if side == 'buy' else 'FALLING'} PATTERN\n"
                f"Below 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 below 21 - {ema_status['ema9_ema21']}\n"
                f"RSI: {rsi:.2f}\n"
                f"First small candle: {first_candle_analysis['text']}\n"
                f"entry - {entry_price}\n"
                f"tp - {tp}\n"
                f"TP Distance: {tp_distance:.2f}%\n"
                f"sl - {sl}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis['text'], first_candle_analysis['status'], first_candle_analysis['body_pct'], pattern))
            send_telegram(f"Step: Falling pattern detected for {symbol} - added to queue")

    except ccxt.RateLimitExceeded:
        time.sleep(5)
        send_telegram(f"Error: Rate limit exceeded for {symbol} - retrying after delay")
    except Exception as e:
        logging.error(f"Error on {symbol}: {e}")
        send_telegram(f"Error processing {symbol}: {e}")

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
    send_telegram(f"Step: Starting scan of {len(symbols)} symbols")
    alert_queue = queue.Queue()

    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    def send_alerts():
        while True:
            try:
                symbol, msg, ema_status, category, side, entry_price, tp, sl, first_candle_analysis, pressure_status, body_pct, pattern = alert_queue.get(timeout=1)
                send_telegram(f"Step: Processing alert for {symbol}")
                with trade_lock:
                    # Check current open positions count via API
                    positions = exchange.fapiPrivate_get_positionrisk()
                    open_count = sum(1 for p in positions if float(p['positionAmt']) != 0)
                    if open_count >= MAX_OPEN_TRADES:
                        send_telegram(f"Step: Max open trades reached ({open_count}/{MAX_OPEN_TRADES}) - checking for replacement")
                        lowest_priority = min(
                            (CATEGORY_PRIORITY[trade['category']] for trade in open_trades.values()),
                            default=0
                        )
                        if CATEGORY_PRIORITY[category] > lowest_priority:
                            for sym, trade in list(open_trades.items()):
                                if CATEGORY_PRIORITY[trade['category']] == lowest_priority:
                                    send_telegram(f"Step: Closing {sym} for higher-priority signal ({symbol})")
                                    # Close lowest priority trade
                                    close_side = 'sell' if trade['side'] == 'buy' else 'buy'
                                    qty = abs(float(exchange.fapiPrivate_get_positionrisk({'symbol': sym.replace('/', '')})[0]['positionAmt']))
                                    exchange.create_order(sym, 'market', close_side, qty, params={'reduceOnly': True, 'positionSide': 'BOTH'})
                                    edit_telegram_message(
                                        trade['msg_id'],
                                        f"{sym} - Trade closed for higher-priority signal."
                                    )
                                    del open_trades[sym]
                                    save_trades()
                                    send_telegram(f"Step: Closed {sym} successfully")
                                    break

                    # Open new trade
                    actual_entry, order_id = open_position(symbol, side, entry_price, tp, sl, pattern)
                    if actual_entry:
                        updated_msg = msg.replace(str(entry_price), str(actual_entry))  # Update with actual entry
                        mid = send_telegram(updated_msg)
                        trade = {
                            'side': side,
                            'entry': actual_entry,
                            'tp': tp,
                            'sl': sl,
                            'msg': updated_msg,
                            'msg_id': mid,
                            'ema_status': ema_status,
                            'category': category,
                            'first_candle_analysis': first_candle_analysis,
                            'pressure_status': pressure_status,
                            'body_pct': body_pct,
                            'entry_time': int(time.time() * 1000),
                            'pattern': pattern,
                            'order_id': order_id
                        }
                        open_trades[symbol] = trade
                        save_trades()
                        logging.info(f"New trade opened for {symbol}")
                        send_telegram(f"Step: New trade opened and saved for {symbol}")
                alert_queue.task_done()
            except queue.Empty:
                time.sleep(1)
                continue
            except Exception as e:
                logging.error(f"Alert thread error: {e}")
                send_telegram(f"Error in alert processing: {e}")
                time.sleep(1)

    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp_sl, daemon=True).start()

    while True:
        next_close = get_next_candle_close()
        wait_time = max(0, next_close - time.time())
        print(f"⏳ Waiting {wait_time:.1f} seconds for next 15m candle close...")
        send_telegram(f"Step: Waiting {wait_time:.1f}s for next candle close")
        time.sleep(wait_time)

        for i, chunk in enumerate(symbol_chunks):
            print(f"Processing batch {i+1}/{NUM_CHUNKS}...")
            send_telegram(f"Step: Processing batch {i+1}/{NUM_CHUNKS}")
            process_batch(chunk, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)

        print("✅ Scan complete.")
        num_open = len(open_trades)
        print(f"📊 Number of open trades: {num_open}")
        send_telegram(f"Step: Scan complete - Open trades: {num_open}")

        current_time = time.time()
        if current_time - last_summary_time >= SUMMARY_INTERVAL:
            send_telegram("Step: Generating hourly summary")
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
            send_telegram("Step: Hourly summary sent")

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
