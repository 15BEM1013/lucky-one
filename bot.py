import ccxt
import time
import threading
import requests
from flask import Flask
from concurrent.futures import ThreadPoolExecutor
import math
import queue
import os
import logging
from datetime import datetime
import pytz

app = Flask(__name__)

# === CONFIG (ALL SECRETS FROM ENVIRONMENT VARIABLES) ===
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

if not all([BOT_TOKEN, CHAT_ID, API_KEY, API_SECRET]):
    raise ValueError("Missing required environment variables! Check Render settings.")

TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 8.0          # USDT base capital per trade
LEVERAGE = 5
SL_PCT = 0.03         # 3%
TP_PCT = 0.01         # 1%
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
        requests.post(url, data=data, timeout=5)
        print(f"Telegram sent: {msg}")
    except Exception as e:
        print(f"Telegram error: {e}")

# === EXCHANGE ===
def initialize_exchange():
    exchange = ccxt.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'options': {'defaultType': 'future'},
        'enableRateLimit': True,
    })
    exchange.load_markets()
    try:
        # One-way mode (as in your code)
        exchange.fapiPrivate_post_positionside_dual({'dualSidePosition': 'false'})
        send_telegram("✅ Bot started - One-way mode set")
    except Exception as e:
        send_telegram(f"Position mode warning: {e}")
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

# === FIRST SMALL CANDLE ANALYSIS ===
def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    wick_ratio = upper_wick / lower_wick if lower_wick != 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick != 0 else float('inf')

    if pattern_type == 'rising':
        if wick_ratio >= 2.5 and body < 0.1:
            return {'text': f"Selling pressure ⚠️\nUpper: {upper_wick:.1f}%\nLower: {lower_wick:.1f}%\nBody: {body:.1f}%", 'status': 'selling_pressure'}
        elif wick_ratio_reverse >= 2.5:
            return {'text': f"Buying pressure ⚠️\nUpper: {upper_wick:.1f}%\nLower: {lower_wick:.1f}%\nBody: {body:.1f}%", 'status': 'buying_pressure'}
        else:
            return {'text': f"Neutral ✅\nUpper: {upper_wick:.1f}%\nLower: {lower_wick:.1f}%\nBody: {body:.1f}%", 'status': 'neutral'}
    else:  # falling
        if wick_ratio_reverse >= 2.5 and body < 0.1:
            return {'text': f"Buying pressure ⚠️\nUpper: {upper_wick:.1f}%\nLower: {lower_wick:.1f}%\nBody: {body:.1f}%", 'status': 'buying_pressure'}
        elif wick_ratio >= 2.5:
            return {'text': f"Selling pressure ⚠️\nUpper: {upper_wick:.1f}%\nLower: {lower_wick:.1f}%\nBody: {body:.1f}%", 'status': 'selling_pressure'}
        else:
            return {'text': f"Neutral ✅\nUpper: {upper_wick:.1f}%\nLower: {lower_wick:.1f}%\nBody: {body:.1f}%", 'status': 'neutral'}

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
    min_qty = float(market['limits']['amount']['min'] or 0)
    min_notional = float(market['limits']['cost']['min'] or 0)
    if qty < min_qty or (qty * entry_price) < min_notional:
        send_telegram(f"Skip {symbol}: Qty {qty:.6f} too small (min {min_qty})")
        return None
    return round_quantity(symbol, qty)

# === OPEN POSITION ===
def open_position(symbol, side, entry_price, tp_price, sl_price, analysis_text):
    send_telegram(f"🚀 Executing {side.upper()} {symbol}\nFirst candle analysis:\n{analysis_text}\nEntry ≈ {entry_price:.4f}")
    
    try:
        balance = exchange.fetch_balance()['USDT']['free']
        if balance < CAPITAL * 1.2:
            send_telegram(f"⚠️ Low balance: {balance:.2f} USDT")
            return
    except Exception as e:
        send_telegram(f"Balance error: {e}")
        return

    qty = calculate_quantity(symbol, entry_price)
    if not qty:
        return

    set_leverage(symbol)

    try:
        # Market entry
        order = exchange.create_order(symbol, 'market', side, qty)
        actual_entry = float(order.get('average') or order.get('price') or entry_price)
        send_telegram(f"✅ Opened {side.upper()} {symbol} at {actual_entry:.4f} (Qty: {qty})")

        # SL & TP
        close_side = 'sell' if side == 'buy' else 'buy'
        exchange.create_order(symbol, 'stop_market', close_side, qty,
                              params={'stopPrice': sl_price, 'reduceOnly': True})
        send_telegram(f"🛑 SL set at {sl_price:.4f}")
        exchange.create_order(symbol, 'take_profit_market', close_side, qty,
                              params={'stopPrice': tp_price, 'reduceOnly': True})
        send_telegram(f"🎯 TP set at {tp_price:.4f}")
    except Exception as e:
        send_telegram(f"❌ Order failed {symbol}: {str(e)}")

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and c1[5] < c2[5]
    small_red_0 = is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and c0[5] < c2[5]
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[5] < c2[5]
    small_green_0 = is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[5] < c2[5]
    return big_red and small_green_1 and small_green_0

sent_signals = {}

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=30)
        if len(candles) < 25: return

        ema21 = calculate_ema(candles, 21)
        ema9 = calculate_ema(candles, 9)
        if ema21 is None or ema9 is None: return

        signal_time = candles[-2][0]
        first_close = candles[-3][4]
        second_close = candles[-2][4]

        if detect_rising_three(candles):
            if body_pct(candles[-3]) > BODY_SIZE_THRESHOLD: return
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if sent_signals.get((symbol, 'rising')) == signal_time: return
            sent_signals[(symbol, 'rising')] = signal_time

            if first_close > ema21 and ema9 > ema21:
                side = 'sell'
                entry = second_close
                tp = round_price(symbol, first_close * (1 - TP_PCT))
                sl = round_price(symbol, entry * (1 + SL_PCT))
                alert_queue.put((symbol, side, entry, tp, sl, analysis['text']))

        elif detect_falling_three(candles):
            if body_pct(candles[-3]) > BODY_SIZE_THRESHOLD: return
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if sent_signals.get((symbol, 'falling')) == signal_time: return
            sent_signals[(symbol, 'falling')] = signal_time

            if first_close < ema21 and ema9 < ema21:
                side = 'buy'
                entry = second_close
                tp = round_price(symbol, first_close * (1 + TP_PCT))
                sl = round_price(symbol, entry * (1 - SL_PCT))
                alert_queue.put((symbol, side, entry, tp, sl, analysis['text']))

    except Exception as e:
        send_telegram(f"Error processing {symbol}: {e}")

# === SCAN LOOP ===
def scan_loop():
    send_telegram("🤖 Auto-Trading Bot Started (LIVE)")
    symbols = [s for s in exchange.markets if 'USDT' in s and exchange.markets[s]['contract'] and exchange.markets[s]['active']]
    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    def get_next_close():
        now = get_ist_time()
        seconds_to_next = (15 * 60) - (now.minute * 60 + now.second) % (15 * 60)
        if seconds_to_next < 5: seconds_to_next += 15 * 60
        return time.time() + seconds_to_next

    alert_queue = queue.Queue()

    def process_alerts():
        while True:
            try:
                symbol, side, entry, tp, sl, analysis_text = alert_queue.get(timeout=1)
                with trade_lock:
                    positions = exchange.fapiPrivate_get_positionrisk()
                    open_count = sum(1 for p in positions if float(p['positionAmt']) != 0)
                    if open_count >= MAX_OPEN_TRADES:
                        send_telegram(f"⚠️ Max {MAX_OPEN_TRADES} trades open - skipping {symbol}")
                        continue
                    open_position(symbol, side, entry, tp, sl, analysis_text)
            except queue.Empty:
                time.sleep(0.5)
            except Exception as e:
                send_telegram(f"Alert processing error: {e}")

    threading.Thread(target=process_alerts, daemon=True).start()

    while True:
        wait_time = max(0, get_next_close() - time.time())
        send_telegram(f"⏳ Waiting {int(wait_time)}s for next candle...")
        time.sleep(wait_time)

        for i, chunk in enumerate(symbol_chunks):
            send_telegram(f"🔍 Scanning batch {i+1}/{NUM_CHUNKS} ({len(chunk)} symbols)")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for sym in chunk:
                    executor.submit(process_symbol, sym, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)

        send_telegram("✅ Scan cycle complete")

# === FLASK KEEP-ALIVE ===
@app.route('/')
def home():
    return "✅ Rising & Falling Three Auto-Trading Bot is Live! (Real Money)"

if __name__ == "__main__":
    threading.Thread(target=scan_loop, daemon=True).start()
    # Gunicorn will be used in Render (do not use app.run here)
