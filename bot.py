import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
import json
import os
import logging
from dotenv import load_dotenv

# Load .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# === CONFIG ===
BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID          = os.getenv("TELEGRAM_CHAT_ID")
TIMEFRAME        = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0

CAPITAL_INITIAL   = 10.0
CAPITAL_DCA       = 20.0
MAX_MARGIN_PER_TRADE = 30.0

LEVERAGE         = 5
TP_INITIAL_PCT   = 1.0 / 100
TP_AFTER_DCA_PCT = 0.5 / 100
DCA_TRIGGER_PCT  = 2.0 / 100

TP_CHECK_INTERVAL = 8

MAX_OPEN_TRADES  = 5
TRADE_FILE       = 'open_trades.json'
CLOSED_TRADE_FILE= 'closed_trades.json'
PATTERN_STATE_FILE = 'pattern_states.json'

API_KEY    = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')
if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

PROXY_LIST = []

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

trade_lock = threading.Lock()

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
        logging.info(f"Trades saved ({len(open_trades)} open)")
    except Exception as e:
        logging.error(f"Save trades error: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
            logging.info(f"Loaded {len(open_trades)} open trades")
    except Exception as e:
        logging.error(f"Load trades error: {e}")
        open_trades = {}

def save_closed_trade(closed):
    try:
        closed_list = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                closed_list = json.load(f)
        closed_list.append(closed)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(closed_list, f, default=str)
    except Exception as e:
        logging.error(f"Save closed trade error: {e}")

# === PATTERN STATE ===
pattern_states = {}  # symbol → {'stage': 0/1/2, 'big_ts': ms, 'big_data': [o,h,l,c,v], 'sent': bool}

def save_pattern_states():
    try:
        with open(PATTERN_STATE_FILE, 'w') as f:
            json.dump(pattern_states, f, default=str)
    except Exception as e:
        logging.error(f"Save pattern states failed: {e}")

def load_pattern_states():
    global pattern_states
    try:
        if os.path.exists(PATTERN_STATE_FILE):
            with open(PATTERN_STATE_FILE, 'r') as f:
                pattern_states = json.load(f)
            logging.info(f"Loaded {len(pattern_states)} partial patterns")
    except Exception as e:
        logging.error(f"Load pattern states failed: {e}")
        pattern_states = {}

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            'chat_id': CHAT_ID,
            'text': msg,
            'parse_mode': 'Markdown'
        }, timeout=10).json()
        return r.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

def edit_telegram_message(mid, new_text):
    if not mid:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    try:
        requests.post(url, data={
            'chat_id': CHAT_ID,
            'message_id': mid,
            'text': new_text,
            'parse_mode': 'Markdown'
        }, timeout=10)
    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

# === FLASK ===
app = Flask(__name__)

# === EXCHANGE ===
exchange = ccxt.binance({
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'options': {'defaultType': 'future', 'marginMode': 'isolated'},
    'enableRateLimit': True,
})
exchange.load_markets()
logging.info("Exchange connected")

sent_signals = {}
open_trades = {}

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0

def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

def round_price(symbol, price):
    try:
        m = exchange.market(symbol)
        tick = float(m['info']['filters'][0]['tickSize'])
        prec = int(round(-math.log10(tick)))
        return round(price, prec)
    except:
        return price

def round_amount(symbol, amt):
    try:
        return exchange.amount_to_precision(symbol, amt)
    except:
        return amt

def prepare_symbol(symbol):
    try:
        exchange.set_margin_mode('isolated', symbol)
        exchange.set_leverage(LEVERAGE, symbol)
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

# === STATEFUL CONTINUOUS PATTERN DETECTION ===
def pattern_monitor_loop():
    load_pattern_states()
    logging.info("Continuous pattern monitor started - using full 15m window")

    while True:
        try:
            now_ms = int(time.time() * 1000)

            # Process in small batches to keep CPU low
            symbols = get_symbols()
            batch_size = 30  # small batch = low CPU
            for start in range(0, len(symbols), batch_size):
                batch = symbols[start:start+batch_size]

                for symbol in batch:
                    state = pattern_states.get(symbol, {'stage': 0})

                    # Fetch last 6 closed candles
                    try:
                        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=6)
                        if len(candles) < 6:
                            continue
                    except Exception as e:
                        continue

                    latest_closed = candles[-1]

                    # Skip if no new candle since last check
                    if latest_closed[0] <= state.get('last_checked', 0):
                        continue

                    state['last_checked'] = latest_closed[0]

                    # Pattern progress
                    if state['stage'] == 0:
                        # Look for big candle in recent past (candles[-3] or [-4])
                        if is_big_candle(candles[-3]):
                            state['stage'] = 1
                            state['big_ts'] = candles[-3][0]
                            state['big_data'] = candles[-3]
                            logging.info(f"{symbol} → Stage 1: big candle")
                            pattern_states[symbol] = state
                            save_pattern_states()

                    elif state['stage'] == 1:
                        if is_small_reversal(latest_closed, state['big_data']):
                            state['stage'] = 2
                            state['first_small_ts'] = latest_closed[0]
                            logging.info(f"{symbol} → Stage 2: first small candle")
                            pattern_states[symbol] = state
                            save_pattern_states()
                        else:
                            pattern_states.pop(symbol, None)
                            save_pattern_states()

                    elif state['stage'] == 2:
                        if is_small_reversal(latest_closed, state['big_data']):
                            if not state.get('sent', False):
                                side = 'buy' if is_bullish(state['big_data']) else 'sell'
                                logging.info(f"{symbol} → PATTERN COMPLETE → ENTRY {side}")

                                # Trigger trade
                                try:
                                    prepare_symbol(symbol)
                                    ticker = exchange.fetch_ticker(symbol)
                                    entry_price = round_price(symbol, ticker['last'])

                                    amount_raw = (CAPITAL_INITIAL * LEVERAGE) / entry_price
                                    amount = float(round_amount(symbol, amount_raw))

                                    if amount <= 0:
                                        continue

                                    entry_order = exchange.create_market_order(symbol, side, amount)
                                    filled_price = entry_order.get('average') or entry_price
                                    filled_price = round_price(symbol, filled_price)

                                    tp = round_price(symbol, filled_price * (1 + TP_INITIAL_PCT) if side == 'buy' else filled_price * (1 - TP_INITIAL_PCT))

                                    entry_msg = (
                                        f"**ENTRY** {symbol} — {'LONG' if side=='buy' else 'SHORT'}\n"
                                        f"Entry: {filled_price:.6f}\n"
                                        f"Pattern: Full Three (tracked live)\n"
                                        f"Size: {amount:.4f} (${CAPITAL_INITIAL:.0f})\n"
                                        f"TP: {tp:.6f} ({TP_INITIAL_PCT*100:.1f}%)\n"
                                        f"No SL • Monitoring DCA/TP"
                                    )
                                    mid = send_telegram(entry_msg)

                                    with trade_lock:
                                        open_trades[symbol] = {
                                            'side': side,
                                            'entries': [{
                                                'price': filled_price,
                                                'amount': amount,
                                                'margin': CAPITAL_INITIAL,
                                                'ts': time.time()
                                            }],
                                            'total_amount': amount,
                                            'avg_entry': filled_price,
                                            'tp': tp,
                                            'dca_done': False,
                                            'msg_id_initial': mid,
                                            'msg_id_dca': None,
                                            'open_ts': time.time()
                                        }
                                        save_trades()

                                    logging.info(f"Opened {side} {symbol} @ {filled_price}")
                                    state['sent'] = True
                                    pattern_states[symbol] = state
                                    save_pattern_states()

                                except Exception as e:
                                    logging.error(f"Entry failed {symbol}: {e}")

                        else:
                            pattern_states.pop(symbol, None)
                            save_pattern_states()

                time.sleep(2.0)  # gentle pause between batches

            # Overall loop pace — check every ~8 seconds
            time.sleep(8)

        except Exception as e:
            logging.error(f"Monitor error: {e}")
            time.sleep(30)

# Helpers
def is_big_candle(c):
    return body_pct(c) >= MIN_BIG_BODY_PCT

def is_small_reversal(c, big):
    return (body_pct(c) < MAX_SMALL_BODY_PCT and
            lower_wick_pct(c) >= MIN_LOWER_WICK_PCT and
            ((is_bullish(big) and is_bearish(c)) or (is_bearish(big) and is_bullish(c))))

# === FLASK ===
@app.route('/')
def home():
    return f"Bot running — {len(open_trades)} open trades | Partial patterns: {len(pattern_states)}"

def run_bot():
    load_trades()
    load_pattern_states()

    startup = (
        f"Bot restarted @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\n"
        f"Open positions: {len(open_trades)}\n"
        f"Partial patterns tracked: {len(pattern_states)}\n"
        f"Continuous monitoring (no 15m wait) enabled"
    )
    send_telegram(startup)

    # Start continuous pattern watcher
    threading.Thread(target=pattern_monitor_loop, daemon=True).start()

    app.run(host='0.0.0.0', port=8080, debug=False)

if __name__ == "__main__":
    run_bot()
