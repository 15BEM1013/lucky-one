import ccxt.async_support as ccxt
import asyncio
import aiohttp
import time
import json
import os
import logging
from dotenv import load_dotenv
from datetime import datetime
import pytz
import math

# Load .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# === CONFIG ===
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TIMEFRAMES = ['5m', '15m']                  
CANDLE_LIMIT = 12          # Increased for proper volume check
MIN_BIG_BODY_PCT = 1.0          
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0
BATCH_DELAY = 2.0
NUM_CHUNKS = 8

CAPITAL_INITIAL = 15.0      
CAPITAL_DCA1 = 25.0
CAPITAL_DCA2 = 15.0

MAX_MARGIN_PER_TRADE = 55.0                 
LEVERAGE = 7

TP_INITIAL_PCT = 1.1 / 100
TP_AFTER_DCA1_PCT = 0.6 / 100
TP_AFTER_DCA2_PCT = 0.8 / 100

DCA1_TRIGGER_PCT = 1.0 / 100      
DCA2_TRIGGER_PCT = 2.5 / 100

SL_PCT = 6.0 / 100                

TP_CHECK_INTERVAL = 0.5           

MAX_OPEN_TRADES = 5
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')

if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

PROXY_LIST = []  

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = asyncio.Lock()

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

# === TELEGRAM ===
async def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={
                'chat_id': CHAT_ID,
                'text': msg,
                'parse_mode': 'Markdown'
            }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                r = await resp.json()
                return r.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

async def edit_telegram_message(mid, new_text):
    if not mid:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={
                'chat_id': CHAT_ID,
                'message_id': mid,
                'text': new_text,
                'parse_mode': 'Markdown'
            }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.release()
    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

# === EXCHANGE ===
async def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxy_url = f"http://{proxy.get('username')}:{proxy.get('password')}@{proxy['host']}:{proxy['port']}"
            proxies = {'http': proxy_url, 'https': proxy_url}
            ex = ccxt.binance({
                'apiKey': API_KEY,
                'secret': API_SECRET,
                'options': {'defaultType': 'future', 'marginMode': 'isolated'},
                'proxies': proxies,
                'enableRateLimit': True,
            })
            await ex.load_markets()
            logging.info("Connected via proxy")
            return ex
        except Exception as e:
            logging.warning(f"Proxy failed: {e}")

    ex = ccxt.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'options': {'defaultType': 'future', 'marginMode': 'isolated'},
        'enableRateLimit': True,
    })
    await ex.load_markets()
    logging.info("Connected directly")
    return ex

exchange = None
sent_signals = {}  
open_trades = {}   

# === HELPERS ===
def format_duration(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0

def lower_wick_pct(c):
    o, h, l, cc = c[1], c[2], c[3], c[4]
    body = abs(cc - o)
    if body == 0: return 0
    lower = min(o, cc) - l
    return (lower / body) * 100

def upper_wick_pct(c):
    o, h, l, cc = c[1], c[2], c[3], c[4]
    body = abs(cc - o)
    if body == 0: return 0
    upper = h - max(o, cc)
    return (upper / body) * 100

def get_wick_signal(candle):
    if body_pct(candle) < 0.5:
        return None, None

    upper = upper_wick_pct(candle)
    lower = lower_wick_pct(candle)
    is_green = is_bullish(candle)

    if is_green:
        if upper > 50 or (upper > 30 and lower > 30):
            reason = f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **SELL** (Rejection)"
            return 'sell', reason
        else:
            reason = f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **BUY** (Strong Bullish)"
            return 'buy', reason
    else:  
        if lower > 30 or (upper > 30 and lower > 30):
            reason = f"Red Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **BUY** (Rejection)"
            return 'buy', reason
        else:
            reason = f"Red Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **SELL** (Bearish Continuation)"
            return 'sell', reason

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

# === PATTERN DETECTION - FIXED ===
def detect_rising_three(candles):
    if len(candles) < 9: 
        return False, None
    
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]   # Big + 2 small
    
    # Previous 4 candles before the big candle
    prev_volumes = [candles[i][5] for i in [-5, -6, -7, -8]]
    
    big_vol = c2[5]
    vol_condition = all(big_vol > v for v in prev_volumes)

    big_green = (is_bullish(c2) and 
                 body_pct(c2) >= MIN_BIG_BODY_PCT and 
                 vol_condition)
    
    small_red_1 = is_bearish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_red_0 = is_bearish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_green and small_red_1 and small_red_0, c2


def detect_falling_three(candles):
    if len(candles) < 9: 
        return False, None
    
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    
    # Previous 4 candles before the big candle
    prev_volumes = [candles[i][5] for i in [-5, -6, -7, -8]]
    
    big_vol = c2[5]
    vol_condition = all(big_vol > v for v in prev_volumes)

    big_red = (is_bearish(c2) and 
               body_pct(c2) >= MIN_BIG_BODY_PCT and 
               vol_condition)
    
    small_green_1 = is_bullish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_green_0 = is_bullish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_red and small_green_1 and small_green_0, c2


# === PROCESS SYMBOL (with insufficient balance message) ===
async def process_symbol(symbol, timeframe):
    try:
        candles = await exchange.fetch_ohlcv(symbol, timeframe, limit=CANDLE_LIMIT)
        if len(candles) < 9:
            return

        signal_time = candles[-2][0]   

        key = (symbol, timeframe, 'pattern')

        async with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES:
                return
            if sent_signals.get(key) == signal_time:
                return

        is_rising, big_candle = detect_rising_three(candles)
        is_falling, big_candle_f = detect_falling_three(candles)

        pattern = None
        side = None
        signal_msg = None

        if is_rising:
            pattern = 'rising_three'
            side, signal_msg = get_wick_signal(big_candle)
        elif is_falling:
            pattern = 'falling_three'
            side, signal_msg = get_wick_signal(big_candle_f)

        if not side or not signal_msg:
            return

        sent_signals[key] = signal_time

        await prepare_symbol(symbol)
        ticker = await exchange.fetch_ticker(symbol)
        entry_price = round_price(symbol, ticker['last'])

        amount_raw = (CAPITAL_INITIAL * LEVERAGE) / entry_price
        amount = float(round_amount(symbol, amount_raw))
        if amount <= 0:
            return

        entry_order = await exchange.create_market_order(symbol, side, amount)
        filled_price = round_price(symbol, entry_order.get('average') or entry_price)

        tp = round_price(symbol, filled_price * (1 + TP_INITIAL_PCT) if side == 'buy' else filled_price * (1 - TP_INITIAL_PCT))

        initial_trade = {
            'side': side,
            'initial_price': filled_price,
            'entries': [{'price': filled_price, 'amount': amount, 'margin': CAPITAL_INITIAL, 'ts': time.time(), 'stage': 0}],
            'total_amount': amount,
            'avg_entry': filled_price,
            'tp': tp,
            'dca_stage': 0,
            'msg_id_initial': None,
            'open_ts': time.time(),
            'timeframe': timeframe,
            'signal_reason': signal_msg,
            'pattern': pattern
        }

        msg_text = build_trade_message(initial_trade, symbol) + f"\n\n{signal_msg}"
        mid = await send_telegram(msg_text)
        initial_trade['msg_id_initial'] = mid

        async with trade_lock:
            open_trades[symbol] = initial_trade
            await asyncio.to_thread(save_trades)

        logging.info(f"Opened {side.upper()} {symbol} on {pattern} | {timeframe}")

    except ccxt.InsufficientFunds as e:
        skipped_msg = f"⚠️ **Trade Not Taken** - Insufficient Balance\n\n" \
                      f"**{side.upper()}** {symbol} ({timeframe})\n" \
                      f"{signal_msg}\n" \
                      f"Required Margin: \~${CAPITAL_INITIAL}\n" \
                      f"Time: {get_ist_time().strftime('%H:%M:%S')}"
        await send_telegram(skipped_msg)
        logging.warning(f"Insufficient balance for {symbol} on {timeframe}")

    except Exception as e:
        logging.error(f"Trade failed {symbol} on {timeframe}: {e}")

# === Rest of the code remains same (monitor, scan_loop, main, etc.) ===
# ... [Copy the rest from your previous full code] ...

# (For brevity I didn't repeat the long monitor function, build_trade_message, scan_loop, etc. 
# They are unchanged from the last full code I gave you.)

if __name__ == "__main__":
    asyncio.run(main())