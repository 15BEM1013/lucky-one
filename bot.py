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
CANDLE_LIMIT = 12
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0
BATCH_DELAY = 2.0
NUM_CHUNKS = 8

# TRADE SETTINGS
CAPITAL_INITIAL = 10.0
LEVERAGE = 9

CAPITAL_DCA1_NORMAL = 20.0
CAPITAL_DCA1_REVERSAL = 10.0
CAPITAL_DCA2 = 5.0

SL_PCT = 8.0 / 100

TP_INITIAL_NORMAL_PCT = 1.0 / 100
TP_INITIAL_REVERSAL_PCT = 0.5 / 100

TP_AFTER_DCA1_PCT = 0.6 / 100
TP_AFTER_DCA2_PCT = 0.4 / 100

DCA2_TRIGGER_PCT = 1.5 / 100

TP_CHECK_INTERVAL = 0.5
MAX_OPEN_TRADES = 5

TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')

if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

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
        logging.info(f"Closed trade saved | PnL: ${closed.get('pnl_usdt', 0):.2f}")
    except Exception as e:
        logging.error(f"Save closed trade error: {e}")

# === TELEGRAM ===
async def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                r = await resp.json()
                return r.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

async def edit_telegram_message(mid, new_text):
    if not mid: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, data={'chat_id': CHAT_ID, 'message_id': mid, 'text': new_text, 'parse_mode': 'Markdown'}, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

# === EXCHANGE ===
async def initialize_exchange():
    ex = ccxt.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'options': {'defaultType': 'future', 'marginMode': 'isolated'},
        'enableRateLimit': True,
    })
    await ex.load_markets()
    logging.info("Connected to Binance Futures")
    return ex

exchange = None
sent_signals = {}
open_trades = {}

# === HELPERS ===
def format_duration(seconds):
    if seconds < 60: return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60: return f"{minutes}m {secs}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"

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

# === NEW: PRECISION HELPERS (Fixed round_price) ===
def round_price(symbol, price):
    try:
        return float(exchange.price_to_precision(symbol, price))
    except Exception as e:
        logging.warning(f"Price rounding failed for {symbol}: {e}")
        # Safe fallback
        try:
            market = exchange.market(symbol)
            precision = market['precision']['price']
            return round(float(price), precision if isinstance(precision, int) else 8)
        except:
            return round(float(price), 8)

def round_amount(symbol, amt):
    try:
        return float(exchange.amount_to_precision(symbol, amt))
    except Exception as e:
        logging.warning(f"Amount rounding failed for {symbol}: {e}")
        return float(amt)

def get_wick_signal(candle):
    # Ignore tiny body candles
    if body_pct(candle) < 0.5:
        return None, None, False

    upper = upper_wick_pct(candle)
    lower = lower_wick_pct(candle)

    is_green = is_bullish(candle)

    # =========================
    # GREEN CANDLE LOGIC
    # =========================
    if is_green:

        # Strong rejection -> SELL reversal
        if upper > 50 or (upper > 30 and lower > 30):
            return (
                'sell',
                f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **SELL**",
                True
            )

        # Normal bullish continuation
        return (
            'buy',
            f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **BUY**",
            False
        )

    # =========================
    # RED CANDLE LOGIC
    # =========================
    else:

        # Block strong rejection red candles completely
        if lower > 30 or (upper > 30 and lower > 30):
            return None, None, False

        # Normal bearish continuation
        return (
            'sell',
            f"Red Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **SELL**",
            False
        )

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    if len(candles) < 9: return False, None
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    prev_volumes = [candles[i][5] for i in [-5, -6, -7, -8]]
    big_vol = c2[5]
    vol_condition = all(big_vol > v for v in prev_volumes)

    big_green = (is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and vol_condition)
    small_red_1 = is_bearish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_red_0 = is_bearish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_green and small_red_1 and small_red_0, c2

def detect_falling_three(candles):
    if len(candles) < 9: return False, None
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    prev_volumes = [candles[i][5] for i in [-5, -6, -7, -8]]
    big_vol = c2[5]
    vol_condition = all(big_vol > v for v in prev_volumes)

    big_red = (is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and vol_condition)
    small_green_1 = is_bullish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_green_0 = is_bullish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_red and small_green_1 and small_green_0, c2

def get_symbols(markets):
    return [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active', True)]

async def prepare_symbol(symbol):
    try:
        await exchange.set_margin_mode('isolated', symbol)
        await exchange.set_leverage(LEVERAGE, symbol)
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

def get_avg_entry_and_total(tr):
    total_pos = sum(e['amount'] for e in tr['entries'])
    weighted = sum(e['price'] * e['amount'] for e in tr['entries'])
    return (weighted / total_pos) if total_pos > 0 else 0.0, total_pos

# === BUILD TRADE MESSAGE ===
def build_trade_message(tr, sym, current=None, is_final=False, hit_type=None, exit_price=None, pnl_usdt=None, pnl_pct=None):
    is_long = tr['side'] == 'buy'
    duration = format_duration(time.time() - tr['open_ts'])
    avg = tr['avg_entry']

    lines = [
        f"**{'LONG' if is_long else 'SHORT'}** {sym} ({tr.get('timeframe', 'N/A')})",
        f"Entry: {tr['initial_price']:.6f} | Avg: {avg:.6f}",
        f"Duration: {duration}"
    ]

    entries_str = [f"{'Initial' if e['stage']==0 else 'DCA'+str(e['stage'])}: {e['price']:.6f} (${e['margin']})" for e in tr['entries']]
    lines.append("Entries: " + " | ".join(entries_str))

    sl_price = round_price(sym, avg * (1 - SL_PCT) if is_long else avg * (1 + SL_PCT))
    lines.append(f"TP: {tr['tp']:.6f} | SL: {sl_price:.6f} (8%)")

    if tr.get('is_reversal'):
        dca1 = tr.get('dca1_level')
        dca2 = tr.get('dca2_level')
        dir_symbol = "+" if not is_long else "-"
        lines.append(f"DCA1 Level: {dca1:.6f} ({dir_symbol}1.0%)")
        lines.append(f"DCA2 Level: {dca2:.6f} ({dir_symbol}1.5% from DCA1)")
    else:
        dca1 = tr.get('dca1_level')
        dca2 = tr.get('dca2_level')
        lines.append(f"DCA1 Level: {dca1:.6f} (Big Candle Open)")
        lines.append(f"DCA2 Level: {dca2:.6f} (1.5% from DCA1)")

    if tr.get('signal_reason'):
        lines.append(f"Signal: {tr['signal_reason']}")

    pattern_type = "Strong Rejection" if tr.get('is_reversal') else "Continuation"
    lines.append(f"{tr.get('pattern', 'Pattern')} - {pattern_type}")

    if is_final and hit_type:
        lines.append(f"**{hit_type} HIT** | Exit: {exit_price:.6f}")
        lines.append(f"PnL: {pnl_pct:.2f}% (${pnl_usdt:+.2f})")

    return "\n".join(lines)

# Rest of your code remains exactly the same...
# (monitor_tp_and_dca, check_and_execute_dca, close_trade, process_symbol, etc.)

# ... [All remaining functions are unchanged] ...

async def main():
    global exchange
    exchange = await initialize_exchange()
    markets = exchange.markets
    symbols = get_symbols(markets)
    load_trades()

    logging.info(f"Starting bot with {len(symbols)} symbols")

    startup_msg = f"🚀 **Bot Restarted** @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\nPatterns + Wick Filter | SL: 8% | Insufficient Warning Active"
    await send_telegram(startup_msg)

    tasks = [
        asyncio.create_task(scan_loop(symbols)),
        asyncio.create_task(monitor_tp_and_dca()),
        asyncio.create_task(daily_summary()),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())