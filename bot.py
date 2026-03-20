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
TIMEFRAMES = ['5m', '30m']
CANDLE_LIMIT = 6
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0
BATCH_DELAY = 2.0
NUM_CHUNKS = 8

CAPITAL_INITIAL = 15.0
CAPITAL_DCA_PER_STEP = 15.0
MAX_MARGIN_PER_TRADE = 45.0
LEVERAGE = 7

TP_INITIAL_PCT = 1.1 / 100
DCA_TRIGGER_PCTS = [1.5 / 100, 2.5 / 100]   # from INITIAL entry
TP_AFTER_DCA_PCTS = [0.65 / 100, 0.8 / 100]
SL_PCT = 6.0 / 100

MAX_DCA_LEVELS = 2
TP_CHECK_INTERVAL = 2
MAX_OPEN_TRADES = 5

TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')
if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

PROXY_LIST = []

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

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
        else:
            open_trades = {}
            logging.info("No open_trades.json found → starting with 0 open trades")
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
        logging.info(f"Closed trade saved. Total closed: {len(closed_list)}")
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
                if r.get('ok'):
                    return r['result']['message_id']
                else:
                    logging.error(f"Telegram API error: {r}")
                    return None
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

    # fallback no proxy
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

# === CANDLE & PATTERN HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0
def lower_wick_pct(c):
    o, h, l, cc = c[1], c[2], c[3], c[4]
    body = abs(cc - o)
    if body == 0: return 0
    lower = min(o, cc) - l
    return (lower / body) * 100

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

def detect_rising_three(candles):
    if len(candles) < 6: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol
    small_red_1 = is_bearish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_red_0 = is_bearish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    if len(candles) < 6: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol
    small_green_1 = is_bullish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT
    small_green_0 = is_bullish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT
    return big_red and small_green_1 and small_green_0

# === SYMBOLS ===
def get_symbols(markets):
    syms = [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active', True)]
    logging.info(f"Found {len(syms)} USDT perpetual markets")
    return syms

async def prepare_symbol(symbol):
    try:
        await exchange.set_margin_mode('isolated', symbol)
        await exchange.set_leverage(LEVERAGE, symbol)
        logging.info(f"Prepared {symbol}: isolated + {LEVERAGE}x")
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

# === TIMING ===
def get_next_candle_close():
    now = get_ist_time()
    secs = now.minute * 60 + now.second
    secs_to = (5 * 60) - (secs % (5 * 60))
    if secs_to < 10:
        secs_to += 5 * 60
    return time.time() + secs_to

def get_avg_entry_and_total(trade):
    total_pos = sum(e['amount'] for e in trade['entries'])
    if total_pos == 0:
        return 0.0, 0.0
    weighted = sum(e['price'] * e['amount'] for e in trade['entries'])
    return weighted / total_pos, total_pos

# === PROCESS SYMBOL (simplified placeholder – add your full logic here) ===
async def process_symbol(symbol, timeframe):
    # Your pattern detection + entry logic goes here
    # This is just a placeholder so the file runs
    logging.info(f"Scanning {symbol} on {timeframe}")
    pass

async def process_batch(symbols_chunk, timeframe):
    tasks = [asyncio.create_task(process_symbol(s, timeframe)) for s in symbols_chunk]
    await asyncio.gather(*tasks, return_exceptions=True)

# === MONITOR (placeholder – add your full DCA/TP/SL logic) ===
async def monitor_tp_and_dca():
    logging.info("monitor_tp_and_dca task started")
    while True:
        await asyncio.sleep(TP_CHECK_INTERVAL)

# === SCAN LOOP (fixed formatting) ===
async def scan_loop(symbols):
    logging.info(f"scan_loop started | {len(symbols)} symbols to monitor")
    while True:
        wait_until = get_next_candle_close()
        sleep_sec = max(0, wait_until - time.time())
        
        minutes_to_wait = int(sleep_sec // 60)
        seconds_to_wait = int(sleep_sec % 60)
        logging.info(f"Waiting for next 5m candle close (~{minutes_to_wait} min {seconds_to_wait:02d} sec)")
        
        await asyncio.sleep(sleep_sec)

        for tf in TIMEFRAMES:
            logging.info(f"Starting scan for timeframe: {tf}")
            chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
            chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]
            for i, chunk in enumerate(chunks, 1):
                logging.info(f"Processing batch {i}/{len(chunks)} for {tf} ({len(chunk)} symbols)")
                await process_batch(chunk, tf)
                if i < len(chunks):
                    await asyncio.sleep(BATCH_DELAY)
            await asyncio.sleep(1.0)
        logging.info("Full multi-timeframe scan completed")

# === MAIN ===
async def main():
    global exchange
    exchange = None
    try:
        exchange = await initialize_exchange()
        markets = exchange.markets
        symbols = get_symbols(markets)
        load_trades()

        startup_msg = (
            f"Bot restarted @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\n"
            f"Open positions: {len(open_trades)}\n"
            f"Max trades: {MAX_OPEN_TRADES} | Lev: {LEVERAGE}x\n"
            f"Initial: ${CAPITAL_INITIAL} → DCA +${CAPITAL_DCA_PER_STEP} × max 2\n"
            f"TP: {TP_INITIAL_PCT*100:.1f}% → {TP_AFTER_DCA_PCTS[0]*100:.2f}% (DCA1) → {TP_AFTER_DCA_PCTS[1]*100:.2f}% (DCA2)\n"
            f"SL: {SL_PCT*100:.0f}% from initial entry\n"
            f"DCA triggers fixed from initial • Checks use MARK PRICE"
        )
        await send_telegram(startup_msg)

        tasks = [
            asyncio.create_task(scan_loop(symbols)),
            asyncio.create_task(monitor_tp_and_dca()),
        ]
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logging.info("Bot stopped by user (Ctrl+C)")
    except Exception as e:
        logging.error(f"Main loop crashed: {e}", exc_info=True)
    finally:
        if exchange is not None:
            await exchange.close()
            logging.info("Binance exchange closed cleanly")

if __name__ == "__main__":
    asyncio.run(main())
