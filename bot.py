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

# === NEW STRATEGY CONFIG ===
CAPITAL_INITIAL = 15.0
CAPITAL_DCA1 = 25.0
CAPITAL_DCA2 = 15.0
MAX_MARGIN_PER_TRADE = 55.0

LEVERAGE = 7

TP_INITIAL_PCT = 1.1 / 100
TP_AFTER_DCA1_PCT = 0.6 / 100
TP_AFTER_DCA2_PCT = 0.8 / 100

DCA1_TRIGGER_PCT = 1.5 / 100
DCA2_TRIGGER_PCT = 2.5 / 100

SL_PCT = 6.0 / 100

TP_CHECK_INTERVAL = 2
MAX_OPEN_TRADES = 5

TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')

if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = asyncio.Lock()

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# === TELEGRAM ===
async def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={
                'chat_id': CHAT_ID,
                'text': msg,
                'parse_mode': 'Markdown'
            }) as resp:
                r = await resp.json()
                return r.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram error: {e}")

# === EXCHANGE ===
async def initialize_exchange():
    ex = ccxt.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'options': {'defaultType': 'future', 'marginMode': 'isolated'},
        'enableRateLimit': True,
    })
    await ex.load_markets()
    return ex

exchange = None
open_trades = {}

# === HELPERS ===
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
        return float(exchange.amount_to_precision(symbol, amt))
    except:
        return amt

def get_avg_entry_and_total(trade):
    total = sum(e['amount'] for e in trade['entries'])
    weighted = sum(e['price'] * e['amount'] for e in trade['entries'])
    return weighted / total, total

# === PATTERN (UNCHANGED) ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]

def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0

def lower_wick_pct(c):
    o, h, l, cc = c[1], c[2], c[3], c[4]
    body = abs(cc - o)
    if body == 0:
        return 0
    lower = min(o, cc) - l
    return (lower / body) * 100

def detect_rising_three(candles):
    if len(candles) < 6: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    return is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol \
        and is_bearish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT \
        and is_bearish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT

def detect_falling_three(candles):
    if len(candles) < 6: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    return is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol \
        and is_bullish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT \
        and is_bullish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT

# === MONITOR (UPDATED CORE LOGIC) ===
async def monitor():
    while True:
        try:
            async with trade_lock:
                if not open_trades:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                tickers = await exchange.fetch_tickers(list(open_trades.keys()))

                for sym in list(open_trades.keys()):
                    tr = open_trades[sym]
                    current = tickers[sym]['last']
                    is_long = tr['side'] == 'buy'

                    # === SL ===
                    sl_price = tr['initial_entry'] * (1 - SL_PCT) if is_long else tr['initial_entry'] * (1 + SL_PCT)
                    if (is_long and current <= sl_price) or (not is_long and current >= sl_price):
                        await exchange.create_order(sym, 'market', 'sell' if is_long else 'buy', tr['total_amount'], params={'reduceOnly': True})
                        await send_telegram(f"❌ SL HIT {sym}")
                        del open_trades[sym]
                        continue

                    # === DCA1 ===
                    if not tr['dca1_done']:
                        trigger = tr['initial_entry'] * (1 - DCA1_TRIGGER_PCT) if is_long else tr['initial_entry'] * (1 + DCA1_TRIGGER_PCT)
                        if (is_long and current <= trigger) or (not is_long and current >= trigger):
                            amt = round_amount(sym, (CAPITAL_DCA1 * LEVERAGE) / current)
                            order = await exchange.create_market_order(sym, tr['side'], amt)
                            price = order.get('average') or current
                            tr['entries'].append({'price': price, 'amount': amt})
                            tr['avg_entry'], tr['total_amount'] = get_avg_entry_and_total(tr)
                            tr['dca1_done'] = True
                            tr['tp'] = tr['avg_entry'] * (1 + TP_AFTER_DCA1_PCT if is_long else 1 - TP_AFTER_DCA1_PCT)
                            await send_telegram(f"📉 DCA1 {sym}")

                    # === DCA2 ===
                    if tr['dca1_done'] and not tr['dca2_done']:
                        trigger = tr['initial_entry'] * (1 - DCA2_TRIGGER_PCT) if is_long else tr['initial_entry'] * (1 + DCA2_TRIGGER_PCT)
                        if (is_long and current <= trigger) or (not is_long and current >= trigger):
                            amt = round_amount(sym, (CAPITAL_DCA2 * LEVERAGE) / current)
                            order = await exchange.create_market_order(sym, tr['side'], amt)
                            price = order.get('average') or current
                            tr['entries'].append({'price': price, 'amount': amt})
                            tr['avg_entry'], tr['total_amount'] = get_avg_entry_and_total(tr)
                            tr['dca2_done'] = True
                            tr['tp'] = tr['avg_entry'] * (1 + TP_AFTER_DCA2_PCT if is_long else 1 - TP_AFTER_DCA2_PCT)
                            await send_telegram(f"📉 DCA2 {sym}")

                    # === TP ===
                    if (is_long and current >= tr['tp']) or (not is_long and current <= tr['tp']):
                        await exchange.create_order(sym, 'market', 'sell' if is_long else 'buy', tr['total_amount'], params={'reduceOnly': True})
                        await send_telegram(f"✅ TP HIT {sym}")
                        del open_trades[sym]

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:
            logging.error(e)
            await asyncio.sleep(5)

# === MAIN ===
async def main():
    global exchange
    exchange = await initialize_exchange()

    await send_telegram("🚀 Bot Started (DCA1 + DCA2 + SL Active)")

    await monitor()

if __name__ == "__main__":
    asyncio.run(main())
