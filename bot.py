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

# Load ENV
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET")

TIMEFRAMES = ['5m','30m']

MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0

BATCH_DELAY = 2.0
NUM_CHUNKS = 8

CAPITAL_INITIAL = 15.0
CAPITAL_DCA = 30.0
MAX_MARGIN_PER_TRADE = 45.0

LEVERAGE = 7

TP_INITIAL_PCT = 1.1/100
TP_AFTER_DCA_PCT = 0.6/100
DCA_TRIGGER_PCT = 2.0/100

TP_CHECK_INTERVAL = 2

MAX_OPEN_TRADES = 5

TRADE_FILE = "open_trades.json"
CLOSED_TRADE_FILE = "closed_trades.json"

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")

trade_lock = asyncio.Lock()

exchange = None
open_trades = {}
sent_signals = {}

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# TELEGRAM
async def send_telegram(msg):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:

        async with aiohttp.ClientSession() as session:
            async with session.post(url,data={
                "chat_id":CHAT_ID,
                "text":msg,
                "parse_mode":"Markdown"
            }) as resp:

                r = await resp.json()
                return r.get("result",{}).get("message_id")

    except Exception as e:
        logging.error(f"Telegram send error {e}")

async def edit_telegram_message(mid,text):

    if not mid:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"

    try:

        async with aiohttp.ClientSession() as session:
            async with session.post(url,data={
                "chat_id":CHAT_ID,
                "message_id":mid,
                "text":text,
                "parse_mode":"Markdown"
            }) as resp:

                await resp.release()

    except Exception as e:
        logging.error(f"Telegram edit error {e}")

# EXCHANGE
async def initialize_exchange():

    ex = ccxt.binance({
        "apiKey":API_KEY,
        "secret":API_SECRET,
        "enableRateLimit":True,
        "options":{
            "defaultType":"future",
            "marginMode":"isolated"
        }
    })

    await ex.load_markets()

    logging.info("Connected to Binance")

    return ex

def get_symbols(markets):

    return [
        s for s in markets
        if "USDT" in s and markets[s].get("swap") and markets[s].get("active",True)
    ]

# HELPERS
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]

def body_pct(c):
    return abs(c[4]-c[1])/c[1]*100 if c[1] else 0

def lower_wick_pct(c):

    o,h,l,c = c[1],c[2],c[3],c[4]

    body = abs(c-o)

    if body==0:
        return 0

    lower = min(o,c)-l

    return (lower/body)*100

# PATTERN
def detect_rising_three(candles):

    c2,c1,c0 = candles[-4],candles[-3],candles[-2]

    avg_vol = sum(c[5] for c in candles[-6:-1])/5

    big_green = is_bullish(c2) and body_pct(c2)>=MIN_BIG_BODY_PCT and c2[5]>avg_vol

    small_red1 = is_bearish(c1) and body_pct(c1)<MAX_SMALL_BODY_PCT and lower_wick_pct(c1)>=MIN_LOWER_WICK_PCT
    small_red0 = is_bearish(c0) and body_pct(c0)<MAX_SMALL_BODY_PCT and lower_wick_pct(c0)>=MIN_LOWER_WICK_PCT

    return big_green and small_red1 and small_red0

def detect_falling_three(candles):

    c2,c1,c0 = candles[-4],candles[-3],candles[-2]

    avg_vol = sum(c[5] for c in candles[-6:-1])/5

    big_red = is_bearish(c2) and body_pct(c2)>=MIN_BIG_BODY_PCT and c2[5]>avg_vol

    small_green1 = is_bullish(c1) and body_pct(c1)<MAX_SMALL_BODY_PCT and lower_wick_pct(c1)>=MIN_LOWER_WICK_PCT
    small_green0 = is_bullish(c0) and body_pct(c0)<MAX_SMALL_BODY_PCT and lower_wick_pct(c0)>=MIN_LOWER_WICK_PCT

    return big_red and small_green1 and small_green0

# TRADE HELPERS
def get_avg_entry_and_total(trade):

    total = 0
    weighted = 0

    for e in trade["entries"]:
        weighted += e["price"] * e["amount"]
        total += e["amount"]

    if total==0:
        return 0,0

    return weighted/total,total

# MONITOR
async def monitor_tp_and_dca():

    while True:

        try:

            async with trade_lock:

                if not open_trades:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                tickers = await exchange.fetch_tickers(list(open_trades.keys()))

                for sym in list(open_trades):

                    tr = open_trades[sym]
                    current = tickers[sym]["last"]

                    is_long = tr["side"] == "buy"

                    # DCA
                    if not tr["dca_done"]:

                        trigger = tr["avg_entry"]*(1-DCA_TRIGGER_PCT) if is_long else tr["avg_entry"]*(1+DCA_TRIGGER_PCT)

                        if (is_long and current<=trigger) or (not is_long and current>=trigger):

                            dca_amount = (CAPITAL_DCA*LEVERAGE)/current

                            order = await exchange.create_market_order(sym,tr["side"],dca_amount)

                            price = order.get("average") or current

                            tr["entries"].append({"price":price,"amount":dca_amount,"margin":CAPITAL_DCA})

                            tr["avg_entry"],_ = get_avg_entry_and_total(tr)

                            tr["dca_done"] = True

                            tr["tp"] = tr["avg_entry"]*(1+TP_AFTER_DCA_PCT) if is_long else tr["avg_entry"]*(1-TP_AFTER_DCA_PCT)

                            await send_telegram(f"⚡ DCA triggered {sym}\nNew avg: {tr['avg_entry']}")

                    # TP
                    hit_tp = (is_long and current>=tr["tp"]) or (not is_long and current<=tr["tp"])

                    if hit_tp:

                        close_side = "sell" if is_long else "buy"

                        await exchange.create_market_order(sym,close_side,tr["total_amount"])

                        pnl = ((current-tr["avg_entry"])/tr["avg_entry"])*100 if is_long else ((tr["avg_entry"]-current)/tr["avg_entry"])*100

                        pnl *= LEVERAGE

                        await edit_telegram_message(tr["msg_id_initial"],f"✅ TP HIT {sym}\nPnL {pnl:.2f}%")

                        del open_trades[sym]

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:
            logging.error(e)
            await asyncio.sleep(5)

# PROCESS SYMBOL
async def process_symbol(symbol):

    try:

        for tf in TIMEFRAMES:

            candles = await exchange.fetch_ohlcv(symbol,tf,limit=6)

            if len(candles)<6:
                continue

            signal_time = candles[-1][0]

            async with trade_lock:

                if len(open_trades)>=MAX_OPEN_TRADES:
                    return

                if sent_signals.get((symbol,tf,"rising")) == signal_time or sent_signals.get((symbol,tf,"falling")) == signal_time:
                    continue

            pattern=None
            side=None

            if detect_rising_three(candles):

                pattern="rising"
                side="buy"
                sent_signals[(symbol,tf,"rising")] = signal_time

            elif detect_falling_three(candles):

                pattern="falling"
                side="sell"
                sent_signals[(symbol,tf,"falling")] = signal_time

            else:
                continue

            ticker = await exchange.fetch_ticker(symbol)

            entry = ticker["last"]

            amount = (CAPITAL_INITIAL*LEVERAGE)/entry

            order = await exchange.create_market_order(symbol,side,amount)

            filled = order.get("average") or entry

            tp = filled*(1+TP_INITIAL_PCT) if side=="buy" else filled*(1-TP_INITIAL_PCT)

            msg = (
                f"📈 ENTRY {symbol}\n"
                f"Side: {side.upper()}\n"
                f"TF: {tf}\n"
                f"Entry: {filled}\n"
                f"TP: {tp}"
            )

            mid = await send_telegram(msg)

            async with trade_lock:

                open_trades[symbol] = {
                    "side":side,
                    "entries":[{"price":filled,"amount":amount,"margin":CAPITAL_INITIAL}],
                    "total_amount":amount,
                    "avg_entry":filled,
                    "tp":tp,
                    "dca_done":False,
                    "msg_id_initial":mid
                }

            break

    except Exception as e:
        logging.error(e)

# BATCH
async def process_batch(chunk):

    tasks = [asyncio.create_task(process_symbol(s)) for s in chunk]

    await asyncio.gather(*tasks,return_exceptions=True)

async def scan_loop(symbols):

    while True:

        chunk_size = math.ceil(len(symbols)/NUM_CHUNKS)

        chunks = [symbols[i:i+chunk_size] for i in range(0,len(symbols),chunk_size)]

        for i,chunk in enumerate(chunks):

            await process_batch(chunk)

            if i < len(chunks)-1:
                await asyncio.sleep(BATCH_DELAY)

# MAIN
async def main():

    global exchange

    exchange = await initialize_exchange()

    symbols = get_symbols(exchange.markets)

    await send_telegram(
        f"🚀 Trading Bot Started\n"
        f"Time: {get_ist_time()}\n"
        f"Symbols: {len(symbols)}\n"
        f"Leverage: {LEVERAGE}x\n"
        f"Timeframes: {', '.join(TIMEFRAMES)}\n"
        f"TP Check: {TP_CHECK_INTERVAL}s"
    )

    await asyncio.gather(
        scan_loop(symbols),
        monitor_tp_and_dca()
    )

# START
try:

    asyncio.run(main())

finally:

    if exchange:
        asyncio.run(exchange.close())
