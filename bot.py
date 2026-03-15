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

# ENV
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET")

TIMEFRAMES = ["5m","30m"]

LEVERAGE = 7

TP_CHECK_INTERVAL = 2

MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0

CAPITAL_INITIAL = 15
CAPITAL_DCA = 30

TP_INITIAL_PCT = 1.1/100
TP_AFTER_DCA_PCT = 0.6/100
DCA_TRIGGER_PCT = 2/100

MAX_OPEN_TRADES = 5

NUM_CHUNKS = 8
BATCH_DELAY = 2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

exchange=None
open_trades={}
sent_signals={}
trade_lock=asyncio.Lock()

# TIME
def get_ist_time():
    return datetime.now(pytz.timezone("Asia/Kolkata"))

# TELEGRAM
async def send_telegram(msg):
    url=f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    async with aiohttp.ClientSession() as session:
        async with session.post(url,data={
            "chat_id":CHAT_ID,
            "text":msg
        }) as resp:

            r=await resp.json()
            return r.get("result",{}).get("message_id")

# EXCHANGE
async def initialize_exchange():

    ex=ccxt.binance({
        "apiKey":API_KEY,
        "secret":API_SECRET,
        "enableRateLimit":True,
        "options":{
            "defaultType":"future"
        }
    })

    await ex.load_markets()

    logging.info("Connected directly")

    return ex

# SYMBOLS
def get_symbols(markets):

    return [
        s for s in markets
        if "USDT" in s and markets[s].get("swap")
    ]

# HELPERS
def is_bullish(c): return c[4]>c[1]
def is_bearish(c): return c[4]<c[1]

def body_pct(c):
    return abs(c[4]-c[1])/c[1]*100 if c[1] else 0

def lower_wick_pct(c):

    o,h,l,c=c[1],c[2],c[3],c[4]

    body=abs(c-o)

    if body==0:
        return 0

    lower=min(o,c)-l

    return (lower/body)*100

# PATTERN
def detect_rising_three(c):

    c2,c1,c0=c[-4],c[-3],c[-2]

    big_green=is_bullish(c2) and body_pct(c2)>=MIN_BIG_BODY_PCT

    small1=is_bearish(c1) and body_pct(c1)<MAX_SMALL_BODY_PCT
    small2=is_bearish(c0) and body_pct(c0)<MAX_SMALL_BODY_PCT

    return big_green and small1 and small2

def detect_falling_three(c):

    c2,c1,c0=c[-4],c[-3],c[-2]

    big_red=is_bearish(c2) and body_pct(c2)>=MIN_BIG_BODY_PCT

    small1=is_bullish(c1) and body_pct(c1)<MAX_SMALL_BODY_PCT
    small2=is_bullish(c0) and body_pct(c0)<MAX_SMALL_BODY_PCT

    return big_red and small1 and small2

# WAIT FOR CANDLE CLOSE
def seconds_to_next(tf):

    now=get_ist_time()

    if tf=="5m":

        secs=now.minute*60+now.second
        return (300-(secs%300))

    if tf=="30m":

        secs=now.minute*60+now.second
        return (1800-(secs%1800))

# PROCESS SYMBOL
async def process_symbol(symbol,tf):

    try:

        candles=await exchange.fetch_ohlcv(symbol,tf,limit=6)

        signal_time=candles[-1][0]

        async with trade_lock:

            if len(open_trades)>=MAX_OPEN_TRADES:
                return

            if sent_signals.get((symbol,tf))==signal_time:
                return

        side=None

        if detect_rising_three(candles):
            side="buy"

        elif detect_falling_three(candles):
            side="sell"

        else:
            return

        ticker=await exchange.fetch_ticker(symbol)

        entry=ticker["last"]

        amount=(CAPITAL_INITIAL*LEVERAGE)/entry

        order=await exchange.create_market_order(symbol,side,amount)

        filled=order.get("average") or entry

        tp=filled*(1+TP_INITIAL_PCT) if side=="buy" else filled*(1-TP_INITIAL_PCT)

        msg=(
            f"ENTRY {symbol}\n"
            f"Side {side}\n"
            f"TF {tf}\n"
            f"Entry {filled}\n"
            f"TP {tp}"
        )

        mid=await send_telegram(msg)

        async with trade_lock:

            open_trades[symbol]={
                "side":side,
                "avg_entry":filled,
                "tp":tp,
                "total_amount":amount,
                "dca_done":False,
                "msg_id":mid
            }

            sent_signals[(symbol,tf)]=signal_time

    except Exception as e:
        logging.error(e)

# BATCH
async def process_batch(symbols,tf):

    tasks=[asyncio.create_task(process_symbol(s,tf)) for s in symbols]

    await asyncio.gather(*tasks,return_exceptions=True)

# SCAN LOOP
async def scan_loop(symbols,tf):

    while True:

        wait=seconds_to_next(tf)

        logging.info(f"Next {tf} close in ~{round(wait/60,1)} min")

        await asyncio.sleep(wait)

        chunk_size=math.ceil(len(symbols)/NUM_CHUNKS)

        chunks=[symbols[i:i+chunk_size] for i in range(0,len(symbols),chunk_size)]

        for chunk in chunks:

            await process_batch(chunk,tf)

            await asyncio.sleep(BATCH_DELAY)

# TP MONITOR
async def monitor_tp():

    while True:

        try:

            async with trade_lock:

                if not open_trades:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                tickers=await exchange.fetch_tickers(list(open_trades.keys()))

                for sym in list(open_trades):

                    tr=open_trades[sym]

                    price=tickers[sym]["last"]

                    is_long=tr["side"]=="buy"

                    hit=(is_long and price>=tr["tp"]) or (not is_long and price<=tr["tp"])

                    if hit:

                        side="sell" if is_long else "buy"

                        await exchange.create_market_order(sym,side,tr["total_amount"])

                        await send_telegram(f"TP HIT {sym}")

                        del open_trades[sym]

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:

            logging.error(e)

            await asyncio.sleep(5)

# MAIN
async def main():

    global exchange

    exchange=await initialize_exchange()

    symbols=get_symbols(exchange.markets)

    await send_telegram(
        f"🚀 BOT STARTED\n"
        f"Symbols {len(symbols)}\n"
        f"Leverage {LEVERAGE}x\n"
        f"TFs {', '.join(TIMEFRAMES)}"
    )

    await asyncio.gather(

        scan_loop(symbols,"5m"),

        scan_loop(symbols,"30m"),

        monitor_tp()

    )

# START
try:

    asyncio.run(main())

finally:

    if exchange:
        asyncio.run(exchange.close())
