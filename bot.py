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

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

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

TP_INITIAL_PCT = 1.1 / 100
TP_AFTER_DCA_PCT = 0.6 / 100

DCA_TRIGGER_PCT = 2.0 / 100

TP_CHECK_INTERVAL = 2

MAX_OPEN_TRADES = 5

TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')

if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

PROXY_LIST = []

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

trade_lock = asyncio.Lock()

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
    except Exception as e:
        logging.error(f"Save trades error: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
        else:
            open_trades = {}
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

async def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url,data={
                'chat_id': CHAT_ID,
                'text': msg,
                'parse_mode': 'Markdown'
            }) as resp:

                r = await resp.json()
                return r.get('result',{}).get('message_id')

    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

async def edit_telegram_message(mid,new_text):

    if not mid:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url,data={
                'chat_id':CHAT_ID,
                'message_id':mid,
                'text':new_text,
                'parse_mode':'Markdown'
            }) as resp:
                await resp.release()

    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

async def initialize_exchange():

    ex = ccxt.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'options': {
            'defaultType': 'future',
            'marginMode': 'isolated'
        },
        'enableRateLimit': True,
    })

    await ex.load_markets()
    logging.info("Connected to Binance")

    return ex

exchange = None
sent_signals = {}
open_trades = {}

def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]

def body_pct(c):
    return abs(c[4]-c[1])/c[1]*100 if c[1]!=0 else 0

def lower_wick_pct(c):

    o,h,l,c = c[1],c[2],c[3],c[4]

    body = abs(c-o)

    if body==0:
        return 0

    lower = min(o,c)-l

    return (lower/body)*100

def round_price(symbol,price):

    try:
        m = exchange.market(symbol)

        tick = float(m['info']['filters'][0]['tickSize'])

        prec = int(round(-math.log10(tick)))

        return round(price,prec)

    except:
        return price

def round_amount(symbol,amt):

    try:
        return exchange.amount_to_precision(symbol,amt)

    except:
        return amt

def detect_rising_three(candles):

    if len(candles)<6:
        return False

    c2,c1,c0 = candles[-4],candles[-3],candles[-2]

    avg_vol = sum(c[5] for c in candles[-6:-1])/5

    big_green = is_bullish(c2) and body_pct(c2)>=MIN_BIG_BODY_PCT and c2[5]>avg_vol

    small_red_1 = is_bearish(c1) and body_pct(c1)<MAX_SMALL_BODY_PCT and lower_wick_pct(c1)>=MIN_LOWER_WICK_PCT

    small_red_0 = is_bearish(c0) and body_pct(c0)<MAX_SMALL_BODY_PCT and lower_wick_pct(c0)>=MIN_LOWER_WICK_PCT

    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):

    if len(candles)<6:
        return False

    c2,c1,c0 = candles[-4],candles[-3],candles[-2]

    avg_vol = sum(c[5] for c in candles[-6:-1])/5

    big_red = is_bearish(c2) and body_pct(c2)>=MIN_BIG_BODY_PCT and c2[5]>avg_vol

    small_green_1 = is_bullish(c1) and body_pct(c1)<MAX_SMALL_BODY_PCT and lower_wick_pct(c1)>=MIN_LOWER_WICK_PCT

    small_green_0 = is_bullish(c0) and body_pct(c0)<MAX_SMALL_BODY_PCT and lower_wick_pct(c0)>=MIN_LOWER_WICK_PCT

    return big_red and small_green_1 and small_green_0

def get_symbols(markets):
    return [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active',True)]

async def prepare_symbol(symbol):

    try:
        await exchange.set_margin_mode('isolated',symbol)
        await exchange.set_leverage(LEVERAGE,symbol)

    except:
        pass

def get_avg_entry_and_total(trade):

    total = 0
    weighted = 0

    for e in trade['entries']:

        weighted += e['price']*e['amount']
        total += e['amount']

    if total==0:
        return 0,0

    return weighted/total,total

async def monitor_tp_and_dca():

    while True:

        try:

            async with trade_lock:

                open_symbols=list(open_trades.keys())

                if not open_symbols:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                tickers = await exchange.fetch_tickers(open_symbols)

                for sym in list(open_trades):

                    tr = open_trades[sym]

                    current = tickers[sym]['last']

                    is_long = tr['side']=='buy'

                    if not tr['dca_done']:

                        trigger = tr['avg_entry']*(1-DCA_TRIGGER_PCT) if is_long else tr['avg_entry']*(1+DCA_TRIGGER_PCT)

                        if (is_long and current<=trigger) or (not is_long and current>=trigger):

                            dca_amount_raw=(CAPITAL_DCA*LEVERAGE)/current

                            dca_amount=float(round_amount(sym,dca_amount_raw))

                            dca_order=await exchange.create_market_order(sym,tr['side'],dca_amount)

                            dca_price=dca_order.get('average') or current

                            tr['entries'].append({'price':dca_price,'amount':dca_amount,'margin':CAPITAL_DCA})

                            tr['avg_entry'],_=get_avg_entry_and_total(tr)

                            tr['dca_done']=True

                            tr['tp']=round_price(sym,tr['avg_entry']*(1+TP_AFTER_DCA_PCT) if is_long else tr['avg_entry']*(1-TP_AFTER_DCA_PCT))

                            await send_telegram(f"DCA {sym} new avg {tr['avg_entry']}")

                    hit_tp = (is_long and current>=tr['tp']) or (not is_long and current<=tr['tp'])

                    if hit_tp:

                        close_side='sell' if is_long else 'buy'

                        await exchange.create_market_order(sym,close_side,tr['total_amount'])

                        pnl_pct=((current-tr['avg_entry'])/tr['avg_entry'])*100 if is_long else ((tr['avg_entry']-current)/tr['avg_entry'])*100

                        leveraged_pnl=pnl_pct*LEVERAGE

                        await edit_telegram_message(tr['msg_id_initial'],f"TP HIT {sym} {leveraged_pnl:.2f}%")

                        del open_trades[sym]

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:
            logging.error(e)
            await asyncio.sleep(5)

async def process_symbol(symbol):

    try:

        for tf in TIMEFRAMES:

            candles=await exchange.fetch_ohlcv(symbol,tf,limit=6)

            if len(candles)<6:
                continue

            signal_time=candles[-1][0]

            async with trade_lock:

                if len(open_trades)>=MAX_OPEN_TRADES:
                    return

                if sent_signals.get((symbol,tf,'rising'))==signal_time or sent_signals.get((symbol,tf,'falling'))==signal_time:
                    continue

            pattern=None
            side=None

            if detect_rising_three(candles):
                pattern='rising three'
                side='buy'
                sent_signals[(symbol,tf,'rising')]=signal_time

            elif detect_falling_three(candles):
                pattern='falling three'
                side='sell'
                sent_signals[(symbol,tf,'falling')]=signal_time

            else:
                continue

            await prepare_symbol(symbol)

            ticker=await exchange.fetch_ticker(symbol)

            entry_price=round_price(symbol,ticker['last'])

            amount_raw=(CAPITAL_INITIAL*LEVERAGE)/entry_price

            amount=float(round_amount(symbol,amount_raw))

            entry_order=await exchange.create_market_order(symbol,side,amount)

            filled_price=entry_order.get('average') or entry_price

            tp=round_price(symbol,filled_price*(1+TP_INITIAL_PCT) if side=='buy' else filled_price*(1-TP_INITIAL_PCT))

            entry_msg=(f"ENTRY {symbol} {side.upper()}\nTF {tf}\nEntry {filled_price}\nTP {tp}")

            mid=await send_telegram(entry_msg)

            async with trade_lock:

                open_trades[symbol]={
                    'side':side,
                    'entries':[{'price':filled_price,'amount':amount,'margin':CAPITAL_INITIAL}],
                    'total_amount':amount,
                    'avg_entry':filled_price,
                    'tp':tp,
                    'dca_done':False,
                    'msg_id_initial':mid
                }

            break

    except Exception as e:
        logging.error(e)

async def process_batch(symbols_chunk):

    tasks=[asyncio.create_task(process_symbol(s)) for s in symbols_chunk]

    await asyncio.gather(*tasks,return_exceptions=True)

async def scan_loop(symbols):

    while True:

        chunk_size=math.ceil(len(symbols)/NUM_CHUNKS)

        chunks=[symbols[i:i+chunk_size] for i in range(0,len(symbols),chunk_size)]

        for i,chunk in enumerate(chunks):

            await process_batch(chunk)

            if i<len(chunks)-1:
                await asyncio.sleep(BATCH_DELAY)

async def main():

    global exchange

    load_trades()

    exchange=await initialize_exchange()

    symbols=get_symbols(exchange.markets)

    await asyncio.gather(
        scan_loop(symbols),
        monitor_tp_and_dca()
    )

try:
    asyncio.run(main())
finally:
    if exchange:
        asyncio.run(exchange.close())
