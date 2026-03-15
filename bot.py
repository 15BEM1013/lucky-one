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

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TIMEFRAME = '5m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0

CAPITAL_INITIAL = 15
CAPITAL_DCA = 30
MAX_MARGIN_PER_TRADE = 45

LEVERAGE = 10

TP_INITIAL_PCT = 1.1 / 100
TP_AFTER_DCA_PCT = 0.6 / 100
DCA_TRIGGER_PCT = 2 / 100

TP_CHECK_INTERVAL = 8
MAX_OPEN_TRADES = 5

TRADE_FILE = "open_trades.json"
CLOSED_TRADE_FILE = "closed_trades.json"

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET")

logging.basicConfig(level=logging.INFO)

trade_lock = asyncio.Lock()

open_trades = {}
sent_signals = {}

exchange = None


def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))


def save_trades():
    with open(TRADE_FILE, "w") as f:
        json.dump(open_trades, f)


def load_trades():
    global open_trades
    if os.path.exists(TRADE_FILE):
        with open(TRADE_FILE) as f:
            open_trades = json.load(f)


async def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    async with aiohttp.ClientSession() as session:
        await session.post(url, data={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown"
        })


async def edit_telegram_message(mid, msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"

    async with aiohttp.ClientSession() as session:
        await session.post(url, data={
            "chat_id": CHAT_ID,
            "message_id": mid,
            "text": msg,
            "parse_mode": "Markdown"
        })


async def initialize_exchange():
    ex = ccxt.binance({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "options": {"defaultType": "future"},
        "enableRateLimit": True
    })

    await ex.load_markets()
    return ex


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

    total = 0
    weighted = 0

    for e in trade['entries']:
        weighted += e['price'] * e['amount']
        total += e['amount']

    if total == 0:
        return 0, 0

    return weighted / total, total


# ===== UPDATE START =====
async def get_real_position_size(symbol):

    try:
        positions = await exchange.fetch_positions([symbol])

        for p in positions:
            if p['symbol'] == symbol:
                return abs(float(p['contracts']))

    except Exception as e:
        logging.warning(f"Position fetch error {symbol}: {e}")

    return 0
# ===== UPDATE END =====


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

                    current = tickers[sym]['last']

                    is_long = tr['side'] == "buy"

                    # ===== UPDATE START =====
                    real_size = await get_real_position_size(sym)

                    bot_size = tr['total_amount']

                    if real_size == 0:

                        logging.info(f"{sym} manually closed")

                        await send_telegram(f"⚠️ {sym} position manually closed")

                        del open_trades[sym]

                        save_trades()

                        continue

                    if abs(real_size - bot_size) > (bot_size * 0.01):

                        logging.info(f"{sym} size changed manually")

                        tr['total_amount'] = real_size

                        await send_telegram(
                            f"⚠️ Manual position change\n{sym}\nOld size: {bot_size}\nNew size: {real_size}"
                        )

                        save_trades()
                    # ===== UPDATE END =====

                    if not tr['dca_done']:

                        trigger = tr['avg_entry'] * (1 - DCA_TRIGGER_PCT) if is_long else tr['avg_entry'] * (1 + DCA_TRIGGER_PCT)

                        if (is_long and current <= trigger) or (not is_long and current >= trigger):

                            dca_amt = (CAPITAL_DCA * LEVERAGE) / current
                            dca_amt = round_amount(sym, dca_amt)

                            order = await exchange.create_market_order(sym, tr['side'], dca_amt)

                            price = order.get("average") or current

                            tr['entries'].append({
                                "price": price,
                                "amount": dca_amt,
                                "margin": CAPITAL_DCA
                            })

                            tr['dca_done'] = True

                            tr['avg_entry'], tr['total_amount'] = get_avg_entry_and_total(tr)

                            tr['tp'] = round_price(sym,
                                tr['avg_entry'] * (1 + TP_AFTER_DCA_PCT)
                                if is_long
                                else tr['avg_entry'] * (1 - TP_AFTER_DCA_PCT)
                            )

                            await send_telegram(f"DCA added {sym}")

                            save_trades()

                    hit_tp = (is_long and current >= tr['tp']) or (not is_long and current <= tr['tp'])

                    if hit_tp:

                        close_side = "sell" if is_long else "buy"

                        # ===== UPDATE START =====
                        await exchange.create_order(
                            sym,
                            "market",
                            close_side,
                            None,
                            params={"reduceOnly": True, "closePosition": True}
                        )
                        # ===== UPDATE END =====

                        await send_telegram(f"TP HIT {sym}")

                        del open_trades[sym]

                        save_trades()

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:

            logging.error(e)

            await asyncio.sleep(30)


async def main():

    global exchange

    exchange = await initialize_exchange()

    markets = exchange.markets

    symbols = [s for s in markets if "USDT" in s and markets[s]['swap']]

    load_trades()

    await send_telegram(
        f"Bot restarted\nOpen trades: {len(open_trades)}"
    )

    await asyncio.gather(
        monitor_tp_and_dca()
    )


if __name__ == "__main__":

    asyncio.run(main())
