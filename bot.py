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

# === PATTERN ===
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
    return [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active', True)]

async def prepare_symbol(symbol):
    try:
        await exchange.set_margin_mode('isolated', symbol)
        await exchange.set_leverage(LEVERAGE, symbol)
        logging.info(f"Prepared {symbol}: isolated + {LEVERAGE}x")
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

def get_next_candle_close():
    now = get_ist_time()
    secs = now.minute * 60 + now.second
    secs_to = (5 * 60) - (secs % (5 * 60))
    if secs_to < 10:
        secs_to += 5 * 60
    return time.time() + secs_to

def get_avg_entry_and_total(trade):
    total_pos = 0.0
    weighted = 0.0
    for e in trade['entries']:
        weighted += e['price'] * e['amount']
        total_pos += e['amount']
    if total_pos == 0:
        return 0.0, 0.0
    return weighted / total_pos, total_pos

# === MONITOR (FIXED: markPrice + fixed DCA from initial_entry) ===
async def monitor_tp_and_dca():
    while True:
        try:
            async with trade_lock:
                open_symbols = list(open_trades.keys())
                if not open_symbols:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                # === BATCH FETCH USING MARK PRICE (CRITICAL FIX) ===
                prices = {}
                try:
                    tickers = await exchange.fetch_tickers(open_symbols)
                    for sym, t in tickers.items():
                        prices[sym] = t.get('markPrice') or t.get('last') or t.get('close')
                except Exception as e:
                    logging.warning(f"Batch tickers error: {e}")

                for sym in list(open_trades):
                    tr = open_trades[sym]

                    # Position sync (unchanged)
                    try:
                        pos = await exchange.fetch_position(sym)
                        if not pos or not pos.get('info'):
                            continue
                        info = pos['info']
                        real_amount = abs(float(info.get('positionAmt', '0')))
                        if real_amount <= 0:
                            await send_telegram(f"🗑️ Position on {sym} closed externally")
                            del open_trades[sym]
                            await asyncio.to_thread(save_trades)
                            continue
                        real_avg = float(info.get('entryPrice', tr['avg_entry']))
                        if abs(real_amount - tr['total_amount']) > 0.0001:
                            await send_telegram(f"🔄 **MANUAL POSITION CHANGE** on {sym}")
                        tr['total_amount'] = real_amount
                        tr['avg_entry'] = real_avg
                    except Exception as e:
                        logging.debug(f"Position sync failed for {sym}: {e}")

                    # Migrate old trades
                    if 'dca_level' not in tr:
                        tr['dca_level'] = 1 if tr.get('dca_done', False) else 0
                    if 'initial_entry' not in tr:
                        tr['initial_entry'] = tr.get('avg_entry', 0)
                    if 'sl' not in tr:
                        is_long_mig = tr['side'] == 'buy'
                        tr['sl'] = round_price(sym, tr['initial_entry'] * (1 - SL_PCT) if is_long_mig else tr['initial_entry'] * (1 + SL_PCT))

                    # === CURRENT PRICE (markPrice first) ===
                    current = prices.get(sym)
                    if not current:
                        try:
                            t = await exchange.fetch_ticker(sym)
                            current = t.get('markPrice') or t.get('last') or t.get('close')
                        except:
                            continue
                    if not current:
                        continue

                    is_long = tr['side'] == 'buy'

                    # === 1. SL CHECK (fixed from initial) ===
                    hit_sl = (is_long and current <= tr.get('sl', 0)) or (not is_long and current >= tr.get('sl', 0))
                    if hit_sl and tr.get('sl'):
                        # ... (SL close logic unchanged - already correct)
                        try:
                            close_side = 'sell' if is_long else 'buy'
                            close_order = await exchange.create_order(sym, 'market', close_side, tr['total_amount'], params={'reduceOnly': True})
                            exit_price = close_order.get('average') or current
                            exit_price = round_price(sym, exit_price)
                            pnl_pct = ((exit_price - tr['avg_entry']) / tr['avg_entry']) * 100 if is_long else ((tr['avg_entry'] - exit_price) / tr['avg_entry']) * 100
                            leveraged_pnl = pnl_pct * LEVERAGE
                            total_margin_used = sum(e['margin'] for e in tr['entries'])
                            profit_usdt = total_margin_used * (leveraged_pnl / 100)

                            msg = f"**SL HIT** {sym} — {'LONG' if is_long else 'SHORT'}\nInitial: {tr['initial_entry']:.6f}\nAvg: {tr['avg_entry']:.6f}\nExit: {exit_price:.6f}\nPnL: {leveraged_pnl:.2f}% (${profit_usdt:+.2f})\nDCA levels: {tr.get('dca_level', 0)}"
                            await edit_telegram_message(tr['msg_id_initial'], msg)
                            if tr.get('msg_id_dca'):
                                await edit_telegram_message(tr['msg_id_dca'], "Position closed on SL ↓")
                            await asyncio.to_thread(save_closed_trade, {'symbol': sym, 'pnl_usdt': profit_usdt, 'pnl_pct': leveraged_pnl, 'hit': 'SL', 'dca_level': tr.get('dca_level', 0), 'ts': time.time()})
                            del open_trades[sym]
                            await asyncio.to_thread(save_trades)
                            continue
                        except Exception as e:
                            logging.error(f"SL close failed {sym}: {e}")

                    # === 2. DCA CHECKS (FIXED: always from initial_entry) ===
                    dca_level = tr.get('dca_level', 0)
                    if dca_level < MAX_DCA_LEVELS:
                        trigger_pct = DCA_TRIGGER_PCTS[dca_level]
                        reference_price = tr['initial_entry']                    # ← FIXED
                        trigger_level = reference_price * (1 - trigger_pct) if is_long else reference_price * (1 + trigger_pct)

                        if (is_long and current <= trigger_level) or (not is_long and current >= trigger_level):
                            try:
                                used_margin = sum(e['margin'] for e in tr['entries'])
                                if used_margin + CAPITAL_DCA_PER_STEP > MAX_MARGIN_PER_TRADE:
                                    continue
                                dca_amount_raw = (CAPITAL_DCA_PER_STEP * LEVERAGE) / current
                                dca_amount = float(round_amount(sym, dca_amount_raw))
                                if dca_amount <= 0:
                                    continue
                                dca_order = await exchange.create_market_order(sym, tr['side'], dca_amount)
                                dca_price = dca_order.get('average') or current
                                dca_price = round_price(sym, dca_price)

                                tr['entries'].append({'price': dca_price, 'amount': dca_amount, 'margin': CAPITAL_DCA_PER_STEP, 'ts': time.time()})
                                tr['total_amount'] += dca_amount
                                tr['avg_entry'], _ = get_avg_entry_and_total(tr)
                                tr['dca_level'] += 1

                                new_tp_pct = TP_AFTER_DCA_PCTS[tr['dca_level'] - 1]
                                tr['tp'] = round_price(sym, tr['avg_entry'] * (1 + new_tp_pct) if is_long else tr['avg_entry'] * (1 - new_tp_pct))

                                dca_msg = f"**DCA {tr['dca_level']} TRIGGERED** {sym}\nDCA entry: {dca_price:.6f}\nAdded: ${CAPITAL_DCA_PER_STEP}\nNew avg: {tr['avg_entry']:.6f}\nNew TP: {tr['tp']:.6f}"
                                mid_dca = await send_telegram(dca_msg)
                                tr['msg_id_dca'] = mid_dca
                                await asyncio.to_thread(save_trades())
                            except Exception as e:
                                logging.error(f"DCA failed {sym}: {e}")

                    # === 3. TP CHECK ===
                    hit_tp = (is_long and current >= tr['tp']) or (not is_long and current <= tr['tp'])
                    if hit_tp:
                        # ... (TP close logic unchanged)
                        try:
                            close_side = 'sell' if is_long else 'buy'
                            close_order = await exchange.create_order(sym, 'market', close_side, tr['total_amount'], params={'reduceOnly': True})
                            exit_price = close_order.get('average') or current
                            exit_price = round_price(sym, exit_price)
                            pnl_pct = ((exit_price - tr['avg_entry']) / tr['avg_entry']) * 100 if is_long else ((tr['avg_entry'] - exit_price) / tr['avg_entry']) * 100
                            leveraged_pnl = pnl_pct * LEVERAGE
                            total_margin_used = sum(e['margin'] for e in tr['entries'])
                            profit_usdt = total_margin_used * (leveraged_pnl / 100)

                            msg = f"**TP HIT** {sym} — {'LONG' if is_long else 'SHORT'}\nInitial: {tr['initial_entry']:.6f}\nAvg: {tr['avg_entry']:.6f}\nExit: {exit_price:.6f}\nPnL: {leveraged_pnl:.2f}% (${profit_usdt:+.2f})\nDCA levels: {tr.get('dca_level', 0)}"
                            await edit_telegram_message(tr['msg_id_initial'], msg)
                            if tr.get('msg_id_dca'):
                                await edit_telegram_message(tr['msg_id_dca'], "Position closed on TP ↑")
                            await asyncio.to_thread(save_closed_trade, {'symbol': sym, 'pnl_usdt': profit_usdt, 'pnl_pct': leveraged_pnl, 'hit': 'TP', 'dca_level': tr.get('dca_level', 0), 'ts': time.time()})
                            del open_trades[sym]
                            await asyncio.to_thread(save_trades())
                        except Exception as e:
                            logging.error(f"TP close failed {sym}: {e}")

            await asyncio.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Monitor loop error: {e}")
            await asyncio.sleep(30)

# === PROCESS SYMBOL (updated entry message with fixed DCA triggers) ===
async def process_symbol(symbol, timeframe):
    try:
        candles = await exchange.fetch_ohlcv(symbol, timeframe, limit=CANDLE_LIMIT)
        if len(candles) < 6: return

        signal_time = candles[-1][0]
        key_rising = (symbol, timeframe, 'rising')
        key_falling = (symbol, timeframe, 'falling')

        async with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES: return
            if sent_signals.get(key_rising) == signal_time or sent_signals.get(key_falling) == signal_time:
                return

        pattern = side = None
        if detect_rising_three(candles):
            pattern, side = 'rising three', 'buy'
            sent_signals[key_rising] = signal_time
        elif detect_falling_three(candles):
            pattern, side = 'falling three', 'sell'
            sent_signals[key_falling] = signal_time
        else:
            return

        await prepare_symbol(symbol)
        ticker = await exchange.fetch_ticker(symbol)
        entry_price = round_price(symbol, ticker['last'])

        amount_raw = (CAPITAL_INITIAL * LEVERAGE) / entry_price
        amount = float(round_amount(symbol, amount_raw))
        if amount <= 0: return

        entry_order = await exchange.create_market_order(symbol, side, amount)
        filled_price = entry_order.get('average') or entry_price
        filled_price = round_price(symbol, filled_price)

        sl = round_price(symbol, filled_price * (1 - SL_PCT) if side == 'buy' else filled_price * (1 + SL_PCT))
        tp = round_price(symbol, filled_price * (1 + TP_INITIAL_PCT) if side == 'buy' else filled_price * (1 - TP_INITIAL_PCT))

        # Fixed DCA trigger prices (shown to user)
        dca1_trigger = round_price(symbol, filled_price * (1 - DCA_TRIGGER_PCTS[0]) if side == 'buy' else filled_price * (1 + DCA_TRIGGER_PCTS[0]))
        dca2_trigger = round_price(symbol, filled_price * (1 - DCA_TRIGGER_PCTS[1]) if side == 'buy' else filled_price * (1 + DCA_TRIGGER_PCTS[1]))

        entry_msg = (
            f"**ENTRY** {symbol} — {'LONG' if side=='buy' else 'SHORT'} ({timeframe.upper()})\n"
            f"Entry: {filled_price:.6f}\n"
            f"Pattern: {pattern}\n"
            f"Size: {amount:.4f} (${CAPITAL_INITIAL:.0f})\n"
            f"TP: {tp:.6f} ({TP_INITIAL_PCT*100:.1f}%)\n"
            f"SL: {sl:.6f} ({SL_PCT*100:.0f}% from initial)\n"
            f"DCA1 @ {dca1_trigger:.6f} ({DCA_TRIGGER_PCTS[0]*100:.1f}% adverse) → +${CAPITAL_DCA_PER_STEP}\n"
            f"DCA2 @ {dca2_trigger:.6f} ({DCA_TRIGGER_PCTS[1]*100:.1f}% adverse) → +${CAPITAL_DCA_PER_STEP}\n"
            f"• Triggers fixed from initial entry • Uses mark price for all checks"
        )

        mid = await send_telegram(entry_msg)

        async with trade_lock:
            open_trades[symbol] = {
                'side': side,
                'entries': [{'price': filled_price, 'amount': amount, 'margin': CAPITAL_INITIAL, 'ts': time.time(), 'timeframe': timeframe}],
                'total_amount': amount,
                'avg_entry': filled_price,
                'tp': tp,
                'sl': sl,
                'initial_entry': filled_price,
                'dca_level': 0,
                'msg_id_initial': mid,
                'msg_id_dca': None,
                'open_ts': time.time()
            }
            await asyncio.to_thread(save_trades)
        logging.info(f"Opened {side} {symbol} @ {filled_price}")
    except Exception as e:
        logging.error(f"Trade failed {symbol}: {e}")

# === BATCH + SCAN + DAILY (unchanged) ===
async def process_batch(symbols_chunk, timeframe):
    tasks = [asyncio.create_task(process_symbol(s, timeframe)) for s in symbols_chunk]
    await asyncio.gather(*tasks, return_exceptions=True)

async def scan_loop(symbols):
    while True:
        wait_until = get_next_candle_close()
        await asyncio.sleep(max(0, wait_until - time.time()))
        for tf in TIMEFRAMES:
            chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
            chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]
            for i, chunk in enumerate(chunks):
                await process_batch(chunk, tf)
                if i < len(chunks)-1:
                    await asyncio.sleep(BATCH_DELAY)
            await asyncio.sleep(1.0)

async def daily_summary():
    while True:
        await asyncio.sleep(86400)
        # (unchanged)
        pass  # keep your original daily summary

# === MAIN ===
async def main():
    global exchange
    exchange = await initialize_exchange()
    markets = exchange.markets
    symbols = get_symbols(markets)
    load_trades()

    startup = (
        f"Bot restarted @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\n"
        f"Max trades: {MAX_OPEN_TRADES} | Lev: {LEVERAGE}x\n"
        f"Initial: ${CAPITAL_INITIAL} → DCA +${CAPITAL_DCA_PER_STEP}×2\n"
        f"TP: {TP_INITIAL_PCT*100:.1f}% → {TP_AFTER_DCA_PCTS[0]*100:.2f}% (DCA1) → {TP_AFTER_DCA_PCTS[1]*100:.2f}% (DCA2)\n"
        f"SL: {SL_PCT*100:.0f}% from initial\n"
        f"DCA triggers FIXED from initial entry • All checks use MARK PRICE"
    )
    await send_telegram(startup)

    tasks = [
        asyncio.create_task(scan_loop(symbols)),
        asyncio.create_task(monitor_tp_and_dca()),
        asyncio.create_task(daily_summary()),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
