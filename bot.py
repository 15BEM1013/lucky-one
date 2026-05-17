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
        logging.info(f"Closed trade saved | PnL: ${closed.get('pnl_usdt', 0):.2f}")
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
            await session.post(url, data={
                'chat_id': CHAT_ID,
                'message_id': mid,
                'text': new_text,
                'parse_mode': 'Markdown'
            }, timeout=aiohttp.ClientTimeout(total=10))
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
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
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

def get_wick_signal(candle):
    if body_pct(candle) < 0.5:
        return None, None
    upper = upper_wick_pct(candle)
    lower = lower_wick_pct(candle)
    is_green = is_bullish(candle)

    if is_green:
        if upper > 50 or (upper > 30 and lower > 30):
            return 'sell', f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **SELL**"
        else:
            return 'buy', f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **BUY**"
    else:
        if lower > 30 or (upper > 30 and lower > 30):
            return 'buy', f"Red Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **BUY**"
        else:
            return 'sell', f"Red Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → **SELL**"

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

def get_avg_entry_and_total(trade):
    total_pos = sum(e['amount'] for e in trade['entries'])
    weighted = sum(e['price'] * e['amount'] for e in trade['entries'])
    return (weighted / total_pos) if total_pos > 0 else 0.0, total_pos

# === BUILD TRADE MESSAGE ===
def build_trade_message(tr, sym, current=None, is_final=False, hit_type=None, exit_price=None, pnl_usdt=None, pnl_pct=None):
    is_long = tr['side'] == 'buy'
    duration = format_duration(time.time() - tr['open_ts'])

    lines = [
        f"**{'LONG' if is_long else 'SHORT'}** {sym} ({tr.get('timeframe', 'N/A')})",
        f"Entry: {tr['initial_price']:.6f} | Avg: {tr['avg_entry']:.6f}",
        f"Duration: {duration}"
    ]

    entries_str = [f"{'Initial' if e['stage']==0 else f'DCA{e['stage']}'}: {e['price']:.6f} (${e['margin']})" for e in tr['entries']]
    lines.append("Entries: " + " | ".join(entries_str))
    lines.append(f"TP: {tr['tp']:.6f} | SL: {SL_PCT*100:.1f}%")

    if tr.get('signal_reason'):
        lines.append(f"Signal: {tr['signal_reason']}")

    if is_final and hit_type:
        lines.append(f"**{hit_type} HIT** | Exit: {exit_price:.6f}")
        lines.append(f"PnL: {pnl_pct:.2f}% (${pnl_usdt:+.2f})")
    else:
        lines.append(f"DCA Stage: {tr['dca_stage']}/2")

    return "\n".join(lines)

# === MONITOR TP + DCA + SL ===
async def monitor_tp_and_dca():
    while True:
        try:
            async with trade_lock:
                if not open_trades:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                symbols = list(open_trades.keys())
                if not symbols:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                try:
                    tickers = await exchange.fetch_tickers(symbols)
                    prices = {sym: t.get('last') or t.get('close') or t.get('markPrice') for sym, t in tickers.items()}
                except Exception as e:
                    logging.error(f"Failed to fetch tickers: {e}")
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                for sym in list(open_trades.keys()):
                    tr = open_trades[sym]
                    current = prices.get(sym)
                    if not current:
                        continue

                    is_long = tr['side'] == 'buy'
                    avg_entry = tr['avg_entry']
                    current_tp = tr['tp']

                    # === SL CHECK ===
                    sl_price = avg_entry * (1 - SL_PCT) if is_long else avg_entry * (1 + SL_PCT)
                    if (is_long and current <= sl_price) or (not is_long and current >= sl_price):
                        await close_trade(sym, "SL", current)
                        continue

                    # === TP CHECK ===
                    if (is_long and current >= current_tp) or (not is_long and current <= current_tp):
                        await close_trade(sym, "TP", current)
                        continue

                    # === DCA LOGIC ===
                    if tr['dca_stage'] < 2:
                        dca_trigger = DCA1_TRIGGER_PCT if tr['dca_stage'] == 0 else DCA2_TRIGGER_PCT
                        dca_level = avg_entry * (1 - dca_trigger) if is_long else avg_entry * (1 + dca_trigger)

                        if (is_long and current <= dca_level) or (not is_long and current >= dca_level):
                            await execute_dca(sym, tr, current)

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:
            logging.error(f"Monitor loop error: {e}")
            await asyncio.sleep(1)

async def execute_dca(sym, tr, current_price):
    try:
        dca_stage = tr['dca_stage'] + 1
        capital = CAPITAL_DCA1 if dca_stage == 1 else CAPITAL_DCA2

        side = tr['side']
        amount_raw = (capital * LEVERAGE) / current_price
        amount = round_amount(sym, amount_raw)

        if amount <= 0:
            return

        order = await exchange.create_market_order(sym, side, amount)
        filled_price = round_price(sym, order.get('average') or current_price)

        tr['entries'].append({
            'price': filled_price,
            'amount': amount,
            'margin': capital,
            'ts': time.time(),
            'stage': dca_stage
        })

        avg_entry, total_amount = get_avg_entry_and_total(tr)
        tr['avg_entry'] = avg_entry
        tr['total_amount'] = total_amount
        tr['dca_stage'] = dca_stage

        # Update TP based on new stage
        if dca_stage == 1:
            tp_pct = TP_AFTER_DCA1_PCT
        else:
            tp_pct = TP_AFTER_DCA2_PCT

        tr['tp'] = round_price(sym, avg_entry * (1 + tp_pct) if side == 'buy' else avg_entry * (1 - tp_pct))

        logging.info(f"DCA{dca_stage} executed on {sym} @ {filled_price}")

        # Update Telegram
        msg_text = build_trade_message(tr, sym)
        if tr.get('msg_id_initial'):
            await edit_telegram_message(tr['msg_id_initial'], msg_text)

    except Exception as e:
        logging.error(f"DCA failed on {sym}: {e}")

async def close_trade(sym, hit_type, exit_price):
    try:
        tr = open_trades[sym]
        side = tr['side']
        close_side = 'sell' if side == 'buy' else 'buy'

        total_amount = sum(e['amount'] for e in tr['entries'])

        close_order = await exchange.create_market_order(sym, close_side, total_amount)
        filled_exit = round_price(sym, close_order.get('average') or exit_price)

        # Calculate PnL
        avg_entry = tr['avg_entry']
        if side == 'buy':
            pnl_pct = (filled_exit - avg_entry) / avg_entry * 100
        else:
            pnl_pct = (avg_entry - filled_exit) / avg_entry * 100

        total_margin = sum(e['margin'] for e in tr['entries'])
        pnl_usdt = total_margin * (pnl_pct / 100) * LEVERAGE

        closed = {
            **tr,
            'exit_price': filled_exit,
            'exit_ts': time.time(),
            'hit_type': hit_type,
            'pnl_pct': pnl_pct,
            'pnl_usdt': pnl_usdt,
            'closed_at': get_ist_time().isoformat()
        }

        save_closed_trade(closed)

        msg_text = build_trade_message(tr, sym, is_final=True, hit_type=hit_type,
                                       exit_price=filled_exit, pnl_usdt=pnl_usdt, pnl_pct=pnl_pct)
        if tr.get('msg_id_initial'):
            await edit_telegram_message(tr['msg_id_initial'], msg_text)
        else:
            await send_telegram(msg_text)

        del open_trades[sym]
        save_trades()
        logging.info(f"Closed {sym} on {hit_type} | PnL: {pnl_pct:.2f}% (${pnl_usdt:.2f})")

    except Exception as e:
        logging.error(f"Close trade failed {sym}: {e}")

# === REST OF THE CODE (unchanged except minor improvements) ===
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

        if is_rising:
            pattern = 'rising_three'
            side, signal_msg = get_wick_signal(big_candle)
        elif is_falling:
            pattern = 'falling_three'
            side, signal_msg = get_wick_signal(big_candle_f)
        else:
            return

        if not side or not signal_msg:
            return

        sent_signals[key] = signal_time
        await prepare_symbol(symbol)

        ticker = await exchange.fetch_ticker(symbol)
        entry_price = round_price(symbol, ticker['last'])

        amount_raw = (CAPITAL_INITIAL * LEVERAGE) / entry_price
        amount = round_amount(symbol, amount_raw)
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

    except ccxt.InsufficientFunds:
        await send_telegram(f"⚠️ **Insufficient Balance** for {symbol}")
    except Exception as e:
        logging.error(f"Trade failed {symbol}: {e}")

# Batch & Scan functions remain the same...
async def process_batch(symbols_chunk, timeframe):
    tasks = [asyncio.create_task(process_symbol(s, timeframe)) for s in symbols_chunk]
    await asyncio.gather(*tasks, return_exceptions=True)

async def scan_loop(symbols):
    while True:
        wait_until = get_next_candle_close()  # Reuse your helper
        sleep_sec = max(0, wait_until - time.time())
        logging.info(f"Next scan in \~{sleep_sec//60} min")
        await asyncio.sleep(sleep_sec)

        for tf in TIMEFRAMES:
            logging.info(f"Scanning {tf}")
            chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
            chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]
            for i, chunk in enumerate(chunks):
                await process_batch(chunk, tf)
                if i < len(chunks) - 1:
                    await asyncio.sleep(BATCH_DELAY)
        logging.info("Full scan completed")

def get_next_candle_close():
    now = get_ist_time()
    secs = now.minute * 60 + now.second
    secs_to = (5 * 60) - (secs % (5 * 60))
    if secs_to < 30:
        secs_to += 5 * 60
    return time.time() + secs_to

async def daily_summary():
    while True:
        await asyncio.sleep(86400)
        try:
            closed = []
            if os.path.exists(CLOSED_TRADE_FILE):
                with open(CLOSED_TRADE_FILE) as f:
                    closed = json.load(f)
            total_pnl = sum(t.get('pnl_usdt', 0) for t in closed)
            bal = await exchange.fetch_balance()
            usdt = bal.get('USDT', {})
            total = usdt.get('free', 0) + usdt.get('total', 0)
            msg = f"📊 *Daily Summary*\nTotal PnL: ${total_pnl:.2f}\nOpen: {len(open_trades)}\nBalance: ${total:.2f}"
            await send_telegram(msg)
        except Exception as e:
            logging.error(f"Daily summary error: {e}")

async def main():
    global exchange
    exchange = await initialize_exchange()
    markets = exchange.markets
    symbols = get_symbols(markets)
    load_trades()

    logging.info(f"Starting bot with {len(symbols)} symbols")

    startup_msg = f"🚀 **Bot Restarted** @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\n" \
                  f"Patterns + Wick Filter Active\nOpen Trades: {len(open_trades)} | Max: {MAX_OPEN_TRADES}"
    await send_telegram(startup_msg)

    tasks = [
        asyncio.create_task(scan_loop(symbols)),
        asyncio.create_task(monitor_tp_and_dca()),
        asyncio.create_task(daily_summary()),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())