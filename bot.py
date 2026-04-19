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
def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

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

# === CANDLE & PATTERN (unchanged) ===
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

def get_symbols(markets):
    return [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active', True)]

async def prepare_symbol(symbol):
    try:
        await exchange.set_margin_mode('isolated', symbol)
        await exchange.set_leverage(LEVERAGE, symbol)
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
    total_pos = sum(e['amount'] for e in trade['entries'])
    weighted = sum(e['price'] * e['amount'] for e in trade['entries'])
    return (weighted / total_pos) if total_pos > 0 else 0.0, total_pos

# === BUILD SINGLE TRADE MESSAGE ===
def build_trade_message(tr, sym, current=None, is_final=False, hit_type=None, exit_price=None, pnl_usdt=None, pnl_pct=None):
    is_long = tr['side'] == 'buy'
    duration = format_duration(time.time() - tr['open_ts'])
    tf = tr.get('timeframe', 'N/A')
    
    lines = [
        f"**{'LONG' if is_long else 'SHORT'}** {sym} ({tf})",
        f"Entry: {tr['initial_price']:.6f} | Avg: {tr['avg_entry']:.6f}",
        f"Duration: {duration}"
    ]

    # Show entries
    entries_str = []
    for e in tr['entries']:
        stage = "Initial" if e['stage'] == 0 else f"DCA{e['stage']}"
        entries_str.append(f"{stage}: {e['price']:.6f} (${e['margin']})")
    lines.append("Entries: " + " | ".join(entries_str))

    lines.append(f"TP: {tr['tp']:.6f} | SL: {SL_PCT*100:.1f}%")

    if is_final and hit_type:
        lines.append(f"**{hit_type} HIT** | Exit: {exit_price:.6f}")
        lines.append(f"PnL: {pnl_pct:.2f}% (${pnl_usdt:+.2f})")
    else:
        lines.append(f"DCA Stage: {tr['dca_stage']}/2")

    return "\n".join(lines)

# === MONITOR TP + DCA + SL (Single Message + Duration) ===
async def monitor_tp_and_dca():
    while True:
        try:
            async with trade_lock:
                open_symbols = list(open_trades.keys())
                if not open_symbols:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue
                
                prices = {}
                try:
                    tickers = await exchange.fetch_tickers(open_symbols)
                    for sym, t in tickers.items():
                        prices[sym] = t.get('last') or t.get('close') or t.get('markPrice')
                except Exception:
                    pass
                
                for sym in list(open_trades):
                    tr = open_trades[sym]

                    # Sync position
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
                        tr['total_amount'] = real_amount
                        tr['avg_entry'] = float(info.get('entryPrice', tr['avg_entry']))
                    except:
                        pass

                    current = prices.get(sym)
                    if not current:
                        try:
                            t = await exchange.fetch_ticker(sym)
                            current = t.get('last') or t.get('markPrice')
                        except:
                            continue

                    is_long = tr['side'] == 'buy'
                    initial_price = tr['initial_price']

                    # SL Check
                    sl_level = initial_price * (1 - SL_PCT) if is_long else initial_price * (1 + SL_PCT)
                    if (is_long and current <= sl_level) or (not is_long and current >= sl_level):
                        try:
                            close_side = 'sell' if is_long else 'buy'
                            close_order = await exchange.create_order(sym, 'market', close_side, tr['total_amount'], params={'reduceOnly': True})
                            exit_price = close_order.get('average') or current
                            exit_price = round_price(sym, exit_price)

                            pnl_pct = ((exit_price - tr['avg_entry']) / tr['avg_entry']) * 100 if is_long else ((tr['avg_entry'] - exit_price) / tr['avg_entry']) * 100
                            leveraged_pnl = pnl_pct * LEVERAGE
                            total_margin = sum(e['margin'] for e in tr['entries'])
                            profit_usdt = total_margin * (leveraged_pnl / 100)

                            final_msg = build_trade_message(tr, sym, is_final=True, hit_type="STOP-LOSS", exit_price=exit_price, pnl_usdt=profit_usdt, pnl_pct=leveraged_pnl)
                            await edit_telegram_message(tr['msg_id_initial'], final_msg)

                            await asyncio.to_thread(save_closed_trade, {
                                'symbol': sym, 'pnl_usdt': profit_usdt, 'pnl_pct': leveraged_pnl,
                                'hit': 'SL', 'avg_entry': tr['avg_entry'], 'exit': exit_price,
                                'dca_stage': tr['dca_stage'], 'total_margin': total_margin, 'ts': time.time()
                            })
                            del open_trades[sym]
                            await asyncio.to_thread(save_trades)
                            continue
                        except Exception as e:
                            logging.error(f"SL close failed {sym}: {e}")

                    # DCA Logic (Only edit message, no new message)
                    trigger1 = initial_price * (1 - DCA1_TRIGGER_PCT) if is_long else initial_price * (1 + DCA1_TRIGGER_PCT)
                    trigger2 = initial_price * (1 - DCA2_TRIGGER_PCT) if is_long else initial_price * (1 + DCA2_TRIGGER_PCT)

                    updated = False
                    if tr['dca_stage'] == 0 and ((is_long and current <= trigger1) or (not is_long and current >= trigger1)):
                        if sum(e['margin'] for e in tr['entries']) + CAPITAL_DCA1 <= MAX_MARGIN_PER_TRADE:
                            try:
                                amt_raw = (CAPITAL_DCA1 * LEVERAGE) / current
                                amt = float(round_amount(sym, amt_raw))
                                if amt > 0:
                                    order = await exchange.create_market_order(sym, tr['side'], amt)
                                    price = round_price(sym, order.get('average') or current)

                                    tr['entries'].append({'price': price, 'amount': amt, 'margin': CAPITAL_DCA1, 'ts': time.time(), 'stage': 1})
                                    tr['total_amount'] += amt
                                    tr['avg_entry'], _ = get_avg_entry_and_total(tr)
                                    tr['tp'] = round_price(sym, tr['avg_entry'] * (1 + TP_AFTER_DCA1_PCT) if is_long else tr['avg_entry'] * (1 - TP_AFTER_DCA1_PCT))
                                    tr['dca_stage'] = 1
                                    updated = True
                            except Exception as e:
                                logging.error(f"DCA1 failed {sym}: {e}")

                    elif tr['dca_stage'] == 1 and ((is_long and current <= trigger2) or (not is_long and current >= trigger2)):
                        if sum(e['margin'] for e in tr['entries']) + CAPITAL_DCA2 <= MAX_MARGIN_PER_TRADE:
                            try:
                                amt_raw = (CAPITAL_DCA2 * LEVERAGE) / current
                                amt = float(round_amount(sym, amt_raw))
                                if amt > 0:
                                    order = await exchange.create_market_order(sym, tr['side'], amt)
                                    price = round_price(sym, order.get('average') or current)

                                    tr['entries'].append({'price': price, 'amount': amt, 'margin': CAPITAL_DCA2, 'ts': time.time(), 'stage': 2})
                                    tr['total_amount'] += amt
                                    tr['avg_entry'], _ = get_avg_entry_and_total(tr)
                                    tr['tp'] = round_price(sym, tr['avg_entry'] * (1 + TP_AFTER_DCA2_PCT) if is_long else tr['avg_entry'] * (1 - TP_AFTER_DCA2_PCT))
                                    tr['dca_stage'] = 2
                                    updated = True
                            except Exception as e:
                                logging.error(f"DCA2 failed {sym}: {e}")

                    if updated:
                        new_msg = build_trade_message(tr, sym)
                        await edit_telegram_message(tr['msg_id_initial'], new_msg)
                        await asyncio.to_thread(save_trades)

                    # TP Check
                    hit_tp = (is_long and current >= tr['tp']) or (not is_long and current <= tr['tp'])
                    if hit_tp:
                        try:
                            close_side = 'sell' if is_long else 'buy'
                            close_order = await exchange.create_order(sym, 'market', close_side, tr['total_amount'], params={'reduceOnly': True})
                            exit_price = close_order.get('average') or current
                            exit_price = round_price(sym, exit_price)

                            pnl_pct = ((exit_price - tr['avg_entry']) / tr['avg_entry']) * 100 if is_long else ((tr['avg_entry'] - exit_price) / tr['avg_entry']) * 100
                            leveraged_pnl = pnl_pct * LEVERAGE
                            total_margin = sum(e['margin'] for e in tr['entries'])
                            profit_usdt = total_margin * (leveraged_pnl / 100)

                            final_msg = build_trade_message(tr, sym, is_final=True, hit_type="TP", exit_price=exit_price, pnl_usdt=profit_usdt, pnl_pct=leveraged_pnl)
                            await edit_telegram_message(tr['msg_id_initial'], final_msg)

                            await asyncio.to_thread(save_closed_trade, {
                                'symbol': sym, 'pnl_usdt': profit_usdt, 'pnl_pct': leveraged_pnl,
                                'hit': 'TP', 'avg_entry': tr['avg_entry'], 'exit': exit_price,
                                'dca_stage': tr['dca_stage'], 'total_margin': total_margin, 'ts': time.time()
                            })
                            del open_trades[sym]
                            await asyncio.to_thread(save_trades)
                        except Exception as e:
                            logging.error(f"TP close failed {sym}: {e}")

            await asyncio.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Monitor loop error: {e}")
            await asyncio.sleep(1)

# === PROCESS SYMBOL (Create Single Message with Timeframe) ===
async def process_symbol(symbol, timeframe):
    try:
        candles = await exchange.fetch_ohlcv(symbol, timeframe, limit=CANDLE_LIMIT)
        if len(candles) < 6:
            return
        signal_time = candles[-1][0]

        key_rising  = (symbol, timeframe, 'rising')
        key_falling = (symbol, timeframe, 'falling')

        async with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES:
                return
            if sent_signals.get(key_rising) == signal_time or sent_signals.get(key_falling) == signal_time:
                return

        pattern = None
        side = None
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
        if amount <= 0:
            return

        entry_order = await exchange.create_market_order(symbol, side, amount)
        filled_price = round_price(symbol, entry_order.get('average') or entry_price)

        tp = round_price(symbol, filled_price * (1 + TP_INITIAL_PCT) if side == 'buy' else filled_price * (1 - TP_INITIAL_PCT))

        # Create initial message
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
            'timeframe': timeframe   # Save timeframe for later use
        }

        msg_text = build_trade_message(initial_trade, symbol)
        mid = await send_telegram(msg_text)
        initial_trade['msg_id_initial'] = mid

        async with trade_lock:
            open_trades[symbol] = initial_trade
            await asyncio.to_thread(save_trades)

        logging.info(f"Opened {side} {symbol} @ {filled_price} on {timeframe}")

    except Exception as e:
        logging.error(f"Trade failed {symbol} on {timeframe}: {e}")

# === BATCH, SCAN, DAILY SUMMARY, MAIN (unchanged except small fixes) ===
async def process_batch(symbols_chunk, timeframe):
    tasks = [asyncio.create_task(process_symbol(s, timeframe)) for s in symbols_chunk]
    await asyncio.gather(*tasks, return_exceptions=True)

async def scan_loop(symbols):
    while True:
        wait_until = get_next_candle_close()
        sleep_sec = max(0, wait_until - time.time())
        logging.info(f"Next relevant candle close in ~{sleep_sec//60} min")
        await asyncio.sleep(sleep_sec)

        for tf in TIMEFRAMES:
            logging.info(f"Starting scan for timeframe: {tf}")
            chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
            chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]
            for i, chunk in enumerate(chunks):
                logging.info(f"Batch {i+1}/{len(chunks)} for {tf}")
                await process_batch(chunk, tf)
                if i < len(chunks)-1:
                    await asyncio.sleep(BATCH_DELAY)
            await asyncio.sleep(1.0)
        logging.info("Full multi-timeframe scan completed")

async def daily_summary():
    while True:
        await asyncio.sleep(86400)
        try:
            closed = []
            if os.path.exists(CLOSED_TRADE_FILE):
                with open(CLOSED_TRADE_FILE) as f:
                    closed = json.load(f)
            total_pnl = sum(t.get('pnl_usdt', 0) for t in closed)
            total_pct = sum(t.get('pnl_pct', 0) for t in closed)
            bal = await exchange.fetch_balance()
            usdt = bal.get('USDT', {})
            total = usdt.get('free', 0) + usdt.get('used', 0)
            msg = f"📊 *Daily Summary*\nAll-time PnL: $ {total_pnl:.2f} (${total_pct:.2f}%)\nOpen positions: {len(open_trades)}\nTotal USDT: ${total:.2f}"
            await send_telegram(msg)
        except Exception as e:
            logging.error(f"Daily summary error: {e}")

async def main():
    global exchange
    exchange = await initialize_exchange()
    markets = exchange.markets
    symbols = get_symbols(markets)
    load_trades()
    logging.info(f"Scanning {len(symbols)} USDT perpetuals")
    
    startup = (
        f"Bot restarted @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\n"
        f"Open positions: {len(open_trades)}\n"
        f"Max trades: {MAX_OPEN_TRADES} | Lev: {LEVERAGE}x\n"
        f"Initial: $15 → DCA1 $25 @ -1% → DCA2 $15 @ -2.5%\n"
        f"TP: 1.1% → 0.6% → 0.8%\n"
        f"SL: 6% fixed • Single updating message"
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