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
TIMEFRAMES = ['5m', '30m']                  # scan both timeframes
CANDLE_LIMIT = 6
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL_INITIAL = 15.0      # first entry margin USDT
CAPITAL_DCA = 30.0          # DCA add margin
MAX_MARGIN_PER_TRADE = 45.0
LEVERAGE = 7                # ← changed to 7x
TP_INITIAL_PCT = 1.1 / 100
TP_AFTER_DCA_PCT = 0.6 / 100
DCA_TRIGGER_PCT = 2.0 / 100
TP_CHECK_INTERVAL = 2       # ← changed to 2 seconds
MAX_OPEN_TRADES = 5
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET') 

if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set") 

PROXY_LIST = []  # format: [{"host": "...", "port": "...", "username": "...", "password": "..."}] 

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

# === TELEGRAM (async) ===
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
sent_signals = {}  # (symbol, timeframe, 'rising'/'falling') → signal_time
open_trades = {}   # {symbol: { 'side', 'entries':[...], 'avg_entry', 'total_amount', 'tp', 'dca_done', ... }} 

# === CANDLE HELPERS ===
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

# === NEXT CANDLE ===
def get_next_candle_close():
    now = get_ist_time()
    secs = now.minute * 60 + now.second
    secs_to = (5 * 60) - (secs % (5 * 60))
    if secs_to < 10:
        secs_to += 5 * 60
    return time.time() + secs_to 

# === HELPERS ===
def get_avg_entry_and_total(trade):
    total_pos = 0.0
    weighted = 0.0
    for e in trade['entries']:
        weighted += e['price'] * e['amount']
        total_pos += e['amount']
    if total_pos == 0:
        return 0.0, 0.0
    return weighted / total_pos, total_pos 

# === MONITOR TP + DCA (async, no WS) ===
async def monitor_tp_and_dca():
    while True:
        try:
            async with trade_lock:
                open_symbols = list(open_trades.keys())
                if not open_symbols:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue
                # Batch fetch prices
                prices = {}
                try:
                    tickers = await exchange.fetch_tickers(open_symbols)
                    for sym, t in tickers.items():
                        prices[sym] = t.get('last') or t.get('close') or t.get('markPrice')
                except Exception as e:
                    logging.warning(f"Batch tickers error: {e}")
                
                for sym in list(open_trades):
                    tr = open_trades[sym]

                    # Sync real position size & avg entry (handles manual adds/reduces)
                    try:
                        pos = await exchange.fetch_position(sym)
                        if not pos or not pos.get('info'):
                            continue

                        info = pos['info']
                        real_amt_str = info.get('positionAmt', '0')
                        real_amount = abs(float(real_amt_str))

                        if real_amount <= 0:
                            await send_telegram(f"🗑️ Position on {sym} closed externally (manual / liquidation?)")
                            del open_trades[sym]
                            await asyncio.to_thread(save_trades)
                            continue

                        real_avg = float(info.get('entryPrice', tr['avg_entry']))

                        # Detect & notify manual change
                        if abs(real_amount - tr['total_amount']) > 0.0001:
                            diff = real_amount - tr['total_amount']
                            direction = "ADDED" if diff > 0 else "REDUCED"
                            change_msg = (
                                f"🔄 **MANUAL POSITION CHANGE** on {sym}\n"
                                f"Direction: {direction}\n"
                                f"Change: {abs(diff):.4f}\n"
                                f"Bot size was: {tr['total_amount']:.4f}\n"
                                f"New real size: {real_amount:.4f}\n"
                                f"New avg entry: {real_avg:.6f}\n"
                                f"Bot will now use real values for DCA/TP/close"
                            )
                            await send_telegram(change_msg)
                            logging.info(f"Manual change on {sym}: {direction} {abs(diff):.4f}")

                        # Update internal state to match reality
                        tr['total_amount'] = real_amount
                        tr['avg_entry'] = real_avg

                    except Exception as e:
                        logging.debug(f"Position sync failed for {sym}: {e}")

                    current = prices.get(sym)
                    if not current:
                        try:
                            t = await exchange.fetch_ticker(sym)
                            current = t.get('last') or t.get('markPrice')
                        except:
                            continue

                    is_long = tr['side'] == 'buy'

                    # 1. Check for DCA (only once)
                    if not tr['dca_done']:
                        trigger_level = tr['avg_entry'] * (1 - DCA_TRIGGER_PCT) if is_long else tr['avg_entry'] * (1 + DCA_TRIGGER_PCT)
                        if (is_long and current <= trigger_level) or (not is_long and current >= trigger_level):
                            try:
                                used_margin = sum(e['margin'] for e in tr['entries'])
                                if used_margin + CAPITAL_DCA > MAX_MARGIN_PER_TRADE:
                                    logging.warning(f"{sym} DCA skipped - margin limit reached")
                                    continue
                                dca_amount_raw = (CAPITAL_DCA * LEVERAGE) / current
                                dca_amount = float(round_amount(sym, dca_amount_raw))
                                if dca_amount <= 0:
                                    continue
                                dca_order = await exchange.create_market_order(sym, tr['side'], dca_amount)
                                dca_price = dca_order.get('average') or current
                                dca_price = round_price(sym, dca_price)
                                # Update trade
                                tr['entries'].append({
                                    'price': dca_price,
                                    'amount': dca_amount,
                                    'margin': CAPITAL_DCA,
                                    'ts': time.time()
                                })
                                tr['total_amount'] += dca_amount
                                tr['avg_entry'], _ = get_avg_entry_and_total(tr)
                                tr['dca_done'] = True
                                # Update TP to 0.6%
                                tr['tp'] = round_price(sym, tr['avg_entry'] * (1 + TP_AFTER_DCA_PCT) if is_long else tr['avg_entry'] * (1 - TP_AFTER_DCA_PCT))
                                dca_msg = (
                                    f"**DCA TRIGGERED** {sym}\n"
                                    f"DCA entry: {dca_price:.6f}\n"
                                    f"Added: {dca_amount:.4f} (${CAPITAL_DCA:.0f})\n"
                                    f"New avg: {tr['avg_entry']:.6f}\n"
                                    f"New TP: {tr['tp']:.6f} ({TP_AFTER_DCA_PCT*100:.1f}% from avg)"
                                )
                                mid_dca = await send_telegram(dca_msg)
                                tr['msg_id_dca'] = mid_dca
                                await asyncio.to_thread(save_trades)
                                logging.info(f"DCA added for {sym} @ {dca_price}")
                            except Exception as e:
                                logging.error(f"DCA failed for {sym}: {e}")

                    # 2. Check TP
                    hit_tp = (is_long and current >= tr['tp']) or (not is_long and current <= tr['tp'])
                    if hit_tp:
                        try:
                            close_side = 'sell' if is_long else 'buy'
                            close_order = await exchange.create_order(
                                sym, 'market', close_side, tr['total_amount'],
                                params={'reduceOnly': True}
                            )
                            exit_price = close_order.get('average') or current
                            exit_price = round_price(sym, exit_price)

                            pnl_pct = ((exit_price - tr['avg_entry']) / tr['avg_entry']) * 100 if is_long else \
                                      ((tr['avg_entry'] - exit_price) / tr['avg_entry']) * 100
                            leveraged_pnl = pnl_pct * LEVERAGE
                            total_margin_used = sum(e['margin'] for e in tr['entries'])
                            profit_usdt = total_margin_used * (leveraged_pnl / 100)

                            msg = (
                                f"**TP HIT** {sym} — {'LONG' if is_long else 'SHORT'} **(FULL REAL POSITION)**\n"
                                f"Avg entry: {tr['avg_entry']:.6f}\n"
                                f"Exit: {exit_price:.6f}\n"
                                f"Closed size: {tr['total_amount']:.4f} (includes any manual adds)\n"
                                f"PnL: {leveraged_pnl:.2f}% (${profit_usdt:+.2f})\n"
                                f"{'DCA used' if tr['dca_done'] else 'No DCA'}"
                            )
                            await edit_telegram_message(tr['msg_id_initial'], msg)
                            if tr.get('msg_id_dca'):
                                await edit_telegram_message(tr['msg_id_dca'], "Position closed on TP ↑")

                            await asyncio.to_thread(save_closed_trade, {
                                'symbol': sym,
                                'pnl_usdt': profit_usdt,
                                'pnl_pct': leveraged_pnl,
                                'hit': 'TP',
                                'avg_entry': tr['avg_entry'],
                                'exit': exit_price,
                                'dca_used': tr['dca_done'],
                                'total_margin': total_margin_used,
                                'ts': time.time()
                            })
                            del open_trades[sym]
                            await asyncio.to_thread(save_trades)
                            logging.info(f"Closed {sym} on TP — PnL ${profit_usdt:.2f}")
                        except Exception as e:
                            logging.error(f"TP close failed {sym}: {e}")

            await asyncio.sleep(TP_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Monitor loop error: {e}")
            await asyncio.sleep(30) 

# === PROCESS SYMBOL (async) ===
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
            if sent_signals.get(key_rising) == signal_time or \
               sent_signals.get(key_falling) == signal_time:
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
            logging.warning(f"Amount too small for {symbol} on {timeframe}")
            return

        entry_order = await exchange.create_market_order(symbol, side, amount)
        filled_price = entry_order.get('average') or entry_price
        filled_price = round_price(symbol, filled_price)

        tp = round_price(symbol, filled_price * (1 + TP_INITIAL_PCT) if side == 'buy' else filled_price * (1 - TP_INITIAL_PCT))

        entry_msg = (
            f"**ENTRY** {symbol} — {'LONG' if side=='buy' else 'SHORT'} ({timeframe.upper()})\n"
            f"Entry: {filled_price:.6f}\n"
            f"Pattern: {pattern}\n"
            f"Size: {amount:.4f} (${CAPITAL_INITIAL:.0f})\n"
            f"TP: {tp:.6f} ({TP_INITIAL_PCT*100:.1f}%)\n"
            f"No SL • Monitoring DCA/TP internally"
        )
        mid = await send_telegram(entry_msg)

        async with trade_lock:
            open_trades[symbol] = {
                'side': side,
                'entries': [{
                    'price': filled_price,
                    'amount': amount,
                    'margin': CAPITAL_INITIAL,
                    'ts': time.time(),
                    'timeframe': timeframe
                }],
                'total_amount': amount,
                'avg_entry': filled_price,
                'tp': tp,
                'dca_done': False,
                'msg_id_initial': mid,
                'msg_id_dca': None,
                'open_ts': time.time()
            }
            await asyncio.to_thread(save_trades)

        logging.info(f"Opened {side} {symbol} @ {filled_price} on {timeframe}")

    except ccxt.InsufficientFunds:
        logging.error(f"Insufficient funds {symbol} on {timeframe}")
    except Exception as e:
        logging.error(f"Trade failed {symbol} on {timeframe}: {str(e)}") 

# === BATCH (async) ===
async def process_batch(symbols_chunk, timeframe):
    tasks = [asyncio.create_task(process_symbol(s, timeframe)) for s in symbols_chunk]
    await asyncio.gather(*tasks, return_exceptions=True) 

# === SCAN LOOP (async) ===
async def scan_loop(symbols):
    while True:
        wait_until = get_next_candle_close()  # reference timing based on 5m
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
            await asyncio.sleep(1.0)  # small delay between timeframes

        logging.info("Full multi-timeframe scan completed") 

# === DAILY SUMMARY (async) ===
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
            msg = (
                f"📊 *Daily Summary*\n"
                f"All-time PnL: ${total_pnl:.2f} (${total_pct:.2f}%)\n"
                f"Open positions: {len(open_trades)}\n"
                f"Total USDT: ${total:.2f}"
            )
            await send_telegram(msg)
        except Exception as e:
            logging.error(f"Daily summary error: {e}") 

# === MAIN ===
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
        f"Initial: ${CAPITAL_INITIAL} → DCA +${CAPITAL_DCA} (max ${MAX_MARGIN_PER_TRADE})\n"
        f"TP: {TP_INITIAL_PCT*100:.1f}% → {TP_AFTER_DCA_PCT*100:.1f}% after DCA • **No SL**\n"
        f"Timeframes: 5m + 30m • Check interval: {TP_CHECK_INTERVAL}s • Manual add detection active"
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