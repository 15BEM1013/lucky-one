import ccxt
import ccxt.async_support as ccxt_async
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
import math
import queue
import json
import os
import logging
import asyncio
from dotenv import load_dotenv

# Load .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# === CONFIG ===
BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID          = os.getenv("TELEGRAM_CHAT_ID")
TIMEFRAME        = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0

CAPITAL_INITIAL   = 10.0
CAPITAL_DCA       = 20.0
MAX_MARGIN_PER_TRADE = 30.0

LEVERAGE         = 5
TP_INITIAL_PCT   = 1.0 / 100
TP_AFTER_DCA_PCT = 0.5 / 100
DCA_TRIGGER_PCT  = 2.0 / 100

TP_CHECK_INTERVAL = 8

MAX_OPEN_TRADES  = 5
TRADE_FILE       = 'open_trades.json'
CLOSED_TRADE_FILE= 'closed_trades.json'

API_KEY    = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')
if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

PROXY_LIST = []

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

trade_lock = threading.Lock()

def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# === TRADE PERSISTENCE (unchanged) ===
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

# === TELEGRAM (unchanged) ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            'chat_id': CHAT_ID,
            'text': msg,
            'parse_mode': 'Markdown'
        }, timeout=10).json()
        return r.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

def edit_telegram_message(mid, new_text):
    if not mid:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    try:
        requests.post(url, data={
            'chat_id': CHAT_ID,
            'message_id': mid,
            'text': new_text,
            'parse_mode': 'Markdown'
        }, timeout=10)
    except Exception as e:
        logging.error(f"Telegram edit error: {e}")

# === FLASK APP ===
app = Flask(__name__)

# === EXCHANGE ===
def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxies = {
                'http': f"http://{proxy.get('username')}:{proxy.get('password')}@{proxy['host']}:{proxy['port']}",
                'https': f"http://{proxy.get('username')}:{proxy.get('password')}@{proxy['host']}:{proxy['port']}"
            }
            ex = ccxt.binance({
                'apiKey': API_KEY, 'secret': API_SECRET,
                'options': {'defaultType': 'future', 'marginMode': 'isolated'},
                'proxies': proxies, 'enableRateLimit': True,
            })
            ex.load_markets()
            logging.info("Connected via proxy")
            return ex
        except Exception as e:
            logging.warning(f"Proxy failed: {e}")

    ex = ccxt.binance({
        'apiKey': API_KEY, 'secret': API_SECRET,
        'options': {'defaultType': 'future', 'marginMode': 'isolated'},
        'enableRateLimit': True,
    })
    ex.load_markets()
    logging.info("Connected directly")
    return ex

exchange = initialize_exchange()

# Async exchange for fast scanning
async_exchange = None

async def init_async_exchange():
    global async_exchange
    async_exchange = ccxt_async.binance({
        'apiKey': API_KEY,
        'secret': API_SECRET,
        'options': {'defaultType': 'future', 'marginMode': 'isolated'},
        'enableRateLimit': True,
        'rateLimit': 100,  # ms
    })
    await async_exchange.load_markets()
    logging.info("Async exchange ready")

sent_signals = {}
open_trades = {}

# === CANDLE & PATTERN HELPERS (unchanged) ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0

def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

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
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active', True)]

def prepare_symbol(symbol):
    try:
        exchange.set_margin_mode('isolated', symbol)
        exchange.set_leverage(LEVERAGE, symbol)
        # logging.info(f"Prepared {symbol}")  # comment if too noisy
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

# === NEXT CANDLE ===
def get_next_candle_close():
    now = get_ist_time()
    secs = now.minute * 60 + now.second
    secs_to = (15 * 60) - (secs % (15 * 60))
    if secs_to < 10:
        secs_to += 15 * 60
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

# === MONITOR TP + DCA (unchanged – websocket focused) ===
def monitor_tp_and_dca():
    prices_cache = {}
    last_fresh_time = {}

    def update_prices_ws():
        while True:
            try:
                active_symbols = list(open_trades.keys())
                if not active_symbols:
                    time.sleep(12)
                    continue
                tickers = exchange.watch_tickers(active_symbols)
                now = time.time()
                for sym, ticker in tickers.items():
                    price = ticker.get('last') or ticker.get('close') or ticker.get('markPrice')
                    if price:
                        prices_cache[sym] = price
                        last_fresh_time[sym] = now
            except Exception as e:
                logging.debug(f"WS error: {e}")
                time.sleep(5)

    threading.Thread(target=update_prices_ws, daemon=True).start()

    while True:
        try:
            with trade_lock:
                for sym, tr in list(open_trades.items()):
                    current = prices_cache.get(sym)
                    last_time = last_fresh_time.get(sym, 0)

                    if current is None or (time.time() - last_time) > 12:
                        try:
                            t = exchange.fetch_ticker(sym)
                            current = t.get('last') or t.get('markPrice')
                            prices_cache[sym] = current
                            last_fresh_time[sym] = time.time()
                        except:
                            continue

                    if current is None:
                        continue

                    is_long = tr['side'] == 'buy'

                    if not tr['dca_done']:
                        trigger_level = tr['avg_entry'] * (1 - DCA_TRIGGER_PCT) if is_long else tr['avg_entry'] * (1 + DCA_TRIGGER_PCT)
                        if (is_long and current <= trigger_level) or (not is_long and current >= trigger_level):
                            try:
                                used_margin = sum(e['margin'] for e in tr['entries'])
                                if used_margin + CAPITAL_DCA > MAX_MARGIN_PER_TRADE:
                                    continue

                                dca_amount_raw = (CAPITAL_DCA * LEVERAGE) / current
                                dca_amount = float(round_amount(sym, dca_amount_raw))
                                if dca_amount <= 0:
                                    continue

                                dca_order = exchange.create_market_order(sym, tr['side'], dca_amount)
                                dca_price = dca_order.get('average') or current
                                dca_price = round_price(sym, dca_price)

                                tr['entries'].append({
                                    'price': dca_price,
                                    'amount': dca_amount,
                                    'margin': CAPITAL_DCA,
                                    'ts': time.time()
                                })
                                tr['total_amount'] += dca_amount
                                tr['avg_entry'], _ = get_avg_entry_and_total(tr)
                                tr['dca_done'] = True

                                tr['tp'] = round_price(sym, tr['avg_entry'] * (1 + TP_AFTER_DCA_PCT) if is_long else tr['avg_entry'] * (1 - TP_AFTER_DCA_PCT))

                                dca_msg = (
                                    f"**DCA TRIGGERED** {sym}\n"
                                    f"DCA entry: {dca_price:.6f}\n"
                                    f"Added: {dca_amount:.4f} (${CAPITAL_DCA:.0f})\n"
                                    f"New avg: {tr['avg_entry']:.6f}\n"
                                    f"New TP: {tr['tp']:.6f} ({TP_AFTER_DCA_PCT*100:.1f}% from avg)"
                                )
                                mid_dca = send_telegram(dca_msg)
                                tr['msg_id_dca'] = mid_dca

                                save_trades()
                                logging.info(f"DCA added for {sym} @ {dca_price}")

                            except Exception as e:
                                logging.error(f"DCA failed for {sym}: {e}")

                    hit_tp = (is_long and current >= tr['tp']) or (not is_long and current <= tr['tp'])
                    if hit_tp:
                        try:
                            close_side = 'sell' if is_long else 'buy'
                            close_order = exchange.create_order(
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
                                f"**TP HIT** {sym} — {'LONG' if is_long else 'SHORT'}\n"
                                f"Avg entry: {tr['avg_entry']:.6f}\n"
                                f"Exit: {exit_price:.6f}\n"
                                f"Total size: {tr['total_amount']:.4f}\n"
                                f"PnL: {leveraged_pnl:.2f}% (${profit_usdt:+.2f})\n"
                                f"{'DCA used' if tr['dca_done'] else 'No DCA'}"
                            )
                            edit_telegram_message(tr['msg_id_initial'], msg)
                            if tr.get('msg_id_dca'):
                                edit_telegram_message(tr['msg_id_dca'], "Position closed on TP ↑")

                            save_closed_trade({
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
                            save_trades()
                            logging.info(f"Closed {sym} on TP — PnL ${profit_usdt:.2f}")

                        except Exception as e:
                            logging.error(f"TP close failed {sym}: {e}")

            time.sleep(TP_CHECK_INTERVAL)

        except Exception as e:
            logging.error(f"Monitor loop error: {e}")
            time.sleep(30)

# === ASYNC PROCESS SYMBOL ===
async def process_symbol_async(symbol):
    try:
        candles = await async_exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=5)
        if len(candles) < 5:
            return

        signal_time = candles[-1][0]

        with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES:
                return
            if sent_signals.get((symbol, 'rising')) == signal_time or \
               sent_signals.get((symbol, 'falling')) == signal_time:
                return

        pattern = None
        side = None
        if detect_rising_three(candles):
            pattern, side = 'rising three', 'buy'
            sent_signals[(symbol, 'rising')] = signal_time
        elif detect_falling_three(candles):
            pattern, side = 'falling three', 'sell'
            sent_signals[(symbol, 'falling')] = signal_time
        else:
            return

        prepare_symbol(symbol)

        # For entry price we use sync exchange (small overhead)
        ticker = exchange.fetch_ticker(symbol)
        entry_price = round_price(symbol, ticker['last'])

        amount_raw = (CAPITAL_INITIAL * LEVERAGE) / entry_price
        amount = float(round_amount(symbol, amount_raw))

        if amount <= 0:
            return

        entry_order = exchange.create_market_order(symbol, side, amount)
        filled_price = entry_order.get('average') or entry_price
        filled_price = round_price(symbol, filled_price)

        tp = round_price(symbol, filled_price * (1 + TP_INITIAL_PCT) if side == 'buy' else filled_price * (1 - TP_INITIAL_PCT))

        entry_msg = (
            f"**ENTRY** {symbol} — {'LONG' if side=='buy' else 'SHORT'}\n"
            f"Entry: {filled_price:.6f}\n"
            f"Pattern: {pattern}\n"
            f"Size: {amount:.4f} (${CAPITAL_INITIAL:.0f})\n"
            f"TP: {tp:.6f} ({TP_INITIAL_PCT*100:.1f}%)\n"
            f"No SL • Monitoring DCA/TP internally"
        )
        mid = send_telegram(entry_msg)

        with trade_lock:
            open_trades[symbol] = {
                'side': side,
                'entries': [{
                    'price': filled_price,
                    'amount': amount,
                    'margin': CAPITAL_INITIAL,
                    'ts': time.time()
                }],
                'total_amount': amount,
                'avg_entry': filled_price,
                'tp': tp,
                'dca_done': False,
                'msg_id_initial': mid,
                'msg_id_dca': None,
                'open_ts': time.time()
            }
            save_trades()

        logging.info(f"Opened {side} {symbol} @ {filled_price}")

    except Exception as e:
        logging.error(f"Async trade failed {symbol}: {str(e)}")

# === ASYNC SCAN ===
async def async_scan():
    scan_start = time.time()

    symbols = get_symbols()

    # Bulk tickers (1 call)
    try:
        all_tickers = await async_exchange.fetch_tickers()
    except Exception as e:
        logging.error(f"Bulk tickers failed: {e}")
        all_tickers = {}

    promising = []
    for sym in symbols:
        t = all_tickers.get(sym, {})
        if t.get('quoteVolume', 0) > 20000 or abs(t.get('percentage', 0)) > 0.3:
            promising.append(sym)

    logging.info(f"Processing {len(promising)} active symbols out of {len(symbols)}")

    # Concurrent OHLCV fetches
    tasks = [process_symbol_async(sym) for sym in promising]

    # Run in batches of 50 to stay under rate limits
    for i in range(0, len(tasks), 50):
        batch = tasks[i:i+50]
        await asyncio.gather(*batch, return_exceptions=True)
        if i + 50 < len(tasks):
            await asyncio.sleep(1.5)  # gentle pause

    scan_duration = time.time() - scan_start
    logging.info(f"Full async scan completed in {scan_duration:.1f} seconds")

# === SCAN LOOP ===
def scan_loop():
    load_trades()

    alert_q = queue.Queue()

    threading.Thread(target=monitor_tp_and_dca, daemon=True).start()

    while True:
        wait_until = get_next_candle_close()
        sleep_sec = max(0, wait_until - time.time())
        logging.info(f"Next 15m close in ~{sleep_sec//60} min")
        time.sleep(sleep_sec)

        asyncio.run(async_scan())

# === DAILY SUMMARY ===
def daily_summary():
    while True:
        time.sleep(86400)
        try:
            closed = []
            if os.path.exists(CLOSED_TRADE_FILE):
                with open(CLOSED_TRADE_FILE) as f:
                    closed = json.load(f)
            total_pnl = sum(t.get('pnl_usdt', 0) for t in closed)
            total_pct = sum(t.get('pnl_pct', 0) for t in closed)

            bal = exchange.fetch_balance()
            usdt = bal.get('USDT', {})
            total = usdt.get('free', 0) + usdt.get('used', 0)

            msg = (
                f"📊 *Daily Summary*\n"
                f"All-time PnL: ${total_pnl:.2f} (${total_pct:.2f}%)\n"
                f"Open positions: {len(open_trades)}\n"
                f"Total USDT: ${total:.2f}"
            )
            send_telegram(msg)
        except Exception as e:
            logging.error(f"Daily summary error: {e}")

# === FLASK ===
@app.route('/')
def home():
    return f"Bot running — {len(open_trades)} open trades | Max {MAX_OPEN_TRADES}"

def run_bot():
    load_trades()

    # Start async exchange
    asyncio.run(init_async_exchange())

    startup = (
        f"Bot restarted @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\n"
        f"Open positions: {len(open_trades)}\n"
        f"Max trades: {MAX_OPEN_TRADES} | Lev: {LEVERAGE}x\n"
        f"Initial: ${CAPITAL_INITIAL} → DCA +${CAPITAL_DCA} (max ${MAX_MARGIN_PER_TRADE})\n"
        f"TP: {TP_INITIAL_PCT*100:.1f}% → {TP_AFTER_DCA_PCT*100:.1f}% after DCA • **No SL**"
    )
    send_telegram(startup)

    threading.Thread(target=scan_loop, daemon=True).start()
    threading.Thread(target=daily_summary, daemon=True).start()

    app.run(host='0.0.0.0', port=8080, debug=False)

if __name__ == "__main__":
    run_bot()
