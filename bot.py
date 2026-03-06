import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import json
import os
import logging
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
MAX_WORKERS      = 5
BATCH_DELAY      = 2.0
NUM_CHUNKS       = 8
CAPITAL          = 20.0       # isolated margin per trade in USDT
LEVERAGE         = 5
TP_PCT           = 1.0 / 100
SL_PCT           = 3.0 / 100
TP_SL_CHECK_INTERVAL = 8      # seconds — fast enough for 1% targets
MAX_OPEN_TRADES  = 5
TRADE_FILE       = 'open_trades.json'
CLOSED_TRADE_FILE= 'closed_trades.json'

API_KEY    = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')
if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

PROXY_LIST = []  # add if needed

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

trade_lock = threading.Lock()

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

# === EXCHANGE ===
def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxies = {'http': f"http://{proxy.get('username')}:{proxy.get('password')}@{proxy['host']}:{proxy['port']}",
                       'https': f"http://{proxy.get('username')}:{proxy.get('password')}@{proxy['host']}:{proxy['port']}"}
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

    # fallback no proxy
    ex = ccxt.binance({
        'apiKey': API_KEY, 'secret': API_SECRET,
        'options': {'defaultType': 'future', 'marginMode': 'isolated'},
        'enableRateLimit': True,
    })
    ex.load_markets()
    logging.info("Connected directly")
    return ex

app = Flask(__name__)
exchange = initialize_exchange()

sent_signals = {}
open_trades = {}   # {symbol: {'side':, 'entry':, 'tp':, 'sl':, 'amount':, 'msg_id':, ...}}

# === CANDLE HELPERS ===
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
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s].get('swap') and markets[s].get('active', True)]

def prepare_symbol(symbol):
    try:
        exchange.set_margin_mode('isolated', symbol)
        exchange.set_leverage(LEVERAGE, symbol)
        logging.info(f"Prepared {symbol}: isolated + {LEVERAGE}x")
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

# === TP/SL MONITOR (internal) ===
def check_tp_sl():
    # Optional: try to use websocket for real-time prices
    prices = {}  # symbol → current mark price

    def update_prices():
        while True:
            try:
                for sym in list(open_trades.keys()):
                    ticker = exchange.watch_ticker(sym)  # or watch_mark_price if supported
                    prices[sym] = ticker.get('markPrice') or ticker['last']
            except Exception as e:
                logging.debug(f"WS error: {e}")
                time.sleep(2)

    threading.Thread(target=update_prices, daemon=True).start()

    while True:
        try:
            with trade_lock:
                for sym, tr in list(open_trades.items()):
                    try:
                        # Get fresh price (prefer WS cache → REST fallback)
                        current = prices.get(sym)
                        if not current:
                            pos_ticker = exchange.fetch_ticker(sym)
                            current = pos_ticker.get('markPrice') or pos_ticker['last']

                        hit_tp = (tr['side'] == 'buy' and current >= tr['tp']) or \
                                 (tr['side'] == 'sell' and current <= tr['tp'])
                        hit_sl = (tr['side'] == 'buy' and current <= tr['sl']) or \
                                 (tr['side'] == 'sell' and current >= tr['sl'])

                        if not (hit_tp or hit_sl):
                            continue

                        # Close position
                        close_side = 'sell' if tr['side'] == 'buy' else 'buy'
                        close_order = exchange.create_order(
                            sym, 'market', close_side, tr['amount'],
                            params={'reduceOnly': True}
                        )
                        exit_price = close_order.get('average') or current

                        # Calc pnl
                        if tr['side'] == 'buy':
                            pnl_pct = (exit_price - tr['entry']) / tr['entry'] * 100
                        else:
                            pnl_pct = (tr['entry'] - exit_price) / tr['entry'] * 100
                        leveraged_pnl = pnl_pct * LEVERAGE
                        profit_usdt = CAPITAL * leveraged_pnl / 100

                        hit = "✅ TP" if hit_tp else "❌ SL"
                        msg = (
                            f"{sym} — {hit} HIT\n"
                            f"Entry: {tr['entry']:.4f}\n"
                            f"Exit:  {exit_price:.4f}\n"
                            f"PnL:   {leveraged_pnl:.2f}%  (${profit_usdt:+.2f})\n"
                            f"{'TP' if hit_tp else 'SL'} price: {tr['tp' if hit_tp else 'sl']:.4f}"
                        )

                        edit_telegram_message(tr.get('msg_id'), msg)
                        save_closed_trade({
                            'symbol': sym, 'pnl_usdt': profit_usdt,
                            'pnl_pct': leveraged_pnl, 'hit': hit,
                            'entry': tr['entry'], 'exit': exit_price,
                            'ts': time.time()
                        })

                        del open_trades[sym]
                        save_trades()
                        logging.info(f"Closed {sym} — {hit} — PnL ${profit_usdt:.2f}")

                    except ccxt.OrderNotFillable as e:
                        logging.warning(f"Close failed (not fillable) {sym}: {e}")
                    except Exception as e:
                        logging.error(f"TP/SL close error {sym}: {e}")

            time.sleep(TP_SL_CHECK_INTERVAL)

        except Exception as e:
            logging.error(f"TP/SL main loop error: {e}")
            time.sleep(30)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=10)
        if len(candles) < 6:
            return

        signal_time = candles[-1][0]  # most recent closed candle

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

        # Prepare & trade
        prepare_symbol(symbol)

        ticker = exchange.fetch_ticker(symbol)
        entry_price = round_price(symbol, ticker['last'])

        notional = CAPITAL * LEVERAGE
        amount_raw = notional / entry_price
        amount = float(round_amount(symbol, amount_raw))

        if amount <= 0:
            logging.warning(f"Amount too small for {symbol}")
            return

        entry_order = exchange.create_market_order(symbol, side, amount)
        filled_price = entry_order.get('average') or entry_price
        filled_price = round_price(symbol, filled_price)

        if side == 'buy':
            tp = round_price(symbol, filled_price * (1 + TP_PCT))
            sl = round_price(symbol, filled_price * (1 - SL_PCT))
        else:
            tp = round_price(symbol, filled_price * (1 - TP_PCT))
            sl = round_price(symbol, filled_price * (1 + SL_PCT))

        entry_msg = (
            f"**ENTRY** {symbol} — {'LONG' if side=='buy' else 'SHORT'}\n"
            f"Entry: {filled_price:.4f}\n"
            f"Pattern: {pattern}\n"
            f"Size: {amount} ({LEVERAGE}x)\n"
            f"TP: {tp:.4f}  |  SL: {sl:.4f}\n"
            f"Monitoring internally..."
        )
        mid = send_telegram(entry_msg)

        with trade_lock:
            open_trades[symbol] = {
                'side': side,
                'entry': filled_price,
                'tp': tp,
                'sl': sl,
                'amount': amount,
                'msg_id': mid,
                'pattern': pattern,
                'open_ts': time.time()
            }
            save_trades()

        logging.info(f"Opened {side} {symbol} @ {filled_price} — TP/SL internal")

    except ccxt.InsufficientFunds:
        logging.error(f"Insufficient funds {symbol}")
    except Exception as e:
        logging.error(f"Trade failed {symbol}: {str(e)}")

# === BATCH ===
def process_batch(symbols_chunk, q):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_symbol, s, q) for s in symbols_chunk]
        for f in as_completed(futures):
            try: f.result()
            except: pass

# === SCAN LOOP ===
def scan_loop():
    load_trades()
    symbols = get_symbols()
    logging.info(f"Scanning {len(symbols)} USDT perpetuals")

    alert_q = queue.Queue()  # not really used now — kept for compatibility

    threading.Thread(target=check_tp_sl, daemon=True).start()

    while True:
        wait_until = get_next_candle_close()
        sleep_sec = max(0, wait_until - time.time())
        logging.info(f"Next 15m close in ~{sleep_sec//60} min")
        time.sleep(sleep_sec)

        chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
        chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]

        for i, chunk in enumerate(chunks):
            logging.info(f"Batch {i+1}/{len(chunks)}")
            process_batch(chunk, alert_q)
            if i < len(chunks)-1:
                time.sleep(BATCH_DELAY)

        logging.info("Full scan done")

# === DAILY SUMMARY (optional — kept) ===
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
                f"All-time PnL: ${total_pnl:.2f} ({total_pct:.2f}%)\n"
                f"Open: {len(open_trades)}\n"
                f"Total USDT: ${total:.2f}"
            )
            send_telegram(msg)
        except Exception as e:
            logging.error(f"Daily summary error: {e}")

# === FLASK + RUN ===
@app.route('/')
def home():
    return f"Bot running — {len(open_trades)} open trades | Max {MAX_OPEN_TRADES}"

def run_bot():
    load_trades()
    startup = (
        f"Bot (re)starrrrted @ {get_ist_time().strftime('%Y-%m-%d %H:%M')}\n"
        f"Open positions: {len(open_trades)}\n"
        f"Max trades: {MAX_OPEN_TRADES} | Lev: {LEVERAGE}x | ${CAPITAL}/trade\n"
        f"TP: {TP_PCT*100:.1f}% | SL: {SL_PCT*100:.1f}% — **internal monitoring**"
    )
    send_telegram(startup)

    threading.Thread(target=scan_loop, daemon=True).start()
    threading.Thread(target=daily_summary, daemon=True).start()

    app.run(host='0.0.0.0', port=8080, debug=False)

if __name__ == "__main__":
    run_bot()
