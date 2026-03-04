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
import os
load_dotenv()
# === CONFIG ===
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 0.1
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 20.0          # isolated margin per trade in USDT
LEVERAGE = 5
TP_PCT = 1.0 / 100
SL_PCT = 3.0 / 100
TP_SL_CHECK_INTERVAL = 60
MAX_OPEN_TRADES = 5
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

# === API KEYS FROM ENVIRONMENT (safer on VPS) ===
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')
if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set as environment variables")

# === PROXY CONFIGURATION (leave empty if not using) ===
PROXY_LIST = [
    # {'host': '', 'port': '', 'username': '', 'password': ''},
]

def get_proxy_config(proxy):
    if not proxy:
        return None
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === THREAD LOCK ===
trade_lock = threading.Lock()

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
        logging.info(f"Trades saved to {TRADE_FILE}")
    except Exception as e:
        logging.error(f"Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                loaded = json.load(f)
                open_trades = {k: v for k, v in loaded.items()}
            logging.info(f"Loaded {len(open_trades)} trades from {TRADE_FILE}")
    except Exception as e:
        logging.error(f"Error loading trades: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                all_closed = json.load(f)
        all_closed.append(closed_trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(all_closed, f, default=str)
        logging.info(f"Closed trade saved")
    except Exception as e:
        logging.error(f"Error saving closed trades: {e}")

def load_closed_trades():
    try:
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        logging.error(f"Error loading closed trades: {e}")
        return []

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    try:
        response = requests.post(url, data=data, timeout=10).json()
        logging.info(f"Telegram sent: {msg[:100]}...")
        return response.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text, 'parse_mode': 'Markdown'}
    try:
        requests.post(url, data=data, timeout=10)
        logging.info(f"Telegram updated")
    except Exception as e:
        logging.error(f"Edit error: {e}")

# === EXCHANGE INIT ===
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    proxies = None
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            logging.info(f"Trying proxy: {proxy.get('host','')}")
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            exchange = ccxt.binance({
                'apiKey': API_KEY,
                'secret': API_SECRET,
                'options': {'defaultType': 'future', 'marginMode': 'isolated'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
            })
            exchange.load_markets()
            logging.info("Connected via proxy")
            return exchange, proxies
        except Exception as e:
            logging.error(f"Proxy failed: {e}")
    # Fallback direct
    try:
        exchange = ccxt.binance({
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'options': {'defaultType': 'future', 'marginMode': 'isolated'},
            'enableRateLimit': True
        })
        exchange.load_markets()
        logging.info("Connected directly")
        return exchange, None
    except Exception as e:
        logging.error(f"Direct connection failed: {e}")
        raise

app = Flask(__name__)
sent_signals = {}
open_trades = {}
closed_trades = []

try:
    exchange, proxies = initialize_exchange()
except Exception as e:
    logging.error(f"Exchange init failed: {e}")
    exit(1)

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0
def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

# === PRICE ROUNDING ===
def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][0]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except:
        return price

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = is_bearish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    small_red_0 = is_bearish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = is_bullish(c1) and body_pct(c1) < MAX_SMALL_BODY_PCT and c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    small_green_0 = is_bullish(c0) and body_pct(c0) < MAX_SMALL_BODY_PCT and c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    return big_red and small_green_1 and small_green_0

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']

# === PREPARE SYMBOL ===
def prepare_symbol_for_trade(symbol):
    try:
        exchange.set_margin_mode('isolated', symbol)
        exchange.set_leverage(LEVERAGE, symbol)
        logging.info(f"Prepared {symbol}: isolated + {LEVERAGE}x")
    except Exception as e:
        logging.error(f"Prepare failed {symbol}: {e}")
        raise

# === CANDLE CLOSE TIMER ===
def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === TP/SL MONITOR ===
def check_tp_sl():
    while True:
        try:
            with trade_lock:
                for sym, trade in list(open_trades.items()):
                    try:
                        positions = exchange.fetch_positions([sym])
                        pos = next((p for p in positions if p['symbol'] == sym), None)
                        if pos is None or float(pos.get('contracts', 0)) <= 0:
                            trades = exchange.fetch_my_trades(sym, limit=5)
                            if trades:
                                last_trade = trades[-1]
                                exit_price = last_trade['price']
                                if trade['side'] == 'buy':
                                    pnl_pct = (exit_price - trade['entry']) / trade['entry'] * 100
                                    hit = "✅ TP hit" if exit_price >= trade['tp'] - 0.0001 else "❌ SL hit"
                                else:
                                    pnl_pct = (trade['entry'] - exit_price) / trade['entry'] * 100
                                    hit = "✅ TP hit" if exit_price <= trade['tp'] + 0.0001 else "❌ SL hit"
                                
                                leveraged_pnl = pnl_pct * LEVERAGE
                                profit = CAPITAL * leveraged_pnl / 100
                                
                                new_msg = (
                                    f"{sym} - {'REVERSED SELL' if trade['side']=='sell' else 'REVERSED BUY'}\n"
                                    f"entry - {trade['entry']}\n"
                                    f"tp - {trade['tp']}\n"
                                    f"sl - {trade['sl']}\n"
                                    f"P/L: {leveraged_pnl:.2f}% (${profit:.2f})\n{hit}"
                                )
                                edit_telegram_message(trade['msg_id'], new_msg)
                                
                                closed = {'symbol': sym, 'pnl': profit, 'pnl_pct': leveraged_pnl, 'hit': hit}
                                save_closed_trades(closed)
                                del open_trades[sym]
                                save_trades()
                                logging.info(f"Closed {sym}: {hit}")
                    except Exception as e:
                        logging.error(f"Check error {sym}: {e}")
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"TP/SL loop error: {e}")
            time.sleep(60)

# === DAILY SUMMARY ===
def daily_summary():
    while True:
        time.sleep(86400)
        try:
            closed = load_closed_trades()
            total_pnl = sum(t['pnl'] for t in closed)
            total_pnl_pct = sum(t['pnl_pct'] for t in closed)
            
            balance = exchange.fetch_balance()
            usdt = balance.get('USDT', {})
            free = usdt.get('free', 0)
            used = usdt.get('used', 0)
            total = free + used
            
            msg = (
                f"📊 *Daily Summary*\n"
                f"Total P/L (all-time): ${total_pnl:.2f} ({total_pnl_pct:.2f}%)\n"
                f"Open trades: {len(open_trades)}\n"
                f"Total USDT: ${total:.2f}\n"
                f"Free USDT: ${free:.2f}\n"
                f"Used in positions: ${used:.2f}"
            )
            send_telegram(msg)
            logging.info("Daily summary sent")
        except Exception as e:
            logging.error(f"Daily summary failed: {e}")

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=10)
        if len(candles) < 6:
            return
        
        signal_time = candles[-2][0]
        
        with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES:
                logging.info(f"Max {MAX_OPEN_TRADES} trades — skipping {symbol}")
                return
        
        if detect_rising_three(candles):
            if sent_signals.get((symbol, 'rising')) == signal_time:
                return
            sent_signals[(symbol, 'rising')] = signal_time
            side = 'buy'
            pattern = 'rising'
        elif detect_falling_three(candles):
            if sent_signals.get((symbol, 'falling')) == signal_time:
                return
            sent_signals[(symbol, 'falling')] = signal_time
            side = 'sell'
            pattern = 'falling'
        else:
            return
        
        # === LIVE TRADE ===
        try:
            prepare_symbol_for_trade(symbol)
            
            ticker = exchange.fetch_ticker(symbol)
            entry_price = round_price(symbol, ticker['last'])
            
            notional = CAPITAL * LEVERAGE
            amount = exchange.amount_to_precision(symbol, notional / entry_price)
            
            entry_order = exchange.create_market_order(symbol, side, amount)
            filled_entry = entry_order.get('average') or entry_price
            filled_entry = round_price(symbol, filled_entry)
            
            if side == 'buy':
                tp = round_price(symbol, filled_entry * (1 + TP_PCT))
                sl = round_price(symbol, filled_entry * (1 - SL_PCT))
            else:
                tp = round_price(symbol, filled_entry * (1 - TP_PCT))
                sl = round_price(symbol, filled_entry * (1 + SL_PCT))
            
            close_side = 'sell' if side == 'buy' else 'buy'
            
            # TP order
            exchange.create_order(
                symbol, 'market', close_side, amount, None,
                params={'reduceOnly': True, 'stopPrice': tp, 'type': 'TAKE_PROFIT_MARKET'}
            )
            # SL order
            exchange.create_order(
                symbol, 'market', close_side, amount, None,
                params={'reduceOnly': True, 'stopPrice': sl, 'type': 'STOP_MARKET'}
            )
            
            tp_dist = abs(tp - filled_entry) / filled_entry * 100
            
            msg = (
                f"**LIVE TRADE OPENED** {symbol} - {'REVERSED BUY' if side=='buy' else 'REVERSED SELL'}\n"
                f"Entry (market): {filled_entry}\n"
                f"TP: {tp} ({tp_dist:.2f}%)\n"
                f"SL: {sl}\n"
                f"Size: {amount}  (5x, margin ${CAPITAL})\n"
                f"Pattern detected."
            )
            
            alert_queue.put((symbol, msg, side, filled_entry, tp, sl, pattern))
            logging.info(f"Opened {side} {symbol} @ {filled_entry}")
            
        except ccxt.InsufficientFunds:
            logging.error(f"Insufficient funds for {symbol}")
        except Exception as e:
            logging.error(f"Trade failed {symbol}: {str(e)}")
            
    except Exception as e:
        logging.error(f"Process error {symbol}: {e}")

# === BATCH PROCESSING ===
def process_batch(symbols, alert_queue):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol, s, alert_queue): s for s in symbols}
        for future in as_completed(futures):
            future.result()

# === SCAN LOOP ===
def scan_loop():
    load_trades()
    symbols = get_symbols()
    logging.info(f"Scanning {len(symbols)} symbols")
    
    alert_queue = queue.Queue()
    
    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]
    
    def send_alerts():
        while True:
            try:
                sym, msg, side, entry, tp, sl, pat = alert_queue.get(timeout=1)
                with trade_lock:
                    mid = send_telegram(msg)
                    if mid and sym not in open_trades:
                        open_trades[sym] = {
                            'side': side, 'entry': entry, 'tp': tp, 'sl': sl,
                            'msg': msg, 'msg_id': mid, 'pattern': pat
                        }
                        save_trades()
                alert_queue.task_done()
            except queue.Empty:
                time.sleep(1)
            except Exception as e:
                logging.error(f"Alert error: {e}")
    
    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp_sl, daemon=True).start()
    threading.Thread(target=daily_summary, daemon=True).start()
    
    while True:
        next_close = get_next_candle_close()
        wait = max(0, next_close - time.time())
        logging.info(f"Waiting {wait:.0f}s for next 15m close")
        time.sleep(wait)
        
        for i, chunk in enumerate(chunks):
            logging.info(f"Batch {i+1}/{NUM_CHUNKS}")
            process_batch(chunk, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)
        
        logging.info("Scan complete")

# === FLASK ===
@app.route('/')
def home():
    return "Binance Rising/Falling Three Bot - Live Trading (max 5 positions)"

# === RUN ===
def run_bot():
    load_trades()
    num = len(open_trades)
    startup = f"BOT STARTED\nTracking {num} open positions\nMax 5 trades | 5x | $20/trade"
    send_telegram(startup)
    
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080, debug=False)

if __name__ == "__main__":
    run_bot()
