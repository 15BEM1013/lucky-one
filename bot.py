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

# TRADE SETTINGS
CAPITAL_INITIAL = 10.0
LEVERAGE = 9

# --- Bullish-trend continuation LONG scheme ---
BULLISH_DCA1_TRIGGER_PCT = 0.2 / 100   # big-candle open + 0.2%
BULLISH_DCA1_CAPITAL = 20.0
BULLISH_TP_AFTER_DCA1_PCT = 0.8 / 100  # from new avg entry
BULLISH_SL_PCT = 3.5 / 100             # fixed, off big-candle open price
BULLISH_TP_INITIAL_PCT = 1.0 / 100     # unchanged, no-DCA case

# --- Sideways-trend SHORT scheme (also used for bullish-reversal SHORTs) ---
SIDEWAYS_DCA1_TRIGGER_PCT = 1.0 / 100  # from entry price
SIDEWAYS_DCA1_CAPITAL = 20.0
SIDEWAYS_TP_AFTER_DCA1_PCT = 0.8 / 100
SIDEWAYS_DCA2_TRIGGER_PCT = 3.0 / 100  # from entry price (not from DCA1)
SIDEWAYS_DCA2_CAPITAL = 10.0
SIDEWAYS_TP_AFTER_DCA2_PCT = 0.6 / 100
SIDEWAYS_SL_PCT = 5.0 / 100            # fixed, off entry price
SIDEWAYS_TP_INITIAL_PCT = 0.8 / 100    # no-DCA case

TP_CHECK_INTERVAL = 0.5      # SL price-poll interval
ORDER_CHECK_INTERVAL = 2.0   # DCA1/DCA2/TP resting-limit-order fill-check interval
MAX_OPEN_TRADES = 5

TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_SECRET')

if not API_KEY or not API_SECRET:
    raise ValueError("BINANCE_API_KEY and BINANCE_SECRET must be set")

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
            async with session.post(url, data={'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                r = await resp.json()
                return r.get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

async def edit_telegram_message(mid, new_text):
    if not mid: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, data={'chat_id': CHAT_ID, 'message_id': mid, 'text': new_text, 'parse_mode': 'Markdown'}, timeout=aiohttp.ClientTimeout(total=10))
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
prepared_symbols = set()  # symbols where margin mode / leverage already set this run

eth_trend = "SIDEWAYS"
eth_phase = "INDECISION"
eth_last_candle = None

eth_ema9 = 0.0
eth_ema21 = 0.0
eth_ema_gap = 0.0

# ===========================
# ETH MARKET PHASE ANALYSIS
# ===========================

eth_market_phases = []
eth_phase_text = ""

# === HELPERS ===
def format_duration(seconds):
    if seconds < 60: return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60: return f"{minutes}m {secs}s"
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
        return None, None, False

    upper = upper_wick_pct(candle)
    lower = lower_wick_pct(candle)
    is_green = is_bullish(candle)

    # GREEN CANDLE
    if is_green:

        if (
            upper > 50
            or lower > 30
            or (upper > 30 and lower > 30)
        ):
            return (
                'sell',
                f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → SELL",
                True
            )

        return (
            'buy',
            f"Green Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → BUY",
            False
        )

    # RED CANDLE
    else:

        if lower > 30 or (upper > 30 and lower > 30):
            return None, None, False

        return (
            'sell',
            f"Red Candle | Upper:{upper:.1f}% Lower:{lower:.1f}% → SELL",
            False
        )
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


# ===========================
# EMA CALCULATION
# ===========================

def calculate_ema(prices, period):

    multiplier = 2 / (period + 1)

    ema = sum(prices[:period]) / period

    for price in prices[period:]:
        ema = ((price - ema) * multiplier) + ema

    return ema


# ===========================
# MARKET PHASE NAME
# ===========================

def phase_name(direction, ema9_dir, ema21_dir, gap_dir):

    # ==========================
    # BULLISH
    # ==========================
    if direction == "BULLISH":

        if ema9_dir == "UP" and ema21_dir == "UP":

            if gap_dir == "UP":
                return "🟢 Bullish Momentum Building"

            return "🟡 Bullish Momentum Fading"

        if ema9_dir == "UP" and ema21_dir == "DOWN":
            return "🟢 Bullish Recovery"

        if ema9_dir == "DOWN" and ema21_dir == "UP":
            return "🟠 Bullish Pullback"

        return "🟡 Bullish Indecision"

    # ==========================
    # BEARISH
    # ==========================
    elif direction == "BEARISH":

        if ema9_dir == "DOWN" and ema21_dir == "DOWN":

            if gap_dir == "UP":
                return "🔴 Bearish Momentum Building"

            return "🟠 Bearish Momentum Fading"

        if ema9_dir == "DOWN" and ema21_dir == "UP":
            return "🔴 Bearish Recovery"

        if ema9_dir == "UP" and ema21_dir == "DOWN":
            return "🟡 Bearish Pullback"

        return "🟠 Bearish Indecision"

    # ==========================
    # SIDEWAYS
    # ==========================
    else:

        if ema9_dir == "UP" and gap_dir == "UP":
            return "⚪ Sideways (Bullish Bias)"

        if ema9_dir == "DOWN" and gap_dir == "UP":
            return "⚪ Sideways (Bearish Bias)"

        if gap_dir == "DOWN":
            return "🟣 Market Compression"

        return "⚪ Transition / Indecision"

# ===========================
# NEXT PHASE PREDICTION
# ===========================

def predict_next_phase(last_phase):

    # ==========================
    # BEARISH
    # ==========================

    if last_phase == "🔴 Bearish Momentum Building":
        return "🟠 Bearish Momentum Fading", "High"

    elif last_phase == "🟠 Bearish Momentum Fading":
        return "⚪ Sideways (Bullish Bias)", "Medium"

    elif last_phase == "🔴 Bearish Recovery":
        return "🟠 Bearish Momentum Fading", "Medium"

    elif last_phase == "🟡 Bearish Pullback":
        return "🔴 Bearish Momentum Building", "Medium"

    elif last_phase == "🟠 Bearish Indecision":
        return "⚪ Sideways (Bearish Bias)", "Low"

    # ==========================
    # SIDEWAYS
    # ==========================

    elif last_phase == "⚪ Sideways (Bullish Bias)":
        return "🟢 Bullish Momentum Building", "High"

    elif last_phase == "⚪ Sideways (Bearish Bias)":
        return "🔴 Bearish Momentum Building", "High"

    elif last_phase == "🟣 Market Compression":
        return "⚡ Strong Breakout Expected", "Medium"

    elif last_phase == "⚪ Transition / Indecision":
        return "Waiting for Confirmation", "Low"

    # ==========================
    # BULLISH
    # ==========================

    elif last_phase == "🟢 Bullish Recovery":
        return "🟢 Bullish Momentum Building", "High"

    elif last_phase == "🟢 Bullish Momentum Building":
        return "🚀 Bullish Breakout", "High"

    elif last_phase == "🚀 Bullish Breakout":
        return "🟢 Bullish Trend Continuation", "High"

    elif last_phase == "🟡 Bullish Momentum Fading":
        return "⚪ Sideways", "Medium"

    elif last_phase == "🟠 Bullish Pullback":
        return "🟢 Bullish Momentum Building", "Medium"

    elif last_phase == "🟡 Bullish Indecision":
        return "⚪ Sideways (Bullish Bias)", "Low"

    return "Unknown", "Low"

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
    if symbol in prepared_symbols:
        return
    try:
        await exchange.set_margin_mode('isolated', symbol)
        await exchange.set_leverage(LEVERAGE, symbol)
        prepared_symbols.add(symbol)
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

def get_avg_entry_and_total(tr):
    total_pos = sum(e['amount'] for e in tr['entries'])
    weighted = sum(e['price'] * e['amount'] for e in tr['entries'])
    return (weighted / total_pos) if total_pos > 0 else 0.0, total_pos

# === TREND-BASED TRADE PLAN ===
def compute_trade_plan(symbol, eth_trend_now, side, is_reversal, big_open, ref_price):
    """
    Decides which DCA/TP/SL scheme a trade uses, and computes the initial
    levels for it.

    ref_price = filled_price (order succeeded) or last known ticker/entry
    price (InsufficientFunds fallback, order never filled).

    Two schemes:
      - 'bullish_long': ETH-bullish trend, continuation LONG (Rising Three,
        non-reversal, buy side) only.
      - 'sideways': everything else that reaches this point — actual
        ETH-sideways SHORTs, and ETH-bullish reversal SHORTs.

    Returns: (dca_scheme, tp, dca1_level, dca2_level_or_None, sl_reference_price)
    All price values are rounded to the symbol's tick size.
    """
    is_long = side == 'buy'
    use_bullish_long_scheme = (eth_trend_now == "BULLISH" and is_long and not is_reversal)

    if use_bullish_long_scheme:
        dca_scheme = 'bullish_long'
        tp_pct = BULLISH_TP_INITIAL_PCT
        dca1_level = big_open * (1 + BULLISH_DCA1_TRIGGER_PCT)
        dca2_level = None
        sl_reference_price = big_open
    else:
        dca_scheme = 'sideways'
        tp_pct = SIDEWAYS_TP_INITIAL_PCT
        dca1_level = ref_price * (1 - SIDEWAYS_DCA1_TRIGGER_PCT) if is_long else ref_price * (1 + SIDEWAYS_DCA1_TRIGGER_PCT)
        dca2_level = ref_price * (1 - SIDEWAYS_DCA2_TRIGGER_PCT) if is_long else ref_price * (1 + SIDEWAYS_DCA2_TRIGGER_PCT)
        sl_reference_price = ref_price

    tp = round_price(symbol, ref_price * (1 + tp_pct) if is_long else ref_price * (1 - tp_pct))
    dca1_level = round_price(symbol, dca1_level)
    dca2_level = round_price(symbol, dca2_level) if dca2_level is not None else None
    sl_reference_price = round_price(symbol, sl_reference_price)

    return dca_scheme, tp, dca1_level, dca2_level, sl_reference_price

# === BUILD TRADE MESSAGE ===
def build_trade_message(tr, sym, current=None, is_final=False, hit_type=None, exit_price=None, pnl_usdt=None, pnl_pct=None):
    is_long = tr['side'] == 'buy'
    duration = format_duration(time.time() - tr['open_ts'])
    avg = tr['avg_entry']
    scheme = tr.get('dca_scheme', 'sideways')

    lines = [
        f"**{'LONG' if is_long else 'SHORT'}** {sym} ({tr.get('timeframe', 'N/A')})",
        f"Entry: {tr['initial_price']:.6f} | Avg: {avg:.6f}",
        f"Duration: {duration}"
    ]

    entries_str = [f"{'Initial' if e['stage']==0 else 'DCA'+str(e['stage'])}: {e['price']:.6f} (${e['margin']})" for e in tr['entries']]
    lines.append("Entries: " + " | ".join(entries_str))

    sl_pct = BULLISH_SL_PCT if scheme == 'bullish_long' else SIDEWAYS_SL_PCT
    sl_ref = tr.get('sl_reference_price', avg)
    sl_price = round_price(sym, sl_ref * (1 - sl_pct) if is_long else sl_ref * (1 + sl_pct))
    lines.append(f"TP: {tr['tp']:.6f} | SL: {sl_price:.6f} ({sl_pct*100:.1f}%)")

    if scheme == 'bullish_long':
        lines.append(f"DCA1 Level: {tr['dca1_level']:.6f} (0.2% above big-candle open) | No DCA2")
    else:
        lines.append(f"DCA1 Level: {tr['dca1_level']:.6f} (1% from entry)")
        lines.append(f"DCA2 Level: {tr['dca2_level']:.6f} (3% from entry)")

    if tr.get('signal_reason'):
        lines.append(f"Signal: {tr['signal_reason']}")

    pattern_type = "Strong Rejection" if tr.get('is_reversal') else "Continuation"
    lines.append(f"{tr.get('pattern', 'Pattern')} - {pattern_type}")

    if is_final and hit_type:
        lines.append(f"**{hit_type} HIT** | Exit: {exit_price:.6f}")
        lines.append(f"PnL: {pnl_pct:.2f}% (${pnl_usdt:+.2f})")

    return "\n".join(lines)

# === SL MONITOR (TP/DCA1/DCA2 are resting limit orders, watched by order_monitor_loop) ===
async def sl_monitor_loop():
    while True:
        try:
            async with trade_lock:
                if not open_trades:
                    await asyncio.sleep(TP_CHECK_INTERVAL)
                    continue

                symbols = list(open_trades.keys())
                tickers = await exchange.fetch_tickers(symbols)
                prices = {sym: t.get('last') or t.get('close') or t.get('markPrice') for sym, t in tickers.items()}

                for sym in list(open_trades.keys()):
                    tr = open_trades[sym]
                    current = prices.get(sym)
                    if not current: continue

                    is_long = tr['side'] == 'buy'
                    avg_entry = tr['avg_entry']

                    scheme = tr.get('dca_scheme', 'sideways')
                    sl_pct = BULLISH_SL_PCT if scheme == 'bullish_long' else SIDEWAYS_SL_PCT
                    sl_ref = tr.get('sl_reference_price', avg_entry)
                    sl_price = sl_ref * (1 - sl_pct) if is_long else sl_ref * (1 + sl_pct)

                    if (is_long and current <= sl_price) or (not is_long and current >= sl_price):
                        await close_trade(sym, "SL", current)
                        continue

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:
            logging.error(f"SL monitor loop error: {e}")
            await asyncio.sleep(1)

# === RESTING LIMIT ORDER HELPERS (DCA1 / DCA2 / TP) ===
async def safe_fetch_order(sym, order_id):
    try:
        return await exchange.fetch_order(order_id, sym)
    except Exception as e:
        logging.error(f"fetch_order failed {sym} {order_id}: {e}")
        return None

async def cancel_order_safe(sym, order_id):
    if not order_id:
        return
    try:
        await exchange.cancel_order(order_id, sym)
    except Exception as e:
        # order may already be filled or cancelled - not fatal
        logging.warning(f"cancel_order {sym} {order_id}: {e}")

async def place_dca_limit_order(sym, side, capital, price, label):
    try:
        amount_raw = (capital * LEVERAGE) / price
        amount = round_amount(sym, amount_raw)
        if amount <= 0:
            return None
        return await exchange.create_order(sym, 'limit', side, amount, price)
    except ccxt.InsufficientFunds:
        await send_telegram(
            f"⚠️ *{label} LIMIT ORDER NOT PLACED (Insufficient Funds)*\n"
            f"Symbol: {sym}\nPrice: {price:.6f}\nRequired Margin: ${capital}"
        )
        logging.warning(f"Insufficient funds placing {label} for {sym}")
        return None
    except Exception as e:
        logging.error(f"Place {label} order failed {sym}: {e}")
        return None

async def place_tp_order(sym, side, amount, price):
    try:
        amount = round_amount(sym, amount)
        if amount <= 0:
            return None
        return await exchange.create_order(sym, 'limit', side, amount, price, {'reduceOnly': True})
    except ccxt.InsufficientFunds:
        await send_telegram(f"⚠️ *TP ORDER NOT PLACED (Insufficient Funds)*\nSymbol: {sym}\nPrice: {price:.6f}")
        logging.warning(f"Insufficient funds placing TP for {sym}")
        return None
    except Exception as e:
        logging.error(f"Place TP order failed {sym}: {e}")
        return None

async def handle_dca_filled(sym, tr, order, stage):
    try:
        filled_price = round_price(sym, order.get('average') or order.get('price'))
        filled_amount = order.get('filled') or order.get('amount')
        scheme = tr.get('dca_scheme', 'sideways')
        capital = (
            BULLISH_DCA1_CAPITAL if scheme == 'bullish_long'
            else (SIDEWAYS_DCA1_CAPITAL if stage == 1 else SIDEWAYS_DCA2_CAPITAL)
        )

        tr['entries'].append({
            'price': filled_price,
            'amount': filled_amount,
            'margin': capital,
            'ts': time.time(),
            'stage': stage
        })

        avg_entry, total_amount = get_avg_entry_and_total(tr)
        tr['avg_entry'] = avg_entry
        tr['dca_stage'] = max(tr['dca_stage'], stage)
        tr[f'dca{stage}_order_id'] = None  # this leg is filled, nothing left to track

        is_long = tr['side'] == 'buy'
        if scheme == 'bullish_long':
            tp_pct = BULLISH_TP_AFTER_DCA1_PCT
        else:
            tp_pct = SIDEWAYS_TP_AFTER_DCA1_PCT if len(tr['entries']) == 2 else SIDEWAYS_TP_AFTER_DCA2_PCT

        new_tp = round_price(sym, avg_entry * (1 + tp_pct) if is_long else avg_entry * (1 - tp_pct))
        tr['tp'] = new_tp

        # replace the resting TP order to cover the new average / full size
        await cancel_order_safe(sym, tr.get('tp_order_id'))
        opposite_side = 'sell' if is_long else 'buy'
        new_tp_order = await place_tp_order(sym, opposite_side, total_amount, new_tp)
        tr['tp_order_id'] = new_tp_order['id'] if new_tp_order else None

        save_trades()
        logging.info(f"DCA{stage} filled on {sym} @ {filled_price} | new avg {avg_entry} | new TP {new_tp}")

        msg_text = build_trade_message(tr, sym)
        if tr.get('msg_id_initial'):
            await edit_telegram_message(tr['msg_id_initial'], msg_text)

    except Exception as e:
        logging.error(f"DCA{stage} fill handling failed {sym}: {e}")

async def handle_tp_filled(sym, tr, order):
    try:
        exit_price = round_price(sym, order.get('average') or order.get('price'))
        side = tr['side']
        avg_entry = tr['avg_entry']
        pnl_pct = (exit_price - avg_entry) / avg_entry * 100 if side == 'buy' else (avg_entry - exit_price) / avg_entry * 100
        total_margin = sum(e['margin'] for e in tr['entries'])
        pnl_usdt = total_margin * (pnl_pct / 100) * LEVERAGE

        # cancel any still-resting DCA orders so they don't open a stray position later
        await cancel_order_safe(sym, tr.get('dca1_order_id'))
        await cancel_order_safe(sym, tr.get('dca2_order_id'))

        closed = {**tr, 'exit_price': exit_price, 'exit_ts': time.time(), 'hit_type': 'TP',
                  'pnl_pct': pnl_pct, 'pnl_usdt': pnl_usdt, 'closed_at': get_ist_time().isoformat()}
        save_closed_trade(closed)

        msg_text = build_trade_message(tr, sym, is_final=True, hit_type='TP', exit_price=exit_price, pnl_usdt=pnl_usdt, pnl_pct=pnl_pct)
        if tr.get('msg_id_initial'):
            await edit_telegram_message(tr['msg_id_initial'], msg_text)

        del open_trades[sym]
        save_trades()
        logging.info(f"TP filled on {sym} @ {exit_price}")

    except Exception as e:
        logging.error(f"TP fill handling failed {sym}: {e}")

async def check_trade_orders(sym):
    """Poll a single trade's resting TP/DCA1/DCA2 orders for fills."""
    async with trade_lock:
        tr = open_trades.get(sym)
        if not tr:
            return

        tp_id = tr.get('tp_order_id')
        if tp_id:
            order = await safe_fetch_order(sym, tp_id)
            if order and order.get('status') == 'closed':
                await handle_tp_filled(sym, tr, order)
                return  # trade is closed, nothing else to check

        dca1_id = tr.get('dca1_order_id')
        if dca1_id:
            order = await safe_fetch_order(sym, dca1_id)
            if order and order.get('status') == 'closed':
                await handle_dca_filled(sym, tr, order, stage=1)

        dca2_id = tr.get('dca2_order_id')
        if dca2_id:
            order = await safe_fetch_order(sym, dca2_id)
            if order and order.get('status') == 'closed':
                await handle_dca_filled(sym, tr, order, stage=2)

async def order_monitor_loop():
    """Watches resting DCA1/DCA2/TP limit orders and reacts to fills."""
    while True:
        try:
            syms = list(open_trades.keys())
            for sym in syms:
                await check_trade_orders(sym)
            await asyncio.sleep(ORDER_CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Order monitor loop error: {e}")
            await asyncio.sleep(ORDER_CHECK_INTERVAL)

async def close_trade(sym, hit_type, exit_price):
    try:
        tr = open_trades[sym]
        side = tr['side']
        close_side = 'sell' if side == 'buy' else 'buy'
        total_amount = sum(e['amount'] for e in tr['entries'])

        # cancel any resting TP/DCA orders first so nothing is left orphaned
        await cancel_order_safe(sym, tr.get('tp_order_id'))
        await cancel_order_safe(sym, tr.get('dca1_order_id'))
        await cancel_order_safe(sym, tr.get('dca2_order_id'))

        close_order = await exchange.create_market_order(sym, close_side, total_amount)
        filled_exit = round_price(sym, close_order.get('average') or exit_price)

        avg_entry = tr['avg_entry']
        pnl_pct = (filled_exit - avg_entry) / avg_entry * 100 if side == 'buy' else (avg_entry - filled_exit) / avg_entry * 100
        total_margin = sum(e['margin'] for e in tr['entries'])
        pnl_usdt = total_margin * (pnl_pct / 100) * LEVERAGE

        closed = {**tr, 'exit_price': filled_exit, 'exit_ts': time.time(), 'hit_type': hit_type,
                  'pnl_pct': pnl_pct, 'pnl_usdt': pnl_usdt, 'closed_at': get_ist_time().isoformat()}

        save_closed_trade(closed)

        msg_text = build_trade_message(tr, sym, is_final=True, hit_type=hit_type, exit_price=filled_exit, pnl_usdt=pnl_usdt, pnl_pct=pnl_pct)
        if tr.get('msg_id_initial'):
            await edit_telegram_message(tr['msg_id_initial'], msg_text)

        del open_trades[sym]
        save_trades()

    except Exception as e:
        logging.error(f"Close trade failed {sym}: {e}")


async def update_eth_trend():

    global eth_trend
    global eth_last_candle
    global eth_market_phases
    global eth_phase_text
    global eth_ema9
    global eth_ema21
    global eth_ema_gap

    try:

        candles = await exchange.fetch_ohlcv(
            "ETH/USDT:USDT",
            "1h",
            limit=50
        )

        candles = candles[:-1]

        if candles[-1][0] == eth_last_candle:
            return

        eth_last_candle = candles[-1][0]

        closes = [c[4] for c in candles]

        phase_history = []

        for i in range(21, len(closes)):

            price = closes[:i + 1]

            ema9 = calculate_ema(price, 9)
            ema21 = calculate_ema(price, 21)

            ema9_prev = calculate_ema(price[:-1], 9)
            ema21_prev = calculate_ema(price[:-1], 21)

            gap = abs(ema9 - ema21)
            gap_prev = abs(ema9_prev - ema21_prev)

            ema9_dir = "UP" if ema9 > ema9_prev else "DOWN"
            ema21_dir = "UP" if ema21 > ema21_prev else "DOWN"
            gap_dir = "UP" if gap > gap_prev else "DOWN"

            diff_pct = gap / ema21 * 100

            if ema9 > ema21 and diff_pct >= 0.30:
                direction = "BULLISH"

            elif ema9 < ema21 and diff_pct >= 0.30:
                direction = "BEARISH"

            else:
                direction = "SIDEWAYS"

            phase = phase_name(
                direction,
                ema9_dir,
                ema21_dir,
                gap_dir
            )

            phase_history.append({
                "time": candles[i][0],
                "phase": phase
            })

        eth_market_phases = []

        start_time = phase_history[0]["time"]
        current_phase = phase_history[0]["phase"]

        for p in phase_history[1:]:

            if p["phase"] != current_phase:

                eth_market_phases.append({
                    "start": start_time,
                    "end": p["time"],
                    "phase": current_phase
                })

                start_time = p["time"]
                current_phase = p["phase"]

        eth_market_phases.append({
            "start": start_time,
            "end": phase_history[-1]["time"],
            "phase": current_phase
        })
        latest = eth_market_phases[-1]["phase"]

        # ==========================
        # Save latest EMA values
        # ==========================
        eth_ema9 = calculate_ema(closes, 9)
        eth_ema21 = calculate_ema(closes, 21)
        eth_ema_gap = abs(eth_ema9 - eth_ema21) / eth_ema21 * 100

        if latest == "🔴 Bearish Momentum Building":
            eth_phase = "BEARISH_MOMENTUM"

        elif latest == "🟠 Bearish Momentum Fading":
            eth_phase = "BEARISH_FADING"

        elif latest == "⚪ Sideways (Bullish Bias)":
            eth_phase = "SIDEWAYS_BULLISH"

        elif latest == "⚪ Sideways (Bearish Bias)":
            eth_phase = "SIDEWAYS_BEARISH"

        elif latest == "🟢 Bullish Momentum Building":
            eth_phase = "BULLISH_MOMENTUM"

        elif latest == "🟡 Bullish Momentum Fading":
            eth_phase = "BULLISH_FADING"

        elif latest == "⚪ Transition / Indecision":
            eth_phase = "TRANSITION"

        else:
            eth_phase = "INDECISION"

        if "Bullish" in latest:
            eth_trend = "BULLISH"

        elif "Bearish" in latest:
            eth_trend = "BEARISH"

        else:
            eth_trend = "SIDEWAYS"

        next_phase, confidence = predict_next_phase(latest)

        text = (
    f"📊 ETH FILTER\n"
    f"Trend: {eth_trend}\n"
    f"EMA9: {eth_ema9:.2f}\n"
    f"EMA21: {eth_ema21:.2f}\n"
    f"EMA Gap: {eth_ema_gap:.2f}%\n\n"
)
        text += "📊 ETH MARKET PHASE (Last Hours)\n\n"

        for p in eth_market_phases[-4:]:
            s = datetime.fromtimestamp(
                p["start"] / 1000,
                pytz.timezone("Asia/Kolkata")
            ).strftime("%H:%M")

            e = datetime.fromtimestamp(
                p["end"] / 1000,
                pytz.timezone("Asia/Kolkata")
            ).strftime("%H:%M")

            text += (
                f"{s} → {e}\n"
                f"{p['phase']}\n\n"
            )

        text += (
            f"➡️ Next Expected Phase\n"
            f"{next_phase}\n"
            f"Confidence: {confidence}"
        )

        eth_phase_text = text

        await send_telegram(text)

    except Exception as e:
        logging.error(f"ETH trend error: {e}")

async def eth_filter_loop():
    while True:
        try:
            await update_eth_trend()
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"ETH loop error: {e}")
            await asyncio.sleep(60)

# === PROCESS SYMBOL WITH INSUFFICIENT HANDLING ===
async def process_symbol(symbol, timeframe):
    side = None
    is_reversal = False
    pattern = None
    big_open = None
    entry_price = None
    try:
        candles = await exchange.fetch_ohlcv(symbol, timeframe, limit=CANDLE_LIMIT)
        if len(candles) < 9: return

        signal_time = candles[-2][0]
        key = (symbol, timeframe, 'pattern')

        async with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES: return
            if sent_signals.get(key) == signal_time: return

            sent_signals[key] = signal_time

        is_rising, big_candle = detect_rising_three(candles)
        is_falling, big_candle_f = detect_falling_three(candles)

        if is_rising:
            pattern = 'Rising Three'
            side, signal_msg, is_reversal = get_wick_signal(big_candle)
            big_open = big_candle[1]
        elif is_falling:
            pattern = 'Falling Three'
            side, signal_msg, is_reversal = get_wick_signal(big_candle_f)
            big_open = big_candle_f[1]
        else:
            return

        if not side:
            return
# ==========================
# ETH FILTER
# ==========================
        if eth_trend == "BULLISH":

            # Rising continuation
            if pattern == "Rising Three" and not is_reversal:
                pass

            elif pattern == "Rising Three" and is_reversal:
                pass

            else:
                logging.info(f"{symbol} rejected - Bullish")
                return

        elif eth_trend == "SIDEWAYS":

            if pattern == "Rising Three" and not is_reversal:
                side = "sell"
            else:
                logging.info(f"{symbol} rejected - Sideways")
                return

        elif eth_trend == "BEARISH":

            logging.info(f"{symbol} rejected - Bearish")
            return

        await prepare_symbol(symbol)
        # Reuse the just-closed signal candle's close instead of an extra fetch_ticker
        # round-trip - cuts latency between signal detection and order placement.
        entry_price = round_price(symbol, candles[-2][4])

        amount_raw = (CAPITAL_INITIAL * LEVERAGE) / entry_price
        amount = round_amount(symbol, amount_raw)
        if amount <= 0: return

        entry_order = await exchange.create_market_order(symbol, side, amount)
        filled_price = round_price(symbol, entry_order.get('average') or entry_price)

        dca_scheme, tp, dca1_level, dca2_level, sl_reference_price = compute_trade_plan(
            symbol, eth_trend, side, is_reversal, big_open, filled_price
        )

        # Place DCA1 (and DCA2, if this scheme has one) as resting limit orders
        dca1_capital = BULLISH_DCA1_CAPITAL if dca_scheme == 'bullish_long' else SIDEWAYS_DCA1_CAPITAL
        dca1_order = await place_dca_limit_order(symbol, side, dca1_capital, dca1_level, 'DCA1')

        dca2_order = None
        if dca2_level is not None:
            dca2_order = await place_dca_limit_order(symbol, side, SIDEWAYS_DCA2_CAPITAL, dca2_level, 'DCA2')

        opposite_side = 'sell' if side == 'buy' else 'buy'
        tp_order = await place_tp_order(symbol, opposite_side, amount, tp)

        initial_trade = {
            'side': side,
            'initial_price': filled_price,
            'entries': [{
                'price': filled_price,
                'amount': amount,
                'margin': CAPITAL_INITIAL,
                'ts': time.time(),
                'stage': 0
            }],
            'avg_entry': filled_price,
            'tp': tp,
            'dca_stage': 0,

            'msg_id_initial': None,
            'open_ts': time.time(),
            'timeframe': timeframe,
            'signal_reason': signal_msg,
            'pattern': pattern,
            'is_reversal': is_reversal,
            'dca_scheme': dca_scheme,
            'sl_reference_price': sl_reference_price,
            'dca1_level': dca1_level,
            'dca2_level': dca2_level,  # None for bullish_long scheme
            'tp_order_id': tp_order['id'] if tp_order else None,
            'dca1_order_id': dca1_order['id'] if dca1_order else None,
            'dca2_order_id': dca2_order['id'] if dca2_order else None,
        }

        msg_text = build_trade_message(initial_trade, symbol)
        mid = await send_telegram(msg_text)
        initial_trade['msg_id_initial'] = mid

        async with trade_lock:
            open_trades[symbol] = initial_trade
            await asyncio.to_thread(save_trades)

        logging.info(f"Opened {side.upper()} {symbol} | {pattern} {'- Strong Reversal' if is_reversal else '- Continuation'} | scheme={dca_scheme}")

    except ccxt.InsufficientFunds:

        dca_scheme, tp, dca1_level, dca2_level, sl_reference_price = compute_trade_plan(
            symbol, eth_trend, side, is_reversal, big_open, entry_price
        )
        dca2_str = f"{dca2_level:.6f}" if dca2_level is not None else "N/A (no DCA2)"

        await send_telegram(
            f"⚠️ *INSUFFICIENT FUNDS*\n\n"
            f"Symbol: {symbol}\n"
            f"Side: {side.upper()}\n"
            f"Pattern: {pattern}\n\n"
            f"Entry: {entry_price:.6f}\n"
            f"TP: {tp:.6f}\n"
            f"DCA1: {dca1_level:.6f}\n"
            f"DCA2: {dca2_str}\n\n"
            f"Required Margin: ${CAPITAL_INITIAL}"
        )

        logging.warning(
            f"Insufficient funds for initial trade on {symbol}"
        )

    except Exception as e:
        logging.error(f"Trade failed {symbol}: {e}")

# === SCANNING ===
async def process_batch(symbols_chunk, timeframe):
    tasks = [asyncio.create_task(process_symbol(s, timeframe)) for s in symbols_chunk]
    await asyncio.gather(*tasks, return_exceptions=True)

async def scan_loop(symbols):
    while True:
        wait_until = get_next_candle_close()
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
    await update_eth_trend()

    logging.info(f"Starting bot with {len(symbols)} symbols")

    startup_msg = f"🚀 **Bot Restarted** @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\nPatterns + Wick Filter | SL: trend-based | Insufficient Warning Active"
    await send_telegram(startup_msg)

    tasks = [
    asyncio.create_task(scan_loop(symbols)),
    asyncio.create_task(sl_monitor_loop()),
    asyncio.create_task(order_monitor_loop()),
    asyncio.create_task(daily_summary()),
    asyncio.create_task(eth_filter_loop()),
]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
