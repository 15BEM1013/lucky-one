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

CAPITAL_DCA1_NORMAL = 20.0
CAPITAL_DCA1_REVERSAL = 10.0
CAPITAL_DCA2 = 5.0

SL_PCT = 8.0 / 100

TP_INITIAL_NORMAL_PCT = 1.0 / 100
TP_INITIAL_REVERSAL_PCT = 0.5 / 100

TP_AFTER_DCA1_PCT = 0.6 / 100
TP_AFTER_DCA2_PCT = 0.4 / 100

DCA2_TRIGGER_PCT = 1.5 / 100

TP_CHECK_INTERVAL = 0.5
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
    try:
        await exchange.set_margin_mode('isolated', symbol)
        await exchange.set_leverage(LEVERAGE, symbol)
    except Exception as e:
        logging.warning(f"Prepare {symbol} failed: {e}")

def get_avg_entry_and_total(tr):
    total_pos = sum(e['amount'] for e in tr['entries'])
    weighted = sum(e['price'] * e['amount'] for e in tr['entries'])
    return (weighted / total_pos) if total_pos > 0 else 0.0, total_pos

# === BUILD TRADE MESSAGE ===
def build_trade_message(tr, sym, current=None, is_final=False, hit_type=None, exit_price=None, pnl_usdt=None, pnl_pct=None):
    is_long = tr['side'] == 'buy'
    duration = format_duration(time.time() - tr['open_ts'])
    avg = tr['avg_entry']

    lines = [
        f"**{'LONG' if is_long else 'SHORT'}** {sym} ({tr.get('timeframe', 'N/A')})",
        f"Entry: {tr['initial_price']:.6f} | Avg: {avg:.6f}",
        f"Duration: {duration}"
    ]

    entries_str = [f"{'Initial' if e['stage']==0 else 'DCA'+str(e['stage'])}: {e['price']:.6f} (${e['margin']})" for e in tr['entries']]
    lines.append("Entries: " + " | ".join(entries_str))

    sl_price = round_price(sym, avg * (1 - SL_PCT) if is_long else avg * (1 + SL_PCT))
    lines.append(f"TP: {tr['tp']:.6f} | SL: {sl_price:.6f} (8%)")

    if tr.get('is_reversal'):
        dca1 = tr.get('dca1_level')
        dca2 = tr.get('dca2_level')
        dir_symbol = "+" if not is_long else "-"
        lines.append(f"DCA1 Level: {dca1:.6f} ({dir_symbol}1.0%)")
        lines.append(f"DCA2 Level: {dca2:.6f} ({dir_symbol}1.5% from DCA1)")
    else:
        dca1 = tr.get('dca1_level')
        dca2 = tr.get('dca2_level')
        lines.append(f"DCA1 Level: {dca1:.6f} (Big Candle Open)")
        lines.append(f"DCA2 Level: {dca2:.6f} (1.5% from DCA1)")

    if tr.get('signal_reason'):
        lines.append(f"Signal: {tr['signal_reason']}")

    pattern_type = "Strong Rejection" if tr.get('is_reversal') else "Continuation"
    lines.append(f"{tr.get('pattern', 'Pattern')} - {pattern_type}")

    if is_final and hit_type:
        lines.append(f"**{hit_type} HIT** | Exit: {exit_price:.6f}")
        lines.append(f"PnL: {pnl_pct:.2f}% (${pnl_usdt:+.2f})")

    return "\n".join(lines)

# === FIXED MONITOR ===
async def monitor_tp_and_dca():
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
                    current_tp = tr['tp']

                    sl_price = avg_entry * (1 - SL_PCT) if is_long else avg_entry * (1 + SL_PCT)
                    if (is_long and current <= sl_price) or (not is_long and current >= sl_price):
                        await close_trade(sym, "SL", current)
                        continue

                    if (is_long and current >= current_tp) or (not is_long and current <= current_tp):
                        await close_trade(sym, "TP", current)
                        continue

                    if tr['dca_stage'] < 2:
                        await check_and_execute_dca(sym, tr, current)

            await asyncio.sleep(TP_CHECK_INTERVAL)

        except Exception as e:
            logging.error(f"Monitor loop error: {e}")
            await asyncio.sleep(1)

# === FIXED DCA WITH INSUFFICIENT HANDLING ===
async def check_and_execute_dca(sym, tr, current_price):
    try:
        dca_stage = tr['dca_stage'] + 1
        is_reversal = tr.get('is_reversal', False)
        is_long = tr['side'] == 'buy'

        if dca_stage == 1:
            dca_trigger_price = tr['dca1_level']
            capital = CAPITAL_DCA1_REVERSAL if is_reversal else CAPITAL_DCA1_NORMAL
        else:
            dca_trigger_price = tr['dca2_level']
            capital = CAPITAL_DCA2

        should_dca = (
            (is_long and current_price <= dca_trigger_price)
            or
            (not is_long and current_price >= dca_trigger_price)
        )

        if not should_dca:
            return

        side = tr['side']

        amount_raw = (capital * LEVERAGE) / current_price
        amount = round_amount(sym, amount_raw)

        if amount <= 0:
            return

        order = await exchange.create_market_order(
            sym,
            side,
            amount
        )

        filled_price = round_price(
            sym,
            order.get('average') or current_price
        )

        tr['entries'].append({
            'price': filled_price,
            'amount': amount,
            'margin': capital,
            'ts': time.time(),
            'stage': dca_stage
        })

        avg_entry, _ = get_avg_entry_and_total(tr)

        tr['avg_entry'] = avg_entry
        tr['dca_stage'] = dca_stage

        tp_pct = (
            TP_AFTER_DCA1_PCT
            if dca_stage == 1
            else TP_AFTER_DCA2_PCT
        )

        tr['tp'] = round_price(
            sym,
            avg_entry * (1 + tp_pct)
            if is_long
            else avg_entry * (1 - tp_pct)
        )

        save_trades()

        logging.info(
            f"DCA{dca_stage} executed on {sym} @ {filled_price}"
        )

        msg_text = build_trade_message(tr, sym)

        if tr.get('msg_id_initial'):
            await edit_telegram_message(
                tr['msg_id_initial'],
                msg_text
            )

    except ccxt.InsufficientFunds:

        warning_key = f"dca{dca_stage}_warning_sent"

        if not tr.get(warning_key, False):

            tr[warning_key] = True
            save_trades()

            await send_telegram(
                f"⚠️ *INSUFFICIENT FUNDS*\n\n"
                f"Symbol: {sym}\n"
                f"Stage: DCA{dca_stage}\n"
                f"Required Margin: ${capital}\n"
                f"Current Price: {current_price:.6f}\n"
                f"Trade remains active."
            )

            logging.warning(
                f"Insufficient funds for DCA{dca_stage} on {sym}"
            )

        return

    except Exception as e:
        logging.error(
            f"DCA failed on {sym}: {e}"
        )
async def close_trade(sym, hit_type, exit_price):
    try:
        tr = open_trades[sym]
        side = tr['side']
        close_side = 'sell' if side == 'buy' else 'buy'
        total_amount = sum(e['amount'] for e in tr['entries'])

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
        ticker = await exchange.fetch_ticker(symbol)
        entry_price = round_price(symbol, ticker['last'])

        amount_raw = (CAPITAL_INITIAL * LEVERAGE) / entry_price
        amount = round_amount(symbol, amount_raw)
        if amount <= 0: return

        entry_order = await exchange.create_market_order(symbol, side, amount)
        filled_price = round_price(symbol, entry_order.get('average') or entry_price)

        if eth_trend == "BULLISH":
            tp_pct = 1.0 / 100
        elif eth_trend == "SIDEWAYS":
            tp_pct = 0.75 / 100
        else:
            tp_pct = TP_INITIAL_REVERSAL_PCT if is_reversal 
   else TP_INITIAL_NORMAL_PCT
        tp = round_price(symbol, filled_price * (1 + tp_pct) if side == 'buy' else filled_price * (1 - tp_pct))

        if is_reversal:
            dca1_level = filled_price * (1 + 0.01) if side == 'sell' else filled_price * (1 - 0.01)
            dca2_level = dca1_level * (1 + DCA2_TRIGGER_PCT) if side == 'sell' else dca1_level * (1 - DCA2_TRIGGER_PCT)
        else:
            dca1_level = big_open
            dca2_level = big_open * (1 - DCA2_TRIGGER_PCT) if side == 'buy' else big_open * (1 + DCA2_TRIGGER_PCT)
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

            'dca1_warning_sent': False,
            'dca2_warning_sent': False,

            'msg_id_initial': None,
            'open_ts': time.time(),
            'timeframe': timeframe,
            'signal_reason': signal_msg,
            'pattern': pattern,
            'is_reversal': is_reversal,
            'dca1_level': round_price(symbol, dca1_level),
            'dca2_level': round_price(symbol, dca2_level)
        }

        msg_text = build_trade_message(initial_trade, symbol)
        mid = await send_telegram(msg_text)
        initial_trade['msg_id_initial'] = mid

        async with trade_lock:
            open_trades[symbol] = initial_trade
            await asyncio.to_thread(save_trades)

        logging.info(f"Opened {side.upper()} {symbol} | {pattern} {'- Strong Reversal' if is_reversal else '- Continuation'}")

    except ccxt.InsufficientFunds:

        tp_pct = TP_INITIAL_REVERSAL_PCT if is_reversal else TP_INITIAL_NORMAL_PCT

        tp = round_price(
            symbol,
            entry_price * (1 + tp_pct)
            if side == "buy"
            else entry_price * (1 - tp_pct)
        )

        if is_reversal:
            dca1_level = (
                entry_price * (1 + 0.01)
                if side == "sell"
                else entry_price * (1 - 0.01)
            )

            dca2_level = (
                dca1_level * (1 + DCA2_TRIGGER_PCT)
                if side == "sell"
                else dca1_level * (1 - DCA2_TRIGGER_PCT)
            )
        else:
            dca1_level = big_open

            dca2_level = (
                big_open * (1 - DCA2_TRIGGER_PCT)
                if side == "buy"
                else big_open * (1 + DCA2_TRIGGER_PCT)
            )

        await send_telegram(
            f"⚠️ *INSUFFICIENT FUNDS*\n\n"
            f"Symbol: {symbol}\n"
            f"Side: {side.upper()}\n"
            f"Pattern: {pattern}\n\n"
            f"Entry: {entry_price:.6f}\n"
            f"TP: {tp:.6f}\n"
            f"DCA1: {dca1_level:.6f}\n"
            f"DCA2: {dca2_level:.6f}\n\n"
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

    startup_msg = f"🚀 **Bot Restarted** @ {get_ist_time().strftime('%Y-%m-%d %H:%M IST')}\nPatterns + Wick Filter | SL: 8% | Insufficient Warning Active"
    await send_telegram(startup_msg)

    tasks = [
    asyncio.create_task(scan_loop(symbols)),
    asyncio.create_task(monitor_tp_and_dca()),
    asyncio.create_task(daily_summary()),
    asyncio.create_task(eth_filter_loop()),
]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())