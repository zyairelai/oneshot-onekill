#!/usr/bin/python3
# PYTHON_ARGCOMPLETE_OK
import json, time, sys, os, re, argparse
import threading, websocket, ssl, shutil, requests

sys.dont_write_bytecode = True
try: import argcomplete
except ImportError:
    argcomplete = None

parser = argparse.ArgumentParser(description="Binance Order Flow")
parser.add_argument('-t', '--t', '-telegram', '--telegram', '-a', '--a', '-alert', '--alert', dest='telegram', action='store_true', help="Enable Telegram alerts")
if argcomplete: argcomplete.autocomplete(parser)
args = parser.parse_args()

from termcolor import colored
from datetime import datetime, timedelta, timezone

# Configuration
SYMBOL = "btcusdt"
BAR_WIDTH = 72
RECENT_TRADES_HISTORY = 30
NORMAL_USD_THRESHOLD = 200000
LONDON_USD_THRESHOLD = 300000
NEWYORK_USD_THRESHOLD = 500000
MARKET_TYPE = "futures" # Options: "spot", "futures"

# ALERT Congurations
TELEGRAM_ALERT = args.telegram
WHALE_BOMB_MULTIPLIER = 18
COMBO_BOMB_MULTIPLIER = 1.8
LIQUIDATION_ALERT_THRESHOLD = 18000000

# Liquidation detection window: if a forceOrder arrived within this many seconds
# of the aggTrade timestamp, the trade is considered a LIQUIDATION
DUEL_ROLLING_MINUTES = 3
LIQUIDATION_WINDOW_SECONDS = 4
COMBO_STACKING_SECONDS = 5
WHALE_REACTION_SECONDS = 10

# Timezone
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Kuala_Lumpur")
except ImportError:
    import pytz
    TZ = pytz.timezone("Asia/Kuala_Lumpur")

def get_current_threshold():
    now = datetime.now(TZ)
    today_date = now.date()

    # US DST: 2nd Sunday March to 1st Sunday November
    us_dst_start = datetime(today_date.year, 3, 14) - timedelta(days=(datetime(today_date.year, 3, 14).weekday() + 1) % 7)
    us_dst_end = datetime(today_date.year, 11, 7) - timedelta(days=(datetime(today_date.year, 11, 7).weekday() + 1) % 7)
    is_us_dst = us_dst_start.date() <= today_date < us_dst_end.date()
    us_shift = 0 if is_us_dst else 1

    # UK DST: Last Sunday March to Last Sunday October
    uk_dst_start = datetime(today_date.year, 3, 31) - timedelta(days=(datetime(today_date.year, 3, 31).weekday() + 1) % 7)
    uk_dst_end = datetime(today_date.year, 10, 31) - timedelta(days=(datetime(today_date.year, 10, 31).weekday() + 1) % 7)
    is_uk_dst = uk_dst_start.date() <= today_date < uk_dst_end.date()
    uk_shift = 0 if is_uk_dst else 1

    # Trading Day Logic (from zones.py)
    reset_hour = 5 + us_shift
    trading_day = now if now.hour >= reset_hour else now - timedelta(days=1)

    if trading_day.weekday() > 4: # Weekend (Sat/Sun)
        return NORMAL_USD_THRESHOLD

    current_hour = now.hour

    # NY Session: 20:00 - 04:00 (DST) -> 20+us_shift to 4+us_shift
    ny_start = 20 + us_shift
    ny_end = 4 + us_shift
    if current_hour >= ny_start or current_hour < ny_end:
        return NEWYORK_USD_THRESHOLD

    # London Session: 15:00 - 19:59 (DST) -> 15+uk_shift to 19+uk_shift
    london_start = 15 + uk_shift
    london_end = 19 + uk_shift
    if london_start <= current_hour <= london_end:
        return LONDON_USD_THRESHOLD

    return NORMAL_USD_THRESHOLD

# Global state
trade_data = [] # Each element: [is_buy, usd_value, time_str, liq, raw_ts, qty]
trade_lock = threading.Lock()
status_msg = "Connecting..."
liq_status = colored("OFFLINE", "red")
CURRENT_PRICE = 0.0
combo_buys = []
combo_sells = []
combo_cooldown_until = 0

# Rolling buffer for ALL trades (unfiltered)
all_trades_rolling = []
all_trades_lock = threading.Lock()

# Latching / Signal Persistence state
latched_action = "NONE"
latch_expiry = 0.0

# forceOrder (liquidation) state
# Stores (timestamp_secs, usd_value, side_is_buy) for recent liquidations
recent_liquidations = []   # list of dicts: {ts, usd, is_buy}
liq_lock = threading.Lock()

ema_status_msg = colored("🐺 NONE 🐺", "white")

def update_ema_status():
    global ema_status_msg
    
    def calculate_ha_touches(klines):
        ha_klines = []
        if not klines: return False
        
        prev_ha_open = float(klines[0][1])
        prev_ha_close = float(klines[0][4])
        
        for i in range(len(klines)):
            o = float(klines[i][1])
            h = float(klines[i][2])
            l = float(klines[i][3])
            c = float(klines[i][4])
            
            if i == 0:
                ha_open = o
                ha_close = c
                ha_high = h
                ha_low = l
            else:
                ha_open = (prev_ha_open + prev_ha_close) / 2
                ha_close = (o + h + l + c) / 4
                ha_high = max(h, ha_open, ha_close)
                ha_low = min(l, ha_open, ha_close)
                
            ha_klines.append({'open': ha_open, 'high': ha_high, 'low': ha_low, 'close': ha_close})
            prev_ha_open = ha_open
            prev_ha_close = ha_close
            
        ema_10 = None
        ema_20 = None
        ema_50 = None
        k_10 = 2 / (10 + 1)
        k_20 = 2 / (20 + 1)
        k_50 = 2 / (50 + 1)
        
        for i in range(len(ha_klines)):
            c = ha_klines[i]['close']
            
            if ema_10 is None:
                if i == 9: ema_10 = sum(k['close'] for k in ha_klines[:10]) / 10
            else:
                ema_10 = c * k_10 + ema_10 * (1 - k_10)
                
            if ema_20 is None:
                if i == 19: ema_20 = sum(k['close'] for k in ha_klines[:20]) / 20
            else:
                ema_20 = c * k_20 + ema_20 * (1 - k_20)
                
            if ema_50 is None:
                if i == 49: ema_50 = sum(k['close'] for k in ha_klines[:50]) / 50
            else:
                ema_50 = c * k_50 + ema_50 * (1 - k_50)
                
            ha_klines[i]['ema_10'] = ema_10
            ha_klines[i]['ema_20'] = ema_20
            ha_klines[i]['ema_50'] = ema_50
            
        touches = False
        for i in range(1, 6):
            if len(ha_klines) >= i:
                candle = ha_klines[-i]
                e20 = candle.get('ema_20')
                if e20 is not None:
                    if candle['low'] <= e20 <= candle['high']:
                        touches = True
                        break
        return touches

    while True:
        try:
            base_url = "https://fapi.binance.com/fapi/v1/klines" if MARKET_TYPE == "futures" else "https://api.binance.com/api/v3/klines"
            
            touches_1m = False
            
            url = f"{base_url}?symbol={SYMBOL.upper()}&interval=1m&limit=100"
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                klines = res.json()
                touches_1m = calculate_ha_touches(klines)
                
            if touches_1m:
                ema_status_msg = colored("1m touching 20 EMA", "red", attrs=["bold"])
            else:
                ema_status_msg = colored("🐺 NONE 🐺", "white")
                
        except Exception:
            pass
            
        time.sleep(60)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def telegram_bot_sendtext(bot_message):
    if not TELEGRAM_ALERT:
        return
    bot_token = os.environ.get('TELEGRAM_LIVERMORE')
    if not bot_token:
        return
    chat_id = "@swinglivermore"
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {'chat_id': chat_id, 'parse_mode': 'html', 'text': bot_message}
    try:
        requests.get(url, params=params, timeout=5)
    except Exception: os._exit(0)

def is_liquidation(trade_time_secs: float, is_buy: bool) -> bool:
    """
    Return True if there is a recent forceOrder on the same side whose
    timestamp is within LIQUIDATION_WINDOW_SECONDS seconds of this aggTrade.

    A forceOrder BUY liquidation (forced close of a short) shows up as
    a BUY in the aggTrade stream (someone else absorbs the liquidated short).
    Same for SELL.
    """
    with liq_lock:
        for liq in recent_liquidations:
            if liq['is_buy'] == is_buy and abs(liq['ts'] - trade_time_secs) < LIQUIDATION_WINDOW_SECONDS:
                return True
    return False

def build_alert(label: str, symbol: str, side_str: str, usd_value: float,
                price: float, time_str: str) -> str:
    """
    Build the Telegram message string.
    """
    lines = []

    if label == "LIQUIDATION":
        if side_str == "SELL":  # Long position got liquidated
            lines.append(f"🩸 {symbol.upper()} LONG LIQUIDATED")
        else:  # Short position got liquidated
            lines.append(f"🥦 {symbol.upper()} SHORT LIQUIDATED")
        lines.append(f"☠️ ${usd_value:,.0f} @ {price:,.0f}")
    else:
        side_emoji = "🚀" if side_str == "BUY" else "💥"
        action_word = "BUYING" if side_str == "BUY" else "SELLING"
        lines.append(f"{side_emoji} {symbol.upper()} {action_word}")
        lines.append(f"🐋 ${usd_value:,.0f} @ {price:,.0f}")

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# UI & Metrics
# ---------------------------------------------------------------------------

vol_history_rolling = []
vol_history_lock = threading.Lock()

def get_rolling_metrics():
    global vol_history_rolling
    now_secs = time.time()
    with all_trades_lock:
        # Use ONLY the latest trade time as the reference to eliminate clock-drift issues
        latest_trade_time = all_trades_rolling[-1][0] if all_trades_rolling else now_secs
        cutoff = latest_trade_time - (DUEL_ROLLING_MINUTES * 60.0)
        while all_trades_rolling and all_trades_rolling[0][0] < cutoff:
            all_trades_rolling.pop(0)
        trades = list(all_trades_rolling)

    total_buy_vol = sum(t[2] for t in trades if t[3])
    total_sell_vol = sum(t[2] for t in trades if not t[3])

    # Calculate liquidation volumes safely
    liq_buy_vol = sum(t[2] for t in trades if t[3] and len(t) > 4 and t[4])
    liq_sell_vol = sum(t[2] for t in trades if not t[3] and len(t) > 4 and t[4])

    total_vol = total_buy_vol + total_sell_vol
    net_delta = total_buy_vol - total_sell_vol
    buy_ratio = total_buy_vol / total_vol if total_vol > 0 else 0.5

    first_price = trades[0][1] if trades else CURRENT_PRICE
    last_price = trades[-1][1] if trades else CURRENT_PRICE
    price_change_pct = ((last_price - first_price) / first_price) * 100 if first_price > 0 else 0.0

    # Update rolling volume history thread-safely
    with vol_history_lock:
        vol_history_rolling.append(total_vol)
        if len(vol_history_rolling) > 28800:  # 2 hours at 4 frames per second
            vol_history_rolling.pop(0)

        # Calculate dynamic threshold: 1.8x average volume of past 2 hours
        # Fallback to $1,000,000 minimum absolute baseline floor
        baseline_frames = int(DUEL_ROLLING_MINUTES * 60.0 * 4) # 4 FPS
        if len(vol_history_rolling) >= baseline_frames:
            avg_rolling_vol = sum(vol_history_rolling) / len(vol_history_rolling)
            min_absorb_threshold = max(1000000.0, avg_rolling_vol * 1.8)
        else:
            min_absorb_threshold = 1000000.0

    if not trades: return 0.0, 0.0, 0.0, False, 0.5, ""
    is_absorbed = False
    absorption_type = ""

    if total_vol >= min_absorb_threshold:
        # BUY_ABSORB / Bearish Setup (Taker Buys are blocked or failing)
        # Supports: 1. Buy Absorption (flat price), 2. Bearish Divergence (price drops while buying)
        if buy_ratio >= 0.75 and price_change_pct <= 0.03:
            is_absorbed = True
            liq_ratio = liq_buy_vol / total_buy_vol if total_buy_vol > 0 else 0
            if liq_ratio > 0.35:
                absorption_type = "EATING"
            else:
                absorption_type = "BUY_ABSORB"
        # SELL_ABSORB / Bullish Setup (Taker Sells are blocked or failing)
        # Supports: 1. Sell Absorption (flat price), 2. Bullish Divergence (price rises while selling)
        elif buy_ratio <= 0.25 and price_change_pct >= -0.03:
            is_absorbed = True
            liq_ratio = liq_sell_vol / total_sell_vol if total_sell_vol > 0 else 0
            if liq_ratio > 0.35:
                absorption_type = "EATING"
            else:
                absorption_type = "SELL_ABSORB"

    return net_delta, total_vol, price_change_pct, is_absorbed, buy_ratio, absorption_type

def render_ui():
    global status_msg

    # Snapshot the data safely to avoid RuntimeError: list size changed during iteration
    with trade_lock: current_trades = list(trade_data)

    # Get rolling metrics early so we have them for the header
    net_delta, total_vol, price_change_pct, is_absorbed, buy_ratio, absorption_type = get_rolling_metrics()

    # Use ANSI escape instead of os.system('clear') for performance
    sys.stdout.write("\033[H\033[J")
    sys.stdout.flush()

    plain_title = f" BINANCE ORDER FLOW [{SYMBOL.upper()}] "
    centered_title = plain_title.center(BAR_WIDTH + 2, "=")
    sym_color = "cyan" if TELEGRAM_ALERT else "yellow"
    final_title = centered_title.replace(f"[{SYMBOL.upper()}]", f"[{colored(SYMBOL.upper(), sym_color)}]")
    print(" " + final_title)
    print()

    mid = (BAR_WIDTH + 2) // 2

    # 1. Status Line
    l1_l = f"STATUS : {status_msg}"
    l1_r = f"LIQ STATUS: {liq_status}"

    def get_len(s):
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        clean_str = ansi_escape.sub('', s)
        extra = sum(1 for char in clean_str if char in "🐺🚀💥⚠️🟢🔴")
        return len(clean_str) + extra

    def print_header_line(left_text, right_text):
        # Anchor left_text to the left, lock '|' to mid
        pad = max(0, mid - 1 - get_len(left_text))
        print(" " + left_text + " " * pad + "| " + right_text)

    def print_centered_line(left_text, right_text):
        # Center both around the '|'
        pad = max(0, mid - 1 - get_len(left_text))
        print(" " + " " * pad + left_text + "| " + right_text)

    print_header_line(f"STATUS: {status_msg}", f"LIQ STATUS : {liq_status}")
    current_threshold = get_current_threshold()

    if DUEL_ROLLING_MINUTES == 0.5:
        time_label = "30s"
    else:
        time_label = f"{int(DUEL_ROLLING_MINUTES)}m" if DUEL_ROLLING_MINUTES == int(DUEL_ROLLING_MINUTES) else f"{DUEL_ROLLING_MINUTES}m"

    filter_text = f"FILTER     : > ${current_threshold:,} / {time_label}"

    if CURRENT_PRICE > 0:
        price_val = colored(f"{CURRENT_PRICE:,.2f}", "light_cyan")
        price_text = f"CURRENT PRICE @ {price_val}"
    else:
        price_text = "CURRENT PRICE: WAITING..."

    print_header_line(price_text, filter_text)

    # Format Net Delta string
    sign_delta = "+" if net_delta >= 0 else ""
    if abs(net_delta) > 1000000.0:
        delta_desc = " (STRONG)"
        color_delta = "green" if net_delta >= 0 else "red"
        delta_attrs = []
    else:
        delta_desc = ""
        color_delta = "green" if net_delta >= 0 else "red"
        delta_attrs = []
    net_delta_str = f"NET DELTA  : {colored(f'{sign_delta}${net_delta:,.0f}{delta_desc}', color_delta, attrs=delta_attrs)}"

    # global latched_action, latch_expiry
    now = time.time()

    # # 1. Determine raw trap state from current frame
    # raw_action = "NONE"
    # if is_absorbed:
    #     if absorption_type == "BUY_ABSORB": raw_action = "BEARISH"
    #     elif absorption_type == "SELL_ABSORB": raw_action = "BULLISH"
    #     elif absorption_type == "EATING": raw_action = "EATING"
    # 
    # # 2. Update latching state
    # if raw_action != "NONE":
    #     latched_action = raw_action
    #     latch_expiry = now + WHALE_REACTION_SECONDS
    # else:
    #     if now >= latch_expiry:
    #         latched_action = "NONE"
    # 
    # # 3. Format action text based on latched status
    # if latched_action == "BEARISH":
    #     action_text = colored("🔴 WHALES SELLING 🔴", "red", attrs=["bold"])
    # elif latched_action == "BULLISH":
    #     action_text = colored("🟢 WHALES BUYING 🟢", "green", attrs=["bold"])
    # elif latched_action == "EATING":
    #     action_text = colored("🟡 WHALES EATING 🟡", "yellow", attrs=["bold"])
    # else:
    #     action_text = colored("🐺 NONE 🐺", "white")
    # 
    # action_str = f"TRAP : {action_text}"

    # Print aligned header stats
    ema_str = f"EMA : {ema_status_msg}"
    print_header_line(ema_str, net_delta_str)
    print()

    # Duel Bar visual (full-width)
    print(" " + "=" * (BAR_WIDTH + 2))
    buy_chars_rolling = int(BAR_WIDTH * buy_ratio)
    sell_chars_rolling = BAR_WIDTH - buy_chars_rolling
    bar_rolling = colored("B" * buy_chars_rolling, "green") + colored("S" * sell_chars_rolling, "red")
    print(" " + f"[{bar_rolling}]")
    print(" " + "=" * (BAR_WIDTH + 2))

    # Rolling Volume Bar numbers centered below the duel bar
    buy_vol_rolling = total_vol * buy_ratio
    sell_vol_rolling = total_vol - buy_vol_rolling
    buy_pct_rolling = buy_ratio * 100
    sell_pct_rolling = (1 - buy_ratio) * 100

    buy_label_rolling = f"(${buy_vol_rolling:,.0f}) BUY: {buy_pct_rolling:.1f}% "
    sell_label_rolling = f" SELL: {sell_pct_rolling:.1f}% (${sell_vol_rolling:,.0f})"
    colored_buy = colored(buy_label_rolling, "green")
    colored_sell = colored(sell_label_rolling, "red")
    print_centered_line(colored_buy, colored_sell)
    print(" ")

    single_whale_bomb = current_threshold * WHALE_BOMB_MULTIPLIER
    latest_trade_time = current_trades[-1][4] if current_trades and len(current_trades[-1]) > 4 else now
    cutoff_rolling = latest_trade_time - (DUEL_ROLLING_MINUTES * 60.0)

    for trade in reversed(current_trades[-RECENT_TRADES_HISTORY:]):
        is_buy, usd_value = trade[0], trade[1]
        ts_str = trade[2] if len(trade) > 2 else ""
        liq_flag = trade[3] if len(trade) > 3 else False
        raw_ts = trade[4] if len(trade) > 4 else 0.0
        qty = trade[5] if len(trade) > 5 else 0.0
        side_str = "BUY" if is_buy else "SELL"
        side_color = "green" if is_buy else "red"
        colored_side = colored(side_str, side_color)
        colored_qty = colored(f"{qty:>7.1f} BTC", side_color)
        colored_usd = colored(f"{usd_value:>11,.0f} USD", side_color)

        colored_ts = colored(ts_str, "light_magenta") if raw_ts >= cutoff_rolling else ts_str

        if liq_flag:
            # BUY trade absorbs a SHORT liquidation, SELL trade absorbs a LONG liquidation
            rekt_side = "SHORT" if is_buy else "LONG"
            liq_tag = colored(f"  LIQUIDATED {rekt_side}", "yellow")
        elif usd_value >= single_whale_bomb:
            liq_tag = colored("  WHALE ORDER", "light_cyan")
        else:
            liq_tag = ""
        prefix = " [" if is_buy else "["
        print(" " + f"{prefix}{colored_side}] {colored_qty} | {colored_usd} @ {colored_ts}{liq_tag}")

# ---------------------------------------------------------------------------
# forceOrder WebSocket handler
# ---------------------------------------------------------------------------

def on_force_order_message(ws, message):
    """
    Handle forceOrder (liquidation) events from Binance futures.
    """
    global recent_liquidations, liq_status, trade_data
    try:
        data = json.loads(message)
        if 'data' in data: data = data['data']
        if data.get('e') != 'forceOrder': return
        liq_status = colored("RECEIVING DATA", "green")

        order = data.get('o', {})
        if order.get('s', '').lower() != SYMBOL.lower():
            return

        price = float(order.get('p', 0))
        qty = float(order.get('q', 0))
        usd_value = price * qty
        is_buy = order.get('S', '').upper() == 'BUY'

        trade_time_ms = order.get('T', data.get('E', int(time.time() * 1000)))
        trade_time_secs = trade_time_ms / 1000.0

        with liq_lock:
            recent_liquidations.append({
                'ts': trade_time_secs,
                'usd': usd_value,
                'is_buy': is_buy,
            })
            # Prune entries older than rolling window to ensure combo timers can find them
            cutoff = trade_time_secs - (DUEL_ROLLING_MINUTES * 60.0)
            recent_liquidations = [l for l in recent_liquidations if l['ts'] >= cutoff]

        # Retroactive tagging: check if any recent trades in trade_data match this liquidation
        with trade_lock:
            for i in range(len(trade_data)):
                # trade_data[i] is [is_buy, usd_value, time_str, liq, raw_ts]
                t_is_buy = trade_data[i][0]
                t_raw_ts = trade_data[i][4]
                if t_is_buy == is_buy and abs(t_raw_ts - trade_time_secs) < LIQUIDATION_WINDOW_SECONDS:
                    trade_data[i][3] = True

        # Retroactive tagging for the rolling buffer
        with all_trades_lock:
            for i in range(len(all_trades_rolling)):
                t_raw_ts = all_trades_rolling[i][0]
                t_is_buy = all_trades_rolling[i][3]
                if t_is_buy == is_buy and abs(t_raw_ts - trade_time_secs) < LIQUIDATION_WINDOW_SECONDS:
                    all_trades_rolling[i][4] = True

    except Exception: os._exit(0)

def on_force_order_error(ws, error):
    global liq_status
    liq_status = colored("ERR", "red")

def on_force_order_open(ws):
    global liq_status
    liq_status = colored("CONNECTED", "cyan")

def on_force_order_close(ws, close_status_code, close_msg):
    global liq_status
    liq_status = colored("OFFLINE", "red")

def run_force_order_ws():
    """Run the forceOrder WebSocket in a background thread with auto-reconnect."""
    url = f"wss://fstream.binance.com/market/ws/{SYMBOL.lower()}@forceOrder"
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_message=on_force_order_message,
                on_error=on_force_order_error,
                on_open=on_force_order_open,
                on_close=on_force_order_close
            )
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=30)
        except Exception: os._exit(0)
        time.sleep(3)  # Brief pause before reconnect

# ---------------------------------------------------------------------------
# aggTrade WebSocket handler
# ---------------------------------------------------------------------------

def process_delayed_combo_alert(combo_trades, is_buy, price, time_str, combo_whale_bomb):
    buy_sum = sum(t[1] for t in combo_trades)
    liq_sum = sum(t[1] for t in combo_trades if is_liquidation(t[0], is_buy))
    combo_liq = liq_sum > buy_sum / 2
    combo_threshold = LIQUIDATION_ALERT_THRESHOLD if combo_liq else combo_whale_bomb

    if buy_sum >= combo_threshold:
        combo_label = "LIQUIDATION" if combo_liq else "REAL ENTRY"
        side_str = "BUY" if is_buy else "SELL"
        msg = build_alert(combo_label, SYMBOL, side_str, buy_sum, price, time_str)
        telegram_bot_sendtext(msg)

def on_message(ws, message):
    global trade_data, status_msg, combo_buys, combo_sells, combo_cooldown_until, CURRENT_PRICE
    data = json.loads(message)

    if 'data' in data: data = data['data']
    if 'p' not in data: return

    status_msg = colored("RECEIVING DATA", "green")
    price = float(data['p'])
    qty = float(data['q'])
    usd_value = price * qty
    is_buy = not data['m']
    CURRENT_PRICE = price

    trade_time_ms = data.get('T', data.get('E', int(time.time() * 1000)))
    trade_time_secs = trade_time_ms / 1000.0

    # Store trade in rolling buffer (unfiltered)
    with all_trades_lock:
        all_trades_rolling.append([trade_time_secs, price, usd_value, is_buy, False])
        cutoff = trade_time_secs - (DUEL_ROLLING_MINUTES * 60.0)
        while all_trades_rolling and all_trades_rolling[0][0] < cutoff:
            all_trades_rolling.pop(0)
    dt_local = datetime.fromtimestamp(trade_time_secs, TZ)
    time_str = dt_local.strftime('%H:%M:%S')

    current_threshold = get_current_threshold()

    # Determine immediate liq status for the UI (might be false positive initially, fixed by background thread later)
    liq = is_liquidation(trade_time_secs, is_buy)
    single_whale_bomb = current_threshold * WHALE_BOMB_MULTIPLIER

    # ------------------------------------------------------------------
    # 2. Combo Whale Bomb Logic
    # ------------------------------------------------------------------
    combo_buys = [t for t in combo_buys if t[0] >= trade_time_secs - COMBO_STACKING_SECONDS]
    combo_sells = [t for t in combo_sells if t[0] >= trade_time_secs - COMBO_STACKING_SECONDS]

    if usd_value >= current_threshold:
        if is_buy: combo_buys.append((trade_time_secs, usd_value))
        else: combo_sells.append((trade_time_secs, usd_value))

    if trade_time_secs > combo_cooldown_until:
        buy_sum = sum(t[1] for t in combo_buys)
        sell_sum = sum(t[1] for t in combo_sells)
        combo_whale_bomb = single_whale_bomb * COMBO_BOMB_MULTIPLIER

        if buy_sum >= combo_whale_bomb:
            snapshot = list(combo_buys)
            threading.Timer(2.5, process_delayed_combo_alert, args=[snapshot, True, price, time_str, combo_whale_bomb]).start()
            combo_buys.clear()
            combo_sells.clear()
            combo_cooldown_until = trade_time_secs + COMBO_STACKING_SECONDS

        elif sell_sum >= combo_whale_bomb:
            snapshot = list(combo_sells)
            threading.Timer(2.5, process_delayed_combo_alert, args=[snapshot, False, price, time_str, combo_whale_bomb]).start()
            combo_buys.clear()
            combo_sells.clear()
            combo_cooldown_until = trade_time_secs + COMBO_STACKING_SECONDS

    # ------------------------------------------------------------------
    # 3. Update Recent Trades for UI
    # ------------------------------------------------------------------
    if usd_value >= current_threshold:
        with trade_lock:
            # Each element: [is_buy, usd_value, time_str, liq, raw_ts, qty]
            trade_data.append([is_buy, usd_value, time_str, liq, trade_time_secs, qty])
            if len(trade_data) > RECENT_TRADES_HISTORY:
                trade_data.pop(0)

def on_error(ws, error):
    global status_msg
    status_msg = colored(f"ERROR: {type(error).__name__} - {error}", "red")

def on_open(ws):
    global status_msg
    status_msg = colored("CONNECTED", "cyan")

def ui_thread():
    while True:
        if CURRENT_PRICE > 0: render_ui()
        time.sleep(0.25) # 4 FPS is plenty for terminal UI and saves CPU

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_agg_trade_ws():
    """Run the main aggTrade WebSocket with auto-reconnect."""
    if MARKET_TYPE == "futures":
        socket_url = f"wss://fstream.binance.com/market/ws/{SYMBOL.lower()}@aggTrade"
    else:
        socket_url = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@aggTrade"

    while True:
        try:
            ws = websocket.WebSocketApp(
                socket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error
            )
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=30)
        except Exception: os._exit(0)
        time.sleep(3)

if __name__ == "__main__":
    os.system('clear')

    # Start UI render loop
    threading.Thread(target=ui_thread, daemon=True).start()

    # Start EMA checker thread
    threading.Thread(target=update_ema_status, daemon=True).start()

    # Start forceOrder WebSocket in background (futures only)
    if MARKET_TYPE == "futures":
        threading.Thread(target=run_force_order_ws, daemon=True).start()

    # Start main aggTrade WebSocket in a persistent thread
    threading.Thread(target=run_agg_trade_ws, daemon=True).start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(0)
