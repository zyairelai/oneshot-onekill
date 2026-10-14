#!/usr/bin/env python3
"""scanner.py — Live BTCUSDT entry signal monitor"""

import json, sys, time, threading, ssl, requests, websocket
from datetime import datetime, timezone, timedelta

SYMBOL   = "BTCUSDT"
UTC8     = timezone(timedelta(hours=8))
BASE_URL = "https://fapi.binance.com"
WS_1M    = "wss://fstream.binance.com/market/ws/btcusdt@kline_1m"
WS_5M    = "wss://fstream.binance.com/market/ws/btcusdt@kline_5m"

# ── signal thresholds — expressed as % of price (scales with any BTC level) ───────
# At $83,500:  0.010% spread = ~$8.35  |  0.002% slope = ~$1.67
# At $30,000:  0.010% spread = ~$3.00  |  0.002% slope = ~$0.60
MIN_SPREAD_PCT = 0.010   # % of price — EMA10 must be this far above/below EMA20
MIN_SLOPE_PCT  = 0.002   # % of price — EMA10 must be moving at least this fast
MIN_5M_STREAK  = 2       # consecutive 5m HA bars same colour (1-bar flip = WAIT)

GRN  = "\033[92m"; RED  = "\033[91m"; YLW  = "\033[93m"
CYN  = "\033[96m"; WHT  = "\033[97m"; DIM  = "\033[2m"
BOLD = "\033[1m";  RST  = "\033[0m"

# ── shared state ──────────────────────────────────────────────────────────────
_lock = threading.Lock()
S = dict(
    ema10=None, ema20=None,
    prev_ema10=None, prev_ema20=None,        # for slope calculation
    ha_prev_o=None, ha_prev_c=None,
    ha5m_bull=None, ha5m_consec=0,
    ha5m_prev_o=None, ha5m_prev_c=None,
    pdh=None, pdl=None, pdm=None,
    price=None, ts=None,
    live_o=None, live_h=None, live_l=None,
    status="BOOTSTRAPPING …", signal="WAIT", trend="NEUTRAL",
)

# ── math helpers ──────────────────────────────────────────────────────────────
def _ema(prev, p, n):
    k = 2.0 / (n + 1)
    return p * k + prev * (1 - k)

def _ema_s(closes, n):
    out = [None] * len(closes)
    if len(closes) < n: return out
    v = sum(closes[:n]) / n; out[n-1] = v
    for i in range(n, len(closes)):
        v = _ema(v, closes[i], n); out[i] = v
    return out

def _ha(o, h, l, c):
    n = len(o)
    hc = [(o[i]+h[i]+l[i]+c[i])/4 for i in range(n)]
    ho = [0.0]*n; ho[0] = (o[0]+c[0])/2
    for i in range(1, n): ho[i] = (ho[i-1]+hc[i-1])/2
    return ho, hc

def _fetch(iv, s, e):
    step = 60_000 if iv == "1m" else 300_000
    out, cur = [], s
    while cur < e:
        r = requests.get(f"{BASE_URL}/fapi/v1/klines",
            params=dict(symbol=SYMBOL, interval=iv, startTime=cur, endTime=e, limit=1500),
            timeout=10)
        r.raise_for_status(); data = r.json()
        if not data: break
        out.extend(data); cur = data[-1][0] + step
        if len(data) < 1500: break
    return out

def _ms(y, mo, d, h=0, mi=0):
    return int(datetime(y, mo, d, h, mi, tzinfo=UTC8).timestamp() * 1000)

# ── bootstrap ────────────────────────────────────────────────────────────────
def bootstrap():
    now   = datetime.now(UTC8)
    today = now.date()
    prev  = today - timedelta(days=1)
    now_ms = int(now.timestamp() * 1000)

    print("  Fetching PD levels …")
    pd = _fetch("1h", _ms(prev.year, prev.month, prev.day),
                      _ms(today.year, today.month, today.day) - 1)
    if pd:
        pdh = max(float(k[2]) for k in pd)
        pdl = min(float(k[3]) for k in pd)
        with _lock:
            S["pdh"] = pdh; S["pdl"] = pdl
            S["pdm"] = pdl + 0.5*(pdh-pdl)

    print("  Fetching 1m klines …")
    m1 = _fetch("1m", _ms(today.year, today.month, today.day) - 60*60_000, now_ms)
    if m1:
        o=[float(k[1])for k in m1]; h=[float(k[2])for k in m1]
        l=[float(k[3])for k in m1]; c=[float(k[4])for k in m1]
        e10=_ema_s(c,10); e20=_ema_s(c,20); ho,hc=_ha(o,h,l,c); n=len(c)
        with _lock:
            S["ema10"]=e10[n-1]; S["ema20"]=e20[n-1]
            S["prev_ema10"]=e10[n-2] if n>=2 else e10[n-1]
            S["prev_ema20"]=e20[n-2] if n>=2 else e20[n-1]
            S["ha_prev_o"]=ho[n-1]; S["ha_prev_c"]=hc[n-1]

    print("  Fetching 5m klines …")
    m5 = _fetch("5m", _ms(today.year, today.month, today.day) - 25*300_000, now_ms)
    if m5:
        o=[float(k[1])for k in m5]; h=[float(k[2])for k in m5]
        l=[float(k[3])for k in m5]; c=[float(k[4])for k in m5]
        ho,hc=_ha(o,h,l,c)
        consec=0; pb=None
        for i in range(len(ho)):
            b=1 if hc[i]>ho[i] else 0
            consec=(consec+1)if b==pb else 1; pb=b
        with _lock:
            S["ha5m_bull"]=pb; S["ha5m_consec"]=consec
            S["ha5m_prev_o"]=ho[-1]; S["ha5m_prev_c"]=hc[-1]

    with _lock: S["status"] = "RECEIVING DATA"
    print("  Done.\n")

# ── signal ────────────────────────────────────────────────────────────────────
def _signal():
    with _lock:
        e10=S["ema10"]; e20=S["ema20"]
        pe10=S["prev_ema10"]; pe20=S["prev_ema20"]
        hpo=S["ha_prev_o"]; hpc=S["ha_prev_c"]
        h5b=S["ha5m_bull"]; h5c=S["ha5m_consec"]
        pdh=S["pdh"]; pdl=S["pdl"]
        pr=S["price"]; lo=S["live_o"]; lh=S["live_h"]; ll=S["live_l"]

    if None in (e10,e20,pe10,pe20,hpo,hpc,h5b,pdh,pr,lo): return "WAIT","NEUTRAL"

    # provisional EMAs for the open candle
    e10p=_ema(e10,pr,10); e20p=_ema(e20,pr,20)
    spread  = e10p - e20p          # + = 10ema above 20ema
    slope10 = e10p - e10           # slope of 10 EMA this candle
    slope20 = e20p - e20           # slope of 20 EMA this candle

    ha_o=(hpo+hpc)/2; ha_c=(lo+lh+ll+pr)/4
    ha_bull=1 if ha_c>ha_o else 0
    rng=pdh-pdl
    pct=(pr-pdl)/rng if rng>0 else 0.5
    zone=4 if pct>=0.75 else 3 if pct>=0.5 else 2 if pct>=0.25 else 1

    # dynamic thresholds based on current price
    min_spread = e20p * MIN_SPREAD_PCT / 100
    min_slope  = e10p * MIN_SLOPE_PCT  / 100

    # trend: both EMAs must slope the same direction AND spread must be meaningful
    if spread > min_spread and slope10 > min_slope and slope20 > 0:  trend = "UPWARD"
    elif spread <-min_spread and slope10 <-min_slope and slope20 < 0: trend = "DOWNWARD"
    else: trend = "NEUTRAL"

    # signal: trend confirmed + HA + 5m streak + pd zone
    if (trend=="UPWARD"   and ha_bull==1 and h5b==1 and h5c>=MIN_5M_STREAK and zone>=3): sig="LONG"
    elif (trend=="DOWNWARD" and ha_bull==0 and h5b==0 and h5c>=MIN_5M_STREAK and zone>=3): sig="SHORT"
    else: sig="WAIT"
    return sig, trend

# ── UI ────────────────────────────────────────────────────────────────────────
def draw():
    with _lock:
        status=S["status"]; pr=S["price"]; ts=S["ts"]
        sig=S["signal"]; tr=S["trend"]

    now   = datetime.now(UTC8).strftime("%H:%M:%S")
    price = f"{pr:,.2f}" if pr else "—"
    ctime = ts.strftime("%H:%M") if ts else "——:——"

    if tr=="UPWARD":    tr_s=f"{GRN}{BOLD}↑ UPWARD{RST}"
    elif tr=="DOWNWARD":tr_s=f"{RED}{BOLD}↓ DOWNWARD{RST}"
    else:               tr_s=f"{YLW}{BOLD}→ NEUTRAL{RST}"

    if sig=="LONG":    act=f"{GRN}{BOLD}🚀  LONG  🚀{RST}"
    elif sig=="SHORT": act=f"{RED}{BOLD}💥  SHORT  💥{RST}"
    else:              act=f"{YLW}{BOLD}🐺  WAIT  🐺{RST}"

    sys.stdout.write("\033[H\033[J")
    print(f"\n  {CYN}{BOLD}BTCUSDT · SIGNAL SCANNER{RST}\n")
    print(f"  {DIM}STATUS{RST}  {status}")
    print(f"  {DIM}PRICE {RST}  {WHT}{BOLD}{price}{RST}")
    print(f"  {DIM}TIME  {RST}  {ctime}  ·  {now}")
    print(f"\n  {DIM}TREND {RST}  {tr_s}")
    print(f"\n  {DIM}ACTION{RST}  {act}\n")
    sys.stdout.flush()

def ui_loop():
    while True:
        try: draw()
        except Exception: pass
        time.sleep(0.5)

# ── WebSocket handlers ────────────────────────────────────────────────────────
def on_1m(ws, message):
    d = json.loads(message)
    k = d.get("k", d)
    if "o" not in k: return
    o=float(k["o"]); h=float(k["h"]); l=float(k["l"]); c=float(k["c"])
    ts=datetime.fromtimestamp(k["t"]/1000, tz=UTC8)
    closed=k.get("x", False)
    with _lock:
        S["price"]=c; S["ts"]=ts
        S["live_o"]=o; S["live_h"]=h; S["live_l"]=l
        S["status"]="RECEIVING DATA"
        if closed and S["ema10"] is not None:
            S["prev_ema10"]=S["ema10"]; S["prev_ema20"]=S["ema20"]
            S["ema10"]=_ema(S["ema10"],c,10)
            S["ema20"]=_ema(S["ema20"],c,20)
            new_ho=(S["ha_prev_o"]+S["ha_prev_c"])/2
            new_hc=(o+h+l+c)/4
            S["ha_prev_o"]=new_ho; S["ha_prev_c"]=new_hc
    sig,tr=_signal()
    with _lock: S["signal"]=sig; S["trend"]=tr

def on_5m(ws, message):
    d = json.loads(message)
    k = d.get("k", d)
    if not k.get("x", False): return
    o=float(k["o"]); h=float(k["h"]); l=float(k["l"]); c=float(k["c"])
    with _lock:
        ha_o=(S["ha5m_prev_o"]+S["ha5m_prev_c"])/2 if S["ha5m_prev_o"] else (o+c)/2
        ha_c=(o+h+l+c)/4
        bull=1 if ha_c>ha_o else 0
        S["ha5m_consec"]=(S["ha5m_consec"]+1) if bull==S["ha5m_bull"] else 1
        S["ha5m_bull"]=bull; S["ha5m_prev_o"]=ha_o; S["ha5m_prev_c"]=ha_c

def on_err(ws, e):
    with _lock: S["status"]="ERROR — reconnecting …"

def on_close(ws, *a):
    with _lock: S["status"]="DISCONNECTED — reconnecting …"

def run_ws(url, on_msg):
    while True:
        try:
            ws=websocket.WebSocketApp(url, on_message=on_msg,
                                      on_error=on_err, on_close=on_close)
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=30)
        except Exception: pass
        time.sleep(3)

# ── entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os; os.system("clear")
    print("\n  BTCUSDT SIGNAL SCANNER — starting up\n")
    try: bootstrap()
    except Exception as e: print(f"  [ERROR] {e}"); sys.exit(1)

    threading.Thread(target=ui_loop, daemon=True).start()
    threading.Thread(target=run_ws, daemon=True, args=(WS_5M, on_5m)).start()
    threading.Thread(target=run_ws, args=(WS_1M, on_1m)).start()

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n^C  Aborted."); sys.exit(0)
