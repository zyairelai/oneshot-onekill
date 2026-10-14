#!/usr/bin/env python3
"""
getklines.py — BTCUSDT 1m klines + pattern features for LLM training.

Output columns
──────────────
Core    : time  open  high  low  close  10ema  20ema
HA      : haopen  hahigh  halow  haclose  ha10ema  ha20ema
Derived : e10_slp  e20_slp  c_vs_e10  e10_vs_e20
HA flags: ha_bull  ha_strong
PD ctx  : pct_pd  abv_pdm  pd_zone
5m ctx  : ha5m_bull  ha5m_consec
Label   : is_entry

Date  : DDMM / today / yesterday / weekday name
Range : HHMM-HHMM  (cross-midnight: 2358-0034 OR 2358-2434)
Entry : HHMM  (optional — marks is_entry=1 on that candle)
"""

import sys
import requests
from datetime import datetime, date as _date, timezone, timedelta

SYMBOL        = "BTCUSDT"
UTC8          = timezone(timedelta(hours=8))
BASE_URL      = "https://fapi.binance.com"
EMA_WARMUP    = 50   # 1m warmup candles for EMA seeding
HA5M_WARMUP   = 20   # 5m warmup candles for HA seeding (= 100 min)

_WEEKDAY_MAP = {
    "monday": 0, "mon": 0, "tuesday": 1, "tue": 1,
    "wednesday": 2, "wed": 2, "thursday": 3, "thu": 3,
    "friday": 4, "fri": 4, "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}

# ── date / time helpers ───────────────────────────────────────────────────────

def resolve_date(s: str) -> _date:
    raw = s.strip().lower()
    today = datetime.now(UTC8).date()
    if raw == "today":       return today
    if raw == "yesterday":   return today - timedelta(days=1)
    if raw in _WEEKDAY_MAP:
        diff = (today.weekday() - _WEEKDAY_MAP[raw]) % 7
        return today - timedelta(days=diff)
    if len(raw) == 4 and raw.isdigit():
        day, month = int(raw[:2]), int(raw[2:])
        try:
            c = _date(today.year, month, day)
        except ValueError as e:
            raise ValueError(f"Invalid date: {e}") from e
        if c > today:
            c = _date(today.year - 1, month, day)
        return c
    raise ValueError(
        f"Unrecognised date {s!r}. Use DDMM, 'today', 'yesterday', or a weekday name."
    )


def parse_time_range(s: str) -> tuple[int, int, int, int, str, str]:
    """Return (h_s, m_s, h_e, m_e, raw_s, raw_e). Accepts 24h+ overflow."""
    s = s.strip()
    parts = s.split("-")
    if len(parts) != 2 or not all(p.isdigit() and len(p) == 4 for p in parts):
        raise ValueError(f"Time range must be HHMM-HHMM, got: {s!r}")
    raw_s, raw_e = parts
    return int(raw_s[:2]), int(raw_s[2:]), int(raw_e[:2]), int(raw_e[2:]), raw_s, raw_e


def to_utc_ms(year, month, day, hour, minute) -> int:
    dt = datetime(year, month, day, hour, minute, 0, tzinfo=UTC8)
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)

# ── Binance fetchers ──────────────────────────────────────────────────────────

def _get_klines(interval: str, start_ms: int, end_ms: int) -> list:
    """Generic paginated kline fetch."""
    out = []
    cur = start_ms
    while cur < end_ms:
        r = requests.get(
            f"{BASE_URL}/fapi/v1/klines",
            params=dict(symbol=SYMBOL, interval=interval,
                        startTime=cur, endTime=end_ms, limit=1500),
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        out.extend(data)
        cur = data[-1][0] + (60_000 if interval == "1m" else 5 * 60_000)
        if len(data) < 1500:
            break
    return out


def fetch_prev_day_levels(resolved: _date):
    """
    Pull previous calendar day's 1h klines (UTC+8 boundaries) and return:
      pdh, pdl, pdm, pd75, pd25
    Printed as a banner; also returned for per-row derived columns.
    """
    prev = resolved - timedelta(days=1)
    start_ms = to_utc_ms(prev.year, prev.month, prev.day, 0, 0)
    end_ms   = to_utc_ms(resolved.year, resolved.month, resolved.day, 0, 0) - 1

    r = requests.get(
        f"{BASE_URL}/fapi/v1/klines",
        params=dict(symbol=SYMBOL, interval="1h",
                    startTime=start_ms, endTime=end_ms, limit=24),
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        return None

    pdh = max(float(k[2]) for k in data)
    pdl = min(float(k[3]) for k in data)
    rng = pdh - pdl
    if rng == 0:
        return None
    pdm  = pdl + 0.50 * rng
    pd75 = pdl + 0.75 * rng
    pd25 = pdl + 0.25 * rng
    return pdh, pdl, pdm, pd75, pd25


def fetch_5m_ha(start_ms: int, end_ms: int) -> dict[int, tuple[int, int]]:
    """
    Fetch 5m klines (with HA5M_WARMUP extra bars before start_ms),
    compute Heikin Ashi, and return:
      { 5m_bar_open_ms: (ha_bull: 0|1, consec: int) }
    consec = how many consecutive same-colour bars up to and including this one.
    """
    fetch_start = start_ms - HA5M_WARMUP * 5 * 60_000
    raw = _get_klines("5m", fetch_start, end_ms)
    if not raw:
        return {}

    ts     = [int(k[0])   for k in raw]
    opens  = [float(k[1]) for k in raw]
    highs  = [float(k[2]) for k in raw]
    lows   = [float(k[3]) for k in raw]
    closes = [float(k[4]) for k in raw]

    n        = len(opens)
    ha_close = [(opens[i]+highs[i]+lows[i]+closes[i])/4 for i in range(n)]
    ha_open  = [0.0] * n
    ha_open[0] = (opens[0] + closes[0]) / 2
    for i in range(1, n):
        ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2

    result: dict[int, tuple[int, int]] = {}
    consec, prev_bull = 0, None
    for i in range(n):
        bull = 1 if ha_close[i] > ha_open[i] else 0
        consec = (consec + 1) if bull == prev_bull else 1
        prev_bull = bull
        result[ts[i]] = (bull, consec)
    return result


def bar5m_ms(dt_utc8: datetime) -> int:
    """Return the UTC-ms open timestamp of the 5m bar that contains dt_utc8."""
    bar_min = (dt_utc8.minute // 5) * 5
    bar_dt  = dt_utc8.replace(minute=bar_min, second=0, microsecond=0)
    return int(bar_dt.timestamp() * 1000)

# ── indicators ────────────────────────────────────────────────────────────────

def ema(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) < period:
        return result
    k    = 2.0 / (period + 1)
    sma  = sum(values[:period]) / period
    result[period - 1] = sma
    prev = sma
    for i in range(period, len(values)):
        prev = values[i] * k + prev * (1 - k)
        result[i] = prev
    return result


def heikin_ashi(opens, highs, lows, closes):
    n        = len(opens)
    ha_close = [(opens[i]+highs[i]+lows[i]+closes[i])/4 for i in range(n)]
    ha_open  = [0.0]*n; ha_high = [0.0]*n; ha_low = [0.0]*n
    ha_open[0] = (opens[0]+closes[0])/2
    ha_high[0] = highs[0]; ha_low[0] = lows[0]
    for i in range(1, n):
        ha_open[i] = (ha_open[i-1]+ha_close[i-1])/2
        ha_high[i] = max(highs[i], ha_open[i], ha_close[i])
        ha_low[i]  = min(lows[i],  ha_open[i], ha_close[i])
    return ha_open, ha_high, ha_low, ha_close

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    date_input  = input("Date (DDMM, Enter = today): ").strip() or "today"
    time_input  = input("Time Range (xxxx-xxxx): ").strip()
    entry_input = input("Entry Point (HHMM / H1-H2 range / comma for multiple, Enter to skip): ").strip()

    try:
        resolved                        = resolve_date(date_input)
        h_s, m_s, h_e, m_e, raw_s, raw_e = parse_time_range(time_input)
    except ValueError as exc:
        print(f"[ERROR] {exc}"); sys.exit(1)

    # entry point — comma = AND, dash = inclusive range
    # e.g.  "1012,1013"   → 1012 and 1013
    #        "1012-1015"   → 1012 1013 1014 1015
    #        "1012,1014-1016" → 1012 1014 1015 1016
    entry_times: set[str] = set()
    if entry_input:
        for token in entry_input.split(","):
            token = token.strip()
            if len(token) == 4 and token.isdigit():
                # single candle
                entry_times.add(token)
            elif (len(token) == 9 and token[4] == "-"
                  and token[:4].isdigit() and token[5:].isdigit()):
                # inclusive range  HHMM-HHMM
                h_s2, m_s2 = int(token[:2]),  int(token[2:4])
                h_e2, m_e2 = int(token[5:7]), int(token[7:9])
                t_s = h_s2 * 60 + m_s2
                t_e = h_e2 * 60 + m_e2
                if t_e < t_s:          # cross-midnight range
                    t_e += 24 * 60
                for mins in range(t_s, t_e + 1):
                    hh = (mins // 60) % 24
                    mm =  mins % 60
                    entry_times.add(f"{hh:02d}{mm:02d}")
            else:
                print(f"[WARN] Entry point {token!r} is not valid HHMM or HHMM-HHMM — ignoring.")

    day, month, year = resolved.day, resolved.month, resolved.year

    # cross-midnight
    end_date = resolved; next_day = False
    if h_e >= 24:
        h_e -= 24; end_date = resolved + timedelta(days=1); next_day = True
    elif (h_e * 60 + m_e) <= (h_s * 60 + m_s):
        end_date = resolved + timedelta(days=1); next_day = True

    start_ms  = to_utc_ms(year, month, day, h_s, m_s)
    end_ms    = to_utc_ms(end_date.year, end_date.month, end_date.day, h_e, m_e)
    warmup_ms = start_ms - EMA_WARMUP * 60_000

    end_tag = f"+1d {h_e:02d}{m_e:02d}" if next_day else f"{h_e:02d}{m_e:02d}"
    print(f"\nFetching {SYMBOL} 1m klines  {day:02d}/{month:02d}/{year} "
          f"{h_s:02d}{m_s:02d}–{end_tag} UTC+8  (+{EMA_WARMUP} warm-up candles) …")

    # ── fetch all data ──
    try:
        raw_1m    = _get_klines("1m", warmup_ms, end_ms)
        pd_levels = fetch_prev_day_levels(resolved)
        ha5m_map  = fetch_5m_ha(start_ms, end_ms)
    except requests.RequestException as exc:
        print(f"[ERROR] Network error: {exc}"); sys.exit(1)

    if not raw_1m:
        print("[ERROR] No 1m klines returned."); sys.exit(1)

    # ── parse 1m candles ──
    times  = [datetime.fromtimestamp(int(k[0])/1000, tz=UTC8) for k in raw_1m]
    opens  = [float(k[1]) for k in raw_1m]
    highs  = [float(k[2]) for k in raw_1m]
    lows   = [float(k[3]) for k in raw_1m]
    closes = [float(k[4]) for k in raw_1m]

    # ── indicators ──
    ema10 = ema(closes, 10)
    ema20 = ema(closes, 20)
    ha_open, ha_high, ha_low, ha_close = heikin_ashi(opens, highs, lows, closes)
    ha_ema10 = ema(ha_close, 10)
    ha_ema20 = ema(ha_close, 20)

    # ── filter to requested window ──
    idx = [i for i, t in enumerate(times) if t.timestamp() * 1000 >= start_ms]
    if not idx:
        print("[ERROR] No candles in requested window."); sys.exit(1)

    # ── PD levels banner ──
    if pd_levels:
        pdh, pdl, pdm, pd75, pd25 = pd_levels
        pd_rng = pdh - pdl
        print(f"\n  Prev 1D High : {pdh:.2f}")
        print(f"  Prev 1D 75%  : {pd75:.2f}")
        print(f"  Prev 1D Mid  : {pdm:.2f}")
        print(f"  Prev 1D 25%  : {pd25:.2f}")
        print(f"  Prev 1D Low  : {pdl:.2f}")
    else:
        pdh = pdl = pdm = pd75 = pd25 = pd_rng = None
        print("\n  [WARN] Could not fetch previous day levels.")

    # ── build output ──
    def f(v, w=10):   return f"{v:{w}.2f}" if v is not None else f"{'—':>{w}}"
    def fs(v, w=8):   return f"{v:{w}.2f}" if v is not None else f"{'—':>{w}}"

    header = (
        f"{'time':<6}  "
        f"{'open':>10}  {'high':>10}  {'low':>10}  {'close':>10}  "
        f"{'10ema':>10}  {'20ema':>10}  "
        f"{'haopen':>10}  {'hahigh':>10}  {'halow':>10}  {'haclose':>10}  "
        f"{'ha10ema':>10}  {'ha20ema':>10}  "
        f"{'e10_slp':>8}  {'e20_slp':>8}  "
        f"{'c_vs_e10':>9}  {'e10_vs_e20':>10}  "
        f"{'ha_bull':>7}  {'ha_strong':>9}  "
        f"{'pct_pd':>6}  {'abv_pdm':>7}  {'pd_zone':>7}  "
        f"{'ha5m_bull':>9}  {'ha5m_consec':>11}  "
        f"{'is_entry':>8}"
    )
    lines = [header]

    for j, i in enumerate(idx):
        time_str = times[i].strftime("%H%M")
        is_entry = 1 if time_str in entry_times else 0

        # ── slopes (use prev candle in full series; safe because EMA_WARMUP >= 1) ──
        e10_slp = (ema10[i] - ema10[i-1]) if (i > 0 and ema10[i] is not None and ema10[i-1] is not None) else None
        e20_slp = (ema20[i] - ema20[i-1]) if (i > 0 and ema20[i] is not None and ema20[i-1] is not None) else None

        # ── price vs EMA spreads ──
        c_vs_e10   = (closes[i] - ema10[i])  if ema10[i]  is not None else None
        e10_vs_e20 = (ema10[i]  - ema20[i])  if (ema10[i] is not None and ema20[i] is not None) else None

        # ── HA candle flags ──
        ha_bull   = 1 if ha_close[i] > ha_open[i] else 0
        if ha_bull:
            ha_strong = 1 if round(ha_low[i], 8) == round(ha_open[i], 8) else 0
        else:
            ha_strong = 1 if round(ha_high[i], 8) == round(ha_open[i], 8) else 0

        # ── PD derived columns ──
        if pd_levels and pd_rng:
            pct_pd  = round((closes[i] - pdl) / pd_rng, 4)
            abv_pdm = 1 if closes[i] > pdm else 0
            if   closes[i] >= pd75: pd_zone = 4
            elif closes[i] >= pdm:  pd_zone = 3
            elif closes[i] >= pd25: pd_zone = 2
            else:                   pd_zone = 1
        else:
            pct_pd = abv_pdm = pd_zone = None

        # ── 5m HA context ──
        bms = bar5m_ms(times[i])
        if bms in ha5m_map:
            ha5m_bull, ha5m_consec = ha5m_map[bms]
        else:
            ha5m_bull = ha5m_consec = None

        # ── format row ──
        row = (
            f"{time_str:<6}  "
            f"{opens[i]:>10.2f}  {highs[i]:>10.2f}  {lows[i]:>10.2f}  {closes[i]:>10.2f}  "
            f"{f(ema10[i])}  {f(ema20[i])}  "
            f"{ha_open[i]:>10.2f}  {ha_high[i]:>10.2f}  {ha_low[i]:>10.2f}  {ha_close[i]:>10.2f}  "
            f"{f(ha_ema10[i])}  {f(ha_ema20[i])}  "
            f"{fs(e10_slp)}  {fs(e20_slp)}  "
            f"{f(c_vs_e10, 9)}  {f(e10_vs_e20, 10)}  "
            f"{ha_bull:>7}  {ha_strong:>9}  "
            f"{(f'{pct_pd:.4f}' if pct_pd is not None else '—'):>6}  "
            f"{(str(abv_pdm) if abv_pdm is not None else '—'):>7}  "
            f"{(str(pd_zone) if pd_zone is not None else '—'):>7}  "
            f"{(str(ha5m_bull) if ha5m_bull is not None else '—'):>9}  "
            f"{(str(ha5m_consec) if ha5m_consec is not None else '—'):>11}  "
            f"{is_entry:>8}"
        )
        lines.append(row)

    # ── write file ──
    filename = f"klines_{day:02d}{month:02d}_{raw_s}-{raw_e}.txt"
    with open(filename, "w") as f_out:
        f_out.write("\n".join(lines) + "\n")

    print(f"\n✓  {len(idx)} candles written → {filename}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n^C  Aborted.")
        sys.exit(0)
