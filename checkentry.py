#!/usr/bin/env python3
"""checkentry.py — Classify a klines session file using scanner.py signal logic.

Usage:
    ./checkentry.py klines_1410_1541-1558.txt

Output (one line):
    bad long | bad short | perfect long | perfect short

Signal rules (mirrored from scanner.py):
    MIN_SPREAD_PCT = 0.010 %   (e10_vs_e20 relative to e20)
    MIN_SLOPE_PCT  = 0.002 %   (e10_slp    relative to e10)
    MIN_5M_STREAK  = 2         (ha5m_consec)

    LONG  : spread > 0, e10_slp > 0, e20_slp > 0, ha_bull==1, ha5m_bull==1,
            ha5m_consec >= 2, pd_zone >= 3
    SHORT : spread < 0, e10_slp < 0, e20_slp < 0, ha_bull==0, ha5m_bull==0,
            ha5m_consec >= 2, pd_zone >= 3

    "perfect" = every condition fully met at the entry candle
    "bad"     = entered but at least one condition was missing/wrong
"""

import sys
import os

# ── thresholds (must match scanner.py) ────────────────────────────────────────
MIN_SPREAD_PCT = 0.010   # % of e20 price
MIN_SLOPE_PCT  = 0.002   # % of e10 price
MIN_5M_STREAK  = 2

# ── ANSI colours ──────────────────────────────────────────────────────────────
GRN  = "\033[92m"; RED  = "\033[91m"; YLW  = "\033[93m"
CYN  = "\033[96m"; WHT  = "\033[97m"; DIM  = "\033[2m"
BOLD = "\033[1m";  RST  = "\033[0m"


def parse_file(path: str) -> list[dict]:
    """Return list of row dicts from a klines txt file."""
    rows = []
    with open(path) as f:
        lines = f.readlines()
    if not lines:
        return rows
    header = lines[0].split()
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != len(header):
            continue
        rows.append(dict(zip(header, parts)))
    return rows


def _is_perfect_long(row: dict) -> bool:
    """All LONG conditions fully met."""
    e20       = float(row["20ema"])
    e10       = float(row["10ema"])
    spread    = float(row["e10_vs_e20"])
    e10_slp   = float(row["e10_slp"])
    e20_slp   = float(row["e20_slp"])
    ha_bull   = int(row["ha_bull"])
    ha5m_bull = int(row["ha5m_bull"])
    ha5m_con  = int(row["ha5m_consec"])
    pd_zone   = int(row["pd_zone"])

    min_spread = e20 * MIN_SPREAD_PCT / 100
    min_slope  = e10 * MIN_SLOPE_PCT  / 100

    return (
        spread    >  min_spread and
        e10_slp   >  min_slope  and
        e20_slp   >  0          and
        ha_bull   == 1          and
        ha5m_bull == 1          and
        ha5m_con  >= MIN_5M_STREAK and
        pd_zone   >= 3
    )


def _is_perfect_short(row: dict) -> bool:
    """All SHORT conditions fully met."""
    e20       = float(row["20ema"])
    e10       = float(row["10ema"])
    spread    = float(row["e10_vs_e20"])
    e10_slp   = float(row["e10_slp"])
    e20_slp   = float(row["e20_slp"])
    ha_bull   = int(row["ha_bull"])
    ha5m_bull = int(row["ha5m_bull"])
    ha5m_con  = int(row["ha5m_consec"])
    pd_zone   = int(row["pd_zone"])

    min_spread = e20 * MIN_SPREAD_PCT / 100
    min_slope  = e10 * MIN_SLOPE_PCT  / 100

    return (
        spread    < -min_spread and
        e10_slp   < -min_slope  and
        e20_slp   <  0          and
        ha_bull   == 0          and
        ha5m_bull == 0          and
        ha5m_con  >= MIN_5M_STREAK and
        pd_zone   >= 3
    )


def _intended_direction(row: dict) -> str:
    """
    Guess the intended trade direction at entry from the dominant conditions.
    Uses ha5m_bull as the primary signal; fall back to spread / ha_bull.
    Returns 'long' or 'short'.
    """
    ha5m_bull = int(row["ha5m_bull"])
    spread    = float(row["e10_vs_e20"])
    ha_bull   = int(row["ha_bull"])

    # majority vote across three directional indicators
    long_votes  = (1 if ha5m_bull == 1 else 0) + \
                  (1 if spread    >  0  else 0) + \
                  (1 if ha_bull   == 1  else 0)
    short_votes = 3 - long_votes

    return "long" if long_votes >= short_votes else "short"


def classify(path: str) -> str:
    rows = parse_file(path)
    if not rows:
        return f"{RED}ERROR: no data rows found in file{RST}"

    entry_rows = [r for r in rows if r.get("is_entry", "0") == "1"]
    if not entry_rows:
        return f"{YLW}NO ENTRY: no is_entry=1 found in file{RST}"

    # Evaluate each entry candle; take worst classification if multiple
    results = []
    for row in entry_rows:
        t = row["time"]
        if _is_perfect_long(row):
            results.append(("perfect long", t))
        elif _is_perfect_short(row):
            results.append(("perfect short", t))
        else:
            # Determine what direction was intended, then mark as "bad"
            direction = _intended_direction(row)
            results.append((f"bad {direction}", t))

    # If multiple entries, show all; collapse to the dominant label for the
    # single-line summary (bad beats perfect if they disagree).
    labels = [r[0] for r in results]
    times  = [r[1] for r in results]

    # Unique labels (preserving order of first occurrence)
    seen = []; unique = []
    for l in labels:
        if l not in seen:
            seen.append(l); unique.append(l)

    summary_label = unique[0] if len(unique) == 1 else " / ".join(unique)

    # Colour coding
    if "perfect long" in summary_label:
        colour = GRN
    elif "perfect short" in summary_label:
        colour = RED
    elif "bad" in summary_label:
        colour = YLW
    else:
        colour = WHT

    fname = os.path.basename(path)
    time_str = ", ".join(times)

    # Detailed per-entry breakdown if multiple entries
    detail_lines = []
    if len(results) > 1:
        for label, t in results:
            c = GRN if "perfect" in label else YLW
            detail_lines.append(f"  {DIM}@{t}{RST}  {c}{BOLD}{label}{RST}")

    out = []
    out.append(f"\n  {CYN}{BOLD}ENTRY CHECK{RST}  {DIM}{fname}{RST}")
    out.append(f"  {DIM}entry candle(s):{RST} {time_str}")
    if detail_lines:
        out.extend(detail_lines)
    out.append(f"\n  {colour}{BOLD}{summary_label}{RST}\n")
    return "\n".join(out)


def _show_conditions(path: str):
    """Verbose mode: print each condition for entry row(s)."""
    rows = parse_file(path)
    entry_rows = [r for r in rows if r.get("is_entry", "0") == "1"]
    if not entry_rows:
        print(f"{YLW}No is_entry=1 rows.{RST}")
        return

    for row in entry_rows:
        e20       = float(row["20ema"])
        e10       = float(row["10ema"])
        spread    = float(row["e10_vs_e20"])
        e10_slp   = float(row["e10_slp"])
        e20_slp   = float(row["e20_slp"])
        ha_bull   = int(row["ha_bull"])
        ha5m_bull = int(row["ha5m_bull"])
        ha5m_con  = int(row["ha5m_consec"])
        pd_zone   = int(row["pd_zone"])
        min_spread = e20 * MIN_SPREAD_PCT / 100
        min_slope  = e10 * MIN_SLOPE_PCT  / 100

        def chk(cond): return f"{GRN}✓{RST}" if cond else f"{RED}✗{RST}"

        print(f"\n  {CYN}{BOLD}Conditions @ {row['time']}{RST}")
        print(f"  {chk(spread >  min_spread)} spread    {spread:+.2f}  (need > +{min_spread:.2f})")
        print(f"  {chk(spread < -min_spread)} spread    {spread:+.2f}  (need < -{min_spread:.2f})")
        print(f"  {chk(e10_slp >  min_slope)}  e10_slp  {e10_slp:+.2f}  (need > +{min_slope:.2f})")
        print(f"  {chk(e10_slp < -min_slope)}  e10_slp  {e10_slp:+.2f}  (need < -{min_slope:.2f})")
        print(f"  {chk(e20_slp > 0)}  e20_slp  {e20_slp:+.2f}  (need > 0)")
        print(f"  {chk(e20_slp < 0)}  e20_slp  {e20_slp:+.2f}  (need < 0)")
        print(f"  {chk(ha_bull == 1)}  ha_bull  {ha_bull}  (need 1 for long)")
        print(f"  {chk(ha_bull == 0)}  ha_bull  {ha_bull}  (need 0 for short)")
        print(f"  {chk(ha5m_bull == 1)}  ha5m_bull {ha5m_bull}  (need 1 for long)")
        print(f"  {chk(ha5m_bull == 0)}  ha5m_bull {ha5m_bull}  (need 0 for short)")
        print(f"  {chk(ha5m_con >= MIN_5M_STREAK)}  ha5m_con {ha5m_con}  (need >= {MIN_5M_STREAK})")
        print(f"  {chk(pd_zone >= 3)}  pd_zone  {pd_zone}  (need >= 3)")
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <klines_file.txt> [-v]")
        sys.exit(1)

    path = sys.argv[1]
    verbose = "-v" in sys.argv

    if not os.path.isfile(path):
        print(f"{RED}File not found: {path}{RST}")
        sys.exit(1)

    if verbose:
        _show_conditions(path)

    print(classify(path))
