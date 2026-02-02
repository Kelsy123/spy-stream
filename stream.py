print("✅ stream.py starting...", flush=True)

import asyncio
import json
import os
import requests
import websockets
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
import time as time_module
from typing import Optional, Tuple

# =========================
# SETTINGS
# =========================

SYMBOL = "SPY"

ET = ZoneInfo("America/New_York")
WS_URL = "wss://socket.massive.com/stocks"
MASSIVE_API_KEY = os.environ["MASSIVE_API_KEY"]

# Phantom detection thresholds
PHANTOM_OUTSIDE_PREV = 1.00      # Must be $1 outside previous day range
PHANTOM_OUTSIDE_RTH_MULT = 0.50  # Must be 50% outside current day’s range

# Normal RTH breakout buffer
RTH_BREAK_BUFFER = 0.10

# Alert cooldowns
PHANTOM_COOLDOWN = 120
RTH_COOLDOWN = 120

# Debug logging
LOG_ALL_TRADES = False


# =========================
# SIP CONDITION FILTERING
# =========================

# These conditions are normal and should be ignored.
IGNORE_CONDITIONS = {
    0, 37,    # Regular sale
    14,       # Auction/cross/official event
    41,       # Derivatively priced
    4, 9, 19, # Out-of-sequence / corrected
    53, 12,   # Odd lot / odd-lot cross
    1,        # Extended hours
}

# These conditions indicate potentially interesting off-book behavior.
PHANTOM_RELEVANT_CONDITIONS = {
    2,  # Special price
    3,  # Special sale
    7,  # Qualified contingent trade
    8,  # Trade through exempt
    16, # Market center open
    17, # Market center close
    20, # Market center move
    21, # Market center token
    22, # Market center price shift
    62, # Cross trade (TRF)
}


# =========================
# HELPERS
# =========================

def to_float(x):
    try:
        return float(x)
    except:
        return None


def ts_str():
    return datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %Z")


def get_previous_session_range(symbol: str) -> Tuple[float, float, str]:
    """
    Get previous day's high/low using Massive aggregates.
    Falls back to manual range if API fails.
    """
    print("Fetching previous session range...", flush=True)

    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=5)

        url = (
            f"https://api.massive.io/v1/stocks/aggregates/{symbol}/range/"
            f"1/day/{start_date}/{end_date}"
        )
        headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}

        r = requests.get(url, headers=headers, timeout=10)

        if r.status_code == 200:
            data = r.json().get("results", [])
            if len(data) >= 2:
                prev = data[-2]
                high = float(prev["h"])
                low = float(prev["l"])
                dstr = datetime.fromtimestamp(prev["t"]/1000).strftime("%Y-%m-%d")

                print(f"Previous session: {dstr} high={high} low={low}", flush=True)
                return high, low, dstr

        print(f"⚠️ Massive returned {r.status_code}, using fallback.", flush=True)

    except Exception as e:
        print(f"⚠️ API error: {e}", flush=True)

    # Fallback (safe wide range)
    fallback_date = (date.today() - timedelta(days=1)).isoformat()
    return 695.0, 685.0, fallback_date


# =========================
# MAIN RUNTIME
# =========================

async def run():
    print("Starting main loop...", flush=True)

    prev_high, prev_low, prev_date = get_previous_session_range(SYMBOL)

    rth_high = None
    rth_low = None

    last_phantom_alert = 0
    last_rth_alert = 0

    async with websockets.connect(WS_URL) as ws:

        # Authenticate
        await ws.send(json.dumps({"action": "auth", "params": MASSIVE_API_KEY}))
        print("Sent auth...", flush=True)
        print("Auth response:", await ws.recv(), flush=True)

        # Subscribe to SPY trades
        await ws.send(json.dumps({"action": "subscribe", "params": f"T.{SYMBOL}"}))
        print(f"Subscribed to {SYMBOL}", flush=True)

        async for raw in ws:
            try:
                msg = json.loads(raw)
            except:
                continue

            events = msg if isinstance(msg, list) else [msg]

            for e in events:
                if e.get("ev") != "T":
                    continue

                price = to_float(e.get("p"))
                if price is None:
                    continue

                size = e.get("s", 0)
                conds = e.get("c", [])
                exch = e.get("x")
                timestamp = e.get("t", 0)

                if LOG_ALL_TRADES:
                    print(f"TRADE: {price} size={size} cond={conds} exch={exch}")

                # Filter by time
                tm = datetime.fromtimestamp(timestamp/1000, tz=ET)
                is_rth = (
                    tm.weekday() < 5 and
                    time(9, 30) <= tm.time() <= time(16, 0)
                )

                # ------------------------------------
                # 1. Determine if this trade is phantom-relevant
                # ------------------------------------

                # Must be FINRA (TRF) off-exchange
                is_darkpool_source = (exch == 4)

                # Check condition filters
                cond_is_ok = (
                    any(c in PHANTOM_RELEVANT_CONDITIONS for c in conds)
                    and not any(c in IGNORE_CONDITIONS for c in conds)
                )

                outside_prev = (
                    price > prev_high + PHANTOM_OUTSIDE_PREV or
                    price < prev_low - PHANTOM_OUTSIDE_PREV
                )

                # Need current RTH range
                has_range = (rth_high is not None and rth_low is not None)

                if has_range:
                    current_range = rth_high - rth_low
                    phantom_gap = max(PHANTOM_OUTSIDE_PREV,
                                      current_range * PHANTOM_OUTSIDE_RTH_MULT)

                    outside_rth_far = (
                        price > rth_high + phantom_gap or
                        price < rth_low - phantom_gap
                    )
                else:
                    outside_rth_far = False

                # Final phantom decision
                is_phantom = (
                    is_darkpool_source and
                    cond_is_ok and
                    outside_prev and
                    outside_rth_far
                )

                now = time_module.time()

                if is_phantom and now - last_phantom_alert > PHANTOM_COOLDOWN:
                    last_phantom_alert = now
                    print(
                        f"🚨🚨 {ts_str()} PHANTOM PRINT DETECTED ${price} size={size} "
                        f"prev=[{prev_low},{prev_high}] rth=[{rth_low},{rth_high}] "
                        f"conds={conds} exch={exch}",
                        flush=True
                    )

                # ------------------------------------
                # 2. Normal RTH breakout alert
                # ------------------------------------
                if is_rth and has_range:
                    if (
                        price > rth_high + RTH_BREAK_BUFFER or
                        price < rth_low - RTH_BREAK_BUFFER
                    ):
                        if not is_phantom and now - last_rth_alert > RTH_COOLDOWN:
                            last_rth_alert = now
                            print(
                                f"🚨 {ts_str()} RTH BREAKOUT {price} rth=[{rth_low},{rth_high}]",
                                flush=True
                            )

                # ------------------------------------
                # 3. Update RTH range sanely
                # ------------------------------------
                if is_rth:
                    if rth_high is None:
                        rth_low = price
                        rth_high = price
                    else:
                        # Do not update range if the trade is phantom or absurd
                        if not is_phantom:
                            rth_low = min(rth_low, price)
                            rth_high = max(rth_high, price)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        import traceback
        print("❌ Crash:", e)
        traceback.print_exc()
        raise
