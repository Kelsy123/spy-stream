print("✅ stream.py starting up...", flush=True)

import asyncio
import json
from datetime import date, timedelta, datetime, time
from zoneinfo import ZoneInfo
import os
import time as time_module
from typing import Optional, Tuple

import requests
import websockets

# =========================
# Settings
# =========================
SYMBOL = "SPY"

# Phantom detection: must be outside BOTH previous AND current range
PHANTOM_THRESHOLD = 1.00  # dollars outside range to trigger

# Current regular-session range break threshold (for non-phantom breakouts)
CURRENT_RANGE_BUFFER = 0.10

# Cooldown (seconds) to avoid alert spam
PHANTOM_COOLDOWN_SEC = 120   # 2 minutes
RTH_COOLDOWN_SEC = 120       # 2 minutes

# -------------------------
# OPTIONAL: Watch exact levels
# -------------------------
ENABLE_WATCH = False
WATCH_PRICES = []  # Add specific prices like [670.29] if needed
WATCH_TOLERANCE = 0.02  # 2 cents

# =========================
# Massive.io API Configuration
# =========================
print("✅ env keys include MASSIVE_API_KEY?", "MASSIVE_API_KEY" in os.environ, flush=True)
MASSIVE_API_KEY = os.environ["MASSIVE_API_KEY"]  # Set this in Railway environment variables

# Massive WebSocket endpoint - Real-time feed
WS_URL = "wss://socket.massive.com/stocks"

ET = ZoneInfo("America/New_York")


def get_previous_session_range(symbol: str) -> Tuple[float, float, str]:
    """
    Fetches the most recent prior trading day using Yahoo Finance (free, no API key needed).
    Returns (high, low, date_str).
    """
    print("✅ fetching previous session from Yahoo Finance...", flush=True)
    
    # Try last 7 days to skip weekends/holidays
    for days_back in range(1, 8):
        end_date = date.today() - timedelta(days=days_back)
        start_date = end_date - timedelta(days=1)
        
        # Yahoo Finance URL format
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            "interval": "1d",
            "period1": int(start_date.timestamp()),
            "period2": int(end_date.timestamp()),
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Parse Yahoo Finance response
            result = data.get("chart", {}).get("result", [])
            if not result:
                continue
                
            quotes = result[0].get("indicators", {}).get("quote", [])
            if not quotes or not quotes[0]:
                continue
            
            quote = quotes[0]
            highs = quote.get("high", [])
            lows = quote.get("low", [])
            
            if highs and lows and highs[0] is not None and lows[0] is not None:
                prev_high = float(highs[0])
                prev_low = float(lows[0])
                prev_date = end_date.isoformat()
                print(f"✅ Found previous session: {prev_date} high={prev_high} low={prev_low}", flush=True)
                return prev_high, prev_low, prev_date
                
        except Exception as e:
            print(f"⚠️ Yahoo Finance fetch failed for {end_date}: {e}", flush=True)
            continue
    
    raise RuntimeError(f"Could not fetch previous session range for {symbol} (tried last 7 days)")


def to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def ts_str() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %Z")


async def run():
    print("✅ run() entered", flush=True)

    print("✅ fetching previous session range...", flush=True)
    prev_high, prev_low, prev_date = get_previous_session_range(SYMBOL)
    print(f"Previous session ({prev_date}) high={prev_high} low={prev_low}", flush=True)

    # Current regular-session range tracking
    rth_high = None
    rth_low = None

    # Cooldown timers
    last_phantom_alert_ts = 0.0
    last_rth_alert_ts = 0.0
    last_watch_alert_ts = 0.0

    print("✅ connecting to Massive websocket...", flush=True)
    
    async with websockets.connect(WS_URL) as ws:
        print("✅ websocket connected", flush=True)

        # Step 1: Authenticate with API key
        auth_msg = {"action": "auth", "params": MASSIVE_API_KEY}
        await ws.send(json.dumps(auth_msg))
        print("✅ Authentication message sent", flush=True)
        
        # Wait for auth confirmation
        auth_response = await ws.recv()
        print(f"✅ Auth response: {auth_response}", flush=True)

        # Step 2: Subscribe to SPY trades
        subscribe_msg = {"action": "subscribe", "params": f"T.{SYMBOL}"}
        await ws.send(json.dumps(subscribe_msg))
        print(f"✅ Subscribed to {SYMBOL} trades", flush=True)

        # Process incoming messages
        async for message in ws:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            # Handle array of events or single event
            events = data if isinstance(data, list) else [data]
            
            for event in events:
                # Massive trade format: {"ev":"T","sym":"SPY","p":686.50,"s":100,"t":1234567890000}
                if event.get("ev") != "T":  # T = Trade
                    continue
                
                last = to_float(event.get("p"))  # price
                if last is None:
                    continue
                
                size = event.get("s", 0)  # size
                timestamp = event.get("t", 0)  # timestamp in milliseconds
                now = time_module.time()
                
                # Determine if this is regular trading hours (9:30 AM - 4:00 PM ET)
                trade_time = datetime.fromtimestamp(timestamp / 1000, tz=ET)
                is_regular_hours = (
                    trade_time.weekday() < 5 and  # Monday-Friday
                    time(9, 30) <= trade_time.time() <= time(16, 0)
                )
                
                # =========================
                # OPTIONAL: Watch exact levels
                # =========================
                if ENABLE_WATCH and WATCH_PRICES:
                    if now - last_watch_alert_ts >= 1.0:
                        for wp in WATCH_PRICES:
                            if abs(last - wp) <= WATCH_TOLERANCE:
                                last_watch_alert_ts = now
                                print(
                                    f"👀 {ts_str()} WATCH HIT: last={last} size={size} "
                                    f"tolerance=±{WATCH_TOLERANCE} level={wp}",
                                    flush=True
                                )
                                break

                # =========================
                # PHANTOM PRINT DETECTION
                # Must be outside BOTH previous range AND current RTH range
                # =========================
                outside_prev = (last > prev_high + PHANTOM_THRESHOLD) or (last < prev_low - PHANTOM_THRESHOLD)
                
                # Only flag as phantom if we have established an RTH range AND price is outside it too
                is_phantom = False
                if outside_prev and rth_high is not None and rth_low is not None:
                    outside_current = (last > rth_high + PHANTOM_THRESHOLD) or (last < rth_low - PHANTOM_THRESHOLD)
                    is_phantom = outside_current

                if is_phantom and (now - last_phantom_alert_ts >= PHANTOM_COOLDOWN_SEC):
                    last_phantom_alert_ts = now
                    print(
                        f"🚨🚨 {ts_str()} PHANTOM PRINT DETECTED: ${last} size={size} "
                        f"prev_range=[{prev_low}, {prev_high}] rth_range=[{rth_low}, {rth_high}]",
                        flush=True
                    )

                # =========================
                # CURRENT RTH range tracking (regular session only)
                # =========================
                if is_regular_hours:
                    # Check for legitimate RTH range breakout
                    if rth_high is not None and rth_low is not None:
                        outside_curr = (last > rth_high + CURRENT_RANGE_BUFFER) or (last < rth_low - CURRENT_RANGE_BUFFER)

                        # Only alert if it's NOT a phantom (already alerted above)
                        if outside_curr and not is_phantom and (now - last_rth_alert_ts >= RTH_COOLDOWN_SEC):
                            last_rth_alert_ts = now
                            print(
                                f"🚨 {ts_str()} CURRENT RTH RANGE BREAK: ${last} size={size} "
                                f"rth_range=[{rth_low}, {rth_high}]",
                                flush=True
                            )

                    # Update RTH range normally
                    rth_low = last if rth_low is None else min(rth_low, last)
                    rth_high = last if rth_high is None else max(rth_high, last)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        import traceback
        print("❌ Script crashed:", repr(e), flush=True)
        traceback.print_exc()
        raise
