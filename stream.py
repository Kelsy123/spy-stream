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

# -------------------------
# Trade filtering options
# -------------------------
# Log ALL trades for debugging (set to True to see every trade)
LOG_ALL_TRADES = False

# Suspicious trade conditions to flag (from Massive's glossary)
# These often indicate irregular trades that might be phantoms
SUSPICIOUS_CONDITIONS = {
    14,  # Odd lot trade
    37,  # Intermarket sweep
    41,  # Derivatively priced
}

# Alert on unusual trade sizes
MIN_SUSPICIOUS_SIZE = 1      # Trades smaller than this
MAX_SUSPICIOUS_SIZE = 50000  # Trades larger than this

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
    Fetches the most recent prior trading day from Massive's own API.
    Falls back to manual range if API fails.
    Returns (high, low, date_str).
    """
    print("✅ fetching previous session range from Massive API...", flush=True)
    
    # Try Massive's aggregates endpoint for previous day
    try:
        # Get data for last 5 days to ensure we get a complete trading day
        end_date = date.today()
        start_date = end_date - timedelta(days=5)
        
        url = f"https://api.massive.io/v1/stocks/aggregates/{symbol}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}"
        headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            
            # Get the second-to-last day (most recent complete day)
            if len(results) >= 2:
                prev_day = results[-2]  # -1 might be today (incomplete), so use -2
                prev_high = float(prev_day["h"])
                prev_low = float(prev_day["l"])
                prev_timestamp = prev_day["t"]
                prev_date = datetime.fromtimestamp(prev_timestamp / 1000).strftime("%Y-%m-%d")
                
                print(f"✅ Found previous session: {prev_date} high={prev_high} low={prev_low}", flush=True)
                return prev_high, prev_low, prev_date
                
        print(f"⚠️ Massive API returned status {response.status_code}", flush=True)
                
    except Exception as e:
        print(f"⚠️ Massive API fetch failed: {e}", flush=True)
    
    # Fallback: Use current SPY approximate range based on recent trading
    print("⚠️ API fetch failed, using conservative fallback range", flush=True)
    prev_date = (date.today() - timedelta(days=1)).isoformat()
    
    # Based on current market conditions (SPY ~692), set reasonable bounds
    # These are intentionally wide to avoid false positives
    prev_high = 695.0  # Update manually if needed
    prev_low = 685.0   # Update manually if needed
    
    print(f"⚠️ Using fallback range: {prev_date} high={prev_high} low={prev_low}", flush=True)
    print("⚠️ WARNING: Manual range in use - update these values or fix API!", flush=True)
    
    return prev_high, prev_low, prev_date


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
                # Massive trade format: {"ev":"T","sym":"SPY","p":686.50,"s":100,"t":1234567890000,"c":[...]}
                if event.get("ev") != "T":  # T = Trade
                    continue
                
                last = to_float(event.get("p"))  # price
                if last is None:
                    continue
                
                size = event.get("s", 0)  # size
                timestamp = event.get("t", 0)  # timestamp in milliseconds
                conditions = event.get("c", [])  # trade conditions
                exchange = event.get("x")  # exchange ID
                now = time_module.time()
                
                # Log trade details for debugging
                if LOG_ALL_TRADES:
                    print(f"📊 Trade: ${last} size={size} conditions={conditions} exchange={exchange}", flush=True)
                
                # Flag suspicious trade characteristics
                has_suspicious_condition = any(c in SUSPICIOUS_CONDITIONS for c in conditions)
                has_unusual_size = size < MIN_SUSPICIOUS_SIZE or size > MAX_SUSPICIOUS_SIZE
                
                if has_suspicious_condition or has_unusual_size:
                    print(
                        f"⚠️ {ts_str()} UNUSUAL TRADE: ${last} size={size} "
                        f"conditions={conditions} exchange={exchange}",
                        flush=True
                    )
                
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
                # Must be SIGNIFICANTLY outside BOTH previous AND current range
                # Use larger threshold for phantom detection vs normal range breaks
                # =========================
                
                # Check if outside previous session range
                outside_prev = (last > prev_high + PHANTOM_THRESHOLD) or (last < prev_low - PHANTOM_THRESHOLD)
                
                # Only flag as phantom if we have established an RTH range AND price is WAY outside it
                is_phantom = False
                if outside_prev and rth_high is not None and rth_low is not None:
                    # For phantom detection, require a BIGGER gap from current range
                    # This filters out normal breakouts that gradually expand the range
                    phantom_gap_threshold = max(PHANTOM_THRESHOLD, (rth_high - rth_low) * 0.5)  # At least 50% of current range
                    
                    outside_current_far = (last > rth_high + phantom_gap_threshold) or (last < rth_low - phantom_gap_threshold)
                    is_phantom = outside_current_far

                if is_phantom and (now - last_phantom_alert_ts >= PHANTOM_COOLDOWN_SEC):
                    last_phantom_alert_ts = now
                    distance_from_range = min(abs(last - rth_high), abs(last - rth_low))
                    print(
                        f"🚨🚨 {ts_str()} PHANTOM PRINT DETECTED: ${last} size={size} "
                        f"prev_range=[{prev_low}, {prev_high}] rth_range=[{rth_low}, {rth_high}] "
                        f"distance=${distance_from_range:.2f} conditions={conditions} exchange={exchange}",
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

                    # Update RTH range - BUT NOT if this looks like a phantom print
                    # Only update if the move is reasonable (not a massive gap)
                    current_range = rth_high - rth_low if rth_high and rth_low else 0
                    max_reasonable_move = max(2.0, current_range * 1.5)  # Max 1.5x current range or $2
                    
                    if rth_high is None or rth_low is None:
                        # First trades of the day - always accept
                        rth_low = last
                        rth_high = last
                    elif not is_phantom:
                        # Only update range if move is reasonable
                        distance_from_range = max(0, last - rth_high, rth_low - last)
                        
                        if distance_from_range <= max_reasonable_move:
                            # Normal move - update range
                            rth_low = min(rth_low, last)
                            rth_high = max(rth_high, last)
                        else:
                            # Suspicious move - don't update range, but log it
                            print(
                                f"⚠️ {ts_str()} SUSPICIOUS MOVE (not updating range): ${last} "
                                f"distance=${distance_from_range:.2f} max_reasonable=${max_reasonable_move:.2f}",
                                flush=True
                            )


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        import traceback
        print("❌ Script crashed:", repr(e), flush=True)
        traceback.print_exc()
        raise
