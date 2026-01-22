print("✅ stream.py starting up...", flush=True)

import asyncio
import json
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
import os
import time
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
# Env / Tradier endpoints
# =========================
print("✅ env keys include TRADIER_TOKEN?", "TRADIER_TOKEN" in os.environ, flush=True)
TRADIER_TOKEN = os.environ["TRADIER_TOKEN"]

SESSION_URL = "https://api.tradier.com/v1/markets/events/session"
WS_URL = "wss://ws.tradier.com/v1/markets/events"
HISTORY_URL = "https://api.tradier.com/v1/markets/history"

HEADERS = {
    "Authorization": f"Bearer {TRADIER_TOKEN}",
    "Accept": "application/json",
}

ET = ZoneInfo("America/New_York")


def create_session_id() -> str:
    print("✅ requesting Tradier sessionid...", flush=True)
    r = requests.post(SESSION_URL, headers=HEADERS, timeout=15)
    print("✅ session endpoint status:", r.status_code, flush=True)
    r.raise_for_status()

    data = r.json()
    if "stream" not in data or "sessionid" not in data["stream"]:
        raise RuntimeError(f"Unexpected session response: {data}")

    sid = data["stream"]["sessionid"]
    print("✅ got sessionid", flush=True)
    return sid


def get_previous_session_range(symbol: str) -> Tuple[float, float, str]:
    """
    Finds the most recent prior trading day daily bar (search back up to 7 calendar days).
    Returns (high, low, date_str).
    """
    for back in range(1, 8):
        d = date.today() - timedelta(days=back)
        params = {
            "symbol": symbol,
            "interval": "daily",
            "start": d.isoformat(),
            "end": d.isoformat(),
        }
        r = requests.get(HISTORY_URL, headers=HEADERS, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()

        days = (data.get("history") or {}).get("day")
        if not days:
            continue

        day = days if isinstance(days, dict) else days[0]
        return float(day["high"]), float(day["low"]), day["date"]

    raise RuntimeError("No previous session bar found (tried last 7 days)")


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

    print("✅ creating streaming session...", flush=True)
    sessionid = create_session_id()
    print("✅ Session ID created", flush=True)

    # Current regular-session range tracking
    rth_high = None
    rth_low = None

    # Cooldown timers
    last_phantom_alert_ts = 0.0
    last_rth_alert_ts = 0.0
    last_watch_alert_ts = 0.0

    sub_payload = {
        "symbols": [SYMBOL],
        "filter": ["timesale"],
        "sessionid": sessionid,
        "linebreak": True,
        "validOnly": True,
    }

    print("✅ connecting to websocket...", flush=True)
    async with websockets.connect(WS_URL, ssl=True, compression=None) as ws:
        print("✅ websocket connected", flush=True)

        await ws.send(json.dumps(sub_payload))
        print("✅ Subscribed to SPY timesales", flush=True)

        async for message in ws:
            for line in message.splitlines():
                line = line.strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if event.get("type") != "timesale":
                    continue

                last = to_float(event.get("last"))
                if last is None:
                    continue

                size = event.get("size")
                sess = event.get("session")
                now = time.time()

                # =========================
                # OPTIONAL: Watch exact levels
                # =========================
                if ENABLE_WATCH and WATCH_PRICES:
                    if now - last_watch_alert_ts >= 1.0:
                        for wp in WATCH_PRICES:
                            if abs(last - wp) <= WATCH_TOLERANCE:
                                last_watch_alert_ts = now
                                print(
                                    f"👀 {ts_str()} WATCH HIT: last={last} size={size} session={sess} "
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
                        f"🚨🚨 {ts_str()} PHANTOM PRINT DETECTED: ${last} size={size} session={sess} "
                        f"prev_range=[{prev_low}, {prev_high}] rth_range=[{rth_low}, {rth_high}]",
                        flush=True
                    )

                # =========================
                # CURRENT RTH range tracking (regular session only)
                # =========================
                if sess == "regular":
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
