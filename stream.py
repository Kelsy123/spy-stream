print("✅ stream.py starting up...", flush=True)

import asyncio
import json
from datetime import date, timedelta
import os

print("✅ env keys include TRADIER_TOKEN?", "TRADIER_TOKEN" in os.environ, flush=True)

import requests
import websockets

# ===== Environment Variables =====
TRADIER_TOKEN = os.environ["TRADIER_TOKEN"]
SYMBOL = "SPY"

PREV_RANGE_BUFFER = 0.10
CURRENT_RANGE_BUFFER = 0.10
CONFIRM_TRADES = 3

SESSION_URL = "https://api.tradier.com/v1/markets/events/session"
WS_URL = "wss://ws.tradier.com/v1/markets/events"
HISTORY_URL = "https://api.tradier.com/v1/markets/history"

HEADERS = {
    "Authorization": f"Bearer {TRADIER_TOKEN}",
    "Accept": "application/json",
}


def create_session_id() -> str:
    print("✅ requesting Tradier sessionid...", flush=True)
    r = requests.post(SESSION_URL, headers=HEADERS, timeout=15)
    print("✅ session endpoint status:", r.status_code, flush=True)
    r.raise_for_status()

    data = r.json()
    # This helps if Tradier ever returns a different shape
    if "stream" not in data or "sessionid" not in data["stream"]:
        raise RuntimeError(f"Unexpected session response: {data}")

    sid = data["stream"]["sessionid"]
    print("✅ got sessionid", flush=True)
    return sid


def get_previous_session_range(symbol: str):
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
    raise RuntimeError("No previous session bar found")


def to_float(x):
    try:
        return float(x)
    except Exception:
        return None


async def run():
    print("✅ run() entered", flush=True)

    print("✅ fetching previous session range...", flush=True)
    prev_high, prev_low, prev_date = get_previous_session_range(SYMBOL)
    print(f"Previous session ({prev_date}) high={prev_high} low={prev_low}", flush=True)

    print("✅ creating streaming session...", flush=True)
    sessionid = create_session_id()
    print("✅ Session ID created", flush=True)

    rth_high = None
    rth_low = None
    prev_confirm = 0
    curr_confirm = 0

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

                event = json.loads(line)
                if event.get("type") != "timesale":
                    continue

                last = to_float(event.get("last"))
                if last is None:
                    continue

                size = event.get("size")
                sess = event.get("session")

                # ---- Outside previous session ----
                outside_prev = (last > prev_high + PREV_RANGE_BUFFER) or (last < prev_low - PREV_RANGE_BUFFER)
                prev_confirm = prev_confirm + 1 if outside_prev else 0

                if prev_confirm == CONFIRM_TRADES:
                    print(f"🚨 PREVIOUS RANGE BREAK: {last}  size={size}", flush=True)

                # ---- Track current RTH ----
                if sess != "regular":
                    continue

                rth_low = last if rth_low is None else min(rth_low, last)
                rth_high = last if rth_high is None else max(rth_high, last)

                outside_curr = (last > rth_high + CURRENT_RANGE_BUFFER) or (last < rth_low - CURRENT_RANGE_BUFFER)
                curr_confirm = curr_confirm + 1 if outside_curr else 0

                if curr_confirm == CONFIRM_TRADES:
                    print(f"🚨 CURRENT RTH RANGE BREAK: {last}  size={size}", flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        import traceback
        print("❌ Script crashed:", repr(e), flush=True)
        traceback.print_exc()
        raise
