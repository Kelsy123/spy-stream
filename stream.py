import asyncio
import json
from datetime import date, timedelta
import os

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


def create_session_id():
    r = requests.post(SESSION_URL, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()["stream"]["sessionid"]


def get_previous_session_range(symbol):
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
        if isinstance(days, dict):
            day = days
        else:
            day = days[0]
        return float(day["high"]), float(day["low"]), day["date"]
    raise RuntimeError("No previous session bar found")


def to_float(x):
    try:
        return float(x)
    except:
        return None


async def run():
    prev_high, prev_low, prev_date = get_previous_session_range(SYMBOL)
    print(f"Previous session ({prev_date}) high={prev_high} low={prev_low}")

    sessionid = create_session_id()
    print("Session ID created")

    rth_high = None
    rth_low = None
    prev_confirm = 0
    curr_confirm = 0

    sub_payload = {
        "symbols": [SYMBOL],
        "filter": ["timesale"],
        "sessionid": sessionid,
        "linebreak": True,
        "validOnly": True
    }

    async with websockets.connect(WS_URL, ssl=True, compression=None) as ws:
        await ws.send(json.dumps(sub_payload))
        print("Subscribed to SPY timesales")

        async for message in ws:
            for line in message.splitlines():
                if not line.strip():
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
                    print(f"🚨 PREVIOUS RANGE BREAK: {last}  size={size}")

                # ---- Track current RTH ----
                if sess != "regular":
                    continue

                rth_low = last if rth_low is None else min(rth_low, last)
                rth_high = last if rth_high is None else max(rth_high, last)

                outside_curr = (last > rth_high + CURRENT_RANGE_BUFFER) or (last < rth_low - CURRENT_RANGE_BUFFER)
                curr_confirm = curr_confirm + 1 if outside_curr else 0

                if curr_confirm == CONFIRM_TRADES:
                    print(f"🚨 CURRENT RTH RANGE BREAK: {last}  size={size}")


if __name__ == "__main__":
    asyncio.run(run())
