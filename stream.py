import asyncio
import json
import os
import requests
import websockets
import asyncpg
import aiohttp
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
import time as time_module

# ======================================================
# SETTINGS
# ======================================================
SYMBOL = "SPY"
ET = ZoneInfo("America/New_York")
WS_URL = "wss://socket.massive.com/stocks"
MASSIVE_API_KEY = os.environ["MASSIVE_API_KEY"]
POSTGRES_URL = os.environ["POSTGRES_URL"]
DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]
TRADIER_API_KEY = os.environ["TRADIER_API_KEY"]  # <-- Set this in Railway

# Phantom thresholds
PHANTOM_OUTSIDE_PREV = 1.00
PHANTOM_OUTSIDE_RTH_MULT = 0.50

# Cooldowns
PHANTOM_COOLDOWN = 5
RTH_COOLDOWN = 120

# RTH breakout buffer
RTH_BREAK_BUFFER = 0.10

# Debug
LOG_ALL_TRADES = False

# ======================================================
# SIP CONDITION FILTERING
# ======================================================
IGNORE_CONDITIONS = {
    0, 37, 14, 41, 4, 9, 19, 53, 12, 1
}
PHANTOM_RELEVANT_CONDITIONS = {
    2, 3, 7, 8, 16, 17, 20, 21, 22, 62
}

# ======================================================
# HELPERS
# ======================================================
def to_float(x):
    try:
        return float(x)
    except:
        return None

def ts_str():
    return datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S %Z")

# ======================================================
# DISCORD ALERTS
# ======================================================
async def send_discord(msg: str):
    async with aiohttp.ClientSession() as session:
        await session.post(DISCORD_WEBHOOK_URL, json={"content": msg})

# ======================================================
# FETCH PREVIOUS DAY RANGE FROM TRADIER
# ======================================================
def fetch_prev_day_range_tradier(symbol, tradier_api_key):
    yesterday = date.today() - timedelta(days=1)
    url = f"https://api.tradier.com/v1/markets/history"
    params = {
        "symbol": symbol,
        "start": yesterday.strftime("%Y-%m-%d"),
        "end": yesterday.strftime("%Y-%m-%d"),
        "interval": "daily"
    }
    headers = {
        "Authorization": f"Bearer {tradier_api_key}",
        "Accept": "application/json"
    }
    r = requests.get(url, params=params, headers=headers)
    if r.status_code == 200:
        data = r.json()
        day = data.get("history", {}).get("day", [])
        if day:
            low = float(day[0]["low"])
            high = float(day[0]["high"])
            print(f"➡️ Previous day range from Tradier: low={low} high={high}", flush=True)
            return low, high
        else:
            print("⚠️ No data returned from Tradier", flush=True)
            return None, None
    else:
        print(f"⚠️ Tradier fetch error {r.status_code}: {r.text}", flush=True)
        return None, None

# ======================================================
# POSTGRES INIT
# ======================================================
async def init_postgres():
    conn = await asyncpg.connect(POSTGRES_URL)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS phantoms (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL,
        sip_ts TIMESTAMPTZ,
        trf_ts TIMESTAMPTZ,
        price DOUBLE PRECISION NOT NULL,
        size INTEGER NOT NULL,
        conditions JSONB NOT NULL,
        exchange INTEGER NOT NULL,
        sequence BIGINT,
        trf_id INTEGER
    );
    """)
    print("🗄️ Postgres table ready.", flush=True)
    return conn

# ======================================================
# WEBSOCKET WITH BACKOFF
# ======================================================
async def connect_with_backoff():
    delay = 2
    while True:
        try:
            return await websockets.connect(WS_URL)
        except Exception as e:
            print(f"⚠️ Websocket connect failed ({e}), retrying in {delay}s", flush=True)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

# ======================================================
# MAIN LOOP
# ======================================================
async def run():
    print("🚀 Starting main loop...", flush=True)
    db = await init_postgres()

    # Get previous day's full session range from Tradier
    prev_low, prev_high = fetch_prev_day_range_tradier(SYMBOL, TRADIER_API_KEY)
    print(f"📌 Previous day range: low={prev_low} high={prev_high}", flush=True)

    # Initialize today's session ranges
    today_low, today_high = None, None
    premarket_low, premarket_high = None, None
    rth_low, rth_high = None, None
    afterhours_low, afterhours_high = None, None

    last_phantom_alert = 0
    last_rth_alert = 0

    while True:
        ws = await connect_with_backoff()
        await ws.send(json.dumps({"action": "auth", "params": MASSIVE_API_KEY}))
        print("🔑 Sent auth...", flush=True)
        print("🔑 Auth response:", await ws.recv(), flush=True)
        await ws.send(json.dumps({"action": "subscribe", "params": f"T.{SYMBOL}"}))
        print(f"📡 Subscribed to {SYMBOL}", flush=True)
        try:
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
                    sip_ts_raw = e.get("t")
                    trf_ts_raw = e.get("trft")
                    sequence = e.get("q")
                    trf_id = e.get("trfi")

                    if LOG_ALL_TRADES:
                        print(f"TRADE {price} size={size} cond={conds} exch={exch}")

                    tm = datetime.fromtimestamp(sip_ts_raw/1000, tz=ET).time()

                    # Update today's full session range
                    if today_low is None or price < today_low:
                        today_low = price
                    if today_high is None or price > today_high:
                        today_high = price

                    # Update session-specific ranges
                    if time(4,0) <= tm < time(9,30):
                        # Premarket
                        if premarket_low is None or price < premarket_low:
                            premarket_low = price
                        if premarket_high is None or price > premarket_high:
                            premarket_high = price
                    elif time(9,30) <= tm < time(16,0):
                        # RTH
                        if rth_low is None or price < rth_low:
                            rth_low = price
                        if rth_high is None or price > rth_high:
                            rth_high = price
                    elif time(16,0) <= tm <= time(20,0):
                        # After-hours
                        if afterhours_low is None or price < afterhours_low:
                            afterhours_low = price
                        if afterhours_high is None or price > afterhours_high:
                            afterhours_high = price

                    bad_conditions = any(c in IGNORE_CONDITIONS for c in conds)
                    is_darkpool = (exch == 4)
                    phantom_cond_ok = (
                        any(c in PHANTOM_RELEVANT_CONDITIONS for c in conds)
                        and not bad_conditions
                    )
                    outside_prev = (
                        price > prev_high + PHANTOM_OUTSIDE_PREV or
                        price < prev_low - PHANTOM_OUTSIDE_PREV
                    )
                    current_range = today_high - today_low if today_low is not None and today_high is not None else 0
                    phantom_gap = max(PHANTOM_OUTSIDE_PREV, current_range * PHANTOM_OUTSIDE_RTH_MULT)
                    outside_rth_far = (
                        price > today_high + phantom_gap or
                        price < today_low - phantom_gap
                    )
                    is_phantom = (
                        is_darkpool and phantom_cond_ok and
                        outside_prev and outside_rth_far
                    )
                    now = time_module.time()
                    if is_phantom:
                        # ALWAYS PRINT TO CONSOLE
                        print(
                            f"🚨🚨 PHANTOM PRINT {ts_str()} ${price} "
                            f"size={size} conds={conds} exch={exch} seq={sequence}",
                            flush=True
                        )
                        # ALWAYS SEND TO DISCORD
                        msg = (
                            f"🚨 Phantom Print Detected\n"
                            f"Price: ${price}\n"
                            f"Size: {size}\n"
                            f"Exchange: {exch}\n"
                            f"Conditions: {conds}\n"
                            f"SIP Time: {datetime.fromtimestamp(sip_ts_raw/1000, tz=ET)}\n"
                            f"TRF Time: {datetime.fromtimestamp(trf_ts_raw/1000, tz=ET) if trf_ts_raw else 'None'}\n"
                            f"Sequence: {sequence}\n"
                            f"TRF ID: {trf_id}"
                        )
                        asyncio.create_task(send_discord(msg))
                        # INSERT INTO POSTGRES
                        await db.execute("""
                        INSERT INTO phantoms (
                            ts, sip_ts, trf_ts, price, size,
                            conditions, exchange, sequence, trf_id
                        )
                        VALUES (
                            NOW(),
                            to_timestamp($1 / 1000.0),
                            to_timestamp($2 / 1000.0),
                            $3, $4, $5, $6, $7, $8
                        );
                        """, sip_ts_raw, trf_ts_raw, price, size,
                        json.dumps(conds), exch, sequence, trf_id)
                        # WINDOW LOGIC
                        if now - last_phantom_alert > PHANTOM_COOLDOWN:
                            last_phantom_alert = now
                            print("🔥🔥 NEW PHANTOM WINDOW OPEN 🔥🔥", flush=True)
                        else:
                            print("⏳ (suppressed due to cooldown)", flush=True)
                    # RTH breakout logic (optional, can be adapted for other sessions)
                    if time(9,30) <= tm < time(16,0) and not bad_conditions:
                        if price > rth_high + RTH_BREAK_BUFFER or price < rth_low - RTH_BREAK_BUFFER:
                            if not is_phantom and now - last_rth_alert > RTH_COOLDOWN:
                                last_rth_alert = now
                                print(
                                    f"🚨 BREAKOUT {ts_str()} ${price} "
                                    f"size={size} conds={conds} exch={exch} "
                                    f"rth=[{rth_low},{rth_high}]",
                                    flush=True
                                )
        except Exception as e:
            print(f"⚠️ Websocket closed: {e}", flush=True)
            print("🔁 Reconnecting…", flush=True)
            await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        import traceback
        print("❌ Fatal crash:", e, flush=True)
        traceback.print_exc()
        raise
