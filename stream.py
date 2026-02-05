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
TRADIER_API_KEY = os.environ["TRADIER_API_KEY"]

# Phantom thresholds
PHANTOM_OUTSIDE_PREV = 0.10
PHANTOM_GAP_FROM_CURRENT = 0.25  # Fixed 25 cent gap from current day's range

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
    0, 14, 4, 9, 19, 53, 1
}
PHANTOM_RELEVANT_CONDITIONS = {
    2, 3, 7, 8, 10, 12, 13, 15, 16, 17, 20, 21, 22, 25, 26, 28, 29, 30, 33, 34, 37, 62
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
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(DISCORD_WEBHOOK_URL, json={"content": msg})
    except Exception as e:
        print(f"⚠️ Discord webhook failed: {e}", flush=True)

# ======================================================
# FETCH PREVIOUS DAY RANGE FROM TRADIER (PRIMARY)
# ======================================================
def fetch_prev_day_range_tradier(symbol, tradier_api_key):
    """
    Fetch the most recent complete trading day (skips weekends/holidays)
    """
    print("📅 Fetching previous day range from Tradier (primary)...", flush=True)
    
    # Try last 7 days to skip weekends/holidays
    for days_back in range(1, 8):
        target_date = date.today() - timedelta(days=days_back)
        
        url = "https://api.tradier.com/v1/markets/history"
        params = {
            "symbol": symbol,
            "start": target_date.strftime("%Y-%m-%d"),
            "end": target_date.strftime("%Y-%m-%d"),
            "interval": "daily"
        }
        headers = {
            "Authorization": f"Bearer {tradier_api_key}",
            "Accept": "application/json"
        }
        
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            
            if r.status_code != 200:
                print(f"⚠️ Tradier returned {r.status_code} for {target_date}", flush=True)
                continue
            
            data = r.json()
            
            # Handle Tradier's response format
            history = data.get("history")
            if not history:
                continue
            
            day_data = history.get("day")
            if not day_data:
                continue
            
            # day_data can be a dict (single day) or list (multiple days)
            if isinstance(day_data, dict):
                day = day_data
            elif isinstance(day_data, list) and len(day_data) > 0:
                day = day_data[0]
            else:
                continue
            
            low = float(day["low"])
            high = float(day["high"])
            date_str = day.get("date", target_date.strftime("%Y-%m-%d"))
            
            print(f"✅ Previous day range from Tradier ({date_str}): low={low} high={high}", flush=True)
            return low, high
            
        except Exception as e:
            print(f"⚠️ Tradier fetch error for {target_date}: {e}", flush=True)
            continue
    
    # If all attempts fail, return None
    print("❌ Tradier failed after 7 attempts", flush=True)
    return None, None


# ======================================================
# FETCH PREVIOUS DAY RANGE FROM MASSIVE (BACKUP)
# ======================================================
def fetch_prev_day_range_massive(symbol, massive_api_key):
    """
    Backup method using Massive's REST API for daily aggregates
    """
    print("📅 Fetching previous day range from Massive (backup)...", flush=True)
    
    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=5)
        
        url = f"https://api.massive.io/v1/stocks/aggregates/{symbol}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}"
        headers = {"Authorization": f"Bearer {massive_api_key}"}
        
        r = requests.get(url, headers=headers, timeout=10)
        
        if r.status_code != 200:
            print(f"⚠️ Massive API returned {r.status_code}", flush=True)
            return None, None
        
        data = r.json()
        results = data.get("results", [])
        
        # Get the second-to-last day (most recent complete day)
        if len(results) >= 2:
            prev_day = results[-2]
            low = float(prev_day["l"])
            high = float(prev_day["h"])
            timestamp = prev_day["t"]
            date_str = datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")
            
            print(f"✅ Previous day range from Massive ({date_str}): low={low} high={high}", flush=True)
            return low, high
        
        print("❌ Massive API returned insufficient data", flush=True)
        return None, None
        
    except Exception as e:
        print(f"❌ Massive API error: {e}", flush=True)
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
# MAIN
# ======================================================
async def run():
    # Fetch previous day range
    prev_low, prev_high = fetch_prev_day_range_tradier(SYMBOL, TRADIER_API_KEY)
    
    if prev_low is None or prev_high is None:
        prev_low, prev_high = fetch_prev_day_range_massive(SYMBOL, MASSIVE_API_KEY)
    
    if prev_low is None or prev_high is None:
        print("❌ FATAL: Could not fetch previous day range from any source", flush=True)
        return
    
    print(f"📊 Using previous day range: low={prev_low}, high={prev_high}", flush=True)
    
    # Connect to Postgres
    db = await init_postgres()
    
    # Session ranges
    today_low = None
    today_high = None
    premarket_low = None
    premarket_high = None
    rth_low = None
    rth_high = None
    afterhours_low = None
    afterhours_high = None
    
    # Cooldown tracking
    last_phantom_alert = 0
    last_rth_alert = 0
    
    # Tracking for initial range establishment
    initial_trades_count = 0
    INITIAL_TRADES_THRESHOLD = 100  # Wait for 100 trades before enabling phantom detection
    
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                # Authenticate
                auth_msg = {"action": "auth", "params": MASSIVE_API_KEY}
                await ws.send(json.dumps(auth_msg))
                print("🔐 Authenticated.", flush=True)
                
                # Subscribe to trades
                sub_msg = {"action": "subscribe", "params": f"T.{SYMBOL}"}
                await ws.send(json.dumps(sub_msg))
                print(f"📡 Subscribed to {SYMBOL}", flush=True)
                
                # Main message loop
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception as e:
                        print(f"⚠️ JSON parse error: {e}", flush=True)
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
                        for c in conds:
                            if c not in PHANTOM_RELEVANT_CONDITIONS and c not in IGNORE_CONDITIONS:
                                print(f"⚠️ Unknown condition code {c} in trade: {e}", flush=True)
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
                        in_premarket = time(4, 0) <= tm < time(9, 30)
                        in_rth = time(9, 30) <= tm < time(16, 0)
                        in_afterhours = time(16, 0) <= tm <= time(20, 0)
                        
                        if in_premarket:
                            if premarket_low is None or price < premarket_low:
                                premarket_low = price
                            if premarket_high is None or price > premarket_high:
                                premarket_high = price
                                
                        elif in_rth:
                            if rth_low is None or price < rth_low:
                                rth_low = price
                            if rth_high is None or price > rth_high:
                                rth_high = price
                                
                        elif in_afterhours:
                            if afterhours_low is None or price < afterhours_low:
                                afterhours_low = price
                            if afterhours_high is None or price > afterhours_high:
                                afterhours_high = price

                        # Increment trade counter for initial range establishment
                        initial_trades_count += 1

                        # Filter out bad conditions
                        bad_conditions = any(c in IGNORE_CONDITIONS for c in conds)
                        
                        phantom_cond_ok = (
                            any(c in PHANTOM_RELEVANT_CONDITIONS for c in conds)
                            and not bad_conditions
                        )
                        
                        # ====================================================================
                        # FIX #1: Check if outside previous day's range (STRICT)
                        # Only trigger if price is genuinely beyond yesterday's boundaries
                        # ====================================================================
                        outside_prev = (
                            price > prev_high + PHANTOM_OUTSIDE_PREV or
                            price < prev_low - PHANTOM_OUTSIDE_PREV
                        )
                        
                        # ====================================================================
                        # FIX #2: Only check phantom prints AFTER initial range established
                        # This prevents false alerts when the range is still being built
                        # ====================================================================
                        if initial_trades_count < INITIAL_TRADES_THRESHOLD:
                            # Skip phantom detection during initial range building
                            is_phantom = False
                        else:
                            # Use FULL day range for comparison
                            compare_low = today_low
                            compare_high = today_high
                            
                            # ====================================================================
                            # FIX #3: Use fixed 25 cent gap from current range
                            # Simpler and more predictable than multiplier approach
                            # ====================================================================
                            outside_current_far = False
                            if compare_high is not None and compare_low is not None:
                                outside_current_far = (
                                    price > compare_high + PHANTOM_GAP_FROM_CURRENT or
                                    price < compare_low - PHANTOM_GAP_FROM_CURRENT
                                )
                            
                            # Phantom detection: must meet ALL criteria
                            is_phantom = (
                                phantom_cond_ok and      # Has valid conditions
                                outside_prev and         # Outside previous day (strictly)
                                outside_current_far      # WAY outside current range
                            )
                        
                        now = time_module.time()
                        
                        if is_phantom:
                            # ALWAYS PRINT TO CONSOLE
                            distance = min(
                                abs(price - compare_high) if compare_high else float('inf'),
                                abs(price - compare_low) if compare_low else float('inf')
                            )
                            
                            print(
                                f"🚨🚨 PHANTOM PRINT {ts_str()} ${price} "
                                f"size={size} conds={conds} exch={exch} seq={sequence} "
                                f"distance=${distance:.2f} from current range "
                                f"prev=[{prev_low},{prev_high}] current=[{compare_low},{compare_high}]",
                                flush=True
                            )
                            
                            # ALWAYS SEND TO DISCORD
                            msg = (
                                f"🚨 **Phantom Print Detected**\n"
                                f"Price: **${price}**\n"
                                f"Size: {size}\n"
                                f"Exchange: {exch}\n"
                                f"Conditions: {conds}\n"
                                f"Distance from range: ${distance:.2f}\n"
                                f"Previous day: [{prev_low}, {prev_high}]\n"
                                f"Current range: [{compare_low}, {compare_high}]\n"
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
                            """, sip_ts_raw, trf_ts_raw or sip_ts_raw, price, size,
                            json.dumps(conds), exch, sequence, trf_id)
                            
                            # WINDOW LOGIC
                            if now - last_phantom_alert > PHANTOM_COOLDOWN:
                                last_phantom_alert = now
                                print("🔥🔥 NEW PHANTOM WINDOW OPEN 🔥🔥", flush=True)
                            else:
                                print("⏳ (within cooldown window)", flush=True)
                        
                        # RTH breakout logic
                        if in_rth and not bad_conditions:
                            if rth_high is not None and rth_low is not None:
                                breakout = price > rth_high + RTH_BREAK_BUFFER or price < rth_low - RTH_BREAK_BUFFER
                                
                                if breakout and not is_phantom and now - last_rth_alert > RTH_COOLDOWN:
                                    last_rth_alert = now
                                    print(
                                        f"🚨 RTH BREAKOUT {ts_str()} ${price} "
                                        f"size={size} conds={conds} exch={exch} "
                                        f"rth=[{rth_low},{rth_high}]",
                                        flush=True
                                    )
                                    
        except websockets.exceptions.ConnectionClosed as e:
            print(f"⚠️ Websocket closed: {e}", flush=True)
            print("🔁 Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            print("🔁 Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        import traceback
        print("❌ Fatal crash:", e, flush=True)
        traceback.print_exc()
        raise
