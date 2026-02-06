import asyncio
import json
import os
import requests
import websockets
import asyncpg
import aiohttp
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
from collections import deque
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

# Manual previous day range override (set in Railway environment variables)
# Set MANUAL_PREV_LOW and MANUAL_PREV_HIGH to numbers, or leave unset/empty to use API
MANUAL_PREV_LOW = None
MANUAL_PREV_HIGH = None

if os.environ.get("MANUAL_PREV_LOW"):
    try:
        MANUAL_PREV_LOW = float(os.environ.get("MANUAL_PREV_LOW"))
    except (ValueError, TypeError):
        print("⚠️ Invalid MANUAL_PREV_LOW, ignoring", flush=True)
        MANUAL_PREV_LOW = None

if os.environ.get("MANUAL_PREV_HIGH"):
    try:
        MANUAL_PREV_HIGH = float(os.environ.get("MANUAL_PREV_HIGH"))
    except (ValueError, TypeError):
        print("⚠️ Invalid MANUAL_PREV_HIGH, ignoring", flush=True)
        MANUAL_PREV_HIGH = None

# Dark pool monitoring
DARK_POOL_SIZE_THRESHOLD = 100000  # Alert on dark pool trades above this size

# Velocity divergence settings
VELOCITY_ENABLED = os.environ.get("VELOCITY_ENABLED", "true").lower() in ("true", "1", "yes")
VELOCITY_WINDOW_SEC = 30  # Rolling window size in seconds
VELOCITY_DROP_THRESHOLD = 0.5  # 50% velocity drop = divergence
VELOCITY_CONFIRMATION_WINDOWS = 2  # Require 2 consecutive windows to confirm
VELOCITY_COOLDOWN = 180  # 3 minutes between divergence alerts
VELOCITY_MIN_TRADES_PER_WINDOW = 10  # Need at least this many trades to be meaningful

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
    0, 14, 4, 9, 19, 53, 1, 52
}
PHANTOM_RELEVANT_CONDITIONS = {
    2, 3, 7, 8, 10, 12, 13, 15, 16, 17, 20, 21, 22, 25, 26, 28, 29, 30, 33, 34, 37, 41, 62
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
# VELOCITY DIVERGENCE TRACKING
# ======================================================
class VelocityWindow:
    """Represents a time window for velocity tracking"""
    def __init__(self, start_time):
        self.start_time = start_time
        self.end_time = start_time + VELOCITY_WINDOW_SEC
        self.trade_count = 0
        self.total_volume = 0
        self.highest_price = None
        self.lowest_price = None
        self.made_new_high = False
        self.made_new_low = False
        
    def add_trade(self, price, size, session_high, session_low):
        """Add a trade to this window"""
        self.trade_count += 1
        self.total_volume += size
        
        # Track window high/low
        if self.highest_price is None or price > self.highest_price:
            self.highest_price = price
        if self.lowest_price is None or price < self.lowest_price:
            self.lowest_price = price
            
        # Check if this window made new session high/low
        if price >= session_high:
            self.made_new_high = True
        if price <= session_low:
            self.made_new_low = True
    
    def is_complete(self, current_time):
        """Check if this window is finished"""
        return current_time >= self.end_time
    
    def get_metrics(self):
        """Return velocity metrics"""
        duration = self.end_time - self.start_time
        return {
            'trade_velocity': self.trade_count / duration if duration > 0 else 0,
            'volume_velocity': self.total_volume / duration if duration > 0 else 0,
            'trade_count': self.trade_count,
            'total_volume': self.total_volume,
            'made_new_high': self.made_new_high,
            'made_new_low': self.made_new_low,
            'highest_price': self.highest_price,
            'lowest_price': self.lowest_price
        }

def detect_velocity_divergence(windows, session_high, session_low):
    """
    Detect velocity divergence - price makes new high/low but velocity drops
    
    Returns: (is_divergence, alert_data) tuple
    """
    if len(windows) < VELOCITY_CONFIRMATION_WINDOWS + 1:
        return False, None
    
    # Get current window and previous windows
    current = windows[-1]
    previous_windows = list(windows)[-VELOCITY_CONFIRMATION_WINDOWS-1:-1]
    
    current_metrics = current.get_metrics()
    
    # Need minimum trades to be meaningful
    if current_metrics['trade_count'] < VELOCITY_MIN_TRADES_PER_WINDOW:
        return False, None
    
    # Check if current window made new high or low
    if not (current_metrics['made_new_high'] or current_metrics['made_new_low']):
        return False, None
    
    # Calculate average velocity of previous windows
    prev_trade_velocities = [w.get_metrics()['trade_velocity'] for w in previous_windows]
    prev_volume_velocities = [w.get_metrics()['volume_velocity'] for w in previous_windows]
    
    if not prev_trade_velocities:
        return False, None
    
    avg_prev_trade_vel = sum(prev_trade_velocities) / len(prev_trade_velocities)
    avg_prev_volume_vel = sum(prev_volume_velocities) / len(prev_volume_velocities)
    
    # Avoid division by zero
    if avg_prev_trade_vel == 0 or avg_prev_volume_vel == 0:
        return False, None
    
    # Check for velocity drop
    trade_vel_drop_pct = 1 - (current_metrics['trade_velocity'] / avg_prev_trade_vel)
    volume_vel_drop_pct = 1 - (current_metrics['volume_velocity'] / avg_prev_volume_vel)
    
    # Divergence detected if BOTH velocities dropped by threshold
    is_divergence = (
        trade_vel_drop_pct >= VELOCITY_DROP_THRESHOLD and
        volume_vel_drop_pct >= VELOCITY_DROP_THRESHOLD
    )
    
    if is_divergence:
        alert_data = {
            'direction': 'HIGH' if current_metrics['made_new_high'] else 'LOW',
            'price': current_metrics['highest_price'] if current_metrics['made_new_high'] else current_metrics['lowest_price'],
            'trade_vel_drop_pct': trade_vel_drop_pct * 100,
            'volume_vel_drop_pct': volume_vel_drop_pct * 100,
            'current_trade_count': current_metrics['trade_count'],
            'current_volume': current_metrics['total_volume'],
            'prev_avg_trades': avg_prev_trade_vel * VELOCITY_WINDOW_SEC,
            'session_high': session_high,
            'session_low': session_low
        }
        return True, alert_data
    
    return False, None

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
    # Check for manual override first
    if MANUAL_PREV_LOW is not None and MANUAL_PREV_HIGH is not None:
        prev_low = MANUAL_PREV_LOW
        prev_high = MANUAL_PREV_HIGH
        print(f"📊 Using MANUAL previous day range: low={prev_low}, high={prev_high}", flush=True)
    else:
        # Fetch previous day range from APIs
        prev_low, prev_high = fetch_prev_day_range_tradier(SYMBOL, TRADIER_API_KEY)
        
        if prev_low is None or prev_high is None:
            prev_low, prev_high = fetch_prev_day_range_massive(SYMBOL, MASSIVE_API_KEY)
        
        if prev_low is None or prev_high is None:
            print("❌ FATAL: Could not fetch previous day range from any source", flush=True)
            return
        
        print(f"📊 Using API previous day range: low={prev_low}, high={prev_high}", flush=True)
    
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
    last_velocity_alert = 0
    
    # Velocity divergence tracking
    velocity_windows = deque(maxlen=10)  # Keep last 10 windows (5 minutes of data)
    current_velocity_window = None
    velocity_confirmation_count = 0  # Track consecutive divergence windows
    
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

                        # Update session-specific categories FIRST (needed for categorization)
                        in_premarket = time(4, 0) <= tm < time(9, 30)
                        in_rth = time(9, 30) <= tm < time(16, 0)
                        in_afterhours = time(16, 0) <= tm <= time(20, 0)
                        
                        # Increment trade counter for initial range establishment
                        initial_trades_count += 1

                        # Filter out bad conditions (needed by multiple detection systems)
                        bad_conditions = any(c in IGNORE_CONDITIONS for c in conds)

                        # ====================================================================
                        # PHANTOM DETECTION - DO THIS *BEFORE* UPDATING RANGES
                        # Critical: Check against existing range before the current trade modifies it
                        # ====================================================================
                        phantom_cond_ok = (
                            any(c in PHANTOM_RELEVANT_CONDITIONS for c in conds)
                            and not bad_conditions
                        )
                        
                        outside_prev = (
                            price > prev_high + PHANTOM_OUTSIDE_PREV or
                            price < prev_low - PHANTOM_OUTSIDE_PREV
                        )
                        
                        if initial_trades_count < INITIAL_TRADES_THRESHOLD:
                            is_phantom = False
                        else:
                            # Use CURRENT range (before this trade updates it)
                            compare_low = today_low
                            compare_high = today_high
                            
                            outside_current_far = False
                            if compare_high is not None and compare_low is not None:
                                outside_current_far = (
                                    price > compare_high + PHANTOM_GAP_FROM_CURRENT or
                                    price < compare_low - PHANTOM_GAP_FROM_CURRENT
                                )
                            
                            is_phantom = (
                                phantom_cond_ok and
                                outside_prev and
                                outside_current_far
                            )
                        
                        now = time_module.time()

                        # ====================================================================
                        # UPDATE RANGES - Only for non-phantom trades
                        # Phantom prints shouldn't pollute the real trading range
                        # ====================================================================
                        if not is_phantom:
                            # Update today's full session range
                            if today_low is None or price < today_low:
                                today_low = price
                            if today_high is None or price > today_high:
                                today_high = price

                            # Update session-specific ranges
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

                        # ====================================================================
                        # VELOCITY DIVERGENCE TRACKING
                        # Track trades in rolling windows, detect when velocity drops at extremes
                        # ====================================================================
                        if VELOCITY_ENABLED and not bad_conditions:
                            current_time = time_module.time()
                            
                            # Initialize or rotate velocity window
                            if current_velocity_window is None:
                                current_velocity_window = VelocityWindow(current_time)
                            elif current_velocity_window.is_complete(current_time):
                                # Window is complete, archive it and start new one
                                velocity_windows.append(current_velocity_window)
                                current_velocity_window = VelocityWindow(current_time)
                            
                            # Add trade to current window
                            current_velocity_window.add_trade(
                                price, size,
                                today_high if today_high else price,
                                today_low if today_low else price
                            )
                            
                            # Check for divergence when window completes
                            if len(velocity_windows) >= VELOCITY_CONFIRMATION_WINDOWS + 1:
                                # Exclude first 5 min and last 5 min of RTH
                                market_open_time = datetime.combine(datetime.now(ET).date(), time(9, 30), tzinfo=ET)
                                market_close_time = datetime.combine(datetime.now(ET).date(), time(16, 0), tzinfo=ET)
                                current_dt = datetime.now(ET)
                                
                                time_since_open = (current_dt - market_open_time).total_seconds()
                                time_until_close = (market_close_time - current_dt).total_seconds()
                                
                                in_valid_window = (
                                    in_rth and 
                                    time_since_open > 300 and  # After first 5 min
                                    time_until_close > 300     # Before last 5 min
                                )
                                
                                if in_valid_window:
                                    is_divergence, alert_data = detect_velocity_divergence(
                                        velocity_windows,
                                        today_high if today_high else 0,
                                        today_low if today_low else 0
                                    )
                                    
                                    if is_divergence:
                                        velocity_confirmation_count += 1
                                        
                                        # Alert after required confirmations and cooldown
                                        if (velocity_confirmation_count >= VELOCITY_CONFIRMATION_WINDOWS and 
                                            now - last_velocity_alert > VELOCITY_COOLDOWN):
                                            
                                            last_velocity_alert = now
                                            velocity_confirmation_count = 0  # Reset counter
                                            
                                            print(
                                                f"⚡ VELOCITY DIVERGENCE {ts_str()} "
                                                f"{alert_data['direction']} @ ${alert_data['price']:.2f} "
                                                f"trade_vel_drop={alert_data['trade_vel_drop_pct']:.1f}% "
                                                f"volume_vel_drop={alert_data['volume_vel_drop_pct']:.1f}%",
                                                flush=True
                                            )
                                            
                                            # Send to Discord
                                            vel_msg = (
                                                f"⚡ **Velocity Divergence Detected**\n"
                                                f"Direction: **{alert_data['direction']}** at **${alert_data['price']:.2f}**\n"
                                                f"Trade Velocity Drop: **{alert_data['trade_vel_drop_pct']:.1f}%**\n"
                                                f"Volume Velocity Drop: **{alert_data['volume_vel_drop_pct']:.1f}%**\n"
                                                f"Current Window: {alert_data['current_trade_count']} trades, "
                                                f"{alert_data['current_volume']:,} shares\n"
                                                f"Previous Avg: {alert_data['prev_avg_trades']:.0f} trades per window\n"
                                                f"Session Range: [{alert_data['session_low']:.2f}, {alert_data['session_high']:.2f}]\n"
                                                f"Time: {ts_str()}"
                                            )
                                            asyncio.create_task(send_discord(vel_msg))
                                    else:
                                        # Reset confirmation counter if divergence not detected
                                        velocity_confirmation_count = 0

                        # Phantom alert handling (is_phantom was already calculated earlier before range updates)
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
                        
                        # Dark pool large print detection
                        is_darkpool = (exch == 4)  # Exchange 4 is dark pool
                        if is_darkpool and size >= DARK_POOL_SIZE_THRESHOLD and not bad_conditions:
                            print(
                                f"🟣 LARGE DARK POOL PRINT {ts_str()} ${price} "
                                f"size={size:,} conds={conds} seq={sequence}",
                                flush=True
                            )
                            
                            # Send to Discord
                            dp_msg = (
                                f"🟣 **Large Dark Pool Print**\n"
                                f"Price: **${price}**\n"
                                f"Size: **{size:,} shares**\n"
                                f"Notional: **${price * size:,.2f}**\n"
                                f"Conditions: {conds}\n"
                                f"SIP Time: {datetime.fromtimestamp(sip_ts_raw/1000, tz=ET)}\n"
                                f"Sequence: {sequence}"
                            )
                            asyncio.create_task(send_discord(dp_msg))
                        
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
