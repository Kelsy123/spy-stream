import asyncio
import json
import os
import requests
import websockets
import asyncpg
import aiohttp
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
from collections import deque, defaultdict
import time as time_module
import csv
from pathlib import Path

# ======================================================
# SETTINGS
# ======================================================
SYMBOL = "SPY"
ET = ZoneInfo("America/New_York")
WS_URL = "wss://socket.massive.com/stocks"
MASSIVE_API_KEY = os.environ["MASSIVE_API_KEY"]
POSTGRES_URL = os.environ["POSTGRES_URL"]
DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]
DISCORD_ZERO_WEBHOOK_URL = os.environ["DISCORD_ZERO_WEBHOOK_URL"]  # Separate channel for zero-size trade alerts
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

# PHASE 1: Option 1 (Relaxed) - Current settings
# These defaults provide quality signals without being too strict
# All values can be overridden via Railway environment variables
VELOCITY_WINDOW_SEC = int(os.environ.get("VELOCITY_WINDOW_SEC", "60"))  # 60 seconds (was 30)
VELOCITY_DROP_THRESHOLD = float(os.environ.get("VELOCITY_DROP_THRESHOLD", "0.30"))  # 30% drop (was 50%)
VELOCITY_CONFIRMATION_WINDOWS = int(os.environ.get("VELOCITY_CONFIRMATION_WINDOWS", "1"))  # 1 window (was 2)
VELOCITY_COOLDOWN = int(os.environ.get("VELOCITY_COOLDOWN", "300"))  # 5 minutes between alerts
VELOCITY_MIN_TRADES_PER_WINDOW = int(os.environ.get("VELOCITY_MIN_TRADES_PER_WINDOW", "20"))  # 20 trades minimum

# PHASE 2: Option 3 (Institutional Grade) - Coming soon!
# When ready to upgrade, these will enable:
# - OR logic (alert on EITHER trade OR volume drop, not requiring both)
# - Exhaustion detection (new high/low on declining volume)
# - Acceleration divergence (price speeding up while participation slows down)
# Will add 3 types of divergence signals instead of 1 for better coverage!

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

# Zero-size trade logging
ZERO_SIZE_LOGGING_ENABLED = os.environ.get("ZERO_SIZE_LOGGING", "true").lower() in ("true", "1", "yes")

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

async def send_discord_zero(msg: str):
    """Send to the dedicated zero-size trade Discord channel (DISCORD_ZERO_WEBHOOK_URL)."""
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(DISCORD_ZERO_WEBHOOK_URL, json={"content": msg})
    except Exception as e:
        print(f"⚠️ Discord zero-size webhook failed: {e}", flush=True)

# ======================================================
# ZERO-SIZE TRADE LOGGER
# ======================================================
class ZeroSizeTradeLogger:
    """
    Logs all zero-size trades for analysis
    Creates daily CSV and JSON files for pattern recognition
    Discord alerts are batched into 5-second windows to prevent rate limiting
    """
    def __init__(self, ticker="SPY"):
        self.ticker = ticker
        
        # Use /tmp for Railway ephemeral storage (files persist during deployment)
        # Railway resets /tmp on each deploy, which is fine for daily logs
        self.log_dir = Path("/tmp/zero_size_logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # In-memory storage for session
        self.zero_trades = []

        # 5-second batching buffer for Discord alerts
        self._batch_buffer = []        # trades collected in current window
        self._batch_task = None        # asyncio task handle
        self._batch_window = 10.0      # seconds to collect before sending remainder

        # Console log batching — flush every 30 seconds to reduce Railway log volume
        self._console_buffer = []
        self._last_console_flush = time_module.time()
        
        print(f"📊 Zero-size logger initialized. Logs: {self.log_dir}", flush=True)
    
    def get_today_str(self):
        """Get current date string - always fresh"""
        return datetime.now(ET).strftime('%Y-%m-%d')
    
    def get_csv_file(self):
        """Get CSV file path for current date"""
        return self.log_dir / f"zero_trades_{self.ticker}_{self.get_today_str()}.csv"
    
    def get_json_file(self):
        """Get JSON file path for current date"""
        return self.log_dir / f"zero_trades_{self.ticker}_{self.get_today_str()}.json"
        
    def init_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        csv_file = self.get_csv_file()
        if not csv_file.exists():
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Timestamp_MS',
                    'Time_EST',
                    'Price',
                    'Size',
                    'Exchange',
                    'Exchange_Name',
                    'Conditions',
                    'Sequence',
                    'SIP_Timestamp',
                    'TRF_Timestamp',
                    'TRF_ID'
                ])
    
    def queue_discord_alert(self, trade_record):
        """
        First trade in a burst: send immediately to Discord.
        Subsequent trades within 10 seconds: batch and send as one grouped message.
        """
        is_first = self._batch_task is None or self._batch_task.done()
        self._batch_buffer.append(trade_record)

        if is_first:
            # Send the first trade immediately as a single alert
            asyncio.create_task(self._send_first_alert(trade_record))
            # Start 10-second window to collect and batch the rest
            self._batch_task = asyncio.create_task(self._flush_after_window())
        # else: already collecting — trade is in buffer, will flush with the rest

    async def _send_first_alert(self, record):
        """Send the very first trade in a burst immediately."""
        now_str = datetime.now(ET).strftime("%H:%M:%S ET")
        exch = record['exchange_name'][:21]
        conds_str = '|'.join(str(c) for c in record['conditions']) if record['conditions'] else '-'
        msg = (
            f"🔍 **Zero-Size Trade** ({now_str})\n"
            f"`{record['time_est']}  {exch}  seq={record['sequence']}  conds={conds_str}`"
        )
        await send_discord_zero(msg)

    async def _flush_after_window(self):
        """Wait 10 seconds, then send one batched message for all buffered trades after the first."""
        await asyncio.sleep(self._batch_window)

        # Skip the first record — it was already sent immediately
        remaining = list(self._batch_buffer[1:])
        self._batch_buffer.clear()

        if not remaining:
            return

        count = len(remaining)
        now_str = datetime.now(ET).strftime("%H:%M:%S ET")
        lines = [f"🔍 **+{count} more zero-size trades in window** ({now_str})"]
        lines.append("```")
        lines.append(f"{'#':<4} {'Time':<12} {'Exchange':<22} {'Seq':<10} {'Conds'}")
        lines.append("-" * 65)
        for i, t in enumerate(remaining, 1):
            exch = t['exchange_name'][:21]
            conds_str = '|'.join(str(c) for c in t['conditions']) if t['conditions'] else '-'
            lines.append(f"{i:<4} {t['time_est']:<12} {exch:<22} {str(t['sequence']):<10} {conds_str}")
        lines.append("```")
        await send_discord_zero("\n".join(lines))

    def log_zero_trade(self, trade_data):
        """
        Log a zero-size trade to CSV, JSON, and in-memory list.
        Console output is batched — prints a summary line per trade,
        with a periodic flush to avoid flooding Railway logs.
        """
        timestamp_ms = trade_data.get('sip_timestamp', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=ET)
        
        trade_record = {
            'timestamp_ms': timestamp_ms,
            'timestamp': timestamp_dt.isoformat(),
            'time_est': timestamp_dt.strftime('%H:%M:%S.%f')[:-3],
            'price': trade_data.get('price'),
            'size': trade_data.get('size'),
            'exchange': trade_data.get('exchange'),
            'exchange_name': self.get_exchange_name(trade_data.get('exchange')),
            'conditions': trade_data.get('conditions', []),
            'sequence': trade_data.get('sequence'),
            'sip_timestamp': trade_data.get('sip_timestamp'),
            'trf_timestamp': trade_data.get('trf_timestamp'),
            'trf_id': trade_data.get('trf_id')
        }
        
        # Add to in-memory list
        self.zero_trades.append(trade_record)
        
        # Write to CSV
        self.write_to_csv(trade_record)
        
        # Write to JSON (append to array)
        self.write_to_json(trade_record)
        
        # Buffer for console — flush every 30 seconds instead of printing every trade
        self._console_buffer.append(trade_record)
        
        return trade_record
    
    def write_to_csv(self, record):
        """Append record to CSV file"""
        # Initialize CSV if needed (handles day change)
        self.init_csv()
        csv_file = self.get_csv_file()
        with open(csv_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                record['timestamp_ms'],
                record['time_est'],
                record['price'],
                record['size'],
                record['exchange'],
                record['exchange_name'],
                '|'.join(str(c) for c in record['conditions']),  # Join conditions with pipe
                record['sequence'],
                record['sip_timestamp'],
                record['trf_timestamp'],
                record['trf_id']
            ])
    
    def write_to_json(self, record):
        """Append record to JSON file"""
        json_file = self.get_json_file()
        # Read existing data
        if json_file.exists():
            with open(json_file, 'r') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = []
        else:
            data = []
        
        # Append new record
        data.append(record)
        
        # Write back
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def flush_console_buffer(self):
        """
        Print a compact summary of buffered zero-size trades to Railway logs.
        Called every 30 seconds from the main loop — one line per trade instead
        of the old multi-line block, dramatically reducing log volume.
        """
        if not self._console_buffer:
            return
        now = time_module.time()
        if now - self._last_console_flush < 30:
            return  # Not time yet

        batch = list(self._console_buffer)
        self._console_buffer.clear()
        self._last_console_flush = now

        count = len(batch)
        now_str = datetime.now(ET).strftime("%H:%M:%S ET")
        print(f"\n🔍 Zero-size trades logged ({count} in last 30s) — {now_str}", flush=True)
        for t in batch:
            conds_str = '|'.join(str(c) for c in t['conditions']) if t['conditions'] else '-'
            print(f"   {t['time_est']}  ${t['price']:.2f}  seq={t['sequence']}  conds={conds_str}", flush=True)
    
    def get_exchange_name(self, code):
        """Convert exchange code to readable name"""
        # Massive.com uses numeric exchange codes
        exchanges = {
            1: 'NYSE American',
            2: 'NASDAQ OMX BX',
            3: 'NYSE National',
            4: 'FINRA ADF (Dark Pool)',
            5: 'Market Independent',
            6: 'MIAX',
            7: 'ISE',
            8: 'EDGA',
            9: 'EDGX',
            10: 'LTSE',
            11: 'Chicago',
            12: 'NYSE',
            13: 'NYSE Arca',
            14: 'NASDAQ',
            15: 'NASDAQ Small Cap',
            16: 'NASDAQ Int',
            17: 'MEMX',
            18: 'IEX',
            19: 'CBOE',
            20: 'NASDAQ PSX',
            21: 'BATS Y',
            22: 'BATS'
        }
        return exchanges.get(code, f'Unknown ({code})')
    
    def get_daily_summary(self):
        """Generate summary statistics for the day"""
        if not self.zero_trades:
            return "No zero-size trades recorded today"
        
        # Count by exchange
        exchange_counts = defaultdict(int)
        for trade in self.zero_trades:
            ex = trade['exchange_name']
            exchange_counts[ex] += 1
        
        # Count by price level
        price_counts = defaultdict(int)
        for trade in self.zero_trades:
            price = trade['price']
            price_counts[price] += 1
        
        # Find repeated levels
        repeated_levels = {p: c for p, c in price_counts.items() if c > 1}
        
        # Count by conditions
        condition_counts = defaultdict(int)
        for trade in self.zero_trades:
            for cond in trade['conditions']:
                condition_counts[cond] += 1
        
        summary = f"""
{'='*80}
ZERO-SIZE TRADE SUMMARY - {self.ticker} - {self.get_today_str()}
{'='*80}

Total Zero-Size Trades: {len(self.zero_trades)}

BY EXCHANGE:
{self._format_dict(exchange_counts)}

REPEATED PRICE LEVELS (appears more than once):
{self._format_dict(repeated_levels, prefix='$')}

TOP CONDITIONS:
{self._format_dict(dict(sorted(condition_counts.items(), key=lambda x: x[1], reverse=True)[:10]))}

Price Range: ${min(t['price'] for t in self.zero_trades):.2f} - ${max(t['price'] for t in self.zero_trades):.2f}

First: {self.zero_trades[0]['time_est']}
Last:  {self.zero_trades[-1]['time_est']}

Log files saved to:
CSV:  {self.get_csv_file()}
JSON: {self.get_json_file()}

{'='*80}
"""
        return summary
    
    def _format_dict(self, d, prefix=''):
        """Helper to format dictionary for printing"""
        if not d:
            return "  None"
        return '\n'.join(f"  {prefix}{k}: {v}" for k, v in sorted(d.items(), key=lambda x: x[1], reverse=True))
    
    async def send_csv_to_discord(self):
        """Upload today's CSV file as a Discord attachment to the zero-size channel."""
        csv_file = self.get_csv_file()
        if not csv_file.exists():
            await send_discord_zero("📎 Zero-size CSV not found — no trades were written today.")
            return

        try:
            with open(csv_file, 'rb') as f:
                csv_bytes = f.read()

            filename = csv_file.name
            form = aiohttp.FormData()
            form.add_field(
                'payload_json',
                json.dumps({"content": f"📎 **Zero-Size Trade Log — {self.ticker} {self.get_today_str()}**"}),
                content_type='application/json'
            )
            form.add_field(
                'file',
                csv_bytes,
                filename=filename,
                content_type='text/csv'
            )

            async with aiohttp.ClientSession() as session:
                resp = await session.post(DISCORD_ZERO_WEBHOOK_URL, data=form)
                if resp.status in (200, 204):
                    print(f"📎 Zero-size CSV uploaded to Discord zero channel: {filename}", flush=True)
                else:
                    text = await resp.text()
                    print(f"⚠️ CSV Discord upload failed ({resp.status}): {text}", flush=True)
        except Exception as e:
            print(f"⚠️ CSV Discord upload error: {e}", flush=True)

    async def save_summary(self):
        """Save daily summary to file, send text summary + CSV attachment to Discord"""
        summary = self.get_daily_summary()
        summary_file = self.log_dir / f"summary_{self.ticker}_{self.get_today_str()}.txt"
        with open(summary_file, 'w') as f:
            f.write(summary)
        print(summary, flush=True)
        
        # Send summary to Discord if we have any zero trades
        if self.zero_trades:
            discord_msg = f"📊 **Zero-Size Trade Summary - {self.ticker}**\n```\n"
            discord_msg += f"Total: {len(self.zero_trades)} zero-size trades detected today\n"
            discord_msg += f"Price Range: ${min(t['price'] for t in self.zero_trades):.2f} - ${max(t['price'] for t in self.zero_trades):.2f}\n"
            
            # Show top 3 repeated levels
            price_counts = defaultdict(int)
            for trade in self.zero_trades:
                price_counts[trade['price']] += 1
            repeated = {p: c for p, c in price_counts.items() if c > 1}
            
            if repeated:
                discord_msg += f"\nRepeated Levels:\n"
                for price, count in sorted(repeated.items(), key=lambda x: x[1], reverse=True)[:3]:
                    discord_msg += f"  ${price:.2f}: {count}x\n"
            
            # List individual trades
            discord_msg += f"\nIndividual Trades:\n"
            discord_msg += f"{'#':<4} {'Time':<12} {'Price':<10} {'Exchange':<15}\n"
            discord_msg += f"{'-'*45}\n"
            
            for i, trade in enumerate(self.zero_trades, 1):
                # Truncate exchange name if too long
                exch_name = trade['exchange_name'][:14]
                discord_msg += f"{i:<4} {trade['time_est']:<12} ${trade['price']:<9.2f} {exch_name:<15}\n"
            
            discord_msg += "```"
            await send_discord_zero(discord_msg)

            # Upload the CSV as a file attachment
            await self.send_csv_to_discord()
        
        return summary_file

# ======================================================
# DARK POOL TRACKER
# ======================================================
class DarkPoolTracker:
    """
    Tracks large dark pool prints throughout the day
    Generates end-of-day summary ranked by notional value
    """
    def __init__(self, ticker="SPY", size_threshold=100000):
        self.ticker = ticker
        self.size_threshold = size_threshold
        
        # Use /tmp for Railway ephemeral storage
        self.log_dir = Path("/tmp/dark_pool_logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # In-memory storage for session
        self.dark_pool_prints = []
        
        # Batching system for Discord alerts
        self.pending_batch = []  # Prints waiting to be batched
        self.batch_task = None   # Background task for batch delay
        self.batch_delay = 3     # Wait 3 seconds to collect similar prints
        
        print(f"🟣 Dark pool tracker initialized. Threshold: {size_threshold:,} shares", flush=True)
    
    def get_today_str(self):
        """Get current date string - always fresh"""
        return datetime.now(ET).strftime('%Y-%m-%d')
    
    def get_csv_file(self):
        """Get CSV file path for current date"""
        return self.log_dir / f"dark_pool_{self.ticker}_{self.get_today_str()}.csv"
        
    def init_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        csv_file = self.get_csv_file()
        if not csv_file.exists():
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Timestamp_MS',
                    'Time_EST',
                    'Price',
                    'Size',
                    'Notional_Value',
                    'Conditions',
                    'Sequence',
                    'SIP_Timestamp',
                    'TRF_Timestamp'
                ])
    
    def log_dark_pool_print(self, trade_data):
        """
        Log a large dark pool print
        
        Args:
            trade_data: Dictionary with Massive.com trade fields
        
        Returns:
            Complete trade_record with calculated notional value
        """
        timestamp_ms = trade_data.get('sip_timestamp', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=ET)
        
        price = trade_data.get('price')
        size = trade_data.get('size')
        notional = price * size
        
        trade_record = {
            'timestamp_ms': timestamp_ms,
            'timestamp': timestamp_dt.isoformat(),
            'time_est': timestamp_dt.strftime('%H:%M:%S.%f')[:-3],
            'price': price,
            'size': size,
            'notional': notional,
            'conditions': trade_data.get('conditions', []),
            'sequence': trade_data.get('sequence'),
            'sip_timestamp': trade_data.get('sip_timestamp'),
            'trf_timestamp': trade_data.get('trf_timestamp')
        }
        
        # Add to in-memory list
        self.dark_pool_prints.append(trade_record)
        
        # Write to CSV
        self.write_to_csv(trade_record)
        
        # Return the complete record for batching
        return trade_record
        
        return trade_record
    
    def write_to_csv(self, record):
        """Append record to CSV file"""
        # Initialize CSV if needed (handles day change)
        self.init_csv()
        csv_file = self.get_csv_file()
        with open(csv_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                record['timestamp_ms'],
                record['time_est'],
                record['price'],
                record['size'],
                record['notional'],
                '|'.join(str(c) for c in record['conditions']),
                record['sequence'],
                record['sip_timestamp'],
                record['trf_timestamp']
            ])
    
    def get_daily_summary(self):
        """
        Generate end-of-day summary with prints ranked by notional value
        Returns formatted string for console/Discord
        """
        if not self.dark_pool_prints:
            return "No large dark pool prints recorded today"
        
        # Sort by notional value (highest first)
        sorted_prints = sorted(self.dark_pool_prints, key=lambda x: x['notional'], reverse=True)
        
        # Calculate statistics
        total_prints = len(sorted_prints)
        total_volume = sum(p['size'] for p in sorted_prints)
        total_notional = sum(p['notional'] for p in sorted_prints)
        avg_size = total_volume / total_prints
        avg_notional = total_notional / total_prints
        
        # Group by price level to find repeated levels
        price_groups = defaultdict(list)
        for p in sorted_prints:
            price_groups[p['price']].append(p)
        
        # Build summary text
        summary = f"""
{'='*100}
LARGE DARK POOL PRINTS SUMMARY - {self.ticker} - {self.get_today_str()}
{'='*100}

STATISTICS:
  Total Prints:        {total_prints}
  Total Volume:        {total_volume:,} shares
  Total Notional:      ${total_notional:,.2f}
  Average Size:        {avg_size:,.0f} shares
  Average Notional:    ${avg_notional:,.2f}
  Size Threshold:      {self.size_threshold:,} shares

{'='*100}
TOP PRINTS BY NOTIONAL VALUE
{'='*100}
{'Rank':<6} {'Time':<12} {'Price':<10} {'Size':<15} {'Notional':<18} {'Conditions':<20}
{'-'*100}
"""
        
        # Add top prints (up to 50)
        for i, p in enumerate(sorted_prints[:50], 1):
            conditions_str = ','.join(str(c) for c in p['conditions'][:5])  # First 5 conditions
            if len(p['conditions']) > 5:
                conditions_str += '...'
            
            summary += f"{i:<6} {p['time_est']:<12} ${p['price']:<9.2f} {p['size']:>14,} ${p['notional']:>17,.2f} {conditions_str:<20}\n"
        
        if len(sorted_prints) > 50:
            summary += f"\n... and {len(sorted_prints) - 50} more prints\n"
        
        # Add price level analysis
        repeated_levels = {price: trades for price, trades in price_groups.items() if len(trades) > 1}
        
        if repeated_levels:
            summary += f"\n{'='*100}\n"
            summary += "REPEATED PRICE LEVELS (Multiple large prints at same price)\n"
            summary += f"{'='*100}\n"
            summary += f"{'Price':<10} {'Count':<8} {'Total Size':<18} {'Total Notional':<20}\n"
            summary += f"{'-'*100}\n"
            
            # Sort by total notional at that level
            level_summary = []
            for price, trades in repeated_levels.items():
                total_size = sum(t['size'] for t in trades)
                total_not = sum(t['notional'] for t in trades)
                level_summary.append((price, len(trades), total_size, total_not))
            
            level_summary.sort(key=lambda x: x[3], reverse=True)  # Sort by total notional
            
            for price, count, total_size, total_not in level_summary[:20]:
                summary += f"${price:<9.2f} {count:<8} {total_size:>17,} ${total_not:>19,.2f}\n"
        
        summary += f"\n{'='*100}\n"
        summary += f"Log file saved to: {self.get_csv_file()}\n"
        summary += f"{'='*100}\n"
        
        return summary
    
    def get_discord_summary(self):
        """
        Generate Discord-friendly summary (shorter, formatted for Discord)
        """
        if not self.dark_pool_prints:
            return None
        
        sorted_prints = sorted(self.dark_pool_prints, key=lambda x: x['notional'], reverse=True)
        
        total_prints = len(sorted_prints)
        total_volume = sum(p['size'] for p in sorted_prints)
        total_notional = sum(p['notional'] for p in sorted_prints)
        
        # Build Discord message
        msg = f"🟣 **Large Dark Pool Prints (≥100k) Summary - {self.ticker}**\n"
        msg += f"**{self.get_today_str()}**\n\n"
        msg += f"**Statistics:**\n"
        msg += f"• Total Prints: **{total_prints}**\n"
        msg += f"• Total Volume: **{total_volume:,} shares**\n"
        msg += f"• Total Notional: **${total_notional:,.2f}**\n\n"
        
        msg += f"**Top 10 by Notional Value:**\n```\n"
        msg += f"{'#':<3} {'Time':<9} {'Price':<8} {'Size':<12} {'Notional':<15}\n"
        msg += f"{'-'*50}\n"
        
        for i, p in enumerate(sorted_prints[:10], 1):
            msg += f"{i:<3} {p['time_est'][:8]:<9} ${p['price']:<7.2f} {p['size']:>11,} ${p['notional']:>14,.0f}\n"
        
        msg += "```\n"
        
        # Add repeated levels if any
        price_groups = defaultdict(list)
        for p in sorted_prints:
            price_groups[p['price']].append(p)
        
        repeated_levels = {price: trades for price, trades in price_groups.items() if len(trades) > 1}
        
        if repeated_levels:
            msg += f"\n**Repeated Price Levels:**\n```\n"
            level_summary = []
            for price, trades in repeated_levels.items():
                total_size = sum(t['size'] for t in trades)
                total_not = sum(t['notional'] for t in trades)
                level_summary.append((price, len(trades), total_size, total_not))
            
            level_summary.sort(key=lambda x: x[3], reverse=True)
            
            msg += f"{'Price':<8} {'Count':<6} {'Total Notional':<15}\n"
            msg += f"{'-'*35}\n"
            
            for price, count, total_size, total_not in level_summary[:5]:
                msg += f"${price:<7.2f} {count:<6} ${total_not:>14,.0f}\n"
            
            msg += "```"
        
        return msg
    
    async def queue_for_alert(self, trade_record):
        """
        Add print to batch queue and schedule alert
        Individual prints are STILL logged to dark_pool_prints list immediately
        This only affects Discord alerts, not data tracking
        """
        self.pending_batch.append(trade_record)
        
        # Cancel existing batch timer if there is one
        if self.batch_task and not self.batch_task.done():
            self.batch_task.cancel()
        
        # Start new batch timer (3 seconds)
        self.batch_task = asyncio.create_task(self._send_batched_alert())
    
    async def _send_batched_alert(self):
        """
        Wait for batch_delay seconds, then send alert(s)
        If more prints come in during wait, timer resets
        """
        try:
            await asyncio.sleep(self.batch_delay)
            
            # After delay, check what we have
            if not self.pending_batch:
                return
            
            # Group by exact price
            batches = self._group_by_price(self.pending_batch)
            
            # Send alerts for each price group
            for price, prints in batches.items():
                if len(prints) == 1:
                    # Single print - send individual alert with all details
                    await self._send_individual_alert(prints[0])
                else:
                    # Multiple prints at same price - send batched alert
                    await self._send_batch_alert(prints)
            
            # Clear the batch
            self.pending_batch = []
            
        except asyncio.CancelledError:
            # Timer was cancelled because new print came in
            # This is normal - the new print will restart the timer
            pass
    
    def _group_by_price(self, prints):
        """
        Group prints by exact price only
        Returns dict: {price: [list of prints at that price]}
        """
        from collections import defaultdict
        groups = defaultdict(list)
        for p in prints:
            groups[p['price']].append(p)
        return groups
    
    async def _send_individual_alert(self, print_data):
        """
        Send single print alert - KEEPS CURRENT DETAILED FORMAT
        """
        msg = (
            f"🟣 **Large Dark Pool Print**\n"
            f"Price: **${print_data['price']}**\n"
            f"Size: **{print_data['size']:,} shares**\n"
            f"Notional: **${print_data['notional']:,.2f}**\n"
            f"Conditions: {print_data['conditions']}\n"
            f"SIP Time: {datetime.fromtimestamp(print_data['sip_timestamp']/1000, tz=ET)}\n"
            f"Sequence: {print_data['sequence']}"
        )
        await send_discord(msg)
    
    async def _send_batch_alert(self, prints):
        """
        Send batched alert for multiple prints at exact same price
        Shows totals and summary while keeping all individual data in tracker
        """
        # Sort by time
        prints_sorted = sorted(prints, key=lambda x: x['timestamp_ms'])
        
        # Calculate totals
        count = len(prints)
        total_size = sum(p['size'] for p in prints)
        total_notional = sum(p['notional'] for p in prints)
        avg_size = total_size / count
        price = prints[0]['price']  # All same price (exact match)
        
        # Time range
        first_time = prints_sorted[0]['time_est']
        last_time = prints_sorted[-1]['time_est']
        
        # Sequences (show first 3, then count remaining)
        sequences = [str(p['sequence']) for p in prints_sorted]
        if len(sequences) <= 3:
            seq_str = ', '.join(sequences)
        else:
            seq_str = f"{', '.join(sequences[:3])}... (+{len(sequences)-3} more)"
        
        # Check if all conditions are the same
        all_conditions = [str(p['conditions']) for p in prints]
        if len(set(all_conditions)) == 1:
            conditions_str = str(prints[0]['conditions'])
        else:
            conditions_str = "Multiple (see end-of-day summary)"
        
        msg = (
            f"🟣🟣 **Dark Pool Print Cluster**\n"
            f"Price: **${price}** (exact match)\n"
            f"Count: **{count} prints**\n"
            f"Total Size: **{total_size:,} shares**\n"
            f"Total Notional: **${total_notional:,.2f}**\n"
            f"Avg Size per Print: **{avg_size:,.0f} shares**\n"
            f"Time Range: {first_time} - {last_time}\n"
            f"Sequences: {seq_str}\n"
            f"Conditions: {conditions_str}"
        )
        await send_discord(msg)
    
    async def save_summary(self):
        """Save daily summary to file and send to Discord"""
        summary = self.get_daily_summary()
        summary_file = self.log_dir / f"summary_dark_pool_{self.ticker}_{self.get_today_str()}.txt"
        with open(summary_file, 'w') as f:
            f.write(summary)
        print(summary, flush=True)
        
        # Send to Discord
        discord_msg = self.get_discord_summary()
        if discord_msg:
            await send_discord(discord_msg)
        
        return summary_file

# ======================================================
# PHANTOM PRINT TRACKER
# ======================================================
class PhantomPrintTracker:
    """
    Tracks phantom prints throughout the day
    Generates end-of-day summary with all prints listed chronologically
    """
    def __init__(self, ticker="SPY"):
        self.ticker = ticker
        
        # Use /tmp for Railway ephemeral storage
        self.log_dir = Path("/tmp/phantom_logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # In-memory storage for session
        self.phantom_prints = []
        
        print(f"👻 Phantom print tracker initialized.", flush=True)
    
    def get_today_str(self):
        """Get current date string - always fresh"""
        return datetime.now(ET).strftime('%Y-%m-%d')
    
    def get_csv_file(self):
        """Get CSV file path for current date"""
        return self.log_dir / f"phantoms_{self.ticker}_{self.get_today_str()}.csv"
        
    def init_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        csv_file = self.get_csv_file()
        if not csv_file.exists():
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Timestamp_MS',
                    'Time_EST',
                    'Price',
                    'Size',
                    'Exchange',
                    'Conditions',
                    'Sequence',
                    'SIP_Timestamp',
                    'TRF_Timestamp',
                    'TRF_ID',
                    'Distance_From_Range'
                ])
    
    def log_phantom_print(self, trade_data, distance_from_range):
        """
        Log a phantom print
        
        Args:
            trade_data: Dictionary with Massive.com trade fields
            distance_from_range: Distance from current trading range
        """
        timestamp_ms = trade_data.get('sip_timestamp', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=ET)
        
        price = trade_data.get('price')
        size = trade_data.get('size')
        
        phantom_record = {
            'timestamp_ms': timestamp_ms,
            'timestamp': timestamp_dt.isoformat(),
            'time_est': timestamp_dt.strftime('%H:%M:%S.%f')[:-3],
            'price': price,
            'size': size,
            'exchange': trade_data.get('exchange'),
            'conditions': trade_data.get('conditions', []),
            'sequence': trade_data.get('sequence'),
            'sip_timestamp': trade_data.get('sip_timestamp'),
            'trf_timestamp': trade_data.get('trf_timestamp'),
            'trf_id': trade_data.get('trf_id'),
            'distance': distance_from_range
        }
        
        # Add to in-memory list
        self.phantom_prints.append(phantom_record)
        
        # Write to CSV
        self.write_to_csv(phantom_record)
        
        return phantom_record
    
    def write_to_csv(self, record):
        """Append record to CSV file"""
        # Initialize CSV if needed (handles day change)
        self.init_csv()
        csv_file = self.get_csv_file()
        with open(csv_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                record['timestamp_ms'],
                record['time_est'],
                record['price'],
                record['size'],
                record['exchange'],
                '|'.join(str(c) for c in record['conditions']),
                record['sequence'],
                record['sip_timestamp'],
                record['trf_timestamp'],
                record['trf_id'],
                record['distance']
            ])
    
    def get_daily_summary(self):
        """
        Generate end-of-day summary with all prints in chronological order
        """
        if not self.phantom_prints:
            return "No phantom prints recorded today"
        
        summary = f"""
{'='*100}
PHANTOM PRINTS SUMMARY - {self.ticker} - {self.get_today_str()}
{'='*100}

STATISTICS:
  Total Phantom Prints: {len(self.phantom_prints)}
  Price Range: ${min(p['price'] for p in self.phantom_prints):.2f} - ${max(p['price'] for p in self.phantom_prints):.2f}
  First: {self.phantom_prints[0]['time_est']}
  Last:  {self.phantom_prints[-1]['time_est']}

{'='*100}
ALL PHANTOM PRINTS (Chronological Order)
{'='*100}
{'#':<5} {'Time':<12} {'Price':<10} {'Size':<12} {'Distance':<12} {'Exchange':<10} {'Conditions':<20}
{'-'*100}
"""
        
        # Add all prints in order
        for i, p in enumerate(self.phantom_prints, 1):
            conditions_str = ','.join(str(c) for c in p['conditions'][:5])
            if len(p['conditions']) > 5:
                conditions_str += '...'
            
            summary += f"{i:<5} {p['time_est']:<12} ${p['price']:<9.2f} {p['size']:>11,} ${p['distance']:>11.2f} {p['exchange']:<10} {conditions_str:<20}\n"
        
        summary += f"\n{'='*100}\n"
        summary += f"Log file saved to: {self.get_csv_file()}\n"
        summary += f"{'='*100}\n"
        
        return summary
    
    def get_discord_summary(self):
        """
        Generate Discord-friendly summary
        """
        if not self.phantom_prints:
            return None
        
        msg = f"👻 **PhantomSpot Summary - {self.ticker}**\n"
        msg += f"**{self.get_today_str()}**\n\n"
        msg += f"**Statistics:**\n"
        msg += f"• Total Prints: **{len(self.phantom_prints)}**\n"
        msg += f"• Price Range: **${min(p['price'] for p in self.phantom_prints):.2f} - ${max(p['price'] for p in self.phantom_prints):.2f}**\n\n"
        
        msg += f"**All Prints (Chronological):**\n```\n"
        msg += f"{'#':<4} {'Time':<9} {'Price':<8} {'Size':<10}\n"
        msg += f"{'-'*35}\n"
        
        for i, p in enumerate(self.phantom_prints, 1):
            msg += f"{i:<4} {p['time_est'][:8]:<9} ${p['price']:<7.2f} {p['size']:>9,}\n"
        
        msg += "```"
        
        return msg
    
    async def save_summary(self):
        """Save daily summary to file and send to Discord"""
        summary = self.get_daily_summary()
        summary_file = self.log_dir / f"summary_phantoms_{self.ticker}_{self.get_today_str()}.txt"
        with open(summary_file, 'w') as f:
            f.write(summary)
        print(summary, flush=True)
        
        # Send to Discord
        discord_msg = self.get_discord_summary()
        if discord_msg:
            await send_discord(discord_msg)
        
        return summary_file

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
    OPTION 3: Institutional Grade Velocity Divergence Detection
    
    Three types of divergence signals:
    1. Classic Divergence (OR logic - either metric can trigger)
    2. Exhaustion (new extreme on weak volume)
    3. Acceleration Divergence (price speeding up, participation slowing)
    
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
    
    # OPTION 3: OR Logic + Extreme Threshold
    # Trigger if EITHER velocity drops significantly, OR both drop moderately
    EXTREME_THRESHOLD = 0.50  # 50% drop in one metric = instant trigger
    
    is_divergence = (
        # Extreme drop in either metric alone
        trade_vel_drop_pct >= EXTREME_THRESHOLD or
        volume_vel_drop_pct >= EXTREME_THRESHOLD or
        # OR both drop moderately
        (trade_vel_drop_pct >= VELOCITY_DROP_THRESHOLD and 
         volume_vel_drop_pct >= VELOCITY_DROP_THRESHOLD)
    )
    
    if is_divergence:
        # Determine signal strength
        if trade_vel_drop_pct >= EXTREME_THRESHOLD or volume_vel_drop_pct >= EXTREME_THRESHOLD:
            signal_strength = "STRONG"
        else:
            signal_strength = "MODERATE"
        
        alert_data = {
            'type': 'CLASSIC_DIVERGENCE',
            'signal_strength': signal_strength,
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

def detect_exhaustion(windows):
    """
    OPTION 3: Exhaustion Detection
    Price makes new extreme but on declining volume - classic exhaustion pattern
    
    Returns: (is_exhaustion, alert_data) tuple
    """
    if len(windows) < 4:  # Need at least 4 windows to establish average
        return False, None
    
    current = windows[-1]
    current_metrics = current.get_metrics()
    
    # Must make new high or low
    if not (current_metrics['made_new_high'] or current_metrics['made_new_low']):
        return False, None
    
    # Need minimum trades
    if current_metrics['trade_count'] < VELOCITY_MIN_TRADES_PER_WINDOW:
        return False, None
    
    # Calculate average volume of previous 3 windows
    prev_volumes = [w.get_metrics()['total_volume'] for w in list(windows)[-4:-1]]
    avg_prev_volume = sum(prev_volumes) / len(prev_volumes)
    
    if avg_prev_volume == 0:
        return False, None
    
    # Current volume as ratio of average
    volume_ratio = current_metrics['total_volume'] / avg_prev_volume
    
    # Exhaustion = new extreme on <70% of average volume
    if volume_ratio < 0.70:
        alert_data = {
            'type': 'EXHAUSTION',
            'direction': 'HIGH' if current_metrics['made_new_high'] else 'LOW',
            'price': current_metrics['highest_price'] if current_metrics['made_new_high'] else current_metrics['lowest_price'],
            'volume_ratio_pct': volume_ratio * 100,
            'current_volume': current_metrics['total_volume'],
            'avg_prev_volume': avg_prev_volume,
            'current_trade_count': current_metrics['trade_count']
        }
        return True, alert_data
    
    return False, None

def detect_acceleration_divergence(windows):
    """
    OPTION 3: Acceleration Divergence
    Price volatility increasing (range expanding) while velocity decreasing
    Classic blow-off top or capitulation bottom pattern
    
    Returns: (is_divergence, alert_data) tuple
    """
    if len(windows) < 3:
        return False, None
    
    # Get last 3 windows
    w1, w2, w3 = windows[-3], windows[-2], windows[-1]
    m1, m2, m3 = w1.get_metrics(), w2.get_metrics(), w3.get_metrics()
    
    # Need minimum trades in current window
    if m3['trade_count'] < VELOCITY_MIN_TRADES_PER_WINDOW:
        return False, None
    
    # Calculate price range (volatility) per window
    def get_range(metrics):
        if metrics['highest_price'] is None or metrics['lowest_price'] is None:
            return 0
        return abs(metrics['highest_price'] - metrics['lowest_price'])
    
    range1 = get_range(m1)
    range2 = get_range(m2)
    range3 = get_range(m3)
    
    if range1 == 0 or range2 == 0:
        return False, None
    
    # Price volatility is accelerating (range expanding)
    price_accelerating = range3 > range2 and range2 > range1
    
    # Calculate percentage increase in volatility
    if price_accelerating:
        volatility_increase_pct = ((range3 - range1) / range1) * 100
    else:
        return False, None
    
    # Velocity is decelerating
    vel1 = m1['trade_velocity']
    vel2 = m2['trade_velocity']
    vel3 = m3['trade_velocity']
    
    if vel1 == 0:
        return False, None
    
    velocity_declining = vel3 < vel2 < vel1
    velocity_decrease_pct = ((vel1 - vel3) / vel1) * 100
    
    # Both conditions must be true
    if price_accelerating and velocity_declining:
        # Determine direction based on price movement
        if m3['made_new_high']:
            direction = "HIGH"
            price = m3['highest_price']
        elif m3['made_new_low']:
            direction = "LOW"
            price = m3['lowest_price']
        else:
            # Acceleration happening mid-range
            direction = "MID-RANGE"
            price = (m3['highest_price'] + m3['lowest_price']) / 2 if m3['highest_price'] else None
        
        if price is None:
            return False, None
        
        alert_data = {
            'type': 'ACCELERATION_DIVERGENCE',
            'direction': direction,
            'price': price,
            'volatility_increase_pct': volatility_increase_pct,
            'velocity_decrease_pct': velocity_decrease_pct,
            'current_range': range3,
            'previous_range': range1,
            'current_velocity': vel3,
            'previous_velocity': vel1,
            'current_trade_count': m3['trade_count']
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
# ======================================================
# MARKET HOLIDAY CHECKER
# ======================================================
async def is_market_holiday():
    """
    Check if today is a market holiday or weekend.
    Uses /v1/marketstatus/upcoming endpoint — checks if today appears as a closed day.
    Returns: True if market is closed (holiday/weekend), False if open.
    """
    today = datetime.now(ET).date()

    # Weekend check first — no API call needed
    if today.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        print(f"📅 {today} is a weekend - market closed", flush=True)
        return True

    # Check Massive upcoming market status for holidays
    try:
        url = "https://api.massive.io/v1/marketstatus/upcoming"
        headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    print(f"⚠️ Market status API returned {response.status}, assuming market open", flush=True)
                    return False
                data = await response.json()

        # Log raw response once for debugging field names
        print(f"📅 Market status response: {str(data)[:300]}", flush=True)

        # The endpoint returns a list of upcoming market status entries.
        # Each entry expected to have a date field and a status/open field.
        # Mark as holiday if today's date appears with a closed/holiday status.
        today_str = today.strftime("%Y-%m-%d")

        entries = data if isinstance(data, list) else data.get("results", data.get("data", []))
        for entry in entries:
            # Try common field name patterns
            entry_date = entry.get("date", entry.get("day", ""))
            if not entry_date.startswith(today_str):
                continue
            # Check if this entry indicates market is closed
            is_open = entry.get("open", entry.get("isOpen", entry.get("status", "open")))
            if is_open is False or str(is_open).lower() in ("false", "closed", "holiday", "0"):
                name = entry.get("name", entry.get("description", "Market Holiday"))
                print(f"📅 {today} is {name} - market closed", flush=True)
                return True
            else:
                print(f"📅 {today} is a trading day - market open", flush=True)
                return False

        # Today not found in upcoming — means it's a normal trading day
        print(f"📅 {today} not in closed dates — market open", flush=True)
        return False

    except Exception as e:
        print(f"⚠️ Error checking market status: {e}, assuming market open", flush=True)
        return False

# ======================================================
# POSTGRES CONNECTION WITH AUTO-RECONNECT
# ======================================================
async def ensure_db_connection(db):
    """
    Ensure database connection is alive, reconnect if needed
    Returns: Valid database connection
    """
    try:
        # Test if connection is alive with a simple query
        await db.execute("SELECT 1")
        return db
    except Exception as e:
        print(f"⚠️ Database connection lost: {e}", flush=True)
        print("🔄 Reconnecting to database...", flush=True)
        try:
            # Close old connection if possible
            try:
                await db.close()
            except:
                pass
            
            # Create new connection
            new_db = await asyncpg.connect(POSTGRES_URL)
            print("✅ Database reconnected successfully", flush=True)
            return new_db
        except Exception as reconnect_error:
            print(f"❌ Failed to reconnect to database: {reconnect_error}", flush=True)
            raise

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
# QCT TRACKER — Conditions [53, 52, 41] Volume Monitor
# Tracks volume % of institutional multi-leg (QCT) trades vs total daily volume
# Reports at 3:30 PM ET (intraday check) and 8:01 PM ET (end-of-day summary)
# ======================================================
class QCTTracker:
    """
    Tracks trades with conditions [53, 52, 41] (Qualified Contingent Trade + Contingent + Trade-Thru-Exempt).
    These are institutional multi-leg hedging/unwinding structures.
    Accumulates volume in-memory and reports % of total daily volume at scheduled times.
    """
    TARGET_CONDITIONS = frozenset([41, 52, 53])

    def __init__(self, ticker="SPY"):
        self.ticker = ticker
        self.total_volume = 0
        self.qct_volume = 0
        self.total_trades = 0
        self.qct_trades = 0
        self.session_date = datetime.now(ET).date()
        print("✅ QCT tracker ENABLED — monitoring conditions [53, 52, 41]", flush=True)

    def record_trade(self, size: int, conds: list):
        """Call this for every trade event."""
        # Reset if date rolled over
        today = datetime.now(ET).date()
        if today != self.session_date:
            self.reset()
            self.session_date = today

        self.total_volume += size
        self.total_trades += 1

        if frozenset(conds) == self.TARGET_CONDITIONS:
            self.qct_volume += size
            self.qct_trades += 1

    def reset(self):
        self.total_volume = 0
        self.qct_volume = 0
        self.total_trades = 0
        self.qct_trades = 0

    def get_pct(self, override_total=None) -> float:
        """Return QCT volume as % of total daily volume. Optional override for official API volume."""
        total = override_total if override_total is not None else self.total_volume
        if total == 0:
            return 0.0
        return round((self.qct_volume / total) * 100, 4)

    def build_report(self, label: str, official_volume: int = None) -> str:
        pct = self.get_pct(override_total=official_volume)
        now_str = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")

        # Volume display — show official if available, streamed as fallback with note
        if official_volume is not None:
            volume_line = f"Total Daily Volume: **{official_volume:,}** shares *(official API)*"
        else:
            volume_line = f"Total Daily Volume: **{self.total_volume:,}** shares *(streamed — may be incomplete)*"

        # Contextual flag based on historically elevated levels
        if pct >= 5.0:
            flag = "🚨 **ELEVATED** — historically preceded sharp sell-offs (e.g., 02-02: 5.33%, 02-10: 5.39%)"
        elif pct >= 3.5:
            flag = "⚠️ **NOTEWORTHY** — above average institutional hedging/unwinding activity"
        elif pct >= 2.0:
            flag = "📊 **MODERATE** — within normal range, worth watching"
        else:
            flag = "✅ **LOW** — no unusual QCT activity"

        return (
            f"{'🔔' if 'Intraday' in label else '📋'} **QCT Report — {label}**\n"
            f"Symbol: **{self.ticker}** | Time: {now_str}\n"
            f"─────────────────────────────\n"
            f"QCT Volume `[53, 52, 41]`: **{self.qct_volume:,}** shares\n"
            f"{volume_line}\n"
            f"**QCT % of Total Volume: {pct:.4f}%**\n"
            f"QCT Trade Count: {self.qct_trades:,} | Total Trades: {self.total_trades:,}\n"
            f"─────────────────────────────\n"
            f"{flag}"
        )


async def fetch_official_daily_volume(symbol: str, api_key: str) -> int | None:
    """
    Fetch today's official total volume from Massive REST API.
    Uses the open-close endpoint: /v1/open-close/{symbol}/{date}
    Returns volume as int, or None if the call fails.
    """
    try:
        today = datetime.now(ET).date()
        url = f"https://api.massive.io/v1/open-close/{symbol}/{today.isoformat()}"
        headers = {"Authorization": f"Bearer {api_key}"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    text = await r.text()
                    print(f"⚠️ Massive open-close API returned {r.status}: {text[:100]}", flush=True)
                    return None
                data = await r.json()

        # Response field for volume — log full response once so we can verify field name
        print(f"✅ Massive open-close response: {data}", flush=True)
        vol = int(data.get("volume", data.get("v", 0)))
        if vol == 0:
            print("⚠️ Volume field was 0 or missing in open-close response", flush=True)
            return None
        print(f"✅ Official daily volume from Massive: {vol:,}", flush=True)
        return vol

    except Exception as e:
        print(f"⚠️ fetch_official_daily_volume error: {e}", flush=True)
        return None


async def run_qct_scheduler(qct_tracker):
    """
    Separate coroutine — runs alongside the main websocket loop via asyncio.gather.
    3:30 PM ET: intraday check using streamed volume.
    8:05 PM ET: EOD report using official Massive API volume (gives API time to finalize).
    Resets flags for the new day automatically.
    """
    reported_330 = False
    reported_eod = False
    last_report_date = None

    print("🕐 QCT scheduler started — will report at 3:30 PM and 8:05 PM ET", flush=True)

    while True:
        now_et = datetime.now(ET)
        today = now_et.date()

        # Reset flags on new trading day
        if last_report_date is not None and today > last_report_date:
            reported_330 = False
            reported_eod = False

        t = now_et.time()

        # 3:30 PM intraday report — streamed volume is fine here, market still open
        if not reported_330 and t >= time(15, 30) and t < time(20, 5):
            report = qct_tracker.build_report("Intraday 3:30 PM Check")
            print(f"\n{report}\n", flush=True)
            await send_discord(report)
            reported_330 = True
            last_report_date = today

        # 8:05 PM EOD report — fetch official volume from Massive API
        if not reported_eod and t >= time(20, 5):
            print("📊 Fetching official daily volume for QCT EOD report...", flush=True)
            official_vol = await fetch_official_daily_volume(SYMBOL, MASSIVE_API_KEY)
            report = qct_tracker.build_report("End-of-Day Summary", official_volume=official_vol)
            print(f"\n{report}\n", flush=True)
            await send_discord(report)
            reported_eod = True
            last_report_date = today

        # Sleep 30 seconds between checks — lightweight
        await asyncio.sleep(30)


# ======================================================
# MAIN
# ======================================================
async def run(shared=None):
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
    
    # Initialize zero-size trade logger
    zero_logger = None
    if ZERO_SIZE_LOGGING_ENABLED:
        zero_logger = ZeroSizeTradeLogger(SYMBOL)
        print("✅ Zero-size trade logging ENABLED", flush=True)
    else:
        print("⏸️ Zero-size trade logging DISABLED", flush=True)
    
    # Initialize dark pool tracker
    dark_pool_tracker = DarkPoolTracker(SYMBOL, DARK_POOL_SIZE_THRESHOLD)
    print("✅ Dark pool tracker ENABLED", flush=True)
    
    # Initialize phantom print tracker
    phantom_tracker = PhantomPrintTracker(SYMBOL)
    print("✅ Phantom print tracker ENABLED", flush=True)

    # Initialize QCT tracker
    qct_tracker = QCTTracker(SYMBOL)
    if shared is not None:
        shared["qct_tracker"] = qct_tracker

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
    
    # Track last summary generation time (generate once at end of day)
    last_summary_date = None
    summary_generated_today = False
    
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,  # Send keepalive ping every 20 seconds
                ping_timeout=10    # Wait 10 seconds for pong response
            ) as ws:
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

                        # QCT volume tracking — record every trade (all sessions, all sizes)
                        qct_tracker.record_trade(size, conds)

                        tm = datetime.fromtimestamp(sip_ts_raw/1000, tz=ET).time()

                        # ====================================================================
                        # END-OF-DAY SUMMARY GENERATION
                        # Generate summary once after market close
                        # Skip weekends and market holidays
                        # ====================================================================
                        current_date = datetime.now(ET).date()
                        is_after_close = tm >= time(20, 0)  # After 8 PM ET
                        
                        if is_after_close and not summary_generated_today and (last_summary_date is None or last_summary_date < current_date):
                            # Check if today was a trading day (not weekend/holiday)
                            is_holiday = await is_market_holiday()
                            
                            if not is_holiday:
                                # Generate zero-size summary
                                if zero_logger and zero_logger.zero_trades:
                                    print("🕐 Market closed. Generating zero-size trade summary...", flush=True)
                                    await zero_logger.save_summary()
                                
                                # Generate dark pool summary
                                if dark_pool_tracker.dark_pool_prints:
                                    print("🕐 Market closed. Generating dark pool summary...", flush=True)
                                    await dark_pool_tracker.save_summary()
                                
                                # Generate phantom print summary
                                if phantom_tracker.phantom_prints:
                                    print("🕐 Market closed. Generating phantom print summary...", flush=True)
                                    await phantom_tracker.save_summary()
                            else:
                                print("📅 Skipping summaries - market was closed today (weekend/holiday)", flush=True)
                            
                            summary_generated_today = True
                            last_summary_date = current_date
                        
                        # Reset flag for new day
                        if last_summary_date is not None and current_date > last_summary_date:
                            summary_generated_today = False

                        # ====================================================================
                        # ZERO-SIZE TRADE DETECTION
                        # Check for zero-size trades and log them for pattern analysis
                        # ====================================================================
                        if zero_logger and size == 0:
                            # Build trade data dict for logger
                            zero_trade_data = {
                                'price': price,
                                'size': size,
                                'exchange': exch,
                                'conditions': conds,
                                'sequence': sequence,
                                'sip_timestamp': sip_ts_raw,
                                'trf_timestamp': trf_ts_raw,
                                'trf_id': trf_id
                            }
                            trade_record = zero_logger.log_zero_trade(zero_trade_data)

                            # Queue for batched Discord alert (first fires immediately, rest batch in 10s)
                            zero_logger.queue_discord_alert(trade_record)

                            # Flush console buffer every 30s to keep Railway logs manageable
                            zero_logger.flush_console_buffer()

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
                                    # OPTION 3: Check all three divergence types
                                    
                                    # 1. Classic Divergence (with OR logic)
                                    is_divergence, alert_data = detect_velocity_divergence(
                                        velocity_windows,
                                        today_high if today_high else 0,
                                        today_low if today_low else 0
                                    )
                                    
                                    # 2. Exhaustion Detection
                                    is_exhaustion, exhaustion_data = detect_exhaustion(velocity_windows)
                                    
                                    # 3. Acceleration Divergence
                                    is_acceleration, acceleration_data = detect_acceleration_divergence(velocity_windows)
                                    
                                    # Handle Classic Divergence Alert
                                    if is_divergence:
                                        velocity_confirmation_count += 1
                                        
                                        # Alert after required confirmations and cooldown
                                        if (velocity_confirmation_count >= VELOCITY_CONFIRMATION_WINDOWS and 
                                            now - last_velocity_alert > VELOCITY_COOLDOWN):
                                            
                                            last_velocity_alert = now
                                            velocity_confirmation_count = 0  # Reset counter
                                            
                                            # Determine emoji based on signal strength
                                            emoji = "⚡⚡" if alert_data.get('signal_strength') == 'STRONG' else "⚡"
                                            
                                            print(
                                                f"{emoji} VELOCITY DIVERGENCE {ts_str()} "
                                                f"{alert_data['direction']} @ ${alert_data['price']:.2f} "
                                                f"trade_vel_drop={alert_data['trade_vel_drop_pct']:.1f}% "
                                                f"volume_vel_drop={alert_data['volume_vel_drop_pct']:.1f}% "
                                                f"strength={alert_data.get('signal_strength', 'MODERATE')}",
                                                flush=True
                                            )
                                            
                                            # Send to Discord with signal strength indicator
                                            vel_msg = (
                                                f"{emoji} **Velocity Divergence Detected**\n"
                                                f"Signal Strength: **{alert_data.get('signal_strength', 'MODERATE')}**\n"
                                                f"Direction: **{alert_data['direction']}** at **${alert_data['price']:.2f}**\n"
                                                f"Trade Velocity Drop: **{alert_data['trade_vel_drop_pct']:.1f}%**\n"
                                                f"Volume Velocity Drop: **{alert_data['volume_vel_drop_pct']:.1f}%**\n"
                                                f"Current Window: {alert_data['current_trade_count']} trades, "
                                                f"{alert_data['current_volume']:,} shares\n"
                                                f"Previous Avg: {alert_data['prev_avg_trades']:.0f} trades per window\n"
                                                f"Session Range: [{alert_data['session_low']:.2f}, {alert_data['session_high']:.2f}]\n"
                                                f"Time: {ts_str()}\n\n"
                                                f"💡 **What This Means:** Price made new {alert_data['direction'].lower()} but "
                                                f"{'with significantly weaker' if alert_data.get('signal_strength') == 'STRONG' else 'with declining'} "
                                                f"participation. Potential reversal or consolidation ahead."
                                            )
                                            asyncio.create_task(send_discord(vel_msg))
                                    else:
                                        # Reset confirmation counter if divergence not detected
                                        velocity_confirmation_count = 0
                                    
                                    # Handle Exhaustion Alert (separate from classic divergence)
                                    if is_exhaustion and now - last_velocity_alert > VELOCITY_COOLDOWN:
                                        last_velocity_alert = now
                                        
                                        print(
                                            f"💀 EXHAUSTION DETECTED {ts_str()} "
                                            f"{exhaustion_data['direction']} @ ${exhaustion_data['price']:.2f} "
                                            f"volume={exhaustion_data['volume_ratio_pct']:.1f}% of avg "
                                            f"({exhaustion_data['current_volume']:,} vs {exhaustion_data['avg_prev_volume']:,.0f})",
                                            flush=True
                                        )
                                        
                                        exh_msg = (
                                            f"💀 **Exhaustion Pattern Detected**\n"
                                            f"Direction: **{exhaustion_data['direction']}** at **${exhaustion_data['price']:.2f}**\n"
                                            f"Volume Ratio: **{exhaustion_data['volume_ratio_pct']:.1f}%** of recent average\n"
                                            f"Current Volume: **{exhaustion_data['current_volume']:,} shares**\n"
                                            f"Recent Avg: **{exhaustion_data['avg_prev_volume']:,.0f} shares**\n"
                                            f"Current Trades: {exhaustion_data['current_trade_count']}\n"
                                            f"Time: {ts_str()}\n\n"
                                            f"💡 **What This Means:** Price pushed to new {exhaustion_data['direction'].lower()} "
                                            f"on only {exhaustion_data['volume_ratio_pct']:.0f}% of normal volume. "
                                            f"Classic exhaustion - fewer participants willing to chase. "
                                            f"{'Bearish reversal' if exhaustion_data['direction'] == 'HIGH' else 'Bullish reversal'} likely."
                                        )
                                        asyncio.create_task(send_discord(exh_msg))
                                    
                                    # Handle Acceleration Divergence Alert
                                    if is_acceleration and now - last_velocity_alert > VELOCITY_COOLDOWN:
                                        last_velocity_alert = now
                                        
                                        print(
                                            f"🔥 ACCELERATION DIVERGENCE {ts_str()} "
                                            f"{acceleration_data['direction']} @ ${acceleration_data['price']:.2f} "
                                            f"volatility+{acceleration_data['volatility_increase_pct']:.1f}% "
                                            f"velocity-{acceleration_data['velocity_decrease_pct']:.1f}%",
                                            flush=True
                                        )
                                        
                                        acc_msg = (
                                            f"🔥 **Acceleration Divergence Detected**\n"
                                            f"Direction: **{acceleration_data['direction']}** at **${acceleration_data['price']:.2f}**\n"
                                            f"Volatility Increase: **+{acceleration_data['volatility_increase_pct']:.1f}%**\n"
                                            f"Velocity Decrease: **-{acceleration_data['velocity_decrease_pct']:.1f}%**\n"
                                            f"Current Range: **${acceleration_data['current_range']:.2f}**\n"
                                            f"Previous Range: **${acceleration_data['previous_range']:.2f}**\n"
                                            f"Current Trades: {acceleration_data['current_trade_count']}\n"
                                            f"Time: {ts_str()}\n\n"
                                            f"💡 **What This Means:** Price is moving faster (wider ranges) but "
                                            f"with fewer participants (declining velocity). Classic blow-off pattern. "
                                            f"{'Buyers exhausting at top' if acceleration_data['direction'] == 'HIGH' else 'Sellers exhausting at bottom'}. "
                                            f"Sharp reversal often follows."
                                        )
                                        asyncio.create_task(send_discord(acc_msg))

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
                            
                            # LOG TO PHANTOM TRACKER
                            phantom_trade_data = {
                                'price': price,
                                'size': size,
                                'exchange': exch,
                                'conditions': conds,
                                'sequence': sequence,
                                'sip_timestamp': sip_ts_raw,
                                'trf_timestamp': trf_ts_raw,
                                'trf_id': trf_id
                            }
                            phantom_tracker.log_phantom_print(phantom_trade_data, distance)
                            
                            # ALWAYS SEND TO DISCORD
                            msg = (
                                f"🚨 **{SYMBOL} Phantom Print Detected**\n"
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
                            
                            # INSERT INTO POSTGRES (with connection check)
                            try:
                                db = await ensure_db_connection(db)
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
                            except Exception as db_error:
                                print(f"⚠️ Failed to insert phantom to database: {db_error}", flush=True)
                                # Continue execution even if database insert fails
                            
                            # WINDOW LOGIC
                            if now - last_phantom_alert > PHANTOM_COOLDOWN:
                                last_phantom_alert = now
                                print("🔥🔥 NEW PHANTOM WINDOW OPEN 🔥🔥", flush=True)
                            else:
                                print("⏳ (within cooldown window)", flush=True)
                        
                        # Dark pool large print detection
                        is_darkpool = (exch == 4)  # Exchange 4 is dark pool
                        if is_darkpool and size >= DARK_POOL_SIZE_THRESHOLD and not bad_conditions:
                            # Log to tracker for end-of-day summary (ALWAYS logged individually!)
                            # Returns complete trade_record with calculated notional
                            dp_trade_data = {
                                'price': price,
                                'size': size,
                                'conditions': conds,
                                'sequence': sequence,
                                'sip_timestamp': sip_ts_raw,
                                'trf_timestamp': trf_ts_raw
                            }
                            complete_record = dark_pool_tracker.log_dark_pool_print(dp_trade_data)
                            
                            # Print to console
                            print(
                                f"🟣 LARGE DARK POOL PRINT {ts_str()} ${price} "
                                f"size={size:,} notional=${price * size:,.2f} conds={conds} seq={sequence}",
                                flush=True
                            )
                            
                            # Queue for smart batched Discord alert
                            # Use complete_record which has notional calculated
                            asyncio.create_task(dark_pool_tracker.queue_for_alert(complete_record))
                        
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
            # Don't generate summaries on every error - this was causing spam
            # Only database connection errors should be handled gracefully
            error_msg = str(e)
            
            # If it's a database error, just log it and continue
            if "connection is closed" in error_msg.lower() or "asyncpg" in error_msg.lower():
                print(f"⚠️ Database connection error (will auto-reconnect): {e}", flush=True)
                # Try to reconnect database
                try:
                    db = await ensure_db_connection(db)
                except:
                    print("⚠️ Database reconnection failed, will retry on next operation", flush=True)
            else:
                # For other errors, print full traceback
                print(f"⚠️ Unexpected error: {e}", flush=True)
                import traceback
                traceback.print_exc()
            
            print("🔁 Reconnecting websocket in 5 seconds...", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    import signal

    # We need qct_tracker accessible to both run() and the scheduler.
    # The cleanest approach: run() returns qct_tracker via a shared container,
    # but since run() is an infinite loop, we use a shared mutable dict instead.
    _shared = {}

    async def main():
        # run() now yields qct_tracker via _shared before entering its loop
        # We launch both coroutines; scheduler waits for qct_tracker to be ready
        await asyncio.gather(
            run(_shared),
            _run_scheduler_when_ready(_shared),
        )

    async def _run_scheduler_when_ready(shared):
        # Wait until run() has initialized qct_tracker
        while "qct_tracker" not in shared:
            await asyncio.sleep(0.5)
        await run_qct_scheduler(shared["qct_tracker"])
    
    # Global references for signal handler
    global_zero_logger = None
    global_dark_pool_tracker = None
    global_phantom_tracker = None
    
    def signal_handler(signum, frame):
        """Handle shutdown signals and generate final summaries"""
        print("\n\n🛑 Shutdown signal received. Generating final summaries...", flush=True)
        
        # Zero-size summary
        if global_zero_logger and global_zero_logger.zero_trades:
            summary = global_zero_logger.get_daily_summary()
            summary_file = global_zero_logger.log_dir / f"summary_{global_zero_logger.ticker}_{global_zero_logger.today}.txt"
            with open(summary_file, 'w') as f:
                f.write(summary)
            print(summary, flush=True)
        
        # Dark pool summary
        if global_dark_pool_tracker and global_dark_pool_tracker.dark_pool_prints:
            summary = global_dark_pool_tracker.get_daily_summary()
            summary_file = global_dark_pool_tracker.log_dir / f"summary_dark_pool_{global_dark_pool_tracker.ticker}_{global_dark_pool_tracker.today}.txt"
            with open(summary_file, 'w') as f:
                f.write(summary)
            print(summary, flush=True)
        
        # Phantom print summary
        if global_phantom_tracker and global_phantom_tracker.phantom_prints:
            summary = global_phantom_tracker.get_daily_summary()
            summary_file = global_phantom_tracker.log_dir / f"summary_phantoms_{global_phantom_tracker.ticker}_{global_phantom_tracker.today}.txt"
            with open(summary_file, 'w') as f:
                f.write(summary)
            print(summary, flush=True)
        
        exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(main())
    except Exception as e:
        import traceback
        print("❌ Fatal crash:", e, flush=True)
        traceback.print_exc()
        raise
