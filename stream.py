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

# ======================================================
# ZERO-SIZE TRADE LOGGER
# ======================================================
class ZeroSizeTradeLogger:
    """
    Logs all zero-size trades for analysis
    Creates daily CSV and JSON files for pattern recognition
    """
    def __init__(self, ticker="SPY"):
        self.ticker = ticker
        
        # Use /tmp for Railway ephemeral storage (files persist during deployment)
        # Railway resets /tmp on each deploy, which is fine for daily logs
        self.log_dir = Path("/tmp/zero_size_logs")
        self.log_dir.mkdir(exist_ok=True)
        
        # In-memory storage for session
        self.zero_trades = []
        
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
    
    def log_zero_trade(self, trade_data):
        """
        Log a zero-size trade to both CSV and JSON
        Also print to console for real-time monitoring
        
        Args:
            trade_data: Dictionary with Massive.com trade fields
        """
        # Extract Massive.com specific fields
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
        
        # Print to console
        self.print_zero_trade(trade_record)
        
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
    
    def print_zero_trade(self, record):
        """Print zero-size trade to console with formatting"""
        print(f"\n{'='*80}", flush=True)
        print(f"🔍 ZERO-SIZE TRADE DETECTED - {self.ticker}", flush=True)
        print(f"{'='*80}", flush=True)
        print(f"Time:       {record['time_est']}", flush=True)
        print(f"Price:      ${record['price']:.2f}", flush=True)
        print(f"Exchange:   {record['exchange_name']} ({record['exchange']})", flush=True)
        print(f"Conditions: {', '.join(str(c) for c in record['conditions']) if record['conditions'] else 'None'}", flush=True)
        print(f"Sequence:   {record['sequence']}", flush=True)
        print(f"TRF ID:     {record['trf_id']}", flush=True)
        print(f"{'='*80}\n", flush=True)
    
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
    
    async def save_summary(self):
        """Save daily summary to file and send to Discord"""
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
            await send_discord(discord_msg)
        
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

                        # ====================================================================
                        # END-OF-DAY SUMMARY GENERATION
                        # Generate summary once after market close
                        # ====================================================================
                        current_date = datetime.now(ET).date()
                        is_after_close = tm >= time(20, 0)  # After 8 PM ET
                        
                        if is_after_close and not summary_generated_today and (last_summary_date is None or last_summary_date < current_date):
                            
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
                            zero_logger.log_zero_trade(zero_trade_data)
                            
                            # Send immediate Discord alert
                            exchange_name = zero_logger.get_exchange_name(exch)
                            zero_msg = (
                                f"🔍 **Zero-Size Trade Detected**\n"
                                f"Price: **${price}**\n"
                                f"Exchange: {exchange_name} ({exch})\n"
                                f"Conditions: {conds}\n"
                                f"SIP Time: {datetime.fromtimestamp(sip_ts_raw/1000, tz=ET)}\n"
                                f"Sequence: {sequence}\n"
                                f"TRF ID: {trf_id}"
                            )
                            asyncio.create_task(send_discord(zero_msg))

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
                            # Log to tracker for end-of-day summary
                            dp_trade_data = {
                                'price': price,
                                'size': size,
                                'conditions': conds,
                                'sequence': sequence,
                                'sip_timestamp': sip_ts_raw,
                                'trf_timestamp': trf_ts_raw
                            }
                            dark_pool_tracker.log_dark_pool_print(dp_trade_data)
                            
                            # Print to console
                            print(
                                f"🟣 LARGE DARK POOL PRINT {ts_str()} ${price} "
                                f"size={size:,} notional=${price * size:,.2f} conds={conds} seq={sequence}",
                                flush=True
                            )
                            
                            # Send immediate alert to Discord
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
            # Generate summaries if data exists
            if zero_logger and zero_logger.zero_trades:
                print("📊 Generating zero-size trade summary before reconnecting...", flush=True)
                await zero_logger.save_summary()
            if dark_pool_tracker.dark_pool_prints:
                print("📊 Generating dark pool summary before reconnecting...", flush=True)
                await dark_pool_tracker.save_summary()
            if phantom_tracker.phantom_prints:
                print("📊 Generating phantom print summary before reconnecting...", flush=True)
                await phantom_tracker.save_summary()
            print("🔁 Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            # Generate summaries if data exists
            if zero_logger and zero_logger.zero_trades:
                print("📊 Generating zero-size trade summary before reconnecting...", flush=True)
                await zero_logger.save_summary()
            if dark_pool_tracker.dark_pool_prints:
                print("📊 Generating dark pool summary before reconnecting...", flush=True)
                await dark_pool_tracker.save_summary()
            if phantom_tracker.phantom_prints:
                print("📊 Generating phantom print summary before reconnecting...", flush=True)
                await phantom_tracker.save_summary()
            print("🔁 Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    import signal
    
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
        asyncio.run(run())
    except Exception as e:
        import traceback
        print("❌ Fatal crash:", e, flush=True)
        traceback.print_exc()
        raise
