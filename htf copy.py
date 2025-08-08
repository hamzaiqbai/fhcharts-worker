import asyncio
import json
import csv
import time
import math
import os
from collections import defaultdict, deque
import websockets
from datetime import datetime, timedelta
import statistics
import numpy as np


# how many past aggTrades to use for our rolling stats
TRADE_SIZES_WINDOW = 1000
trade_size_buffer   = deque(maxlen=TRADE_SIZES_WINDOW)

# fallback minimum until 100 samples collected
STATIC_FLOOR = 10.0   # adjust as you like

TICK_SIZE = 0.01   #  ← NEW (from exchangeInfo["tickSize"])

# Configuration: timeframes in ms and corresponding price bin sizes
TF_CONFIG = {
    '1m':  {'ms': 1 * 60 * 1000,  'bin_size': 0.05},
    '3m':  {'ms': 3 * 60 * 1000,  'bin_size': 0.10},
    '5m':  {'ms': 5 * 60 * 1000,  'bin_size': 0.10},
    '15m': {'ms':15 * 60 * 1000,  'bin_size': 0.20},
    '30m': {'ms':30 * 60 * 1000,  'bin_size': 0.30},
    '1h':  {'ms':60 * 60 * 1000,  'bin_size': 0.50},
    '4h':  {'ms':4  * 60 * 60 * 1000,'bin_size': 1.00},
}

def is_large_order(qty):
    """
    Returns True if this trade quantity exceeds the dynamic threshold
    (mean + 2×stdev of last TRADE_SIZES_WINDOW qtys), else False.
    Falls back to STATIC_FLOOR until 100 samples are in the buffer.
    """
    trade_size_buffer.append(qty)
    if len(trade_size_buffer) >= 100:
        mu    = statistics.mean(trade_size_buffer)
        sigma = statistics.stdev(trade_size_buffer)
        threshold = mu + 2 * sigma
    else:
        threshold = STATIC_FLOOR
    return qty >= threshold

def classify_order_type(maker_flag: bool) -> str:
    """
    If maker_flag is True, this was a resting limit order;
    if False, it was a market (taker) order.
    """
    return 'limit'  if maker_flag else 'market'


def should_reset_cvd(tf, prev_ts_ms, curr_ts_ms):
    prev_dt = datetime.utcfromtimestamp(prev_ts_ms / 1000)
    curr_dt = datetime.utcfromtimestamp(curr_ts_ms / 1000)

    if tf in ('1m', '3m'):
        prev_qtr = (prev_dt.minute // 15, prev_dt.hour, prev_dt.day)
        curr_qtr = (curr_dt.minute // 15, curr_dt.hour, curr_dt.day)
        return prev_qtr != curr_qtr

    elif tf in ('5m', '15m'):
        return prev_dt.date() != curr_dt.date()

    elif tf in ('30m', '1h'):
        prev_week = prev_dt.isocalendar()[1]
        curr_week = curr_dt.isocalendar()[1]
        return prev_week != curr_week

    elif tf == '4h':
        return False

    return False


# Initialize bucket state

def init_bucket(bin_size):
    return {
        'bucket':       None,
        'open':         None,
        'high':         float('-inf'),
        'low':          float('inf'),
        'close':        None,
        'total_volume': 0.0,
        'buy_volume':   0.0,
        'sell_volume':  0.0,
        'buy_contracts':0,
        'sell_contracts':0,
        'delta':        0.0,
        'max_delta':    float('-inf'),
        'min_delta':    float('inf'),
        'CVD':          0.0,
        'large_order_count': 0,
        'market_volume': 0.0,
        'limit_volume':  0.0,
        'price_bins':   defaultdict(lambda: {
        'buy_volume': 0.0,
        'sell_volume': 0.0,
        'buy_contracts': 0,
        'sell_contracts': 0}),
    }

# Load existing CSV data at startup
def load_existing_csv_data(tf):
    """Load existing CSV data for a timeframe to preserve historical data"""
    path = f"xmrusdt_{tf}.csv"
    if not os.path.exists(path):
        return []
    
    try:
        with open(path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            print(f"Loaded {len(rows)} existing rows for {tf}")
            return rows
    except Exception as e:
        print(f"Warning: Could not load existing CSV data for {tf}: {e}")
        return []

def parse_bucket_from_csv_row(row, bin_size):
    """Convert a CSV row back into a bucket structure"""
    import json
    
    bucket = init_bucket(bin_size)
    
    # Parse basic fields
    bucket['bucket'] = int(float(row['bucket'])) * 1000  # Convert back to milliseconds
    bucket['open'] = float(row['open']) if row['open'] else None
    bucket['high'] = float(row['high']) if row['high'] else float('-inf')
    bucket['low'] = float(row['low']) if row['low'] else float('inf')
    bucket['close'] = float(row['close']) if row['close'] else None
    bucket['total_volume'] = float(row['total_volume'])
    bucket['buy_volume'] = float(row['buy_volume'])
    bucket['sell_volume'] = float(row['sell_volume'])
    bucket['buy_contracts'] = int(row['buy_contracts'])
    bucket['sell_contracts'] = int(row['sell_contracts'])
    bucket['delta'] = float(row['delta'])
    bucket['max_delta'] = float(row['max_delta'])
    bucket['min_delta'] = float(row['min_delta'])
    bucket['CVD'] = float(row['CVD'])
    bucket['large_order_count'] = int(row['large_order_count'])
    
    # Parse price_levels JSON back to price_bins
    try:
        price_levels = json.loads(row['price_levels'])
        for price_str, data in price_levels.items():
            price = float(price_str)
            bucket['price_bins'][price] = {
                'buy_volume': data['buy_volume'],
                'sell_volume': data['sell_volume'],
                'buy_contracts': data['buy_contracts'],
                'sell_contracts': data['sell_contracts']
            }
    except:
        pass  # If parsing fails, keep empty price_bins
    
    return bucket

# Global state per timeframe
def initialize_state():
    """Initialize state with existing CSV data loaded and resume incomplete buckets"""
    import time
    current_time_ms = int(time.time() * 1000)
    
    state = {}
    for tf, cfg in TF_CONFIG.items():
        historical_rows = load_existing_csv_data(tf)
        ms = cfg['ms']
        bin_size = cfg['bin_size']
        
        # Check if we should resume the last bucket
        current_bucket_start = current_time_ms - (current_time_ms % ms)
        resume_bucket = None
        
        if historical_rows:
            # Get the last row
            last_row = historical_rows[-1]
            last_bucket_timestamp = int(float(last_row['bucket'])) * 1000  # Convert to ms
            
            # Check if current time falls within the last bucket's timeframe
            if last_bucket_timestamp == current_bucket_start:
                print(f"Resuming incomplete {tf} bucket at {current_bucket_start}")
                # Remove the last row from historical_rows and use it as current bucket
                historical_rows = historical_rows[:-1]
                resume_bucket = parse_bucket_from_csv_row(last_row, bin_size)
        
        # Initialize current bucket
        if resume_bucket:
            current_bucket = resume_bucket
        else:
            current_bucket = init_bucket(bin_size)
        
        state[tf] = {
            'current': current_bucket,
            'completed': [],
            'historical_rows': historical_rows  # Store existing CSV rows (minus resumed bucket)
        }
    
    return state

STATE = initialize_state()

# Track last CVD timestamp and global CVD per timeframe
last_cvd_ts = {tf: 0 for tf in TF_CONFIG}
global_cvd_state = {tf: 0.0 for tf in TF_CONFIG}

# Resume CVD state from existing data
for tf in TF_CONFIG:
    current_bucket = STATE[tf]['current']
    if current_bucket['bucket'] is not None:
        # Resume from the CVD of the current bucket
        global_cvd_state[tf] = current_bucket['CVD']
        last_cvd_ts[tf] = current_bucket['bucket']
        print(f"Resumed {tf} CVD at {current_bucket['CVD']}")
    elif STATE[tf]['historical_rows']:
        # No current bucket, but we have historical data - get CVD from last historical row
        last_row = STATE[tf]['historical_rows'][-1]
        global_cvd_state[tf] = float(last_row['CVD'])
        last_cvd_ts[tf] = int(float(last_row['bucket'])) * 1000
        print(f"Resumed {tf} CVD from historical data at {global_cvd_state[tf]}")


# CSV header fields
CSV_FIELDS = [
    'bucket','total_volume','buy_volume','sell_volume','buy_contracts','sell_contracts',
    'open','high','low','close','delta','max_delta','min_delta','CVD', 'large_order_count', 'market_limit_ratio',
    'buy_sell_ratio','poc','price_levels','hvns','lvns','imbalances'
]

# Atomic CSV writer with Windows fallback - preserves historical data
def write_csv(tf):
    path      = f"xmrusdt_{tf}.csv"
    temp_path = path + ".tmp"
    rows      = []

    # ── fetch bin_size for this timeframe ──
    bin_size = TF_CONFIG[tf]['bin_size']

    # Start with existing historical rows
    rows.extend(STATE[tf]['historical_rows'])

    # Add completed buckets from current session
    for b in STATE[tf]['completed']:
        rows.append(format_row(b, bin_size))

    # Add current bucket if it exists
    curr = STATE[tf]['current']
    if curr['bucket'] is not None:
        rows.append(format_row(curr, bin_size))

    # write out the temp file
    with open(temp_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    # attempt an atomic replace, retrying if the target is locked
    for _ in range(5):
        try:
            os.replace(temp_path, path)
            return
        except OSError:
            # maybe the GUI still has it open—wait a bit and retry
            time.sleep(0.1)

    # if we get here, all replaces failed; drop back to a non-atomic write
    try:
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(f"Warning: could not update {path}: {e}")

def transfer_completed_to_historical(tf):
    """Transfer completed buckets to historical_rows to avoid rewriting them repeatedly"""
    bin_size = TF_CONFIG[tf]['bin_size']
    
    # Add completed buckets to historical_rows
    for bucket in STATE[tf]['completed']:
        STATE[tf]['historical_rows'].append(format_row(bucket, bin_size))
    
    # Clear completed buckets since they're now in historical_rows
    STATE[tf]['completed'] = []


# Compute Point of Control (POC)
def compute_poc(price_bins):
    max_vol = 0
    poc_price = None
    for price, data in price_bins.items():
        vol = data['buy_volume'] + data['sell_volume']
        if vol > max_vol:
            max_vol = vol
            poc_price = price
    return poc_price

# ─── Imbalance parameters & helper ─────────────────────────────────────────
MIN_VOL      = 5.0     # minimum opposite-side volume to qualify
R_NORM       = 3.0     # ratio ≥ 3 → “normal”
R_STRONG     = 5.0     # ratio ≥ 5 → “strong”
R_HEAVY      = 7.0     # ratio ≥ 7 → “heavy”

def compute_imbalances(price_levels_output):
    """
    price_levels_output: dict price → {buy_volume, sell_volume, …}
    Returns a dict: price → [ { ...curr_stats,
                                'imbalance_type': str,
                                'imbalance_strength': str }, … ]
    Calculates both same-level and diagonal imbalances.
    """
    imbalance_map = {}
    prices = sorted(price_levels_output.keys())

    # infer bin size from price spacing (default to 0 if only one level)
    if len(prices) > 1:
        bin_size = round(prices[1] - prices[0], 2)
    else:
        bin_size = 0.0

    for price in prices:
        curr   = price_levels_output[price]
        buy_v  = curr["buy_volume"]
        sell_v = curr["sell_volume"]

        # 1) same-level BUY imbalance
        if sell_v > MIN_VOL:
            ratio = buy_v / sell_v
            if ratio >= R_NORM:
                strength = ("heavy" if ratio >= R_HEAVY else
                            "strong" if ratio >= R_STRONG else
                            "normal")
                imbalance_map.setdefault(price, []).append({
                    **curr,
                    "imbalance_type":     "buy_same_level",
                    "imbalance_strength": strength
                })

        # 2) same-level SELL imbalance
        if buy_v > MIN_VOL:
            ratio = sell_v / buy_v
            if ratio >= R_NORM:
                strength = ("heavy" if ratio >= R_HEAVY else
                            "strong" if ratio >= R_STRONG else
                            "normal")
                imbalance_map.setdefault(price, []).append({
                    **curr,
                    "imbalance_type":     "sell_same_level",
                    "imbalance_strength": strength
                })

        # ───────────── diagonal imbalances ────────────────────────────────
        if bin_size > 0:

            # 3) BUY-diagonal  =  Ask(P) / Bid(P-bin)
            prev_price = round(price - bin_size, 2)
            prev_bid   = price_levels_output.get(prev_price, {}).get("sell_volume", 0.0)
            if prev_bid > MIN_VOL:
                ratio = buy_v / prev_bid           # ← Ask ÷ Bid
                if ratio >= R_NORM:
                    strength = ("heavy" if ratio >= R_HEAVY else
                                "strong" if ratio >= R_STRONG else
                                "normal")
                    imbalance_map.setdefault(price, []).append({
                        **curr,
                        "imbalance_type":     "buy_diagonal",
                        "imbalance_strength": strength
                    })

            # 4) SELL-diagonal =  Bid(P) / Ask(P+bin)
            next_price = round(price + bin_size, 2)
            next_ask   = price_levels_output.get(next_price, {}).get("buy_volume", 0.0)
            if next_ask > MIN_VOL:
                ratio = sell_v / next_ask          # ← Bid ÷ Ask
                if ratio >= R_NORM:
                    strength = ("heavy" if ratio >= R_HEAVY else
                                "strong" if ratio >= R_STRONG else
                                "normal")
                    imbalance_map.setdefault(price, []).append({
                        **curr,
                        "imbalance_type":     "sell_diagonal",
                        "imbalance_strength": strength
                    })


    return imbalance_map

def find_hvns_and_lvns(price_levels, hvn_frac=0.6, lvn_frac=0.2):
    # price_levels: dict price → total_volume
    prices = sorted(price_levels)
    vols   = [price_levels[p] for p in prices]
    max_vol = max(vols)
    hvn_thresh = hvn_frac * max_vol
    lvn_thresh = lvn_frac * max_vol

    hvns, lvns = [], []
    for i, p in enumerate(prices):
        v = price_levels[p]
        # local peak?
        if ((i == 0 or v >= price_levels[prices[i-1]]) and
            (i == len(prices)-1 or v >= price_levels[prices[i+1]])):
            if v == max_vol or v >= hvn_thresh:
                hvns.append(p)
        # local valley?
        if v > 0 and ((i == 0 or v <= price_levels[prices[i-1]]) and
                      (i == len(prices)-1 or v <= price_levels[prices[i+1]])):
            if v <= lvn_thresh:
                lvns.append(p)

    # cap counts: max 3 HVNs; LVNs no more than #HVNs
    return hvns[:3], lvns[:len(hvns)]


# Format a bucket into a CSV row with 3-decimal rounding and timestamp in seconds
def format_row(b, bin_size):

    buy = b['buy_volume']
    sell = b['sell_volume']
    bs_ratio = buy / sell if sell else float('inf')

    # Convert bucket timestamp from ms to seconds
    bucket_sec = b['bucket'] // 1000 if b['bucket'] is not None else None

    # Round core metrics
    total_vol = round(b['total_volume'], 2)
    buy_vol   = round(buy, 2)
    sell_vol  = round(sell, 2)
    buy_ctr   = b['buy_contracts']
    sell_ctr  = b['sell_contracts']
    open_p    = round(b['open'], 2) if b['open'] is not None else None
    high_p    = round(b['high'], 2) if b['high'] != float('-inf') else None
    low_p     = round(b['low'], 2) if b['low'] != float('inf') else None
    close_p   = round(b['close'], 2) if b['close'] is not None else None
    delta     = round(b['delta'], 2)
    max_d     = round(b['max_delta'], 2)
    min_d     = round(b['min_delta'], 2)
    cvd       = round(b['CVD'], 2)
    bs_r      = round(bs_ratio, 2) if bs_ratio != float('inf') else float('inf')
    poc_val   = compute_poc(b['price_bins'])
    poc_r     = round(poc_val, 2) if poc_val is not None else None

    # --- recreate Code 2’s price_levels structure ---
    price_levels_output = {}
    for price, stats in b['price_bins'].items():
        price_levels_output[round(price, 2)] = {
            "buy_volume":   round(stats["buy_volume"],   2),
            "sell_volume":  round(stats["sell_volume"],  2),
            "buy_contracts": stats["buy_contracts"],
            "sell_contracts":stats["sell_contracts"]
        }
    pl_json = json.dumps(price_levels_output)

    # ── identify multiple HVNs/LVNs ────────────────────────────────
    vol_map = { p: stats["buy_volume"] + stats["sell_volume"]
                for p, stats in price_levels_output.items() }
    hvns, lvns = find_hvns_and_lvns(vol_map)
    hvns_json  = json.dumps([round(p,2) for p in hvns])
    lvns_json  = json.dumps([round(p,2) for p in lvns])


    # ─── compute filtered same‐level imbalances ───────────────────────────────
    imbalance_map = compute_imbalances(price_levels_output)
    im_json      = json.dumps(imbalance_map)


    # after you’ve rounded everything…
    if b['limit_volume'] > 0:
        ml_ratio = b['market_volume'] / b['limit_volume']
    else:
        ml_ratio = None



    return {
        'bucket':         bucket_sec,
        'total_volume':   total_vol,
        'buy_volume':     buy_vol,
        'sell_volume':    sell_vol,
        'buy_contracts':  buy_ctr,
        'sell_contracts': sell_ctr,
        'open':           open_p,
        'high':           high_p,
        'low':            low_p,
        'close':          close_p,
        'delta':          delta,
        'max_delta':      max_d,
        'min_delta':      min_d,
        'CVD':            cvd,
        'large_order_count': b.get('large_order_count', 0),
        'market_limit_ratio':  round(ml_ratio, 2) if ml_ratio is not None else None,
        'buy_sell_ratio': bs_r,
        'poc':            poc_r,
        'price_levels':   pl_json,
        'hvns':            hvns_json,
        'lvns':            lvns_json,
        'imbalances':     im_json
        
    }


# Process aggTrade ticks
def process_trade(trade):
    price = float(trade['p'])
    qty   = float(trade['q'])

    # Ignore zero price/volume
    if price == 0.0 or qty == 0.0:
        return
    ts      = trade['T']
    is_sell = trade['m']
    maker_flag = trade['m']               # True = maker/resting limit; False = taker/market
    order_type = classify_order_type(maker_flag)
    n_tr    = trade['l'] - trade['f'] + 1

    # ensure we’re updating module‐level percentiles
    global burst_pct_75, burst_pct_90, burst_pct_99

   
    for tf, cfg in TF_CONFIG.items():
        ms       = cfg['ms']
        bin_size = cfg['bin_size']
        state    = STATE[tf]
        curr     = state['current']
        start    = ts - (ts % ms)

        # ── NEW: large order & type detection ──────────────────────────────────
        if is_large_order(qty):
            curr['large_order_count'] += 1


        if order_type == 'market':
            curr['market_volume'] += qty
        else:
            curr['limit_volume']  += qty

        # ──────────────────────────────────────────────────────────────────────

        # New bucket?
        if curr['bucket'] is None:
            curr['bucket'] = start
        elif start != curr['bucket']:
            # Transfer any existing completed buckets to historical before adding new one
            transfer_completed_to_historical(tf)
            state['completed'].append(curr)
            curr = init_bucket(bin_size)
            curr['bucket'] = start
            state['current'] = curr

        # OHLC
        if curr['open'] is None:
            curr['open'] = price
        curr['close'] = price
        curr['high']  = max(curr['high'], price)
        curr['low']   = min(curr['low'], price)

        # Volume & contracts
        curr['total_volume'] += qty
        if is_sell:
            curr['sell_volume']    += qty
            curr['sell_contracts'] += n_tr
            curr['delta']          -= qty
        else:
            curr['buy_volume']     += qty
            curr['buy_contracts']  += n_tr
            curr['delta']          += qty

        
        # CVD reset logic with initial timestamp protection
        if last_cvd_ts[tf] > 0 and should_reset_cvd(tf, last_cvd_ts[tf], ts):
            global_cvd_state[tf] = 0.0


        # Update global and current bucket CVD
        if is_sell:
            global_cvd_state[tf] -= qty
        else:
            global_cvd_state[tf] += qty

        curr['CVD'] = global_cvd_state[tf]
        last_cvd_ts[tf] = ts

        # CVD & extremes
        curr['max_delta'] = max(curr['max_delta'], curr['delta'])
        curr['min_delta'] = min(curr['min_delta'], curr['delta'])

         # 1) Quantise raw trade price to legal tick       (140.871 → 140.87)
        price_q = round(price / TICK_SIZE) * TICK_SIZE

        # 2) Snap DOWN into the timeframe’s bin           (140.87 → 140.85 for 0.05 bins)
        bin_p   = math.floor(price_q / bin_size) * bin_size
        bin_p   = round(bin_p, 2)   # keep exactly 2-decimals, avoids float fuzz
        side_vol_key  = 'sell_volume' if is_sell else 'buy_volume'
        side_ctr_key  = 'sell_contracts' if is_sell else 'buy_contracts'
        curr['price_bins'][bin_p][side_vol_key]  += qty
        curr['price_bins'][bin_p][side_ctr_key]  += n_tr

        # Persist CSV
        write_csv(tf)

# Entry point
async def main():
    uri = "wss://fstream.binance.com/ws/xmrusdt@aggTrade"
    while True:
        try:
            # Send a ping every 30s, wait up to 20s for its pong
            async with websockets.connect(
                    uri,
                    ping_interval=30,
                    ping_timeout=20
                ) as ws:
                print("🔗 Connected")
                async for msg in ws:
                    process_trade(json.loads(msg))
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"⚠️ Connection lost ({e}); reconnecting in 5s…")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error: {e!r}; exiting.")
            break  # or `continue` if you’d rather keep trying


if __name__ == '__main__':
    os.makedirs('.', exist_ok=True)
    asyncio.run(main())
