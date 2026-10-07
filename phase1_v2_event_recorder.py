import asyncio
import json
import websockets
import time
import uuid
from collections import deque
from datetime import datetime
import threading
from flask import Flask
import asyncpg
import urllib.parse
import sys

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# =====================================================================
# NEXUS CLOUD RECORDER (PostgreSQL + Advanced Microstructure)
# =====================================================================

# ⚠️ URL-Encoding the password to handle special characters like @, #, $, %
PASSWORD = urllib.parse.quote_plus("Kavi@#$%9687809464")
DB_URL = f"postgresql://postgres.vzjgoblrwwsqiwhlyorm:{PASSWORD}@aws-0-ap-northeast-2.pooler.supabase.com:6543/postgres"

# --- RENDER / KOYEB KEEP-ALIVE ---
app = Flask("")
@app.route('/')
def home():
    return "NEXUS Cloud Database Engine is LIVE 24/7!"
def run_server():
    app.run(host='0.0.0.0', port=8080)
def keep_alive():
    threading.Thread(target=run_server, daemon=True).start()

# --- CONFIGURATION ---
SNAPSHOT_INTERVAL = 1.0  
LABEL_HORIZON = 60.0     

# --- IN-MEMORY BUFFERS ---
trade_buffer = deque()
event_buffer = deque()

# --- GLOBAL STATE ---
current_price = 0.0
cumulative_cvd = 0.0
buy_wall_price = 0.0
buy_wall_size = 0.0
sell_wall_price = 0.0
sell_wall_size = 0.0
local_liquidity_bids = 0.0
local_liquidity_asks = 0.0

async def init_db():
    """Create Supabase Postgres Tables with ADVANCED Features"""
    conn = await asyncpg.connect(DB_URL)
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS raw_trades (
            ts DOUBLE PRECISION, price DOUBLE PRECISION, qty DOUBLE PRECISION, is_seller BOOLEAN
        )
    ''')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS micro_events (
            event_id TEXT PRIMARY KEY,
            ts DOUBLE PRECISION, price DOUBLE PRECISION, cvd_total DOUBLE PRECISION,
            cvd_5s DOUBLE PRECISION, cvd_15s DOUBLE PRECISION, cvd_30s DOUBLE PRECISION,
            buy_wall_price DOUBLE PRECISION, buy_wall_size DOUBLE PRECISION,
            sell_wall_price DOUBLE PRECISION, sell_wall_size DOUBLE PRECISION, imbalance DOUBLE PRECISION,
            
            local_liquidity_bids DOUBLE PRECISION, local_liquidity_asks DOUBLE PRECISION,
            
            ret_5s DOUBLE PRECISION, ret_30s DOUBLE PRECISION, ret_60s DOUBLE PRECISION,
            mfe_60s DOUBLE PRECISION, mae_60s DOUBLE PRECISION
        )
    ''')
    return conn

# --- WEBSOCKET TASKS ---
async def process_trades(pool):
    global current_price, cumulative_cvd
    uri = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    
    async with websockets.connect(uri) as websocket:
        while True:
            try:
                data = json.loads(await websocket.recv())
                ts, price, qty, is_seller = time.time(), float(data['p']), float(data['q']), data['m']
                
                current_price = price
                cumulative_cvd += -qty if is_seller else qty
                trade_buffer.append({'ts': ts, 'price': price, 'qty': qty, 'is_seller': is_seller})
                
                # Insert into Supabase async pool
                async with pool.acquire() as conn:
                    await conn.execute("INSERT INTO raw_trades (ts, price, qty, is_seller) VALUES ($1, $2, $3, $4)", ts, price, qty, is_seller)
            except:
                await asyncio.sleep(1)

async def process_depth():
    global buy_wall_price, buy_wall_size, sell_wall_price, sell_wall_size
    global local_liquidity_bids, local_liquidity_asks
    
    uri = "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
    async with websockets.connect(uri) as websocket:
        while True:
            try:
                data = json.loads(await websocket.recv())
                
                max_bid = max(data['bids'], key=lambda x: float(x[1]))
                buy_wall_price, buy_wall_size = float(max_bid[0]), float(max_bid[1])
                
                max_ask = max(data['asks'], key=lambda x: float(x[1]))
                sell_wall_price, sell_wall_size = float(max_ask[0]), float(max_ask[1])
                
                local_liquidity_bids = sum([float(b[1]) for b in data['bids']])
                local_liquidity_asks = sum([float(a[1]) for a in data['asks']])
            except:
                await asyncio.sleep(1)

# --- FEATURE ENGINE ---
def get_cvd_over_window(seconds, current_ts):
    cutoff_ts = current_ts - seconds
    window_cvd = 0.0
    for i in range(len(trade_buffer) - 1, -1, -1):
        tr = trade_buffer[i]
        if tr['ts'] < cutoff_ts: break
        window_cvd += -tr['qty'] if tr['is_seller'] else tr['qty']
    return window_cvd

async def snapshot_engine(pool):
    while True:
        await asyncio.sleep(SNAPSHOT_INTERVAL)
        now = time.time()
        if not trade_buffer: continue
            
        cvd_5s = get_cvd_over_window(5, now)
        cvd_15s = get_cvd_over_window(15, now)
        cvd_30s = get_cvd_over_window(30, now)
        
        imbalance = local_liquidity_bids / (local_liquidity_bids + local_liquidity_asks) if (local_liquidity_bids + local_liquidity_asks) > 0 else 0.5
        event_id = f"EVT_{uuid.uuid4().hex[:8].upper()}"
        
        async with pool.acquire() as conn:
            await conn.execute('''INSERT INTO micro_events 
                (event_id, ts, price, cvd_total, cvd_5s, cvd_15s, cvd_30s, 
                 buy_wall_price, buy_wall_size, sell_wall_price, sell_wall_size, imbalance,
                 local_liquidity_bids, local_liquidity_asks)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)''',
                event_id, now, current_price, cumulative_cvd, cvd_5s, cvd_15s, cvd_30s,
                buy_wall_price, buy_wall_size, sell_wall_price, sell_wall_size, imbalance,
                local_liquidity_bids, local_liquidity_asks)
                
        event_buffer.append({'event_id': event_id, 'ts': now, 'price': current_price})

# --- LABELING ENGINE ---
async def labeling_engine(pool):
    while True:
        await asyncio.sleep(1)
        now = time.time()
        
        while len(trade_buffer) > 0 and (now - trade_buffer[0]['ts']) > 120.0:
            trade_buffer.popleft()
            
        while len(event_buffer) > 0 and (now - event_buffer[0]['ts']) >= LABEL_HORIZON:
            evt = event_buffer.popleft()
            event_id, evt_ts, entry_price = evt['event_id'], evt['ts'], evt['price']
            
            prices = [tr['price'] for tr in trade_buffer if evt_ts <= tr['ts'] <= (evt_ts + 60.0)]
            if not prices: continue
            
            p_5s = prices[min(5, len(prices)-1)]
            p_30s = prices[min(30, len(prices)-1)]
            p_60s = prices[-1]
            
            ret_5s = ((p_5s - entry_price) / entry_price) * 100
            ret_30s = ((p_30s - entry_price) / entry_price) * 100
            ret_60s = ((p_60s - entry_price) / entry_price) * 100
            mfe_60s = ((max(prices) - entry_price) / entry_price) * 100
            mae_60s = ((min(prices) - entry_price) / entry_price) * 100
                
            async with pool.acquire() as conn:
                await conn.execute('''UPDATE micro_events 
                    SET ret_5s=$1, ret_30s=$2, ret_60s=$3, mfe_60s=$4, mae_60s=$5 
                    WHERE event_id=$6''', ret_5s, ret_30s, ret_60s, mfe_60s, mae_60s, event_id)
            
            print(f"✅ Cloud Saved {event_id} | +60s Ret: {ret_60s:+.3f}% | Imb: {imbalance:.2f}")

async def main():
    print("==================================================")
    print("☁️ NEXUS CLOUD DATABASE RECORDER (SUPABASE)")
    print("==================================================")
    keep_alive()
    
    # Initialize DB (Creates tables if not exist)
    await init_db()
    
    # Create Connection Pool
    pool = await asyncpg.create_pool(DB_URL)
    print("✅ Connected to Supabase PostgreSQL!")
    
    await asyncio.gather(process_trades(pool), process_depth(), snapshot_engine(pool), labeling_engine(pool))

if __name__ == "__main__":
    asyncio.run(main())
