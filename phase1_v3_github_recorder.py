import asyncio
import json
import websockets
import time
import uuid
from collections import deque
from datetime import datetime, timezone
import threading
from flask import Flask
import os
import gzip
import csv
import base64
import requests
import sys

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# =====================================================================
# NEXUS CLOUD RECORDER V3: GITHUB AUTOMATED ARCHIVER
# (Zero Database Limits | 100% Free | Auto Gzip Compressed)
# =====================================================================

# GitHub Configuration from Environment Variables (Render Dashboard me set karenge)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.getenv("GITHUB_REPO", "").strip() # Format: username/repo-name

# Local Temporary Storage Folder
DATA_DIR = "recorded_batches"
os.makedirs(DATA_DIR, exist_ok=True)

# --- RENDER KEEP-ALIVE SERVER (UptimeRobot ke liye) ---
app = Flask("")

@app.route('/')
def home():
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"NEXUS GitHub Archiver is LIVE 24/7! | Status: Active | Server Time: {now_utc}"

def run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    threading.Thread(target=run_server, daemon=True).start()

# --- CONFIGURATION ---
SNAPSHOT_INTERVAL = 1.0  # Har 1 second ka snapshot
LABEL_HORIZON = 60.0     # 60 second forward returns

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

# Current active day's CSV writer
current_day_str = ""
current_csv_file = ""
csv_lock = threading.Lock()

def get_today_filepath():
    global current_day_str, current_csv_file
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != current_day_str:
        current_day_str = today
        current_csv_file = os.path.join(DATA_DIR, f"btc_micro_{today}.csv")
        if not os.path.exists(current_csv_file):
            with open(current_csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "event_id", "ts", "price", "cvd_total", "cvd_5s", "cvd_15s", "cvd_30s",
                    "buy_wall_price", "buy_wall_size", "sell_wall_price", "sell_wall_size", "imbalance",
                    "local_liquidity_bids", "local_liquidity_asks",
                    "ret_5s", "ret_30s", "ret_60s", "mfe_60s", "mae_60s"
                ])
    return current_csv_file

def append_event_to_local_csv(event_data):
    filepath = get_today_filepath()
    with csv_lock:
        with open(filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(event_data)

# =====================================================================
# GITHUB AUTO-UPLOADER (REST API - 100% Native Python)
# =====================================================================
def upload_file_to_github(local_file_path, repo_file_path, commit_message):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        print("⚠️ GitHub Token ya Repo Name set nahi hai! Skipping GitHub upload.")
        return False

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_file_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Nexus-Quant-Archiver"
    }

    # Compress file to .csv.gz (reduces size from 20MB to ~3MB)
    gz_path = local_file_path + ".gz"
    with open(local_file_path, 'rb') as f_in:
        with gzip.open(gz_path, 'wb') as f_out:
            f_out.writelines(f_in)

    # Read binary and encode to base64
    with open(gz_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("utf-8")

    # Check if file already exists in repo (to get sha for update)
    sha = None
    try:
        check_res = requests.get(url, headers=headers, timeout=15)
        if check_res.status_code == 200:
            sha = check_res.json().get("sha")
    except Exception as e:
        print(f"⚠️ Error checking GitHub file sha: {e}")

    payload = {
        "message": commit_message,
        "content": content_b64
    }
    if sha:
        payload["sha"] = sha

    try:
        put_res = requests.put(url, headers=headers, json=payload, timeout=60)
        # Clean up local gz file
        if os.path.exists(gz_path):
            os.remove(gz_path)

        if put_res.status_code in [200, 201]:
            print(f"🚀 [SUCCESS] Uploaded {repo_file_path} to GitHub {GITHUB_REPO}!")
            return True
        else:
            print(f"❌ GitHub API Error: {put_res.status_code} - {put_res.text}")
            return False
    except Exception as e:
        print(f"❌ GitHub Upload Exception: {e}")
        return False

# Periodic GitHub Sync Task (Every 6 hours and at Midnight UTC)
async def github_sync_daemon():
    while True:
        # Har 4 ghante baad sync check karega
        await asyncio.sleep(4 * 3600)
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Check all csv files in DATA_DIR
        for filename in os.listdir(DATA_DIR):
            if filename.endswith(".csv"):
                local_path = os.path.join(DATA_DIR, filename)
                date_part = filename.replace("btc_micro_", "").replace(".csv", "")
                
                # Agar purana din ka file hai (yaani pura complete ho chuka hai)
                # Ya current day ka periodic backup lena ho
                repo_path = f"data/{date_part[:7]}/{filename}.gz" # e.g. data/2026-10/btc_micro_2026-10-27.csv.gz
                commit_msg = f"Auto: Synced BTC Orderflow {date_part}"
                
                print(f"📦 Starting GitHub Sync for: {repo_path}...")
                success = upload_file_to_github(local_path, repo_path, commit_msg)
                
                # Agar purane din ka data upload ho gaya, to local file delete karke disk free kar dein
                if success and date_part != today_str:
                    try:
                        os.remove(local_path)
                        print(f"🧹 Cleaned up local file: {local_path}")
                    except Exception:
                        pass

# =====================================================================
# WEBSOCKET WORKERS (Binance Live Streams)
# =====================================================================
async def process_trades():
    global current_price, cumulative_cvd
    uri = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                print("📡 Binance Trades WebSocket Connected!")
                while True:
                    data = json.loads(await websocket.recv())
                    ts = time.time()
                    price = float(data['p'])
                    qty = float(data['q'])
                    is_seller = data['m']
                    
                    current_price = price
                    cumulative_cvd += -qty if is_seller else qty
                    trade_buffer.append({'ts': ts, 'price': price, 'qty': qty, 'is_seller': is_seller})
        except Exception as e:
            print(f"⚠️ Trades stream reconnecting: {e}")
            await asyncio.sleep(2)

async def process_depth():
    global buy_wall_price, buy_wall_size, sell_wall_price, sell_wall_size
    global local_liquidity_bids, local_liquidity_asks
    
    uri = "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                print("📡 Binance Depth WebSocket Connected!")
                while True:
                    data = json.loads(await websocket.recv())
                    
                    max_bid = max(data['bids'], key=lambda x: float(x[1]))
                    buy_wall_price, buy_wall_size = float(max_bid[0]), float(max_bid[1])
                    
                    max_ask = max(data['asks'], key=lambda x: float(x[1]))
                    sell_wall_price, sell_wall_size = float(max_ask[0]), float(max_ask[1])
                    
                    local_liquidity_bids = sum([float(b[1]) for b in data['bids']])
                    local_liquidity_asks = sum([float(a[1]) for a in data['asks']])
        except Exception as e:
            print(f"⚠️ Depth stream reconnecting: {e}")
            await asyncio.sleep(2)

# =====================================================================
# FEATURE ENGINE & LABELING
# =====================================================================
def get_cvd_over_window(seconds, current_ts):
    cutoff_ts = current_ts - seconds
    window_cvd = 0.0
    for i in range(len(trade_buffer) - 1, -1, -1):
        tr = trade_buffer[i]
        if tr['ts'] < cutoff_ts:
            break
        window_cvd += -tr['qty'] if tr['is_seller'] else tr['qty']
    return window_cvd

async def snapshot_engine():
    while True:
        await asyncio.sleep(SNAPSHOT_INTERVAL)
        now = time.time()
        if not trade_buffer:
            continue
            
        cvd_5s = get_cvd_over_window(5, now)
        cvd_15s = get_cvd_over_window(15, now)
        cvd_30s = get_cvd_over_window(30, now)
        
        total_liq = local_liquidity_bids + local_liquidity_asks
        imbalance = local_liquidity_bids / total_liq if total_liq > 0 else 0.5
        event_id = f"EVT_{uuid.uuid4().hex[:8].upper()}"
        
        snapshot = {
            'event_id': event_id,
            'ts': now,
            'price': current_price,
            'cvd_total': cumulative_cvd,
            'cvd_5s': cvd_5s,
            'cvd_15s': cvd_15s,
            'cvd_30s': cvd_30s,
            'buy_wall_price': buy_wall_price,
            'buy_wall_size': buy_wall_size,
            'sell_wall_price': sell_wall_price,
            'sell_wall_size': sell_wall_size,
            'imbalance': imbalance,
            'local_liquidity_bids': local_liquidity_bids,
            'local_liquidity_asks': local_liquidity_asks
        }
        event_buffer.append(snapshot)

async def labeling_engine():
    log_counter = 0
    while True:
        await asyncio.sleep(1)
        now = time.time()
        
        # Clean RAM buffer: drop trades older than 120s
        while len(trade_buffer) > 0 and (now - trade_buffer[0]['ts']) > 120.0:
            trade_buffer.popleft()
            
        # Label completed events older than 60s
        while len(event_buffer) > 0 and (now - event_buffer[0]['ts']) >= LABEL_HORIZON:
            evt = event_buffer.popleft()
            event_id, evt_ts, entry_price = evt['event_id'], evt['ts'], evt['price']
            
            prices = [tr['price'] for tr in trade_buffer if evt_ts <= tr['ts'] <= (evt_ts + 60.0)]
            if not prices:
                continue
            
            p_5s = prices[min(5, len(prices)-1)]
            p_30s = prices[min(30, len(prices)-1)]
            p_60s = prices[-1]
            
            ret_5s = ((p_5s - entry_price) / entry_price) * 100
            ret_30s = ((p_30s - entry_price) / entry_price) * 100
            ret_60s = ((p_60s - entry_price) / entry_price) * 100
            mfe_60s = ((max(prices) - entry_price) / entry_price) * 100
            mae_60s = ((min(prices) - entry_price) / entry_price) * 100
            
            row = [
                evt['event_id'], f"{evt['ts']:.5f}", f"{evt['price']:.2f}",
                f"{evt['cvd_total']:.5f}", f"{evt['cvd_5s']:.5f}", f"{evt['cvd_15s']:.5f}", f"{evt['cvd_30s']:.5f}",
                f"{evt['buy_wall_price']:.2f}", f"{evt['buy_wall_size']:.5f}",
                f"{evt['sell_wall_price']:.2f}", f"{evt['sell_wall_size']:.5f}",
                f"{evt['imbalance']:.5f}", f"{evt['local_liquidity_bids']:.5f}", f"{evt['local_liquidity_asks']:.5f}",
                f"{ret_5s:.5f}", f"{ret_30s:.5f}", f"{ret_60s:.5f}", f"{mfe_60s:.5f}", f"{mae_60s:.5f}"
            ]
            
            append_event_to_local_csv(row)
            log_counter += 1
            if log_counter % 30 == 0:
                print(f"✅ Recorded {event_id} | Price: ${entry_price:,.2f} | 60s Ret: {ret_60s:+.3f}% | Imb: {evt['imbalance']:.2f}")

# =====================================================================
# MAIN ENTRY POINT
# =====================================================================
async def main():
    print("==================================================")
    print("🚀 NEXUS GITHUB RECORDER V3 (Zero Database Limits)")
    print(f"📂 Target GitHub Repo: {GITHUB_REPO or 'Not Configured Yet'}")
    print("==================================================")
    
    keep_alive()
    get_today_filepath()
    
    await asyncio.gather(
        process_trades(),
        process_depth(),
        snapshot_engine(),
        labeling_engine(),
        github_sync_daemon()
    )

if __name__ == "__main__":
    asyncio.run(main())
