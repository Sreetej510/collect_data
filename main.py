import websocket # pip install websocket-client
import json
import pyotp
import time
import struct # For binary parsing
import threading # For the heartbeat AND batching
import os
import csv
# import gzip # Removed
import shutil # For compression
import subprocess # To run Git commands
from SmartApi import SmartConnect
from datetime import datetime, time as datetime_time, timedelta
import pytz # pip install pytz
import warnings
import config # Import your config file

# Suppress DeprecationWarning
warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- 1. Configuration ---
SAVE_INTERVAL_SECONDS = 30
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GIT_REPO_PATH = SCRIPT_DIR
BASE_DATA_FOLDER = os.path.join(SCRIPT_DIR, 'live_market_data')

# --- 2. Scheduling & Timezone Configuration ---
IST = pytz.timezone('Asia/Kolkata')
MARKET_OPEN = datetime_time(9, 15)
MARKET_CLOSE = datetime_time(15, 30)
HOLIDAYS_LIST = config.HOLIDAYS_LIST
print(f"Loaded {len(HOLIDAYS_LIST)} holidays from config.py")


# --- 3. Global Variables ---
csv_writers = {}
csv_files = {}
data_buffer = []
buffer_lock = threading.Lock()
keep_running_threads = True 

SNAP_QUOTE_HEADER = [
    'token', 'exchange_timestamp', 'ltp', 'last_traded_quantity', 'avg_trade_price',
    'volume', 'total_buy_qty', 'total_sell_qty',
    'open', 'high', 'low', 'close', 'open_interest',
    'upper_circuit', 'lower_circuit', '52wk_high', '52wk_low',
    'buy_price_1', 'buy_qty_1', 'buy_orders_1',
    'buy_price_2', 'buy_qty_2', 'buy_orders_2',
    'buy_price_3', 'buy_qty_3', 'buy_orders_3',
    'buy_price_4', 'buy_qty_4', 'buy_orders_4',
    'buy_price_5', 'buy_qty_5', 'buy_orders_5',
    'sell_price_1', 'sell_qty_1', 'sell_orders_1',
    'sell_price_2', 'sell_qty_2', 'sell_orders_2',
    'sell_price_3', 'sell_qty_3', 'sell_orders_3',
    'sell_price_4', 'sell_qty_4', 'sell_orders_4',
    'sell_price_5', 'sell_qty_5', 'sell_orders_5',
]

# --- 4. Login Function ---
def loginUser():
    print("Logging in to Angel One...")
    smartApi = SmartConnect(config.API_KEY)
    try:
        totp = pyotp.TOTP(config.TOTP_CODE).now()
    except Exception as e:
        print(f"Invalid TOTP Code: {e}")
        raise e
    data = smartApi.generateSession(config.CLIENT_CODE, config.PASSWORD, totp)
    if not data['status']:
        print(f"Login Failed: {data}")
        raise Exception("Login failed")
    print("Session generated. Getting tokens...")
    refreshToken = data['data']['refreshToken']
    token_data = smartApi.generateToken(refreshToken)
    if not token_data['status']:
        print(f"Failed to get tokens: {token_data}")
        raise Exception("Token generation failed")
    print("Login Successful.")
    return {
        "jwt_token": token_data['data']['jwtToken'].replace("Bearer ", ""),
        "feed_token": token_data['data']['feedToken'],
        "client_code": config.CLIENT_CODE,
        "api_key": config.API_KEY
    }

# --- 5. CSV Management ---
def initialize_csv_writers():
    global csv_writers, csv_files, data_buffer, buffer_lock
    current_date = datetime.now(IST).strftime('%Y-%m-%d')
    print(f"Initializing CSV files for date: {current_date}")
    
    data_buffer = []
    buffer_lock = threading.Lock()
    
    for stock in config.STOCKS_TO_TRACK: 
        symbol = stock['symbol']
        token = stock['token']
        symbol_folder = os.path.join(BASE_DATA_FOLDER, symbol)
        os.makedirs(symbol_folder, exist_ok=True)
        filename = f"{symbol_folder}/{current_date}_{symbol}_snap_quote.csv"
        file_exists = os.path.isfile(filename)
        
        f = open(filename, 'a', newline='')
        writer = csv.writer(f)
        
        if not file_exists:
            writer.writerow(SNAP_QUOTE_HEADER)
            
        csv_files[token] = f
        csv_writers[token] = writer
    print(f"CSV files successfully initialized in '{BASE_DATA_FOLDER}' folder.")

def close_csv_files():
    global csv_writers, csv_files
    print("Closing all CSV files...")
    for f in csv_files.values():
        f.close()
    csv_files = {}
    csv_writers = {}

# --- 6. Market Status Check ---
def is_market_open_day(date_to_check):
    """Helper function to check if a specific date is a trading day."""
    if date_to_check.weekday() >= 5: # 5=Sat, 6=Sun
        return False
    if date_to_check.strftime('%Y-%m-%d') in HOLIDAYS_LIST:
        return False
    return True

def is_market_open():
    """Checks if the Indian market is open right now."""
    now_ist = datetime.now(IST)
    if not is_market_open_day(now_ist):
        return False
    return MARKET_OPEN <= now_ist.time() <= MARKET_CLOSE

# --- 7. Parser ---
def parse_snap_quote_data(binary_data):
    if len(binary_data) != 379: return None
    try:
        token = struct.unpack_from('<25s', binary_data, 2)[0].decode('utf-8').strip('\x00')
        exchange_timestamp_ms = struct.unpack_from('<q', binary_data, 35)[0]
        exchange_timestamp = datetime.fromtimestamp(exchange_timestamp_ms / 1000, tz=IST)
        ltp = struct.unpack_from('<q', binary_data, 43)[0] / 100.0
        last_traded_quantity = struct.unpack_from('<q', binary_data, 51)[0]
        avg_trade_price = struct.unpack_from('<q', binary_data, 59)[0] / 100.0
        volume = struct.unpack_from('<q', binary_data, 67)[0]
        total_buy_qty = struct.unpack_from('<d', binary_data, 75)[0]
        total_sell_qty = struct.unpack_from('<d', binary_data, 83)[0]
        open_price = struct.unpack_from('<q', binary_data, 91)[0] / 100.0
        high_price = struct.unpack_from('<q', binary_data, 99)[0] / 100.0
        low_price = struct.unpack_from('<q', binary_data, 107)[0] / 100.0
        close_price = struct.unpack_from('<q', binary_data, 115)[0] / 100.0
        open_interest = struct.unpack_from('<q', binary_data, 131)[0]
        best_five_data = []
        for i in range(10):
            offset = 147 + (i * 20)
            flag = struct.unpack_from('<h', binary_data, offset)[0]
            qty = struct.unpack_from('<q', binary_data, offset + 2)[0]
            price = struct.unpack_from('<q', binary_data, offset + 10)[0] / 100.0
            orders = struct.unpack_from('<h', binary_data, offset + 18)[0]
            best_five_data.append({"flag": flag, "qty": qty, "price": price, "orders": orders})
        upper_circuit = struct.unpack_from('<q', binary_data, 347)[0] / 100.0
        lower_circuit = struct.unpack_from('<q', binary_data, 355)[0] / 100.0
        high_52wk = struct.unpack_from('<q', binary_data, 363)[0] / 100.0
        low_52wk = struct.unpack_from('<q', binary_data, 371)[0] / 100.0
        
        return {
            "token": token, "ltp": ltp, "last_traded_quantity": last_traded_quantity,
            "avg_trade_price": avg_trade_price, "volume": volume, "total_buy_qty": total_buy_qty,
            "total_sell_qty": total_sell_qty, "open": open_price, "high": high_price,
            "low": low_price, "close": close_price, "exchange_timestamp": exchange_timestamp,
            "open_interest": open_interest, "best_five_data": best_five_data, 
            "upper_circuit": upper_circuit, "lower_circuit": lower_circuit, 
            "52wk_high": high_52wk, "52wk_low": low_52wk
        }
    except Exception as e:
        print(f"Error parsing binary data: {e}")
        return None

# --- 8. Heartbeat Function ---
def send_heartbeat(ws):
    global keep_running_threads
    while keep_running_threads:
        try:
            time.sleep(20)
            if keep_running_threads:
                ws.send("ping")
        except:
            break

# --- 9. Batch Saving ---
def save_buffer_to_disk():
    global data_buffer
    data_to_save = []
    
    with buffer_lock:
        if not data_buffer:
            return
        data_to_save = data_buffer.copy()
        data_buffer.clear()
    
    print(f"[{datetime.now(IST)}] Writing batch of {len(data_to_save)} ticks to disk...")
    
    for parsed_data in data_to_save:
        token = parsed_data['token']
        writer = csv_writers.get(token)
        file_handle = csv_files.get(token)
        
        if writer and file_handle:
            try:
                row_data = [
                    parsed_data['token'], parsed_data['exchange_timestamp'], parsed_data['ltp'],
                    parsed_data['last_traded_quantity'], parsed_data['avg_trade_price'],
                    parsed_data['volume'], parsed_data['total_buy_qty'], parsed_data['total_sell_qty'],
                    parsed_data['open'], parsed_data['high'], parsed_data['low'], parsed_data['close'],
                    parsed_data['open_interest'],
                    parsed_data['upper_circuit'], parsed_data['lower_circuit'],
                    parsed_data['52wk_high'], parsed_data['52wk_low']
                ]
                for item in parsed_data['best_five_data']:
                    row_data.extend([item['price'], item['qty'], item['orders']])
                
                writer.writerow(row_data)
            except Exception as e:
                print(f"Error writing row to CSV: {e}")

    for f in csv_files.values():
        f.flush()

def save_data_loop():
    global keep_running_threads
    while keep_running_threads:
        time.sleep(SAVE_INTERVAL_SECONDS)
        save_buffer_to_disk()

# --- 10. Git Functions ---
# --- REMOVED compress_daily_files ---

def run_git_command(command_list):
    print(f"Running: {' '.join(command_list)}")
    try:
        result = subprocess.run(
            command_list,
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8'
        )
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Git command failed:")
        print(e.stderr)
        return False

def clean_local_data_folder():
    print(f"--- Deleting local data folder: {BASE_DATA_FOLDER} ---")
    try:
        shutil.rmtree(BASE_DATA_FOLDER)
        print("Successfully deleted local data folder.")
    except Exception as e:
        print(f"Error deleting data folder: {e}")

def push_to_github():
    print("--- Starting End-of-Day GitHub Push ---")
    commit_message = f"Data: Auto-commit for {datetime.now(IST).strftime('%Y-%m-%d')}"
    
    if not run_git_command(["git", "pull"]):
        print("Git pull failed. Aborting push.")
        return
        
    # Force-add the uncompressed .csv files
    if not run_git_command(["git", "add", "--force", "live_market_data/"]):
        print("Git add --force failed. Aborting push.")
        return
        
    if not run_git_command(["git", "commit", "-m", commit_message]):
        print("Git commit failed. (Maybe no changes to commit?)")
    
    if not run_git_command(["git", "push"]):
        print("Git push failed.")
    else:
        print("--- GitHub Push Successful ---")
        clean_local_data_folder()

# --- 11. Direct WebSocket Callbacks ---
def on_open(ws):
    global keep_running_threads
    keep_running_threads = True 
    
    print("--- WebSocket connection opened (Direct) ---")
    threading.Thread(target=send_heartbeat, args=(ws,), daemon=True).start()
    threading.Thread(target=save_data_loop, daemon=True).start()

    token_list = [stock['token'] for stock in config.STOCKS_TO_TRACK] 
    subscription_request = {
      "correlationID": f"my_feed_{int(time.time())}",
      "action": 1, 
      "params": {
        "mode": 3,
        "tokenList": [ {"exchangeType": 1, "tokens": token_list} ]
      }
    }
    print(f"Sending subscription: {json.dumps(subscription_request)}")
    ws.send(json.dumps(subscription_request))
    print("Subscription request sent.")

def on_message(ws, message):
    if isinstance(message, str) and message == "pong":
        return
    
    if not is_market_open():
        print(f"[{datetime.now(IST)}] Market is closed. Sending close signal...")
        ws.close()
        return

    parsed_data = parse_snap_quote_data(message)
    
    if parsed_data:
        with buffer_lock:
            data_buffer.append(parsed_data)

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    global keep_running_threads
    keep_running_threads = False 
    
    print("--- WebSocket connection closed ---")
    print("Saving any remaining data in buffer...")
    save_buffer_to_disk()
    close_csv_files()

# --- 12. Main Server Supervisor Loop ---

def get_seconds_until_market_open():
    """
    Calculates the exact number of seconds from now until the next
    market open (9:15 AM IST on the next weekday/non-holiday).
    """
    now_ist = datetime.now(IST)
    
    if is_market_open_day(now_ist) and now_ist.time() < MARKET_OPEN:
        next_open_datetime = now_ist.replace(hour=MARKET_OPEN.hour, 
                                             minute=MARKET_OPEN.minute, 
                                             second=0, microsecond=0)
    else:
        next_day = now_ist + timedelta(days=1)
        while not is_market_open_day(next_day):
            next_day += timedelta(days=1)
        
        next_open_datetime = next_day.replace(hour=MARKET_OPEN.hour, 
                                              minute=MARKET_OPEN.minute, 
                                              second=0, microsecond=0)
    
    delta_seconds = (next_open_datetime - now_ist).total_seconds()
    return delta_seconds

# --- MAIN SUPERVISOR LOOP ---
if __name__ == "__main__":
    
    end_of_day_tasks_done = False

    while True: # The 24/7 "supervisor" loop
        try:
            if is_market_open():
                print(f"[{datetime.now(IST)}] Market is OPEN. Starting collector.")
                end_of_day_tasks_done = False 
                
                auth_data = loginUser()
                initialize_csv_writers() 
                
                ws_url = "wss://smartapisocket.angelone.in/smart-stream"
                headers = {
                    "Authorization": auth_data['jwt_token'],
                    "x-api-key": auth_data['api_key'],
                    "x-client-code": auth_data['client_code'],
                    "x-feed-token": auth_data['feed_token']
                }
                
                ws = websocket.WebSocketApp(ws_url,
                                          header=headers,
                                          on_open=on_open,
                                          on_message=on_message,
                                          on_error=on_error,
                                          on_close=on_close)
                
                ws.run_forever()
                print("Collector has stopped.")
                
            else:
                now_ist = datetime.now(IST)
                
                is_after_market_close_today = (
                    now_ist.time() > MARKET_CLOSE and
                    is_market_open_day(now_ist) and 
                    not end_of_day_tasks_done
                )
                
                if is_after_market_close_today:
                    print(f"[{now_ist}] Market just closed. Running end-of-day tasks...")
                    # --- COMPRESSION CALL REMOVED ---
                    push_to_github()
                    end_of_day_tasks_done = True
                    print("End-of-day tasks complete.")

                seconds_to_sleep = get_seconds_until_market_open()
                seconds_to_sleep += 3 # Add a 10-second buffer
                
                sleep_hours = seconds_to_sleep / 3600
                print(f"[{now_ist}] Market is CLOSED. Sleeping for {sleep_hours:.2f} hours until next market open.")
                
                time.sleep(seconds_to_sleep)
        
        except KeyboardInterrupt:
            print("\nStopping server (Ctrl+C)...")
            try:
                keep_running_threads = False
                ws.close()
            except:
                pass
            break
        except Exception as e:
            print(f"An unexpected error occurred in the main loop: {e}")
            print("Restarting in 60 seconds...")
            time.sleep(60)
    
    print("Script has terminated.")