import websocket # pip install websocket-client
import json
import pyotp
import time
import struct # For binary parsing
import threading # For the heartbeat AND batching
import os
import csv
import logging # For logging
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

# --- 3. Logging Setup ---
def setup_logging():
    """
    Sets up logging to write all logs to a daily file and errors to console.
    """
    log_dir = os.path.join(SCRIPT_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    current_date_str = datetime.now(IST).strftime('%Y-%m-%d')
    log_filename = os.path.join(log_dir, f"log_{current_date_str}.txt")
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
        
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # File Handler: Log EVERYTHING (INFO and above)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Console Handler: Log ERRORS only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    print(f"Logging initialized. Writing to {log_filename}")
    return logger

logging.info(f"Loaded {len(HOLIDAYS_LIST)} holidays from config.py")


# --- 4. Global Variables ---
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

# --- 5. Login Function ---
def loginUser():
    logging.info("Logging in to Angel One...")
    smartApi = SmartConnect(config.API_KEY)
    try:
        totp = pyotp.TOTP(config.TOTP_CODE).now()
    except Exception as e:
        logging.error(f"Invalid TOTP Code: {e}")
        raise e
    data = smartApi.generateSession(config.CLIENT_CODE, config.PASSWORD, totp)
    if not data['status']:
        logging.error(f"Login Failed: {data}")
        raise Exception("Login failed")
    logging.info("Session generated. Getting tokens...")
    refreshToken = data['data']['refreshToken']
    token_data = smartApi.generateToken(refreshToken)
    if not token_data['status']:
        logging.error(f"Failed to get tokens: {token_data}")
        raise Exception("Token generation failed")
    logging.info("Login Successful.")
    return {
        "jwt_token": token_data['data']['jwtToken'].replace("Bearer ", ""),
        "feed_token": token_data['data']['feedToken'],
        "client_code": config.CLIENT_CODE,
        "api_key": config.API_KEY
    }

# --- 6. CSV Management (Modified for Raw Storage) ---
def initialize_csv_writers():
    """
    Creates the raw data directory for the current date.
    No longer opens individual stock files upfront.
    """
    global csv_writers, csv_files
    # Clear globals just in case
    csv_writers = {}
    csv_files = {}
    
    current_date = datetime.now(IST).strftime('%Y-%m-%d')
    raw_dir = os.path.join(BASE_DATA_FOLDER, 'raw', current_date)
    os.makedirs(raw_dir, exist_ok=True)
    logging.info(f"Initialized raw data directory: {raw_dir}")

def close_csv_files():
    """
    No longer needed to close per-stock files, but kept for compatibility 
    if we have any lingering handles.
    """
    pass

# --- 7. Market Status Check ---
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

# --- 8. Parser ---
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
        logging.error(f"Error parsing binary data: {e}")
        return None

# --- 9. Heartbeat Function ---
def send_heartbeat(ws):
    global keep_running_threads
    while keep_running_threads:
        try:
            time.sleep(10)
            if keep_running_threads:
                ws.send("ping")
        except:
            break

# --- 10. Batch Saving (to Raw Minute Files) ---
def save_buffer_to_disk():
    global data_buffer
    data_to_save = []
    
    with buffer_lock:
        if not data_buffer:
            return
        data_to_save = data_buffer.copy()
        data_buffer.clear()
    
    start_time = time.time()
    
    # 1. Group data by minute (YYYY-MM-DD_HH-MM)
    grouped_data = {} # { "2023-10-27_09-15": [row_list, row_list...] }
    
    for parsed_data in data_to_save:
        # Construct the row data once
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
        
        ts = parsed_data['exchange_timestamp']
        date_str = ts.strftime('%Y-%m-%d')
        time_str = ts.strftime('%H-%M')
        
        key = (date_str, time_str)
        if key not in grouped_data:
            grouped_data[key] = []
        grouped_data[key].append(row_data)

    logging.info(f"Writing {len(data_to_save)} records to {len(grouped_data)} raw files...")

    # 2. Write to files
    for (date_str, time_str), rows in grouped_data.items():
        raw_dir = os.path.join(BASE_DATA_FOLDER, 'raw', date_str)
        os.makedirs(raw_dir, exist_ok=True)
        
        filename = os.path.join(raw_dir, f"{time_str}.csv")
        file_exists = os.path.isfile(filename)
        
        try:
            with open(filename, 'a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(SNAP_QUOTE_HEADER)
                writer.writerows(rows)
        except Exception as e:
            logging.error(f"Error writing to raw file {filename}: {e}")

    final_end_time = time.time()
    logging.info(f"Write complete. Took {final_end_time - start_time:.2f}s")

def save_data_loop():
    global keep_running_threads
    while keep_running_threads:
        time.sleep(SAVE_INTERVAL_SECONDS)
        save_buffer_to_disk()

# --- 11. EOD Processing (Split Raw -> Individual) ---
def process_raw_data_to_individual_files():
    logging.info("--- Starting Post-Processing: Splitting Raw Data ---")
    current_date_str = datetime.now(IST).strftime('%Y-%m-%d')
    raw_dir = os.path.join(BASE_DATA_FOLDER, 'raw', current_date_str)
    
    if not os.path.exists(raw_dir):
        logging.info(f"No raw data directory found for {current_date_str}. Skipping processing.")
        return

    token_map = {stock['token']: stock['symbol'] for stock in config.STOCKS_TO_TRACK}
    file_handles = {}
    writers = {}
    
    def get_writer(token):
        if token in writers:
            return writers[token]
        
        symbol = token_map.get(token)
        if not symbol:
            return None # Unknown token
            
        symbol_folder = os.path.join(BASE_DATA_FOLDER, symbol)
        os.makedirs(symbol_folder, exist_ok=True)
        out_filename = f"{symbol_folder}/{current_date_str}_{symbol}_snap_quote.csv"
        
        exists = os.path.isfile(out_filename)
        f = open(out_filename, 'a', newline='')
        w = csv.writer(f)
        if not exists:
            w.writerow(SNAP_QUOTE_HEADER)
            
        file_handles[token] = f
        writers[token] = w
        return w

    raw_files = sorted([f for f in os.listdir(raw_dir) if f.endswith('.csv')])
    logging.info(f"Found {len(raw_files)} raw minute-files to process.")
    
    total_records = 0
    
    for rf in raw_files:
        path = os.path.join(raw_dir, rf)
        logging.info(f"Processing {rf}...")
        try:
            with open(path, 'r') as f_in:
                reader = csv.reader(f_in)
                header = next(reader, None) # skip header
                
                for row in reader:
                    if not row: continue
                    token = row[0]
                    w = get_writer(token)
                    if w:
                        w.writerow(row)
                        total_records += 1
        except Exception as e:
            logging.error(f"Failed to process {rf}: {e}")

    logging.info("Closing all destination files...")
    for f in file_handles.values():
        f.flush()
        f.close()
        
    logging.info(f"--- Post-Processing Complete. Processed {total_records} records. ---")


# --- 12. Git Functions ---

def run_git_command(command_list):
    """Helper function to run a Git command."""
    logging.info(f"Running: {' '.join(command_list)}")
    try:
        result = subprocess.run(
            command_list,
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8'
        )
        logging.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Git command failed:")
        logging.error(e.stderr)
        return False

def get_todays_file_paths():
    """
    Gets the list of relative file paths that were created today.
    """
    today_date_str = datetime.now(IST).strftime('%Y-%m-%d')
    file_paths = []
    
    for stock in config.STOCKS_TO_TRACK:
        symbol = stock['symbol']
        symbol_folder = os.path.join(BASE_DATA_FOLDER, symbol)
        csv_filename = f"{symbol_folder}/{today_date_str}_{symbol}_snap_quote.csv"

        if os.path.exists(csv_filename):
            relative_path = os.path.relpath(csv_filename, GIT_REPO_PATH)
            file_paths.append(relative_path.replace(os.path.sep, '/'))
            
    return file_paths

def clean_local_data_folder():
    """
    Deletes the entire data folder from the local server after a successful push.
    """
    logging.info(f"--- Deleting local data folder: {BASE_DATA_FOLDER} ---")
    try:
        shutil.rmtree(BASE_DATA_FOLDER)
        logging.info("Successfully deleted local data folder.")
    except Exception as e:
        logging.error(f"Error deleting data folder: {e}")

def push_to_github():
    logging.info("--- Starting End-of-Day GitHub Push ---")
    
    # Process raw data first!
    process_raw_data_to_individual_files()
    
    commit_message = f"Data: Auto-commit for {datetime.now(IST).strftime('%Y-%m-%d')}"
    
    if not run_git_command(["git", "pull"]):
        logging.error("Git pull failed. Aborting push.")
        return
        
    todays_files = get_todays_file_paths()
    if not todays_files:
        logging.info("No new data files found to commit.")
        return

    add_command = ["git", "add", "--force"]
    add_command.extend(todays_files)
    if not run_git_command(add_command):
        logging.error("Git add --force failed. Aborting push.")
        return
        
    if not run_git_command(["git", "commit", "-m", commit_message]):
        logging.info("Git commit failed. (Maybe no changes to commit?)")
    
    if not run_git_command(["git", "push", "origin", "HEAD:main"]):
        logging.error("Git push failed.")
    else:
        logging.info("--- GitHub Push Successful ---")
        clean_local_data_folder()

# --- 13. Direct WebSocket Callbacks ---
def on_open(ws):
    global keep_running_threads
    keep_running_threads = True 
    
    logging.info("--- WebSocket connection opened (Direct) ---")
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
    logging.info(f"Sending subscription: {json.dumps(subscription_request)}")
    ws.send(json.dumps(subscription_request))
    logging.info("Subscription request sent.")

def on_message(ws, message):
    if isinstance(message, str) and message == "pong":
        return
    
    if not is_market_open():
        logging.info("Market is closed. Sending close signal...")
        ws.close()
        return

    parsed_data = parse_snap_quote_data(message)
    
    if parsed_data:
        with buffer_lock:
            data_buffer.append(parsed_data)

def on_error(ws, error):
    logging.error(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    global keep_running_threads
    keep_running_threads = False 
    
    logging.info("--- WebSocket connection closed ---")
    logging.info("Saving any remaining data in buffer...")
    save_buffer_to_disk()
    close_csv_files()

# --- 14. Main Server Supervisor Loop ---

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
    
    setup_logging()
    
    end_of_day_tasks_done = False

    while True: # The 24/7 "supervisor" loop
        try:
            if is_market_open():
                # Re-setup logging daily to ensure day rollover if script ran overnight
                setup_logging()
                
                logging.info(f"Market is OPEN. Starting collector.")
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
                logging.info("Collector has stopped.")
                
            else:
                now_ist = datetime.now(IST)
                
                is_after_market_close_today = (
                    now_ist.time() > MARKET_CLOSE and
                    is_market_open_day(now_ist) and 
                    not end_of_day_tasks_done
                )
                
                if is_after_market_close_today:
                    logging.info("Market just closed. Running end-of-day tasks...")
                    push_to_github()
                    end_of_day_tasks_done = True
                    logging.info("End-of-day tasks complete.")

                seconds_to_sleep = get_seconds_until_market_open()
                seconds_to_sleep += 3 # Add a 10-second buffer
                
                sleep_hours = seconds_to_sleep / 3600
                logging.info(f"Market is CLOSED. Sleeping for {sleep_hours:.2f} hours until next market open.")
                
                time.sleep(seconds_to_sleep)
        
        except KeyboardInterrupt:
            logging.info("\nStopping server (Ctrl+C)...")
            try:
                keep_running_threads = False
                ws.close()
            except:
                pass
            break
        except Exception as e:
            logging.error(f"An unexpected error occurred in the main loop: {e}")
            logging.info("Restarting in 60 seconds...")
            time.sleep(60)
    
    logging.info("Script has terminated.")
