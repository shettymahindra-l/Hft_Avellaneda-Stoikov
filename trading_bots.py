import os
import atexit
import time
import datetime
import math
import json
from decouple import config
from binance.client import Client

# --- CONFIGURATION ---
API_KEY = config("API_KEY")
API_SECRET = config("API_SECRET")
client = Client(API_KEY, API_SECRET, testnet=True)

SYMBOL = "ETHUSDT"
UNIT_PER_TRADE = 1
MAX_LOSS = 2
SLEEP_TIME = 5
STATUS_FILE = "bot_logic.status"

# --- NEW: FUNCTIONS TO CONNECT TO FRONTEND ---

def log(msg):
    """A simple logger that prints to console."""
    print(msg)
    # This function can be expanded to write to a log file if needed
    # For now, we keep it simple to match your script's behavior.

def update_pnl_file(pnl_value):
    """Writes the latest PnL to a file for the server to read."""
    with open("pnl_state.json", "w") as f:
        json.dump({"cumulative_pnl": pnl_value}, f)

def trade_log(sym, side, price, amount):
    """Writes a trade to a CSV file for the server."""
    if not os.path.isdir("trades"):
        os.mkdir("trades")
    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    header = "time,bot_name,sym,side,amount,price\n"
    file_path = f"trades/{today}.csv"
    if not os.path.isfile(file_path):
        with open(file_path, "w") as f:
            f.write(header)
    with open(file_path, "a+") as f:
        f.write(f"{now.isoformat()},Logic Bot,{sym},{side},{amount},{price}\n")

def cleanup():
    """Removes status file on exit."""
    if os.path.exists(STATUS_FILE):
        os.remove(STATUS_FILE)
    log("Bot stopped manually")

# --- YOUR TRADING LOGIC (UNCHANGED) ---

def get_live_price(symbol):
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker['price'])

def compute_avellaneda_prices(mid, k=1.5, sigma=0.00025):
    spread = k * sigma
    bid_price = math.floor(mid - spread)
    ask_price = math.ceil(mid + spread)
    return bid_price, ask_price

def main():
    atexit.register(cleanup)
    with open(STATUS_FILE, "w") as f:
        f.write("running")

    cumulative_pnl = 0
    position_open = False
    buy_price = None
    current_unit = UNIT_PER_TRADE
    last_price_print = time.time()
    update_pnl_file(0)  # Initialize PnL file at the start

    while True:
        try:
            current_price = get_live_price(SYMBOL)
            mid_price = current_price
            current_int = int(current_price)

            if time.time() - last_price_print >= 10:
                log(f"[INFO] Current Price: {current_price}")
                last_price_print = time.time()

            bid_price, ask_price = compute_avellaneda_prices(mid_price)

            if not position_open:
                buy_price = bid_price
                log(f"[BUY ] {current_unit} unit(s) @ {buy_price} | Current Price: {current_price}")
                trade_log(SYMBOL, 'buy', buy_price, current_unit)
                position_open = True
            else:
                if current_int >= buy_price + 1:
                    sell_price = ask_price
                    pnl = (sell_price - buy_price) * current_unit
                    cumulative_pnl += pnl
                    log(f"[SELL] {current_unit} unit(s) @ {sell_price} | Current Price: {current_price} | PnL={pnl} | Cumulative PnL={cumulative_pnl}")
                    trade_log(SYMBOL, 'sell', sell_price, current_unit)
                    update_pnl_file(cumulative_pnl)  # NEW: Update PnL file
                    position_open = False
                    current_unit = UNIT_PER_TRADE
                elif current_int <= buy_price - MAX_LOSS:
                    sell_price = bid_price - 1
                    pnl = (sell_price - buy_price) * current_unit
                    cumulative_pnl += pnl
                    log(f"[SELL] {current_unit} unit(s) @ {sell_price} | Current Price: {current_price} | PnL={pnl} | Cumulative PnL={cumulative_pnl}")
                    trade_log(SYMBOL, 'sell', sell_price, current_unit)
                    update_pnl_file(cumulative_pnl)  # NEW: Update PnL file
                    position_open = False
                    current_unit *= 2

            time.sleep(SLEEP_TIME)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log(f"Error: {e}")
            time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    main()

