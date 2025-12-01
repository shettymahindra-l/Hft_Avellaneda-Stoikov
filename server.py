import csv
import glob
import os
import asyncio
import json
from contextlib import asynccontextmanager
from typing import List, Dict

import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi import UploadFile, File

from binance.client import AsyncClient
from binance.streams import BinanceSocketManager


# ---------------------------
# Helpers
# ---------------------------


def get_latest_file(path_pattern: str):
    files = sorted(glob.glob(path_pattern))
    return files[-1] if files else None

def json_safe_number(x: float) -> float:
    """Replace NaN / +/-inf with 0.0 to keep JSON valid."""
    if x is None:
        return 0.0
    try:
        if np.isnan(x) or np.isinf(x):
            return 0.0
    except Exception:
        return 0.0
    return float(x)

def json_safe_list(arr):
    return [json_safe_number(v) for v in arr]


# ---------------------------
# Lifespan (startup/shutdown)
# ---------------------------

binance_client = None
price_stream_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global binance_client, price_stream_task
    print("🚀 Starting Binance connection...")

    # Create client
    binance_client = await AsyncClient.create()

    # Start price stream task
    price_stream_task = asyncio.create_task(binance_price_stream())

    # Run app
    yield

    # Teardown
    print("🛑 Shutting down Binance stream...")
    if price_stream_task:
        price_stream_task.cancel()
        try:
            await price_stream_task
        except asyncio.CancelledError:
            pass

    if binance_client:
        await binance_client.close_connection()
    print("✅ Binance client closed.")


# Create app (must be before using @app.get)
app = FastAPI(lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------
# WebSocket manager
# ---------------------------

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        # Be resilient to clients disconnecting
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()


# ---------------------------
# Binance live price stream
# ---------------------------

async def binance_price_stream():
    """
    Use manual enter/exit for the socket to avoid recent __aexit__ fail_connection errors.
    """
    global binance_client
    bsm = BinanceSocketManager(binance_client)

    # Manually manage the context
    socket = bsm.trade_socket("ETHUSDT")
    stream = await socket.__aenter__()
    try:
        while True:
            try:
                msg = await stream.recv()
                if msg and msg.get("e") == "trade":
                    price_data = {"time": msg["E"], "price": msg["p"]}
                    await manager.broadcast(json.dumps(price_data))
            except asyncio.CancelledError:
                # Task cancelled during shutdown
                break
            except Exception:
                # Backoff slightly on transient errors
                await asyncio.sleep(1)
    finally:
        try:
            await socket.__aexit__(None, None, None)
        except Exception:
            # Ignore any weird __aexit__ errors from the lib
            pass


@app.websocket("/ws/price")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ---------------------------
# API endpoints
# ---------------------------

@app.get("/api/backtest")
async def get_backtest_results():
    try:
        import pandas as pd
        df = pd.read_csv("./bt_out/backtest_results.csv")

        equity = df["equity"].tolist()
        drawdown = df["drawdown"].tolist()

        # daily returns
        returns = df["returns"].tolist() if "returns" in df else []

        # trade PnL
        trade_pnl = df["trade_pnl"].tolist() if "trade_pnl" in df else []

        # timestamps (for heatmap)
        timestamps = df["time"].tolist() if "time" in df else []

        # rolling volatility (20)
        if "returns" in df:
            df["rolling_vol"] = df["returns"].rolling(20).std().fillna(0)
            rolling_vol = df["rolling_vol"].tolist()
        else:
            rolling_vol = []

        sharpe = 0
        if df["equity"].std() != 0:
            sharpe = float(df["equity"].mean() / df["equity"].std())

        return {
            "equity": equity,
            "drawdown": drawdown,
            "returns": returns,
            "trade_pnl": trade_pnl,
            "timestamps": timestamps,
            "rolling_vol": rolling_vol,
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown": round(min(drawdown), 2),
            "total_trades": len(trade_pnl),
        }

    except Exception as e:
        print("BACKTEST ERROR:", e)
        return {"error": "No backtest data available"}
@app.get("/api/backtest-local")
async def get_local_backtest():
    import os, json, pandas as pd
    try:
        # Ensure file exists
        csv_file = "./bt_out/backtest_results.csv"
        if not os.path.exists(csv_file):
            return {"error": "Run backtest_from_trade_logs.py first to generate results"}

        df = pd.read_csv(csv_file)
        equity = df["equity"].tolist() if "equity" in df else []
        total_pnl = float(equity[-1]) if equity else 0.0

        return {
            "total_pnl": round(total_pnl, 2),
            "equity": equity
        }

    except Exception as e:
        return {"error": str(e)}

@app.get("/api/logs")
def get_logs():
    log_dir = "logs"
    latest_file = get_latest_file(os.path.join(log_dir, "*.txt"))
    if not latest_file:
        return {"logs": ["No logs yet."]}

    try:
        with open(latest_file, "r") as f:
            return {"logs": [line.strip() for line in f.readlines()[-20:]]}
    except Exception as e:
        return {"logs": [f"Error reading logs: {e}"]}


@app.get("/api/pnl")
def get_pnl_data():
    """Reads the latest cumulative PnL written by the trading bot."""
    try:
        if os.path.exists("pnl_state.json"):
            with open("pnl_state.json", "r") as f:
                data = json.load(f)
                return {"total_pnl": round(float(data.get("cumulative_pnl", 0)), 2)}

        return {"total_pnl": 0.0}

    except Exception as e:
        print(f"Error reading PnL file: {e}")
        return {"total_pnl": 0.0}


@app.post("/api/backtest-upload")
async def backtest_upload(file: UploadFile = File(...)):
    import pandas as pd
    import io
    try:
        # Read uploaded CSV into DataFrame
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        # Normalize columns for FIFO backtest
        trades = []
        for _, row in df.iterrows():
            trades.append({
                "bot_name": row.get("bot_name", "unknown"),
                "side": row.get("side", "").lower(),
                "amount": float(row.get("amount", 0)),
                "price": float(row.get("price", 0))
            })

        # Compute equity and PnL using same logic as backtest_from_trade_logs
        pnl = 0.0
        equity = []
        open_positions = {}

        for t in trades:
            bot = t["bot_name"]
            side = t["side"]
            qty = t["amount"]
            price = t["price"]

            if bot not in open_positions:
                open_positions[bot] = []

            if side == "buy" and qty > 0:
                open_positions[bot].append((price, qty))
            elif side == "sell" and qty > 0:
                qty_to_sell = qty
                while qty_to_sell > 0 and open_positions[bot]:
                    entry_price, entry_qty = open_positions[bot][0]
                    if entry_qty <= qty_to_sell + 1e-12:
                        pnl += (price - entry_price) * entry_qty
                        qty_to_sell -= entry_qty
                        open_positions[bot].pop(0)
                    else:
                        pnl += (price - entry_price) * qty_to_sell
                        open_positions[bot][0] = (entry_price, entry_qty - qty_to_sell)
                        qty_to_sell = 0.0
            equity.append(float(pnl))

        total_pnl = equity[-1] if equity else 0.0

        return {"total_pnl": round(total_pnl, 2), "equity": equity}

    except Exception as e:
        return {"error": str(e)}

@app.get("/api/trades", response_model=List[Dict])
def get_trades():
    trade_dir = "trades"
    latest_file = get_latest_file(os.path.join(trade_dir, "*.csv"))
    if not latest_file:
        return []

    try:
        with open(latest_file, "r", newline="") as f:
            reader = csv.DictReader(f)
            trades = list(reader)
        trades.sort(key=lambda x: x.get("time", ""), reverse=True)
        return trades[:10]
    except Exception:
        return []


@app.get("/api/status")
def get_bot_status():
    return {
        "Avellaneda Bot": os.path.exists("ava_stoi.status"),
        "DCA Bot": os.path.exists("dca.status"),
    }
