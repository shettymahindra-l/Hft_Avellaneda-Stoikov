import os
import csv
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple

# config
TRADES_FOLDER = "../trades"
OUT_DIR = "../bt_out"
OUTPUT_CSV = os.path.join(OUT_DIR, "backtest_results.csv")
OUTPUT_JSON = os.path.join(OUT_DIR, "backtest_results.json")

# expected headers (used as fallback)
HEADERS = ["time", "bot_name", "sym", "side", "amount", "price"]


def _parse_time(s: str):
    """Try several common timestamp formats, return datetime or None."""
    if not s or str(s).strip() == "":
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%H:%M:%S"):
        try:
            return datetime.fromisoformat(s) if "T" in s else datetime.strptime(s, fmt)
        except Exception:
            continue
    # try pandas fallback (handles many formats)
    try:
        return pd.to_datetime(s).to_pydatetime()
    except Exception:
        return None


def read_all_trades(folder: str) -> List[Dict]:
    """Read all CSV files in folder and return normalized list of trade dicts."""
    trades = []
    if not os.path.isdir(folder):
        print(f"Trades folder not found: {folder}")
        return trades

    files = sorted(f for f in os.listdir(folder) if f.endswith(".csv"))
    for f in files:
        path = os.path.join(folder, f)
        try:
            with open(path, newline="") as fh:
                # try normal DictReader first
                reader = csv.DictReader(fh)
                # if header is missing, re-open with fallback headers
                if reader.fieldnames is None or set(reader.fieldnames) <= set([None, ""]):
                    fh.seek(0)
                    reader = csv.DictReader(fh, fieldnames=HEADERS)
                for row in reader:
                    # normalize keys/values
                    r = {k.strip() if k else k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
                    trades.append(r)
        except Exception as e:
            print(f"Failed reading {path}: {e}")
    return trades


def normalize_and_sort(trades: List[Dict]) -> List[Dict]:
    """Ensure required fields exist and sort by parsed time (if available)."""
    normalized = []
    for row in trades:
        try:
            time_raw = row.get("time") or row.get("timestamp") or ""
            t = _parse_time(time_raw)
            bot = (row.get("bot_name") or row.get("bot") or "unknown").strip()
            sym = (row.get("sym") or row.get("symbol") or "").strip()
            side = (row.get("side") or "").strip().lower()
            amount = row.get("amount") or row.get("qty") or row.get("quantity") or row.get("size") or 0
            price = row.get("price") or row.get("p") or 0
            # attempt convert
            try:
                amount = float(amount)
            except Exception:
                amount = 0.0
            try:
                price = float(price)
            except Exception:
                price = 0.0
            normalized.append({
                "time": t,
                "bot_name": bot,
                "sym": sym,
                "side": side,
                "amount": amount,
                "price": price,
                "_raw_time": time_raw
            })
        except Exception:
            # skip row but keep a note
            continue

    # sort: rows with parsed time first (ascending), then those without time
    normalized.sort(key=lambda r: (r["time"] is None, r["time"] or datetime.min))
    return normalized


def compute_backtest(trades: List[Dict]) -> Tuple[List[float], List[float], float]:
    """FIFO per bot. Returns (equity_list, drawdown_list, final_pnl)."""
    pnl = 0.0
    equity = []
    open_positions: Dict[str, List[Tuple[float, float]]] = {}  # bot -> list of (entry_price, qty)

    for row in trades:
        bot = row["bot_name"]
        side = row["side"]
        qty = float(row["amount"] or 0.0)
        price = float(row["price"] or 0.0)

        if bot not in open_positions:
            open_positions[bot] = []

        if side == "buy" and qty > 0:
            open_positions[bot].append((price, qty))
        elif side == "sell" and qty > 0:
            qty_to_sell = qty
            # FIFO match
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
            # if no buys to match, we simply ignore unmatched sell (you could also record as short)
        # append current pnl to equity curve
        equity.append(float(pnl))

    if not equity:
        equity = [0.0]

    # compute drawdown (series - cummax)
    ser = pd.Series(equity)
    drawdown = (ser - ser.cummax()).astype(float).tolist()
    equity = [float(x) for x in ser.tolist()]

    return equity, drawdown, float(pnl)


def compute_sharpe(equity: List[float]) -> float:
    """Simple daily-equivalent Sharpe using returns of the equity series.
       If series too short or std=0 returns 0."""
    if len(equity) < 2:
        return 0.0
    returns = pd.Series(equity).pct_change().dropna()
    if returns.empty or returns.std() == 0:
        return 0.0
    # annualize assuming 252 trading periods (approx)
    sharpe = (returns.mean() / returns.std()) * (252 ** 0.5)
    return float(sharpe)


def save_outputs(equity: List[float], drawdown: List[float], pnl: float, total_trades: int):
    os.makedirs(OUT_DIR, exist_ok=True)
    # save CSV
    df = pd.DataFrame({"equity": equity, "drawdown": drawdown})
    df.to_csv(OUTPUT_CSV, index=False)

    # save JSON metrics (plain Python types)
    metrics = {
        "equity": equity,
        "drawdown": drawdown,
        "total_trades": int(total_trades),
        "final_pnl": float(pnl),
        "max_drawdown": float(min(drawdown)) if drawdown else 0.0,
        "sharpe_ratio": round(float(compute_sharpe(equity)), 4)
    }
    with open(OUTPUT_JSON, "w") as f:
        json.dump(metrics, f, indent=2)


def backtest_trade_logs():
    print("Reading trades...")
    raw_trades = read_all_trades(TRADES_FOLDER)
    trades = normalize_and_sort(raw_trades)
    if not trades:
        print("No trades found.")
        # write empty outputs so frontend doesn't break
        save_outputs([0.0], [0.0], 0.0, 0)
        return

    print(f"Processing {len(trades)} trade rows...")
    equity, drawdown, pnl = compute_backtest(trades)
    total_trades = sum(1 for t in trades if t["side"] in ("buy", "sell"))
    save_outputs(equity, drawdown, pnl, total_trades)

    print("\n✅ Backtesting Complete")
    print(f"📊 Total Estimated PnL: {pnl:.2f} USDT")
    print(f"📉 Max drawdown: {min(drawdown):.2f} USDT")
    print(f"📁 Results saved to: {OUTPUT_CSV} and {OUTPUT_JSON}")


if __name__ == "__main__":
    backtest_trade_logs()
