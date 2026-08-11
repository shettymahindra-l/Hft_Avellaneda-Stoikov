# High-Frequency Trading Framework

A modular **High-Frequency Trading (HFT) market-making framework** based on the **Avellaneda–Stoikov model**. The system is designed for real-time market data ingestion, dynamic bid–ask quote generation, order execution, risk management, and historical backtesting.

## Overview

This project implements a modular architecture for algorithmic market making using real-time market data from **Binance Testnet**.

The framework combines stochastic modeling with dynamic execution logic to determine optimal bid and ask prices while considering:

* Market volatility
* Market depth
* Inventory position
* Risk aversion
* Order execution
* Profit and loss
* Drawdown and inventory exposure

The modular design allows additional components such as volatility forecasting and machine-learning-based trading strategies to be integrated with minimal changes to the existing system.

## Architecture

```text
                 ┌──────────────────────┐
                 │   Binance Testnet    │
                 └──────────┬───────────┘
                            │
                     Market Data
                            │
                            ▼
                 ┌──────────────────────┐
                 │  Market Data Streamer│
                 │   WebSocket Feed     │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │    Strategy Engine   │
                 │ Avellaneda–Stoikov   │
                 └──────────┬───────────┘
                            │
                     Bid / Ask Quotes
                            │
                            ▼
                 ┌──────────────────────┐
                 │ Order Execution      │
                 │      Engine          │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │ PnL & Risk Manager   │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │ Backtesting Module   │
                 └──────────────────────┘
```

## Modules

### 1. Market Data Streamer

Connects to the Binance Testnet through WebSocket and continuously receives:

* Trade ticks
* Order book information
* Market prices
* Market depth

The incoming data is normalized before being passed to the strategy engine.

### 2. Strategy Engine

The strategy engine implements the **Avellaneda–Stoikov market-making framework**.

The quote calculation considers:

* Mid-price
* Volatility (`σ`)
* Inventory (`q`)
* Risk-aversion coefficient (`γ`)
* Market-depth parameter (`k`)
* Remaining trading time

The model generates dynamic bid and ask quotes according to current market conditions.

### 3. Order Execution Engine

Responsible for real-time order management.

Functions include:

* Limit order submission
* Market order submission
* Order cancellation
* Order modification
* Local order-state management
* Consistency checking against exchange order-book snapshots

### 4. PnL & Risk Manager

The risk-management module continuously monitors:

* Realized PnL
* Unrealized PnL
* Inventory exposure
* Loss thresholds
* Trading positions

A corrective doubling mechanism is used when the defined loss condition is triggered.

### 5. Backtesting Module

The backtesting module replays historical OHLCV data obtained through the Binance REST API.

It is used to evaluate the strategy under different market conditions and analyze:

* Cumulative PnL
* Maximum drawdown
* Inventory variance
* Sharpe ratio

Sensitivity analysis is performed by varying `γ` and `k` to study the effects of risk aversion and market depth on profitability.

## Mathematical Model

The mid-price is modeled using a stochastic diffusion process:

```text
dSₜ = σdWₜ
```

where:

* `Sₜ` = mid-price
* `σ` = instantaneous volatility
* `Wₜ` = standard Brownian motion

The inventory process is represented as:

```text
dqₜ = Nᵦ − Nₐ
```

where `Nᵦ` and `Nₐ` represent executed buy and sell orders.

The objective is to maximize the expected exponential utility of terminal wealth:

```text
max E[-e^(-γ(Xₜ + qₜSₜ))]
```

The Avellaneda–Stoikov framework is then used to determine optimal bid and ask quotes based on market conditions and inventory exposure.

## Execution Logic

The execution layer follows discrete price steps to comply with Binance tick-size rules.

```text
Buy  → floor(Sₜ)
Sell → floor(Sₜ) + 1
```

When the price moves downward beyond the defined threshold after a purchase, an offset sell order with double the volume is placed as a corrective action.

## Technology Stack

* **Python**
* **Binance Testnet**
* **Binance WebSocket API**
* **Binance REST API**
* **Avellaneda–Stoikov Model**
* **Pandas**
* **NumPy**
* **Matplotlib**
* **Docker / PostgreSQL / TimescaleDB** *(if included in your implementation)*

## Project Structure

```text
HFT-Framework/
│
├── data/
│   └── historical_data/
│
├── strategy/
│   └── avellaneda_stoikov.py
│
├── execution/
│   └── order_execution.py
│
├── risk/
│   └── pnl_risk_manager.py
│
├── backtesting/
│   └── backtest.py
│
├── streaming/
│   └── market_data_streamer.py
│
├── config/
│   └── config.py
│
├── requirements.txt
├── README.md
└── main.py
```

## Installation

Clone the repository:

```bash
git clone https://github.com/your-username/hft-framework.git
cd hft-framework
```

Create a virtual environment:

```bash
python -m venv venv
```

Activate it on Windows:

```bash
venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file and add your Binance Testnet credentials:

```env
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```

**Never commit API keys or secrets to GitHub.**

Add `.env` to `.gitignore`:

```text
.env
venv/
__pycache__/
*.pyc
```

## Running the Framework

Start the market-data streamer:

```bash
python streaming/market_data_streamer.py
```

Run the strategy:

```bash
python main.py
```

Run backtesting:

```bash
python backtesting/backtest.py
```

*Update these commands if your actual project filenames differ.*

## Evaluation Metrics

The framework evaluates performance using:

| Metric             | Purpose                                       |
| ------------------ | --------------------------------------------- |
| Cumulative PnL     | Measures overall trading profitability        |
| Maximum Drawdown   | Measures the largest decline from peak equity |
| Inventory Variance | Measures inventory exposure and stability     |
| Sharpe Ratio       | Measures risk-adjusted performance            |

## Sensitivity Analysis

The strategy parameters `γ` and `k` are varied to analyze their impact on performance.

### Risk Aversion (`γ`)

Higher values of `γ` increase the penalty associated with inventory risk and influence the width and positioning of quotes.

### Market Depth (`k`)

The `k` parameter influences the relationship between quote distance and expected order arrival behavior.

The sensitivity experiments are used to determine parameter configurations that provide a suitable balance between profitability and inventory risk.

## Key Features

* Real-time market-data ingestion
* WebSocket-based asynchronous data flow
* Avellaneda–Stoikov market making
* Dynamic bid–ask quote generation
* Inventory-aware pricing
* Real-time order execution
* PnL monitoring
* Risk management
* Historical backtesting
* Parameter sensitivity analysis
* Modular architecture

## Future Scope

The modular architecture allows future integration of:

* Machine-learning-based volatility forecasting
* Reinforcement learning strategies
* Adaptive parameter optimization
* Advanced order-book prediction
* More sophisticated risk-management techniques
* Additional cryptocurrency exchanges

## Disclaimer

This project is developed for **research, educational, and simulation purposes**. It uses Binance Testnet for experimentation and does not constitute financial advice.

## Authors

**Team Members:**
Add team member names here.

**Guide:**
Add guide name here.

## License

This project is intended for academic and research purposes.
