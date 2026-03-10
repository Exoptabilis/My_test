# Polymarket Bot | Polymarket Trading Bot | Polymarket Trading Bot - 5 min market | Polymarket Arbitrage Bot

Polymarket Bot, Polymarket Trading Bot, Polymarket 5min market Trading Bot, Polymarket Automatic Trading Bot

A high-performance, automated trading system for [Polymarket](https://polymarket.com) prediction markets. Built in Python with real-time WebSocket streaming, gasless L2 execution, and a risk-management framework suited to short-term and high-frequency environments.



<img width="1058" height="698" alt="image" src="https://github.com/user-attachments/assets/fbe3a09e-e33b-40d7-8eed-d0cfe2f54caa" />



## Contact

| Channel | Link |
|---------|------|
| **Email** | benjamin.bigdev@gmail.com |
| **Telegram** | [@BenjaminCup](https://t.me/BenjaminCup) |
| **X (Twitter)** | [@benjaminccup](https://x.com/benjaminccup) |

---

## If you'd like, I can show you a profitable bot in action through a meeting.


## Recording Video





https://github.com/user-attachments/assets/b038aa3b-e42b-4f72-ac5d-a130cdb56a9f






---
## Running Bot Screenshot

<img width="1796" height="937" alt="github-update-4-" src="https://github.com/user-attachments/assets/6b5f2782-a0f8-4333-9727-7699cd88c839" />


<img width="1796" height="930" alt="github-update-5-" src="https://github.com/user-attachments/assets/13d4b57a-7268-4102-ba28-93017026884f" />


---
## Result Screenshot

<img width="1384" height="895" alt="github-update-1" src="https://github.com/user-attachments/assets/e95f5fd9-df93-4d8f-be58-bee7568619c0" />

<img width="1489" height="920" alt="github-update-3" src="https://github.com/user-attachments/assets/33237de3-cd03-4a27-9c78-13455b925572" />


---
## Telegram Trial Version Bot
You can also test the bot directly on Telegram.

**@benjamincup_polymarket_bot[https://t.me/benjamincup_polymarket_bot]**

<img width="463" height="308" alt="image" src="https://github.com/user-attachments/assets/1ca27ab4-f200-4e84-995a-075b7ff6c9a9" />

<img width="552" height="569" alt="image" src="https://github.com/user-attachments/assets/b54449bd-e8b6-4ecd-894d-71fa471e7ea8" />



---

## Features

- **Real-time WebSocket monitoring** — 6 parallel connections, up to 1,500 markets
- **Automatic arbitrage detection and execution** — Buy YES + NO when combined cost &lt; $1.00
- **Low-latency execution** — Async HTTP/2, parallel order signing, 10s timeout with auto-cancel
- **Risk management** — Position sizing (risk-per-trade %), circuit breakers (consecutive losses, session/daily/monthly drawdown), pre-trade filters (time to resolution, optional volatility/volume/z-score/RSI)
- **Market filters** — Liquidity ($10k+ default), resolution horizon (7 days default), min seconds to resolution
- **Web dashboard** — Live order visibility over HTTPS with optional auth
- **Notifications** — Slack (and optional Telegram) for trades and alerts
- **Geo bypass** — SOCKS5 proxy support for order placement from non–US regions

---

## Strategy

### Pure arbitrage (this bot)

When the best ask for YES and the best ask for NO sum to **less than $1.00**, buying both locks in profit: one side always pays $1.00 at resolution.

| Example   | YES ask | NO ask | Combined | Payout | Profit   |
|----------|---------|--------|----------|--------|----------|
| Arbitrage | $0.48   | $0.49  | $0.97    | $1.00  | ~3.09%   |

The bot scans order books in real time, sizes positions by risk, applies circuit breakers and filters, and executes when the combined cost is below your configured threshold (e.g. 0.5%).

### Other strategies (reference only)

Directional or momentum strategies (e.g. moving the book then buying the cheap side) are **not** implemented in this repo. For discussion of those approaches, see the [Medium article](https://medium.com/@benjamin.bigdev/high-roi-polymarket-arbitrage-in-2026-programmatic-dutch-book-strategies-bots-and-portfolio-41372221bb79).

---

## Risk management

The bot includes a configurable risk framework aimed at short-term / 5-minute-style markets:

- **Position sizing** — Max risk per trade as % of balance (e.g. 0.5–1.5%), stop-based distance, cap as % of account (e.g. 25%).
- **Circuit breakers** — Pause after N consecutive losses; pause or stop when session/daily/monthly drawdown limits are hit; optional volatility kill (skip entries when 1-min std dev exceeds a threshold).
- **Pre-trade filters** — Min seconds until resolution (e.g. 90s), optional min 60s volume, z-score, and RSI filters.

See `.env.example` for all risk-related variables (`RISK_PER_TRADE_PCT`, `STOP_LOSS_PCT`, `CONSECUTIVE_LOSSES_PAUSE`, `SESSION_DRAWDOWN_PCT`, `DAILY_DRAWDOWN_PCT`, `MIN_SECONDS_UNTIL_RESOLUTION`, etc.). Tune after backtesting; default values are conservative starting points.

---

## Setup

```bash
# Clone
git clone https://github.com/Gabagool2-2/polymarket-trading-bot-python.git
cd polymarket-trading-bot-python

# Install
pip install -e .

# Configure
cp .env.example .env
# Edit .env with your wallet, API credentials, and risk/strategy parameters

# Generate Polymarket L2 API credentials
python -c "
from py_clob_client.client import ClobClient
import os
client = ClobClient('https://clob.polymarket.com', key=os.environ['PRIVATE_KEY'], chain_id=137)
creds = client.create_or_derive_api_creds()
print(f'POLY_API_KEY={creds.api_key}')
print(f'POLY_API_SECRET={creds.api_secret}')
print(f'POLY_API_PASSPHRASE={creds.api_passphrase}')
"

# One-time: approve Polymarket contracts for USDC
python scripts/approve_usdc.py

# Run (realtime WebSocket mode)
rarb run --live --realtime
```

---

## Configuration

**Required**

| Variable | Description |
|----------|-------------|
| `PRIVATE_KEY` | Wallet private key (hex, `0x` prefix) |
| `WALLET_ADDRESS` | Wallet address |
| `POLY_API_KEY` / `POLY_API_SECRET` / `POLY_API_PASSPHRASE` | L2 API credentials (from script above) |

**Trading**

| Variable | Default | Description |
|----------|---------|-------------|
| `MIN_PROFIT_THRESHOLD` | 0.005 | Min profit (0.5%) to trigger execution |
| `MAX_POSITION_SIZE` | 100 | Max USD per trade |
| `MIN_LIQUIDITY_USD` | 10000 | Min market liquidity |
| `MAX_DAYS_UNTIL_RESOLUTION` | 7 | Skip markets resolving later |
| `NUM_WS_CONNECTIONS` | 6 | WebSocket connections |
| `DRY_RUN` | true | Set `false` for live trading |

**Risk (see `.env.example` for full list)**  
`RISK_PER_TRADE_PCT`, `STOP_LOSS_PCT`, `TIME_STOP_SECONDS`, `POSITION_CAP_PCT`, `CONSECUTIVE_LOSSES_PAUSE`, `SESSION_DRAWDOWN_PCT`, `DAILY_DRAWDOWN_PCT`, `MIN_SECONDS_UNTIL_RESOLUTION`, etc.

**Optional**  
Dashboard (`DASHBOARD_USERNAME`, `DASHBOARD_PASSWORD`), Slack/Telegram, SOCKS5 proxy (see Geo-Restrictions below).

Full reference: `.env.example`.

---

## Contract approvals

Before trading, approve Polymarket contracts to spend your USDC.e:

```bash
python scripts/approve_usdc.py
```

This approves CTF Exchange, Neg Risk Exchange, Conditional Tokens, and Neg Risk Adapter.

---

## Geo-restrictions

Polymarket restricts order placement from certain jurisdictions (e.g. US IPs). A common setup:

- **Bot / scanner:** Low-latency server (e.g. US) for WebSocket market data.
- **Orders:** Routed through a SOCKS5 proxy in an allowed region (e.g. Canada).

Configure in `.env`:

```bash
SOCKS5_PROXY_HOST=your-proxy-host
SOCKS5_PROXY_PORT=1080
SOCKS5_PROXY_USER=rarb
SOCKS5_PROXY_PASS=your-password
```

Deployment examples (OpenTofu + Ansible): `infra/`.

---

## Documentation

- **[PRD.md](PRD.md)** — Product requirements and technical architecture.
- **Risk logic** — `src/rarb/risk/manager.py` (position sizing, circuit breakers, pre-trade filters).

---



---


## License

MIT
