"""FastAPI dashboard for rarb bot monitoring."""

import asyncio
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
from pathlib import Path

from rarb.config import get_settings
from rarb.tracking.trades import TradeLog
from rarb.tracking.portfolio import PortfolioTracker
from rarb.data.repositories import AlertRepository, StatsRepository
from rarb.utils.logging import get_logger

log = get_logger(__name__)

# Setup templates
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# Security
security = HTTPBasic()

app = FastAPI(title="rarb Dashboard")


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    """Verify dashboard credentials."""
    settings = get_settings()
    if not settings.dashboard_password:
        # No auth required
        return True

    correct_username = secrets.compare_digest(credentials.username, settings.dashboard_username)
    correct_password = secrets.compare_digest(credentials.password, settings.dashboard_password)

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, auth: bool = Depends(verify_credentials)):
    """Main dashboard page."""
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "title": "rarb Dashboard"}
    )


@app.get("/api/stats")
async def get_stats(auth: bool = Depends(verify_credentials)):
    """Get current bot statistics."""
    trade_log = TradeLog()
    tracker = PortfolioTracker()

    # Get recent stats from database
    recent_alerts = await AlertRepository.get_recent(limit=50)
    scanner_stats = await StatsRepository.get_latest()

    # Get trade summary
    today = datetime.now().strftime("%Y-%m-%d")
    daily = trade_log.get_daily_summary(today)
    all_time = trade_log.get_all_time_summary()

    # Get portfolio summary
    portfolio = tracker.get_portfolio_summary()

    # Get recent balance snapshots for chart
    snapshots = tracker.get_snapshots(days=7)

    return {
        "scanner": scanner_stats,
        "trading": {
            "daily": daily,
            "all_time": all_time,
        },
        "portfolio": portfolio,
        "recent_alerts": recent_alerts,
        "balance_history": [
            {"timestamp": s.timestamp, "balance": s.total_usd}
            for s in snapshots
        ],
    }


@app.get("/api/alerts")
async def get_alerts(
    limit: int = 100,
    days: int = 1,
    auth: bool = Depends(verify_credentials),
):
    """Get recent arbitrage alerts."""
    alerts = await AlertRepository.get_recent(limit=limit, days=days)
    return {"alerts": alerts}


@app.get("/api/trades")
async def get_trades(
    limit: int = 50,
    platform: Optional[str] = None,
    auth: bool = Depends(verify_credentials),
):
    """Get recent trades."""
    trade_log = TradeLog()
    trades = trade_log.get_trades(limit=limit, platform=platform)

    return {
        "trades": [
            {
                "timestamp": t.timestamp,
                "platform": t.platform,
                "market": t.market_name,
                "outcome": t.outcome,
                "side": t.side,
                "price": t.price,
                "size": t.size,
                "profit": t.profit_expected,
                "status": t.status,
            }
            for t in trades
        ]
    }


@app.get("/api/positions")
async def get_positions(auth: bool = Depends(verify_credentials)):
    """Get current open positions."""
    from rarb.executor.async_clob import AsyncClobClient
    from rarb.utils.signer import Signer

    settings = get_settings()
    if not settings.wallet_address:
        return {"positions": []}

    try:
        signer = Signer()
        async with AsyncClobClient(signer=signer) as clob:
            positions = await clob.get_positions()

        # Filter to open positions
        open_positions = [
            p for p in positions
            if float(p.get("size", 0)) > 0 and not p.get("redeemable", False)
        ]

        return {"positions": open_positions}
    except Exception as e:
        log.error("Failed to fetch positions", error=str(e))
        return {"positions": [], "error": str(e)}


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
    }


def run_dashboard(host: str = "0.0.0.0", port: int = 8080) -> None:
    """Run the dashboard server."""
    import uvicorn

    log.info(f"Starting dashboard on {host}:{port}")
    uvicorn.run(app, host=host, port=port)