"""Portfolio tracking and balance monitoring."""

import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from decimal import Decimal

from rarb.config import get_settings
from rarb.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class BalanceSnapshot:
    """Snapshot of balances at a point in time."""
    timestamp: str
    polymarket_usdc: float
    kalshi_usd: float = 0.0
    total_usd: float = 0.0
    positions_value: float = 0.0


class PortfolioTracker:
    """Track portfolio value over time."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        if db_path is None:
            db_path = Path.home() / ".rarb" / "portfolio.db"
            db_path.parent.mkdir(exist_ok=True)

        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS balance_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL UNIQUE,
                    polymarket_usdc REAL NOT NULL,
                    kalshi_usd REAL NOT NULL,
                    total_usd REAL NOT NULL,
                    positions_value REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON balance_snapshots(timestamp)")

    def record_snapshot(self, snapshot: BalanceSnapshot) -> int:
        """Record a balance snapshot."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO balance_snapshots (
                    timestamp, polymarket_usdc, kalshi_usd, total_usd, positions_value
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                snapshot.timestamp,
                snapshot.polymarket_usdc,
                snapshot.kalshi_usd,
                snapshot.total_usd,
                snapshot.positions_value,
            ))
            return cursor.lastrowid

    async def record_snapshot_async(self, snapshot: BalanceSnapshot) -> int:
        """Async version of record_snapshot."""
        return self.record_snapshot(snapshot)

    def get_snapshots(
        self,
        days: int = 30,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[BalanceSnapshot]:
        """Get balance snapshots for a time range."""
        query = "SELECT * FROM balance_snapshots"
        params = []

        where_clauses = []
        if start_date:
            where_clauses.append("timestamp >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("timestamp <= ?")
            params.append(end_date)

        if not start_date and not end_date:
            # Default: last N days
            where_clauses.append("timestamp >= datetime('now', '-' || ? || ' days')")
            params.append(str(days))

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY timestamp ASC"

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

        snapshots = []
        for row in rows:
            snapshots.append(BalanceSnapshot(
                timestamp=row["timestamp"],
                polymarket_usdc=row["polymarket_usdc"],
                kalshi_usd=row["kalshi_usd"],
                total_usd=row["total_usd"],
                positions_value=row["positions_value"],
            ))

        return snapshots

    async def get_current_balances(self) -> dict:
        """Get current balances from all platforms."""
        from rarb.api.clob import ClobClient

        settings = get_settings()
        result = {
            "polymarket_usdc": 0.0,
            "kalshi_usd": 0.0,
            "total_usd": 0.0,
            "timestamp": datetime.now().isoformat(),
        }

        # Get Polymarket USDC balance
        if settings.wallet_address:
            try:
                async with ClobClient() as clob:
                    balance = await clob.get_usdc_balance()
                    result["polymarket_usdc"] = float(balance)
            except Exception as e:
                log.error("Failed to fetch Polymarket balance", error=str(e))

        # Try Kalshi if configured
        if settings.is_kalshi_enabled():
            try:
                from rarb.api.kalshi import KalshiClient
                async with KalshiClient() as kalshi:
                    balance = await kalshi.get_balance()
                    result["kalshi_usd"] = float(balance)
            except Exception as e:
                log.error("Failed to fetch Kalshi balance", error=str(e))

        result["total_usd"] = result["polymarket_usdc"] + result["kalshi_usd"]
        return result

    def get_portfolio_summary(self) -> dict:
        """Get portfolio summary with daily P&L."""
        with sqlite3.connect(self.db_path) as conn:
            # Get latest snapshot
            cursor = conn.execute("""
                SELECT * FROM balance_snapshots
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            latest = cursor.fetchone()

            if not latest:
                return {}

            # Get snapshot from 24h ago
            cursor = conn.execute("""
                SELECT * FROM balance_snapshots
                WHERE timestamp <= datetime('now', '-1 day')
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            day_ago = cursor.fetchone()

            # Get snapshot from 7 days ago
            cursor = conn.execute("""
                SELECT * FROM balance_snapshots
                WHERE timestamp <= datetime('now', '-7 days')
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            week_ago = cursor.fetchone()

            # Calculate changes
            current = latest[3]  # total_usd
            day_change = current - (day_ago[3] if day_ago else current)
            week_change = current - (week_ago[3] if week_ago else current)

            return {
                "current_balance": current,
                "day_change": day_change,
                "day_change_pct": (day_change / (day_ago[3] if day_ago else current) * 100) if day_ago else 0,
                "week_change": week_change,
                "week_change_pct": (week_change / (week_ago[3] if week_ago else current) * 100) if week_ago else 0,
                "last_updated": latest[1],  # timestamp
            }