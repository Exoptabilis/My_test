"""Trade logging and history tracking."""

import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List


@dataclass
class TradeRecord:
    """Record of a completed trade."""
    timestamp: str
    platform: str  # "polymarket" or "kalshi"
    market_id: str
    market_name: str
    outcome: str  # "yes", "no", or "yes+no" for arbitrage
    side: str  # "buy" or "sell"
    price: float
    size: float  # USD amount
    profit_expected: Optional[float] = None
    profit_realized: Optional[float] = None
    status: str = "filled"  # filled, partial, cancelled
    order_ids: Optional[str] = None  # JSON list of order IDs
    notes: Optional[str] = None


class TradeLog:
    """Persistent trade logging to SQLite."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        if db_path is None:
            db_path = Path.home() / ".rarb" / "trades.db"
            db_path.parent.mkdir(exist_ok=True)

        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    profit_expected REAL,
                    profit_realized REAL,
                    status TEXT NOT NULL,
                    order_ids TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Index for faster queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON trades(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_platform ON trades(platform)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")

    def record_trade(self, trade: TradeRecord) -> int:
        """Record a trade in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO trades (
                    timestamp, platform, market_id, market_name, outcome,
                    side, price, size, profit_expected, profit_realized,
                    status, order_ids, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.timestamp,
                trade.platform,
                trade.market_id,
                trade.market_name,
                trade.outcome,
                trade.side,
                trade.price,
                trade.size,
                trade.profit_expected,
                trade.profit_realized,
                trade.status,
                trade.order_ids,
                trade.notes,
            ))
            return cursor.lastrowid

    def get_trades(
        self,
        limit: int = 100,
        platform: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[TradeRecord]:
        """Get recent trades with optional filters."""
        query = "SELECT * FROM trades"
        params = []

        where_clauses = []
        if platform:
            where_clauses.append("platform = ?")
            params.append(platform)
        if status:
            where_clauses.append("status = ?")
            params.append(status)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

        trades = []
        for row in rows:
            trades.append(TradeRecord(
                timestamp=row["timestamp"],
                platform=row["platform"],
                market_id=row["market_id"],
                market_name=row["market_name"],
                outcome=row["outcome"],
                side=row["side"],
                price=row["price"],
                size=row["size"],
                profit_expected=row["profit_expected"],
                profit_realized=row["profit_realized"],
                status=row["status"],
                order_ids=row["order_ids"],
                notes=row["notes"],
            ))

        return trades

    def update_profit(self, trade_id: int, profit_realized: float) -> bool:
        """Update realized profit for a trade."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE trades SET profit_realized = ? WHERE id = ?",
                (profit_realized, trade_id),
            )
            return cursor.rowcount > 0

    def get_daily_summary(self, date: Optional[str] = None) -> dict:
        """Get summary for a specific date (default: today)."""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        with sqlite3.connect(self.db_path) as conn:
            # Total trades and volume
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as trade_count,
                    SUM(size) as total_volume,
                    SUM(profit_expected) as total_expected_profit,
                    SUM(profit_realized) as total_realized_profit
                FROM trades
                WHERE date(timestamp) = ?
            """, (date,))
            row = cursor.fetchone()

        return {
            "date": date,
            "trade_count": row[0] or 0,
            "total_volume": row[1] or 0.0,
            "total_expected_profit": row[2] or 0.0,
            "total_realized_profit": row[3] or 0.0,
        }

    def get_all_time_summary(self) -> dict:
        """Get all-time trading summary."""
        with sqlite3.connect(self.db_path) as conn:
            # Total trades and volume
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as trade_count,
                    SUM(size) as total_volume,
                    SUM(profit_expected) as total_expected_profit,
                    SUM(profit_realized) as total_realized_profit,
                    MIN(timestamp) as first_trade,
                    MAX(timestamp) as last_trade
                FROM trades
            """)
            row = cursor.fetchone()

            # Win rate
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as winning_trades
                FROM trades
                WHERE profit_realized > 0
            """)
            winning = cursor.fetchone()[0] or 0

        total_trades = row[0] or 0
        return {
            "trade_count": total_trades,
            "total_volume": row[1] or 0.0,
            "total_expected_profit": row[2] or 0.0,
            "total_realized_profit": row[3] or 0.0,
            "first_trade": row[4],
            "last_trade": row[5],
            "winning_trades": winning,
            "win_rate": (winning / total_trades * 100) if total_trades > 0 else 0,
        }