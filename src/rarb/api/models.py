"""Data models for Polymarket API responses."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional


@dataclass
class Token:
    """Represents a YES or NO token for a market."""
    token_id: str
    outcome: str  # "Yes" or "No"
    price: Optional[Decimal] = None


@dataclass
class Market:
    """A Polymarket market (question)."""
    id: str
    question: str
    description: Optional[str] = None
    condition_id: str
    slug: str
    resolution_source: Optional[str] = None
    end_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    creation_date: Optional[datetime] = None
    liquidity: Decimal = Decimal("0")
    volume: Decimal = Decimal("0")
    open_interest: Decimal = Decimal("0")
    yes_price: Decimal = Decimal("0.5")
    no_price: Decimal = Decimal("0.5")
    yes_token: Optional[Token] = None
    no_token: Optional[Token] = None
    raw_data: dict[str, Any] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        """Check if market is active (not resolved and not ended)."""
        if self.end_date is None:
            return True
        return datetime.now() < self.end_date.replace(tzinfo=None)


@dataclass
class OrderBookLevel:
    """A single level in an orderbook."""
    price: Decimal
    size: Decimal


@dataclass
class OrderBook:
    """Orderbook for a single token."""
    asset_id: str
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    timestamp: Optional[float] = None

    @property
    def best_bid(self) -> Optional[Decimal]:
        """Best bid price."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        """Best ask price."""
        return self.asks[0].price if self.asks else None

    @property
    def best_bid_size(self) -> Optional[Decimal]:
        """Size at best bid."""
        return self.bids[0].size if self.bids else None

    @property
    def best_ask_size(self) -> Optional[Decimal]:
        """Size at best ask."""
        return self.asks[0].size if self.asks else None

    @property
    def spread(self) -> Optional[Decimal]:
        """Bid-ask spread."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid


@dataclass
class ArbitrageOpportunity:
    """Represents a detected arbitrage opportunity."""
    market: Market
    yes_ask: Decimal
    no_ask: Decimal
    combined_cost: Decimal
    profit_pct: Decimal
    yes_size_available: Decimal
    no_size_available: Decimal
    max_trade_size: Decimal  # In shares

    @property
    def expected_profit_usd(self) -> Decimal:
        """Expected profit in USD for max trade size."""
        return self.max_trade_size * self.profit_pct


@dataclass
class OrderResult:
    """Result of an order execution."""
    order_id: str
    token_id: str
    side: str  # "BUY" or "SELL"
    price: Decimal
    size: Decimal
    status: str  # "OPEN", "FILLED", "CANCELLED"
    filled_size: Decimal = Decimal("0")
    transaction_hash: Optional[str] = None