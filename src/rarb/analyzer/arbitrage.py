"""Arbitrage opportunity detection and analysis."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from rarb.api.models import Market, ArbitrageOpportunity
from rarb.config import get_settings
from rarb.scanner.market_scanner import MarketSnapshot
from rarb.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class AnalyzerStats:
    """Statistics for the arbitrage analyzer."""
    snapshots_analyzed: int = 0
    opportunities_found: int = 0
    last_opportunity_time: float = 0
    min_profit_seen: float = 0
    max_profit_seen: float = 0


class ArbitrageAnalyzer:
    """
    Analyzes market snapshots for arbitrage opportunities.

    The core strategy is simple: if combined cost of YES + NO < $1.00,
    buying both locks in profit. This is pure arbitrage with no directional bias.
    """

    def __init__(self, min_profit_threshold: Optional[float] = None) -> None:
        settings = get_settings()
        self.min_profit_threshold = Decimal(
            str(min_profit_threshold if min_profit_threshold is not None
                else settings.min_profit_threshold)
        )
        self.stats = AnalyzerStats()

    def analyze(self, snapshot: MarketSnapshot) -> Optional[ArbitrageOpportunity]:
        """
        Analyze a single market snapshot for arbitrage.

        Args:
            snapshot: Market data including orderbooks

        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        self.stats.snapshots_analyzed += 1

        # Check if we have valid prices
        if snapshot.combined_ask is None:
            return None

        # Calculate profit
        profit_pct = snapshot.arbitrage_spread
        if profit_pct is None:
            return None

        # Update stats
        profit_float = float(profit_pct)
        if profit_float > 0:
            if self.stats.min_profit_seen == 0 or profit_float < self.stats.min_profit_seen:
                self.stats.min_profit_seen = profit_float
            if profit_float > self.stats.max_profit_seen:
                self.stats.max_profit_seen = profit_float

        # Check if profit meets threshold
        if profit_pct < self.min_profit_threshold:
            return None

        # Calculate max trade size based on available liquidity
        max_size = snapshot.min_liquidity_at_ask
        if max_size is None or max_size <= 0:
            log.debug("Skipping opportunity - no liquidity", market=snapshot.market.question[:30])
            return None

        # Create opportunity
        opportunity = ArbitrageOpportunity(
            market=snapshot.market,
            yes_ask=snapshot.yes_best_ask or Decimal("0"),
            no_ask=snapshot.no_best_ask or Decimal("0"),
            combined_cost=snapshot.combined_ask,
            profit_pct=profit_pct,
            yes_size_available=snapshot.yes_orderbook.best_ask_size or Decimal("0"),
            no_size_available=snapshot.no_orderbook.best_ask_size or Decimal("0"),
            max_trade_size=max_size,
        )

        self.stats.opportunities_found += 1
        self.stats.last_opportunity_time = __import__("time").time()

        log.debug(
            "Arbitrage opportunity found",
            market=snapshot.market.question[:30],
            profit=f"{float(profit_pct) * 100:.2f}%",
            max_size=f"${float(max_size):.0f}",
        )

        return opportunity

    def analyze_batch(self, snapshots: list[MarketSnapshot]) -> list[ArbitrageOpportunity]:
        """
        Analyze multiple snapshots and return all opportunities.

        Args:
            snapshots: List of market snapshots

        Returns:
            List of arbitrage opportunities (may be empty)
        """
        opportunities = []
        for snapshot in snapshots:
            opp = self.analyze(snapshot)
            if opp is not None:
                opportunities.append(opp)

        # Sort by profit (best first)
        opportunities.sort(key=lambda x: x.profit_pct, reverse=True)

        return opportunities

    def get_stats(self) -> dict:
        """Get analyzer statistics."""
        return {
            "snapshots_analyzed": self.stats.snapshots_analyzed,
            "opportunities_found": self.stats.opportunities_found,
            "last_opportunity_time": self.stats.last_opportunity_time,
            "min_profit_seen": self.stats.min_profit_seen,
            "max_profit_seen": self.stats.max_profit_seen,
        }