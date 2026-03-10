"""Order execution engine for Polymarket."""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Union

from rarb.api.clob import ClobClient
from rarb.api.models import ArbitrageOpportunity, OrderResult
from rarb.config import get_settings
from rarb.notifications.slack import get_notifier
from rarb.tracking.trades import TradeLog
from rarb.utils.logging import get_logger
from rarb.utils.signer import Signer

log = get_logger(__name__)


class ExecutionStatus(Enum):
    """Status of an execution attempt."""
    PENDING = "pending"
    FILLED = "filled"
    PARTIAL = "partial"
    CANCELLED = "cancelled"
    FAILED = "failed"
    SKIPPED = "skipped"  # e.g., due to risk checks or dry run


@dataclass
class ExecutionResult:
    """Result of executing an arbitrage opportunity."""
    opportunity: ArbitrageOpportunity
    status: ExecutionStatus
    yes_order: Optional[OrderResult] = None
    no_order: Optional[OrderResult] = None
    expected_profit: Decimal = Decimal("0")
    execution_time_ms: float = 0
    error: Optional[str] = None
    dry_run: bool = False

    @property
    def success(self) -> bool:
        """Whether execution was successful."""
        return self.status == ExecutionStatus.FILLED

    @property
    def filled_size(self) -> Decimal:
        """Actual filled size (min of both orders)."""
        yes_filled = self.yes_order.filled_size if self.yes_order else Decimal("0")
        no_filled = self.no_order.filled_size if self.no_order else Decimal("0")
        return min(yes_filled, no_filled)


class OrderExecutor:
    """
    Executes arbitrage opportunities on Polymarket.

    Responsibilities:
    - Sign and place orders for both YES and NO tokens
    - Monitor order status
    - Handle partial fills and cancellations
    - Track execution metrics
    """

    def __init__(
        self,
        clob_client: Optional[ClobClient] = None,
        signer: Optional[Signer] = None,
        dry_run: Optional[bool] = None,
    ) -> None:
        settings = get_settings()

        self.signer = signer or Signer()
        self.clob = clob_client or ClobClient(signer=self.signer)
        self.dry_run = dry_run if dry_run is not None else settings.dry_run

        self.trade_log = TradeLog()
        self._pending_orders: dict[str, asyncio.Task] = {}
        self._stats = {
            "execution_attempts": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "total_volume_usd": Decimal("0"),
            "total_profit_usd": Decimal("0"),
            "avg_execution_time_ms": 0,
        }

    async def execute(
        self,
        opportunity: ArbitrageOpportunity,
        *,
        timeout_seconds: int = 10,
        detection_timestamp_ms: Optional[float] = None,
    ) -> ExecutionResult:
        """
        Execute an arbitrage opportunity.

        Places two orders simultaneously:
        - BUY YES token at best ask
        - BUY NO token at best ask

        Args:
            opportunity: The arbitrage opportunity to execute
            timeout_seconds: How long to wait for fills before cancelling
            detection_timestamp_ms: When opportunity was first detected (for latency tracking)

        Returns:
            ExecutionResult with details of the execution
        """
        self._stats["execution_attempts"] += 1
        start_time = time.time()

        # Track latency if detection timestamp provided
        if detection_timestamp_ms:
            latency_ms = (start_time * 1000) - detection_timestamp_ms
            log.debug("Execution latency", latency_ms=f"{latency_ms:.1f}ms")

        # Dry run mode - simulate execution
        if self.dry_run:
            log.info(
                "[DRY RUN] Would execute arbitrage",
                market=opportunity.market.question[:30],
                size=float(opportunity.max_trade_size),
                profit=f"${float(opportunity.expected_profit_usd):.2f}",
            )
            return ExecutionResult(
                opportunity=opportunity,
                status=ExecutionStatus.SKIPPED,
                expected_profit=opportunity.expected_profit_usd,
                dry_run=True,
            )

        # Check if we're configured for live trading
        if not self.signer.is_configured:
            log.error("Cannot execute live trade: signer not configured")
            return ExecutionResult(
                opportunity=opportunity,
                status=ExecutionStatus.FAILED,
                error="Signer not configured",
            )

        log.info(
            "Executing arbitrage",
            market=opportunity.market.question[:30],
            size=float(opportunity.max_trade_size),
            yes_price=f"${float(opportunity.yes_ask):.4f}",
            no_price=f"${float(opportunity.no_ask):.4f}",
            expected_profit=f"${float(opportunity.expected_profit_usd):.2f}",
        )

        try:
            # Place both orders concurrently
            yes_order_task = self.clob.place_order(
                token_id=opportunity.market.yes_token.token_id,
                side="BUY",
                price=opportunity.yes_ask,
                size=opportunity.max_trade_size,
            )
            no_order_task = self.clob.place_order(
                token_id=opportunity.market.no_token.token_id,
                side="BUY",
                price=opportunity.no_ask,
                size=opportunity.max_trade_size,
            )

            # Wait for both orders with timeout
            yes_result, no_result = await asyncio.wait_for(
                asyncio.gather(
                    yes_order_task,
                    no_order_task,
                    return_exceptions=True,
                ),
                timeout=timeout_seconds,
            )

            # Check for exceptions
            if isinstance(yes_result, Exception):
                log.error("YES order failed", error=str(yes_result))
                yes_result = None
            if isinstance(no_result, Exception):
                log.error("NO order failed", error=str(no_result))
                no_result = None

            # If both orders failed, execution failed
            if yes_result is None and no_result is None:
                raise Exception("Both orders failed")

            # Determine status based on fills
            status = self._determine_status(yes_result, no_result)

            # Calculate actual filled size and profit
            yes_filled = yes_result.filled_size if yes_result else Decimal("0")
            no_filled = no_result.filled_size if no_result else Decimal("0")
            filled_shares = min(yes_filled, no_filled)
            actual_profit = filled_shares * opportunity.profit_pct

            # Log the trade
            self.trade_log.record_trade(
                platform="polymarket",
                market_id=opportunity.market.id,
                market_name=opportunity.market.question,
                outcome="yes+no",
                side="buy",
                price=float(opportunity.combined_cost / 2),  # Average price per token
                size=float(filled_shares * opportunity.combined_cost),  # Total cost
                profit_expected=float(actual_profit),
                status=status.value,
            )

            # Update stats
            self._stats["successful_executions"] += 1 if status == ExecutionStatus.FILLED else 0
            self._stats["failed_executions"] += 1 if status == ExecutionStatus.FAILED else 0
            self._stats["total_volume_usd"] += filled_shares * opportunity.combined_cost
            self._stats["total_profit_usd"] += actual_profit

            execution_time_ms = (time.time() - start_time) * 1000
            self._update_avg_time(execution_time_ms)

            # Send notification
            if status == ExecutionStatus.FILLED:
                asyncio.create_task(self._notify_success(opportunity, filled_shares, actual_profit))
            elif status == ExecutionStatus.PARTIAL:
                asyncio.create_task(self._notify_partial(opportunity, filled_shares, actual_profit))

            log.info(
                "Execution completed",
                status=status.value,
                filled_shares=float(filled_shares),
                actual_profit=f"${float(actual_profit):.2f}",
                time_ms=f"{execution_time_ms:.1f}",
            )

            return ExecutionResult(
                opportunity=opportunity,
                status=status,
                yes_order=yes_result,
                no_order=no_result,
                expected_profit=actual_profit,
                execution_time_ms=execution_time_ms,
            )

        except asyncio.TimeoutError:
            log.warning("Execution timeout - cancelling orders")
            # Cancel pending orders
            await self._cancel_pending(opportunity)
            return ExecutionResult(
                opportunity=opportunity,
                status=ExecutionStatus.CANCELLED,
                error="Timeout",
            )

        except Exception as e:
            log.error("Execution failed", error=str(e))
            self._stats["failed_executions"] += 1
            return ExecutionResult(
                opportunity=opportunity,
                status=ExecutionStatus.FAILED,
                error=str(e),
            )

    def _determine_status(
        self,
        yes_order: Optional[OrderResult],
        no_order: Optional[OrderResult],
    ) -> ExecutionStatus:
        """Determine execution status based on order results."""
        if yes_order is None or no_order is None:
            return ExecutionStatus.FAILED

        if yes_order.status == "FILLED" and no_order.status == "FILLED":
            return ExecutionStatus.FILLED

        if yes_order.filled_size > 0 and no_order.filled_size > 0:
            return ExecutionStatus.PARTIAL

        return ExecutionStatus.FAILED

    async def _cancel_pending(self, opportunity: ArbitrageOpportunity) -> None:
        """Cancel any pending orders for this opportunity."""
        try:
            if opportunity.market.yes_token:
                await self.clob.cancel_orders(token_id=opportunity.market.yes_token.token_id)
            if opportunity.market.no_token:
                await self.clob.cancel_orders(token_id=opportunity.market.no_token.token_id)
        except Exception as e:
            log.error("Failed to cancel orders", error=str(e))

    def _update_avg_time(self, new_time_ms: float) -> None:
        """Update average execution time."""
        n = self._stats["execution_attempts"]
        old_avg = self._stats["avg_execution_time_ms"]
        self._stats["avg_execution_time_ms"] = (old_avg * (n - 1) + new_time_ms) / n

    async def _notify_success(
        self,
        opportunity: ArbitrageOpportunity,
        filled_shares: Decimal,
        profit: Decimal,
    ) -> None:
        """Send success notification."""
        try:
            notifier = get_notifier()
            await notifier.send_message(
                f"✅ Trade filled: {opportunity.market.question[:50]}\n"
                f"Profit: ${float(profit):.2f} on {float(filled_shares):.0f} shares"
            )
        except Exception as e:
            log.debug("Failed to send notification", error=str(e))

    async def _notify_partial(
        self,
        opportunity: ArbitrageOpportunity,
        filled_shares: Decimal,
        profit: Decimal,
    ) -> None:
        """Send partial fill notification."""
        try:
            notifier = get_notifier()
            await notifier.send_message(
                f"⚠️ Partial fill: {opportunity.market.question[:50]}\n"
                f"Filled: {float(filled_shares):.0f}/{float(opportunity.max_trade_size):.0f} shares\n"
                f"Profit: ${float(profit):.2f}"
            )
        except Exception as e:
            log.debug("Failed to send notification", error=str(e))

    async def close(self) -> None:
        """Close the executor and cancel any pending orders."""
        log.info("Closing executor")
        if self._pending_orders:
            for task in self._pending_orders.values():
                task.cancel()
            await asyncio.gather(*self._pending_orders.values(), return_exceptions=True)
        await self.clob.close()

    def get_stats(self) -> dict:
        """Get execution statistics."""
        return {
            **self._stats,
            "total_volume_usd": float(self._stats["total_volume_usd"]),
            "total_profit_usd": float(self._stats["total_profit_usd"]),
        }

    async def __aenter__(self) -> "OrderExecutor":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()