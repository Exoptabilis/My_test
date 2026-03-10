"""Slack notifications for trade alerts and bot status."""

import asyncio
from typing import Optional

import httpx

from rarb.config import get_settings
from rarb.utils.logging import get_logger

log = get_logger(__name__)


class SlackNotifier:
    """Send notifications to Slack via webhook."""

    def __init__(self, webhook_url: Optional[str] = None) -> None:
        settings = get_settings()
        self.webhook_url = webhook_url or settings.slack_webhook_url
        self._client: Optional[httpx.AsyncClient] = None
        self._enabled = self.webhook_url is not None

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def send_message(self, text: str) -> bool:
        """Send a simple text message."""
        if not self._enabled:
            return False

        try:
            client = await self._ensure_client()
            payload = {"text": text}
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()
            log.debug("Slack message sent", text=text[:50])
            return True
        except Exception as e:
            log.debug("Failed to send Slack message", error=str(e))
            return False

    async def notify_arbitrage(
        self,
        market: str,
        yes_ask: float,
        no_ask: float,
        combined: float,
        profit_pct: float,
    ) -> None:
        """Notify about arbitrage opportunity."""
        if not self._enabled:
            return

        message = (
            f"🚀 *Arbitrage Opportunity*\n"
            f"*Market:* {market[:60]}\n"
            f"*YES Ask:* ${yes_ask:.4f}\n"
            f"*NO Ask:* ${no_ask:.4f}\n"
            f"*Combined:* ${combined:.4f}\n"
            f"*Profit:* {profit_pct * 100:.2f}%"
        )
        await self.send_message(message)

    async def notify_startup(self, mode: str) -> None:
        """Notify that bot is starting."""
        message = f"🤖 *rarb Bot Started*\nMode: {mode}"
        await self.send_message(message)

    async def notify_shutdown(self, reason: str = "normal") -> None:
        """Notify that bot is shutting down."""
        message = f"🛑 *rarb Bot Shutdown*\nReason: {reason}"
        await self.send_message(message)

    async def notify_trade(
        self,
        market: str,
        side: str,
        size: float,
        price: float,
        profit: Optional[float] = None,
    ) -> None:
        """Notify about a completed trade."""
        profit_str = f", Profit: ${profit:.2f}" if profit is not None else ""
        message = (
            f"💰 *Trade Executed*\n"
            f"*Market:* {market[:50]}\n"
            f"*Action:* {side.upper()}\n"
            f"*Size:* ${size:.2f}\n"
            f"*Price:* ${price:.4f}{profit_str}"
        )
        await self.send_message(message)

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


# Global notifier instance
_notifier: Optional[SlackNotifier] = None


def get_notifier() -> SlackNotifier:
    """Get or create the global notifier."""
    global _notifier
    if _notifier is None:
        _notifier = SlackNotifier()
    return _notifier