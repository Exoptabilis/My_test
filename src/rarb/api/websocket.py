"""WebSocket client for Polymarket real-time data."""

import asyncio
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Optional

import websockets

from rarb.api.models import OrderBook, OrderBookLevel
from rarb.config import get_settings
from rarb.utils.logging import get_logger

log = get_logger(__name__)

# WebSocket endpoints
PRICE_FEED_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class OrderBookUpdate:
    """Orderbook snapshot update from WebSocket."""
    asset_id: str
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class PriceChange:
    """Real-time price change event."""
    asset_id: str
    price: Decimal
    size: Decimal
    side: str  # "BUY" or "SELL"
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    timestamp: float = field(default_factory=time.time)


class WebSocketClient:
    """
    WebSocket client for Polymarket real-time data.

    Subscribes to orderbook and price updates for multiple tokens.
    """

    def __init__(
        self,
        on_book: Optional[Callable[[OrderBookUpdate], None]] = None,
        on_price_change: Optional[Callable[[PriceChange], None]] = None,
    ) -> None:
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._on_book = on_book
        self._on_price_change = on_price_change
        self._subscribed_assets: set[str] = set()
        self._running = False
        self._reconnect_delay = 1
        self._last_message_time: float = 0
        self._orderbooks: dict[str, OrderBook] = {}

    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._ws is not None and not self._ws.closed

    @property
    def subscribed_count(self) -> int:
        """Number of subscribed assets."""
        return len(self._subscribed_assets)

    @property
    def seconds_since_last_message(self) -> float:
        """Seconds since last message was received."""
        if self._last_message_time == 0:
            return float('inf')
        return time.time() - self._last_message_time

    async def connect(self) -> None:
        """Establish WebSocket connection."""
        if self.is_connected:
            return

        try:
            self._ws = await websockets.connect(PRICE_FEED_URL)
            log.info("WebSocket connected")
            self._reconnect_delay = 1

            # If we had previous subscriptions, re-subscribe
            if self._subscribed_assets:
                await self.subscribe(list(self._subscribed_assets))

        except Exception as e:
            log.error("WebSocket connection failed", error=str(e))
            raise

    async def subscribe(self, asset_ids: list[str]) -> None:
        """
        Subscribe to price updates for multiple assets.

        Args:
            asset_ids: List of token IDs to subscribe to
        """
        if not self.is_connected:
            await self.connect()

        # Filter out already subscribed
        new_assets = [aid for aid in asset_ids if aid not in self._subscribed_assets]
        if not new_assets:
            return

        message = {
            "assets_ids": new_assets,
            "type": "market",
        }

        try:
            await self._ws.send(json.dumps(message))
            self._subscribed_assets.update(new_assets)
            log.debug("Subscribed to assets", count=len(new_assets))
        except Exception as e:
            log.error("Failed to subscribe", error=str(e))
            raise

    async def listen(self) -> None:
        """Listen for incoming messages."""
        if not self.is_connected:
            await self.connect()

        self._running = True

        try:
            async for message in self._ws:
                self._last_message_time = time.time()
                await self._handle_message(message)
        except websockets.ConnectionClosed:
            log.warning("WebSocket connection closed")
        except Exception as e:
            log.error("WebSocket error", error=str(e))
        finally:
            self._running = False

    async def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            # Different message types
            if "book" in data:
                await self._handle_book(data)
            elif "price_change" in data:
                await self._handle_price_change(data)
            elif "error" in data:
                log.error("WebSocket error message", error=data["error"])

        except json.JSONDecodeError:
            log.warning("Invalid JSON message")
        except Exception as e:
            log.error("Message handling error", error=str(e))

    async def _handle_book(self, data: dict) -> None:
        """Handle orderbook snapshot."""
        asset_id = data.get("asset_id")
        if not asset_id:
            return

        # Parse bids and asks
        bids = []
        asks = []

        for bid in data.get("bids", []):
            bids.append(OrderBookLevel(
                price=Decimal(str(bid.get("price", 0))),
                size=Decimal(str(bid.get("size", 0))),
            ))

        for ask in data.get("asks", []):
            asks.append(OrderBookLevel(
                price=Decimal(str(ask.get("price", 0))),
                size=Decimal(str(ask.get("size", 0))),
            ))

        # Sort
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        # Store orderbook
        self._orderbooks[asset_id] = OrderBook(
            asset_id=asset_id,
            bids=bids,
            asks=asks,
            timestamp=time.time(),
        )

        # Create update
        update = OrderBookUpdate(
            asset_id=asset_id,
            bids=bids,
            asks=asks,
            best_bid=bids[0].price if bids else None,
            best_ask=asks[0].price if asks else None,
        )

        if self._on_book:
            self._on_book(update)

    async def _handle_price_change(self, data: dict) -> None:
        """Handle real-time price change."""
        asset_id = data.get("asset_id")
        if not asset_id:
            return

        # Get best prices from cached orderbook
        orderbook = self._orderbooks.get(asset_id)
        best_bid = orderbook.best_bid if orderbook else None
        best_ask = orderbook.best_ask if orderbook else None

        change = PriceChange(
            asset_id=asset_id,
            price=Decimal(str(data.get("price", 0))),
            size=Decimal(str(data.get("size", 0))),
            side=data.get("side", "SELL"),
            best_bid=best_bid,
            best_ask=best_ask,
        )

        if self._on_price_change:
            self._on_price_change(change)

    def get_orderbook(self, asset_id: str) -> Optional[OrderBook]:
        """Get cached orderbook for an asset."""
        return self._orderbooks.get(asset_id)

    async def close(self) -> None:
        """Close WebSocket connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None
            log.info("WebSocket closed")

    async def __aenter__(self) -> "WebSocketClient":
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()