"""Async CLOB client for Polymarket with HTTP/2 and EIP-712 signing."""

import asyncio
import time
from decimal import Decimal
from typing import Any, Optional, Union
from urllib.parse import urljoin

import httpx
from eth_account import Account
from eth_account.messages import encode_typed_data

from rarb.api.models import OrderBook, OrderBookLevel, OrderResult
from rarb.config import get_settings
from rarb.utils.logging import get_logger
from rarb.utils.signer import Signer

log = get_logger(__name__)


class AsyncClobClient:
    """
    Async CLOB client with HTTP/2 and native EIP-712 signing.

    Features:
    - HTTP/2 for multiplexing
    - Connection pooling
    - SOCKS5 proxy support (for geo-restriction bypass)
    - Parallel order signing
    - Pre-signed order templates (optimization)
    """

    def __init__(
        self,
        signer: Optional[Signer] = None,
        base_url: Optional[str] = None,
        proxy_url: Optional[str] = None,
    ) -> None:
        settings = get_settings()
        self.base_url = base_url or settings.clob_base_url
        self.signer = signer or Signer()
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore = asyncio.Semaphore(20)  # Rate limiting
        self._neg_risk_cache: dict[str, bool] = {}  # token_id -> is_neg_risk

        # Track stats
        self._request_count = 0
        self._signing_time_ms = 0
        self._request_time_ms = 0

        # Configure proxy if provided
        self.proxy_url = proxy_url or settings.get_socks5_proxy_url()

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Get or create HTTP/2 client with connection pooling."""
        if self._client is None:
            limits = httpx.Limits(
                max_keepalive_connections=20,
                max_connections=50,
            )
            timeout = httpx.Timeout(15.0, connect=5.0)

            client_kwargs = {
                "base_url": self.base_url,
                "timeout": timeout,
                "limits": limits,
                "http2": True,  # Enable HTTP/2
            }

            if self.proxy_url:
                client_kwargs["proxy"] = self.proxy_url
                log.info("Using proxy for CLOB client", proxy=self.proxy_url)

            self._client = httpx.AsyncClient(**client_kwargs)

        return self._client

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> Any:
        """Make authenticated request with rate limiting."""
        async with self._semaphore:
            client = await self._ensure_client()
            url = urljoin(self.base_url, path)

            start = time.time()
            self._request_count += 1

            try:
                # Add authentication headers if we have credentials
                if self.signer.has_api_creds:
                    headers = kwargs.pop("headers", {})
                    timestamp = str(int(time.time() * 1000))
                    signature = self.signer.sign_request(
                        method=method,
                        path=path,
                        timestamp=timestamp,
                    )
                    headers.update({
                        "POLY_API-KEY": self.signer.api_key,
                        "POLY_API-SIGNATURE": signature,
                        "POLY_API-TIMESTAMP": timestamp,
                    })
                    kwargs["headers"] = headers

                resp = await client.request(method, url, **kwargs)
                resp.raise_for_status()

                duration = (time.time() - start) * 1000
                self._request_time_ms = (self._request_time_ms * (self._request_count - 1) + duration) / self._request_count

                return resp.json()

            except httpx.HTTPStatusError as e:
                log.error("CLOB API error", path=path, status=e.response.status_code)
                raise
            except Exception as e:
                log.error("CLOB request failed", path=path, error=str(e))
                raise

    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Get orderbook for a token."""
        data = await self._request("GET", f"/book", params={"token_id": token_id})

        bids = []
        for b in data.get("bids", []):
            bids.append(OrderBookLevel(
                price=Decimal(str(b.get("price", 0))),
                size=Decimal(str(b.get("size", 0))),
            ))

        asks = []
        for a in data.get("asks", []):
            asks.append(OrderBookLevel(
                price=Decimal(str(a.get("price", 0))),
                size=Decimal(str(a.get("size", 0))),
            ))

        return OrderBook(
            asset_id=token_id,
            bids=bids,
            asks=asks,
            timestamp=time.time(),
        )

    async def get_neg_risk(self, token_id: str) -> bool:
        """Check if token is negative risk."""
        # Check cache first
        if token_id in self._neg_risk_cache:
            return self._neg_risk_cache[token_id]

        try:
            data = await self._request("GET", f"/neg-risk/{token_id}")
            is_neg_risk = data.get("neg_risk", False)
            self._neg_risk_cache[token_id] = is_neg_risk
            return is_neg_risk
        except Exception:
            # Default to False if endpoint fails
            return False

    async def prefetch_neg_risk(self, token_ids: list[str]) -> None:
        """Pre-fetch neg_risk status for multiple tokens."""
        tasks = [self.get_neg_risk(tid) for tid in token_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Pre-fetched neg_risk status", count=len(token_ids))

    def _sign_order(self, order_data: dict) -> str:
        """Sign order using EIP-712."""
        start = time.time()

        # EIP-712 domain for Polymarket
        domain = {
            "name": "Polymarket CTF Exchange",
            "version": "1",
            "chainId": 137,
            "verifyingContract": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
        }

        # Order types
        types = {
            "Order": [
                {"name": "salt", "type": "uint256"},
                {"name": "maker", "type": "address"},
                {"name": "signer", "type": "address"},
                {"name": "taker", "type": "address"},
                {"name": "tokenId", "type": "uint256"},
                {"name": "makerAmount", "type": "uint256"},
                {"name": "takerAmount", "type": "uint256"},
                {"name": "expiration", "type": "uint256"},
                {"name": "nonce", "type": "uint256"},
                {"name": "feeRateBps", "type": "uint256"},
                {"name": "side", "type": "uint8"},
                {"name": "signatureType", "type": "uint8"},
            ]
        }

        # Create signable message
        signable = encode_typed_data(domain, types, order_data)

        # Sign with private key
        private_key = self.signer.private_key.get_secret_value()
        signed = Account.sign_message(signable, private_key)

        duration = (time.time() - start) * 1000
        self._signing_time_ms = (self._signing_time_ms * (self._request_count - 1) + duration) / self._request_count

        return signed.signature.hex()

    async def place_order(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        *,
        expiration: int = 0,
        wait_for_fill: bool = True,
        timeout_seconds: int = 10,
    ) -> OrderResult:
        """
        Place an order on CLOB.

        Args:
            token_id: Token ID to trade
            side: "BUY" or "SELL"
            price: Limit price
            size: Number of shares
            expiration: Expiration timestamp (0 = no expiry)
            wait_for_fill: Wait for order to fill
            timeout_seconds: How long to wait for fill

        Returns:
            OrderResult with order details
        """
        # Convert price/size to integer amounts (6 decimals for USDC)
        maker_amount = int(size * 1_000_000)  # USDC amount
        taker_amount = int(size * price * 1_000_000)  # Token amount

        # Get nonce from signer
        nonce = self.signer.get_nonce()

        # Prepare order data
        order_data = {
            "salt": int(time.time() * 1000),  # Use timestamp as salt
            "maker": self.signer.address,
            "signer": self.signer.address,
            "taker": "0x0000000000000000000000000000000000000000",  # Anyone can take
            "tokenId": int(token_id, 16) if token_id.startswith("0x") else int(token_id),
            "makerAmount": maker_amount,
            "takerAmount": taker_amount,
            "expiration": expiration,
            "nonce": nonce,
            "feeRateBps": 0,
            "side": 0 if side.upper() == "BUY" else 1,
            "signatureType": 2,
        }

        # Sign order
        signature = self._sign_order(order_data)

        # Prepare order payload
        payload = {
            "order": {
                **order_data,
                "signature": signature,
            },
            "owner": self.signer.address,
        }

        # Submit order
        data = await self._request("POST", "/order", json=payload)

        order_id = data.get("id")
        if not order_id:
            raise Exception("No order ID in response")

        result = OrderResult(
            order_id=order_id,
            token_id=token_id,
            side=side.upper(),
            price=price,
            size=size,
            status="OPEN",
            filled_size=Decimal("0"),
        )

        # Wait for fill if requested
        if wait_for_fill:
            result = await self._wait_for_fill(order_id, timeout_seconds)

        return result

    async def _wait_for_fill(self, order_id: str, timeout: int) -> OrderResult:
        """Wait for order to fill or timeout."""
        start = time.time()
        check_interval = 0.5  # 500ms

        while time.time() - start < timeout:
            try:
                data = await self._request("GET", f"/orders/{order_id}")

                status = data.get("status", "OPEN")
                filled_size = Decimal(str(data.get("filled_size", 0)))

                if status == "FILLED" or filled_size > 0:
                    return OrderResult(
                        order_id=order_id,
                        token_id=data.get("token_id", ""),
                        side=data.get("side", "BUY"),
                        price=Decimal(str(data.get("price", 0))),
                        size=Decimal(str(data.get("size", 0))),
                        status=status,
                        filled_size=filled_size,
                        transaction_hash=data.get("transaction_hash"),
                    )

                await asyncio.sleep(check_interval)

            except Exception as e:
                log.debug("Error checking order status", order_id=order_id, error=str(e))
                await asyncio.sleep(check_interval)

        # Timeout - cancel order
        await self.cancel_order(order_id)
        raise TimeoutError(f"Order {order_id} not filled within {timeout}s")

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order."""
        try:
            await self._request("DELETE", f"/orders/{order_id}")
            return True
        except Exception as e:
            log.error("Failed to cancel order", order_id=order_id, error=str(e))
            return False

    async def cancel_orders(self, token_id: Optional[str] = None) -> int:
        """Cancel all open orders (optionally for specific token)."""
        params = {}
        if token_id:
            params["token_id"] = token_id

        data = await self._request("DELETE", "/orders", params=params)
        return data.get("cancelled", 0)

    async def get_open_orders(self) -> list[OrderResult]:
        """Get all open orders."""
        data = await self._request("GET", "/orders")

        orders = []
        for item in data:
            orders.append(OrderResult(
                order_id=item.get("id", ""),
                token_id=item.get("token_id", ""),
                side=item.get("side", "BUY"),
                price=Decimal(str(item.get("price", 0))),
                size=Decimal(str(item.get("size", 0))),
                status=item.get("status", "OPEN"),
                filled_size=Decimal(str(item.get("filled_size", 0))),
            ))

        return orders

    async def get_positions(self) -> list[dict]:
        """Get current positions."""
        return await self._request("GET", "/positions")

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def get_stats(self) -> dict:
        """Get client statistics."""
        return {
            "requests": self._request_count,
            "avg_signing_time_ms": round(self._signing_time_ms, 2),
            "avg_request_time_ms": round(self._request_time_ms, 2),
            "neg_risk_cache_size": len(self._neg_risk_cache),
        }

    async def __aenter__(self) -> "AsyncClobClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()