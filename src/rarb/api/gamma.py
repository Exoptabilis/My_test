"""Gamma API client for Polymarket market data."""

import asyncio
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urljoin

import httpx

from rarb.api.models import Market, Token
from rarb.config import get_settings
from rarb.utils.logging import get_logger

log = get_logger(__name__)


class GammaClient:
    """Client for Polymarket Gamma API (market metadata)."""

    def __init__(self, base_url: Optional[str] = None) -> None:
        settings = get_settings()
        self.base_url = base_url or settings.gamma_base_url
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore = asyncio.Semaphore(10)  # Rate limiting

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            timeout = httpx.Timeout(30.0, connect=10.0)
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=timeout,
                follow_redirects=True,
            )
        return self._client

    async def _get(self, path: str, params: Optional[dict] = None) -> Any:
        """Make GET request with rate limiting."""
        async with self._semaphore:
            client = await self._ensure_client()
            try:
                resp = await client.get(path, params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as e:
                log.error("Gamma API error", path=path, status=e.response.status_code)
                raise
            except Exception as e:
                log.error("Gamma API request failed", path=path, error=str(e))
                raise

    async def get_markets(
        self,
        active: bool = True,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
        **kwargs,
    ) -> list[dict]:
        """Get list of markets from Gamma API."""
        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
            **kwargs,
        }
        return await self._get("/markets", params=params)

    def parse_market(self, data: dict) -> Optional[Market]:
        """Parse raw Gamma API response into Market object."""
        try:
            # Extract token data
            tokens_data = data.get("tokens", [])
            yes_token = None
            no_token = None

            for token_data in tokens_data:
                token = Token(
                    token_id=str(token_data.get("token_id", "")),
                    outcome=token_data.get("outcome", ""),
                )
                if token.outcome.lower() == "yes":
                    yes_token = token
                elif token.outcome.lower() == "no":
                    no_token = token

            # Parse dates
            end_date = None
            if data.get("end_date"):
                try:
                    end_date = datetime.fromisoformat(data["end_date"].replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

            # Get prices
            outcome_prices = data.get("outcomePrices", ["0.5", "0.5"])
            yes_price = float(outcome_prices[0]) if outcome_prices else 0.5
            no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.5

            return Market(
                id=str(data.get("id", "")),
                question=str(data.get("question", "")),
                description=data.get("description"),
                condition_id=str(data.get("condition_id", "")),
                slug=str(data.get("slug", "")),
                resolution_source=data.get("resolutionSource"),
                end_date=end_date,
                liquidity=float(data.get("liquidity", 0)),
                volume=float(data.get("volume", 0)),
                open_interest=float(data.get("openInterest", 0)),
                yes_price=yes_price,
                no_price=no_price,
                yes_token=yes_token,
                no_token=no_token,
                raw_data=data,
            )
        except Exception as e:
            log.debug("Failed to parse market", error=str(e), market_id=data.get("id"))
            return None

    async def fetch_all_active_markets(
        self,
        min_liquidity: float = 0,
        min_volume: float = 0,
        max_days_until_resolution: Optional[int] = None,
    ) -> list[Market]:
        """Fetch all active markets with pagination."""
        all_markets = []
        offset = 0
        limit = 100

        while True:
            try:
                data = await self.get_markets(
                    active=True,
                    closed=False,
                    limit=limit,
                    offset=offset,
                )

                if not data:
                    break

                for item in data:
                    market = self.parse_market(item)
                    if market:
                        # Apply filters
                        if market.liquidity < min_liquidity:
                            continue
                        if market.volume < min_volume:
                            continue
                        if max_days_until_resolution and market.end_date:
                            days = (market.end_date - datetime.now()).days
                            if days > max_days_until_resolution:
                                continue
                        all_markets.append(market)

                if len(data) < limit:
                    break

                offset += limit
                await asyncio.sleep(0.1)  # Rate limiting

            except Exception as e:
                log.error("Failed to fetch markets page", offset=offset, error=str(e))
                break

        log.info("Fetched active markets", count=len(all_markets))
        return all_markets

    async def get_market(self, market_id: str) -> Optional[Market]:
        """Get single market by ID."""
        try:
            data = await self._get(f"/markets/{market_id}")
            return self.parse_market(data)
        except Exception as e:
            log.error("Failed to fetch market", market_id=market_id, error=str(e))
            return None

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "GammaClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()