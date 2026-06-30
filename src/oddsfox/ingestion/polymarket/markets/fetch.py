"""API client factory for Polymarket Gamma ingestion."""

from typing import Optional

from oddsfox.config.settings import GAMMA_API_URL, MARKETS_REQUESTS_PER_SECOND
from oddsfox.resources.http import APIClient, RateLimiter


def build_client(requests_per_second: Optional[int] = None) -> APIClient:
    """Create an API client configured for the Gamma endpoint with shared token-bucket limiting."""
    rps = (
        MARKETS_REQUESTS_PER_SECOND
        if requests_per_second is None
        else requests_per_second
    )
    limiter = RateLimiter(float(rps)) if rps and rps > 0 else None
    return APIClient(base_url=GAMMA_API_URL, rate_limiter=limiter)
