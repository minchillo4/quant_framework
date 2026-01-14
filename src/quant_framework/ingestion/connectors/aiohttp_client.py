"""Concrete HTTP client implementation for async requests.

Wraps aiohttp behind IHttpClient abstraction.
"""

from typing import Any

import aiohttp

from quant_framework.ingestion.config.value_objects import HttpClientConfig
from quant_framework.ingestion.ports.http import (
    HttpResponse,
    IHttpClient,
)


class AiohttpClient(IHttpClient):
    """HTTP client implementation using aiohttp."""

    def __init__(self, config: HttpClientConfig):
        """Initialize HTTP client.

        Args:
            config: HTTP client configuration
        """
        self.config = config
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """Execute GET request.

        Args:
            url: Full URL
            params: Query parameters
            headers: HTTP headers
            timeout: Request timeout override

        Returns:
            HttpResponse with status, body, headers

        Raises:
            aiohttp.ClientError: On connection errors
        """
        session = await self._get_session()
        timeout_obj = aiohttp.ClientTimeout(total=timeout or self.config.timeout)

        async with session.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout_obj,
            ssl=False,  # May need adjustment for production
        ) as resp:
            body = await resp.json() if resp.status == 200 else await resp.text()
            return HttpResponse(
                status_code=resp.status,
                body=body if isinstance(body, dict) else {"error": body},
                headers=dict(resp.headers),
                url=str(resp.url),
            )

    async def post(
        self,
        url: str,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """Execute POST request.

        Args:
            url: Full URL
            data: Request body
            headers: HTTP headers
            timeout: Request timeout override

        Returns:
            HttpResponse with status, body, headers

        Raises:
            aiohttp.ClientError: On connection errors
        """
        session = await self._get_session()
        timeout_obj = aiohttp.ClientTimeout(total=timeout or self.config.timeout)

        async with session.post(
            url,
            json=data,
            headers=headers,
            timeout=timeout_obj,
            ssl=False,
        ) as resp:
            body = await resp.json() if resp.status == 200 else await resp.text()
            return HttpResponse(
                status_code=resp.status,
                body=body if isinstance(body, dict) else {"error": body},
                headers=dict(resp.headers),
                url=str(resp.url),
            )

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
