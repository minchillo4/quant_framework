"""HTTP communication abstractions for adapter plugins.

Separates HTTP transport layer from business logic (validation, error handling).
Allows easy mocking and swapping of HTTP implementations in tests.
"""

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class HttpResponse:
    """HTTP response data container."""

    status_code: int
    body: Any  # JSON-decoded response body
    headers: dict[str, str]
    url: str


class IHttpClient(Protocol):
    """Abstraction for HTTP client.

    Single Responsibility: Execute HTTP requests and return responses.
    Does NOT handle:
    - Response validation
    - Error mapping
    - Retry logic
    - API key injection
    """

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """Execute GET request.

        Args:
            url: Full URL to request
            params: Query parameters
            headers: HTTP headers
            timeout: Request timeout in seconds

        Raises:
            HttpException: On network or connection errors
        """
        ...

    async def post(
        self,
        url: str,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """Execute POST request.

        Args:
            url: Full URL to request
            data: Request body as JSON-serializable dict
            headers: HTTP headers
            timeout: Request timeout in seconds

        Raises:
            HttpException: On network or connection errors
        """
        ...


class IApiKeyProvider(Protocol):
    """Abstraction for API key management.

    Single Responsibility: Provide headers with valid API keys.
    Handles key rotation, expiration, and refresh transparently.
    """

    async def get_headers(self) -> dict[str, str]:
        """Get HTTP headers with valid API keys.

        Returns:
            Dict with Authorization or API-Key headers.

        Raises:
            KeyRotationException: If no valid keys available
        """
        ...

    async def rotate_key_on_failure(self, failed_key: str | None = None) -> None:
        """Signal key rotation after detection of key failure.

        Args:
            failed_key: The key that failed (optional)
        """
        ...
