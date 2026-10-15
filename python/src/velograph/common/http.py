"""HTTP client factory and the retry policy for transient network failures."""

from __future__ import annotations

import logging

import httpx
from tenacity import before_sleep_log, retry, retry_if_exception, stop_after_attempt, wait_exponential

USER_AGENT = "VeloGraph/0.1 (INT315 student project)"

_log = logging.getLogger("velograph.http")


def http_client(timeout: float = 30.0) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout, connect=10.0),
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )


def is_transient(exc: BaseException) -> bool:
    """Network hiccups and server-side errors are worth retrying; client errors (4xx) are not."""
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500


def retrying(attempts: int = 6, max_wait: float = 30.0):
    """Decorator: retry transient failures with exponential backoff, then re-raise the last error."""
    return retry(
        retry=retry_if_exception(is_transient),
        stop=stop_after_attempt(attempts),
        wait=wait_exponential(multiplier=1, min=1, max=max_wait),
        before_sleep=before_sleep_log(_log, logging.WARNING),
        reraise=True,
    )
