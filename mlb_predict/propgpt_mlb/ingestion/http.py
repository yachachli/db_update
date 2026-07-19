"""Shared HTTP helpers for ingestion clients.

Provides a single `request_with_retry` function that wraps `requests` with:
- Configurable retry on 429 and 5xx responses
- Exponential backoff with jitter
- Per-request timeout
- Structured logging
"""
from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


class HttpError(RuntimeError):
    """Raised when an HTTP request fails after retries."""

    def __init__(self, message: str, status_code: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: float = 15.0,
    max_retries: int = 4,
    backoff_base: float = 1.0,
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Response:
    """Issue an HTTP request, retrying on transient failures.

    Returns the final Response on success. Raises HttpError on terminal failure.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
        except requests.RequestException as e:
            last_exc = e
            if attempt < max_retries:
                wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Request %s %s failed (%s) — retry %d/%d in %.1fs",
                    method, url, e, attempt, max_retries, wait,
                )
                time.sleep(wait)
                continue
            raise HttpError(f"Request failed after {max_retries} attempts: {e}") from e

        if response.status_code in retry_statuses and attempt < max_retries:
            wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            logger.warning(
                "Got %d on %s %s — retry %d/%d in %.1fs",
                response.status_code, method, url, attempt, max_retries, wait,
            )
            time.sleep(wait)
            continue

        if not response.ok:
            raise HttpError(
                f"HTTP {response.status_code} for {method} {url}: {response.text[:300]}",
                status_code=response.status_code,
                body=response.text,
            )
        return response

    raise HttpError(f"Exhausted retries for {method} {url}: {last_exc}")
