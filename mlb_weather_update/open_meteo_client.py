"""Open-Meteo client — minimal subset for mlb_weather_update.

Free, no API key. Mirrors the retry/backoff style of the sibling MLB clients.

Two compatible endpoints share the same hourly-variable shape:
  - forecast (used here):  https://api.open-meteo.com/v1/forecast
  - historical (later):    https://archive-api.open-meteo.com/v1/archive

The base URL + endpoint are parameterizable so the later historical backfill can
reuse this client unchanged, but this step only ever calls the forecast path.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

logger = logging.getLogger("mlb_weather_update.open_meteo_client")

FORECAST_BASE_URL = "https://api.open-meteo.com/v1"
ARCHIVE_BASE_URL = "https://archive-api.open-meteo.com/v1"  # historical — not used in this step

# Hourly variables we request. Fahrenheit + mph units set via query params below.
HOURLY_VARS = (
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation_probability",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "cloud_cover",
)

HTTP_TIMEOUT_SEC = 20.0
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_BASE_SEC = 1.0
RETRY_STATUSES = (429, 500, 502, 503, 504)


class OpenMeteoError(RuntimeError):
    pass


def _http_get(url: str, params: dict[str, Any]) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=HTTP_TIMEOUT_SEC)
        except requests.RequestException as e:
            last_exc = e
            if attempt < HTTP_MAX_RETRIES:
                wait = HTTP_BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning("GET %s failed (%s) — retry %d/%d in %.1fs",
                               url, e, attempt, HTTP_MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            raise OpenMeteoError(f"GET {url} failed after {HTTP_MAX_RETRIES} attempts: {e}") from e

        if response.status_code in RETRY_STATUSES and attempt < HTTP_MAX_RETRIES:
            wait = HTTP_BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            logger.warning("GET %s returned %d — retry %d/%d in %.1fs",
                           url, response.status_code, attempt, HTTP_MAX_RETRIES, wait)
            time.sleep(wait)
            continue

        if not response.ok:
            raise OpenMeteoError(f"HTTP {response.status_code} for GET {url}: {response.text[:300]}")
        return response.json()

    raise OpenMeteoError(f"Exhausted retries for {url}: {last_exc}")


class OpenMeteoClient:
    """Thin client. No auth. Base URL + endpoint parameterizable for later reuse."""

    def __init__(self, base_url: str = FORECAST_BASE_URL, endpoint: str = "forecast") -> None:
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint

    def get_hourly(self, latitude: float, longitude: float, start_date: str, end_date: str) -> dict[str, Any]:
        """Hourly forecast for a lat/long over [start_date, end_date] (YYYY-MM-DD, UTC).

        Returns the raw payload; `hourly` holds parallel arrays keyed by variable, with
        `hourly.time` as UTC ISO-8601 strings (timezone=UTC requested).
        """
        payload = _http_get(
            f"{self.base_url}/{self.endpoint}",
            {
                "latitude": latitude,
                "longitude": longitude,
                "hourly": ",".join(HOURLY_VARS),
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "timezone": "UTC",
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        if not isinstance(payload, dict) or "hourly" not in payload:
            raise OpenMeteoError(f"Unexpected forecast shape for ({latitude},{longitude})")
        return payload
