import asyncio
import random
import ssl
import types
import typing

import httpx
import msgspec.json
from httpx._client import USE_CLIENT_DEFAULT, EventHook, UseClientDefault
from httpx._config import (
    DEFAULT_LIMITS,
    DEFAULT_MAX_REDIRECTS,
    DEFAULT_TIMEOUT_CONFIG,
    Limits,
)
from httpx._transports.base import AsyncBaseTransport
from httpx._types import (
    AuthTypes,
    CertTypes,
    CookieTypes,
    HeaderTypes,
    ProxyTypes,
    QueryParamTypes,
    RequestExtensions,
    TimeoutTypes,
)
from httpx._urls import URL

from db_update.env import Env
from db_update.logger import logger

T = typing.TypeVar("T")
U = typing.TypeVar("U", bound="AsyncCachingClient")


class AsyncCachingClient:
    __slots__ = ("_client",)

    _client: httpx.AsyncClient

    def __init__(
        self,
        *,
        auth: AuthTypes | None = None,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        cookies: CookieTypes | None = None,
        verify: ssl.SSLContext | str | bool = True,
        cert: CertTypes | None = None,
        http1: bool = True,
        http2: bool = False,
        proxy: ProxyTypes | None = None,
        mounts: None | (typing.Mapping[str, AsyncBaseTransport | None]) = None,
        timeout: TimeoutTypes = DEFAULT_TIMEOUT_CONFIG,
        follow_redirects: bool = False,
        limits: Limits = DEFAULT_LIMITS,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        event_hooks: None | (typing.Mapping[str, list[EventHook]]) = None,
        base_url: URL | str = "",
        transport: AsyncBaseTransport | None = None,
        trust_env: bool = True,
        default_encoding: str | typing.Callable[[bytes], str] = "utf-8",
    ):
        self._client = httpx.AsyncClient(
            auth=auth,
            params=params,
            headers=headers,
            cookies=cookies,
            verify=verify,
            cert=cert,
            http1=http1,
            http2=http2,
            proxy=proxy,
            mounts=mounts,
            timeout=timeout,
            follow_redirects=follow_redirects,
            limits=limits,
            max_redirects=max_redirects,
            event_hooks=event_hooks,
            base_url=base_url,
            transport=transport,
            trust_env=trust_env,
            default_encoding=default_encoding,
        )

    async def __aenter__(self: U) -> U:
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def get(
        self,
        url: URL | str,
        *,
        cache_key: str,
        ty: typing.Type[T],
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        cookies: CookieTypes | None = None,
        auth: AuthTypes | UseClientDefault | None = USE_CLIENT_DEFAULT,
        follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
        timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        extensions: RequestExtensions | None = None,
    ) -> T:
        async def request_with_retries() -> httpx.Response:
            last_exc: Exception | None = None

            for attempt in range(1, 7):
                try:
                    response = await self._client.get(
                        url,
                        params=params,
                        headers=headers,
                        cookies=cookies,
                        auth=auth,
                        follow_redirects=follow_redirects,
                        timeout=timeout,
                        extensions=extensions,
                    )
                except httpx.HTTPError as exc:
                    last_exc = exc
                    wait_s = min(2**attempt, 10) + random.uniform(0, 0.25)
                    logger.warning(
                        f"HTTP error fetching {url}: {exc}. Retrying in {wait_s:.2f}s "
                        f"(attempt {attempt}/6)"
                    )
                    if attempt >= 6:
                        break
                    await asyncio.sleep(wait_s)
                    continue

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        wait_s = (
                            float(retry_after)
                            if retry_after is not None
                            else min(2**attempt, 30)
                        )
                    except ValueError:
                        wait_s = min(2**attempt, 30)
                    wait_s += random.uniform(0, 0.25)

                    quota_info = ""
                    remaining = (
                        response.headers.get("x-ratelimit-requests-remaining")
                        or response.headers.get("X-RateLimit-Remaining")
                    )
                    limit = (
                        response.headers.get("x-ratelimit-requests-limit")
                        or response.headers.get("X-RateLimit-Limit")
                    )
                    if remaining or limit:
                        quota_info = f" (remaining: {remaining or 'unknown'} / {limit or 'unknown'})"

                    logger.warning(
                        f"429 Too Many Requests from {url}{quota_info}. "
                        f"Retrying in {wait_s:.2f}s (attempt {attempt}/6)"
                    )
                    last_exc = httpx.HTTPStatusError(
                        f"429 Too Many Requests after {attempt} attempts{quota_info}",
                        request=response.request,
                        response=response,
                    )
                    if attempt >= 6:
                        break
                    await asyncio.sleep(wait_s)
                    continue

                if response.status_code == 403:
                    quota_info = ""
                    remaining = (
                        response.headers.get("x-ratelimit-requests-remaining")
                        or response.headers.get("X-RateLimit-Remaining")
                    )
                    limit = (
                        response.headers.get("x-ratelimit-requests-limit")
                        or response.headers.get("X-RateLimit-Limit")
                    )
                    if remaining or limit:
                        quota_info = f" (remaining: {remaining or 'unknown'} / {limit or 'unknown'})"

                    wait_s = min(30 * attempt, 180) + random.uniform(0, 1)
                    logger.warning(
                        f"403 Forbidden from {url}{quota_info}. "
                        f"Retrying in {wait_s:.2f}s (attempt {attempt}/6)"
                    )
                    last_exc = httpx.HTTPStatusError(
                        "403 Forbidden after repeated attempts. This may indicate quota "
                        "exhaustion or a RapidAPI subscription issue.",
                        request=response.request,
                        response=response,
                    )
                    if attempt >= 6:
                        break
                    await asyncio.sleep(wait_s)
                    continue

                if 500 <= response.status_code < 600:
                    wait_s = min(2**attempt, 20) + random.uniform(0, 0.25)
                    logger.warning(
                        f"{response.status_code} from {url}. Retrying in {wait_s:.2f}s "
                        f"(attempt {attempt}/6)"
                    )
                    last_exc = httpx.HTTPStatusError(
                        f"{response.status_code} server error after {attempt} attempts",
                        request=response.request,
                        response=response,
                    )
                    if attempt >= 6:
                        break
                    await asyncio.sleep(wait_s)
                    continue

                response.raise_for_status()
                return response

            assert last_exc is not None
            raise last_exc

        if Env.API_CACHE_DIR is not None:
            from pathlib import Path
            from urllib.parse import urlparse

            import aiofiles

            url_str = str(url)
            parsed_url = urlparse(url_str)

            host_dir = parsed_url.netloc.replace(":", "_")  # Handle ports in hostname
            path_segments = parsed_url.path.strip("/").split("/")

            cache_path = Path(Env.API_CACHE_DIR) / host_dir / "/".join(path_segments)

            cache_file = cache_path / f"{cache_key}.json"

            try:
                async with aiofiles.open(cache_file, mode="r") as f:
                    data = msgspec.json.decode(await f.read(), type=ty)
                    return data
            except Exception:
                response = await request_with_retries()

                cache_path.mkdir(parents=True, exist_ok=True)

                data = msgspec.json.decode(response.content, type=ty)
                # data = response.json()
                async with aiofiles.open(cache_file, mode="w") as f:
                    await f.write(response.text)

                return data

        assert Env.API_CACHE_DIR is None

        response = await request_with_retries()
        data = msgspec.json.decode(response.content, type=ty)
        # data = response.json()
        return data
