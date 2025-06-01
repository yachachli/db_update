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
                response.raise_for_status()

                cache_path.mkdir(parents=True, exist_ok=True)

                data = msgspec.json.decode(response.content, type=ty)
                # data = response.json()
                async with aiofiles.open(cache_file, mode="w") as f:
                    await f.write(response.text)

                return data

        assert Env.API_CACHE_DIR is None

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
        response.raise_for_status()
        data = msgspec.json.decode(response.content, type=ty)
        # data = response.json()
        return data
