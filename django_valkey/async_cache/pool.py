from typing import Any
from unittest.mock import sentinel
from urllib.parse import urlparse, parse_qs

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from valkey.asyncio import Valkey as AValkey
from valkey.asyncio.connection import ConnectionPool, DefaultParser
from valkey.asyncio.sentinel import Sentinel
from valkey._parsers.url_parser import to_bool


class AsyncConnectionFactory:
    _pools: dict[str, ConnectionPool | Any] = {}

    @classmethod
    async def create(cls, options: dict):
        return cls(options)

    def __init__(self, options: dict):
        pool_cls_path = options.get(
            "CONNECTION_POOL_CLASS", "valkey.asyncio.connection.ConnectionPool"
        )

        self.pool_cls: type[ConnectionPool] | type = import_string(pool_cls_path)
        self.pool_cls_kwargs: dict = options.get("CONNECTION_POOL_CLS_KWARGS", {})

        base_client_cls_path = options.get(
            "BASE_CLIENT_CLASS", "valkey.asyncio.client.Valkey"
        )
        self.base_client_cls: type[AValkey] | type = import_string(base_client_cls_path)
        self.base_client_cls_kwargs = options.get("BASE_CLIENT_CLS_KWARGS", {})

        self.options = options

    async def make_connection_params(self, url: str | None) -> dict:
        kwarg = {
            "url": url,
            "parser_class": await self.get_parser_cls(),
        }

        socket_timeout = self.options.get("SOCKET_TIMEOUT", None)
        if socket_timeout:
            if not isinstance(socket_timeout, (int, float)):
                error_message = "Socket timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwarg["socket_timeout"] = socket_timeout

        socket_connect_timeout = self.options.get("SOCKET_CONNECT_TIMEOUT", None)
        if socket_connect_timeout:
            if not isinstance(socket_connect_timeout, (int, float)):
                error_message = "Socket connect timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwarg["socket_connect_timeout"] = socket_connect_timeout

        password = self.options.get("PASSWORD", None)
        if password:
            kwarg["password"] = password

        return kwarg

    async def connect(self, url: str) -> AValkey | Any:
        params = await self.make_connection_params(url)
        return await self.get_connection(params)

    async def disconnect(self, connection: type[AValkey] | type) -> None:
        await connection.connection_pool.disconnect()

    async def get_connection(self, params: dict) -> AValkey | Any:
        pool = await self.get_or_create_connection_pool(params)
        return self.base_client_cls(connection_pool=pool, **self.base_client_cls_kwargs)

    async def get_parser_cls(self) -> type[DefaultParser] | type:
        cls = self.options.get("PARSER_CLS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)

    async def get_or_create_connection_pool(self, params: dict) -> ConnectionPool:
        key = params["url"]
        if key not in self._pools:
            self._pools[key] = await self.get_connection_pool(params)
        return self._pools[key]

    async def get_connection_pool(self, params: dict) -> ConnectionPool:
        cp_params = params
        cp_params.update(self.pool_cls_kwargs)
        pool = self.pool_cls.from_url(**cp_params)

        if pool.connection_kwargs.get("password", None) is None:
            pool.connection_kwargs["password"] = params.get("password", None)
            pool.reset()

        return pool


class AsyncSentinelConnectionFactory(AsyncConnectionFactory):
    @classmethod
    async def create(cls, options: dict):
        self = cls(options)
        connection_kwargs = await self.make_connection_params(None)
        connection_kwargs.pop("url")
        connection_kwargs.update(self.pool_cls_kwargs)
        self._sentinel = Sentinel(
            self.sentinels,
            sentinel_kwargs=options.get("SENTINEL_KWARGS"),
            **connection_kwargs,
        )
        return self

    def __init__(self, options: dict):
        try:
            self.sentinels = options["SENTINELS"]
        except KeyError:
            e = "SENTINELS must be provided as a list of (host, port)."
            raise ImproperlyConfigured(e)

        options.setdefault(
            "CONNECTION_POOL_CLASS", "valkey.asyncio.sentinel.SentinelConnectionPool"
        )
        super().__init__(options)

    async def get_connection_pool(self, params: dict) -> ConnectionPool | Any:
        url = urlparse(params["url"])
        cp_params = params
        cp_params.update(service_name=url.hostname, sentinel_manager=self._sentinel)
        pool = await super().get_connection_pool(cp_params)

        is_master = parse_qs(url.query).get("is_master")
        if is_master:
            pool.is_master = to_bool(is_master[0])

        return pool


async def get_connection_factory(
    path: str | None = None, options: dict | None = None
) -> AsyncConnectionFactory | AsyncSentinelConnectionFactory | Any:
    if path is None:
        path = getattr(
            sentinel,
            "DJANGO_VALKEY_CONNECTION_FACTORY",
            "django_valkey.async.pool.AsyncConnectionFactory",
        )

    opt_conn_factory = options.get("CONNECTION_FACTORY")
    if opt_conn_factory:
        path = opt_conn_factory

    cls = import_string(path)
    return cls.create(options)
