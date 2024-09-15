from typing import Any
from urllib.parse import urlparse, parse_qs

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from valkey.asyncio import Valkey as AValkey
from valkey.asyncio.connection import ConnectionPool, DefaultParser
from valkey.asyncio.sentinel import Sentinel
from valkey._parsers.url_parser import to_bool

from django_valkey.base_pool import BaseConnectionPool


class AsyncConnectionFactory(BaseConnectionPool[AValkey, ConnectionPool]):
    path_pool_cls = "valkey.asyncio.connection.ConnectionPool"
    path_base_cls = "valkey.asyncio.client.Valkey"

    async def disconnect(self, connection: type[AValkey] | type) -> None:
        await connection.connection_pool.disconnect()

    def get_parser_cls(self) -> type[DefaultParser] | type:
        cls = self.options.get("PARSER_CLS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)


class AsyncSentinelConnectionFactory(AsyncConnectionFactory):
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
        connection_kwargs = self.make_connection_params(None)
        connection_kwargs.pop("url")
        connection_kwargs.update(self.pool_cls_kwargs)
        self._sentinel = Sentinel(
            self.sentinels,
            sentinel_kwargs=options.get("SENTINEL_KWARGS"),
            **connection_kwargs,
        )

    async def get_connection_pool(self, params: dict) -> ConnectionPool | Any:
        url = urlparse(params["url"])
        cp_params = params
        cp_params.update(service_name=url.hostname, sentinel_manager=self._sentinel)
        pool = await super().get_connection_pool(cp_params)

        is_master = parse_qs(url.query).get("is_master")
        if is_master:
            pool.is_master = to_bool(is_master[0])

        return pool
