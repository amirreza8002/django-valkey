from typing import Any
from urllib.parse import parse_qs, urlparse

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from valkey import Valkey
from valkey.connection import ConnectionPool, DefaultParser
from valkey.sentinel import Sentinel
from valkey._parsers.url_parser import to_bool

from django_valkey.base_pool import BaseConnectionFactory, Base


class ConnectionFactory(BaseConnectionFactory[Valkey, ConnectionPool]):
    path_pool_cls = "valkey.connection.ConnectionPool"
    path_base_cls = "valkey.client.Valkey"

    def disconnect(self, connection: type[Valkey] | type) -> None:
        """
        Given a not null client connection it disconnects from the Valkey server.

        The default implementation uses a pool to hold connections.
        """
        connection.connection_pool.disconnect()

    def get_parser_cls(self) -> type[DefaultParser] | type:
        cls = self.options.get("PARSER_CLASS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)

    def connect(self, url: str) -> Valkey | Any:
        params = self.make_connection_params(url)
        return self.get_connection(params)

    def get_connection(self, params: dict) -> Base | Any:
        pool = self.get_or_create_connection_pool(params)
        return self.base_client_cls(connection_pool=pool, **self.base_client_cls_kwargs)


class SentinelConnectionFactory(ConnectionFactory):
    def __init__(self, options: dict):
        # allow overriding the default SentinelConnectionPool class
        options.setdefault(
            "CONNECTION_POOL_CLASS", "valkey.sentinel.SentinelConnectionPool"
        )
        super().__init__(options)

        sentinels = options.get("SENTINELS")
        if not sentinels:
            error_message = "SENTINELS must be provided as a list of (host, port)."
            raise ImproperlyConfigured(error_message)

        # provide the connection pool kwargs to the sentinel in case it
        # needs to use the socket options for the sentinels themselves
        connection_kwargs = self.make_connection_params(None)
        connection_kwargs.pop("url")
        connection_kwargs.update(self.pool_cls_kwargs)
        self._sentinel = Sentinel(
            sentinels,
            sentinel_kwargs=options.get("SENTINEL_KWARGS"),
            **connection_kwargs,
        )

    def get_connection_pool(self, params: dict) -> ConnectionPool | Any:
        """
        Given a connection parameters, return a new sentinel connection pool
        for them.
        """
        url = urlparse(params["url"])

        # explicitly set service_name and sentinel_manager for the
        # SentinelConnectionPool constructor since will be called by from_url
        cp_params = params
        cp_params.update(service_name=url.hostname, sentinel_manager=self._sentinel)
        pool = super().get_connection_pool(cp_params)

        # convert "is_master" to a boolean if set on the URL, otherwise if not
        # provided it defaults to True.
        is_master: list[str] = parse_qs(url.query).get("is_master")
        if is_master:
            pool.is_master = to_bool(is_master[0])

        return pool


def get_connection_factory(
    path: str | None = None, options: dict | None = None
) -> ConnectionFactory | SentinelConnectionFactory | Any:

    path = getattr(settings, "DJANGO_VALKEY_CONNECTION_FACTORY", path)
    opt_conn_factory = options.get("CONNECTION_FACTORY")
    if opt_conn_factory:
        path = opt_conn_factory

    cls = import_string(path)
    return cls(options)
