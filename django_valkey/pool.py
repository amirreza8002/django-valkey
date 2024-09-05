from typing import Dict, Any
from urllib.parse import parse_qs, urlparse

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from valkey import Valkey
from valkey.connection import ConnectionPool, DefaultParser
from valkey.sentinel import Sentinel
from valkey._parsers.url_parser import to_bool


class ConnectionFactory:
    # Store connection pool by cache backend options.
    #
    # _pools is a process-global, as otherwise _pools is cleared every time
    # ConnectionFactory is instantiated, as Django creates new cache client
    # (DefaultClient) instance for every request.

    _pools: Dict[str, ConnectionPool | Any] = {}

    def __init__(self, options: dict):
        pool_cls_path = options.get(
            "CONNECTION_POOL_CLASS", "valkey.connection.ConnectionPool"
        )
        self.pool_cls: ConnectionPool | Any = import_string(pool_cls_path)
        self.pool_cls_kwargs = options.get("CONNECTION_POOL_KWARGS", {})

        valkey_client_cls_path = options.get(
            "VALKEY_CLIENT_CLASS", "valkey.client.Valkey"
        )
        self.valkey_client_cls: Valkey | Any = import_string(valkey_client_cls_path)
        self.valkey_client_cls_kwargs = options.get("CLIENT_KWARGS", {})

        self.options = options

    def make_connection_params(self, url: str | None) -> dict:
        """
        Given a main connection parameters, build a complete
        dict of connection parameters.
        """

        kwargs = {
            "url": url,
            "parser_class": self.get_parser_cls(),
        }

        socket_timeout = self.options.get("SOCKET_TIMEOUT", None)
        # TODO: do we need to check for existence?
        if socket_timeout:
            if not isinstance(socket_timeout, (int, float)):
                error_message = "Socket timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwargs["socket_timeout"] = socket_timeout

        socket_connect_timeout = self.options.get("SOCKET_CONNECT_TIMEOUT", None)
        if socket_connect_timeout:
            if not isinstance(socket_connect_timeout, (int, float)):
                error_message = "Socket connect timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwargs["socket_connect_timeout"] = socket_connect_timeout

        password = self.options.get("PASSWORD", None)
        if password:
            kwargs["password"] = password

        return kwargs

    def connect(self, url: str) -> Valkey | Any:
        """
        Given a basic connection parameters,
        return a new connection.
        """
        params = self.make_connection_params(url)
        return self.get_connection(params)

    def disconnect(self, connection: Valkey) -> None:
        """
        Given a not null client connection it disconnects from the Valkey server.

        The default implementation uses a pool to hold connections.
        """
        connection.connection_pool.disconnect()

    def get_connection(self, params: dict) -> Valkey | Any:
        """
        Given a now preformatted params, return a
        new connection.

        The default implementation uses a cached pools
        for create new connection.
        """
        pool = self.get_or_create_connection_pool(params)
        return self.valkey_client_cls(
            connection_pool=pool, **self.valkey_client_cls_kwargs
        )

    def get_parser_cls(self):
        cls = self.options.get("PARSER_CLASS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)

    def get_or_create_connection_pool(
        self, params: dict
    ) -> dict[str, ConnectionPool | Any]:
        """
        Given a connection parameters and return a new
        or cached connection pool for them.

        Reimplement this method if you want distinct
        connection pool instance caching behavior.
        """
        key: str = params["url"]
        if key not in self._pools:
            self._pools[key] = self.get_connection_pool(params)
        return self._pools[key]

    def get_connection_pool(self, params: dict) -> ConnectionPool | Any:
        """
        Given a connection parameters, return a new
        connection pool for them.

        Overwrite this method if you want a custom
        behavior on creating connection pool.
        """
        cp_params = params
        cp_params.update(self.pool_cls_kwargs)
        pool = self.pool_cls.from_url(**cp_params)

        if pool.connection_kwargs.get("password", None) is None:
            pool.connection_kwargs["password"] = params.get("password", None)
            pool.reset()

        return pool


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
) -> ConnectionFactory | Any:
    if path is None:
        path = getattr(
            settings,
            "DJANGO_VALKEY_CONNECTION_FACTORY",
            "django_valkey.pool.ConnectionFactory",
        )
    opt_conn_factory = options.get("CONNECTION_FACTORY")
    if opt_conn_factory:
        path = opt_conn_factory

    cls = import_string(path)
    return cls(options or {})
