from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from django.core.exceptions import ImproperlyConfigured
from valkey.sentinel import SentinelConnectionPool

from django_valkey.client.default import DefaultClient


def replace_query(url, query):
    return urlunparse((*url[:4], urlencode(query, doseq=True), url[5]))


class SentinelClient(DefaultClient):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.SentinelConnectionFactory"

    """
    Sentinel client which uses the single valkey URL specified by the CACHE's
    LOCATION to create a LOCATION configuration for two connection pools; One
    pool for the primaries and another pool for the replicas, and upon
    connecting ensures the connection pool factory is configured correctly.
    """

    def __init__(self, server, params, backend):
        if isinstance(server, str):
            url = urlparse(server)
            primary_query = parse_qs(url.query, keep_blank_values=True)
            replica_query = primary_query
            primary_query["is_master"] = [1]  # type: ignore
            replica_query["is_master"] = [0]  # type: ignore

            server = [replace_query(url, i) for i in (primary_query, replica_query)]

        super().__init__(server, params, backend)

    def connect(self, *args, **kwargs):
        connection = super().connect(*args, **kwargs)
        if not isinstance(connection.connection_pool, SentinelConnectionPool):
            error_message = (
                "Settings DJANGO_VALKEY_CONNECTION_FACTORY or "
                "CACHE[].OPTIONS.CONNECTION_POOL_CLASS is not configured correctly."
            )
            raise ImproperlyConfigured(error_message)

        return connection
