from inspect import isawaitable

VERSION = (0, 0, 16)
__version__ = ".".join(map(str, VERSION))


def get_valkey_connection(alias="default", write=True):
    """
    Helper used for obtaining a raw valkey client.
    """

    from django.core.cache import caches

    cache = caches[alias]

    error_message = "This backend does not support this feature"
    if not hasattr(cache, "client"):
        raise NotImplementedError(error_message)

    if not hasattr(cache.client, "get_client"):
        raise NotImplementedError(error_message)

    if isawaitable(cache.client.get_client):
        raise "use django_valkey.async_cache.get_valkey_connection for async backends"

    return cache.client.get_client(write)
