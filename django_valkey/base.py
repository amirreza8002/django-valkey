import builtins
from asyncio import iscoroutinefunction
import contextlib
import functools
import logging
from typing import (
    Any,
    TypeVar,
    Generic,
    Iterator,
    AsyncGenerator,
    Callable,
    TYPE_CHECKING,
)

from django.conf import settings
from django.core.cache.backends.base import get_key_func
from django.utils.module_loading import import_string

from django_valkey.exceptions import ConnectionInterrupted

if TYPE_CHECKING:
    from valkey.lock import Lock
    from valkey.asyncio.lock import Lock as ALock

Client = TypeVar("Client")
Backend = TypeVar("Backend")

CONNECTION_INTERRUPTED = object()
DEFAULT_TIMEOUT = object()
ATTR_DOES_NOT_EXIST = object()


def omit_exception(method: Callable | None = None, return_value: Any | None = None):
    """
    Simple decorator that intercepts connection
    errors and ignores these if settings specify this.
    """

    if method is None:
        return functools.partial(omit_exception, return_value=return_value)

    @functools.wraps(method)
    def _decorator(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except ConnectionInterrupted as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions:
                    self.logger.exception("Exception ignored")

                return return_value
            raise e.__cause__  # noqa: B904

    @functools.wraps(method)
    async def _async_decorator(self, *args, **kwargs):
        try:
            return await method(self, *args, **kwargs)
        except ConnectionInterrupted as e:
            if self._ignore_exceptions:
                if self._log_ignored_exceptions:
                    self.logger.exception("Exception ignored")
                return return_value
            raise e.__cause__

    return _async_decorator if iscoroutinefunction(method) else _decorator


class BaseValkeyCache(Generic[Client, Backend]):
    _missing_key = object()

    def __init__(self, server: str, params: dict[str, Any]) -> None:
        timeout = params.get("timeout", params.get("TIMEOUT", 300))
        if timeout is not None:
            try:
                timeout = int(timeout)
            except (ValueError, TypeError):
                timeout = 300
        self.default_timeout = timeout

        options = params.get("OPTIONS", {})
        max_entries = params.get("max_entries", options.get("MAX_ENTRIES", 300))
        try:
            self._max_entries = int(max_entries)
        except (ValueError, TypeError):
            self._max_entries = 300

        cull_frequency = params.get("cull_frequency", options.get("CULL_FREQUENCY", 3))
        try:
            self._cull_frequency = int(cull_frequency)
        except (ValueError, TypeError):
            self._cull_frequency = 3

        self.key_prefix = params.get("KEY_PREFIX", "")
        self.version = params.get("VERSION", 1)
        self.key_func = get_key_func(params.get("KEY_FUNCTION"))

        self._server = server
        self._params = params
        self._default_scan_itersize = getattr(
            settings, "DJANGO_VALKEY_SCAN_ITERSIZE", 10
        )

        options: dict = params.get("OPTIONS", {})
        self._client_cls = self.get_client_class()
        self._client = None

        self._ignore_exceptions = options.get(
            "IGNORE_EXCEPTIONS",
            getattr(settings, "DJANGO_VALKEY_IGNORE_EXCEPTIONS", False),
        )
        self._log_ignored_exceptions = options.get(
            "LOG_IGNORE_EXCEPTIONS",
            getattr(settings, "DJANGO_VALKEY_LOG_IGNORED_EXCEPTIONS", False),
        )
        self.logger = (
            logging.getLogger(getattr(settings, "DJANGO_VALKEY_LOGGER", __name__))
            if self._log_ignored_exceptions
            else None
        )

    def get_client_class(self) -> type[Client] | type:
        options = self._params.get("OPTIONS", {})
        _client_cls = options.get("CLIENT_CLASS", self.DEFAULT_CLIENT_CLASS)
        return import_string(_client_cls)

    @property
    def client(self) -> Client:
        """
        Lazy client connection property.
        """
        if self._client is None:
            self._client = self._client_cls(self._server, self._params, self)
        return self._client


class BackendCommands:
    @omit_exception
    def set(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.set(*args, **kwargs)

    @omit_exception
    def incr_version(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.incr_version(*args, **kwargs)

    @omit_exception
    def add(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.add(*args, **kwargs)

    def get(self, key, default=None, version=None, client=None) -> Any:
        value = self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    def _get(self: BaseValkeyCache, key, default, version, client) -> Any:
        return self.client.get(key, default=default, version=version, client=client)

    def get_or_set(self, key, default, timeout=DEFAULT_TIMEOUT, version=None) -> Any:
        """
        Fetch a given key from the cache. If the key does not exist,
        add the key and set it to the default value. The default value can
        also be any callable. If timeout is given, use that timeout for the
        key; otherwise use the default cache timeout.

        Return the value of the key stored or retrieved.
        """
        val = self.get(key, self._missing_key, version=version)
        if val is self._missing_key:
            if callable(default):
                default = default()
            self.add(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # added a value between the first get() and the add() above.
            return self.get(key, default, version=version)
        return val

    @omit_exception
    def delete(self: BaseValkeyCache, *args, **kwargs) -> int:
        result = self.client.delete(*args, **kwargs)
        return bool(result)

    @omit_exception
    def delete_many(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.delete_many(*args, **kwargs)

    @omit_exception
    def delete_pattern(self: BaseValkeyCache, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return self.client.delete_pattern(*args, **kwargs)

    @omit_exception
    def clear(self: BaseValkeyCache) -> bool:
        return self.client.clear()

    @omit_exception(return_value={})
    def get_many(self: BaseValkeyCache, *args, **kwargs) -> dict:
        return self.client.get_many(*args, **kwargs)

    @omit_exception
    def set_many(self: BaseValkeyCache, *args, **kwargs) -> None:
        return self.client.set_many(*args, **kwargs)

    @omit_exception
    def incr(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.incr(*args, **kwargs)

    @omit_exception
    def decr(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.decr(*args, **kwargs)

    @omit_exception
    def has_key(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.has_key(*args, **kwargs)

    @omit_exception
    def keys(self: BaseValkeyCache, *args, **kwargs) -> list[Any]:
        return self.client.keys(*args, **kwargs)

    @omit_exception
    def iter_keys(self: BaseValkeyCache, *args, **kwargs) -> Iterator[Any]:
        return self.client.iter_keys(*args, **kwargs)

    @omit_exception
    def close(self: BaseValkeyCache) -> None:
        self.client.close()

    @omit_exception
    def touch(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.touch(*args, **kwargs)

    @omit_exception
    def mset(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.mset(*args, **kwargs)

    @omit_exception
    def mget(self: BaseValkeyCache, *args, **kwargs) -> dict | list[Any]:
        return self.client.mget(*args, **kwargs)

    @omit_exception
    def persist(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.persist(*args, **kwargs)

    @omit_exception
    def expire(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.expire(*args, **kwargs)

    @omit_exception
    def expire_at(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.expire_at(*args, **kwargs)

    @omit_exception
    def pexpire(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.pexpire(*args, **kwargs)

    @omit_exception
    def pexpire_at(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.pexpire_at(*args, **kwargs)

    @omit_exception
    def get_lock(self: BaseValkeyCache, *args, **kwargs) -> "Lock":
        return self.client.get_lock(*args, **kwargs)

    lock = get_lock

    @omit_exception
    def ttl(self: BaseValkeyCache, *args, **kwargs) -> int | None:
        return self.client.ttl(*args, **kwargs)

    @omit_exception
    def pttl(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.pttl(*args, **kwargs)

    @omit_exception
    def sadd(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sadd(*args, **kwargs)

    @omit_exception
    def scard(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.scard(*args, **kwargs)

    @omit_exception
    def sdiff(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sdiff(*args, **kwargs)

    @omit_exception
    def sdiffstore(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sdiffstore(*args, **kwargs)

    @omit_exception
    def sinter(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sinter(*args, **kwargs)

    @omit_exception
    def sinterstore(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sinterstore(*args, **kwargs)

    @omit_exception
    def smismember(self: BaseValkeyCache, *args, **kwargs) -> list[bool]:
        return self.client.smismember(*args, **kwargs)

    @omit_exception
    def sismember(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.sismember(*args, **kwargs)

    @omit_exception
    def smembers(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.smembers(*args, **kwargs)

    @omit_exception
    def smove(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.smove(*args, **kwargs)

    @omit_exception
    def spop(self: BaseValkeyCache, *args, **kwargs) -> builtins.set | Any:
        return self.client.spop(*args, **kwargs)

    @omit_exception
    def srandmember(self: BaseValkeyCache, *args, **kwargs) -> list | Any:
        return self.client.srandmember(*args, **kwargs)

    @omit_exception
    def srem(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.srem(*args, **kwargs)

    @omit_exception
    def sscan(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sscan(*args, **kwargs)

    @omit_exception
    def sscan_iter(self: BaseValkeyCache, *args, **kwargs) -> Iterator:
        return self.client.sscan_iter(*args, **kwargs)

    @omit_exception
    def sunion(self: BaseValkeyCache, *args, **kwargs) -> builtins.set[Any]:
        return self.client.sunion(*args, **kwargs)

    @omit_exception
    def sunionstore(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.sunionstore(*args, **kwargs)

    @omit_exception
    def hset(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.hset(*args, **kwargs)

    @omit_exception
    def hdel(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.hdel(*args, **kwargs)

    @omit_exception
    def hlen(self: BaseValkeyCache, *args, **kwargs) -> int:
        return self.client.hlen(*args, **kwargs)

    @omit_exception
    def hkeys(self: BaseValkeyCache, *args, **kwargs) -> list[Any]:
        return self.client.hkeys(*args, **kwargs)

    @omit_exception
    def hexists(self: BaseValkeyCache, *args, **kwargs) -> bool:
        return self.client.hexists(*args, **kwargs)


class AsyncBackendCommands:
    def __getattr__(self, item):
        if item.startswith("a"):
            attr = getattr(self, item[1:], ATTR_DOES_NOT_EXIST)
            if attr is not ATTR_DOES_NOT_EXIST:
                return attr
        raise AttributeError(
            f"{self.__class__.__name__} object has no attribute {item}"
        )

    @omit_exception
    async def set(self: BaseValkeyCache, *args, **kwargs):
        return await self.client.aset(*args, **kwargs)

    @omit_exception
    async def incr_version(self, *args, **kwargs):
        return await self.client.aincr_version(*args, **kwargs)

    @omit_exception
    async def add(self, *args, **kwargs):
        return await self.client.aadd(*args, **kwargs)

    async def get(self, key, default=None, version=None, client=None):
        value = await self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    async def _get(self, key, default=None, version=None, client=None):
        return await self.client.aget(key, default, version, client)

    async def get_or_set(self, key, default, timeout=DEFAULT_TIMEOUT, version=None):
        """See get_or_set()."""
        val = await self.aget(key, self._missing_key, version=version)
        if val is self._missing_key:
            if callable(default):
                default = default()
            await self.aadd(key, default, timeout=timeout, version=version)
            # Fetch the value again to avoid a race condition if another caller
            # added a value between the first aget() and the aadd() above.
            return await self.aget(key, default, version=version)
        return val

    async def delete(self, *args, **kwargs):
        result = await self.client.adelete(*args, **kwargs)
        return bool(result)

    @omit_exception
    async def delete_many(self, *args, **kwargs):
        return await self.client.adelete_many(*args, **kwargs)

    @omit_exception
    async def clear(self):
        return await self.client.aclear()

    @omit_exception(return_value={})
    async def get_many(self, *args, **kwargs):
        return await self.client.aget_many(*args, **kwargs)

    @omit_exception
    async def set_many(self, *args, **kwargs):
        return await self.client.aset_many(*args, **kwargs)

    @omit_exception
    async def incr(self, *args, **kwargs):
        return await self.client.aincr(*args, **kwargs)

    @omit_exception
    async def decr(self, *args, **kwargs):
        return await self.client.adecr(*args, **kwargs)

    @omit_exception
    async def has_key(self, *args, **kwargs):
        return await self.client.ahas_key(*args, **kwargs)

    @omit_exception
    async def close(self, *args, **kwargs):
        return await self.client.aclose()

    @omit_exception
    async def touch(self, *args, **kwargs):
        return await self.client.touch(*args, **kwargs)

    @omit_exception
    async def mset(self, *args, **kwargs):
        return await self.client.amset(*args, **kwargs)

    @omit_exception
    async def mget(self, *args, **kwargs):
        return await self.client.amget(*args, **kwargs)

    @omit_exception
    async def persist(self, *args, **kwargs) -> bool:
        return await self.client.apersist(*args, **kwargs)

    @omit_exception
    async def expire(self, *args, **kwargs) -> bool:
        return await self.client.aexpire(*args, **kwargs)

    @omit_exception
    async def expire_at(self, *args, **kwargs) -> bool:
        return await self.client.aexpire_at(*args, **kwargs)

    @omit_exception
    async def pexpire(self, *args, **kwargs) -> bool:
        return await self.client.apexpire(*args, **kwargs)

    @omit_exception
    async def pexpire_at(self, *args, **kwargs) -> bool:
        return await self.client.apexpire_at(*args, **kwargs)

    @omit_exception
    async def get_lock(self, *args, **kwargs) -> "ALock":
        return await self.client.aget_lock(*args, **kwargs)

    lock = get_lock

    @omit_exception
    async def delete_pattern(self, *args, **kwargs) -> int:
        kwargs.setdefault("itersize", self._default_scan_itersize)
        return await self.client.adelete_pattern(*args, **kwargs)

    @omit_exception
    async def ttl(self, *args, **kwargs) -> int:
        return await self.client.attl(*args, **kwargs)

    @omit_exception
    async def pttl(self, *args, **kwargs) -> int:
        return await self.client.apttl(*args, **kwargs)

    @omit_exception
    async def iter_keys(self, *args, **kwargs) -> AsyncGenerator[Any]:
        async with contextlib.aclosing(self.client.aiter_keys(*args, **kwargs)) as it:
            async for key in it:
                yield key

    @omit_exception
    async def keys(self, *args, **kwargs) -> list[Any]:
        return await self.client.akeys(*args, **kwargs)

    @omit_exception
    async def sadd(self, *args, **kwargs) -> int:
        return await self.client.asadd(*args, **kwargs)

    @omit_exception
    async def scard(self, *args, **kwargs) -> int:
        return await self.client.ascard(*args, **kwargs)

    @omit_exception
    async def sdiff(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.asdiff(*args, **kwargs)

    @omit_exception
    async def sdiffstore(self, *args, **kwargs) -> int:
        return await self.client.asdiffstore(*args, **kwargs)

    @omit_exception
    async def sinter(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.asinter(*args, **kwargs)

    @omit_exception
    async def sinterstore(self, *args, **kwargs) -> int:
        return await self.client.asinterstore(*args, **kwargs)

    @omit_exception
    async def smismember(self, *args, **kwargs) -> list[bool]:
        return await self.client.asmismember(*args, **kwargs)

    @omit_exception
    async def sismember(self, *args, **kwargs) -> bool:
        return await self.client.asismember(*args, **kwargs)

    @omit_exception
    async def smembers(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.asmembers(*args, **kwargs)

    @omit_exception
    async def smove(self, *args, **kwargs) -> bool:
        return await self.client.asmove(*args, **kwargs)

    @omit_exception
    async def spop(self, *args, **kwargs) -> builtins.set | Any:
        return await self.client.aspop(*args, **kwargs)

    @omit_exception
    async def srandmember(self, *args, **kwargs) -> list | Any:
        return await self.client.asrandmember(*args, **kwargs)

    @omit_exception
    async def srem(self, *args, **kwargs) -> int:
        return await self.client.asrem(*args, **kwargs)

    @omit_exception
    async def sscan(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.asscan(*args, **kwargs)

    @omit_exception
    async def sscan_iter(self, *args, **kwargs) -> AsyncGenerator[Any]:
        async with contextlib.aclosing(self.client.asscan_iter(*args, **kwargs)) as it:
            async for key in it:
                yield key

    @omit_exception
    async def sunion(self, *args, **kwargs) -> builtins.set[Any]:
        return await self.client.asunion(*args, **kwargs)

    @omit_exception
    async def sunionstore(self, *args, **kwargs) -> int:
        return await self.client.asunionstore(*args, **kwargs)

    @omit_exception
    async def hset(self, *args, **kwargs) -> int:
        return await self.client.hset(*args, **kwargs)

    @omit_exception
    async def hdel(self, *args, **kwargs) -> int:
        return await self.client.hdel(*args, **kwargs)

    @omit_exception
    async def hlen(self, *args, **kwargs) -> int:
        return await self.client.hlen(*args, **kwargs)

    @omit_exception
    async def hkeys(self, *args, **kwargs) -> list[Any]:
        return await self.client.ahkeys(*args, **kwargs)

    @omit_exception
    async def hexists(self, *args, **kwargs) -> bool:
        return await self.client.ahexists(*args, **kwargs)
