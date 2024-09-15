import logging
from typing import Any, TypeVar, Generic, Iterator, AsyncGenerator, Set

from django.conf import settings
from django.core.cache.backends.base import BaseCache
from django.utils.module_loading import import_string

Client = TypeVar("Client")
Backend = TypeVar("Backend")


class BaseValkeyCache(BaseCache, Generic[Client, Backend]):
    def __init__(self, server: str, params: dict[str, Any]) -> None:
        super().__init__(params)
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

    def persist(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    async def apersist(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    def expire(
        self, key, timeout, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    async def aexpire(
        self, key, timeout, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    def expire_at(
        self, key, when, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    async def aexpire_at(
        self, key, when, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    def pexpire(
        self, key, timeout, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    async def apexpire(
        self, key, timeout, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    def pexpire_at(
        self, key, when, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    async def apexpire_at(
        self, key, when, version: int | None = None, client: Backend | None = None
    ) -> bool:
        pass

    def get_lock(
        self,
        key,
        version: int | None = None,
        timeout: float | int | None = None,
        sleep: float = 0.1,
        blocking_timeout: float | None = None,
        client: Backend | None = None,
        thread_local: bool = True,
    ):
        pass

    async def aget_lock(
        self,
        key,
        version: int | None = None,
        timeout: float | int | None = None,
        sleep: float = 0.1,
        blocking_timeout: float | None = None,
        client: Backend | None = None,
        thread_local: bool = True,
    ):
        """
        As of now, valkey's `lock` method is a sync method
        """
        return self.get_lock()

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | None = None,
        itersize: int | None = None,
    ) -> int:
        pass

    async def adelete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | None = None,
        itersize: int | None = None,
    ) -> int:
        pass

    def decode(self, value) -> Any:
        pass

    async def adecode(self, value) -> Any:
        """
        Serializers only have sync methods
        """
        return self.decode(value)

    def ttl(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> int | None:
        pass

    async def attl(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    def pttl(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    async def apttl(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    def iter_keys(
        self,
        search,
        itersize: int | None = None,
        client: Backend | None = None,
        version: int | None = None,
    ) -> Iterator:
        pass

    async def aiter_keys(
        self,
        search,
        itersize: int | None = None,
        client: Backend | None = None,
        version: int | None = None,
    ) -> AsyncGenerator:
        pass

    def keys(
        self, search: str, version: int | None = None, client: Backend | None = None
    ) -> list[Any]:
        pass

    async def akeys(
        self, search: str, version: int | None = None, client: Backend | None = None
    ) -> list[Any]:
        pass

    def make_key(self, key, version: int | None = None, prefix: str | None = None):
        pass

    async def amake_key(
        self, key, version: int | None = None, prefix: str | None = None
    ):
        return self.make_key(key, version, prefix)

    def sadd(
        self,
        key,
        *values: Any,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    async def asadd(
        self, *values: Any, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    def scard(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    async def ascard(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    def sdiff(
        self, *keys, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    async def asdiff(
        self, *keys, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    def sdiffstore(
        self,
        dest,
        *keys,
        version_dest: int | None = None,
        version_keys: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    async def asdiffstore(
        self,
        dest,
        *keys,
        version_dest: int | None = None,
        version_keys: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    def sinter(
        self, *keys, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    async def asinter(
        self, *keys, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    def sinterstore(
        self, dest, *keys, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    async def asintstore(
        self, dest, *keys, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    def smismember(
        self,
        key,
        *members: Any,
        version: int | None = None,
        client: Backend | None = None,
    ) -> list[bool]:
        pass

    async def asmismember(
        self,
        key,
        *members: Any,
        version: int | None = None,
        client: Backend | None = None,
    ) -> list[bool]:
        pass

    def sismember(
        self,
        key,
        member: Any,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        pass

    async def asismember(
        self,
        key,
        member: Any,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        pass

    def smembers(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    async def asmembers(
        self, key, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    def smove(
        self,
        source,
        destination,
        member: Any,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        pass

    async def asmove(
        self,
        source,
        destination,
        member: Any,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        pass

    def spop(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> Set | Any:
        pass

    async def aspop(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> Set | Any:
        pass

    def srandmember(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> list | Any:
        pass

    async def asrandmember(
        self,
        key,
        count: int | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> list | Any:
        pass

    def srem(
        self, key, *members, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    async def asrem(
        self, key, *members, version: int | None = None, client: Backend | None = None
    ) -> int:
        pass

    def sscan(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | None = None,
    ) -> Set[Any]:
        pass

    async def asscan(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | None = None,
    ) -> Set[Any]:
        pass

    def sscan_iter(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | None = None,
    ) -> Iterator:
        pass

    async def asscan_iter(
        self,
        key,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | None = None,
    ) -> AsyncGenerator:
        pass

    def sunion(
        self, *keys, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    async def asunion(
        self, *keys, version: int | None = None, client: Backend | None = None
    ) -> Set[Any]:
        pass

    def sunionstore(
        self,
        destination: Any,
        *keys,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    async def asunionstore(
        self,
        destination: Any,
        *keys,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    def hset(
        self,
        name: str,
        key,
        value,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    async def ahset(
        self,
        name: str,
        key,
        value,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    def hdel(
        self,
        name: str,
        key,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    async def ahdel(
        self,
        name: str,
        key,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        pass

    def hlen(self, name: str, client: Backend | None = None) -> int:
        pass

    async def ahlen(self, name: str, client: Backend | None = None) -> int:
        pass

    def hkeys(self, name: str, client: Backend | None = None) -> list[Any]:
        pass

    async def ahkeys(self, name: str, client: Backend | None = None) -> list[Any]:
        pass

    def hexists(
        self,
        name: str,
        key,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        pass

    async def ahexists(
        self,
        name: str,
        key,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        pass
