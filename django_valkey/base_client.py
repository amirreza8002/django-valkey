import builtins
from collections.abc import Iterable, Iterator
import random
import re
import socket
from contextlib import suppress
from typing import (
    Any,
    cast,
    TYPE_CHECKING,
    Generic,
    TypeVar,
)

from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT, get_key_func
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from valkey.exceptions import ConnectionError, ResponseError, TimeoutError
from valkey.typing import AbsExpiryT, EncodableT, ExpiryT, PatternT

from django_valkey import pool
from django_valkey.compressors.identity import IdentityCompressor
from django_valkey.exceptions import CompressorError, ConnectionInterrupted
from django_valkey.serializers.pickle import PickleSerializer
from django_valkey.util import CacheKey
from django_valkey.typing import KeyT

if TYPE_CHECKING:
    from valkey.lock import Lock
    from django_valkey.cache import ValkeyCache


_main_exceptions = (TimeoutError, ResponseError, ConnectionError, socket.timeout)

special_re = re.compile("([*?[])")


def glob_escape(s: str) -> str:
    return special_re.sub(r"[\1]", s)


Backend = TypeVar("Backend")


class BaseClient(Generic[Backend]):
    def __init__(
        self,
        server: str | Iterable,
        params: dict[str, Any],
        backend: "ValkeyCache",
    ) -> None:
        self._backend = backend
        self._server = server
        if not self._server:
            error_message = "Missing connections string"
            raise ImproperlyConfigured(error_message)
        if not isinstance(self._server, (list, tuple, set)):
            self._server = self._server.split(",")

        self._params = params

        self.reverse_key = get_key_func(
            params.get("REVERSE_KEY_FUNCTION")
            or "django_valkey.util.default_reverse_key"
        )

        self._clients: list[Backend | Any | None] = [None] * len(self._server)
        self._options: dict = params.get("OPTIONS", {})
        self._replica_read_only = self._options.get("REPLICA_READ_ONLY", True)

        serializer_path = self._options.get(
            "SERIALIZER", "django_valkey.serializers.pickle.PickleSerializer"
        )
        serializer_cls = import_string(serializer_path)

        compressor_path = self._options.get(
            "COMPRESSOR", "django_valkey.compressors.identity.IdentityCompressor"
        )
        compressor_cls = import_string(compressor_path)

        self._serializer: PickleSerializer | Any = serializer_cls(options=self._options)
        self._compressor: IdentityCompressor | Any = compressor_cls(
            options=self._options
        )

        self._connection_factory = getattr(
            settings, "DJANGO_VALKEY_CONNECTION_FACTORY", self.CONNECTION_FACTORY_PATH
        )
        self.connection_factory = pool.get_connection_factory(
            options=self._options, path=self._connection_factory
        )

    def __contains__(self, key: KeyT) -> bool:
        return self.has_key(key)

    def _has_compression_enabled(self) -> bool:
        return (
            self._options.get(
                "COMPRESSOR", "django_valkey.compressors.identity.IdentityCompressor"
            )
            != "django_valkey.compressors.identity.IdentityCompressor"
        )

    def get_next_client_index(
        self, write: bool = True, tried: list[int] | None = None
    ) -> int:
        """
        Return a next index for read client. This function implements a default
        behavior for get a next read client for a replication setup.

        Overwrite this function if you want a specific
        behavior.
        """
        if write or len(self._server) == 1:
            return 0

        if tried is None:
            tried = []

        if tried and len(tried) < len(self._server):
            not_tried = [i for i in range(0, len(self._server)) if i not in tried]
            return random.choice(not_tried)

        return random.randint(1, len(self._server) - 1)

    def _get_client(self, write=True, tried=None, client=None):
        if client:
            return client
        return self.get_client(write=write, tried=tried)

    def get_client(
        self,
        write: bool = True,
        tried: list[int] | None = None,
    ) -> Backend | Any:
        """
        Method used for obtain a raw valkey client.

        This function is used by almost all cache backend
        operations for obtain a native valkey client/connection
        instance.
        """
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = self.connect(index)

        return self._clients[index]  # type:ignore

    def get_client_with_index(
        self,
        write: bool = True,
        tried: list[int] | None = None,
    ) -> tuple[Backend | Any, int]:
        """
        Method used for obtain a raw valkey client.

        This function is used by almost all cache backend
        operations for obtain a native valkey client/connection
        instance.
        """
        index = self.get_next_client_index(write=write, tried=tried)

        if self._clients[index] is None:
            self._clients[index] = self.connect(index)

        return self._clients[index], index  # type:ignore

    def connect(self, index: int = 0) -> Backend | Any:
        """
        Given a connection index, returns a new raw valkey client/connection
        instance. Index is used for replication setups and indicates that
        connection string should be used. In normal setups, index is 0.
        """
        return self.connection_factory.connect(self._server[index])

    def disconnect(self, index: int = 0, client: Backend | Any | None = None) -> None:
        """
        delegates the connection factory to disconnect the client
        """
        if client is None:
            client = self._clients[index]

        if client is not None:
            self.connection_factory.disconnect(client)

    def set(
        self,
        key: KeyT,
        value: EncodableT,
        timeout: int | float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """
        Persist a value to the cache, and set an optional expiration time.

        Also supports optional nx parameter. If set to True - will use valkey
        setnx instead of set.
        """
        nkey = self.make_key(key, version=version)
        nvalue = self.encode(value)

        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        original_client = client
        tried: list[int] = []
        while True:
            try:
                if client is None:
                    client, index = self.get_client_with_index(write=True, tried=tried)

                if timeout is not None:
                    # Convert to milliseconds
                    timeout = int(timeout * 1000)

                    if timeout <= 0:
                        if nx:
                            # Using negative timeouts when nx is True should
                            # not expire (in our case delete) the value if it exists.
                            # Obviously expire not existent value is noop.
                            return not self.has_key(key, version=version, client=client)

                        # TODO: check if this is still valid
                        # valkey doesn't support negative timeouts in ex flags
                        # so it seems that it's better to just delete the key
                        # than to set it and then expire in a pipeline
                        return bool(self.delete(key, client=client, version=version))

                return client.set(nkey, nvalue, nx=nx, px=timeout, xx=xx)
            except _main_exceptions as e:
                if (
                    not original_client
                    and not self._replica_read_only
                    and len(tried) < len(self._server)
                ):
                    tried.append(index)
                    client = None
                    continue
                raise ConnectionInterrupted(connection=client) from e

    def incr_version(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.
        """

        client = self._get_client(write=True, client=client)

        new_key, old_key, value, ttl, version = self._incr_version(
            key, delta, version, client
        )
        self.set(new_key, value, timeout=ttl, client=client)
        self.delete(old_key, client=client)
        return version + delta

    def _incr_version(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> tuple:
        if version is None:
            version = self._backend.version

        old_key = self.make_key(key, version)
        value = self.get(old_key, version=version, client=client)

        try:
            ttl = self.ttl(old_key, version=version, client=client)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            error_message = f"Key '{key!r}' not found"
            raise ValueError(error_message)

        if isinstance(key, CacheKey):
            new_key = self.make_key(key.original_key(), version=version + delta)
        else:
            new_key = self.make_key(key, version=version + delta)

        return new_key, old_key, value, ttl, version

    def add(
        self,
        key: KeyT,
        value: EncodableT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        return self.set(key, value, timeout, version=version, client=client, nx=True)

    def get(
        self,
        key: KeyT,
        default: Any | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> Any:
        """
        Retrieve a value from the cache.

        Returns decoded value if key is found, the default if not.
        """
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)

        try:
            value = client.get(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if value is None:
            return default

        return self.decode(value)

    def persist(
        self, key: KeyT, version: int | None = None, client: Backend | Any | None = None
    ) -> bool:
        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        return client.persist(key)

    def expire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout  # type: ignore

        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        # for some strange reason mypy complains,
        # saying that timeout type is float | timedelta
        return client.expire(key, timeout)  # type: ignore

    def expire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Set an expiry flag on a ``key`` to ``when``, which can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        return client.expireat(key, when)

    def pexpire(
        self,
        key: KeyT,
        timeout: ExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout  # type: ignore

        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        # TODO: see if the casting is necessary
        # for some strange reason mypy complains,
        # saying that timeout type is float | timedelta
        return client.pexpire(key, timeout)

    def pexpire_at(
        self,
        key: KeyT,
        when: AbsExpiryT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Set an expiry flag on a ``key`` to ``when``, which can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)

        return client.pexpireat(key, when)

    def get_lock(
        self,
        key: KeyT,
        version: int | None = None,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
        client: Backend | Any | None = None,
        lock_class=None,
        thread_local: bool = True,
    ) -> "Lock":
        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)
        return client.lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            lock_class=lock_class,
            thread_local=thread_local,
        )

    # TODO: delete this in future releases
    lock = get_lock

    def delete(
        self,
        key: KeyT,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Remove a key from the cache.
        """
        client = self._get_client(write=True, client=client)

        try:
            return client.delete(self.make_key(key, version=version, prefix=prefix))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def delete_pattern(
        self,
        pattern: str,
        version: int | None = None,
        prefix: str | None = None,
        client: Backend | Any | None = None,
        itersize: int | None = None,
    ) -> int:
        """
        Remove all keys matching pattern.
        """

        client = self._get_client(write=True, client=client)

        pattern = self.make_pattern(pattern, version=version, prefix=prefix)

        try:
            count = 0
            pipeline = client.pipeline()

            for key in client.scan_iter(match=pattern, count=itersize):
                pipeline.delete(key)
                count += 1
            pipeline.execute()

            return count
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def delete_many(
        self,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Remove multiple keys at once.
        """
        keys = [self.make_key(k, version=version) for k in keys]

        if not keys:
            return 0

        client = self._get_client(write=True, client=client)

        try:
            return client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def clear(self, client: Backend | Any | None = None) -> bool:
        """
        Flush all cache keys.
        """

        client = self._get_client(write=True, client=client)

        try:
            return client.flushdb()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def decode(self, value: bytes) -> Any:
        """
        Decode the given value.
        """
        try:
            if value.isdigit():
                value = int(value)
            else:
                value = float(value)
        except (ValueError, TypeError):
            # Handle little values, chosen to be not compressed
            with suppress(CompressorError):
                value = self._compressor.decompress(value)
            value = self._serializer.loads(value)
        except AttributeError:
            # if value is None:
            return value
        return value

    def encode(self, value: EncodableT) -> bytes | int | float:
        """
        Encode the given value.
        """

        if type(value) is not int and type(value) is not float:
            value = self._serializer.dumps(value)
            return self._compressor.compress(value)

        return value

    def _decode_iterable_result(
        self, result: Any, convert_to_set: bool = True
    ) -> list[Any] | builtins.set[Any] | Any | None:
        if result is None:
            return None
        if isinstance(result, list):
            if convert_to_set:
                return {self.decode(value) for value in result}
            return [self.decode(value) for value in result]
        return self.decode(result)

    def mget(
        self,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: Backend | Any | None = None,
        return_list: bool = False,
    ) -> dict | list[Any]:
        """
        atomic method.
        Retrieve many keys.
        """

        client = self._get_client(write=False, client=client)

        if not keys:
            return {}

        recovered_data = {}

        map_keys = {self.make_key(k, version=version): k for k in keys}

        try:
            results = client.mget(map_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if return_list:
            return [self.decode(value) for value in results]

        for key, value in zip(map_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.decode(value)
        return recovered_data

    def get_many(
        self,
        keys: Iterable[KeyT],
        version: int | None = None,
        client: Backend | None = None,
    ) -> dict:
        """
        non-atomic bulk method.
        get values of the provided keys
        """
        client = self._get_client(write=False, client=client)

        try:
            pipeline = client.pipeline()
            for key in keys:
                key = self.make_key(key, version=version)
                pipeline.get(key)
            values = pipeline.execute()

        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        recovered_data = {}
        for key, value in zip(keys, values):
            if not value:
                continue
            recovered_data[key] = self.decode(value)
        return recovered_data

    def set_many(
        self,
        data: dict[KeyT, EncodableT],
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> None:
        """a non-atomic bulk method
        Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        client = self._get_client(write=True, client=client)

        try:
            pipeline = client.pipeline()
            for key, value in data.items():
                self.set(key, value, timeout, version=version, client=pipeline)
            pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def mset(
        self,
        data: dict[KeyT, Any],
        timeout: float | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> bool:
        """
        an atomic bulk method
        """
        client = self._get_client(write=True, client=client)
        data = {
            self.make_key(k, version=version): self.encode(v) for k, v in data.items()
        }
        try:
            return client.mset(data)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def _incr(
        self,
        key,
        amount: int = 1,
        version: int | None = None,
        ignore_key_check: bool = False,
        client: Backend | None = None,
        _operation="incr",
    ):
        client = self._get_client(write=True, client=client)
        op = client.incrby if _operation == "incr" else client.decrby
        key = self.make_key(key, version=version)
        try:
            if ignore_key_check:
                value = op(key, amount)
            else:
                if client.exists(key):
                    value = op(key, amount)
                else:
                    error_message = f"Key '{key!r}' not found"
                    raise ValueError(error_message)
        except ResponseError as e:
            # if cached value or total value is greater than 64-bit signed
            # integer.
            # elif int is encoded. so valkey sees the data as string.
            # In these situations valkey will throw ResponseError

            # try to keep TTL of key
            timeout = self.ttl(key, version=version, client=client)

            # returns -2 if the key does not exist
            # means, that key have expired
            if timeout == -2:
                error_message = f"Key '{key!r}' not found"
                raise ValueError(error_message) from e
            if _operation == "incr":
                value = self.get(key, version=version, client=client) + amount
            else:
                value = self.get(key, version=version, client=client) - amount
            self.set(key, value, version=version, timeout=timeout, client=client)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return value

    def incr(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
        ignore_key_check: bool = False,
    ) -> int:
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception. if ignore_key_check=True then the key will be
        created and set to the delta value by default.
        """
        return self._incr(
            key=key,
            amount=delta,
            version=version,
            client=client,
            ignore_key_check=ignore_key_check,
            _operation="incr",
        )

    def decr(
        self,
        key: KeyT,
        delta: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Decrease delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return self._incr(
            key=key, amount=delta, version=version, client=client, _operation="decr"
        )

    def ttl(
        self, key: KeyT, version: int | None = None, client: Backend | Any | None = None
    ) -> int | None:
        """
        Executes TTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        if not client.exists(key):
            return 0

        t = client.ttl(key)

        if t >= 0:
            return t
        if t == -2:
            return 0

        # Should never reach here
        return None

    def pttl(
        self, key: KeyT, version: int | None = None, client: Backend | Any | None = None
    ) -> int | None:
        """
        Executes PTTL valkey command and return the "time-to-live" of specified key.
        If key is a non-volatile key, it returns None.
        """
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        if not client.exists(key):
            return 0

        t = client.pttl(key)

        if t >= 0:
            return t
        if t == -2:
            return 0

        # Should never reach here
        return None

    def has_key(
        self, key: KeyT, version: int | None = None, client: Backend | Any | None = None
    ) -> bool:
        """
        Test if key exists.
        """

        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        try:
            return client.exists(key) == 1
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def iter_keys(
        self,
        search: str,
        itersize: int | None = None,
        client: Backend | Any | None = None,
        version: int | None = None,
    ) -> Iterator[str]:
        """
        Same as keys, but uses cursors
        for make memory efficient keys iteration.
        """

        client = self._get_client(write=False, client=client)

        pattern = self.make_pattern(search, version=version)
        for item in client.scan_iter(match=pattern, count=itersize):
            yield self.reverse_key(item.decode())

    def keys(
        self,
        search: str,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> list[Any]:
        """
        Execute KEYS command and return matched results.
        Warning: this can return huge number of results, in
        this case, it strongly recommended use iter_keys
        for it.
        """

        client = self._get_client(write=False, client=client)

        pattern = self.make_pattern(search, version=version)
        try:
            return [self.reverse_key(k.decode()) for k in client.keys(pattern)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def scan(
        self,
        cursor: int = 0,
        match: PatternT | None = None,
        count: int | None = None,
        _type: str | None = None,
        version: int | None = None,
        client: Backend | None = None,
    ) -> tuple[int, list[str]]:
        if self._has_compression_enabled() and match:
            raise ValueError("Using match with compression enables is not supported")
        client = self._get_client(write=False, client=client)

        try:
            cursor, result = client.scan(
                cursor=cursor,
                match=self.make_pattern(match, version=version) if match else None,
                count=count,
                _type=_type,
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        return cursor, [self.reverse_key(val.decode()) for val in result]

    def make_key(
        self, key: KeyT, version: int | None = None, prefix: str | None = None
    ) -> KeyT:
        """Return key as a CacheKey instance so it has additional methods"""
        if isinstance(key, CacheKey):
            return key

        if prefix is None:
            prefix = self._backend.key_prefix

        if version is None:
            version = self._backend.version

        return CacheKey(self._backend.key_func(key, prefix, version))

    def make_pattern(
        self, pattern: str, version: int | None = None, prefix: str | None = None
    ) -> KeyT:
        if isinstance(pattern, CacheKey):
            return pattern

        if prefix is None:
            prefix = self._backend.key_prefix
        prefix = glob_escape(prefix)

        if version is None:
            version = self._backend.version
        version_str = glob_escape(str(version))

        return CacheKey(self._backend.key_func(pattern, prefix, version_str))

    def sadd(
        self,
        key: KeyT,
        *values: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)
        encoded_values = [self.encode(value) for value in values]
        return client.sadd(key, *encoded_values)

    def scard(
        self,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        return client.scard(key)

    def sdiff(
        self,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        client = self._get_client(write=False, client=client)

        nkeys = [self.make_key(key, version=version) for key in keys]
        return self._decode_iterable_result(
            client.sdiff(*nkeys), convert_to_set=return_set
        )

    def sdiffstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version_dest: int | None = None,
        version_keys: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        dest = self.make_key(dest, version=version_dest)
        nkeys = [self.make_key(key, version=version_keys) for key in keys]
        return client.sdiffstore(dest, *nkeys)

    def sinter(
        self,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        client = self._get_client(write=False, client=client)

        nkeys = [self.make_key(key, version=version) for key in keys]
        return self._decode_iterable_result(
            client.sinter(*nkeys), convert_to_set=return_set
        )

    def sintercard(
        self,
        numkeys: int,
        keys: Iterable[str],
        limit: int = 0,
        version: int | None = None,
        client: Backend | None = None,
    ) -> int:
        client = self._get_client(write=False, client=client)
        nkeys = [self.make_key(key, version=version) for key in keys]
        return client.sintercard(numkeys, keys=nkeys, limit=limit)

    def sinterstore(
        self,
        dest: KeyT,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        dest = self.make_key(dest, version=version)
        nkeys = [self.make_key(key, version=version) for key in keys]
        return client.sinterstore(dest, *nkeys)

    def smismember(
        self,
        key: KeyT,
        *members: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> list[bool]:
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        encoded_members = [self.encode(member) for member in members]
        return [bool(value) for value in client.smismember(key, *encoded_members)]

    def sismember(
        self,
        key: KeyT,
        member: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        member = self.encode(member)
        return bool(client.sismember(key, member))

    def smembers(
        self,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        return self._decode_iterable_result(
            client.smembers(key), convert_to_set=return_set
        )

    def smove(
        self,
        source: KeyT,
        destination: KeyT,
        member: Any,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        client = self._get_client(write=True, client=client)

        source = self.make_key(source, version=version)
        destination = self.make_key(destination)
        member = self.encode(member)
        return client.smove(source, destination, member)

    def spop(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set | list | Any:
        client = self._get_client(write=True, client=client)

        nkey = self.make_key(key, version=version)
        result = client.spop(nkey, count)
        return self._decode_iterable_result(result, convert_to_set=return_set)

    def srandmember(
        self,
        key: KeyT,
        count: int | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
        return_set: bool = True,
    ) -> list | Any:
        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        result = client.srandmember(key, count)
        return self._decode_iterable_result(result, convert_to_set=return_set)

    def srem(
        self,
        key: KeyT,
        *members: EncodableT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)
        nmembers = [self.encode(member) for member in members]
        return client.srem(key, *nmembers)

    def sscan(
        self,
        key: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | Any | None = None,
        return_set: bool = True,
    ) -> tuple[int, builtins.set[Any]] | tuple[int, list[Any]]:
        if self._has_compression_enabled() and match:
            err_msg = "Using match with compression is not supported."
            raise ValueError(err_msg)

        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)

        cursor, result = client.sscan(
            name=key,
            cursor=cursor,
            match=cast(PatternT, self.encode(match)) if match else None,
            count=count,
        )
        return cursor, self._decode_iterable_result(result, convert_to_set=return_set)

    def sscan_iter(
        self,
        key: KeyT,
        match: str | None = None,
        count: int = 10,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> Iterator[Any]:
        if self._has_compression_enabled() and match:
            err_msg = "Using match with compression is not supported."
            raise ValueError(err_msg)

        client = self._get_client(write=False, client=client)

        key = self.make_key(key, version=version)
        for value in client.sscan_iter(
            key,
            match=cast(PatternT, self.encode(match)) if match else None,
            count=count,
        ):
            yield self.decode(value)

    def sunion(
        self,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
        return_set: bool = True,
    ) -> builtins.set[Any] | list[Any]:
        client = self._get_client(write=False, client=client)

        nkeys = [self.make_key(key, version=version) for key in keys]
        return self._decode_iterable_result(
            client.sunion(*nkeys), convert_to_set=return_set
        )

    def sunionstore(
        self,
        destination: Any,
        *keys: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)

        destination = self.make_key(destination, version=version)
        encoded_keys = [self.make_key(key, version=version) for key in keys]
        return client.sunionstore(destination, *encoded_keys)

    def close(self) -> None:
        close_flag = self._options.get(
            "CLOSE_CONNECTION",
            getattr(settings, "DJANGO_VALKEY_CLOSE_CONNECTION", False),
        )
        if close_flag:
            self._close()

    def _close(self) -> None:
        """
        default implementation: Override in custom client
        """
        num_clients = len(self._clients)
        for idx in range(num_clients):
            self.disconnect(index=idx)
        self._clients = [None] * num_clients

    def touch(
        self,
        key: KeyT,
        timeout: float | None = DEFAULT_TIMEOUT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Sets a new expiration for a key.
        """

        if timeout is DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout

        client = self._get_client(write=True, client=client)

        key = self.make_key(key, version=version)
        if timeout is None:
            return bool(client.persist(key))

        # Convert to milliseconds
        timeout = int(timeout * 1000)
        return bool(client.pexpire(key, timeout))

    def hset(
        self,
        name: str,
        key: KeyT | None = None,
        value: EncodableT | None = None,
        mapping: dict | None = None,
        items: list | None = None,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Set the value of hash name at key to value.
        Returns the number of fields added to the hash.
        """
        client = self._get_client(write=True, client=client)
        if key and value:
            key = self.make_key(key, version=version)
            value = self.encode(value)
        if mapping:
            mapping = {
                self.make_key(key): self.encode(value) for key, value in mapping.items()
            }
        if items:
            items = [
                (self.encode if index & 1 else self.make_key)(item)
                for index, item in enumerate(items)
            ]

        return client.hset(name, key, value, mapping=mapping, items=items)

    def hsetnx(
        self,
        name: str,
        key: KeyT,
        value: EncodableT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        nvalue = self.encode(value)
        return client.hsetnx(name, nkey, nvalue)

    def hdel(
        self,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Remove keys from hash name.
        Returns the number of fields deleted from the hash.
        """
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        return client.hdel(name, nkey)

    def hdel_many(
        self,
        name: str,
        keys: list,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)
        nkeys = [self.make_key(key) for key in keys]
        return client.hdel(name, *nkeys)

    def hget(
        self,
        name: str,
        key: str,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> str | None:
        client = self._get_client(write=False, client=client)
        key = self.make_key(key, version=version)
        try:
            value = client.hget(name, key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        if value is None:
            return None
        return self.decode(value)

    def hgetall(
        self, name: str, client: Backend | Any | None = None
    ) -> dict[str, str] | dict:
        client = self._get_client(write=False, client=client)
        try:
            _values = client.hgetall(name)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        values = {}
        for key, value in _values.items():
            values[key.decode()] = self.decode(value)

        return values

    def hmget(
        self,
        name: str,
        keys: list,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> list:
        client = self._get_client(write=False, client=client)
        nkeys = [self.make_key(key, version=version) for key in keys]
        try:
            values = client.hmget(name, nkeys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        values = [self.decode(val) for val in values]
        return values

    def hincrby(
        self,
        name: str,
        key: str,
        amount: int = 1,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            value = client.hincrby(name, nkey, amount)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return value

    def hincrbyfloat(
        self,
        name: str,
        key: str,
        amount: float = 1.0,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> float:
        client = self._get_client(write=True, client=client)
        nkey = self.make_key(key, version=version)
        try:
            value = client.hincrbyfloat(name, nkey, amount)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
        return value

    def hlen(
        self,
        name: str,
        client: Backend | Any | None = None,
    ) -> int:
        """
        Return the number of items in hash name.
        """
        client = self._get_client(write=False, client=client)
        return client.hlen(name)

    def hkeys(
        self,
        name: str,
        client: Backend | Any | None = None,
    ) -> list[Any]:
        """
        Return a list of keys in hash name.
        """
        client = self._get_client(write=False, client=client)
        try:
            return [self.reverse_key(k.decode()) for k in client.hkeys(name)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hexists(
        self,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> bool:
        """
        Return True if key exists in hash name, else False.
        """
        client = self._get_client(write=False, client=client)
        nkey = self.make_key(key, version=version)
        return client.hexists(name, nkey)

    def hvals(self, name: str, client: Backend | Any | None = None) -> list:
        client = self._get_client(write=False, client=client)
        try:
            return [self.decode(val) for val in client.hvals(name)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hstrlen(
        self,
        name: str,
        key: KeyT,
        version: int | None = None,
        client: Backend | Any | None = None,
    ) -> int:
        client = self._get_client(write=False, client=client)
        nkey = self.make_key(key, version=version)
        try:
            return client.hstrlen(name, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hrandfield(
        self,
        name: str,
        count: int | None = None,
        withvalues: bool = False,
        client: Backend | None = None,
    ) -> str | list | None:
        client = self._get_client(write=False, client=client)
        try:
            result = client.hrandfield(key=name, count=count, withvalues=withvalues)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

        if not result:
            return None
        elif count and withvalues:
            return [
                (self.decode(val) if index & 1 else self.reverse_key(val.decode()))
                for index, val in enumerate(result)
            ]
        elif count:
            return [self.reverse_key(val.decode()) for val in result]
        return self.reverse_key(result.decode())
