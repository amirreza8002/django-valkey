import datetime
import threading
import time
from collections.abc import Iterable
from datetime import timedelta
from typing import List, cast

import pytest
from pytest_django.fixtures import SettingsWrapper
from pytest_mock import MockerFixture

from django.core.cache import caches
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.test import override_settings

from django_valkey.cache import ValkeyCache
from django_valkey.client import ShardClient, herd
from django_valkey.cluster_cache.client import DefaultClusterClient
from django_valkey.serializers.json import JSONSerializer
from django_valkey.serializers.msgpack import MSGPackSerializer
from django_valkey.serializers.pickle import PickleSerializer


@pytest.fixture
def patch_itersize_setting() -> Iterable[None]:
    # destroy cache to force recreation with overridden settings
    del caches["default"]
    with override_settings(DJANGO_VALKEY_SCAN_ITERSIZE=30):
        yield
    # destroy cache to force recreation with original settings
    del caches["default"]


class TestDjangoValkeyCache:
    def test_set_int(self, cache: ValkeyCache):
        if isinstance(cache.client, herd.HerdClient):
            pytest.skip("herd client's set method works differently")
        cache.set("test_key", 1)
        result = cache.get("test_key")
        assert type(result) is int
        # shard client doesn't have get_client()
        if not isinstance(cache.client, ShardClient):
            raw_client = cache.client._get_client(write=False, client=None)
        else:
            raw_client = cache.client._get_client(key=":1:test_key")
        assert raw_client.get(":1:test_key") == b"1"

    def test_set_float(self, cache: ValkeyCache):
        if isinstance(cache.client, herd.HerdClient):
            pytest.skip("herd client's set method works differently")
        cache.set("test_key2", 1.1)
        result = cache.get("test_key2")
        assert type(result) is float
        if not isinstance(cache.client, ShardClient):
            raw_client = cache.client._get_client(write=False, client=None)
        else:
            raw_client = cache.client._get_client(key=":1:test_key2")
        assert raw_client.get(":1:test_key2") == b"1.1"

    def test_setnx(self, cache: ValkeyCache):
        # we should ensure there is no test_key_nx in valkey
        cache.delete("test_key_nx")
        res = cache.get("test_key_nx")
        assert res is None

        res = cache.set("test_key_nx", 1, nx=True)
        assert bool(res) is True
        # test that second set will have
        res = cache.set("test_key_nx", 2, nx=True)
        assert res is None
        res = cache.get("test_key_nx")
        assert res == 1

        cache.delete("test_key_nx")
        res = cache.get("test_key_nx")
        assert res is None

    def test_setnx_timeout(self, cache: ValkeyCache):
        # test that timeout still works for nx=True
        res = cache.set("test_key_nx", 1, timeout=2, nx=True)
        assert res is True
        time.sleep(3)
        res = cache.get("test_key_nx")
        assert res is None

        # test that timeout will not affect key, if it was there
        cache.set("test_key_nx", 1)
        res = cache.set("test_key_nx", 2, timeout=2, nx=True)
        assert res is None
        time.sleep(3)
        res = cache.get("test_key_nx")
        assert res == 1

        cache.delete("test_key_nx")
        res = cache.get("test_key_nx")
        assert res is None

    def test_unicode_keys(self, cache: ValkeyCache):
        cache.set("ключ", "value")
        res = cache.get("ключ")
        assert res == "value"

    def test_save_an_integer(self, cache: ValkeyCache):
        cache.set("test_key", 2)
        res = cache.get("test_key", "Foo")

        assert isinstance(res, int)
        assert res == 2

    def test_save_string(self, cache: ValkeyCache):
        cache.set("test_key", "hello" * 1000)
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "hello" * 1000

        cache.set("test_key", "2")
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "2"

    def test_save_unicode(self, cache: ValkeyCache):
        cache.set("test_key", "heló")
        res = cache.get("test_key")

        assert isinstance(res, str)
        assert res == "heló"

    def test_save_dict(self, cache: ValkeyCache):
        if isinstance(cache.client._serializer, (JSONSerializer, MSGPackSerializer)):
            # JSONSerializer and MSGPackSerializer use the isoformat for
            # datetimes.
            now_dt: str | datetime.datetime = datetime.datetime.now().isoformat()
        else:
            now_dt = datetime.datetime.now()

        test_dict = {"id": 1, "date": now_dt, "name": "Foo"}

        cache.set("test_key", test_dict)
        res = cache.get("test_key")

        assert isinstance(res, dict)
        assert res["id"] == 1
        assert res["name"] == "Foo"
        assert res["date"] == now_dt

    def test_save_float(self, cache: ValkeyCache):
        float_val = 1.345620002

        cache.set("test_key", float_val)
        res = cache.get("test_key")

        assert isinstance(res, float)
        assert res == float_val

    def test_timeout(self, cache: ValkeyCache):
        cache.set("test_key", 222, timeout=3)
        time.sleep(4)

        res = cache.get("test_key")
        assert res is None

    def test_timeout_0(self, cache: ValkeyCache):
        cache.set("test_key", 222, timeout=0)
        res = cache.get("test_key")
        assert res is None

    def test_timeout_parameter_as_positional_argument(self, cache: ValkeyCache):
        cache.set("test_key", 222, -1)
        res = cache.get("test_key")
        assert res is None

        cache.set("test_key", 222, 1)
        res1 = cache.get("test_key")
        time.sleep(2)
        res2 = cache.get("test_key")
        assert res1 == 222
        assert res2 is None

        # nx=True should not overwrite expire of key already in db
        cache.set("test_key", 222, None)
        cache.set("test_key", 222, -1, nx=True)
        res = cache.get("test_key")
        assert res == 222

    def test_timeout_negative(self, cache: ValkeyCache):
        cache.set("test_key", 222, timeout=-1)
        res = cache.get("test_key")
        assert res is None

        cache.set("test_key", 222, timeout=None)
        cache.set("test_key", 222, timeout=-1)
        res = cache.get("test_key")
        assert res is None

        # nx=True should not overwrite expire of key already in db
        cache.set("test_key", 222, timeout=None)
        cache.set("test_key", 222, timeout=-1, nx=True)
        res = cache.get("test_key")
        assert res == 222

    def test_timeout_tiny(self, cache: ValkeyCache):
        cache.set("test_key", 222, timeout=0.00001)
        res = cache.get("test_key")
        assert res in (None, 222)

    def test_set_add(self, cache: ValkeyCache):
        cache.set("add_key", "Initial value")
        res = cache.add("add_key", "New value")
        assert res is None

        res = cache.get("add_key")
        assert res == "Initial value"
        res = cache.add("other_key", "New value")
        assert res is True

    def test_get_many(self, cache: ValkeyCache):
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_mget(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient) or isinstance(
            cache.client, DefaultClusterClient
        ):
            pytest.skip()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        res = cache.mget(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_get_many_unicode(self, cache: ValkeyCache):
        cache.set("a", "1")
        cache.set("ب", "2")
        cache.set("c", "الف")

        res = cache.get_many(["a", "ب", "c"])
        assert res == {"a": "1", "ب": "2", "c": "الف"}

    def test_mget_unicode(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient) or isinstance(
            cache.client, DefaultClusterClient
        ):
            pytest.skip()

        cache.set("fooa", "1")
        cache.set("fooب", "2")
        cache.set("fooc", "الف")

        res = cache.mget(["fooa", "fooب", "fooc"])
        assert res == {"fooa": "1", "fooب": "2", "fooc": "الف"}

    def test_set_many(self, cache: ValkeyCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.get_many(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_mset(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient) or isinstance(
            cache.client, DefaultClusterClient
        ):
            pytest.skip()
        cache.mset({"a": 1, "b": 2, "c": 3})
        res = cache.mget(["a", "b", "c"])
        assert res == {"a": 1, "b": 2, "c": 3}

    def test_set_call_empty_pipeline(
        self,
        cache: ValkeyCache,
        mocker: MockerFixture,
        settings: SettingsWrapper,
    ):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support get_client")

        pipeline = cache.client.get_client(write=True).pipeline()
        key = "key"
        value = "value"

        mocked_set = mocker.patch.object(pipeline, "set")

        cache.set(key, value, client=pipeline)

        if isinstance(cache.client, herd.HerdClient):
            default_timeout = cache.client._backend.default_timeout
            herd_timeout = (default_timeout + settings.CACHE_HERD_TIMEOUT) * 1000
            herd_pack_value = cache.client._pack(value, default_timeout)
            mocked_set.assert_called_once_with(
                cache.client.make_key(key, version=None),
                cache.client.encode(herd_pack_value),
                nx=False,
                px=herd_timeout,
                xx=False,
            )
        else:
            mocked_set.assert_called_once_with(
                cache.client.make_key(key, version=None),
                cache.client.encode(value),
                nx=False,
                px=cache.client._backend.default_timeout * 1000,
                xx=False,
            )

    def test_delete(self, cache: ValkeyCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.delete("a")
        assert bool(res) is True

        res = cache.get_many(["a", "b", "c"])
        assert res == {"b": 2, "c": 3}

        res = cache.delete("a")
        assert bool(res) is False

    def test_delete_return_value_type(self, cache: ValkeyCache):
        """delete() returns a boolean instead of int since django version 3.1"""
        cache.set("a", 1)
        res = cache.delete("a")
        assert isinstance(res, bool)
        assert res is True
        res = cache.delete("b")
        assert isinstance(res, bool)
        assert res is False

    def test_delete_many(self, cache: ValkeyCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.delete_many(["a", "b"])
        assert bool(res) is True

        res = cache.get_many(["a", "b", "c"])
        assert res == {"c": 3}

        res = cache.delete_many(["a", "b"])
        assert bool(res) is False

    def test_delete_many_generator(self, cache: ValkeyCache):
        cache.set_many({"a": 1, "b": 2, "c": 3})
        res = cache.delete_many(key for key in ["a", "b"])
        assert bool(res) is True

        res = cache.get_many(["a", "b", "c"])
        assert res == {"c": 3}

        res = cache.delete_many(["a", "b"])
        assert bool(res) is False

    def test_delete_many_empty_generator(self, cache: ValkeyCache):
        res = cache.delete_many(key for key in cast(List[str], []))
        assert bool(res) is False

    def test_incr(self, cache: ValkeyCache):
        if isinstance(cache.client, herd.HerdClient):
            pytest.skip("HerdClient doesn't support incr")

        cache.set("num", 1)

        cache.incr("num")
        res = cache.get("num")
        assert res == 2

        cache.incr("num", 10)
        res = cache.get("num")
        assert res == 12

        # max 64-bit signed int
        cache.set("num", 9223372036854775807)

        cache.incr("num")
        res = cache.get("num")
        assert res == 9223372036854775808

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775810

        cache.set("num", 3)

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 5

    def test_incr_no_timeout(self, cache: ValkeyCache):
        if isinstance(cache.client, herd.HerdClient):
            pytest.skip("HerdClient doesn't support incr")

        cache.set("num", 1, timeout=None)

        cache.incr("num")
        res = cache.get("num")
        assert res == 2

        cache.incr("num", 10)
        res = cache.get("num")
        assert res == 12

        # max 64-bit signed int
        cache.set("num", 9223372036854775807, timeout=None)

        cache.incr("num")
        res = cache.get("num")
        assert res == 9223372036854775808

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775810

        cache.set("num", 3, timeout=None)

        cache.incr("num", 2)
        res = cache.get("num")
        assert res == 5

    def test_incr_error(self, cache: ValkeyCache):
        if isinstance(cache.client, herd.HerdClient):
            pytest.skip("HerdClient doesn't support incr")

        with pytest.raises(ValueError):
            # key does not exist
            cache.incr("numnum")

    def test_incr_ignore_check(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support argument ignore_key_check to incr")
        if isinstance(cache.client, herd.HerdClient):
            pytest.skip("HerdClient doesn't support incr")

        # key exists check will be skipped and the value will be incremented by
        # '1' which is the default delta
        cache.incr("num", ignore_key_check=True)
        res = cache.get("num")
        assert res == 1
        cache.delete("num")

        # since key doesn't exist it is set to the delta value, 10 in this case
        cache.incr("num", 10, ignore_key_check=True)
        res = cache.get("num")
        assert res == 10
        cache.delete("num")

        # following are just regression checks to make sure it still works as
        # expected with incr max 64-bit signed int
        cache.set("num", 9223372036854775807)

        cache.incr("num", ignore_key_check=True)
        res = cache.get("num")
        assert res == 9223372036854775808

        cache.incr("num", 2, ignore_key_check=True)
        res = cache.get("num")
        assert res == 9223372036854775810

        cache.set("num", 3)

        cache.incr("num", 2, ignore_key_check=True)
        res = cache.get("num")
        assert res == 5

    def test_get_set_bool(self, cache: ValkeyCache):
        cache.set("bool", True)
        res = cache.get("bool")

        assert isinstance(res, bool)
        assert res is True

        cache.set("bool", False)
        res = cache.get("bool")

        assert isinstance(res, bool)
        assert res is False

    def test_decr(self, cache: ValkeyCache):
        if isinstance(cache.client, herd.HerdClient):
            pytest.skip("HerdClient doesn't support decr")

        cache.set("num", 20)

        cache.decr("num")
        res = cache.get("num")
        assert res == 19

        cache.decr("num", 20)
        res = cache.get("num")
        assert res == -1

        cache.decr("num", 2)
        res = cache.get("num")
        assert res == -3

        cache.set("num", 20)

        cache.decr("num")
        res = cache.get("num")
        assert res == 19

        # max 64-bit signed int + 1
        cache.set("num", 9223372036854775808)

        cache.decr("num")
        res = cache.get("num")
        assert res == 9223372036854775807

        cache.decr("num", 2)
        res = cache.get("num")
        assert res == 9223372036854775805

    def test_version(self, cache: ValkeyCache):
        cache.set("keytest", 2, version=2)
        res = cache.get("keytest")
        assert res is None

        res = cache.get("keytest", version=2)
        assert res == 2

    def test_incr_version(self, cache: ValkeyCache):
        cache.set("keytest", 2)
        cache.incr_version("keytest")

        res = cache.get("keytest")
        assert res is None

        res = cache.get("keytest", version=2)
        assert res == 2

    def test_ttl_incr_version_no_timeout(self, cache: ValkeyCache):
        cache.set("my_key", "hello world!", timeout=None)

        cache.incr_version("my_key")

        my_value = cache.get("my_key", version=2)

        assert my_value == "hello world!"

    def test_delete_pattern(self, cache: ValkeyCache):
        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is True

        keys = cache.keys("foo*")
        assert set(keys) == {"foo-bb", "foo-bc"}

        res = cache.delete_pattern("*foo-a*")
        assert bool(res) is False

    def test_delete_pattern_with_custom_count(self, cache: ValkeyCache, mocker):
        client_mock = mocker.patch("django_valkey.cache.ValkeyCache.client")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")

        cache.delete_pattern("*foo-a*", itersize=2)

        client_mock.delete_pattern.assert_called_once_with("*foo-a*", itersize=2)

    def test_delete_pattern_with_settings_default_scan_count(
        self,
        patch_itersize_setting,
        cache: ValkeyCache,
        settings: SettingsWrapper,
        mocker,
    ):
        client_mock = mocker.patch("django_valkey.cache.ValkeyCache.client")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        for key in ["foo-aa", "foo-ab", "foo-bb", "foo-bc"]:
            cache.set(key, "foo")
        expected_count = settings.DJANGO_VALKEY_SCAN_ITERSIZE

        cache.delete_pattern("*foo-a*")

        client_mock.delete_pattern.assert_called_once_with(
            "*foo-a*", itersize=expected_count
        )

    def test_close(self, cache: ValkeyCache, settings: SettingsWrapper):
        settings.DJANGO_VALKEY_CLOSE_CONNECTION = True
        cache.set("f", "1")
        cache.close()

    def test_close_client(self, cache: ValkeyCache, mocker: MockerFixture):
        mock = mocker.patch.object(cache.client, "close")

        cache.close()
        assert mock.called

    def test_ttl(self, cache: ValkeyCache):
        cache.set("foo", "bar", 10)
        ttl = cache.ttl("foo")

        if isinstance(cache.client, herd.HerdClient):
            assert pytest.approx(ttl) == 12
        else:
            assert pytest.approx(ttl) == 10

        # Test ttl None
        cache.set("foo", "foo", timeout=None)
        ttl = cache.ttl("foo")
        assert ttl is None

        # Test ttl with expired key
        cache.set("foo", "foo", timeout=-1)
        ttl = cache.ttl("foo")
        assert ttl == 0

        # Test ttl with not existent key
        ttl = cache.ttl("not-existent-key")
        assert ttl == 0

    def test_pttl(self, cache: ValkeyCache):
        # Test pttl
        cache.set("foo", "bar", 10)
        ttl = cache.pttl("foo")

        # delta is set to 10 as precision error causes tests to fail
        if isinstance(cache.client, herd.HerdClient):
            assert pytest.approx(ttl, 10) == 12000
        else:
            assert pytest.approx(ttl, 10) == 10000

        # Test pttl with float value
        cache.set("foo", "bar", 5.5)
        ttl = cache.pttl("foo")

        if isinstance(cache.client, herd.HerdClient):
            assert pytest.approx(ttl, 10) == 7500
        else:
            assert pytest.approx(ttl, 10) == 5500

        # Test pttl None
        cache.set("foo", "foo", timeout=None)
        ttl = cache.pttl("foo")
        assert ttl is None

        # Test pttl with expired key
        cache.set("foo", "foo", timeout=-1)
        ttl = cache.pttl("foo")
        assert ttl == 0

        # Test pttl with not existent key
        ttl = cache.pttl("not-existent-key")
        assert ttl == 0

    def test_persist(self, cache: ValkeyCache):
        cache.set("foo", "bar", timeout=20)
        assert cache.persist("foo") is True

        ttl = cache.ttl("foo")
        assert ttl is None
        assert cache.persist("not-existent-key") is False

    def test_expire(self, cache: ValkeyCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.expire("foo", 20) is True
        ttl = cache.ttl("foo")
        assert pytest.approx(ttl) == 20
        assert cache.expire("not-existent-key", 20) is False

    def test_expire_with_default_timeout(self, cache: ValkeyCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.expire("foo", DEFAULT_TIMEOUT) is True
        assert cache.expire("not-existent-key", DEFAULT_TIMEOUT) is False

    def test_pexpire(self, cache: ValkeyCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.pexpire("foo", 20500) is True
        ttl = cache.pttl("foo")
        # delta is set to 10 as precision error causes tests to fail
        assert pytest.approx(ttl, 10) == 20500
        assert cache.pexpire("not-existent-key", 20500) is False

    def test_pexpire_with_default_timeout(self, cache: ValkeyCache):
        cache.set("foo", "bar", timeout=None)
        assert cache.pexpire("foo", DEFAULT_TIMEOUT) is True
        assert cache.pexpire("not-existent-key", DEFAULT_TIMEOUT) is False

    def test_pexpire_at(self, cache: ValkeyCache):
        # Test settings expiration time 1 hour ahead by datetime.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=1)
        assert cache.pexpire_at("foo", expiration_time) is True
        ttl = cache.pttl("foo")
        assert pytest.approx(ttl, 10) == timedelta(hours=1).total_seconds()

        # Test settings expiration time 1 hour ahead by Unix timestamp.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.pexpire_at("foo", int(expiration_time.timestamp() * 1000)) is True
        ttl = cache.pttl("foo")
        assert pytest.approx(ttl, 10) == timedelta(hours=2).total_seconds() * 1000

        # Test settings expiration time 1 hour in the past, which effectively
        # deletes the key.
        expiration_time = datetime.datetime.now() - timedelta(hours=2)
        assert cache.pexpire_at("foo", expiration_time) is True
        value = cache.get("foo")
        assert value is None

        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.pexpire_at("not-existent-key", expiration_time) is False

    def test_expire_at(self, cache: ValkeyCache):
        # Test settings expiration time 1 hour ahead by datetime.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=1)
        assert cache.expire_at("foo", expiration_time) is True
        ttl = cache.ttl("foo")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds()

        # Test settings expiration time 1 hour ahead by Unix timestamp.
        cache.set("foo", "bar", timeout=None)
        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.expire_at("foo", int(expiration_time.timestamp())) is True
        ttl = cache.ttl("foo")
        assert pytest.approx(ttl, 1) == timedelta(hours=1).total_seconds() * 2

        # Test settings expiration time 1 hour in the past, which effectively
        # deletes the key.
        expiration_time = datetime.datetime.now() - timedelta(hours=2)
        assert cache.expire_at("foo", expiration_time) is True
        value = cache.get("foo")
        assert value is None

        expiration_time = datetime.datetime.now() + timedelta(hours=2)
        assert cache.expire_at("not-existent-key", expiration_time) is False

    def test_lock(self, cache: ValkeyCache):
        lock = cache.lock("foobar")
        lock.acquire(blocking=True)

        assert cache.has_key("foobar")
        lock.release()
        assert not cache.has_key("foobar")

    def test_lock_context_manager(self, cache: ValkeyCache):
        with cache.lock("foobar"):
            assert cache.has_key("foobar")
        assert not cache.has_key("foobar")

    def test_lock_released_by_thread(self, cache: ValkeyCache):
        lock = cache.lock("foobar", thread_local=False)
        lock.acquire(blocking=True)

        def release_lock(lock_):
            lock_.release()

        t = threading.Thread(target=release_lock, args=[lock])
        t.start()
        t.join()

        assert not cache.has_key("foobar")

    def test_iter_keys(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support iter_keys")

        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test simple result
        result = set(cache.iter_keys("foo*"))
        assert result == {"foo1", "foo2", "foo3"}

    def test_iter_keys_itersize(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support iter_keys")

        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test limited result
        result = list(cache.iter_keys("foo*", itersize=2))
        assert len(result) == 3

    def test_iter_keys_generator(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support iter_keys")

        cache.set("foo1", 1)
        cache.set("foo2", 1)
        cache.set("foo3", 1)

        # Test generator object
        result = cache.iter_keys("foo*")
        next_value = next(result)
        assert next_value is not None

    def test_primary_replica_switching(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("shard client handles connections differently")
        cache = cast(ValkeyCache, caches["sample"])
        client = cache.client
        client._server = ["foo", "bar"]
        client._clients = ["Foo", "Bar"]

        assert client.get_client(write=True) == "Foo"
        assert client.get_client(write=False) == "Bar"

    def test_primary_replica_switching_with_index(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support get_client")

        cache = cast(ValkeyCache, caches["sample"])
        client = cache.client
        client._server = ["foo", "bar"]
        client._clients = ["Foo", "Bar"]

        assert client.get_client_with_index(write=True) == ("Foo", 0)
        assert client.get_client_with_index(write=False) == ("Bar", 1)

    def test_touch_zero_timeout(self, cache: ValkeyCache):
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", 0) is True
        res = cache.get("test_key")
        assert res is None

    def test_touch_positive_timeout(self, cache: ValkeyCache):
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", 2) is True
        assert cache.get("test_key") == 222
        time.sleep(3)
        assert cache.get("test_key") is None

    def test_touch_negative_timeout(self, cache: ValkeyCache):
        cache.set("test_key", 222, timeout=10)

        assert cache.touch("test_key", -1) is True
        res = cache.get("test_key")
        assert res is None

    def test_touch_missed_key(self, cache: ValkeyCache):
        assert cache.touch("test_key_does_not_exist", 1) is False

    def test_touch_forever(self, cache: ValkeyCache):
        cache.set("test_key", "foo", timeout=1)
        result = cache.touch("test_key", None)
        assert result is True
        assert cache.ttl("test_key") is None
        time.sleep(2)
        assert cache.get("test_key") == "foo"

    def test_touch_forever_nonexistent(self, cache: ValkeyCache):
        result = cache.touch("test_key_does_not_exist", None)
        assert result is False

    def test_touch_default_timeout(self, cache: ValkeyCache):
        cache.set("test_key", "foo", timeout=1)
        result = cache.touch("test_key")
        assert result is True
        time.sleep(2)
        assert cache.get("test_key") == "foo"

    def test_clear(self, cache: ValkeyCache):
        cache.set("foo", "bar")
        value_from_cache = cache.get("foo")
        assert value_from_cache == "bar"
        cache.clear()
        value_from_cache_after_clear = cache.get("foo")
        assert value_from_cache_after_clear is None

    def test_hset(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support hash operations")
        cache.hset("foo_hash1", "foo1", "bar1")
        cache.hset("foo_hash1", "foo2", "bar2")
        assert cache.hlen("foo_hash1") == 2
        assert cache.hexists("foo_hash1", "foo1")
        assert cache.hexists("foo_hash1", "foo2")

    def test_hdel(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support hash operations")
        cache.hset("foo_hash2", "foo1", "bar1")
        cache.hset("foo_hash2", "foo2", "bar2")
        assert cache.hlen("foo_hash2") == 2
        deleted_count = cache.hdel("foo_hash2", "foo1")
        assert deleted_count == 1
        assert cache.hlen("foo_hash2") == 1
        assert not cache.hexists("foo_hash2", "foo1")
        assert cache.hexists("foo_hash2", "foo2")

    def test_hlen(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support hash operations")
        assert cache.hlen("foo_hash3") == 0
        cache.hset("foo_hash3", "foo1", "bar1")
        assert cache.hlen("foo_hash3") == 1
        cache.hset("foo_hash3", "foo2", "bar2")
        assert cache.hlen("foo_hash3") == 2

    def test_hkeys(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support hash operations")
        cache.hset("foo_hash4", "foo1", "bar1")
        cache.hset("foo_hash4", "foo2", "bar2")
        cache.hset("foo_hash4", "foo3", "bar3")
        keys = cache.hkeys("foo_hash4")
        assert len(keys) == 3
        for i in range(len(keys)):
            assert keys[i] == f"foo{i + 1}"

    def test_hexists(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support hash operations")
        cache.hset("foo_hash5", "foo1", "bar1")
        assert cache.hexists("foo_hash5", "foo1")
        assert not cache.hexists("foo_hash5", "foo")

    def test_sadd(self, cache: ValkeyCache):
        assert cache.sadd("foo", "bar") == 1
        assert cache.smembers("foo") == {"bar"}

    def test_sadd_int(self, cache: ValkeyCache):
        cache.sadd("foo", 1)
        assert cache.smembers("foo") == {1}
        if not isinstance(cache.client, ShardClient):
            raw_client = cache.client._get_client(write=False, client=None)
        else:
            raw_client = cache.client._get_client(key=":1:foo")
        assert raw_client.smembers(":1:foo") == [b"1"]

    def test_sadd_float(self, cache: ValkeyCache):
        cache.sadd("foo", 1.2)
        assert cache.smembers("foo") == {1.2}
        if not isinstance(cache.client, ShardClient):
            raw_client = cache.client._get_client(write=False, client=None)
        else:
            raw_client = cache.client._get_client(key=":1:foo")
        assert raw_client.smembers(":1:foo") == [b"1.2"]

    def test_scard(self, cache: ValkeyCache):
        cache.sadd("foo", "bar", "bar2")
        assert cache.scard("foo") == 2

    def test_sdiff(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sdiff")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2")
        cache.sadd("foo2", "bar2", "bar3")
        assert cache.sdiff("foo1", "foo2") == {"bar1"}

    def test_sdiffstore(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sdiffstore")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2")
        cache.sadd("foo2", "bar2", "bar3")
        assert cache.sdiffstore("foo3", "foo1", "foo2") == 1
        assert cache.smembers("foo3") == {"bar1"}

    def test_sdiffstore_with_keys_version(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sdiffstore")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2", version=2)
        cache.sadd("foo2", "bar2", "bar3", version=2)
        assert cache.sdiffstore("foo3", "foo1", "foo2", version_keys=2) == 1
        assert cache.smembers("foo3") == {"bar1"}

    def test_sdiffstore_with_different_keys_versions_without_initial_set_in_version(
        self, cache: ValkeyCache
    ):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sdiffstore")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2", version=1)
        cache.sadd("foo2", "bar2", "bar3", version=2)
        assert cache.sdiffstore("foo3", "foo1", "foo2", version_keys=2) == 0

    def test_sdiffstore_with_different_keys_versions_with_initial_set_in_version(
        self, cache: ValkeyCache
    ):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sdiffstore")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2", version=2)
        cache.sadd("foo2", "bar2", "bar3", version=1)
        assert cache.sdiffstore("foo3", "foo1", "foo2", version_keys=2) == 2

    def test_sinter(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sinter")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2")
        cache.sadd("foo2", "bar2", "bar3")
        assert cache.sinter("foo1", "foo2") == {"bar2"}

    def test_sinterstore(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sinterstore")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2")
        cache.sadd("foo2", "bar2", "bar3")
        assert cache.sinterstore("foo3", "foo1", "foo2") == 1
        assert cache.smembers("foo3") == {"bar2"}

    def test_sismember_str(self, cache: ValkeyCache):
        cache.sadd("foo", "bar")
        assert cache.sismember("foo", "bar") is True
        assert cache.sismember("foo", "bar2") is False

    def test_sismember_int(self, cache: ValkeyCache):
        cache.sadd("baz", 3)
        assert cache.sismember("baz", 3) is True
        assert cache.sismember("baz", 2) is False

    def test_sismember_float(self, cache: ValkeyCache):
        cache.sadd("foo", 3.0)
        assert cache.sismember("foo", 3.0) is True
        assert cache.sismember("foo", 2.0) is False

    def test_sismember_byte(self, cache: ValkeyCache):
        if isinstance(cache.client._serializer, JSONSerializer):
            pytest.skip("JSONSerializer doesn't support the byte type")
        cache.sadd("foo", b"abc")
        assert cache.sismember("foo", b"abc") is True
        assert cache.sismember("foo", b"def") is False

    def test_sismember_bytearray(self, cache: ValkeyCache):
        if isinstance(cache.client._serializer, JSONSerializer):
            pytest.skip("JSONSerializer doesn't support the bytearray type")
        right_val = bytearray(b"abc")
        wrong_val = bytearray(b"def")
        cache.sadd("foo", right_val)
        assert cache.sismember("foo", right_val) is True
        assert cache.sismember("foo", wrong_val) is False

    def test_sismember_memoryview(self, cache: ValkeyCache):
        if isinstance(cache.client._serializer, (PickleSerializer, JSONSerializer)):
            pytest.skip(
                "PickleSerializer/JSONSerializer doesn't support the memoryview type"
            )
        right_val = memoryview(b"abc")
        wrong_val = memoryview(b"def")
        cache.sadd("foo", right_val)
        assert cache.sismember("foo", right_val) is True
        assert cache.sismember("foo", wrong_val) is False

    def test_sismember_complex(self, cache: ValkeyCache):
        if isinstance(cache.client._serializer, (JSONSerializer, MSGPackSerializer)):
            pytest.skip(
                "JSONSerializer/MSGPackSerializer doesn't support the complex type"
            )
        cache.sadd("foo", 3j)
        assert cache.sismember("foo", 3j) is True
        assert cache.sismember("foo", 4j) is False

    def test_sismember_list(self, cache: ValkeyCache):
        cache.sadd("foo", [1, 2, 3])
        assert cache.sismember("foo", [1, 2, 3]) is True
        assert cache.sismember("foo", [1, 2, 4]) is False

    def test_sismember_tuple(self, cache: ValkeyCache):
        cache.sadd("foo", (1, 2, 3))
        assert cache.sismember("foo", (1, 2, 3)) is True
        assert cache.sismember("foo", (1, 2, 4)) is False

    def test_sismember_set(self, cache: ValkeyCache):
        if isinstance(cache.client._serializer, (MSGPackSerializer, JSONSerializer)):
            pytest.skip("MSGPackSerializer doesn't support the set type")
        cache.sadd("foo", {1, 2, 3})
        assert cache.sismember("foo", {1, 2, 3}) is True
        assert cache.sismember("foo", {1, 2, 4}) is False

    def test_sismember_frozenset(self, cache: ValkeyCache):
        if isinstance(cache.client._serializer, (MSGPackSerializer, JSONSerializer)):
            pytest.skip("MSGPackSerializer doesn't support the frozenset type")
        cache.sadd("foo", frozenset(("a", "b")))
        assert cache.sismember("foo", frozenset(("a", "b"))) is True
        assert cache.sismember("foo", frozenset(("d", "c"))) is False

    def test_sismember_dict(self, cache: ValkeyCache):
        cache.sadd("foo", {"a": 1, "b": 2})
        assert cache.sismember("foo", {"a": 1, "b": 2}) is True
        assert cache.sismember("foo", {"a": 1, "c": 3}) is False

    def test_sismember_bool(self, cache: ValkeyCache):
        cache.sadd("foo", True)
        assert cache.sismember("foo", True) is True
        assert cache.sismember("foo", False) is False

    def test_smove(self, cache: ValkeyCache):
        # if isinstance(cache.client, ShardClient):
        #     pytest.skip("ShardClient doesn't support get_client")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2")
        cache.sadd("foo2", "bar2", "bar3")
        assert cache.smove("foo1", "foo2", "bar1") is True
        assert cache.smove("foo1", "foo2", "bar4") is False
        assert cache.smembers("foo1") == {"bar2"}
        assert cache.smembers("foo2") == {"bar1", "bar2", "bar3"}

    def test_spop_default_count(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.spop("foo") in {"bar1", "bar2"}
        assert cache.smembers("foo") in [{"bar1"}, {"bar2"}]

    def test_spop(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.spop("foo", 1) in [{"bar1"}, {"bar2"}]
        assert cache.smembers("foo") in [{"bar1"}, {"bar2"}]

    def test_srandmember_default_count(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srandmember("foo") in {"bar1", "bar2"}

    def test_srandmember(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srandmember("foo", 1) in [["bar1"], ["bar2"]]

    def test_srem(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2")
        assert cache.srem("foo", "bar1") == 1
        assert cache.srem("foo", "bar3") == 0

    def test_sscan(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2")
        items = cache.sscan("foo")
        assert items == {"bar1", "bar2"}

    def test_sscan_with_match(self, cache: ValkeyCache):
        if cache.client._has_compression_enabled():
            pytest.skip("Compression is enabled, sscan with match is not supported")
        cache.sadd("foo", "bar1", "bar2", "zoo")
        items = cache.sscan("foo", match="zoo")
        assert items == {"zoo"}

    def test_sscan_iter(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2")
        items = cache.sscan_iter("foo")
        assert set(items) == {"bar1", "bar2"}

    def test_sscan_iter_with_match(self, cache: ValkeyCache):
        if cache.client._has_compression_enabled():
            pytest.skip(
                "Compression is enabled, sscan_iter with match is not supported"
            )
        cache.sadd("foo", "bar1", "bar2", "zoo")
        items = cache.sscan_iter("foo", match="bar*")
        assert set(items) == {"bar1", "bar2"}

    def test_smismember(self, cache: ValkeyCache):
        cache.sadd("foo", "bar1", "bar2", "bar3")
        assert cache.smismember("foo", "bar1", "bar2", "xyz") == [True, True, False]

    def test_sunion(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sunion")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2")
        cache.sadd("foo2", "bar2", "bar3")
        assert cache.sunion("foo1", "foo2") == {"bar1", "bar2", "bar3"}

    def test_sunionstore(self, cache: ValkeyCache):
        if isinstance(cache.client, ShardClient):
            pytest.skip("ShardClient doesn't support sunionstore")

        if isinstance(cache.client, DefaultClusterClient):
            pytest.skip("cluster client has a specific test")

        cache.sadd("foo1", "bar1", "bar2")
        cache.sadd("foo2", "bar2", "bar3")
        assert cache.sunionstore("foo3", "foo1", "foo2") == 3
        assert cache.smembers("foo3") == {"bar1", "bar2", "bar3"}
