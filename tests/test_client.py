from collections.abc import Iterable

import pytest
from pytest_django.fixtures import SettingsWrapper
from pytest_mock import MockerFixture

from django.core.cache import DEFAULT_CACHE_ALIAS, cache as default_cache

from django_valkey.cache import ValkeyCache
from django_valkey.client import DefaultClient, ShardClient


@pytest.fixture
def cache_client(cache: ValkeyCache) -> Iterable[DefaultClient]:
    client = cache.client
    client.set("TestClientClose", 0)
    yield client
    client.delete("TestClientClose")


class TestClientClose:
    def test_close_client_disconnect_default(
        self, cache_client: DefaultClient, mocker: MockerFixture
    ):
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")

        cache_client.close()
        assert not mock.called

    def test_close_disconnect_settings(
        self,
        cache_client: DefaultClient,
        settings: SettingsWrapper,
        mocker: MockerFixture,
    ):
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")

        settings.DJANGO_VALKEY_CLOSE_CONNECTION = True
        cache_client.close()
        assert mock.called

    def test_close_disconnect_settings_cache(
        self,
        cache_client: DefaultClient,
        mocker: MockerFixture,
        settings: SettingsWrapper,
    ):
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")

        settings.CACHES[DEFAULT_CACHE_ALIAS]["OPTIONS"]["CLOSE_CONNECTION"] = True
        cache_client.set("TestClientClose", 0)

        cache_client.close()
        assert mock.called

    def test_close_disconnect_client_options(
        self, cache_client: DefaultClient, mocker: MockerFixture
    ):
        mock = mocker.patch.object(cache_client.connection_factory, "disconnect")

        cache_client._options["CLOSE_CONNECTION"] = True

        cache_client.close()
        assert mock.called


@pytest.mark.skipif(
    not isinstance(default_cache.client, DefaultClient), reason="shard only test"
)
class TestDefaultClient:
    def test_delete_pattern_calls_get_client_given_no_client(self, mocker):
        get_client_mock = mocker.patch(
            "django_valkey.base_client.ClientCommands.get_client"
        )
        mocker.patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
        client = DefaultClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""

        client.delete_pattern(pattern="foo*")
        get_client_mock.assert_called_once_with(write=True, tried=None)

    def test_delete_pattern_calls_make_pattern(self, mocker):
        make_pattern_mock = mocker.patch(
            "django_valkey.base_client.BaseClient.make_pattern"
        )
        get_client_mock = mocker.patch(
            "django_valkey.base_client.ClientCommands.get_client",
            return_value=mocker.Mock(),
        )
        mocker.patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
        client = DefaultClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="foo*")

        kwargs = {"version": None, "prefix": None}
        make_pattern_mock.assert_called_once_with("foo*", **kwargs)

    def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(self, mocker):
        mocker.patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
        get_client_mock = mocker.patch(
            "django_valkey.base_client.ClientCommands.get_client",
            return_value=mocker.Mock(),
        )
        make_pattern_mock = mocker.patch(
            "django_valkey.base_client.BaseClient.make_pattern"
        )
        client = DefaultClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="foo*", itersize=90210)

        get_client_mock.return_value.scan_iter.assert_called_once_with(
            count=90210, match=make_pattern_mock.return_value
        )

    def test_delete_pattern_calls_pipeline_delete_and_execute(self, mocker):
        mocker.patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
        get_client_mock = mocker.patch(
            "django_valkey.base_client.ClientCommands.get_client",
            return_value=mocker.Mock(),
        )
        mocker.patch("django_valkey.base_client.BaseClient.make_pattern")
        client = DefaultClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = [":1:foo", ":1:foo-a"]
        get_client_mock.return_value.pipeline.return_value = mocker.Mock()
        get_client_mock.return_value.pipeline.return_value.delete = mocker.Mock()
        get_client_mock.return_value.pipeline.return_value.execute = mocker.Mock()

        client.delete_pattern(pattern="foo*")

        assert get_client_mock.return_value.pipeline.return_value.delete.call_count == 2
        get_client_mock.return_value.pipeline.return_value.delete.assert_has_calls(
            [mocker.call(":1:foo"), mocker.call(":1:foo-a")]
        )
        get_client_mock.return_value.pipeline.return_value.execute.assert_called_once()


@pytest.mark.skipif(
    not isinstance(default_cache.client, ShardClient), reason="shard only test"
)
class TestShardClient:
    CLIENT_METHODS_FOR_MOCK = {
        "add",
        "close",
        "expire",
        "get",
        "ttl",
        "touch",
        "sadd",
        "scan_iter",
        "spop",
    }

    @pytest.fixture
    def shard_cache(self):
        from django.core.cache import caches, ConnectionProxy

        cache = ConnectionProxy(caches, "default")
        yield cache
        cache.clear()

    @pytest.fixture
    def connection(self, mocker):
        connection = mocker.Mock()

        for m in self.CLIENT_METHODS_FOR_MOCK:
            setattr(connection, m, mocker.Mock(spec_set=()))

        connection.scan_iter.return_value = []
        connection.get.return_value = "this"
        connection.add.return_value = True
        connection.expire.return_value = False
        connection.close.return_value = None
        connection.ttl.return_value = 0
        connection.touch.return_value = True
        connection.sadd.return_value = 1
        connection.spop.return_value = {"this"}

        yield connection

    def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(
        self, connection, mocker
    ):
        mocker.patch(
            "django_valkey.client.sharded.ShardClient.__init__", return_value=None
        )
        make_pattern_mock = mocker.patch(
            "django_valkey.base_client.BaseClient.make_pattern"
        )
        client = ShardClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""

        client._server_dict = {"test": connection}

        client.delete_pattern(pattern="foo*", itersize=10)

        connection.scan_iter.assert_called_once_with(
            count=10, match=make_pattern_mock.return_value
        )

    def test_delete_pattern_calls_scan_iter(self, connection, mocker):
        mocker.patch(
            "django_valkey.client.sharded.ShardClient.__init__", return_value=None
        )
        make_pattern_mock = mocker.patch(
            "django_valkey.base_client.BaseClient.make_pattern"
        )
        client = ShardClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        client._server_dict = {"test": connection}

        client.delete_pattern(pattern="foo*")

        connection.scan_iter.assert_called_once_with(
            match=make_pattern_mock.return_value
        )

    def test_delete_pattern_calls_delete_for_given_keys(
        self, connection, cache, mocker
    ):
        mocker.patch(
            "django_valkey.client.sharded.ShardClient.__init__", return_value=None
        )
        mocker.patch("django_valkey.base_client.BaseClient.make_pattern")
        client = ShardClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        connection.scan_iter.return_value = [mocker.Mock(), mocker.Mock()]
        connection.delete.return_value = 0
        client._server_dict = {"test": connection}

        client.delete_pattern(pattern="foo*")

        connection.delete.assert_called_once_with(*connection.scan_iter.return_value)

    def test_shard_client_get_server_name_is_called(
        self, mocker, connection, shard_cache
    ):
        if not isinstance(shard_cache.client, ShardClient):
            pytest.skip("shard only test")
        client = ShardClient(
            server=shard_cache._server, params=shard_cache._params, backend=shard_cache
        )
        spy = mocker.spy(client, "get_server_name")

        client.add("that", "this")
        client.get("that")
        client.ttl("that")
        client.expire("that", 1)
        client.touch("that")
        client.sadd("key", "value1", "value2")
        client.spop("key")
        assert spy.call_count == 7
