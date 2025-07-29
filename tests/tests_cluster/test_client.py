from collections.abc import Iterable

import pytest

from django_valkey.cluster_cache.cache import ClusterValkeyCache
from django_valkey.cluster_cache.client import DefaultClusterClient


@pytest.fixture
def cache_client(cache: ClusterValkeyCache) -> Iterable[DefaultClusterClient]:
    client = cache.client
    client.set("TestClientClose", 0)
    yield client
    client.delete("TestClientClose")


class TestDefaultClusterClient:
    def test_delete_pattern_calls_get_client_given_no_client(self, mocker):
        mocker.patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
        get_client_mock = mocker.patch(
            "django_valkey.base_client.ClientCommands.get_client"
        )
        client = DefaultClusterClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""

        client.delete_pattern(pattern="{foo}*")
        get_client_mock.assert_called_once_with(write=True, tried=None)

    def test_delete_pattern_calls_make_pattern(self, mocker):
        mocker.patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
        get_client_mock = mocker.patch(
            "django_valkey.base_client.ClientCommands.get_client",
            return_value=mocker.Mock(),
        )
        make_pattern_mock = mocker.patch(
            "django_valkey.base_client.BaseClient.make_pattern"
        )
        client = DefaultClusterClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="{foo}*")

        kwargs = {"version": None, "prefix": None}
        make_pattern_mock.assert_called_once_with("{foo}*", **kwargs)

    def test_delete_pattern_calls_scan_iter_with_count_if_itersize_given(self, mocker):
        mocker.patch("django_valkey.base_client.BaseClient.__init__", return_value=None)
        get_client_mock = mocker.patch(
            "django_valkey.base_client.ClientCommands.get_client",
            return_value=mocker.Mock(),
        )
        make_pattern_mock = mocker.patch(
            "django_valkey.base_client.BaseClient.make_pattern"
        )
        client = DefaultClusterClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = []

        client.delete_pattern(pattern="{foo}*", itersize=90210)

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

        client = DefaultClusterClient()
        client._backend = mocker.Mock()
        client._backend.key_prefix = ""
        get_client_mock.return_value.scan_iter.return_value = [":1:{foo}", ":1:{foo}-a"]
        get_client_mock.return_value.pipeline.return_value = mocker.Mock()
        get_client_mock.return_value.pipeline.return_value.delete = mocker.Mock()
        get_client_mock.return_value.pipeline.return_value.execute = mocker.Mock()

        client.delete_pattern(pattern="{foo}*")

        assert get_client_mock.return_value.pipeline.return_value.delete.call_count == 2
        get_client_mock.return_value.pipeline.return_value.delete.assert_has_calls(
            [mocker.call(":1:{foo}"), mocker.call(":1:{foo}-a")]
        )
        get_client_mock.return_value.pipeline.return_value.execute.assert_called_once()
