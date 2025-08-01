import pytest

from django.core import signals
from django.core.cache import close_caches

from django_valkey.base import close_async_caches
from django_valkey.async_cache.cache import AsyncValkeyCache


pytestmark = pytest.mark.anyio


class TestWithOldSignal:
    @pytest.fixture(autouse=True)
    def setup(self):
        signals.request_finished.disconnect(close_async_caches)
        signals.request_finished.connect(close_caches)
        yield
        signals.request_finished.disconnect(close_caches)
        signals.request_finished.connect(close_async_caches)

    def test_old_receiver_is_registered_and_new_receiver_unregistered(self, setup):
        sync_receivers, async_receivers = signals.request_finished._live_receivers(None)
        assert close_caches in sync_receivers
        assert close_async_caches not in async_receivers

    async def test_warning_output_when_request_finished(self, async_client):
        with pytest.warns(
            RuntimeWarning,
            match="coroutine 'AsyncBackendCommands.close' was never awaited",
        ) as record:
            await async_client.get("/async/")

        assert (
            str(record[0].message)
            == "coroutine 'AsyncBackendCommands.close' was never awaited"
        )

    async def test_manually_await_signal(self, recwarn):
        await signals.request_finished.asend(self.__class__)
        assert len(recwarn) == 1

        assert (
            str(recwarn[0].message)
            == "coroutine 'AsyncBackendCommands.close' was never awaited"
        )

    # TODO: find why garbage collector doesn't collect the coroutine when the method is
    # sync (even when gc is called manually, it doesn't collect)
    async def test_manually_call_signal(self):
        with pytest.warns(
            RuntimeWarning,
            match="coroutine 'AsyncBackendCommands.close' was never awaited",
        ) as record:
            signals.request_finished.send(self.__class__)
        assert len(record) == 1

        assert (
            str(record[0].message)
            == "coroutine 'AsyncBackendCommands.close' was never awaited"
        )


class TestWithNewSignal:
    async def test_warning_output_when_request_finished(self, async_client, recwarn):
        await async_client.get("/async/")

        assert len(recwarn) == 0

    async def test_manually_await_signal(self, recwarn):
        await signals.request_finished.asend(self.__class__)
        assert len(recwarn) == 0

    def test_manually_call_signal(self, recwarn):
        signals.request_finished.send(self.__class__)
        assert len(recwarn) == 0

    def test_receiver_is_registered_and_old_receiver_unregistered(self):
        sync_receivers, async_receivers = signals.request_finished._live_receivers(None)
        assert close_async_caches in async_receivers
        assert close_caches not in sync_receivers

    async def test_close_is_called_by_signal(self, mocker):
        close_spy = mocker.spy(AsyncValkeyCache, "close")
        await signals.request_finished.asend(self.__class__)
        assert close_spy.await_count == 1
        assert close_spy.call_count == 1
