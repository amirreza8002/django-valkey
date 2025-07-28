import base64
import string
from datetime import timedelta
from typing import Optional, Type

import pytest

from django.conf import settings
from django.contrib.sessions.backends.base import SessionBase
from django.contrib.sessions.backends.cache import SessionStore as CacheSession
from django.core.cache import caches
from django.test import override_settings
from django.utils import timezone


pytestmark = pytest.mark.anyio

SessionType = Type[SessionBase]


# Copied from Django's sessions test suite. Keep in sync with upstream.
# https://github.com/django/django/blob/main/tests/sessions_tests/tests.py
class SessionTestsMixin:
    # This does not inherit from TestCase to avoid any tests being run with this
    # class, which wouldn't work, and to allow different TestCase subclasses to
    # be used.

    backend: Optional[SessionType] = None  # subclasses must specify

    @pytest.fixture(autouse=True)
    async def setup(self):
        self.session = self.backend()
        yield
        await self.session.adelete()

    def test_new_session(self):
        assert self.session.modified is False
        assert self.session.accessed is False

    async def test_get_empty(self):
        assert await self.session.aget("cat") is None

    async def test_store(self):
        await self.session.aset("cat", "dog")
        assert self.session.modified is True
        assert await self.session.apop("cat") == "dog"

    async def test_pop(self):
        await self.session.aset("some key", "exists")
        # Need to reset these to pretend we haven't accessed it:
        self.accessed = False
        self.modified = False

        assert await self.session.apop("some key") == "exists"
        assert self.session.accessed is True
        assert self.session.modified is True
        assert await self.session.aget("some key") is None

    async def test_pop_default(self):
        assert await self.session.apop("some key", "does not exist") == "does not exist"
        assert self.session.accessed is True
        assert self.session.modified is False

    async def test_pop_default_named_argument(self):
        assert (
            await self.session.apop("some key", default="does not exist")
            == "does not exist"
        )
        assert self.session.accessed is True
        assert self.session.modified is False

    async def test_pop_no_default_keyerror_raised(self):
        with pytest.raises(KeyError):
            await self.session.apop("some key")

    async def test_setdefault(self):
        assert await self.session.asetdefault("foo", "bar") == "bar"
        assert await self.session.asetdefault("foo", "baz") == "bar"
        assert self.session.accessed is True
        assert self.session.modified is True

    async def test_update(self):
        await self.session.aupdate({"update key": 1})
        assert self.session.accessed is True
        assert self.session.modified is True
        assert await self.session.aget("update key") == 1

    async def test_has_key(self):
        await self.session.aset("some key", 1)
        self.session.modified = False
        self.session.accessed = False

        assert await self.session.ahas_key("some key")
        assert self.session.accessed is True
        assert self.session.modified is False

    async def test_values(self):
        assert list(await self.session.avalues()) == []
        assert self.session.accessed is True
        await self.session.aset("some key", 1)
        self.session.modified = False
        self.session.accessed = False
        assert list(await self.session.avalues()) == [1]
        assert self.session.accessed is True
        assert self.session.modified is False

    async def test_keys(self):
        await self.session.aset("x", 1)
        self.session.modified = False
        self.session.accessed = False
        assert list(await self.session.akeys()) == ["x"]
        assert self.session.accessed is True
        assert self.session.modified is False

    async def test_items(self):
        await self.session.aset("x", 1)
        self.session.modified = False
        self.session.accessed = False
        assert list(await self.session.aitems()) == [("x", 1)]
        assert self.session.accessed is True
        assert self.session.modified is False

    async def test_clear(self):
        await self.session.aset("x", 1)
        self.session.modified = False
        self.session.accessed = False
        assert list(await self.session.aitems()) == [("x", 1)]
        self.session.clear()
        assert list(await self.session.aitems()) == []
        assert self.session.accessed is True
        assert self.session.modified is True

    async def test_save(self):
        await self.session.asave()
        assert await self.session.aexists(self.session.session_key)

    async def test_delete(self):
        await self.session.asave()
        await self.session.adelete(self.session.session_key)
        assert not await self.session.aexists(self.session.session_key)

    async def test_flush(self):
        await self.session.aset("foo", "bar")
        await self.session.asave()
        prev_key = self.session.session_key
        await self.session.aflush()
        assert not await self.session.aexists(prev_key)
        assert self.session.session_key != prev_key
        assert self.session.session_key is None
        assert self.session.accessed is True
        assert self.session.modified is True

    async def test_cycle(self):
        await self.session.aset("a", "c")
        await self.session.aset("b", "d")
        await self.session.asave()
        prev_key = self.session.session_key
        prev_data = list(await self.session.aitems())
        await self.session.acycle_key()
        assert not await self.session.aexists(prev_key)
        assert self.session.session_key != prev_key
        assert list(await self.session.aitems()) == prev_data

    async def test_cycle_with_no_session_cache(self):
        await self.session.aset("a", "c")
        await self.session.aset("b", "d")
        await self.session.asave()
        prev_data = await self.session.aitems()
        self.session = self.backend(self.session.session_key)

        assert not hasattr(self.session, "_session_cache")
        await self.session.acycle_key()
        assert await self.session.aitems() == prev_data

    async def test_save_doesnt_clear_data(self):
        await self.session.aset("a", "b")
        await self.session.asave()
        assert await self.session.aget("a") == "b"

    async def test_invalid_key(self):
        # Submitting an invalid session key (either by guessing, or if the db has
        # removed the key) results in a new key being generated.
        try:
            session = self.backend("1")
            await session.asave()
            assert session.session_key != "1"
            assert not await self.session.aget("cat")
            await session.adelete()
        finally:
            # Some backends leave a stale cache entry for the invalid
            # session key; make sure that entry is manually deleted
            await session.adelete("1")

    def test_session_key_empty_string_invalid(self):
        """Falsey values (Such as an empty string) are rejected."""
        self.session._session_key = ""
        assert not (self.session.session_key)

    def test_session_key_too_short_invalid(self):
        """Strings shorter than 8 characters are rejected."""
        self.session._session_key = "1234567"
        assert not (self.session.session_key)

    def test_session_key_valid_string_saved(self):
        """Strings of length 8 and up are accepted and stored."""
        self.session._session_key = "12345678"
        assert self.session.session_key == "12345678"

    async def test_session_key_is_read_only(self):
        async def set_session_key(session):
            session.session_key = await session._aget_new_session_key()

        with pytest.raises(AttributeError):
            await set_session_key(self.session)

    # Custom session expiry
    async def test_default_expiry(self):
        # A normal session has a max age equal to settings
        assert await self.session.aget_expiry_age() == settings.SESSION_COOKIE_AGE

        # So does a custom session with an idle expiration time of 0 (but it'll
        # expire at browser close)
        await self.session.aset_expiry(0)
        assert await self.session.aget_expiry_age() == settings.SESSION_COOKIE_AGE

    async def test_custom_expiry_seconds(self):
        modification = timezone.now()

        await self.session.aset_expiry(10)

        date = await self.session.aget_expiry_date(modification=modification)
        assert date == modification + timedelta(seconds=10)

        age = await self.session.aget_expiry_age(modification=modification)
        assert age == 10

    async def test_custom_expiry_timedelta(self):
        modification = timezone.now()

        # Mock timezone.now, because set_expiry calls it on this code path.
        original_now = timezone.now
        try:
            timezone.now = lambda: modification
            await self.session.aset_expiry(timedelta(seconds=10))
        finally:
            timezone.now = original_now

        date = await self.session.aget_expiry_date(modification=modification)
        assert date == modification + timedelta(seconds=10)

        age = await self.session.aget_expiry_age(modification=modification)
        assert age == 10

    async def test_custom_expiry_datetime(self):
        modification = timezone.now()

        await self.session.aset_expiry(modification + timedelta(seconds=10))

        date = await self.session.aget_expiry_date(modification=modification)
        assert date == modification + timedelta(seconds=10)

        age = await self.session.aget_expiry_age(modification=modification)
        assert age == 10

    async def test_custom_expiry_reset(self):
        await self.session.aset_expiry(None)
        await self.session.aset_expiry(10)
        await self.session.aset_expiry(None)
        assert await self.session.aget_expiry_age() == settings.SESSION_COOKIE_AGE

    async def test_get_expire_at_browser_close(self):
        # Tests get_expire_at_browser_close with different settings and different
        # set_expiry calls
        with override_settings(SESSION_EXPIRE_AT_BROWSER_CLOSE=False):
            await self.session.aset_expiry(10)
            assert await self.session.aget_expire_at_browser_close() is False

            await self.session.aset_expiry(0)
            assert await self.session.aget_expire_at_browser_close() is True

            await self.session.aset_expiry(None)
            assert await self.session.aget_expire_at_browser_close() is False

        with override_settings(SESSION_EXPIRE_AT_BROWSER_CLOSE=True):
            await self.session.aset_expiry(10)
            assert await self.session.aget_expire_at_browser_close() is False

            await self.session.aset_expiry(0)
            assert await self.session.aget_expire_at_browser_close() is True

            await self.session.aset_expiry(None)
            assert await self.session.aget_expire_at_browser_close() is True

    def test_decode(self):
        # Ensure we can decode what we encode
        data = {"a test key": "a test value"}
        encoded = self.session.encode(data)
        assert self.session.decode(encoded) == data

    def test_decode_failure_logged_to_security(self, caplog):
        bad_encode = base64.b64encode(b"flaskdj:alkdjf").decode("ascii")
        with caplog.at_level("WARNING", "django.security.SuspiciousSession"):
            assert {} == self.session.decode(bad_encode)
        # The failed decode is logged.
        assert "corrupted" in caplog.records[0].message

    async def test_actual_expiry(self):
        self.session = self.backend()  # reinitialize after overriding settings

        # Regression test for #19200
        old_session_key = None
        new_session_key = None
        try:
            await self.session.aset("foo", "bar")
            await self.session.aset_expiry(-timedelta(seconds=10))
            await self.session.asave()
            old_session_key = self.session.session_key
            # With an expiry date in the past, the session expires instantly.
            new_session = self.backend(self.session.session_key)
            new_session_key = new_session.session_key
            assert not await new_session.ahas_key("foo")
        finally:
            await self.session.adelete(old_session_key)
            await self.session.adelete(new_session_key)

    async def test_session_load_does_not_create_record(self):
        """
        Loading an unknown session key does not create a session record.
        Creating session records on load is a DOS vulnerability.
        """
        session = self.backend("someunknownkey")
        await session.aload()

        assert session.session_key is None
        assert not await session.aexists(session.session_key)
        # provided unknown key was cycled, not reused
        assert session.session_key != "someunknownkey"

    async def test_session_save_does_not_resurrect_session_logged_out_in_other_context(
        self,
    ):
        """
        Sessions shouldn't be resurrected by a concurrent request.
        """
        from django.contrib.sessions.backends.base import UpdateError

        # Create new session.
        s1 = self.backend()
        await s1.aset("test_data", "value1")
        await s1.asave(must_create=True)

        # Logout in another context.
        s2 = self.backend(s1.session_key)
        await s2.adelete()

        # Modify session in first context.
        await s1.aset("test_data", "value2")
        with pytest.raises(UpdateError):
            # This should throw an exception as the session is deleted, not
            # resurrect the session.
            await s1.asave()

        assert await s1.aload() == {}


class TestSession(SessionTestsMixin):
    backend = CacheSession

    async def test_load_overlong_key(self):
        self.session._session_key = (string.ascii_letters + string.digits) * 20
        assert await self.session.aload() == {}

    async def test_default_cache(self):
        await self.session.asave()
        assert await caches["default"].aget(self.session.cache_key) is not None

    @pytest.mark.filterwarnings("ignore:coroutine 'AsyncBackendCommands.close'")
    async def test_non_default_cache(self, settings):
        settings.CACHES = {
            "default": {
                "BACKEND": "django.core.cache.backends.dummy.DummyCache",
            },
            "sessions": {
                "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
                "LOCATION": "valkey://localhost:6379",
            },
        }
        settings.SESSION_CACHE_ALIAS = "sessions"
        # Re-initialize the session backend to make use of overridden settings.
        self.session = self.backend()

        await self.session.asave()
        assert await caches["default"].aget(self.session.cache_key) is None
        assert await caches["sessions"].get(self.session.cache_key) is not None

    async def test_create_and_save(self):
        self.session = self.backend()
        await self.session.acreate()
        await self.session.asave()
        assert await caches["default"].get(self.session.cache_key) is not None
