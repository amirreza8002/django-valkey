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

from django_valkey.cache import ValkeyCache

SessionType = Type[SessionBase]


# Copied from Django's sessions test suite. Keep in sync with upstream.
# https://github.com/django/django/blob/main/tests/sessions_tests/tests.py
class SessionTestsMixin:
    # This does not inherit from TestCase to avoid any tests being run with this
    # class, which wouldn't work, and to allow different TestCase subclasses to
    # be used.

    backend: Optional[SessionType] = None  # subclasses must specify

    @pytest.fixture(autouse=True)
    def setup(self):
        self.session = self.backend()
        yield
        self.session.delete()

    def test_new_session(self):
        assert self.session.modified is False
        assert self.session.accessed is False

    def test_get_empty(self):
        assert self.session.get("cat") is None

    def test_store(self):
        self.session["cat"] = "dog"
        assert self.session.modified is True
        assert self.session.pop("cat") == "dog"

    def test_pop(self):
        self.session["some key"] = "exists"
        # Need to reset these to pretend we haven't accessed it:
        self.accessed = False
        self.modified = False

        assert self.session.pop("some key") == "exists"
        assert self.session.accessed is True
        assert self.session.modified is True
        assert self.session.get("some key") is None

    def test_pop_default(self):
        assert self.session.pop("some key", "does not exist") == "does not exist"
        assert self.session.accessed is True
        assert self.session.modified is False

    def test_pop_default_named_argument(self):
        assert (
            self.session.pop("some key", default="does not exist") == "does not exist"
        )
        assert self.session.accessed is True
        assert self.session.modified is False

    def test_pop_no_default_keyerror_raised(self):
        with pytest.raises(KeyError):
            self.session.pop("some key")

    def test_setdefault(self):
        assert self.session.setdefault("foo", "bar") == "bar"
        assert self.session.setdefault("foo", "baz") == "bar"
        assert self.session.accessed is True
        assert self.session.modified is True

    def test_update(self):
        self.session.update({"update key": 1})
        assert self.session.accessed is True
        assert self.session.modified is True
        assert self.session.get("update key") == 1

    def test_has_key(self):
        self.session["some key"] = 1
        self.session.modified = False
        self.session.accessed = False

        assert "some key" in self.session
        assert self.session.accessed is True
        assert self.session.modified is False

    def test_values(self):
        assert list(self.session.values()) == []
        assert self.session.accessed is True
        self.session["some key"] = 1
        self.session.modified = False
        self.session.accessed = False
        assert list(self.session.values()) == [1]
        assert self.session.accessed is True
        assert self.session.modified is False

    def test_keys(self):
        self.session["x"] = 1
        self.session.modified = False
        self.session.accessed = False
        assert list(self.session.keys()) == ["x"]
        assert self.session.accessed is True
        assert self.session.modified is False

    def test_items(self):
        self.session["x"] = 1
        self.session.modified = False
        self.session.accessed = False
        assert list(self.session.items()) == [("x", 1)]
        assert self.session.accessed is True
        assert self.session.modified is False

    def test_clear(self):
        self.session["x"] = 1
        self.session.modified = False
        self.session.accessed = False
        assert list(self.session.items()) == [("x", 1)]
        self.session.clear()
        assert list(self.session.items()) == []
        assert self.session.accessed is True
        assert self.session.modified is True

    def test_save(self):
        self.session.save()
        assert self.session.exists(self.session.session_key)

    def test_delete(self):
        self.session.save()
        self.session.delete(self.session.session_key)
        assert not self.session.exists(self.session.session_key)

    def test_flush(self):
        self.session["foo"] = "bar"
        self.session.save()
        prev_key = self.session.session_key
        self.session.flush()
        assert not self.session.exists(prev_key)
        assert self.session.session_key != prev_key
        assert self.session.session_key is None
        assert self.session.accessed is True
        assert self.session.modified is True

    def test_cycle(self):
        self.session["a"], self.session["b"] = "c", "d"
        self.session.save()
        prev_key = self.session.session_key
        prev_data = list(self.session.items())
        self.session.cycle_key()
        assert not self.session.exists(prev_key)
        assert self.session.session_key != prev_key
        assert list(self.session.items()) == prev_data

    def test_cycle_with_no_session_cache(self):
        self.session["a"], self.session["b"] = "c", "d"
        self.session.save()
        prev_data = self.session.items()
        self.session = self.backend(self.session.session_key)

        assert not hasattr(self.session, "_session_cache")
        self.session.cycle_key()
        assert self.session.items() == prev_data

    def test_save_doesnt_clear_data(self):
        self.session["a"] = "b"
        self.session.save()
        assert self.session["a"] == "b"

    def test_invalid_key(self):
        # Submitting an invalid session key (either by guessing, or if the db has
        # removed the key) results in a new key being generated.
        try:
            session = self.backend("1")
            session.save()
            assert session.session_key != "1"
            assert not self.session.get("cat")
            session.delete()
        finally:
            # Some backends leave a stale cache entry for the invalid
            # session key; make sure that entry is manually deleted
            session.delete("1")

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

    def test_session_key_is_read_only(self):
        def set_session_key(session):
            session.session_key = session._get_new_session_key()

        with pytest.raises(AttributeError):
            set_session_key(self.session)

    # Custom session expiry
    def test_default_expiry(self):
        # A normal session has a max age equal to settings
        assert self.session.get_expiry_age() == settings.SESSION_COOKIE_AGE

        # So does a custom session with an idle expiration time of 0 (but it'll
        # expire at browser close)
        self.session.set_expiry(0)
        assert self.session.get_expiry_age() == settings.SESSION_COOKIE_AGE

    def test_custom_expiry_seconds(self):
        modification = timezone.now()

        self.session.set_expiry(10)

        date = self.session.get_expiry_date(modification=modification)
        assert date == modification + timedelta(seconds=10)

        age = self.session.get_expiry_age(modification=modification)
        assert age == 10

    def test_custom_expiry_timedelta(self):
        modification = timezone.now()

        # Mock timezone.now, because set_expiry calls it on this code path.
        original_now = timezone.now
        try:
            timezone.now = lambda: modification
            self.session.set_expiry(timedelta(seconds=10))
        finally:
            timezone.now = original_now

        date = self.session.get_expiry_date(modification=modification)
        assert date == modification + timedelta(seconds=10)

        age = self.session.get_expiry_age(modification=modification)
        assert age == 10

    def test_custom_expiry_datetime(self):
        modification = timezone.now()

        self.session.set_expiry(modification + timedelta(seconds=10))

        date = self.session.get_expiry_date(modification=modification)
        assert date == modification + timedelta(seconds=10)

        age = self.session.get_expiry_age(modification=modification)
        assert age == 10

    def test_custom_expiry_reset(self):
        self.session.set_expiry(None)
        self.session.set_expiry(10)
        self.session.set_expiry(None)
        assert self.session.get_expiry_age() == settings.SESSION_COOKIE_AGE

    def test_get_expire_at_browser_close(self):
        # Tests get_expire_at_browser_close with different settings and different
        # set_expiry calls
        with override_settings(SESSION_EXPIRE_AT_BROWSER_CLOSE=False):
            self.session.set_expiry(10)
            assert self.session.get_expire_at_browser_close() is False

            self.session.set_expiry(0)
            assert self.session.get_expire_at_browser_close() is True

            self.session.set_expiry(None)
            assert self.session.get_expire_at_browser_close() is False

        with override_settings(SESSION_EXPIRE_AT_BROWSER_CLOSE=True):
            self.session.set_expiry(10)
            assert self.session.get_expire_at_browser_close() is False

            self.session.set_expiry(0)
            assert self.session.get_expire_at_browser_close() is True

            self.session.set_expiry(None)
            assert self.session.get_expire_at_browser_close() is True

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

    def test_actual_expiry(self):
        self.session = self.backend()  # reinitialize after overriding settings

        # Regression test for #19200
        old_session_key = None
        new_session_key = None
        try:
            self.session["foo"] = "bar"
            self.session.set_expiry(-timedelta(seconds=10))
            self.session.save()
            old_session_key = self.session.session_key
            # With an expiry date in the past, the session expires instantly.
            new_session = self.backend(self.session.session_key)
            new_session_key = new_session.session_key
            assert "foo" not in new_session
        finally:
            self.session.delete(old_session_key)
            self.session.delete(new_session_key)

    def test_session_load_does_not_create_record(self):
        """
        Loading an unknown session key does not create a session record.
        Creating session records on load is a DOS vulnerability.
        """
        session = self.backend("someunknownkey")
        session.load()

        assert session.session_key is None
        assert not session.exists(session.session_key)
        # provided unknown key was cycled, not reused
        assert session.session_key != "someunknownkey"

    def test_session_save_does_not_resurrect_session_logged_out_in_other_context(self):
        """
        Sessions shouldn't be resurrected by a concurrent request.
        """
        from django.contrib.sessions.backends.base import UpdateError

        # Create new session.
        s1 = self.backend()
        s1["test_data"] = "value1"
        s1.save(must_create=True)

        # Logout in another context.
        s2 = self.backend(s1.session_key)
        s2.delete()

        # Modify session in first context.
        s1["test_data"] = "value2"
        with pytest.raises(UpdateError):
            # This should throw an exception as the session is deleted, not
            # resurrect the session.
            s1.save()

        assert s1.load() == {}


class TestSession(SessionTestsMixin):
    backend = CacheSession

    def test_load_overlong_key(self):
        self.session._session_key = (string.ascii_letters + string.digits) * 20
        assert self.session.load() == {}

    def test_default_cache(self):
        self.session.save()
        assert caches["default"].get(self.session.cache_key) is not None

    @pytest.mark.skipif(
        caches["default"].client is not ValkeyCache,
        reason="settings is set for normal server",
    )
    def test_non_default_cache(self, settings):
        settings.CACHES = {
            "default": {
                "BACKEND": "django.core.cache.backends.dummy.DummyCache",
            },
            "sessions": {
                "BACKEND": "django_valkey.cache.ValkeyCache",
                "LOCATION": "valkey://localhost:6379",
            },
        }
        settings.SESSION_CACHE_ALIAS = "sessions"
        # Re-initialize the session backend to make use of overridden settings.
        self.session = self.backend()

        self.session.save()
        assert caches["default"].get(self.session.cache_key) is None
        assert caches["sessions"].get(self.session.cache_key) is not None

    def test_create_and_save(self):
        self.session = self.backend()
        self.session.create()
        self.session.save()
        assert caches["default"].get(self.session.cache_key) is not None
