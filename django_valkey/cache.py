from valkey import Valkey

from django_valkey.base import BaseValkeyCache, omit_exception
from django_valkey.client import DefaultClient

CONNECTION_INTERRUPTED = object()


class ValkeyCache(BaseValkeyCache[DefaultClient, Valkey]):
    DEFAULT_CLIENT_CLASS = "django_valkey.client.DefaultClient"

    @omit_exception
    def set(self, *args, **kwargs):
        return self.client.set(*args, **kwargs)

    @omit_exception
    def incr_version(self, *args, **kwargs):
        return self.client.incr_version(*args, **kwargs)

    @omit_exception
    def add(self, *args, **kwargs):
        return self.client.add(*args, **kwargs)

    def get(self, key, default=None, version=None, client=None):
        value = self._get(key, default, version, client)
        if value is CONNECTION_INTERRUPTED:
            value = default
        return value

    @omit_exception(return_value=CONNECTION_INTERRUPTED)
    def _get(self, key, default, version, client):
        return self.client.get(key, default=default, version=version, client=client)

    @omit_exception
    def delete(self, *args, **kwargs):
        result = self.client.delete(*args, **kwargs)
        return bool(result)

    @omit_exception
    def delete_many(self, *args, **kwargs):
        return self.client.delete_many(*args, **kwargs)

    @omit_exception
    def clear(self):
        return self.client.clear()

    @omit_exception(return_value={})
    def get_many(self, *args, **kwargs):
        return self.client.get_many(*args, **kwargs)

    @omit_exception
    def set_many(self, *args, **kwargs):
        return self.client.set_many(*args, **kwargs)

    @omit_exception
    def incr(self, *args, **kwargs):
        return self.client.incr(*args, **kwargs)

    @omit_exception
    def decr(self, *args, **kwargs):
        return self.client.decr(*args, **kwargs)

    @omit_exception
    def has_key(self, *args, **kwargs):
        return self.client.has_key(*args, **kwargs)

    @omit_exception
    def keys(self, *args, **kwargs):
        return self.client.keys(*args, **kwargs)

    @omit_exception
    def iter_keys(self, *args, **kwargs):
        return self.client.iter_keys(*args, **kwargs)

    @omit_exception
    def close(self):
        self.client.close()

    @omit_exception
    def touch(self, *args, **kwargs):
        return self.client.touch(*args, **kwargs)
