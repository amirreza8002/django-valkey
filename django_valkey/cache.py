from valkey import Valkey

from django_valkey.base import (
    BaseValkeyCache,
    BackendCommands,
    decorate_all_methods,
    omit_exception,
)
from django_valkey.client import DefaultClient


@decorate_all_methods(omit_exception)
class DecoratedBackendCommands(BackendCommands):
    pass


class ValkeyCache(BaseValkeyCache[DefaultClient, Valkey], DecoratedBackendCommands):
    DEFAULT_CLIENT_CLASS = "django_valkey.client.DefaultClient"
