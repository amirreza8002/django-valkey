from typing import Iterable

import pytest
from django.core.cache import cache as default_cache

from django_valkey.cache import BaseValkeyCache


@pytest.fixture
def cache() -> Iterable[BaseValkeyCache]:
    yield default_cache
    default_cache.clear()
