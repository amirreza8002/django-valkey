SECRET_KEY = "django_tests_secret_key"

CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": ["unix:///tmp/valkey.sock?db=1", "unix:///tmp/valkey.sock?db=1"],
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient"
        },
    },
    "doesnotexist": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:56379?db=1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient"
        },
    },
    "sample": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1,valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient"
        },
    },
    "with_prefix": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379?db=1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient"
        },
        "KEY_PREFIX": "test-prefix",
    },
}

INSTALLED_APPS = ["django.contrib.sessions"]

USE_TZ = False


ROOT_URLCONF = "tests.settings.urls"
