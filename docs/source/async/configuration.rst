==========================
Configure The Async Client
==========================

**Important**: the async client is not compatible with django's cache middlewares.
if you need the middlewares, consider using the sync client or implement a new middleware.

there are two async clients available, a normal client and a herd client.

Default client
##############

to setup the async client you can configure your settings file to look like this:

.. code-block:: python

    CACHES = {
        "default": {
            "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
            "LOCATION": "valkey://127.0.0.1:6379",
            "OPTIONS": {...},
            },
        }

take a look at :ref:`url` to see other ways to write the URL.
And that's it, the backend defaults to use AsyncDefaultClient as client interface, AsyncConnectionFactory as connection factory and valkey-py's async client.

you can, of course configure it to use any other class, or pass in extras args and kwargs, the same way that was discussed at :doc:`../configure/advanced_configurations`.

Herd client
###########

to set up herd client configure your settings like this:

.. code-block:: python

    CACHES = {
        "default": {
            "BACKEND": "django_valkey.async_cache.caches.AsyncValkeyCache",
            "LOCATION": "valkey://127.0.0.1:6379",
            "OPTIONS": {
                "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncHerdClient",
                },
            },
    }

for a more specified guide look at :doc:`advanced_configurations`.