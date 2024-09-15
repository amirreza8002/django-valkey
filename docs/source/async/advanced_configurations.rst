============================
Advanced Async Configuration
============================

most of the subject discussed in :doc:`../configure/advanced_configurations` apply to async mode as well, just don't use a sync client :)

also all the compressor details we talked about in :doc:`../configure/compressors` work as is in async mode


Clients
#######

as of now, we only have one async client, ``AsyncDefaultClient``, available in ``django_valkey.async_cache.client.default``.
the default client can also be used with sentinels, as we'll discuss later.

the ``AsyncDefaultClient`` is configured by default by ``AsyncValkeyCache``, so if you have configured that as your backend you are all set, but if you want to be explicit or use the client with a different backend you can write it like this:

.. code-block:: python

    CACHES = {
        "default": {
            "BACKEND": "path.to.backend",
            "LOCATION": [
                "valkey://user:pass@127.0.0.1:6379",
                ]
            "OPTIONS": {
                "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncDefaultClient",
                }
            }
        }

or you can replace the client with your own like that.

Connection Factory
##################

django_valkey's async library comes with two connection factories, ``AsyncConnectionFactory`` for general uses and ``AsyncSentinelConnectionFactory`` for sentinel uses.

the default connection factory is ``AsyncConnectionFactory``, so if you are using a sentinel server you should configure your caches like this:

.. code-block:: python

    CACHES = {
        "default": {
            ...
            "OPTIONS": {
                "CONNECTION_FACTORY": "django_valkey.async_cache.pool.AsyncSentinelConnectionFactory"
                }
            }
        }

or set it as the global connection factory like this:

.. code-block:: python

    DJANGO_VALKEY_CONNECTION_FACTORY = "django_valkey.async_cache.client.default.AsyncDefaultClient"

note that ``"CONNECTION_FACTORY"`` overrides ``DJANGO_VALKEY_CONNECTION_FACTORY`` for the specified server.

if you want to use another factory you can use the same code with the path to your class.

