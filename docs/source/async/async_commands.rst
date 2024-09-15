======================
Valkey native commands
======================

you can directly work with valkey using django's cache object.

most subject discussed in :doc:`../commands/valkey_native_commands` also applies here.

.. code-block:: python

    from django.core.cache import cache

    await cache.get("foo")


the method names are the same as the sync ones discussed in :doc:`../commands/valkey_native_commands`, and the API is almost the same.

the only difference is that the async backend returns a coroutine or async generator depending on the method, and you should ``await`` it or iterate over it.

.. code-block:: python

    from django.core.cache import cache

    async with contextlib.aclosing(cache.iter_keys("foo*")) as keys:
        async for k in keys:
            result.add(k)

another thing to notice is that the method names are the same as the sync ones: ``await get()`` or ``get()``.
but if you want to be explicit the same methods are also available with a ``a`` prepended to them: ``await aget()``.
this goes for all public methods of the async client.