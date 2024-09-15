=========================
Customizing django-valkey
=========================

The basics of how to intrduce your own classes to be used by django-valkeey has been discussed in length in :doc:`configure/advanced_configurations`.

in this section we're going to look at two base classes that django-valkey provides and you can use them to write your classes faster.

django-valkey comes with two base classes: ``django_valkey.base.BaseValkeyCache`` and ``django_valkey.base_pool.BaseConnectionPool``.

BaseValkeyCache
###############

``BaseValkeyCache`` inherits from django's ``BaseCache`` class and ``typing.Generic`` .
``BaseCache`` adds basic cache functionality, such as ``get()`` and ``set()``, and ``typing.Generic`` allows for a more robust type hinting.
``BaseValkeyCache`` adds more valkey oriented methods to BaseCache, things like ``expire()`` and ``get_lock()``, and uses Generic to type hint two things:
#. the client, such as ``django_valkey.client.default.DefaultClient``.
#. the underlying backend, such as ``valkey.Valkey``.

to inherit from this base class you can take the example of our own cache backend:

.. code-block:: python

    from valkey import Valkey
    from django_valkey.client import DefaultClient

    class ValkeyCache(BaseValkeyCache[DefaultClient, Valkey]):
        DEFAULT_CLIENT_CLASS = "django_valkey.client.DefaultClient"
        ...

the class attribute defined in the example is **mandatory**, it is so we can have imports in other modules.

``BaseValkeyCache`` has both *sync* and *async* methods implemented, but there is no logic in them, most methods need to be overwritten.


BaseConnectionPool
##################

the ``BaseConnectionPool`` inherits from ``typing.Generic`` to give more robust type hinting, and allow our four connection pools to have cleaner codebase.

to inherit from this class you need to pass in the underlying backend that you are using and the connection pool, for example this is one of the connection pools in this project:

.. code-block:: python

    from valkey import Valkey
    from valkey.connection import ConnectionPool


    class ConnectionFactory(BaseConnectionPool[Valkey, ConnectionPool]):
        path_pool_cls = "valkey.connection.ConnectionPool"
        path_base_cls = "valkey.client.Valkey"

the two class attributes defined there are also **mandatory** since they are passed to other modules.

this base class has eight methods implemented, but four of them raise ``NotImplementedError``, so lets have a look at those:

#. ``connect()`` this method can be both sync and async, depending on your work.
#. ``disconnect()`` this method, as well, can be both sync and async.
#. ``get_connection()`` in our implementation, connect() calls this method to get the connection, it also can be both sync and async, you can omit this one tho
#. ``get_parser_cls()`` this method can only be sync, it return a parser class (and not object)
