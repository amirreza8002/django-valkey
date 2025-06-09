# Customizing django-valkey

The basics of how to introduce your own classes to be used by django-valkey has been discussed in length in [Advanced Configuration](configure/advanced_configurations.md).

in this section we're going to look at the base classes that django-valkey provides, and you can use them to write your classes faster.

django-valkey comes with three base classes: `django_valkey.base.BaseValkeyCache`, `django_valkey.base_client.BaseClient` and `django_valkey.base_pool.BaseConnectionFactory`.

## BaseValkeyCache

`BaseValkeyCache` is not a standalone class, to make use of it you need to add the actual methods, in `django-valkey` this is done by `django_valkey.base.BackendCommands` or `django_valkey.base.AsyncBackendCommands` depending if you use sync or async clients
`BaseValkeyCache` contains connection methods and configures the behaviour of the cache
`BaseValkeyCache` inherits from `typing.Generic` to type hint two things:
1. the client, such as `django_valkey.client.default.DefaultClient`.
2. the underlying backend, such as `valkey.Valkey`.

to inherit from this base class you can take the example of our own cache backend:

```python
from valkey import Valkey

from django_valkey.base import BaseValkeyCache, BackendCommands
from django_valkey.client import DefaultClient

class ValkeyCache(BaseValkeyCache[DefaultClient, Valkey], BackendCommands):
    DEFAULT_CLIENT_CLASS = "django_valkey.client.DefaultClient"
    ...
```        

the `DEFAULT_CLIENT_CLASS` class attribute defined in the example is **mandatory**, it is so we can have imports in other modules.

`BaseValkeyCache` can work with both *sync* and *async* subclasses, but it doesn't implement any of the methods, you need to inherit the command classes for this to work.


## BaseClient
like `BaseValkeyCache`, `BaseClient` is not a standalone class.
this class has all the logic necessary to connect to a cache server, and utility methods that helps with different operations,
but it does not handle any of the operations by itself, you need one of `django_valkey.base_client.ClientCommands` or `django_valkey.base_client.AsyncClientCommands` for sync or async clients, respectively.
the command classes implement the actual operations such as `get` and `set`.

`BaseClient` inherits from `typing.Generic` to make cleaner type hints.
the `typing.Generic` needs a backend to be passed in, e.g: `valkey.Valkey`

the base class also needs the subclasses to have a `CONNECTION_FACTORY_PATH` class variable pointing to the connection factory class.

an example code would look like this:

```python
from valkey import Valkey

from django_valkey.base_client import BaseClient, ClientCommands

class DefaultClient(BaseClient[Valkey], ClientCommands[Valkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.ConnectionFactory"
```

*note* that CONNECTION_FACTORY_PATH is only used if `DJANGO_VALKEY_CONNECTION_FACTORY` is not set.

`BaseClient` can work with both sync and async subclasses, you would use one of `django_valkey.base_client.ClientCommands` for sync, and `django_valkey.base_client.AsyncClientCommands` for async clients.


## BaseConnectionFactory

the `BaseConnectionFactory` inherits from `typing.Generic` to give more robust type hinting, and allow our four connection pools to have cleaner codebase.

to inherit from this class you need to pass in the underlying backend that you are using and the connection pool, for example this is one of the connection pools in this project:

```python
from valkey import Valkey
from valkey.connection import ConnectionPool

from django_valkey.base_pool import BaseConnectionFactory


class ConnectionFactory(BaseConnectionFactory[Valkey, ConnectionPool]):
    path_pool_cls = "valkey.connection.ConnectionPool"
    path_base_cls = "valkey.client.Valkey"
```

the two class attributes defined there are also **mandatory** since they are passed to other modules.

this base class has eight methods implemented, but four of them raise `NotImplementedError`, so let's have a look at those:

1. `connect()` this method can be both sync and async, depending on your work.
2. `disconnect()` this method, as well, can be both sync and async.
3. `get_connection()` in our implementation, connect() calls this method to get the connection, it also can be both sync and async, you can omit this one tho
4. `get_parser_cls()` this method can only be sync, it returns a parser class (and not object)
