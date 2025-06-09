# Raw operations

## Access the underlying valkey client
if for whatever reason you need to do things that the clients don't support, you can access the underlying valkey connections and send commands by hand
to get a connection, you can do:

```python
from django_valkey import get_valkey_connection

raw_client = get_valkey_connection("default")
```

in this example `"default"` is the alias name of the backend, that you configured in django's `CACHES` setting
the signature of the function is as follows:
```python
def get_valkey_connection(alias: str="default", write: bool=True, key=None): ...
```

`alias` is the name you gave each server in django's `CACHES` setting.
`write` is used to determine if the operation will write to the cache database, so it should be `True` for `set` and `False` for `get`.
`key` is only used with the shard client, it'll be explaind below.

### get raw access while using shard client
**note**: this only works as of v0.3.0

to get access to the client while using the shard client, you need to pass in the key you are going to work with,
this is because the shard client determines which server to use by the key.

**note**: if you are trying to use a key that was set by django_valkey's interface, you need to make the key before passing it to `get_valkey_connection`
we explain how to make a key below.


### raw operation utilities

as of `django_valkey` v0.3.0, we provide some additional utilities that might be of interest while doing raw operations:

```python
from django_valkey.util import make_key, make_pattern, encode, decode
```

#### make_key
`make_key` is used to create keys when we are setting a value in the cache server, and so the same operation is used to read that key.
`make_key` was an internal method, but as of v0.3.0 it's a function you can use easily

```python
from django.core.cache.backends.base import default_key_func  # this is the default key func, if you are using a custom one, use that instead
from django_valkey.util import make_key

make_key(
    key="my_key", 
    key_func=default_key_func,  # the default key func, customize based on your configs
    version=1,   # 1 is default, customize it based on your configs
    prefix="",  # default prefix, customize based on your config
)
```

the above call will generate a key that looks like `":1:my_key"` 
might be worthy to note that the return value of `make_key` is not a `str`, but a subclass of str, you can find it as `django_valkey.util.CacheKey`

to communicate with cache objects created by `django_valkey` in a raw operation, you should to use this function to make things easy,
but it's recommended to always use it to make things consistent and up to conventions.

if you don't want to handwrite the arguments, you can find the values used by your client from the `cache` object:
```python
from django.core.cache import cache

key_func = cache.key_func
version = cache.version
key_prefix = cache.key_prefix
```

#### make_pattern
`make_pattern` is used to make a pattern, which is used for searching and finding keys that are similar,
for example `foo*` will match `foo1`, `foo2`, `foo_something` and so one

`make_pattern` is used in operations such as `iter_keys`, `delete_pattern` and so on.

to use `make_pattern` notice the following example

```python
from django.core.cache.backends.base import default_key_func  # this is the default key func, if you are using a custom one, use that instead
from django_valkey.util import make_pattern

make_pattern(
    pattern="my_*",
    key_func=default_key_func,  # the default key func, customize based on your configs
    version=1,   # 1 is default, customize it based on your configs
    prefix="",  # default prefix, customize based on your config
)
```

if you don't want to handwrite the arguments, you can find the values used by your client from the `cache` object:
```python
from django.core.cache import cache

key_func = cache.key_func
version = cache.version
key_prefix = cache.key_prefix
```

#### encode and decode
`encode` and `decode` are called on values that will be saved/read (not on keys, only values)

encode does two things:
first it serializes the data
then it compresses that data

decode is the opposite:
first decompresses the data
then deserializes it

to use encode and decode, you need an instance of the serializer and compressor you are using

you can access the ones your config uses from `cache.client._compressor` and `cache.client._serializer`,
or you can pass in any object you want (note that you need to pass an instance, not the class)

```python
from django.core.cache import cache

from django_valkey.util import encode, decode

encode(value="my_value", serializer=cache.client._serializer, compressor=cache.client._compressor)
decode(value="my_value", serializer=cache.client._serializer, compressor=cache.client._compressor)
```

if you want to pass in the serializer and compressor by hand, you can instantiate one of the classes we provide and pass in the object, or instantiate your custom class,
just note that the classes need a dictionary of configurations to be passed to them to be instantiated, 
you can see what configs they get from the [serializer](../configure/advanced_configurations.md#configure-the-serializer) and [compressor](../configure/compressors.md) docs.