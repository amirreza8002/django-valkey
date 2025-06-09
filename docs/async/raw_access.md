# Raw operations

## Access the underlying valkey client
if for whatever reason you need to do things that the clients don't support, you can access the underlying valkey connections and send commands by hand
to get a connection, you can do:

```python
from django_valkey.async_cache import get_valkey_connection

raw_client = await get_valkey_connection("default")
```

in this example `"default"` is the alias name of the backend, that you configured in django's `CACHES` setting
the signature of the function is as follows:
```python
async def get_valkey_connection(alias: str="default", write: bool=True): ...
```

`alias` is the name you gave each server in django's `CACHES` setting.
`write` is used to determine if the operation will write to the cache database, so it should be `True` for `set` and `False` for `get`.

### raw operation utilities
visit the [raw operation utilities](../commands/raw_access.md#raw-operation-utilities) of the sync documentations, the same concepts applies here as well.
