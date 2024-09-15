import pytest

from django_valkey import pool


@pytest.mark.parametrize(
    "connection_string",
    [
        "unix://tmp/foo.bar?db=1",
        "valkey://localhost/2",
        "valkeys://localhost:3333?db=2",
    ],
)
@pytest.mark.asyncio
async def test_connection_strings(connection_string: str):
    cf = pool.get_connection_factory(
        path="django_valkey.async_cache.pool.AsyncConnectionFactory", options={}
    )
    res = cf.make_connection_params(connection_string)
    assert res["url"] == connection_string
