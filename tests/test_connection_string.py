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
def test_connection_strings(connection_string: str):
    cf = pool.get_connection_factory(
        options={"CONNECTION_FACTORY": "django_valkey.pool.ConnectionFactory"}
    )
    res = cf.make_connection_params(connection_string)
    assert res["url"] == connection_string
