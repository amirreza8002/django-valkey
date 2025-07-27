import pytest


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


# this keeps the event loop open for the entire test suite
@pytest.fixture(scope="session", autouse=True)
async def keepalive(anyio_backend):
    pass
