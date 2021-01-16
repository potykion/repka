import os
import asyncio
import sys
from typing import Iterator

import pytest

# Enable testdir fixture (https://docs.pytest.org/en/latest/reference.html#testdir)
pytest_plugins = "pytester"


@pytest.fixture()
def db_url() -> str:
    return os.environ["DB_URL"]


@pytest.fixture
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """
    On py38 win32 WindowsProactorEventLoopPolicy is default event loop
    This loop doesn't implement some methods that aiopg use
    So this fixture set WindowsSelectorEventLoopPolicy as default event loop

    Set custom event loop policy for pytest-asyncio:
    https://github.com/pytest-dev/pytest-asyncio/pull/174#issuecomment-650800383
    """
    if sys.platform == 'win32':
        policy = asyncio.WindowsSelectorEventLoopPolicy()
    else:
        policy = asyncio.DefaultEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
    asyncio.set_event_loop_policy(None)
