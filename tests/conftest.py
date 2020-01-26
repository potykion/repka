import os

import pytest

# Enable testdir fixture (https://docs.pytest.org/en/latest/reference.html#testdir)
pytest_plugins = "pytester"


@pytest.fixture()
def db_url() -> str:
    return os.environ["DB_URL"]
