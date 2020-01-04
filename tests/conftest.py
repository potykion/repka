import os

# Enable testdir fixture (https://docs.pytest.org/en/latest/reference.html#testdir)
pytest_plugins = "pytester"

DB_URL = os.environ["DB_URL"]
