# repka

Python repository pattern implementation

## Installation

Via pip:

```
pip install repka
```

Via poetry:

```
poetry add repka
```


## Usage

See [/tests](https://github.com/potykion/repka/tree/master/tests) for **all** examples


### BaseRepository

This kind of repository used to work with psql via aiopg & pydantic transforming sql-rows to/from pydantic models:

```python
from typing import Any
import sqlalchemy as sa
from aiopg.sa import create_engine
from repka.api import BaseRepository, IdModel

# Define SA table
metadata = sa.MetaData()
transactions_table = sa.Table(
    "transactions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    ...
)

# Define pydantic model
class Transaction(IdModel):
    ...


# Define repository
class TransactionRepo(BaseRepository):
    table = transactions_table

    def deserialize(self, **kwargs: Any) -> Transaction:
        return Transaction(**kwargs)

# Create SA connection
connection_params = dict(user='aiopg', database='aiopg', host='127.0.0.1', password='passwd')
async with create_engine(**connection_params) as engine:
    async with engine.acquire() as conn:
        # Instantiate repository 
        repo = TransactionRepo(conn)
        # Now you can use the repo
        # Here we select first matching row from table and convert it to model
        transaction = await repo.first(transactions_table.c.id == 1)

```

### DictJsonRepo

This kind of repository used to save/load json objects from file:

```python
from repka.json_ import DictJsonRepo

repo = DictJsonRepo()

songs = [{"artist": "Pig Destroyer", "title": "Thumbsucker"}, {"artist": "Da Menace", "title": "Bag of Funk"}]
repo.write(songs, "songs.json")

assert repo.read("songs.json") == songs
```

## Tests 

To run tests:

1. Setup [database url](https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls) via `DATABASE_URL` environment variable  

**WARNING:** Every test run will drop all tables from db

2. Run tests via `pytest`

## Contribution

1. Create fork/branch for new feature/fix/whatever

2. Install pre-commit hooks: `pre-commit install` (for manual pre-commit run use`pre-commit run -a`)

3. When you done create pull request and wait for approval
