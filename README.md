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

```python
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
    entity_type = Transaction

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
