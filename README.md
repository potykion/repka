# repka

![ci](https://travis-ci.org/potykion/repka.svg?branch=master)

Repository pattern implementation - isolate db manipulation from domain models

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

### repka.api.BaseRepository

BaseRepository used to execute sql-queries (via [aiopg & sqlalchemy](https://github.com/aio-libs/aiopg)) and convert sql-rows to/from [pydantic](https://github.com/samuelcolvin/pydantic) models

```python
import sqlalchemy as sa
from repka.api import BaseRepository, IdModel
from repka.utils import create_async_db_connection

# Define pydantic model
# It should inherit repka.api.IdModel 
#   to set id on entity insert, to update entity with id and more
# IdModel inherits pydantic.BaseModel and defines int id field
class Task(IdModel):
    title: str

# Define sqlachemy table with same model columns
metadata = sa.MetaData()
tasks_table = sa.Table(
    "tasks", metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("title", sa.String)
)

# Define repository
# You should inherit repka.api.BaseRepository 
#   set sqlalchemy-table via table property and 
#   set deserialize method which create pydantic model from kwargs 
# Kwargs is sql-row data returned by sqlalchemy  
class TaskRepo(BaseRepository[Task]):
    table = tasks_table

    def deserialize(self, **kwargs):
        return Task(**kwargs)

# To use the repository you should instantiate it with async sqlalchemy-connection
db_url = "postgresql://postgres@localhost/test"
async with create_async_db_connection(db_url) as conn:
    repo = TaskRepo(conn)

    # Now you can use the repo
    # Here we select first task with matching title
    task = await repo.first(tasks_table.c.title == "My first task")
```

#### BaseRepository methods

>`T` means generic type passed to BaseRepository (e.g. `BaseRepository[Task]` means that type of `T` is `Task`)

##### Select methods

- `repo.first(*filters: BinaryExpression, orders: Optional[Columns])` - get first entity matching sqlalchemy {filters} and {orders}; if no entity matches {filters} then `None` is returned
    
    > Example of {filters}: `table.c.title == 'test task'` - equals to sql where clause: `where title = 'test task'` 
 
    > Example of {orders}: `table.c.title` - equals to sql order by clause: `order by title`
 
- `repo.get_by_ids(entity_ids: List[int])` - get all entities whose id in {entity_ids} (same as sql `where id in ({entity_ids})`)
- `repo.get_by_id(entity_id: int)` - get entity with id = {entity_id}
- `repo.get_or_create(filters: Optional[List[BinaryExpression]], defaults: Optional[Dict])` - get entity that matches {filters} if no entity found create new entity with {defaults}; return tuple of entity and entity existence flag
- `repo.get_all(filters: Optional[List[BinaryExpression]], orders: Optional[Columns])` - return all entities matching {filters} and {orders}
- `repo.get_all_ids(filters: Optional[List[BinaryExpression]], orders: Optional[Columns])` - return ids of entites matching {filters} and {orders}
- `repo.exists(*filters: BinaryExpression)` - check that entity matching {filters} exists using sql `count` statement

##### Insert methods

- `repo.insert(entity: T)` - insert entity to db table and set id field to the entity
- `repo.insert_many(entities: List[T])` - insert multiple entities and set ids to them in single transaction

##### Update methods

- `repo.update(entity: T)` - updates entity in db
- `repo.update_partial(entity: T, **updated_values)` - update entity fields via kwargs and update entity fields in db
- `repo.update_many(entities: List[T])` - update multiple entities in single transaction

##### Delete methods

- `repo.delete(*filters: BinaryExpression)` - delete entities matching {filters} via sql `delete` statement
- `repo.delete_by_id(entity_id: int)` - delete entity with {entity_id}
- `repo.delete_by_ids(entity_ids: List[int])` - delete entities whose id in {entity_ids}

##### Other methods & properties

- `repo.serialize(entity: T)` - convert {entity} to dict (e.g. in `insert` and `update` methods)  
- `repo.deserialize(**kwargs)` - convert {kwargs} to entity (e.g. in `first` and `get_all` methods)
- `repo.execute_in_transaction()` - context manager that allows execute multiple queries in transaction 

    Example: delete all old entities and insert new one in single transaction:
    
    ```python
    async with repo.execute_in_transaction():
      await repo.delete()
      await repo.insert(Task(title="New task"))
    ``` 
  
- `repo.ignore_insert` - list of entity fields that will be ignored on insert and set after insert, useful for auto incrementing / default fields like dates or sequence numbers

#### repka.api.ConnectionVarMixin

ConnectionVarMixin allows you to create lazy-connection repositories with context vars

```python
from contextvars import ContextVar
from repka.api import ConnectionVarMixin, BaseRepository  
from repka.utils import create_async_db_connection

# Add mixin to repository 
class TaskRepo(ConnectionVarMixin, BaseRepository[Task]):
    table = tasks_table

    def deserialize(self, **kwargs):
        return Task(**kwargs)

# Create context var and instantiate repository
db_connection = ContextVar("db_connection")
repo = TaskRepo(db_connection)

# Now you should set the context var somewhere (e.g. in middleware)
#   And start using the repository
async with create_async_db_connection(db_url) as conn:
    db_connection.set(conn)

    await repo.insert(Task(title="New task"))
```

### repka.json_.DictJsonRepo

This kind of repository used to save/load json objects from file:

```python
from repka.json_ import DictJsonRepo

repo = DictJsonRepo()

songs = [{"artist": "Pig Destroyer", "title": "Thumbsucker"}, {"artist": "Da Menace", "title": "Bag of Funk"}]
repo.write(songs, "songs.json")

assert repo.read("songs.json") == songs
```

## Development and contribution

### Dependencies 

Install production and development dependencies via poetry:

```
poetry install
```

### Tests 

To run tests:

1. Setup [database url](https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls) via `DB_URL` environment variable (e.g. via .env file)

**WARNING:** Every test run will drop all tables from the db

2. Run tests via `pytest`

### Contribution

1. Create fork/branch for new feature/fix/whatever

2. [Optional] Install pre-commit hooks: `pre-commit install` (for manual pre-commit run use`pre-commit run -a`)

3. When you done create pull request and wait for approval

### Deploy

To deploy new version you need to increment version via bump2version and publish it to PyPI via poetry:

```
bump2version major/minor/patch
poetry publish --build
``` 

Don't forget to fill the CHANGELOG.md before release 