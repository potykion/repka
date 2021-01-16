import datetime as dt
import operator
from contextlib import suppress
from contextvars import ContextVar
from typing import Optional, List, Union

import pytest
import sqlalchemy as sa
from aiopg.sa import create_engine, SAConnection
from pydantic import validator

from repka.api import BaseRepository, IdModel

# Enable async tests (https://github.com/pytest-dev/pytest-asyncio#pytestmarkasyncio)
from repka.repositories.aiopg_ import AiopgQueryExecutor

pytestmark = pytest.mark.asyncio


class Transaction(IdModel):
    date: dt.date = None  # type: ignore
    price: int

    @validator("date", pre=True, always=True)
    def set_now_if_no_date(cls, value: Optional[dt.date]) -> dt.date:
        if value:
            return value

        return dt.datetime.now().date()


metadata = sa.MetaData()

transactions_table = sa.Table(
    "transactions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("date", sa.Date),
    sa.Column("price", sa.Integer),
)


class TransactionRepo(BaseRepository[Transaction]):
    table = transactions_table

    async def sum(self) -> int:
        query = sa.select([sa.func.sum(transactions_table.c.price)])
        sum_ = await self._connection.scalar(query)
        return sum_


class TransactionRepoWithConnectionMixin(BaseRepository[Transaction]):
    table = transactions_table


class UnionModel(IdModel):
    int_or_str: Union[int, str]


int_models_table = sa.Table(
    "int_models",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("int_or_str", sa.Integer),
)


class UnionModelRepo(BaseRepository[UnionModel]):
    table = int_models_table


class DefaultFieldsModel(IdModel):
    a: int = 0
    b: Optional[str]
    seq_field: int = 0


default_fields_seq = sa.Sequence("default_fields_seq", metadata=metadata)

default_fields_table = sa.Table(
    "default_fields",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("a", sa.Integer, default=5),
    sa.Column("b", sa.String, default="aue"),
    sa.Column("seq_field", sa.Integer, server_default=default_fields_seq.next_value()),
)


class DefaultFieldsRepo(BaseRepository[DefaultFieldsModel]):
    table = default_fields_table
    ignore_default = ["a", "b", "seq_field"]


@pytest.fixture()
async def conn(db_url: str) -> SAConnection:
    # recreate all tables
    engine = sa.create_engine(db_url)
    metadata.drop_all(engine)
    metadata.create_all(engine)

    # create async connection
    async with create_engine(db_url) as engine:
        async with engine.acquire() as conn_:
            yield conn_


@pytest.fixture()
async def query_executor(conn: SAConnection) -> AiopgQueryExecutor:
    return AiopgQueryExecutor(conn)


@pytest.fixture()
async def repo(conn: SAConnection) -> TransactionRepo:
    return TransactionRepo(conn)


@pytest.fixture()
async def transactions(repo: TransactionRepo) -> List[Transaction]:
    transactions_ = [
        Transaction(price=100, date=dt.date(2019, 1, 3)),
        Transaction(price=200),
        Transaction(price=100, date=dt.date(2019, 1, 1)),
    ]
    transactions_ = await repo.insert_many(transactions_)
    return transactions_


async def test_base_repository_insert_sets_id_and_inserts_to_db(repo: TransactionRepo) -> None:
    trans = Transaction(price=100)

    trans = await repo.insert(trans)

    assert trans.id == 1

    db_trans = await repo.first()
    assert db_trans
    assert db_trans.id == trans.id


async def test_base_repo_insert_many_sets_ids(repo: TransactionRepo) -> None:
    transactions = [Transaction(price=100), Transaction(price=200)]

    transactions = await repo.insert_many(transactions)

    assert transactions[0].id == 1
    assert transactions[1].id == 2


async def test_base_repo_update_updates_row_in_db(repo: TransactionRepo) -> None:
    trans = Transaction(price=100)
    trans = await repo.insert(trans)
    trans.price = 300
    trans.date = dt.date(2019, 7, 1)

    await repo.update(trans)

    updated_trans = await repo.first()
    assert updated_trans
    assert updated_trans.price == trans.price
    assert updated_trans.date == trans.date


async def test_base_repo_update_partial_updates_some_fields(repo: TransactionRepo) -> None:
    old_price = 100
    old_date = dt.date(2019, 7, 1)
    trans = Transaction(price=old_price, date=old_date)
    trans = await repo.insert(trans)

    trans.price = 200
    new_date = dt.date(2019, 8, 1)
    await repo.update_partial(trans, date=new_date)

    updated_trans = await repo.first()
    assert updated_trans
    assert updated_trans.price == old_price
    assert updated_trans.date == new_date
    assert trans.date == new_date


async def test_base_repo_update_many(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    new_price = 300
    for trans in transactions:
        trans.price = new_price

    await repo.update_many(transactions)

    updated_trans = await repo.get_all()
    all(updated.price == new_price for updated in updated_trans)


async def test_base_repo_first_return_first_matching_row(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    trans = await repo.first(transactions_table.c.price == 100)

    assert trans
    assert trans.id == transactions[0].id


async def test_base_repo_get_all_return_all_rows_filtered_and_sorted(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    db_transactions = await repo.get_all(
        filters=[transactions_table.c.price == 100], orders=[transactions_table.c.date]
    )
    assert db_transactions == [transactions[2], transactions[0]]


async def test_base_repo_delete_deletes_row_from_db(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    await repo.delete(transactions_table.c.price == 100)

    db_transactions = await repo.get_all()
    assert len(db_transactions) == 1


async def test_transaction_repo_custom_method_works(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    sum_ = await repo.sum()

    assert sum_ == sum(map(operator.attrgetter("price"), transactions))


async def test_base_repo_get_by_id_returns_row_with_id(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    transaction_id = transactions[0].id
    assert transaction_id
    db_trans = await repo.get_by_id(transaction_id)
    assert db_trans == transactions[0]


async def test_base_repo_get_or_create_creates_entity_if_no_entities(
    repo: TransactionRepo
) -> None:
    price = 400
    trans, created = await repo.get_or_create(defaults={"price": price})
    assert created
    assert trans.price == price


async def test_base_repo_get_or_create_returns_entity_if_match(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    price = 400
    trans, created = await repo.get_or_create(
        filters=[transactions_table.c.id == transactions[0].id], defaults={"price": price}
    )
    assert not created
    assert trans == transactions[0]


async def test_get_by_ids_returns_multiple_objects(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    actual_transactions = await repo.get_by_ids([trans.id for trans in transactions if trans.id])
    assert actual_transactions == transactions


async def test_delete_by_id_deletes_object(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    trans_id = transactions[0].id
    assert trans_id

    await repo.delete_by_id(trans_id)
    assert not await repo.get_by_id(trans_id)


async def test_delete_by_ids_deletes_multiple_objects(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    await repo.delete_by_ids([trans.id for trans in transactions if trans.id])
    assert not await repo.get_all()


async def test_exists_returns_true_if_exists(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    assert await repo.exists(transactions_table.c.price == transactions[0].price)


async def test_exists_returns_false_if_not_exists(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    assert not await repo.exists(transactions_table.c.price + 9993 == transactions[0].price)


async def test_connection_var_mixin_allows_to_create_repo_without_connection_if_connection_var_is_third_party(
    conn: SAConnection
) -> None:
    trans = Transaction(price=100)

    new_db_connection_var: ContextVar[SAConnection] = ContextVar("new_db_connection_var")
    new_db_connection_var.set(conn)

    repo = TransactionRepoWithConnectionMixin(new_db_connection_var)
    trans = await repo.insert(trans)

    assert trans.id


async def test_first_returns_transaction_with_greatest_price(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    trans = await repo.first(orders=[-transactions_table.c.price])
    assert trans
    assert trans.price == max(trans.price for trans in transactions)


async def test_get_all_ids(repo: TransactionRepo, transactions: List[Transaction]) -> None:
    ids = await repo.get_all_ids()
    assert ids == [trans.id for trans in transactions]


async def test_delete_without_args_raises_error(repo: TransactionRepo) -> None:
    with pytest.raises(ValueError):
        await repo.delete()


async def test_delete_with_none_deletes_all_entities(
    repo: TransactionRepo, transactions: List[Transaction]
) -> None:
    await repo.delete(None)
    assert (await repo.get_all()) == []


async def test_insert_many_in_transaction_rollback_on_error(conn: SAConnection) -> None:
    repo = UnionModelRepo(conn)

    with suppress(Exception):
        await repo.insert_many([UnionModel(int_or_str=1), UnionModel(int_or_str="ass")])

    assert len(await repo.get_all()) == 0


async def test_execute_in_transaction(conn: SAConnection) -> None:
    repo = UnionModelRepo(conn)

    async with repo.execute_in_transaction():
        await repo.insert(UnionModel(int_or_str=1))
        await repo.insert(UnionModel(int_or_str=1))

    assert len(await repo.get_all()) == 2


async def test_error_in_transaction_inside_transaction_rollback(conn: SAConnection) -> None:
    repo = UnionModelRepo(conn)

    with suppress(Exception):
        async with repo.execute_in_transaction():
            await repo.insert(UnionModel(int_or_str=1))
            await repo.insert_many([UnionModel(int_or_str=1), UnionModel(int_or_str="ass")])

    assert len(await repo.get_all()) == 0


async def test___get_generic_type(repo: TransactionRepo) -> None:
    type_ = repo._get_generic_type()
    assert type_ is Transaction


async def test_insert_does_not_insert_ignore_default_fields_with_simple_default_value(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert(DefaultFieldsModel())
    inserted = (await repo.get_all())[0]

    assert res.a == 5
    assert res == inserted


async def test_insert_does_not_insert_ignore_default_fields_with_default_value_if_field_is_optional(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert(DefaultFieldsModel())
    inserted = (await repo.get_all())[0]

    assert res.b == "aue"
    assert res == inserted


async def test_insert_does_not_insert_ignore_default_fields_with_sequence_column(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert(DefaultFieldsModel())
    inserted = (await repo.get_all())[0]

    assert res.seq_field == 1
    assert res == inserted


async def test_insert_inserts_ignore_default_fields_with_non_default_value(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert(DefaultFieldsModel(a=60))
    inserted = (await repo.get_all())[0]

    assert res.a == 60
    assert res == inserted


async def test_insert_inserts_ignore_default_fields_with_non_default_value_if_field_is_optional(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert(DefaultFieldsModel(b="ssjv"))
    inserted = (await repo.get_all())[0]

    assert res.b == "ssjv"
    assert res == inserted


async def test_insert_inserts_ignore_default_fields_with_non_default_value_if_field_is_sequence(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert(DefaultFieldsModel(seq_field=60))
    inserted = (await repo.get_all())[0]

    assert res.seq_field == 60
    assert res == inserted


async def test_insert_many_inserts_ignore_default_sequence_fields_with_default_values_correctly(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert_many(
        [DefaultFieldsModel(), DefaultFieldsModel(), DefaultFieldsModel()]
    )
    inserted = await repo.get_all()

    assert res[0].seq_field == 1
    assert res[1].seq_field == 2
    assert res[2].seq_field == 3

    assert res == inserted


async def test_insert_many_inserts_ignore_default_sequence_fields_without_default_values_correctly(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    res = await repo.insert_many(
        [
            DefaultFieldsModel(seq_field=5),
            DefaultFieldsModel(seq_field=9),
            DefaultFieldsModel(seq_field=7),
        ]
    )
    inserted = await repo.get_all()

    assert res[0].seq_field == 5
    assert res[1].seq_field == 9
    assert res[2].seq_field == 7

    assert res == inserted


async def test_insert_many_raises_value_error_if_inconsistent_server_default_fields_passed(
    conn: SAConnection
) -> None:
    repo = DefaultFieldsRepo(conn)

    with pytest.raises(ValueError):
        await repo.insert_many(
            [
                DefaultFieldsModel(seq_field=5),
                DefaultFieldsModel(),
                DefaultFieldsModel(seq_field=7),
            ]
        )


async def test_update_values(repo: TransactionRepo) -> None:
    await repo.insert(Transaction(price=100))
    await repo.insert(Transaction(price=200))

    await repo.update_values({"price": 300}, filters=[repo.table.c.price == 100])

    assert {t.price for t in await repo.get_all()} == {300, 200}


async def test_fetch_one_works_ok_with_sa_params(query_executor: AiopgQueryExecutor) -> None:
    query = sa.text("select :aue as col")
    res = await query_executor.fetch_one(query, aue=123)

    assert res is not None
    assert res["col"] == 123


async def test_fetch_all_works_ok_with_sa_params(query_executor: AiopgQueryExecutor) -> None:
    query = sa.text("select :aue as col")
    res = [e async for e in await query_executor.fetch_all(query, aue=123)]

    assert res[0]["col"] == 123


async def test_fetch_val_works_ok_with_sa_params(query_executor: AiopgQueryExecutor) -> None:
    query = sa.text("select :aue as col")

    res = await query_executor.fetch_val(query, aue=123)

    assert res == 123


async def test_update_or_insert_many_by_field(repo: TransactionRepo) -> None:
    await repo.insert(Transaction(price=100, date=dt.date(2020, 1, 1)))

    await repo.update_or_insert_many_by_field(
        [
            Transaction(price=200, date=dt.date(2020, 1, 1)),
            Transaction(price=300, date=dt.date(2021, 2, 2)),
        ],
        "date",
    )

    examples = await repo.get_all()
    assert len(examples) == 2
    assert next(i for i in examples if i.date == dt.date(2020, 1, 1)).price == 200
    assert next(i for i in examples if i.date == dt.date(2021, 2, 2)).price == 300


async def test_update_or_insert_first(repo: TransactionRepo) -> None:
    await repo.insert(Transaction(price=100, date=dt.date(2020, 1, 1)))

    await repo.update_or_insert_first(Transaction(price=200, date=dt.date(2020, 1, 1)), "date")
    await repo.update_or_insert_first(Transaction(price=300, date=dt.date(2021, 2, 2)), "date")

    examples = await repo.get_all()
    assert len(examples) == 2
    assert next(i for i in examples if i.date == dt.date(2020, 1, 1)).price == 200
    assert next(i for i in examples if i.date == dt.date(2021, 2, 2)).price == 300
