import datetime as dt
import operator
import os
from typing import Optional, List

import pytest
import sqlalchemy as sa
from aiopg.sa import create_engine, SAConnection
from pydantic import validator

from repka.api import BaseRepository, IdModel

pytestmark = pytest.mark.asyncio

DATABASE_URL = os.environ["DATABASE_URL"]


class Transaction(IdModel):
    date: dt.date = None
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
    entity_type = Transaction
    table = transactions_table

    async def sum(self) -> int:
        query = sa.select([sa.func.sum(transactions_table.c.price)])
        sum_ = await self.connection.scalar(query)
        return sum_


@pytest.fixture()
async def conn() -> SAConnection:
    # recreate all tables
    engine = sa.create_engine(DATABASE_URL)
    metadata.drop_all(engine)
    metadata.create_all(engine)

    # create async connection
    async with create_engine(DATABASE_URL) as engine:
        async with engine.acquire() as conn_:
            yield conn_


@pytest.fixture()
async def repo(conn: SAConnection) -> TransactionRepo:
    return TransactionRepo(conn)


@pytest.fixture()
async def transactions(repo: TransactionRepo) -> List[Transaction]:
    transactions_ = [
        Transaction(price=100, date=dt.date(2019, 1, 3)),
        Transaction(price=200),
        Transaction(price=100, date=dt.date(2019, 1, 1))
    ]
    transactions_ = await repo.insert_many(transactions_)
    return transactions_


async def test_base_repository_insert_sets_id_and_inserts_to_db(repo: TransactionRepo) -> None:
    trans = Transaction(price=100)

    trans = await repo.insert(trans)

    assert trans.id == 1

    db_trans = await repo.first()
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
    assert updated_trans.price == trans.price
    assert updated_trans.date == trans.date


async def test_base_repo_first_return_first_matching_row(repo: TransactionRepo, transactions: List[Transaction]) -> None:
    trans = await repo.first(transactions_table.c.price == 100)

    assert trans.id == transactions[0].id


async def test_base_repo_get_all_return_all_rows_filtered_and_sorted(repo: TransactionRepo, transactions: List[Transaction]) -> None:
    db_transactions = await repo.get_all(
        filters=[transactions_table.c.price == 100],
        orders=[transactions_table.c.date]
    )
    assert db_transactions == [transactions[2], transactions[0]]


async def test_base_repo_delete_deletes_row_from_db(repo: TransactionRepo, transactions: List[Transaction]) -> None:
    await repo.delete(transactions_table.c.price == 100)

    db_transactions = await repo.get_all()
    assert len(db_transactions) == 1


async def test_transaction_repo_custom_method_works(repo: TransactionRepo, transactions: List[Transaction]) -> None:
    sum_ = await repo.sum()

    assert sum_ == sum(map(operator.attrgetter("price"), transactions))
