from abc import ABC
from contextvars import ContextVar
from typing import Union, Optional, Mapping, Sequence, Any

from aiopg.sa import SAConnection
from aiopg.sa.result import RowProxy, ResultProxy
from aiopg.sa.transaction import Transaction as SATransaction

from repka.repositories.base import GenericIdModel, AsyncBaseRepo, AsyncQueryExecutor
from repka.repositories.queries import SqlAlchemyQuery


class AiopgRepository(AsyncBaseRepo[GenericIdModel], ABC):
    """
    Execute sql-queries, convert sql-row-dicts to/from pydantic models
    """

    def __init__(
        self, connection_or_context_var: Union[SAConnection, ContextVar[SAConnection]]
    ) -> None:
        self.connection_or_context_var = connection_or_context_var

    @property
    def _connection(self) -> SAConnection:
        if isinstance(self.connection_or_context_var, SAConnection):
            return self.connection_or_context_var
        else:
            return self.connection_or_context_var.get()

    @property
    def query_executor(self) -> AsyncQueryExecutor:
        return AiopgQueryExecutor(self._connection)


class AiopgQueryExecutor(AsyncQueryExecutor):
    def __init__(self, connection: SAConnection) -> None:
        self._connection = connection

    async def fetch_one(self, query: SqlAlchemyQuery) -> Optional[Mapping]:
        rows: ResultProxy = await self._connection.execute(query)
        row: RowProxy = await rows.first()
        return row

    async def fetch_all(self, query: SqlAlchemyQuery) -> Sequence[Mapping]:
        return await self._connection.execute(query)

    async def fetch_val(self, query: SqlAlchemyQuery) -> Any:
        return await self._connection.scalar(query)

    async def insert(self, query: SqlAlchemyQuery) -> Mapping:
        rows = await self._connection.execute(query)
        row = await rows.first()
        return row

    async def insert_many(self, query: SqlAlchemyQuery) -> Sequence[Mapping]:
        return await self._connection.execute(query)

    async def update(self, query: SqlAlchemyQuery) -> None:
        await self._connection.execute(query)

    async def delete(self, query: SqlAlchemyQuery) -> None:
        await self._connection.execute(query)

    def execute_in_transaction(self) -> SATransaction:
        return self._connection.begin()
