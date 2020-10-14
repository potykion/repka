from abc import ABC
from contextvars import ContextVar
from typing import Union, Optional, Mapping, Any, AsyncIterator

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

    async def fetch_one(self, query: SqlAlchemyQuery, **sa_params: Any) -> Optional[Mapping]:
        rows: ResultProxy = await self._connection.execute(query, **sa_params)
        row: RowProxy = await rows.first()
        return row

    async def fetch_all(self, query: SqlAlchemyQuery, **sa_params: Any) -> AsyncIterator[Mapping]:
        return await self._connection.execute(query, **sa_params)

    async def fetch_val(self, query: SqlAlchemyQuery, **sa_params: Any) -> Any:
        return await self._connection.scalar(query, **sa_params)

    async def insert(self, query: SqlAlchemyQuery, **sa_params: Any) -> Mapping:
        rows = await self._connection.execute(query, **sa_params)
        row = await rows.first()
        return row

    async def insert_many(
        self, query: SqlAlchemyQuery, **sa_params: Any
    ) -> AsyncIterator[Mapping]:
        return await self._connection.execute(query, **sa_params)

    async def update(self, query: SqlAlchemyQuery, **sa_params: Any) -> None:
        return await self._connection.execute(query, **sa_params)

    async def delete(self, query: SqlAlchemyQuery, **sa_params: Any) -> None:
        await self._connection.execute(query, **sa_params)

    def execute_in_transaction(self) -> SATransaction:
        return self._connection.begin()
