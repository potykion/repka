from contextvars import ContextVar
from abc import ABC
from typing import Union, Mapping, Any, Sequence, Optional

from databases import Database
from databases.core import Transaction

from repka.repositories.base import AsyncBaseRepo, GenericIdModel, AsyncQueryExecutor
from repka.repositories.queries import SqlAlchemyQuery


class DatabasesRepository(AsyncBaseRepo[GenericIdModel], ABC):
    """
    Execute sql-queries, convert sql-row-dicts to/from pydantic models
    """

    def __init__(self, connection_or_context_var: Union[Database, ContextVar[Database]]) -> None:
        self.connection_or_context_var = connection_or_context_var

    @property
    def _connection(self) -> Database:
        if isinstance(self.connection_or_context_var, Database):
            connection = self.connection_or_context_var
        else:
            connection = self.connection_or_context_var.get()
        assert (
            connection.is_connected
        ), "You should run `await database.connect()` before using the repo"
        return connection

    @property
    def _query_executor(self) -> AsyncQueryExecutor:
        return DatabasesQueryExecutor(self._connection)


class DatabasesQueryExecutor(AsyncQueryExecutor):
    def __init__(self, connection: Database) -> None:
        self.connection = connection

    async def fetch_one(self, query: SqlAlchemyQuery) -> Optional[Mapping]:
        return await self.connection.fetch_one(query)

    async def fetch_all(self, query: SqlAlchemyQuery) -> Sequence[Mapping]:
        return await self.connection.fetch_all(query)

    async def fetch_val(self, query: SqlAlchemyQuery) -> Any:
        return await self.connection.fetch_val(query)

    async def insert(self, query: SqlAlchemyQuery) -> Mapping:
        return await self.connection.execute(query)

    async def update(self, query: SqlAlchemyQuery) -> None:
        await self.connection.execute(query)

    async def delete(self, query: SqlAlchemyQuery) -> None:
        await self.connection.execute(query)

    def execute_in_transaction(self) -> Transaction:
        return self.connection.transaction()
