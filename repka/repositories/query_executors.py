from abc import abstractmethod
from typing import Optional, Mapping, Sequence, Any

from aiopg.sa import SAConnection
from aiopg.sa.result import RowProxy, ResultProxy

from repka.repositories.queries import SqlAlchemyQuery


class AsyncQueryExecutor:
    @abstractmethod
    async def fetch_one(self, query: SqlAlchemyQuery) -> Optional[Mapping]:
        ...

    @abstractmethod
    async def fetch_all(self, query: SqlAlchemyQuery) -> Sequence[Mapping]:
        ...

    @abstractmethod
    async def fetch_val(self, query: SqlAlchemyQuery) -> Any:
        ...

    @abstractmethod
    async def insert(self, query: SqlAlchemyQuery) -> Mapping:
        ...


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
