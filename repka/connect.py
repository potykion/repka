import typing
from typing import AsyncContextManager

from aiopg.sa import SAConnection
from aiopg.sa.result import ResultProxy
from sqlalchemy.sql import ClauseElement
from typing_extensions import Protocol


class DatabasesConnectionProto(Protocol):
    """
    See databases.Database
    https://www.encode.io/databases/database_queries/
    """

    async def fetch_all(
        self, query: typing.Union[ClauseElement, str], values: dict = None
    ) -> typing.List[typing.Mapping]:
        ...

    async def fetch_one(
        self, query: typing.Union[ClauseElement, str], values: dict = None
    ) -> typing.Optional[typing.Mapping]:
        ...

    async def fetch_val(
        self, query: typing.Union[ClauseElement, str], values: dict = None, column: typing.Any = 0
    ) -> typing.Any:
        ...

    async def execute(
        self, query: typing.Union[ClauseElement, str], values: dict = None
    ) -> typing.Any:
        ...

    async def execute_many(self, query: typing.Union[ClauseElement, str], values: list) -> None:
        ...

    async def transaction(self, *, force_rollback: bool = False) -> typing.Any:
        ...


class SAConnection2DatabasesAdapter:
    def __init__(self, connection: SAConnection) -> None:
        self.connection = connection

    async def fetch_all(
        self, query: typing.Union[ClauseElement, str], values: dict = None
    ) -> typing.List[typing.Mapping]:
        return await self.connection.execute(query, values)

    async def fetch_one(
        self, query: typing.Union[ClauseElement, str], values: dict = None
    ) -> typing.Optional[typing.Mapping]:
        rows: ResultProxy = await self.connection.execute(query, values)
        row = await rows.first()
        return row

    async def fetch_val(
        self, query: typing.Union[ClauseElement, str], values: dict = None, column: typing.Any = 0
    ) -> typing.Any:
        rows: ResultProxy = await self.connection.execute(query, values)
        val = await rows.scalar()
        return val

    async def execute(
        self, query: typing.Union[ClauseElement, str], values: dict = None
    ) -> typing.Any:
        return await self.connection.execute(query, values)

    async def execute_many(self, query: typing.Union[ClauseElement, str], values: list) -> None:
        async with self.transaction():
            for entity in values:
                await self.execute(query, entity)

    def transaction(self, *, force_rollback: bool = False) -> AsyncContextManager:
        return self.connection.begin()
