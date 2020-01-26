from abc import ABC, abstractmethod
from functools import reduce
from typing import Sequence, Union, cast, Any, AsyncContextManager

import sqlalchemy as sa
from aiopg.sa import SAConnection
from aiopg.sa.result import ResultProxy
from databases import Database
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import BinaryExpression

from repka.models import IdModel

Columns = Sequence[Union[sa.Column, str]]


class Command(ABC):
    @abstractmethod
    def build_query(self) -> ClauseElement:
        ...

    @abstractmethod
    async def execute_query(self, query: ClauseElement) -> Any:
        ...

    def _apply_filters(
        self, query: ClauseElement, filters: Sequence[BinaryExpression]
    ) -> ClauseElement:
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

    def _apply_orders(self, query: ClauseElement, orders: Columns) -> ClauseElement:
        return reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)


class SelectAllCommand(Command):
    def __init__(
        self,
        table: sa.Table,
        connection: Union[SAConnection, Database],
        filters: Sequence[BinaryExpression] = None,
        orders: Columns = None,
        columns: Columns = None,
    ):
        self.table = table
        self.connection = connection
        self.filters = filters or []
        self.orders = orders or []
        self.columns = columns

    def build_query(self) -> ClauseElement:
        if self.columns:
            query = sa.select(self.columns)
        else:
            query = self.table.select()
        query = self._apply_filters(query, self.filters)
        query = self._apply_orders(query, self.orders)
        return query

    async def execute_query(self, query: ClauseElement) -> Any:
        if isinstance(self.connection, SAConnection):
            return await self.connection.execute(query)
        elif isinstance(self.connection, Database):
            return await self.connection.fetch_all(query)


class SelectOneCommand(SelectAllCommand):
    async def execute_query(self, query: ClauseElement) -> Any:
        if isinstance(self.connection, SAConnection):
            rows: ResultProxy = await self.connection.execute(query)
            row = await rows.first()
            return row
        elif isinstance(self.connection, Database):
            return await self.connection.fetch_one(query)


class SelectValCommand(SelectAllCommand):
    def build_query(self) -> ClauseElement:
        query = sa.select([sa.func.count("*")])
        query = self._apply_filters(query, self.filters)
        return query

    async def execute_query(self, query: ClauseElement) -> Any:
        if isinstance(self.connection, SAConnection):
            return await self.connection.scalar(query)
        elif isinstance(self.connection, Database):
            return await self.connection.fetch_val(query)


class InsertCommand(Command):
    def __init__(
        self,
        table: sa.Table,
        connection: Union[SAConnection, Database],
        insert_data: dict,
        returning_columns: Columns,
        entity: IdModel,
        ignore_insert: Sequence[str],
    ):
        self.table = table
        self.connection = connection
        self.insert_data = insert_data
        self.returning_columns = returning_columns
        self.entity = entity
        self.ignore_insert = ignore_insert

    def build_query(self) -> ClauseElement:
        if isinstance(self.connection, SAConnection):
            return self.table.insert().values(self.insert_data).returning(*self.returning_columns)
        elif isinstance(self.connection, Database):
            return self.table.insert().values(self.insert_data)

    async def execute_query(self, query: ClauseElement) -> Any:
        if isinstance(self.connection, SAConnection):
            rows = await self.connection.execute(query)
            row = await rows.first()
            return row
        elif isinstance(self.connection, Database):
            return await self.connection.execute(query)

    def process_query_result(self, insert_res: Any) -> Any:
        if isinstance(self.connection, SAConnection):
            self.entity.id = insert_res["id"]
            for col in self.ignore_insert:
                setattr(self.entity, col, insert_res[col])
            return self.entity
        elif isinstance(self.connection, Database):
            self.entity.id = insert_res
            return self.entity


class UpdateCommand(Command):
    def __init__(
        self,
        table: sa.Table,
        connection: Union[SAConnection, Database],
        update_id: int,
        update_data: dict,
    ) -> None:
        self.table = table
        self.connection = connection
        self.update_id = update_id
        self.update_data = update_data

    def build_query(self) -> ClauseElement:
        query = (
            self.table.update().values(self.update_data).where(self.table.c.id == self.update_id)
        )
        return query

    async def execute_query(self, query: ClauseElement) -> Any:
        if isinstance(self.connection, SAConnection):
            await self.connection.execute(query)
        if isinstance(self.connection, Database):
            await self.connection.execute(query)


class DeleteCommand(Command):
    def __init__(
        self,
        table: sa.Table,
        connection: Union[SAConnection, Database],
        filters: Sequence[BinaryExpression],
    ) -> None:
        self.table = table
        self.connection = connection
        self.filters = filters

    def build_query(self) -> ClauseElement:
        query = self.table.delete()
        query = self._apply_filters(query, cast(Sequence[BinaryExpression], self.filters))
        return query

    async def execute_query(self, query: ClauseElement) -> Any:
        if isinstance(self.connection, SAConnection):
            await self.connection.execute(query)
        if isinstance(self.connection, Database):
            await self.connection.execute(query)


def execute_in_transaction(connection: Union[SAConnection, Database]) -> AsyncContextManager:
    if isinstance(connection, SAConnection):
        return connection.begin()
    elif isinstance(connection, Database):
        return connection.transaction()
    raise ValueError(f"Invalid connection type: {type(connection)}")
