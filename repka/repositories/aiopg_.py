from abc import ABC
from contextvars import ContextVar
from typing import Optional, Sequence, List, cast, Any, Union

from aiopg.sa import SAConnection
from aiopg.sa.transaction import Transaction as SATransaction
from sqlalchemy.sql.elements import BinaryExpression

from repka.repositories.base import GenericIdModel, AsyncBaseRepo
from repka.repositories.query_executors import AsyncQueryExecutor, AiopgQueryExecutor


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
    def _query_executor(self) -> AsyncQueryExecutor:
        return AiopgQueryExecutor(self._connection)

    # ==============
    # INSERT METHODS
    # ==============

    async def insert(self, entity: GenericIdModel) -> GenericIdModel:
        # key should be removed manually (not in .serialize) due to compatibility
        serialized = {
            key: value
            for key, value in self.serialize(entity).items()
            if key not in self.ignore_insert
        }
        returning_columns = (
            self.table.c.id,
            *(getattr(self.table.c, col) for col in self.ignore_insert),
        )
        query = self.table.insert().values(serialized).returning(*returning_columns)

        rows = await self._connection.execute(query)
        row = await rows.first()

        entity.id = row.id
        for col in self.ignore_insert:
            setattr(entity, col, getattr(row, col))

        return entity

    async def insert_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        if not entities:
            return entities

        async with self.execute_in_transaction():
            entities = [await self.insert(entity) for entity in entities]

        return entities

    # ==============
    # UPDATE METHODS
    # ==============

    async def update(self, entity: GenericIdModel) -> GenericIdModel:
        assert entity.id
        query = (
            self.table.update().values(self.serialize(entity)).where(self.table.c.id == entity.id)
        )
        await self._connection.execute(query)
        return entity

    async def update_partial(
        self, entity: GenericIdModel, **updated_values: Any
    ) -> GenericIdModel:
        assert entity.id

        for field, value in updated_values.items():
            setattr(entity, field, value)

        serialized_entity = self.serialize(entity)
        serialized_values = {key: serialized_entity[key] for key in updated_values.keys()}

        query = self.table.update().values(serialized_values).where(self.table.c.id == entity.id)
        await self._connection.execute(query)

        return entity

    async def update_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        """
        No way to update many in single query:
        https://github.com/aio-libs/aiopg/issues/546

        So update entities sequentially in transaction.
        """
        if not entities:
            return entities

        async with self.execute_in_transaction():
            entities = [await self.update(entity) for entity in entities]

        return entities

    # ==============
    # DELETE METHODS
    # ==============

    async def delete(self, *filters: Optional[BinaryExpression]) -> None:
        if not len(filters):
            raise ValueError(
                """No filters set, are you sure you want to delete all table rows?
            If so call the method with None:
            repo.delete(None)"""
            )

        # None passed => delete all table rows
        if filters[0] is None:
            filters = tuple()

        query = self.table.delete()
        query = self._apply_filters(query, cast(Sequence[BinaryExpression], filters))
        await self._connection.execute(query)

    async def delete_by_id(self, entity_id: int) -> None:
        return await self.delete(self.table.c.id == entity_id)

    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        return await self.delete(self.table.c.id.in_(entity_ids))

    # ==============
    # OTHER METHODS
    # ==============

    def execute_in_transaction(self) -> SATransaction:
        return self._connection.begin()
