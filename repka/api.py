import json
from abc import abstractmethod
from functools import reduce
from typing import TypeVar, Optional, Generic, Dict, Sequence, List, cast, Tuple, Any, Union
from contextvars import ContextVar

import sqlalchemy as sa
from aiopg.sa import SAConnection
from aiopg.sa.result import ResultProxy
from aiopg.sa.transaction import Transaction as SATransaction
from pydantic import BaseModel
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression, ClauseElement

Created = bool

db_connection_var: ContextVar[SAConnection] = ContextVar("db_connection")

Columns = List[Union[sa.Column, str]]


class IdModel(BaseModel):
    id: Optional[int]


T = TypeVar("T", bound=IdModel)


class BaseRepository(Generic[T]):
    def __init__(self, connection: SAConnection) -> None:
        self.connection = connection

    @property
    @abstractmethod
    def table(self) -> Table:
        pass

    def serialize(self, entity: T) -> Dict:
        return model_to_primitive(entity, without_id=True)

    @abstractmethod
    def deserialize(self, **kwargs: Any) -> T:
        pass

    async def insert(self, entity: T) -> T:
        query = self.table.insert().values(self.serialize(entity)).returning(self.table.c.id)
        id_ = await self.connection.scalar(query)
        entity.id = id_

        return entity

    async def insert_many(self, entities: List[T]) -> List[T]:
        if not entities:
            return entities

        query = (
            self.table.insert()
            .values([self.serialize(entity) for entity in entities])
            .returning(self.table.c.id)
        )
        rows = await self.connection.execute(query)
        for index, row in enumerate(rows):
            entities[index].id = row[0]

        return entities

    async def update(self, entity: T) -> T:
        assert entity.id
        query = (
            self.table.update().values(self.serialize(entity)).where(self.table.c.id == entity.id)
        )
        await self.connection.execute(query)
        return entity

    async def update_partial(self, entity: T, **updated_values: Any) -> T:
        assert entity.id

        for field, value in updated_values.items():
            setattr(entity, field, value)

        serialized_entity = self.serialize(entity)
        serialized_values = {key: serialized_entity[key] for key in updated_values.keys()}

        query = self.table.update().values(serialized_values).where(self.table.c.id == entity.id)
        await self.connection.execute(query)

        return entity

    async def update_many(self, entities: List[T]) -> List[T]:
        """
        No way to update many in single query:
        https://github.com/aio-libs/aiopg/issues/546

        So update entities sequentially in transaction.
        """
        async with self.execute_in_transaction():
            for entity in entities:
                await self.update(entity)

        return entities

    async def first(
        self, *filters: BinaryExpression, orders: Optional[Columns] = None
    ) -> Optional[T]:
        orders = orders or []

        query = self.table.select()
        query = reduce(lambda query_, filter_: query_.where(filter_), filters, query)
        query = reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)

        rows: ResultProxy = await self.connection.execute(query)
        row = await rows.first()
        if row:
            return cast(T, self.deserialize(**row))

        return None

    async def get_by_ids(self, entity_ids: Sequence[int]) -> List[T]:
        return await self.get_all(filters=[self.table.c.id.in_(entity_ids)])

    async def get_by_id(self, entity_id: int) -> Optional[T]:
        return await self.first(self.table.c.id == entity_id)

    async def get_or_create(
        self, filters: Optional[List[BinaryExpression]] = None, defaults: Optional[Dict] = None
    ) -> Tuple[T, Created]:
        filters = filters or []
        defaults = defaults or {}

        entity = await self.first(*filters)
        if entity:
            return entity, False

        entity = self.deserialize(**defaults)
        entity = await self.insert(entity)
        return entity, True

    async def get_all(
        self, filters: Optional[List[BinaryExpression]] = None, orders: Optional[Columns] = None
    ) -> List[T]:
        filters = filters or []
        orders = orders or []

        query = self.table.select()
        query = self._apply_filters(query, filters)
        query = reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)

        rows = await self.connection.execute(query)
        return [cast(T, self.deserialize(**row)) for row in rows]

    async def delete(self, *filters: BinaryExpression) -> None:
        query = self.table.delete()
        query = self._apply_filters(query, filters)
        await self.connection.execute(query)

    async def delete_by_id(self, entity_id: int) -> None:
        return await self.delete(self.table.c.id == entity_id)

    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        return await self.delete(self.table.c.id.in_(entity_ids))

    async def exists(self, *filters: BinaryExpression) -> bool:
        query = sa.select([sa.func.count("*")])
        query = self._apply_filters(query, filters)
        result = await self.connection.scalar(query)
        return bool(result)

    def _apply_filters(
        self, query: ClauseElement, filters: Sequence[BinaryExpression]
    ) -> ClauseElement:
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

    def execute_in_transaction(self) -> SATransaction:
        return self.connection.begin()


class ConnectionVarMixin:
    """
    Usage:
    class TransactionRepo(ConnectionVarMixin, BaseRepository[Transaction]):
        table = transactions_table

        def deserialize(self, **kwargs: Any) -> Transaction:
            return Transaction(**kwargs)

    db_connection_var.set(conn)
    repo = TransactionRepo()
    trans = await repo.insert(trans)
    """

    def __init__(self, context_var: ContextVar[SAConnection] = db_connection_var):
        super().__init__(connection=context_var.get())  # type: ignore


def model_to_primitive(model: BaseModel, without_id: bool = False) -> Dict:
    data: Dict = json.loads(model.json())
    if without_id:
        data.pop("id", None)
    return data
