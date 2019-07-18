import json
from abc import abstractmethod
from functools import reduce, partial
from typing import (
    TypeVar,
    Optional,
    Generic,
    Dict,
    Sequence,
    List,
    cast,
    Tuple,
    Callable,
)

from aiopg.sa import SAConnection
from aiopg.sa.result import ResultProxy
from aiopg.sa.transaction import Transaction as SATransaction
from pydantic import BaseModel
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression, ClauseElement

Created = bool


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

    @property
    def serializer(self) -> Callable[[T], Dict]:
        return cast(Callable[[T], Dict], partial(model_to_primitive, without_id=True))

    @property
    @abstractmethod
    def deserializer(self) -> Callable[..., T]:
        # should return callable that acccepts kwargs and return entity (e.g. entity constructor)
        pass

    async def insert(self, entity: T) -> T:
        query = (
            self.table.insert()
            .values(self.serializer(entity))
            .returning(self.table.c.id)
        )
        id_ = await self.connection.scalar(query)
        entity.id = id_

        return entity

    async def insert_many(self, entities: List[T]) -> List[T]:
        if not entities:
            return entities

        query = (
            self.table.insert()
            .values([self.serializer(entity) for entity in entities])
            .returning(self.table.c.id)
        )
        rows = await self.connection.execute(query)
        for index, row in enumerate(rows):
            entities[index].id = row[0]

        return entities

    async def update(self, entity: T) -> T:
        assert entity.id
        query = (
            self.table.update()
            .values(self.serializer(entity))
            .where(self.table.c.id == entity.id)
        )
        await self.connection.execute(query)
        return entity

    async def first(self, *filters: BinaryExpression) -> Optional[T]:
        query = self.table.select()
        query = reduce(lambda query_, filter_: query_.where(filter_), filters, query)

        rows: ResultProxy = await self.connection.execute(query)
        row = await rows.first()
        if row:
            return cast(T, self.deserializer(**row))

        return None

    async def get_by_id(self, entity_id: int) -> T:
        return await self.first(self.table.c.id == entity_id)

    async def get_or_create(
        self,
        filters: Optional[List[BinaryExpression]] = None,
        defaults: Optional[Dict] = None,
    ) -> Tuple[T, Created]:
        filters = filters or []
        defaults = defaults or {}

        entity = await self.first(*filters)
        if entity:
            return entity, False

        entity = self.deserializer(**defaults)
        entity = await self.insert(entity)
        return entity, True

    async def get_all(
        self,
        filters: Optional[List[BinaryExpression]] = None,
        orders: Optional[List[BinaryExpression]] = None,
    ) -> List[T]:
        filters = filters or []
        orders = orders or []

        query = self.table.select()
        query = self._apply_filters(query, filters)
        query = reduce(
            lambda query_, order_by: query_.order_by(order_by), orders, query
        )

        rows = await self.connection.execute(query)
        return [cast(T, self.deserializer(**row)) for row in rows]

    async def delete(self, *filters: BinaryExpression) -> None:
        query = self.table.delete()
        query = self._apply_filters(query, filters)
        await self.connection.execute(query)

    def _apply_filters(
        self, query: ClauseElement, filters: Sequence[BinaryExpression]
    ) -> ClauseElement:
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

    def execute_in_transaction(self) -> SATransaction:
        return self.connection.begin()


def model_to_primitive(model: BaseModel, without_id: bool = False) -> Dict:
    data: Dict = json.loads(model.json())
    if without_id:
        data.pop("id", None)
    return data
