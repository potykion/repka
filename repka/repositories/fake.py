from abc import ABC
from contextlib import asynccontextmanager
from typing import Optional, Dict, Sequence, List, cast, Tuple, Any

from sqlalchemy.sql.elements import BinaryExpression

from repka.repositories.base import GenericIdModel, Columns, Created, AsyncBaseRepo
from repka.repositories.queries import Filters


class FakeRepo(AsyncBaseRepo[GenericIdModel], ABC):
    def __init__(self) -> None:
        self.entities: List[GenericIdModel] = []
        self.id_counter = 1

    async def first(
        self, *filters: BinaryExpression, orders: Optional[Columns] = None
    ) -> Optional[GenericIdModel]:
        return next(iter(self.entities), None)

    async def get_by_ids(self, entity_ids: Sequence[int]) -> List[GenericIdModel]:
        return [entity for entity in self.entities if entity.id in entity_ids]

    async def get_by_id(self, entity_id: int) -> Optional[GenericIdModel]:
        return next((entity for entity in self.entities if entity.id == entity_id), None)

    async def get_or_create(
        self, filters: Filters = None, defaults: Dict = None
    ) -> Tuple[GenericIdModel, Created]:
        raise NotImplementedError()

    async def get_all(
        self, filters: Filters = None, orders: Columns = None
    ) -> List[GenericIdModel]:
        return self.entities

    async def get_all_ids(
        self, filters: Sequence[BinaryExpression] = None, orders: Columns = None
    ) -> Sequence[int]:
        return [cast(int, entity.id) for entity in self.entities]

    async def exists(self, *filters: BinaryExpression) -> bool:
        raise NotImplementedError()

    async def insert(self, entity: GenericIdModel) -> GenericIdModel:
        entity.id = self.id_counter
        self.id_counter += 1
        self.entities.append(entity)
        return entity

    async def insert_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        return [await self.insert(entity) for entity in entities]

    async def update(self, entity: GenericIdModel) -> GenericIdModel:
        return entity

    async def update_partial(
        self, entity: GenericIdModel, **updated_values: Any
    ) -> GenericIdModel:
        for field, value in updated_values.items():
            setattr(entity, field, value)
        return entity

    async def update_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        return entities

    async def delete(self, *filters: Optional[BinaryExpression]) -> None:
        raise NotImplementedError()

    async def delete_by_id(self, entity_id: int) -> None:
        self.entities = [entity for entity in self.entities if entity.id != entity_id]

    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        self.entities = [entity for entity in self.entities if entity.id not in entity_ids]

    @asynccontextmanager
    def execute_in_transaction(self) -> Any:
        yield None
