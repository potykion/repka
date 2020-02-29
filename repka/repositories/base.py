from abc import abstractmethod, ABC
from functools import reduce
from typing import TypeVar, Optional, List, Sequence, Dict, Any, Tuple, Type, cast, Generic

import sqlalchemy as sa
import typing_inspect
from pydantic import BaseModel
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression, ClauseElement

from repka.repositories.queries import (
    SelectQuery,
    Filters,
    Columns,
    InsertQuery,
    UpdateQuery,
    DeleteQuery,
)
from repka.repositories.query_executors import AsyncQueryExecutor
from repka.utils import model_to_primitive

Created = bool


class IdModel(BaseModel):
    id: Optional[int]


GenericIdModel = TypeVar("GenericIdModel", bound=IdModel)


class AsyncBaseRepo(Generic[GenericIdModel], ABC):
    """
    Execute sql-queries, convert sql-row-dicts to/from pydantic models in async way
    """

    # =============
    # CONFIGURATION
    # =============

    @property
    @abstractmethod
    def table(self) -> Table:
        pass

    @property
    def ignore_insert(self) -> Sequence[str]:
        """
        Columns will be ignored on insert while serialization
        These columns will be set after insert

        See following tests for example:
         - tests.test_api.test_insert_sets_ignored_column
         - tests.test_api.test_insert_many_inserts_sequence_rows
        """
        return []

    def serialize(self, entity: GenericIdModel) -> Dict:
        return model_to_primitive(entity, without_id=True)

    def deserialize(self, **kwargs: Any) -> GenericIdModel:
        entity_type = self._get_generic_type()
        return entity_type(**kwargs)

    @property
    @abstractmethod
    def _query_executor(self) -> AsyncQueryExecutor:
        ...

    # ==============
    # SELECT METHODS
    # ==============

    async def first(
        self, *filters: BinaryExpression, orders: Columns = None
    ) -> Optional[GenericIdModel]:
        query = SelectQuery(self.table, filters, orders or [])()
        row = await self._query_executor.fetch_one(query)
        return self.deserialize(**row) if row else None

    async def get_by_id(self, entity_id: int) -> Optional[GenericIdModel]:
        return await self.first(self.table.c.id == entity_id)

    async def get_or_create(
        self, filters: Filters = None, defaults: Dict = None
    ) -> Tuple[GenericIdModel, Created]:
        entity = await self.first(*(filters or []))
        if entity:
            return entity, False

        entity = self.deserialize(**(defaults or {}))
        entity = await self.insert(entity)
        return entity, True

    async def get_all(
        self, filters: Filters = None, orders: Columns = None
    ) -> List[GenericIdModel]:
        query = SelectQuery(self.table, filters or [], orders or [])()
        rows = await self._query_executor.fetch_all(query)
        return [cast(GenericIdModel, self.deserialize(**row)) for row in rows]

    async def get_by_ids(self, entity_ids: Sequence[int]) -> List[GenericIdModel]:
        return await self.get_all(filters=[self.table.c.id.in_(entity_ids)])

    async def get_all_ids(
        self, filters: Sequence[BinaryExpression] = None, orders: Columns = None
    ) -> Sequence[int]:
        """
        Same as get_all() but returns only ids.
        :param filters: List of conditions
        :param orders: List of orders
        :return: List of ids
        """
        query = SelectQuery(
            self.table, filters or [], orders or [], select_columns=[self.table.c.id]
        )()
        rows = await self._query_executor.fetch_all(query)
        return [row["id"] for row in rows]

    async def exists(self, *filters: BinaryExpression) -> bool:
        query = SelectQuery(self.table, filters, select_columns=[sa.func.count("*")])()
        result = await self._query_executor.fetch_val(query)
        return bool(result)

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
        query = InsertQuery(self.table, serialized, returning_columns)()

        row = await self._query_executor.insert(query)

        entity.id = row["id"]
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
        update_values = self.serialize(entity)
        query = UpdateQuery(self.table, update_values, entity.id)()
        await self._query_executor.update(query)
        return entity

    async def update_partial(
        self, entity: GenericIdModel, **updated_values: Any
    ) -> GenericIdModel:
        assert entity.id

        for field, value in updated_values.items():
            setattr(entity, field, value)

        serialized_entity = self.serialize(entity)
        serialized_values = {key: serialized_entity[key] for key in updated_values.keys()}

        query = UpdateQuery(self.table, serialized_values, entity.id)()
        await self._query_executor.update(query)

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
        query = DeleteQuery(self.table, filters)()
        await self._query_executor.delete(query)

    async def delete_by_id(self, entity_id: int) -> None:
        return await self.delete(self.table.c.id == entity_id)

    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        return await self.delete(self.table.c.id.in_(entity_ids))

    # ==============
    # OTHER METHODS
    # ==============

    def execute_in_transaction(self) -> Any:
        return self._query_executor.execute_in_transaction()

    # ==============
    # PROTECTED & PRIVATE METHODS
    # ==============

    def _apply_filters(
        self, query: ClauseElement, filters: Sequence[BinaryExpression]
    ) -> ClauseElement:
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

    def _apply_orders(self, query: ClauseElement, orders: Columns) -> ClauseElement:
        return reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)

    def _get_generic_type(self) -> Type[GenericIdModel]:
        """
        Get generic type of inherited BaseRepository:

        >>> class TransactionRepo(AiopgRepository[Transaction]):
        ...     table = transactions_table
        ... # doctest: +SKIP
        >>> assert TransactionRepo().__get_generic_type() is Transaction # doctest: +SKIP
        """
        return cast(
            Type[GenericIdModel],
            typing_inspect.get_args(typing_inspect.get_generic_bases(self)[-1])[0],
        )
