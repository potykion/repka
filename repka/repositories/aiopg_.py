from abc import abstractmethod
from contextvars import ContextVar
from functools import reduce
from typing import Optional, Generic, Dict, Sequence, List, cast, Tuple, Any, Union, Type

import sqlalchemy as sa
import typing_inspect
from aiopg.sa import SAConnection
from aiopg.sa.result import ResultProxy
from aiopg.sa.transaction import Transaction as SATransaction
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression, ClauseElement

from repka.repositories.base import GenericIdModel, Columns, Created
from repka.utils import model_to_primitive


class AiopgRepository(Generic[GenericIdModel]):
    """
    Execute sql-queries, convert sql-row-dicts to/from pydantic models
    """

    def __init__(
        self, connection_or_context_var: Union[SAConnection, ContextVar[SAConnection]]
    ) -> None:
        self.connection_or_context_var = connection_or_context_var

    @property
    def connection(self) -> SAConnection:
        if isinstance(self.connection_or_context_var, SAConnection):
            return self.connection_or_context_var
        else:
            return self.connection_or_context_var.get()

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

    # ==============
    # SELECT METHODS
    # ==============

    async def first(
        self, *filters: BinaryExpression, orders: Optional[Columns] = None
    ) -> Optional[GenericIdModel]:
        orders = orders or []

        query = self.table.select()
        query = reduce(lambda query_, filter_: query_.where(filter_), filters, query)
        query = reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)

        rows: ResultProxy = await self.connection.execute(query)
        row = await rows.first()
        if row:
            return cast(GenericIdModel, self.deserialize(**row))

        return None

    async def get_by_ids(self, entity_ids: Sequence[int]) -> List[GenericIdModel]:
        return await self.get_all(filters=[self.table.c.id.in_(entity_ids)])

    async def get_by_id(self, entity_id: int) -> Optional[GenericIdModel]:
        return await self.first(self.table.c.id == entity_id)

    async def get_or_create(
        self, filters: Optional[List[BinaryExpression]] = None, defaults: Optional[Dict] = None
    ) -> Tuple[GenericIdModel, Created]:
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
    ) -> List[GenericIdModel]:
        filters = filters or []
        orders = orders or []

        query = self.table.select()
        query = self._apply_filters(query, filters)
        query = reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)

        rows = await self.connection.execute(query)
        return [cast(GenericIdModel, self.deserialize(**row)) for row in rows]

    async def get_all_ids(
        self, filters: Sequence[BinaryExpression] = None, orders: Columns = None
    ) -> Sequence[int]:
        """
        Same as get_all() but returns only ids.
        :param filters: List of conditions
        :param orders: List of orders
        :return: List of ids
        """
        filters = filters or []
        orders = orders or []

        query = sa.select([self.table.c.id])
        query = self._apply_filters(query, filters)
        query = reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)

        rows = await self.connection.execute(query)
        return [row.id for row in rows]

    async def exists(self, *filters: BinaryExpression) -> bool:
        query = sa.select([sa.func.count("*")])
        query = self._apply_filters(query, filters)
        result = await self.connection.scalar(query)
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
        query = self.table.insert().values(serialized).returning(*returning_columns)

        rows = await self.connection.execute(query)
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
        await self.connection.execute(query)
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
        await self.connection.execute(query)

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
        await self.connection.execute(query)

    async def delete_by_id(self, entity_id: int) -> None:
        return await self.delete(self.table.c.id == entity_id)

    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        return await self.delete(self.table.c.id.in_(entity_ids))

    # ==============
    # OTHER METHODS
    # ==============

    def execute_in_transaction(self) -> SATransaction:
        return self.connection.begin()

    # ==============
    # PROTECTED & PRIVATE METHODS
    # ==============

    def _apply_filters(
        self, query: ClauseElement, filters: Sequence[BinaryExpression]
    ) -> ClauseElement:
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

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