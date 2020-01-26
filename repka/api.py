from abc import abstractmethod
from contextvars import ContextVar
from typing import Optional, Generic, Dict, Sequence, List, cast, Tuple, Any, Union, Type

import typing_inspect
from aiopg.sa import SAConnection
from aiopg.sa.transaction import Transaction as SATransaction
from databases import Database
from repka.commands import (
    SelectAllCommand,
    SelectOneCommand,
    SelectValCommand,
    InsertCommand,
    execute_in_transaction,
    UpdateCommand,
    DeleteCommand,
)
from repka.models import Created, Columns, T
from repka.utils import model_to_primitive
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression

db_connection_var: ContextVar[SAConnection] = ContextVar("db_connection")


class SAConnectionMixin:
    def __init__(self, connection: SAConnection) -> None:
        self._connection = connection

    @property
    def connection(self) -> Union[SAConnection, Database]:
        return self._connection


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
        self.context_var = context_var

    @property
    def connection(self) -> SAConnection:
        return self.context_var.get()


class BaseRepository(SAConnectionMixin, Generic[T]):
    """
    Execute sql-queries, convert sql-row-dicts to/from pydantic models
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

    def serialize(self, entity: T) -> Dict:
        return model_to_primitive(entity, without_id=True, keep_dates=True)

    def deserialize(self, **kwargs: Any) -> T:
        entity_type = self.__get_generic_type()
        return entity_type(**kwargs)

    ############
    # PRIMITIVES
    ############

    async def get_all(
        self, filters: Optional[List[BinaryExpression]] = None, orders: Optional[Columns] = None
    ) -> List[T]:
        command = SelectAllCommand(self.table, self.connection, filters, orders)
        query = command.build_query()
        rows = await command.execute_query(query)
        return [cast(T, self.deserialize(**row)) for row in rows]

    async def first(
        self, *filters: BinaryExpression, orders: Optional[Columns] = None
    ) -> Optional[T]:
        command = SelectOneCommand(self.table, self.connection, filters, orders)
        query = command.build_query()
        row = await command.execute_query(query)
        return cast(T, self.deserialize(**row)) if row else None

    async def get_all_ids(
        self, filters: Sequence[BinaryExpression] = None, orders: Columns = None
    ) -> Sequence[int]:
        """
        Same as get_all() but returns only ids.
        :param filters: List of conditions
        :param orders: List of orders
        :return: List of ids
        """
        command = SelectAllCommand(
            self.table, self.connection, filters, orders, columns=[self.table.c.id]
        )
        query = command.build_query()
        rows = await command.execute_query(query)
        return [row["id"] for row in rows]

    async def exists(self, *filters: BinaryExpression) -> bool:
        command = SelectValCommand(self.table, self.connection, filters)
        query = command.build_query()
        val = await command.execute_query(query)
        return bool(val)

    async def insert(self, entity: T) -> T:
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

        command = InsertCommand(
            self.table, self.connection, serialized, returning_columns, entity, self.ignore_insert
        )
        query = command.build_query()
        insert_res = await command.execute_query(query)
        command.process_query_result(insert_res)

        return entity

    async def update(self, entity: T) -> T:
        assert entity.id
        command = UpdateCommand(self.table, self.connection, entity.id, self.serialize(entity))
        query = command.build_query()
        await command.execute_query(query)
        return entity

    async def update_partial(self, entity: T, **updated_values: Any) -> T:
        assert entity.id

        for field, value in updated_values.items():
            setattr(entity, field, value)

        serialized_entity = self.serialize(entity)
        serialized_values = {key: serialized_entity[key] for key in updated_values.keys()}

        command = UpdateCommand(self.table, self.connection, entity.id, serialized_values)
        query = command.build_query()
        await command.execute_query(query)

        return entity

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

        command = DeleteCommand(self.table, self.connection, filters)
        query = command.build_query()
        await command.execute_query(query)

    def execute_in_transaction(self) -> SATransaction:
        return execute_in_transaction(self.connection)

    ############
    # COMPOSITES
    ############

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

    async def insert_many(self, entities: List[T]) -> List[T]:
        if not entities:
            return entities

        async with self.execute_in_transaction():
            entities = [await self.insert(entity) for entity in entities]

        return entities

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

    async def delete_by_id(self, entity_id: int) -> None:
        return await self.delete(self.table.c.id == entity_id)

    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        return await self.delete(self.table.c.id.in_(entity_ids))

    # ==============
    # PROTECTED & PRIVATE METHODS
    # ==============

    def __get_generic_type(self) -> Type[T]:
        """
        Get generic type of inherited BaseRepository:

        >>> class TransactionRepo(BaseRepository[Transaction]):
        ...     table = transactions_table
        ... # doctest: +SKIP
        >>> assert TransactionRepo().__get_generic_type() is Transaction # doctest: +SKIP
        """
        return cast(
            Type[T], typing_inspect.get_args(typing_inspect.get_generic_bases(self)[0])[0]
        )
