from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import (
    TypeVar,
    Optional,
    List,
    Sequence,
    Dict,
    Any,
    Tuple,
    Type,
    cast,
    Generic,
    Mapping,
    Set,
    AsyncIterator,
)

import sqlalchemy as sa
import typing_inspect
from pydantic import BaseModel
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression

from repka.repositories.queries import (
    SelectQuery,
    Filters,
    Columns,
    InsertQuery,
    UpdateQuery,
    DeleteQuery,
    SqlAlchemyQuery,
    InsertManyQuery,
)
from repka.utils import model_to_primitive, is_field_equal_to_default, mixed_zip, aiter_to_list

Created = bool


class IdModel(BaseModel):
    """Pydantic model with optional id field"""

    id: Optional[int]


GenericIdModel = TypeVar("GenericIdModel", bound=IdModel)


class AsyncQueryExecutor:
    @abstractmethod
    async def fetch_one(self, query: SqlAlchemyQuery, **sa_params: Any) -> Optional[Mapping]:
        """Execute SELECT query and return first result row"""

    @abstractmethod
    async def fetch_all(self, query: SqlAlchemyQuery, **sa_params: Any) -> AsyncIterator[Mapping]:
        """Execute SELECT query and return all result rows"""

    @abstractmethod
    async def fetch_val(self, query: SqlAlchemyQuery, **sa_params: Any) -> Any:
        """Execute SELECT query and return first column of first result row"""

    @abstractmethod
    async def insert(self, query: SqlAlchemyQuery, **sa_params: Any) -> Mapping:
        """Execute INSERT query and return returning columns"""

    @abstractmethod
    async def insert_many(
        self, query: SqlAlchemyQuery, **sa_params: Any
    ) -> AsyncIterator[Mapping]:
        """Execute INSERT query and return list of returning columns"""

    @abstractmethod
    async def update(self, query: SqlAlchemyQuery, **sa_params: Any) -> None:
        """Execute UPDATE query"""

    @abstractmethod
    async def delete(self, query: SqlAlchemyQuery, **sa_params: Any) -> None:
        """Execute DELETE query"""

    @abstractmethod
    def execute_in_transaction(self) -> Any:
        """Execute queries in transaction"""


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
        """SQLAlchemy table definition"""

    @property
    def ignore_default(self) -> Sequence[str]:
        """
        Columns will be inserted only if their values are not equal to default values of
        corresponding models' fields
        These columns will be set after insert
        """
        return []

    def serialize(self, entity: GenericIdModel) -> Dict:
        """Convert pydantic model to dict"""
        return model_to_primitive(entity, without_id=True)

    def deserialize(self, **kwargs: Any) -> GenericIdModel:
        """Create pydantic model from kwargs"""
        entity_type = self._get_generic_type()
        return entity_type(**kwargs)

    @property
    @abstractmethod
    def query_executor(self) -> AsyncQueryExecutor:
        """repka.repositories.base.AsyncQueryExecutor instance"""

    # ==============
    # SELECT METHODS
    # ==============

    async def first(
        self, *filters: BinaryExpression, orders: Columns = None
    ) -> Optional[GenericIdModel]:
        """Get first entity from DB matching filters and orders"""
        query = SelectQuery(self.table, filters, orders or [])()
        row = await self.query_executor.fetch_one(query)
        return self.deserialize(**row) if row else None

    async def get_by_id(self, entity_id: int) -> Optional[GenericIdModel]:
        """Get entity from DB with id = {entity_id}"""
        return await self.first(self.table.c.id == entity_id)

    async def get_or_create(
        self, filters: Filters = None, defaults: Dict = None
    ) -> Tuple[GenericIdModel, Created]:
        """Get first entity from DB  matching filters or create it"""
        entity = await self.first(*(filters or []))
        if entity:
            return entity, False

        entity = self.deserialize(**(defaults or {}))
        entity = await self.insert(entity)
        return entity, True

    async def get_all(
        self, filters: Filters = None, orders: Columns = None
    ) -> List[GenericIdModel]:
        """Get all entities from DB matching filters and orders"""
        return await aiter_to_list(await self.get_all_aiter(filters, orders))

    async def get_all_aiter(
        self, filters: Filters = None, orders: Columns = None
    ) -> AsyncIterator[GenericIdModel]:
        """Get all entities from DB matching filters and orders as an async iterator"""
        query = SelectQuery(self.table, filters or [], orders or [])()
        rows = await self.query_executor.fetch_all(query)
        return self._rows_to_entities(rows)

    async def get_by_ids(self, entity_ids: Sequence[int]) -> List[GenericIdModel]:
        """Get all entities from DB with id from {entity_ids}"""
        return await aiter_to_list(await self.get_by_ids_aiter(entity_ids))

    async def get_by_ids_aiter(self, entity_ids: Sequence[int]) -> AsyncIterator[GenericIdModel]:
        """Get all entities from DB with id from {entity_ids} as an async iterator"""
        return await self.get_all_aiter(filters=[self.table.c.id.in_(entity_ids)])

    async def get_all_ids(
        self, filters: Sequence[BinaryExpression] = None, orders: Columns = None
    ) -> Sequence[int]:
        """Same as get_all() but returns only ids."""
        query = SelectQuery(
            self.table, filters or [], orders or [], select_columns=[self.table.c.id]
        )()
        rows = await self.query_executor.fetch_all(query)
        return [row["id"] async for row in rows]

    async def exists(self, *filters: BinaryExpression) -> bool:
        """Check entity matching filters exists in DB"""
        query = SelectQuery(
            self.table, filters, select_columns=[sa.func.count(self.table.c.id)]
        )()
        result = await self.query_executor.fetch_val(query)
        return bool(result)

    # ==============
    # INSERT METHODS
    # ==============

    async def insert(self, entity: GenericIdModel) -> GenericIdModel:
        """Insert entity to DB"""
        return await InsertImpl(self).insert(entity)

    async def insert_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        """Insert multiple entities to DB"""
        return await InsertManyImpl(self).insert_many(entities)

    async def insert_many_aiter(
        self, entities: List[GenericIdModel]
    ) -> AsyncIterator[GenericIdModel]:
        """Insert multiple entities to DB. Returns an async iterable with inserted entities"""
        return await InsertManyImpl(self).insert_many_aiter(entities)

    # ==============
    # UPDATE METHODS
    # ==============

    async def update(self, entity: GenericIdModel) -> GenericIdModel:
        """Update entity in DB"""
        assert entity.id
        update_values = self.serialize(entity)
        query = UpdateQuery.by_id(entity.id, self.table, update_values)()
        await self.query_executor.update(query)
        return entity

    async def update_partial(
        self, entity: GenericIdModel, **updated_values: Any
    ) -> GenericIdModel:
        """Update particular entity fields in DB"""
        assert entity.id

        for field, value in updated_values.items():
            setattr(entity, field, value)

        serialized_entity = self.serialize(entity)
        serialized_values = {key: serialized_entity[key] for key in updated_values.keys()}

        query = UpdateQuery.by_id(entity.id, self.table, serialized_values)()
        await self.query_executor.update(query)

        return entity

    async def update_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        """
        Update multiple entities in DB

        No way to update many in single query:
        https://github.com/aio-libs/aiopg/issues/546

        So update entities sequentially in transaction.
        """
        if not entities:
            return entities

        async with self.execute_in_transaction():
            entities = [await self.update(entity) for entity in entities]

        return entities

    async def update_values(self, values: dict, filters: Filters) -> None:
        """Update particular entity fields for entities matching filters (perform SQL UPDATE)"""
        query = UpdateQuery(self.table, values, filters)()
        await self.query_executor.update(query)

    async def update_or_insert_first(self, entity: GenericIdModel, field: str) -> GenericIdModel:
        """Update one entity with field or add it to DB"""
        value = getattr(entity, field)
        entity_with_field = await self.get_all(filters=[self.table.c[field] == value])

        async with self.execute_in_transaction():
            if entity_with_field:
                entity.id = entity_with_field[0].id
                entity = await self.update(entity)
            else:
                entity = await self.insert(entity)
        return entity

    async def update_or_insert_many_by_field(
        self, entities: Sequence[GenericIdModel], field: str
    ) -> Sequence[GenericIdModel]:
        """Update all entities with field and add entities without it to DB"""
        values = [getattr(e, field) for e in entities]
        entities_with_field = await self.get_all(filters=[self.table.c[field].in_(values)])
        field_entities = {getattr(e, field): e for e in entities_with_field}

        async with self.execute_in_transaction():
            entities_to_insert, entities_to_update = [], []
            for e in entities:
                if getattr(e, field) in field_entities:
                    entities_to_update.append(e)
                else:
                    entities_to_insert.append(e)

            entities_to_insert = await self.insert_many(entities_to_insert)

            for e in entities_to_update:
                e.id = field_entities[getattr(e, field)].id
            entities_to_update = await self.update_many(entities_to_update)

        return [*entities_to_insert, *entities_to_update]

    # ==============
    # DELETE METHODS
    # ==============

    async def delete(self, *filters: Optional[BinaryExpression]) -> None:
        """Delete entities matching filters from DB"""
        query = DeleteQuery(self.table, filters)()
        await self.query_executor.delete(query)

    async def delete_by_id(self, entity_id: int) -> None:
        """Delete entity by id from DB"""
        return await self.delete(self.table.c.id == entity_id)

    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        """Delete multiple entities from DB with id in {entity_ids}"""
        return await self.delete(self.table.c.id.in_(entity_ids))

    # ==============
    # OTHER METHODS
    # ==============

    def execute_in_transaction(self) -> Any:
        """
        Execute queries in transaction

        Usage:

        async with repo.execute_in_transaction():
            repo.delete(...)
            repo.insert(...)
            ...
        """
        return self.query_executor.execute_in_transaction()

    # ==============
    # PROTECTED & PRIVATE METHODS
    # ==============

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

    async def _rows_to_entities(
        self, rows: AsyncIterator[Mapping]
    ) -> AsyncIterator[GenericIdModel]:
        """
        Converts an async iterator of DB rows to an async iterator of GenericIdModel
        """
        async for row in rows:
            yield cast(GenericIdModel, self.deserialize(**row))


@dataclass
class InsertImpl:
    """Entity DB insertion implementation"""

    repo: AsyncBaseRepo

    async def insert(self, entity: GenericIdModel) -> GenericIdModel:
        """Perform entity insertion"""
        query = InsertQuery(
            self.repo.table, self._serialize_for_insertion(entity), self.insert_returning_columns
        )()

        row = await self.repo.query_executor.insert(query)

        return self._set_ignored_fields(entity, row)

    def _serialize_for_insertion(self, entity: GenericIdModel) -> Dict[str, Any]:
        """
        Remove ignored fields from serialized entity

        Field should be removed here (not in .serialize) due to compatibility
        """
        return {
            key: value
            for key, value in self.repo.serialize(entity).items()
            if key not in self._get_ignored_fields(entity)
        }

    @property
    def insert_returning_columns(self) -> Columns:
        """All columns except columns from {AsyncBaseRepo.ignore_default}"""
        return (
            self.repo.table.c.id,
            *(getattr(self.repo.table.c, col) for col in self.repo.ignore_default),
        )

    def _set_ignored_fields(self, entity: GenericIdModel, row: Mapping) -> GenericIdModel:
        """Set returned from db values to entity"""
        entity.id = row["id"]
        for col in self._get_ignored_fields(entity):
            setattr(entity, col, row[col])
        return entity

    def _get_ignored_fields(self, entity: GenericIdModel) -> Set[str]:
        """Get entity fields which values equal to default value"""
        return {
            field
            for field in self.repo.ignore_default
            if is_field_equal_to_default(entity, field)
        }


class InsertManyImpl(InsertImpl):
    """Multiple entities DB insertion implementation"""

    async def insert_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        """
        Inserts many entities with a single query.

        :raises ValueError if some entities' fields from self.ignore_default have default values
        while other fields have non-default values
        """
        return await aiter_to_list(await self.insert_many_aiter(entities))

    async def insert_many_aiter(
        self, entities: List[GenericIdModel]
    ) -> AsyncIterator[GenericIdModel]:
        if not entities:

            async def _empty_aiter() -> AsyncIterator[GenericIdModel]:
                return
                yield

            return _empty_aiter()

        self._check_server_defaults(entities)

        query = InsertManyQuery(
            self.repo.table,
            [self._serialize_for_insertion(entity) for entity in entities],
            self.insert_returning_columns,
        )()

        rows = await self.repo.query_executor.insert_many(query)

        return self._updated_entities_aiter(entities, rows)

    def _check_server_defaults(self, entities: Sequence[GenericIdModel]) -> None:
        """Check all entity values either equal to default values or not"""
        server_default_fields = [
            col.key
            for col in self.repo.table.c
            if col.server_default is not None and col.key in self.repo.ignore_default
        ]
        for server_default_field in server_default_fields:
            first = is_field_equal_to_default(next(iter(entities)), server_default_field)
            is_consistent = all(
                is_field_equal_to_default(entity, server_default_field) == first
                for entity in entities
            )
            if not is_consistent:
                raise ValueError(
                    "All fields from ignore default should either be equal to default values or not be "
                    f"equal. Got inconsistency with {server_default_field} field"
                )

    async def _updated_entities_aiter(
        self, entities: List[GenericIdModel], rows: AsyncIterator[Mapping]
    ) -> AsyncIterator[GenericIdModel]:
        """
        Returns an async iterator which yielding entities with fields updated by passed rows
        """
        async for entity, row in mixed_zip(entities, rows):  # type: ignore
            yield self._set_ignored_fields(entity, row)
