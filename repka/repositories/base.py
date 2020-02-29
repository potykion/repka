from abc import abstractmethod, ABC
from functools import reduce
from typing import TypeVar, Optional, List, Union, Sequence, Dict, Any, Tuple, Type, cast, Generic

import sqlalchemy as sa
import typing_inspect
from pydantic import BaseModel
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression, ClauseElement

from repka.utils import model_to_primitive

Created = bool

Columns = List[Union[sa.Column, str]]


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

    # ==============
    # SELECT METHODS
    # ==============

    @abstractmethod
    async def first(
        self, *filters: BinaryExpression, orders: Optional[Columns] = None
    ) -> Optional[GenericIdModel]:
        ...

    @abstractmethod
    async def get_by_ids(self, entity_ids: Sequence[int]) -> List[GenericIdModel]:
        ...

    @abstractmethod
    async def get_by_id(self, entity_id: int) -> Optional[GenericIdModel]:
        ...

    @abstractmethod
    async def get_or_create(
        self, filters: Optional[List[BinaryExpression]] = None, defaults: Optional[Dict] = None
    ) -> Tuple[GenericIdModel, Created]:
        ...

    @abstractmethod
    async def get_all(
        self, filters: Optional[List[BinaryExpression]] = None, orders: Optional[Columns] = None
    ) -> List[GenericIdModel]:
        ...

    @abstractmethod
    async def get_all_ids(
        self, filters: Sequence[BinaryExpression] = None, orders: Columns = None
    ) -> Sequence[int]:
        """
        Same as get_all() but returns only ids.
        :param filters: List of conditions
        :param orders: List of orders
        :return: List of ids
        """
        ...

    @abstractmethod
    async def exists(self, *filters: BinaryExpression) -> bool:
        ...

    # ==============
    # INSERT METHODS
    # ==============

    @abstractmethod
    async def insert(self, entity: GenericIdModel) -> GenericIdModel:
        # key should be removed manually (not in .serialize) due to compatibility
        ...

    @abstractmethod
    async def insert_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        ...

    # ==============
    # UPDATE METHODS
    # ==============

    @abstractmethod
    async def update(self, entity: GenericIdModel) -> GenericIdModel:
        ...

    @abstractmethod
    async def update_partial(
        self, entity: GenericIdModel, **updated_values: Any
    ) -> GenericIdModel:
        ...

    @abstractmethod
    async def update_many(self, entities: List[GenericIdModel]) -> List[GenericIdModel]:
        """
        No way to update many in single query:
        https://github.com/aio-libs/aiopg/issues/546

        So update entities sequentially in transaction.
        """
        ...

    # ==============
    # DELETE METHODS
    # ==============

    @abstractmethod
    async def delete(self, *filters: Optional[BinaryExpression]) -> None:
        ...

    @abstractmethod
    async def delete_by_id(self, entity_id: int) -> None:
        ...

    @abstractmethod
    async def delete_by_ids(self, entity_ids: Sequence[int]) -> None:
        ...

    # ==============
    # OTHER METHODS
    # ==============

    @abstractmethod
    def execute_in_transaction(self) -> Any:
        ...

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
