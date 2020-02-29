from abc import ABC
from contextvars import ContextVar
from typing import Optional, Sequence, cast, Union

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
