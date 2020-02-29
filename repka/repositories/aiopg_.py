from abc import ABC
from contextvars import ContextVar
from typing import Union

from aiopg.sa import SAConnection

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
