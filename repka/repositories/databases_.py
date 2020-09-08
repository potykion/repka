from contextvars import ContextVar
from abc import ABC
from typing import Union, Mapping, Any, Sequence, Optional, cast, Dict

from databases import Database
from databases.core import Transaction

from repka.repositories.base import AsyncBaseRepo, GenericIdModel, AsyncQueryExecutor
from repka.repositories.queries import SqlAlchemyQuery
from repka.utils import model_to_primitive


class DatabasesRepository(AsyncBaseRepo[GenericIdModel], ABC):
    """
    Execute sql-queries, convert sql-row-dicts to/from pydantic models
    """

    def __init__(self, connection_or_context_var: Union[Database, ContextVar[Database]]) -> None:
        self.connection_or_context_var = connection_or_context_var

    @property
    def _connection(self) -> Database:
        if isinstance(self.connection_or_context_var, Database):
            connection = self.connection_or_context_var
        else:
            connection = self.connection_or_context_var.get()
        assert (
            connection.is_connected
        ), "You should run `await database.connect()` before using the repo"
        return connection

    @property
    def query_executor(self) -> AsyncQueryExecutor:
        return DatabasesQueryExecutor(self._connection)

    def serialize(self, entity: GenericIdModel) -> Dict:
        return model_to_primitive(entity, without_id=True, keep_python_primitives=True)


class DatabasesQueryExecutor(AsyncQueryExecutor):
    def __init__(self, connection: Database) -> None:
        self.connection = connection

    async def fetch_one(self, query: SqlAlchemyQuery, **sa_params: Any) -> Optional[Mapping]:
        _check_sa_params(sa_params)
        return await self.connection.fetch_one(query)

    async def fetch_all(self, query: SqlAlchemyQuery, **sa_params: Any) -> Sequence[Mapping]:
        _check_sa_params(sa_params)
        return await self.connection.fetch_all(query, sa_params)

    async def fetch_val(self, query: SqlAlchemyQuery, **sa_params: Any) -> Any:
        _check_sa_params(sa_params)
        return await self.connection.fetch_val(query, sa_params)

    async def insert(self, query: SqlAlchemyQuery, **sa_params: Any) -> Mapping:
        _check_sa_params(sa_params)
        return cast(Mapping, await self.connection.fetch_one(query, sa_params))

    async def insert_many(self, query: SqlAlchemyQuery, **sa_params: Any) -> Sequence[Mapping]:
        _check_sa_params(sa_params)
        return await self.connection.fetch_all(query, sa_params)

    async def update(self, query: SqlAlchemyQuery, **sa_params: Any) -> None:
        _check_sa_params(sa_params)
        await self.connection.execute(query, sa_params)

    async def delete(self, query: SqlAlchemyQuery, **sa_params: Any) -> None:
        _check_sa_params(sa_params)
        await self.connection.execute(query, sa_params)

    def execute_in_transaction(self) -> Transaction:
        return self.connection.transaction()


def _check_sa_params(sa_params: Dict[str, Any]) -> None:
    if sa_params:
        raise ValueError("Non-empty sa_params kwargs are not supported for databases backend")
