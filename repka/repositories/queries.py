from dataclasses import dataclass, field
from functools import reduce
from typing import Sequence, Union, Mapping, cast

import sqlalchemy as sa
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression, ClauseElement

Filters = Sequence[BinaryExpression]
Columns = Sequence[Union[sa.Column, str]]

# ClauseElement is a weird name for sql-alchemy query, that's why SqlAlchemyQuery was defined
# Union used because SqlAlchemyQuery = ClauseElement raises mypy error:
# https://github.com/python/mypy/issues/7866
SqlAlchemyQuery = Union[ClauseElement]


@dataclass
class SelectQuery:
    table: Table
    filters: Filters = field(default_factory=list)
    orders: Columns = field(default_factory=list)
    select_columns: Columns = field(default_factory=list)

    def __call__(self) -> SqlAlchemyQuery:
        select_columns = self.select_columns if self.select_columns else [self.table]
        query = sa.select(select_columns)
        query = self.apply_filters(query, self.filters)
        query = self.apply_orders(query, self.orders)
        return query

    @staticmethod
    def apply_filters(query: ClauseElement, filters: Sequence[BinaryExpression]) -> ClauseElement:
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

    @staticmethod
    def apply_orders(query: ClauseElement, orders: Columns) -> ClauseElement:
        return reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)


@dataclass
class InsertQuery:
    table: Table
    insert_values: Mapping
    returning_columns: Columns = field(default_factory=list)

    def __call__(self) -> SqlAlchemyQuery:
        query = self.table.insert().values(self.insert_values)
        if self.returning_columns:
            query = query.returning(*self.returning_columns)
        return query


@dataclass
class InsertManyQuery:
    table: Table
    insert_values: Sequence[Mapping]
    returning_columns: Columns = field(default_factory=list)

    def __call__(self) -> SqlAlchemyQuery:
        query = self.table.insert().values(self.insert_values)
        if self.returning_columns:
            query = query.returning(*self.returning_columns)
        return query


@dataclass
class UpdateQuery:
    table: Table
    update_values: Mapping
    filters: Filters

    def __call__(self) -> SqlAlchemyQuery:
        query = self.table.update().values(self.update_values).where(sa.and_(*self.filters))
        return query

    @classmethod
    def by_id(
        cls, id_: int, table: Table, update_values: Mapping, extra_filters: Filters = None
    ) -> 'UpdateQuery':
        """Create update query with id filter"""
        extra_filters = extra_filters or []
        return UpdateQuery(table, update_values, [table.c.id == id_, *extra_filters])


@dataclass
class DeleteQuery:
    table: Table
    filters: Union[Filters, Sequence[None]]

    def __call__(self) -> SqlAlchemyQuery:
        if not len(self.filters):
            raise ValueError(
                """No filters set, are you sure you want to delete all table rows?
            If so call the method with None:
            repo.delete(None)"""
            )

        # None passed => delete all table rows
        if self.filters[0] is None:
            filters: Filters = tuple()
        else:
            filters = self.filters

        query = self.table.delete()
        query = SelectQuery.apply_filters(query, cast(Filters, filters))
        return query
