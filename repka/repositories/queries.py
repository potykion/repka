from dataclasses import dataclass, field
from functools import reduce
from typing import Sequence, Union, Mapping

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
    """SQL SELECT query with customizable filters, orders, columns"""

    table: Table
    filters: Filters = field(default_factory=list)
    orders: Columns = field(default_factory=list)
    select_columns: Columns = field(default_factory=list)

    def __call__(self) -> SqlAlchemyQuery:
        """Create SQL SELECT query"""
        select_columns = self.select_columns if self.select_columns else [self.table]
        query = sa.select(select_columns)
        query = self.apply_filters(query, self.filters)
        query = self.apply_orders(query, self.orders)
        return query

    @staticmethod
    def apply_filters(query: SqlAlchemyQuery, filters: Filters) -> ClauseElement:
        """Append WHERE clause to query"""
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

    @staticmethod
    def apply_orders(query: SqlAlchemyQuery, orders: Columns) -> ClauseElement:
        """Append ORDER BY clause to query"""
        return reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)


@dataclass
class InsertQuery:
    """SQL INSERT query with customizable insert values and returning columns"""

    table: Table
    insert_values: Mapping
    returning_columns: Columns = field(default_factory=list)

    def __call__(self) -> SqlAlchemyQuery:
        """Create SQL INSERT query"""
        query = self.table.insert().values(self.insert_values)
        if self.returning_columns:
            query = query.returning(*self.returning_columns)
        return query


@dataclass
class InsertManyQuery:
    """Same as InsertQuery, but insert multiple values"""

    table: Table
    insert_values: Sequence[Mapping]
    returning_columns: Columns = field(default_factory=list)

    def __call__(self) -> SqlAlchemyQuery:
        """Create SQL INSERT query"""
        query = self.table.insert().values(self.insert_values)
        if self.returning_columns:
            query = query.returning(*self.returning_columns)
        return query


@dataclass
class UpdateQuery:
    """SQL UPDATE query with customizable update values and filters"""

    table: Table
    update_values: Mapping
    filters: Filters

    def __call__(self) -> SqlAlchemyQuery:
        """Create SQL UPDATE query"""
        return SelectQuery.apply_filters(
            self.table.update().values(self.update_values), self.filters
        )

    @classmethod
    def by_id(
        cls, id_: int, table: Table, update_values: Mapping, extra_filters: Filters = None
    ) -> 'UpdateQuery':
        """Create update query with id filter"""
        extra_filters = extra_filters or []
        return UpdateQuery(table, update_values, [table.c.id == id_, *extra_filters])


@dataclass
class DeleteQuery:
    """SQL DELETE query with customizable filters"""

    table: Table
    filters: Union[Filters, Sequence[None]]

    def __call__(self) -> SqlAlchemyQuery:
        """
        Create SQL DELETE query

        :raise ValueError if {filters} is empty. To delete all table rows pass {filters} = [None]
        """
        if not len(self.filters):
            raise ValueError(
                """No filters set, are you sure you want to delete all table rows?
            If so call the method with None:
            repo.delete(None)"""
            )

        # None passed => delete all table rows
        filters: Filters
        if self.filters[0] is None:
            filters = tuple()
        else:
            filters = self.filters

        query = self.table.delete()
        query = SelectQuery.apply_filters(query, filters)
        return query
