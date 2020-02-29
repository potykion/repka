from dataclasses import dataclass, field
from functools import reduce
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy import Table
from sqlalchemy.sql.elements import BinaryExpression, ClauseElement

Filters = Sequence[BinaryExpression]
Columns = Sequence[Union[sa.Column, str]]

SqlAlchemyQuery = ClauseElement


@dataclass
class SelectQuery:
    table: Table
    filters: Filters = field(default_factory=list)
    orders: Columns = field(default_factory=list)
    select_columns: Columns = field(default_factory=list)

    def __call__(self) -> ClauseElement:
        select_columns = self.select_columns if self.select_columns else [self.table]
        query = sa.select(select_columns)
        query = self._apply_filters(query, self.filters)
        query = self._apply_orders(query, self.orders)
        return query

    def _apply_filters(
        self, query: ClauseElement, filters: Sequence[BinaryExpression]
    ) -> ClauseElement:
        return reduce(lambda query_, filter_: query_.where(filter_), filters, query)

    def _apply_orders(self, query: ClauseElement, orders: Columns) -> ClauseElement:
        return reduce(lambda query_, order_by: query_.order_by(order_by), orders, query)
