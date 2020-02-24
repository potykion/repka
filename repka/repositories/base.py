from typing import TypeVar, Optional, List, Union

import sqlalchemy as sa
from pydantic import BaseModel

Created = bool

Columns = List[Union[sa.Column, str]]


class IdModel(BaseModel):
    id: Optional[int]


GenericIdModel = TypeVar("GenericIdModel", bound=IdModel)
