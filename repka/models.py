from typing import List, Union, Optional, TypeVar

import sqlalchemy as sa
from pydantic import BaseModel

Created = bool
Columns = List[Union[sa.Column, str]]


class IdModel(BaseModel):
    id: Optional[int]


T = TypeVar("T", bound=IdModel)
