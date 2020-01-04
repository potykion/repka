import json
from contextlib import asynccontextmanager
from typing import Sequence, Dict, Set, Union

from aiopg.sa import SAConnection, create_engine
from pydantic import BaseModel


def model_to_primitive(
    model: BaseModel, without_id: bool = False, exclude: Sequence[str] = None
) -> Dict:
    """
    Convert pydantic-{model} to dict transforming complex types to primitives (e.g. datetime to str)
    :param model: Pydantic model
    :param without_id: Remove id key from result dict
    :param exclude: List of field to exclude from result dict
    :return: Dict with fields from given model
    """
    exclude_set: Set[Union[int, str]] = set(exclude or [])
    if without_id:
        exclude_set.add("id")

    data: Dict = json.loads(model.json(exclude=exclude_set))
    return data


@asynccontextmanager
async def create_async_db_connection(db_url: str) -> SAConnection:
    """Create async db connection via aiopg"""
    async with create_engine(db_url) as engine:
        async with engine.acquire() as connection:
            yield connection
