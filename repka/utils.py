import json
from contextlib import asynccontextmanager
from typing import Sequence, Dict, Set, Union, TypeVar, Tuple, AsyncIterator, Iterator

from aiopg.sa import SAConnection, create_engine
from pydantic import BaseModel


def model_to_primitive(
    model: BaseModel,
    without_id: bool = False,
    exclude: Sequence[str] = None,
    keep_python_primitives: bool = False,
) -> Dict:
    """
    Convert pydantic-{model} to dict transforming complex types to primitives (e.g. datetime to str)
    :param model: Pydantic model
    :param without_id: Remove id key from result dict
    :param exclude: List of field to exclude from result dict
    :param keep_python_primitives: If True result dict will have python-primitives (e.g. datetime, Decimal)
    :return: Dict with fields from given model
    """
    exclude_set: Set[Union[int, str]] = set(exclude or [])
    if without_id:
        exclude_set.add("id")

    data: Dict
    if keep_python_primitives:
        data = model.dict(exclude=exclude_set)
    else:
        data = json.loads(model.json(exclude=exclude_set))

    return data


@asynccontextmanager
async def create_async_db_connection(db_url: str) -> SAConnection:
    """Create async db connection via aiopg"""
    async with create_engine(db_url) as engine:
        async with engine.acquire() as connection:
            yield connection


def is_field_equal_to_default(entity: BaseModel, field_name: str) -> bool:
    return getattr(entity, field_name) == entity.__fields__[field_name].default


T = TypeVar("T")
F = TypeVar("F")


async def mixed_zip(first: Iterator[T], second: AsyncIterator[F]) -> AsyncIterator[Tuple[T, F]]:
    """
    A zip-like generator in which the first arg is a synchronous iterator, while the second one
    is an async iterator.
    """
    first_iter = iter(first)
    async for s in second:
        try:
            f = next(first_iter)
        except StopIteration:
            break
        yield f, s
