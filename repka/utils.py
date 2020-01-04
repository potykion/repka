import json
from typing import Sequence, Dict, Set, Union

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
