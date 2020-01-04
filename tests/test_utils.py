import datetime as dt

import pytest
from pydantic import BaseModel

from repka.utils import model_to_primitive


class MyModel(BaseModel):
    id: int
    title: str
    created: dt.datetime


@pytest.fixture()
def model() -> MyModel:
    return MyModel(id=1, title="model", created=dt.datetime(2020, 1, 4))


def test_model_to_primitive(model: MyModel) -> None:
    dict_ = model_to_primitive(model)
    assert dict_ == {
        "id": model.id,
        "title": model.title,
        "created": model.created.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def test_model_to_primitive_excludes_id(model: MyModel) -> None:
    dict_ = model_to_primitive(model, without_id=True)
    assert "id" not in dict_


def test_model_to_primitive_excludes_fields_from_list(model: MyModel) -> None:
    dict_ = model_to_primitive(model, exclude=["title", "created"])
    assert "title" not in dict_ and "created" not in dict_
