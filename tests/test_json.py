import json
import os

from _pytest.pytester import Testdir
from typing import Sequence, Dict

from repka.json_ import DictJsonRepo


def test_dict_json_repo_reads_json_file_as_list_of_dicts(testdir: Testdir) -> None:
    data = [{"field": "value"}]
    file = testdir.makefile(".json", json.dumps(data))

    repo: DictJsonRepo[Sequence[Dict]] = DictJsonRepo()

    assert repo.read(file) == data


def test_dict_json_repo_write_list_of_dicts_as_json(testdir: Testdir) -> None:
    data = [{"field": "value"}]
    file = "sam.json"

    repo: DictJsonRepo[Sequence[Dict]] = DictJsonRepo(str(testdir.tmpdir))
    repo.write(data, file)

    assert repo.read(file) == data


def test_dict_json_repo_creates_file_if_no_file(testdir: Testdir) -> None:
    file = "sam.json"

    repo: DictJsonRepo[Sequence[Dict]] = DictJsonRepo(str(testdir.tmpdir))
    repo.read_or_write_default(file, lambda: [{"field": "value"}])

    assert os.path.exists(file)


def test_dict_json_repo_reads_file_if_exist(testdir: Testdir) -> None:
    data = [{"field": "value"}]
    file = testdir.makefile(".json", json.dumps(data))

    repo: DictJsonRepo[Sequence[Dict]] = DictJsonRepo()

    assert repo.read_or_write_default(file, lambda: []) == (data, True)
