import pytest
import re

from bson import Regex

from motordantic.document import Document
from motordantic.query.extra import ExtraQueryMapper


class User(Document):
    id: str
    name: str
    counter: int
    date: str


def test_in_extra_param():
    with pytest.raises(TypeError):
        ExtraQueryMapper(User, "name").query(["in"], (1, 3))
    extra = ExtraQueryMapper(User, "name").query(["in"], ["1", "3"])
    value = {"name": {"$in": ["1", "3"]}}
    assert extra == value


def test_nin_extra_param():
    with pytest.raises(TypeError):
        ExtraQueryMapper(User, "name").query(["nin"], (1, 3))
    extra = ExtraQueryMapper(User, "name").query(["nin"], ["1", "3"])
    value = {"name": {"$nin": ["1", "3"]}}
    assert extra == value


def test_ne_extra_param():
    extra = ExtraQueryMapper(User, "name").query(["ne"], "test")
    value = {"name": {"$ne": "test"}}
    assert extra == value


def test_regex_extra_param():
    extra = ExtraQueryMapper(User, "name").query(["regex"], "test")
    value = {"name": {"$regex": Regex.from_native(re.compile("test"))}}
    assert extra == value


def test_regex_ne_extra_param():
    extra = ExtraQueryMapper(User, "name").query(["regex_ne"], "test")
    value = {"name": {"$not": Regex.from_native(re.compile("test"))}}
    assert extra == value


def test_startswith_extra_param():
    extra = ExtraQueryMapper(User, "name").query(["startswith"], "test")
    value = {"name": {"$regex": Regex.from_native(re.compile("^test"))}}
    assert extra == value


def test_endswith_extra_param():
    extra = ExtraQueryMapper(User, "name").query(["endswith"], "test")
    value = {"name": {"$regex": Regex.from_native(re.compile("test$"))}}
    assert extra == value


def test_not_endswith_extra_param():
    extra = ExtraQueryMapper(User, "name").query(["not_endswith"], "test")
    value = {"name": {"$not": Regex.from_native(re.compile("test$"))}}
    assert extra == value


def test_not_startswith_extra_param():
    extra = ExtraQueryMapper(User, "name").query(["not_startswith"], "test")
    value = {"name": {"$not": Regex.from_native(re.compile("^test"))}}
    assert extra == value


def test_range_extra_param():
    with pytest.raises(ValueError):
        ExtraQueryMapper(User, "date").query(["range"], "test")
    extra = ExtraQueryMapper(User, "date").query(
        ["range"], ["2020-01-01", "2020-03-01"]
    )
    value = {"date": {"$gte": "2020-01-01", "$lte": "2020-03-01"}}
    assert extra == value


def test_lts_gts_params():
    extra = ExtraQueryMapper(User, "date").query(["lt"], "2020-01-01")
    assert extra == {"date": {"$lt": "2020-01-01"}}

    extra = ExtraQueryMapper(User, "date").query(["gt"], "2020-01-01")
    assert extra == {"date": {"$gt": "2020-01-01"}}

    extra = ExtraQueryMapper(User, "date").query(["gte"], "2020-01-01")
    assert extra == {"date": {"$gte": "2020-01-01"}}

    extra = ExtraQueryMapper(User, "date").query(["lte"], "2020-01-01")
    assert extra == {"date": {"$lte": "2020-01-01"}}


def test_inc_params():
    with pytest.raises(ValueError):
        ExtraQueryMapper(User, "counter").query(["inc"], "2313123131")

    extra = ExtraQueryMapper(User, "counter").query(["inc"], 23)
    assert extra == {"$inc": {"counter": 23}}


def test_exists_params():
    with pytest.raises(TypeError):
        ExtraQueryMapper(User, "counter").query(["exists"], "2313123131")

    extra = ExtraQueryMapper(User, "counter").query(["exists"], False)
    assert extra == {"counter": {"$exists": False}}
