import pytest
import pytest_asyncio

from bson import ObjectId

from motordantic.document import Document, DynamicCollectionDocument
from motordantic.session import SessionSync
from motordantic.config import ConfigDict
from motordantic.utils.pydantic import IS_PYDANTIC_V2

collection_name = "dynamic_ticket_sync"


class TicketSync(Document):
    name: str
    position: int
    config: dict
    sign: int = 1
    type_: str = "ga"
    array: list = [1, 2]

    if IS_PYDANTIC_V2:
        model_config = ConfigDict(excluded_query_fields=("sign", "type"))
    else:

        class Config:
            excluded_query_fields = ("sign", "type")


class DynamicTicketSync(DynamicCollectionDocument):
    name: str
    position: int
    config: dict
    sign: int = 1
    type_: str = "ga"
    array: list = [1, 2]

    if IS_PYDANTIC_V2:
        model_config = ConfigDict(excluded_query_fields=("sign", "type"))
    else:

        class Config:
            excluded_query_fields = ("sign", "type")


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_ticket_sync_collection(event_loop):
    yield
    await TicketSync.Q().drop_collection(force=True)


@pytest.fixture(scope="session", autouse=True)
def drop_dynamic_ticket_sync_collection(event_loop):
    yield
    DynamicTicketSync.QSync(collection_name).drop_collection(force=True)


def test_sync_insert_one(connection):
    data = {"name": "sync1", "position": 2310, "config": {"as_sync": True}}
    dynamic_data = {"name": "sync2", "position": 2311, "config": {"as_sync": True}}
    object_id = TicketSync.QSync().insert_one(**data)
    dynamic_object_id = DynamicTicketSync.QSync(collection_name).insert_one(
        **dynamic_data
    )
    assert isinstance(object_id, ObjectId)
    assert isinstance(dynamic_object_id, ObjectId)


def test_sync_find_one(connection):
    ticket = TicketSync.QSync().find_one(name="sync1")
    assert ticket.name == "sync1"
    assert ticket.position == 2310
    ticket = DynamicTicketSync.QSync(collection_name).find_one(name="sync2")
    assert ticket.name == "sync2"
    assert ticket.position == 2311


def test_sync_insert_many(connection):
    data = [
        {"name": "sync", "position": 2, "config": {"as_sync": True}},
        {"name": "sync3", "position": 3, "config": {"as_sync": True}},
        {"name": "sync_fourth", "position": 4, "config": {"as_sync": False}},
    ]
    inserted = TicketSync.QSync().insert_many(data)
    assert inserted == 3
    inserted = DynamicTicketSync.QSync(collection_name).insert_many(data)
    assert inserted == 3


def test_sync_aggregation(connection):
    summ = TicketSync.QSync().aggregate_sum("position")
    assert summ == 2319

    max_ = TicketSync.QSync().aggregate_max("position")
    assert max_ == 2310

    min_ = TicketSync.QSync().aggregate_min("position")
    assert min_ == 2

    avg = TicketSync.QSync().aggregate_avg("position")
    assert avg == 579.75

    summ = DynamicTicketSync.QSync(collection_name).aggregate_sum("position")
    assert summ == 2320

    max_ = DynamicTicketSync.QSync(collection_name).aggregate_max("position")
    assert max_ == 2311

    min_ = DynamicTicketSync.QSync(collection_name).aggregate_min("position")
    assert min_ == 2

    avg = DynamicTicketSync.QSync(collection_name).aggregate_avg("position")
    assert avg == 580


def test_find_sync(connection):
    data = TicketSync.QSync().find().list
    assert len(data) == 4
    assert data[-1].name == "sync_fourth"

    data = DynamicTicketSync.QSync(collection_name).find().list
    assert len(data) == 4
    assert data[-1].name == "sync_fourth"


def test_sync_distinct(connection):
    data = TicketSync.QSync().distinct("position", name="sync")
    assert data == [2]
    data = DynamicTicketSync.QSync(collection_name).distinct("position", name="sync")
    assert data == [2]


def test_session_sync(connection):
    with SessionSync(TicketSync.manager) as session:
        ticket = TicketSync.QSync().find_one(session=session)
        assert ticket is not None
    with SessionSync(DynamicTicketSync.manager) as session:
        ticket = DynamicTicketSync.QSync(collection_name).find_one(session=session)
        assert ticket is not None
