import pytest_asyncio

from bson import ObjectId

from motordantic.document import Document
from motordantic.session import SessionSync
from motordantic.config import ConfigDict
from motordantic.utils.pydantic import IS_PYDANTIC_V2


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


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_ticket_sync_collection(event_loop):
    yield
    await TicketSync.Q().drop_collection(force=True)


def test_sync_insert_one(connection):
    data = {"name": "sync1", "position": 2310, "config": {"as_sync": True}}
    object_id = TicketSync.QSync().insert_one(**data)
    assert isinstance(object_id, ObjectId)


def test_sync_find_one(connection):
    ticket = TicketSync.QSync().find_one(name="sync1")
    assert ticket.name == "sync1"
    assert ticket.position == 2310


def test_sync_insert_many(connection):
    data = [
        {"name": "sync2", "position": 2, "config": {"as_sync": True}},
        {"name": "sync3", "position": 3, "config": {"as_sync": True}},
        {"name": "sync_fourth", "position": 4, "config": {"as_sync": False}},
    ]
    inserted = TicketSync.QSync().insert_many(data)
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


def test_find_sync(connection):
    data = TicketSync.QSync().find().list
    assert len(data) == 4
    assert data[-1].name == "sync_fourth"


def test_sync_distinct(connection):
    data = TicketSync.QSync().distinct("position", name="sync2")
    assert data == [2]


def test_session_sync(connection):
    with SessionSync(TicketSync.manager) as session:
        ticket = TicketSync.QSync().find_one(session=session)
        assert ticket is not None
