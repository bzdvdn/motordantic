import pytest
import pytest_asyncio

from bson import ObjectId

from motor.motor_asyncio import AsyncIOMotorClientSession
from motordantic.exceptions import DoesNotExist
from motordantic.document import Document

from motordantic.session import Session
from motordantic.config import ConfigDict
from motordantic.utils.pydantic import IS_PYDANTIC_V2


class Ticket(Document):
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


class Trash(Document):
    name: str
    date: str


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_ticket_collection(event_loop):
    yield
    await Ticket.Q().drop_collection(force=True)
    await Trash.Q().drop_collection(force=True)


@pytest.mark.asyncio
async def test_insert_one(connection):
    data = {
        "name": "second",
        "position": 2,
        "config": {"param1": "value2"},
        "array": ["test", "adv", "comagic"],
    }
    object_id = await Ticket.Q().insert_one(**data)
    assert isinstance(object_id, ObjectId)


@pytest.mark.asyncio
async def test_insert_many(connection):
    data = [
        {
            "name": "first",
            "position": 1,
            "config": {"param1": "value"},
            "array": ["test", "adv", "calltouch"],
        },
        {
            "name": "third",
            "position": 3,
            "config": {"param1": "value3"},
            "array": ["test", "adv", "comagic", "cost"],
        },
        {
            "name": "third",
            "position": 4,
            "config": {"param1": "value3"},
            "array": ["test", "adv", "comagic", "cost", "trash"],
        },
    ]
    inserted = await Ticket.Q().insert_many(data)
    assert inserted == 3


@pytest.mark.asyncio
async def test_distinct(connection):
    data = await Ticket.Q().distinct("position", name="second")
    assert data == [2]


@pytest.mark.asyncio
async def test_queryset_serialize(connection):
    result = await Ticket.Q().find(name="second")
    data = result.serialize(fields=["name", "config"])
    assert len(data[0]) == 2
    assert data[0]["config"] == {"param1": "value2"}
    assert data[0]["name"] == "second"
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_find_one(connection):
    await test_insert_one(connection)  # type: ignore
    data = await Ticket.Q().find_one(name="second")
    second = await Ticket.Q().find_one(_id=data._id)
    assert isinstance(data, Document)
    assert data.name == "second"  # type: ignore
    assert data.position == 2  # type: ignore
    assert second._id == data._id


@pytest.mark.asyncio
async def test_find(connection):
    result = await Ticket.Q().find(limit_rows=1, name="second")
    assert len(result.data) == 1


@pytest.mark.asyncio
async def test_async_get(connection):
    # await .test_insert_one()
    data = await Ticket.Q().get(name="second")
    second = await Ticket.Q().get(_id=data._id)
    assert isinstance(data, Document)
    assert data.name == "second"  # type: ignore
    assert data.position == 2  # type: ignore
    assert second._id == data._id
    with pytest.raises(DoesNotExist):
        _ = await Ticket.Q().get(name="invalid_name")


@pytest.mark.asyncio
async def test_save(connection):
    obj = await Ticket.Q().find_one(name="second")
    obj.position = 2310
    obj.name = "updated"
    await obj.save()
    none_obj = await Ticket.Q().find_one(name="second", position=222222)
    assert none_obj is None
    new_obj = await Ticket.Q().find_one(_id=obj._id)
    assert new_obj.name == "updated"
    assert new_obj.position == 2310

    obj.name = "second"
    obj.position = 2

    await obj.save()
    last_obj = await Ticket.Q().find_one(name="second")
    assert last_obj.name == "second"
    assert last_obj.position == 2


@pytest.mark.asyncio
async def test_update_one(connection):
    updated = await Ticket.Q().update_one(name="second", config__set={"updated": 1})
    data = await Ticket.Q().find_one(name="second")
    assert updated == 1
    assert data.config == {"updated": 1}


@pytest.mark.asyncio
async def test_update_many(connection):
    data = await Ticket.Q().update_many(name="third", config__set={"updated": 3})
    updated = await Ticket.Q().find_one(name="third")
    assert data == 2
    assert updated.config == {"updated": 3}


@pytest.mark.asyncio
async def test_gte_lte_in_one_field(connection):

    await Trash.Q().insert_one(name="first", date="2022-01-01")
    await Trash.Q().insert_one(name="second", date="2022-01-01")
    await Trash.Q().insert_one(name="third", date="2022-01-03")
    await Trash.Q().insert_one(name="4", date="2022-01-05")

    data = await Trash.Q().count(
        date__gte="2022-01-01",
        date__lte="2022-01-03",
    )
    assert data == 3


@pytest.mark.asyncio
async def test_find_with_regex(connection):
    second = await Ticket.Q().find_one(name__regex="ond")
    assert second.name == "second"

    starts = await Ticket.Q().find_one(name__startswith="s")
    assert starts._id == second._id

    istarts = await Ticket.Q().find_one(name__istartswith="S")
    assert istarts._id == second._id

    not_istarts = await Ticket.Q().find_one(name__not_istartswith="S", position=1)
    assert not_istarts.name == "first"


@pytest.mark.asyncio
async def test_find_and_update(connection):
    data_default = await Ticket.Q().find_one_and_update(name="second", position__set=23)
    assert data_default.position == 23

    data_with_prejection = await Ticket.Q().find_one_and_update(
        name="first", position__set=12, projection_fields=["position"]
    )
    assert isinstance(data_with_prejection, dict)
    assert data_with_prejection["position"] == 12


@pytest.mark.asyncio
async def test_session_start(connection):
    session = await Ticket.manager._start_session()
    assert isinstance(session, AsyncIOMotorClientSession)
    await session.end_session()  # type: ignore


@pytest.mark.asyncio
async def test_session_find(connection):
    session = await Ticket.manager._start_session()
    ticket = await Ticket.Q().find_one(session=session)
    assert ticket is not None


@pytest.mark.asyncio
async def test_session_context(connection):
    async with Session(Ticket.manager) as session:
        ticket = await Ticket.Q().find_one(session=session)
        assert ticket is not None
        new_ticket_id = await Ticket.Q().insert_one(
            name="session ticket",
            position=100,
            config={"param1": "session"},
            session=session,
        )
        assert isinstance(new_ticket_id, ObjectId)

        deleted = await Ticket.Q().delete_one(_id=new_ticket_id)
        assert deleted == 1


@pytest.mark.asyncio
async def test_delete_one(connection):
    deleted = await Ticket.Q().delete_one(name="second")
    assert deleted == 1


@pytest.mark.asyncio
async def test_delete_many(connection):
    deleted = await Ticket.Q().delete_many(name="second")
    assert deleted == 1
