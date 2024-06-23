import pytest_asyncio
import pytest

from motordantic.document import Document
from motordantic.utils.pydantic import IS_PYDANTIC_V2
from motordantic.config import ConfigDict


class InnerTicket(Document):
    name: str
    position: int
    config: dict
    params: dict
    sign: int = 1
    type_: str = "ga"
    array: list = []

    if IS_PYDANTIC_V2:
        model_config = ConfigDict(excluded_query_fields=("sign", "type"))
    else:

        class Config:
            excluded_query_fields = ("sign", "type")


@pytest_asyncio.fixture(scope="session", autouse=True)
async def innert_tickets(event_loop):

    await InnerTicket.Q.insert_one(
        name="first",
        position=1,
        config={"url": "localhost", "username": "admin"},
        params={},
        array=["1", "type"],
    )
    await InnerTicket.Q.insert_one(
        name="second",
        position=2,
        config={"url": "google.com", "username": "staff"},
        params={"1": 2},
        array=["second", "type2"],
    )
    await InnerTicket.Q.insert_one(
        name="third",
        position=3,
        config={"url": "yahoo.com", "username": "trololo"},
        params={"1": 1},
        array=["third", "type3"],
    )
    await InnerTicket.Q.insert_one(
        name="fourth",
        position=4,
        config={"url": "yahoo.com", "username": "trololo"},
        params={"2": 2},
        array=["fourth", "type4"],
    )
    yield
    await InnerTicket.Q.drop_collection(force=True)


@pytest.mark.asyncio
async def test_update_many(connection):
    # .create_documents()
    updated = await InnerTicket.Q.update_many(
        position__range=[3, 4], name__ne="hhh", config__url__set="test.io"
    )
    assert updated == 2
    last = await InnerTicket.Q.find_one(position=4)
    assert last.config["url"] == "test.io"


@pytest.mark.asyncio
async def test_inner_find_one_in_array(connection):
    data = await InnerTicket.Q.find_one(array__1__regex="2")
    assert data.name == "second"

    data = await InnerTicket.Q.find_one(array__1__regex="2", array__0__regex="1")
    assert data is None

    data = await InnerTicket.Q.find_one(array__0="1")
    assert data.name == "first"


@pytest.mark.asyncio
async def test_inner_find_one(connection):
    data = await InnerTicket.Q.find_one(config__url__startswith="google", params__1=2)
    assert data.name == "second"

    data = await InnerTicket.Q.find_one(
        config__url__startswith="yahoo", params__1="qwwe"
    )
    assert data is None


@pytest.mark.asyncio
async def test_inner_update_one(connection):
    updated = await InnerTicket.Q.update_one(
        config__url__startswith="goo", config__url__set="test.io"
    )
    assert updated == 1
    data = await InnerTicket.Q.find_one(config__url__startswith="test")
    assert data.name == "second"
