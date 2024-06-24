import pytest
import pytest_asyncio

from motordantic.document import DynamicCollectionDocument
from motordantic.query.query import Q

collection_name = "dynamic_ticker_for_query"


class TicketForQuery(DynamicCollectionDocument):
    name: str
    position: int


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_ticket_collection(event_loop):
    yield
    await TicketForQuery.Q(collection_name).drop_collection(force=True)


def test_query_organization(connection):
    query = Q(name="123") | Q(name__ne="124") & Q(position=1) | Q(position=2)
    data = query.to_query(TicketForQuery.Q(collection_name))
    value = {
        "$or": [
            {"name": "123"},
            {"$and": [{"name": {"$ne": "124"}}, {"position": 1}]},
            {"position": 2},
        ]
    }
    assert data == value


@pytest.mark.asyncio
async def test_query_result(connection):
    query = [
        TicketForQuery(name="first", position=1),
        TicketForQuery(name="second", position=2),
    ]
    inserted = await TicketForQuery.Q(collection_name).insert_many(query)
    assert inserted == 2

    query = Q(name="first") | Q(position=1) & Q(name="second")
    data = await TicketForQuery.Q(collection_name).find_one(query)
    assert data is not None
    assert data.name == "first"

    query = Q(position=3) | Q(position=1) & Q(name="second")
    data = await TicketForQuery.Q(collection_name).find_one(query)
    assert data is None

    query = Q(position=3) | Q(position=2) & Q(name="second")
    data = await TicketForQuery.Q(collection_name).find_one(query)
    assert data is not None
    assert data.name == "second"
