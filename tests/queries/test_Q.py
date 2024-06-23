import pytest
import pytest_asyncio

from motordantic.document import Document
from motordantic.query.query import Q


class TicketForQuery(Document):
    name: str
    position: int


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_ticket_collection(event_loop):
    yield
    await TicketForQuery.Q().drop_collection(force=True)


def test_query_organization(connection):
    query = Q(name="123") | Q(name__ne="124") & Q(position=1) | Q(position=2)
    data = query.to_query(TicketForQuery.Q())
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
    inserted = await TicketForQuery.Q().insert_many(query)
    assert inserted == 2

    query = Q(name="first") | Q(position=1) & Q(name="second")
    data = await TicketForQuery.Q().find_one(query)
    assert data.name == "first"

    query = Q(position=3) | Q(position=1) & Q(name="second")
    data = await TicketForQuery.Q().find_one(query)
    assert data is None

    query = Q(position=3) | Q(position=2) & Q(name="second")
    data = await TicketForQuery.Q().find_one(query)
    assert data.name == "second"
