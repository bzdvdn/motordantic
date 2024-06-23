import pytest
import pytest_asyncio

from pymongo.collection import WriteConcern

from motordantic.document import Document


class Box(Document):
    name: str
    position: int


@pytest_asyncio.fixture(scope='session', autouse=True)
async def drop_ticket_collection(event_loop):
    yield
    await Box.Q.drop_collection(force=True)


@pytest.mark.asyncio
async def test_insert_many_with_wrice_concern(connection):
    data = [
        {
            'name': 'first',
            'position': 1,
        },
        {
            'name': 'second',
            'position': 2,
        },
    ]
    write_concern = WriteConcern(w=0)
    inserted_count = await Box.Q.insert_many(
        data, ordered=False, write_concern=write_concern
    )
    assert inserted_count == 2
