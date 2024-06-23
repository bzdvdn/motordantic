import uuid

import pytest_asyncio
import pytest

from bson import ObjectId

from motordantic.document import Document
from motordantic.exceptions import MotordanticValidationError


class User(Document):
    id: str
    name: str
    email: str


@pytest_asyncio.fixture(scope='session', autouse=True)
async def innert_users(event_loop):
    yield
    await User.Q.drop_collection(force=True)


@pytest.mark.asyncio
async def test_raw_insert_one(connection):
    with pytest.raises(MotordanticValidationError):
        result = await User.Q.raw_query(
            'insert_one', {'id': str(uuid.uuid4()), 'name': {}, 'email': []}
        )
    result = await User.Q.raw_query(
        'insert_one',
        {'id': str(uuid.uuid4()), 'name': 'first', 'email': 'first@mail.ru'},
    )
    assert isinstance(result.inserted_id, ObjectId)


@pytest.mark.asyncio
async def test_raw_find_one(connection):
    result = await User.Q.raw_query('find_one', {'name': 'first'})
    assert result['name'] == 'first'
    assert result['email'] == 'first@mail.ru'


@pytest.mark.asyncio
async def test_raw_update_one(connection):
    with pytest.raises(MotordanticValidationError):
        result = await User.Q.raw_query(
            'update_one', [{'id': uuid.uuid4(), 'name': {}, 'email': []}]
        )
    result = await User.Q.raw_query(
        'update_one', raw_query=({'name': 'first'}, {'$set': {'name': 'updated'}})
    )

    assert result.modified_count == 1

    modifed_result = await User.Q.find_one(email='first@mail.ru')
    assert modifed_result.name == 'updated'
