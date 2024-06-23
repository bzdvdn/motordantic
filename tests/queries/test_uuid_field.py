import uuid

import pytest_asyncio
import pytest

from bson import ObjectId

from motordantic.document import Document
from motordantic.types import UUIDField
from motordantic.exceptions import MotordanticValidationError


class Article(Document):
    article_id: UUIDField
    title: str
    tags: list


@pytest_asyncio.fixture(scope='session', autouse=True)
async def innert_users(event_loop):
    yield
    await Article.Q.drop_collection(force=True)


@pytest.mark.asyncio
async def test_insert_one_with_uuid_field(connection):
    with pytest.raises(MotordanticValidationError):
        article = Article(article_id='2310', title='invalid', tags=['invalid'])

    uid = uuid.uuid1()
    article = Article(article_id=uid, title='invalid', tags=['invalid'])
    await article.save()
    assert isinstance(article._id, ObjectId)
    assert isinstance(article.article_id, uuid.UUID)
    assert article.article_id == uid

    db_article = await Article.Q.find_one(article_id=uid)
    assert db_article._id == article._id

    db_article_hex = await Article.Q.find_one(article_id=uid.hex)
    assert db_article_hex._id == db_article._id
