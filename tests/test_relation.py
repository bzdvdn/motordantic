from typing import List, Optional

import pytest_asyncio
import pytest

from motordantic.document import Document
from motordantic.types import Relation


class Author(Document):
    name: str


class Book(Document):
    title: str
    author: Relation[Author]


class Publish(Document):
    name: str
    books: List[Relation[Book]]


class OptionalTestPublisher(Document):
    name: str
    author: Optional[Relation[Author]] = None


@pytest_asyncio.fixture(scope="session", autouse=True)
async def relation_data(event_loop, connection):
    authors = [Author(name="Author1"), Author(name="Author2")]
    await Author.Q().insert_many(authors)

    author1 = await Author.Q().find_one(name="Author1")
    author2 = await Author.Q().find_one(name="Author2")
    book1 = Book(
        title="first book from author1",
        author=author1,
    )
    book2 = await Book(
        title="first book from author2",
        author=author2,
    ).save()
    book1_id = await Book.Q().insert_one(**book1.data)

    book = await Book.Q().find_one(title__regex="2")
    publish1 = Publish(name="publush1", books=[book])
    # print(Publish.__db_refs__)
    await publish1.save()
    publish2 = Publish(name="publush2", books=[Book.to_relation(book1_id), book2])
    await publish2.save()
    optional_publisher = await OptionalTestPublisher.Q().insert_one(
        name="test_publisher1"
    )
    optional_publisher = await OptionalTestPublisher.Q().insert_one(
        name="test_publisher2", author=author2
    )
    yield
    await Author.Q().drop_collection(force=True)
    await Book.Q().drop_collection(force=True)
    await Publish.Q().drop_collection(force=True)
    await OptionalTestPublisher.Q().drop_collection(force=True)


@pytest.mark.asyncio
async def test_book_relation(connection):
    book = await Book.Q().find_one(title="first book from author1")
    book_author = await book.author.get()
    # book_author = await book.author.get()
    native_author = await Author.Q().find_one(name="Author1")
    assert book_author == native_author

    book = await Book.Q().find_one(
        title="first book from author1", with_relations_objects=True
    )
    # assert book_author == 1
    assert book.author == native_author


@pytest.mark.asyncio
async def test_publish_relation(connection):
    publish = await Publish.Q().find_one(name="publush1", with_relations_objects=True)

    assert publish.data["books"][0]["title"] == "first book from author2"
    book = await Book.Q().find_one(
        title="first book from author2", with_relations_objects=True
    )
    assert publish.books == [book]
    publish2 = await Publish.Q().find_one(name__regex="2", with_relations_objects=True)
    assert len(publish2.books) == 2
    assert isinstance(publish2.books[0], Book)


@pytest.mark.asyncio
async def test_find_relation_models_without_relations_objects(connection):
    book = await Book.Q().find_one(title__regex="1")
    assert book.author.db_ref.collection == "author"
    assert isinstance(book.data["author"], dict)


@pytest.mark.asyncio
async def test_create_object(connection):
    author = Author(name="J. R. R. Tolkien")
    await author.save()

    book = Book(title="lord of the rings", author=author.data)
    await book.save()

    assert isinstance(book.author, Relation)

    book = await Book.Q().find_one(_id=book._id)
    assert book.title == "lord of the rings"
    relation_author = await book.author.get()
    assert relation_author.name == "J. R. R. Tolkien"


@pytest.mark.asyncio
async def test_optional_publisher(connection):
    optional_publisher = await OptionalTestPublisher.Q().find_one(
        name="test_publisher1", with_relations_objects=True
    )
    assert optional_publisher.author is None

    optional_publisher = await OptionalTestPublisher.Q().find_one(
        name="test_publisher2", with_relations_objects=True
    )
    assert optional_publisher.author is not None
    assert optional_publisher.author.name == "Author2"
