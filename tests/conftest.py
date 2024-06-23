import asyncio

import pytest
from motordantic.connection import connect

# from motordantic.models import MongoModel


@pytest.fixture(scope="session", autouse=True)
async def connection():
    connection = connect("mongodb://127.0.0.1:27017", "test")
    # MongoModel.use(connection)
    yield connection


@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()
