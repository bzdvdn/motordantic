import pytest
import pytest_asyncio
from typing import Any

from motordantic.exceptions import MotordanticValidationError
from motordantic.document import Document
from motordantic.utils.pydantic import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from pydantic import (
        GetCoreSchemaHandler,
    )
    from pydantic_core import CoreSchema, core_schema
    from pydantic_core.core_schema import (
        ValidationInfo,
        str_schema,
    )


class TestField(str):
    type_ = str
    required = False
    default = None
    validate_always = False
    alias = ""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    if IS_PYDANTIC_V2:

        @classmethod
        def validate(cls, value, _: ValidationInfo):  # type: ignore
            if not value or value == "no":
                raise ValueError("invalid value")
            return value

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:  # type: ignore
            return core_schema.json_or_python_schema(
                python_schema=core_schema.with_info_plain_validator_function(
                    cls.validate  # type: ignore
                ),
                json_schema=str_schema(),
                serialization=core_schema.plain_serializer_function_ser_schema(
                    lambda instance: str(instance)
                ),
            )

    else:

        @classmethod
        def validate(cls, value):  # type: ignore
            if not value or value == "no":
                raise ValueError("invalid value")
            return value


class Validate(Document):
    name: str
    position: int
    config: dict
    sign: int = 1
    type_: str = "ga"
    test: TestField = TestField("test value")
    array: list = [1, 2]


@pytest_asyncio.fixture(scope="session", autouse=True)
async def drop_ticket_collection(event_loop):
    yield
    await Validate.Q().drop_collection(force=True)


@pytest.mark.asyncio
async def test_validate(connection):
    with pytest.raises(MotordanticValidationError):
        await Validate.Q().find_one(array="invalid")

    with pytest.raises(MotordanticValidationError):
        await Validate.Q().find_one(_id="invalid")

    with pytest.raises(MotordanticValidationError):
        await Validate.Q().find_one(position="invalid")
        await Validate.Q().find_one(config="invalid")

    with pytest.raises(MotordanticValidationError):
        Validate(
            array={"invalid": "true"},
            name="123",
            position="invalid postion",
            config={},
        )

    with pytest.raises(MotordanticValidationError):
        Validate(array=[], name="123", position=1, config={}, test="no")
