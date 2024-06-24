import pytest
import pytest_asyncio

from motordantic.document import Document, DynamicCollectionDocument
from motordantic.exceptions import MotordanticValidationError
from pydantic import BaseModel


class Config(BaseModel):
    path: str = "/home/"
    env: str = "test"


class Application(Document):
    name: str
    cfg: Config
    lang: str


@pytest_asyncio.fixture(scope="session", autouse=True)
async def application_data(connection):
    application = await Application(name="test", cfg=Config(), lang="python").save()
    yield
    await Application.Q().drop_collection(force=True)


@pytest.mark.asyncio
async def test_application_data(connection):
    application = await Application.Q().find_one(name="test")
    assert application is not None
    data = application.data
    assert isinstance(data["cfg"], dict)
    assert data["cfg"]["env"] == "test"

    data = await Application.Q().find_one(cfg__env="test")
    assert data is not None
    assert data.name == "test"
    data = await Application.Q().find_one(cfg__env="invalid")
    assert data is None


@pytest.mark.asyncio
async def test_raise_with_field_mongo_model(connection):
    class Default(Document):
        name: str
        app: Application

    with pytest.raises(MotordanticValidationError):
        app = await Application.Q().find_one()
        d = Default(name="default", app=app)

    with pytest.raises(MotordanticValidationError):

        class FieldModel(Document):
            name: str

        class TestValidateModel(Document):
            name: str
            field_model: FieldModel

        fm = FieldModel(name="123")
        _ = TestValidateModel(field_model=fm, name="123")
    with pytest.raises(MotordanticValidationError):

        class FieldModelDynamic(DynamicCollectionDocument):
            name: str

        class TestValidateModelDynamic(DynamicCollectionDocument):
            name: str
            field_model: FieldModel

        fm = FieldModelDynamic(name="123")
        _ = TestValidateModelDynamic(field_model=fm, name="123")

    with pytest.raises(MotordanticValidationError):
        from motordantic.types import Relation

        class FieldModelDynamicRelation(DynamicCollectionDocument):
            name: str

        class TestValidateModelDynamicRelation(DynamicCollectionDocument):
            name: str
            field_model: Relation[FieldModel]

        fm = FieldModelDynamicRelation(name="123")
        _ = TestValidateModelDynamicRelation(field_model=fm, name="123")
