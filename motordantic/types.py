from typing import Optional, TypeVar, Generic, Union, Any, Type, get_args
from enum import Enum
from uuid import UUID

from bson import ObjectId, DBRef
from bson.errors import InvalidId
from pydantic import BaseModel
from pydantic.fields import FieldInfo as ModelField


from .utils.pydantic import IS_PYDANTIC_V2, parse_object_as
from .custom_typing import DocumentType


if IS_PYDANTIC_V2:
    from pydantic import (  # type: ignore
        GetCoreSchemaHandler,  # type: ignore
        GetJsonSchemaHandler,  # type: ignore
    )
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema, core_schema
    from pydantic_core.core_schema import (
        ValidationInfo,
        str_schema,
    )

else:
    from pydantic.json import ENCODERS_BY_TYPE


__all__ = (
    "ObjectIdStr",
    "UUIDField",
    "Relation",
    "RelationInfo",
    "RelationTypes",
)


T = TypeVar("T")


class ObjectIdStr(str):
    """Field for validate string like ObjectId"""

    type_ = ObjectId
    required = False
    default = None
    validate_always = False
    alias = ""

    if IS_PYDANTIC_V2:

        @classmethod
        def validate(cls, v, _: ValidationInfo):  # type: ignore
            if isinstance(v, ObjectId):
                return v
            try:
                return ObjectId(str(v))
            except InvalidId:
                raise ValueError(f"invalid ObjectId - {v}")

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

        @classmethod
        def __get_pydantic_json_schema__(
            cls, schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler  # type: ignore
        ) -> JsonSchemaValue:
            json_schema = handler(schema)
            json_schema.update(
                type="string",
                example="5eb7cf5a86d9755df3a6c593",
            )
            return json_schema

    else:

        @classmethod
        def __get_validators__(cls):
            yield cls.validate

        @classmethod
        def validate(cls, v: str) -> ObjectId:
            if isinstance(v, ObjectId):
                return v
            try:
                return ObjectId(str(v))
            except InvalidId:
                raise ValueError(f"invalid ObjectId - {v}")

        @classmethod
        def __modify_schema__(cls, field_schema):
            field_schema.update(type="string")


class UUIDField(str):
    """Field for validate string like UUID"""

    type_ = UUID
    required = False
    default = None
    validate_always = False
    alias = ""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    if IS_PYDANTIC_V2:

        @classmethod
        def validate(cls, v: str, _: ValidationInfo):  # type: ignore
            if isinstance(v, UUID):
                return v
            try:
                return UUID(str(v))
            except ValueError:
                raise ValueError(f"invalid UUID - {v}")

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

        @classmethod
        def __get_pydantic_json_schema__(
            cls, schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler  # type: ignore
        ) -> JsonSchemaValue:
            json_schema = handler(schema)
            json_schema.update(
                type="string",
            )
            return json_schema

    else:

        @classmethod
        def validate(cls, v: str) -> UUID:
            if isinstance(v, UUID):
                return v
            try:
                return UUID(str(v))
            except ValueError:
                raise ValueError(f"invalid UUID - {v}")

        @classmethod
        def __modify_schema__(cls, field_schema):
            field_schema.update(type="string")


class RelationTypes(str, Enum):
    SINGLE = "SINGLE"
    OPTIONAL_SINGLE = "OPTIONAL_SINGLE"
    ARRAY = "ARRAY"


class RelationInfo(BaseModel):
    field: str
    document_class: Type[BaseModel]
    relation_type: RelationTypes


class Relation(Generic[T]):
    def __init__(self, db_ref: DBRef, document_class: DocumentType):
        self.db_ref = db_ref
        self.document_class = document_class

    async def get(self) -> Optional[BaseModel]:
        result = await self.document_class.Q().find_one(_id=self.db_ref.id, with_relations_objects=True)  # type: ignore
        return result

    @classmethod
    def _validate_for_model(
        cls, v: Union[dict, BaseModel], document_class: DocumentType
    ) -> "Relation":
        validate_func = (
            document_class.model_validate if IS_PYDANTIC_V2 else document_class.validate
        )
        parsed = (
            document_class.parse_obj(v) if isinstance(v, dict) else validate_func(v)
        )
        new_id = (
            parsed._id
            if isinstance(parsed._id, ObjectId)
            else parse_object_as(
                ObjectIdStr,
                parsed._id,  # type: ignore
            )
        )
        db_ref = DBRef(collection=document_class.get_collection_name(), id=new_id)
        return cls(db_ref=db_ref, document_class=document_class)

    if IS_PYDANTIC_V2:

        @staticmethod
        def serialize(value: Union["Relation", BaseModel]):
            if isinstance(value, Relation):
                return value.to_dict()
            return value.model_dump()  # type: ignore

        @classmethod
        def build_validation(cls, handler, source_type):
            def validate(v: Union[DBRef, T], validation_info: ValidationInfo):  # type: ignore
                document_class = get_args(source_type)[0]
                if isinstance(v, DBRef):
                    return cls(db_ref=v, document_class=document_class)
                if isinstance(v, Relation):
                    return v
                if isinstance(v, dict):
                    try:
                        return cls(db_ref=DBRef(**v), document_class=document_class)
                    except TypeError:
                        return cls._validate_for_model(v, document_class)
                if isinstance(v, BaseModel):
                    return cls._validate_for_model(v, document_class)
                raise ValueError(f"invalod type - {v}")

            return validate

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source_type: Any, handler: GetCoreSchemaHandler
        ) -> CoreSchema:  # type: ignore
            return core_schema.json_or_python_schema(
                python_schema=core_schema.with_info_plain_validator_function(
                    cls.build_validation(handler, source_type)
                ),
                json_schema=core_schema.typed_dict_schema(
                    {
                        "id": core_schema.typed_dict_field(core_schema.str_schema()),
                        "collection": core_schema.typed_dict_field(
                            core_schema.str_schema()
                        ),
                    }
                ),
                serialization=core_schema.plain_serializer_function_ser_schema(  # type: ignore
                    lambda instance: cls.serialize(instance)  # type: ignore
                ),
            )

    else:

        @classmethod
        def __get_validators__(cls):
            yield cls.validate

        @classmethod
        def validate(cls, v: Union[DBRef, T], field: ModelField) -> "Relation":
            document_class = field.sub_fields[0].type_  # type: ignore
            if isinstance(v, DBRef):
                return cls(db_ref=v, document_class=document_class)
            if isinstance(v, Relation):
                return v
            if isinstance(v, dict):
                try:
                    return cls(db_ref=DBRef(**v), document_class=document_class)
                except TypeError:
                    return cls._validate_for_model(v, document_class)
            if isinstance(v, BaseModel):
                return cls._validate_for_model(v, document_class)
            raise ValueError(f"invalod type - {v}")

    def to_ref(self) -> DBRef:
        return self.db_ref

    def to_dict(self) -> dict:
        return {"id": str(self.db_ref.id), "collection": self.db_ref.collection}

    @property
    def data(self) -> dict:
        return self.to_dict()


if not IS_PYDANTIC_V2:
    ENCODERS_BY_TYPE[Relation] = lambda r: r.to_dict()
    ENCODERS_BY_TYPE[ObjectIdStr] = lambda o: str(o)
