import json
from typing import (
    Dict,
    Any,
    Union,
    Optional,
    List,
    Tuple,
    TYPE_CHECKING,
    ClassVar,
    TypeVar,
)

from bson import ObjectId, DBRef, decode as bson_decode
from bson.raw_bson import RawBSONDocument
from motor.core import AgnosticClientSession

from pydantic import (
    BaseModel as BasePydanticModel,
    ValidationError,
)

from pymongo import IndexModel

from .utils.pydantic import IS_PYDANTIC_V2
from .relation import RelationManager
from .types import ObjectIdStr, RelationInfo, Relation
from .exceptions import (
    MotordanticValidationError,
    MotordanticConnectionError,
)
from .property import classproperty
from .query.extra import take_relation
from .config import ConfigDict

from .manager import ODMManager

if IS_PYDANTIC_V2:
    import pydantic.main as pydantic_main
    from pydantic import model_validator  # type: ignore
    from .utils.typing import resolve_annotations

    PydanticModelMetaclass = pydantic_main._model_construction.ModelMetaclass  # type: ignore
else:
    from pydantic.main import (
        ModelMetaclass as PydanticModelMetaclass,
    )
    from pydantic.typing import resolve_annotations
    from pydantic import root_validator  # type: ignore


__all__ = ("Document", "DocumentMetaclass", "DocType")

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from .custom_typing import DictStrAny, AbstractSetIntStr, SetStr
    from .query import Builder
    from .sync.query import SyncQueryBuilder


_is_document_class_defined = False
DocType = TypeVar("DocType", bound="Document")


class DocumentMetaclass(PydanticModelMetaclass):  # type: ignore
    def __new__(mcs, name, bases, namespace, **kwargs):  # type: ignore
        annotations = resolve_annotations(
            namespace.get("__annotations__", {}), namespace.get("__module__")
        )
        mapping_query_fields = {"_id": "_id"}
        for field_name in annotations:
            mapping_query_fields[field_name] = field_name
        namespace["__mapping_query_fields__"] = mapping_query_fields
        namespace["__mapping_from_fields__"] = {
            db_field: cls_field for cls_field, db_field in mapping_query_fields.items()
        }
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        indexes = set()
        if _is_document_class_defined and issubclass(cls, Document):
            db_refs = {}
            for k, v in cls.model_fields.items():
                relation_info = take_relation(k, v)
                if relation_info is not None:
                    db_refs[k] = relation_info
            setattr(cls, "__db_refs__", db_refs)
            if db_refs:
                setattr(cls, "has_relations", True)
                setattr(cls, "__relation_manager__", RelationManager(cls))  # type: ignore
        if IS_PYDANTIC_V2:
            cls_config = getattr(cls, "model_config")
        else:
            cls_config = getattr(cls, "Config")
        json_encoders = getattr(cls_config, "json_encoders", {})  # type: ignore
        json_encoders.update({ObjectId: lambda f: str(f)})
        if IS_PYDANTIC_V2:
            cls_config["json_encoders"] = json_encoders  # type: ignore
            exclude_fields = cls_config.get("exclude_fields", tuple())  # type: ignore
            collection_name = (
                cls_config.get("collection_name", None) or cls.__name__.lower()
            )
        else:
            setattr(cls_config, "json_encoders", json_encoders)  # type: ignore
            exclude_fields = getattr(cls_config, "exclude_fields", tuple())  # type: ignore
            collection_name = (
                getattr(cls_config, "collection_name", None) or cls.__name__.lower()
            )
        setattr(cls, "__collection_name__", collection_name)
        setattr(cls, "__indexes__", indexes)
        setattr(cls, "__database_exclude_fields__", exclude_fields)
        setattr(cls, "__manager__", ODMManager(cls))  # type: ignore
        return cls


class Document(BasePydanticModel, metaclass=DocumentMetaclass):
    __indexes__: "SetStr" = set()
    __manager__: ODMManager
    __database_exclude_fields__: Union[Tuple, List] = tuple()
    __db_refs__: ClassVar[Optional[Dict[str, RelationInfo]]] = None
    __relation_manager__: Optional[RelationManager] = None
    __motordantic_computed_fields__: Dict[str, dict] = {}
    __mapping_query_fields__: Dict[str, str] = {}
    __mapping_from_fields__: Dict[str, str] = {}
    __collection_name__: Optional[str] = None
    _id: Optional[ObjectIdStr] = None
    has_relations: ClassVar[bool] = False
    model_config: ClassVar[ConfigDict]

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            raise MotordanticValidationError(e.errors(), e)

    def __setattr__(self, key, value):
        if key == "_id":
            self.__dict__[key] = value
            return value
        else:
            return super().__setattr__(key, value)

    @property
    def _io_loop(self) -> "AbstractEventLoop":
        return self.manager._io_loop

    @classmethod
    async def ensure_indexes(cls):
        """method for create/update/delete indexes if indexes declared in Config property"""
        if IS_PYDANTIC_V2:
            indexes = cls.model_config.get("indexes", [])
        else:
            indexes = getattr(cls.__config__, "indexes", [])
        if not all([isinstance(index, IndexModel) for index in indexes]):
            raise ValueError("indexes must be list of IndexModel instances")
        if indexes:
            db_indexes = await cls.Q().list_indexes()
            indexes_to_create = [
                i for i in indexes if i.document["name"] not in db_indexes
            ]
            indexes_to_delete = [
                i
                for i in db_indexes
                if i not in [i.document["name"] for i in indexes] and i != "_id_"
            ]
            result = []
            if indexes_to_create:
                try:
                    result = await cls.Q().create_indexes(indexes_to_create)
                except MotordanticConnectionError:
                    pass
            if indexes_to_delete:
                for index_name in indexes_to_delete:
                    await cls.Q().drop_index(index_name)
                db_indexes = await cls.Q().list_indexes()
            indexes = set(list(db_indexes.keys()) + result)
        setattr(cls, "__indexes__", indexes)

    @classmethod
    def _get_properties(cls) -> list:
        return [
            prop
            for prop in dir(cls)
            if prop
            not in (
                "__values__",
                "data",
                "querybuilder",
                "Q",
                "QSync",
                "pk",
                "_query_data",
                "_mongo_query_data",
                "__fields_set__",
                "model_fields_set",
                "model_extra",
                "fields_all",
                "_io_loop",
            )
            and isinstance(getattr(cls, prop), property)
        ]

    if IS_PYDANTIC_V2:

        @classproperty
        def __fields_set__(cls):
            return cls.__pydantic_fields_set__

    @classmethod
    def parse_obj(cls, data: Any) -> Any:
        if IS_PYDANTIC_V2:
            obj = super().model_validate(data)  # type: ignore
        else:
            obj = super().parse_obj(data)
        if "_id" in data:
            obj._id = data["_id"].__str__()
        return obj

    @classmethod
    def model_validate(cls, data: Any) -> Any:
        return cls.parse_obj(data)

    async def save(
        self,
        updated_fields: Union[Tuple, List] = [],
        session: Optional[AgnosticClientSession] = None,
    ) -> Any:
        if self._id is not None:
            data = {
                "_id": (
                    self._id if isinstance(self._id, ObjectId) else ObjectId(self._id)
                )
            }
            if updated_fields:
                if not all(
                    field in self.model_fields for field in updated_fields
                ) or any(
                    field in self.__motordantic_computed_fields__
                    for field in updated_fields
                ):
                    raise MotordanticValidationError("invalid field in updated_fields")
            else:
                updated_fields = tuple(self.model_fields.keys())
            for field in updated_fields:
                if field in self.__motordantic_computed_fields__:
                    continue
                else:
                    data[f"{field}__set"] = getattr(self, field)
            await self.Q().update_one(
                session=session,
                **data,  # type: ignore
            )
            return self
        data = {
            field: value
            for field, value in self.__dict__.items()
            if field in self.model_fields
        }
        object_id = await self.Q().insert_one(
            session=session,
            **data,
        )
        self._id = object_id  # type: ignore
        return self

    def save_sync(
        self,
        updated_fields: Union[Tuple, List] = [],
        session: Optional[AgnosticClientSession] = None,
    ):
        return self._io_loop.run_until_complete(self.save(updated_fields, session))

    async def delete(self) -> None:
        await self.Q().delete_one(_id=self.pk)

    def delete_sync(self) -> None:
        return self._io_loop.run_until_complete(self.delete())

    @classproperty
    def fields_all(cls) -> list:
        """return all fields with properties(not document fields)"""
        fields = list(cls.model_fields.keys())
        return_fields = fields + cls._get_properties()
        return return_fields

    @classproperty
    def manager(cls) -> ODMManager:
        return cls.__manager__

    @classmethod
    def Q(cls) -> "Builder":
        return cls.manager.querybuilder()

    if not IS_PYDANTIC_V2:

        @classproperty
        def model_fields(cls):
            return cls.__fields__

    @classmethod
    def QSync(cls) -> "SyncQueryBuilder":
        return cls.manager.sync_querybuilder()

    def model_dump(  # type: ignore
        self,
        *,
        include: Optional["AbstractSetIntStr"] = None,
        exclude: Optional["AbstractSetIntStr"] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        with_props: bool = True,
    ) -> "DictStrAny":
        """
        Generate a dictionary representation of the model, optionally specifying which fields to include or exclude.

        """
        if IS_PYDANTIC_V2:
            model_dump_func = super().model_dump  # type: ignore
        else:
            model_dump_func = super().dict
        attribs = model_dump_func(
            include=include,  # type: ignore
            exclude=exclude,  # type: ignore
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )
        if with_props:
            props = self._get_properties()
            # Include and exclude properties
            if include:
                props = [prop for prop in props if prop in include]
            if exclude:
                props = [prop for prop in props if prop not in exclude]

            # Update the attribute dict with the properties
            if props:
                attribs.update({prop: getattr(self, prop) for prop in props})
        if self.has_relations:
            for field in self.__db_refs__:  # type: ignore
                attrib_data = attribs[field]
                if attrib_data and not isinstance(attrib_data, dict):
                    attribs[field] = (
                        attrib_data.to_dict()
                        if not isinstance(attrib_data, list)
                        else [
                            a.to_dict() if not isinstance(a, dict) else a
                            for a in attrib_data
                        ]
                    )
        if self._id is not None and not include and not exclude:
            attribs["_id"] = self._id
        return attribs

    @classmethod
    def from_bson(cls, bson_raw_data: RawBSONDocument) -> "Document":
        data = bson_decode(bson_raw_data.raw)
        data = {
            cls.__mapping_from_fields__[field]: value for field, value in data.items()
        }
        obj = cls(**data)
        obj._id = data.get("_id")
        return obj

    @classmethod
    def to_db_ref(cls, object_id: Union[str, ObjectId]) -> DBRef:
        if isinstance(object_id, str):
            object_id = ObjectId(object_id)
        db_ref = DBRef(collection=cls.get_collection_name(), id=object_id)
        return db_ref

    @classmethod
    def to_relation(cls, object_id: Union[str, ObjectId]) -> Relation:
        db_ref = cls.to_db_ref(object_id=object_id)
        return Relation(db_ref, cls)

    @property
    def data(self) -> "DictStrAny":
        return self.model_dump(with_props=True)

    @property
    def _query_data(self) -> "DictStrAny":
        return self.model_dump(with_props=False)

    @property
    def _mongo_query_data(self) -> "DictStrAny":
        return self._query_data

    @classmethod
    def get_collection_name(cls) -> str:
        """main method for set collection

        Returns:
            str: collection name
        """
        return cls.__collection_name__ or cls.__name__.lower()

    def serialize(self, fields: Union[Tuple, List]) -> "DictStrAny":
        data: dict = self.model_dump(include=set(fields))
        return {f: data[f] for f in fields}

    def serialize_json(self, fields: Union[Tuple, List]) -> str:
        return json.dumps(self.serialize(fields))

    @property
    def pk(self):
        return self._id

    if IS_PYDANTIC_V2:

        @model_validator(mode="before")
        def validate_all_fields(cls, values):  # type: ignore
            for field, value in values.items():
                if isinstance(value, Document) and field not in cls.__db_refs__:
                    raise ValueError(
                        f"{field} - cant be instance of Document without Relation"
                    )
            return values

    else:

        @root_validator  # type: ignore
        def validate_all_fields(cls, values):
            for field, value in values.items():
                if isinstance(value, Document) and field not in cls.__db_refs__:
                    raise ValueError(
                        f"{field} - cant be instance of Document without Relation"
                    )
            return values


_is_document_class_defined = True
