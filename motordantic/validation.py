from typing import Any, Union, Optional, Tuple, TYPE_CHECKING

from bson import ObjectId
from bson.errors import InvalidId
from pydantic import BaseModel, ValidationError

from .utils.pydantic import IS_PYDANTIC_V2, get_model_fields
from .types import ObjectIdStr, UUID
from .exceptions import MotordanticValidationError

if not IS_PYDANTIC_V2:
    from pydantic.error_wrappers import ErrorWrapper


__all__ = ("validate_field_value", "sort_validation")

if TYPE_CHECKING:
    from .document import Document
    from .custom_typing import DocumentType


def call_validate(
    document: Union["Document", "DocumentType"], field_name: str, value: Any
) -> Any:
    if IS_PYDANTIC_V2:
        if field_name == "_id":
            field = ObjectIdStr()  # type: ignore
            try:
                value = field.validate(value, None)  # type: ignore
                return value, None
            except ValueError as e:
                return None, e
        try:
            m = document.__pydantic_validator__.validate_assignment(  # type: ignore
                document.model_construct(), field_name, value  # type: ignore
            )
            v = getattr(m, field_name)
            return v, None
        except ValidationError as e:
            return None, e.errors()
    else:
        model_fields = get_model_fields(document)
        field = model_fields.get(field_name)
        if field_name == "_id":
            field = ObjectIdStr()  # type: ignore
        else:
            field = model_fields.get(field_name)  # type: ignore
        error_ = None
        if isinstance(field, ObjectIdStr):
            try:
                value = field.validate(value)  # type: ignore
            except ValueError as e:
                error_ = ErrorWrapper(e, str(e))
        elif not field:
            raise AttributeError(f"invalid field - {field_name}")
        else:
            value, error_ = field.validate(value, {}, loc=field.alias, cls=document)  # type: ignore
        return value, error_


def validate_field_value(
    document: Union["Document", "DocumentType"], field_name: str, value: Any
) -> Any:
    """extra helper value validation

    Args:
        cls ('Document'): mongo document class
        field_name (str): name of field
        value (Any): value

    Raises:
        AttributeError: if not field in __fields__
        MongoValidationError: if invalid value type

    Returns:
        Any: value
    """
    value, error_ = call_validate(document=document, field_name=field_name, value=value)
    if error_:
        if IS_PYDANTIC_V2:
            raise MotordanticValidationError(str(error_))
        else:
            pydantic_validation_error = ValidationError([error_], document)  # type: ignore
            raise MotordanticValidationError(
                pydantic_validation_error.errors(), pydantic_validation_error
            )
    if field_name in document.__db_refs__:  # type: ignore
        if isinstance(value, list):
            s = [v.to_ref() for v in value]
            return s
        return value.to_ref() if value else None
    elif isinstance(value, UUID):
        return value.hex
    else:
        if IS_PYDANTIC_V2:
            return value.model_dump() if isinstance(value, BaseModel) else value  # type: ignore
        else:
            return value.dict() if isinstance(value, BaseModel) else value  # type: ignore


def sort_validation(
    sort: Optional[int] = None, sort_fields: Union[list, tuple, None] = None
) -> Tuple[Any, ...]:
    if sort is not None:
        if sort not in (1, -1):
            raise ValueError(f"invalid sort value must be 1 or -1 not {sort}")
        if not sort_fields:
            sort_fields = ("_id",)
    return sort, sort_fields


def validate_object_id(document: "Document", value: str) -> ObjectId:
    try:
        o_id = ObjectId(value)
    except InvalidId as e:
        if IS_PYDANTIC_V2:
            raise MotordanticValidationError(str(e))
        else:
            error_ = ErrorWrapper(e, str(e))
            pydantic_validation_error = ValidationError([error_], document)  # type: ignore
            raise MotordanticValidationError(
                pydantic_validation_error.errors(), pydantic_validation_error
            )
    return o_id
