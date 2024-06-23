from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    List,
    Mapping,
    Set,
    Type,
    Union,
)

__all__ = (
    "DocumentType",
    "DictStrList",
    "DictStrAny",
    "DictAny",
    "SetStr",
    "ListStr",
    "IntStr",
    "AbstractSetIntStr",
    "DictIntStrAny",
    "ExcludeInclude",
    "MappingIntStrAny",
)

if TYPE_CHECKING:
    from .document import DocType

IntStr = Union[int, str]
DictIntStrAny = Dict[IntStr, Any]
MappingIntStrAny = Mapping[IntStr, Any]

DocumentType = Type["DocType"]
DictStrList = Dict[str, List]
DictStrAny = Dict[str, Any]
DictAny = Dict[Any, Any]
SetStr = Set[str]
ListStr = List[str]
AbstractSetIntStr = AbstractSet[IntStr]
ExcludeInclude = Union[AbstractSetIntStr, MappingIntStrAny, Any]
