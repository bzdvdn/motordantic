from typing import TypedDict, Union
from .utils.pydantic import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from pydantic import ConfigDict as BaseConfigDict

    class ConfigDict(BaseConfigDict):  # type: ignore
        indexes: list
        excluded_query_fields: Union[tuple, list]

else:

    class ConfigDict(TypedDict, total=False):  # type: ignore
        indexes: list
        excluded_query_fields: Union[tuple, list]
