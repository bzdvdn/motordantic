from typing import Type, Any, Dict, TYPE_CHECKING

from ..query.query import BaseQuery
from ..query.builder import Builder

if TYPE_CHECKING:
    from ..custom_typing import DictStrAny

__all__ = ("SyncQuery", "SyncQueryBuilder")


class SyncQuery(BaseQuery):
    def __getattr__(self, method_name: str) -> "SyncQuery":
        return SyncQuery(self._builder, method_name)

    def __call__(self, *args, **kwargs):
        method = getattr(self._builder, self.method_name)
        return self._builder.odm_manager._io_loop.run_until_complete(
            method(*args, **kwargs)
        )


class SyncQueryBuilder(object):
    query_class: Type[BaseQuery] = SyncQuery

    __slots__ = ("builder", "_valid_methods")

    def __init__(self, builder: "Builder"):
        self.builder = builder
        self._valid_methods = {
            attr_name: attr_name
            for attr_name in dir(self.builder)
            if attr_name != "odm_manager" and not attr_name.endswith("__")
        }

    def __getattr__(self, method_name: str) -> Any:
        if method_name in self._valid_methods:
            return self.query_class(self.builder, method_name)
        raise AttributeError(f"invalid Q attr query: {method_name}")

    def _validate_query_data(self, query: Dict) -> "DictStrAny":
        return self.builder._validate_query_data(query)

    def _check_query_args(self, *args, **kwargs):
        return self.builder._check_query_args(*args, **kwargs)

    def _validate_raw_query(self, *args, **kwargs):
        return self.builder._validate_raw_query(*args, **kwargs)

    def __call__(self, method_name, *args, **method_kwargs):
        return getattr(self, method_name)(*args, **method_kwargs)
