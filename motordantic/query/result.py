from json import dumps
from typing import Generator, List, Union, Any, Tuple, List, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..document import Document


class AggregateResult(object):
    __slots__ = ('_native_result', '_document_class')

    def __init__(self, native_result: list, document_class: 'Document'):
        self._native_result = native_result
        self._document_class = document_class

    def __iter__(self):
        for obj in self.native_result:
            yield obj

    def __next__(self):
        return next(self.__iter__())

    def __len__(self):
        return len(self.native_result)

    def __contains__(self, value):
        return value in self.native_result

    def __lt__(self, other):
        return self.native_result < self.__cast(other)

    def __le__(self, other):
        return self.native_result <= self.__cast(other)

    def __eq__(self, other):
        return self.native_result == self.__cast(other)

    def __gt__(self, other):
        return self.native_result > self.__cast(other)

    def __ge__(self, other):
        return self.native_result >= self.__cast(other)

    def __cast(self, other):
        return other.native_result if isinstance(other, AggregateResult) else other

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            return self.__class__(self.native_result[idx])  # type: ignore
        else:
            return self.native_result[idx]

    @property
    def native_result(self) -> list:
        return self._native_result

    @property
    def document_class(self) -> 'Document':
        return self._document_class


class SimpleAggregateResult(object):
    __slots__ = ('_data', 'document_class')

    def __init__(
        self,
        document_class: 'Document',
        data: dict,
    ):
        self._data = data
        self.document_class = document_class

    def json(self) -> str:
        return dumps(self._data)

    @property
    def data(self) -> dict:
        return self._data


class FindResult(object):
    __slots__ = ('_data', 'document_class')

    def __init__(
        self,
        document_class: 'Document',
        data: list,
    ):
        self._data = data
        self.document_class = document_class

    # @handle_and_convert_connection_errors
    def __iter__(self):
        for obj in self._data:
            yield obj

    def __next__(self):
        return next(self.__iter__())

    @property
    def data(self) -> List:
        return [obj.data for obj in self.__iter__()]

    @property
    def generator(self) -> Generator:
        return self.__iter__()

    @property
    def data_generator(self) -> Generator:
        for obj in self.__iter__():
            yield obj.data

    @property
    def list(self) -> List:
        return list(self.__iter__())

    def json(self) -> str:
        return dumps(self.data)

    def first(self) -> Any:
        return next(self.__iter__())

    def serialize(
        self, fields: Union[Tuple, List], to_list: bool = True
    ) -> Union[Tuple, List]:
        return (
            [obj.serialize(fields) for obj in self.__iter__()]
            if to_list
            else tuple(obj.serialize(fields) for obj in self.__iter__())
        )

    def serialize_generator(self, fields: Union[Tuple, List]) -> Generator:
        for obj in self.__iter__():
            yield obj.serialize(fields)

    def serialize_json(self, fields: Union[Tuple, List]) -> str:
        return dumps(self.serialize(fields))
