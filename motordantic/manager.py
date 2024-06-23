from typing import Optional, TYPE_CHECKING, List
import asyncio

from motor.core import AgnosticClientSession, AgnosticCollection, AgnosticDatabase
from pymongo.errors import AutoReconnect, ServerSelectionTimeoutError, NetworkTimeout


from .query import ExtraQueryMapper
from .query.builder import Builder
from .exceptions import NotDeclaredField
from .relation import RelationManager

from .sync import SyncQueryBuilder
from .aggregate.aggregate import Aggregate
from .utils.pydantic import get_model_fields

if TYPE_CHECKING:
    from .document import Document
    from .connection import MotordanticConnection


class ODMManager(object):
    __database__: Optional[AgnosticDatabase] = None
    __collection__: Optional[AgnosticCollection] = None
    __connection__: Optional["MotordanticConnection"] = None
    __relation_manager__: Optional["RelationManager"] = None

    def __init__(self, document: "Document"):
        self._builder: Builder = Builder(self)
        self.querybuilder = self._builder
        self.sync_querybuilder: SyncQueryBuilder = SyncQueryBuilder(self._builder)
        self.__document__ = document
        if self.__document__.has_relations:
            self.__relation_manager__ = RelationManager(self.__document__)
        else:
            self.__relation_manager__ = None

    @property
    def database(self) -> AgnosticDatabase:
        """Returns the database that is currently associated with this document."""
        if not hasattr(self, "__database__") or self.__database__ is None:
            raise AttributeError("Accessing database without using it first.")
        return self.__database__

    @property
    def connection(self) -> "MotordanticConnection":
        assert self.__connection__ is not None
        return self.__connection__

    @property
    def motor_client(self):
        return self.connection._get_motor_client()

    @property
    def _io_loop(self) -> asyncio.AbstractEventLoop:
        try:
            _io_loop = (
                self.motor_client.io_loop
                if self.motor_client.io_loop is not None
                else asyncio.get_running_loop()
            )
        except RuntimeError:
            _io_loop = asyncio.new_event_loop()
        return _io_loop

    @property
    def collection(self) -> AgnosticCollection:
        """Returns the collection for this :class:`Document`."""
        if (
            self.__collection__ is None
            or self.__collection__.database is not self.database
        ):
            self.__collection__ = self.database.get_collection(self.document.get_collection_name())  # type: ignore
        return self.__collection__  # type: ignore

    async def _start_session(self) -> AgnosticClientSession:
        return await self.motor_client.start_session()

    @property
    def relation_manager(self) -> Optional[RelationManager]:
        return self.__relation_manager__

    @classmethod
    def use(cls, connection: "MotordanticConnection") -> None:
        assert connection is not None
        cls.__connection__ = connection
        cls.__database__ = connection._get_motor_client().get_database(
            connection.database_name
        )

    @property
    def document(self) -> "Document":
        assert self.__document__ is not None
        return self.__document__

    def aggregate(self) -> Aggregate:
        aggregate = Aggregate(self.__document__)
        return aggregate

    def _validate_field(self, field: str) -> bool:
        # if field in self.document.__mapping_from_fields__:
        #     return True
        model_fields = get_model_fields(self.document)
        if field not in model_fields and field != "_id":
            raise NotDeclaredField(field, list(model_fields))
        elif field in self.document.__database_exclude_fields__:
            return False
        return True

    def _parse_extra_params(self, extra_params: List) -> tuple:
        field_param, extra = [], []
        methods = ExtraQueryMapper.methods
        for param in extra_params:
            if param in methods:
                extra.append(param)
            else:
                field_param.append(param)
        return field_param, extra

    async def ensure_indexes(self):
        """method for create/update/delete indexes if indexes declared in Config property"""

        indexes = self.document.__indexes__
        if indexes:
            db_indexes = await self.querybuilder.list_indexes()
            indexes_to_create = [
                i for i in indexes if i.document["name"] not in db_indexes  # type: ignore
            ]
            indexes_to_delete = [
                i
                for i in db_indexes
                if i not in [i.document["name"] for i in indexes] and i != "_id_"  # type: ignore
            ]
            result = []
            if indexes_to_create:
                try:
                    result = await self.querybuilder.create_indexes(indexes_to_create)  # type: ignore
                except (AutoReconnect, ServerSelectionTimeoutError, NetworkTimeout):
                    pass
            if indexes_to_delete:
                for index_name in indexes_to_delete:
                    await self.querybuilder.drop_index(index_name)
                db_indexes = await self.querybuilder.list_indexes()
            indexes = set(list(db_indexes.keys()) + result)


class DynamicCollectionODMManager(ODMManager):
    pass
