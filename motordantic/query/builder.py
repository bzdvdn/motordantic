from typing import (
    AsyncIterable,
    AsyncGenerator,
    Union,
    List,
    Dict,
    Optional,
    Any,
    Tuple,
    TYPE_CHECKING,
    Iterable,
)

from bson import ObjectId, decode as bson_decode
from bson.raw_bson import RawBSONDocument
from pymongo import ReturnDocument, IndexModel
from pymongo.collection import WriteConcern
from motor.core import AgnosticClientSession as ClientSession

from .query import generate_basic_query, Q, QCombination
from .result import FindResult, SimpleAggregateResult
from .extra import group_by_aggregate_generation, generate_name_field

from ..aggregate.expressions import Sum, Max, Min, Avg
from ..exceptions import (
    MotordanticInvalidArgsParams,
    MotordanticValidationError,
    MotordanticIndexError,
    DoesNotExist,
)
from ..validation import sort_validation


__all__ = ("Builder",)

if TYPE_CHECKING:
    from ..manager import ODMManager
    from ..custom_typing import DictStrAny
    from ..document import Document


class Builder(object):
    __slots__ = ("odm_manager", "_collection")

    def __init__(self, odm_manager: "ODMManager", query_collectcion_name: str):
        self.odm_manager: "ODMManager" = odm_manager
        self._collection = self.odm_manager.get_collection(query_collectcion_name)

    def _validate_query_data(self, query: Dict) -> "DictStrAny":
        """main validation method

        Args:
            query (Dict): basic query

        Returns:
            Dict: parsed query
        """
        query_params: dict = generate_basic_query(self.odm_manager, query)

        return query_params

    def _check_query_args(
        self,
        logical_query: Union[
            List[Any], Dict[Any, Any], str, Q, QCombination, None
        ] = None,
    ) -> "DictStrAny":
        """check if query = Query obj or QueryCombination

        Args:
            logical_query (Union[ List[Any], Dict[Any, Any], str, Q, QCombination ], optional): Query | QueryCombination. Defaults to None.

        Raises:
            InvalidArgsParams: if not Query | QueryCombination

        Returns:
            Dict: generated query dict
        """
        if not isinstance(logical_query, (Q, QCombination)):
            raise MotordanticInvalidArgsParams()
        return logical_query.to_query(self)  # type: ignore

    async def _make_query(
        self,
        method_name: str,
        query_params: Union[List, Dict, str, Q, QCombination],
        set_values: Optional[Dict] = None,
        session: Optional[ClientSession] = None,
        logical: bool = False,
        write_concern: Optional[WriteConcern] = None,
        **kwargs,
    ) -> Any:
        """main query function

        Args:
            method_name (str): query method like find, find_one and other
            query_params (Union[List, Dict, str, Query, LogicalCombination]): query params: dict or Query or LogicalCombination
            set_values (Optional[Dict], optional): for updated method. Defaults to None.
            session (Optional[ClientSession], optional): motor session. Defaults to None.
            logical (bool, optional): if logical. Defaults to False.

        Returns:
            Any: query result
        """
        if logical:
            query_params = self._check_query_args(query_params)
        elif isinstance(query_params, dict):
            query_params = self._validate_query_data(query_params)
        if write_concern:
            collection = self._collection.with_options(  # type: ignore
                write_concern=write_concern
            )
        else:
            collection = self._collection
        # print(query_params)
        method = getattr(collection, method_name)
        query: tuple = (query_params,)
        if session:
            kwargs["session"] = session
        if set_values:
            query = (query_params, set_values)
        if kwargs:
            return await method(*query, **kwargs)
        return await method(*query)

    async def get(self, session: Optional[ClientSession] = None, **query) -> "Document":
        obj = await self.find_one(session=session, **query)
        if not obj:
            raise DoesNotExist(self.odm_manager.document.__name__)  # type: ignore
        return obj

    async def count(
        self,
        logical_query: Union[Q, QCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        """count query

        Args:
            logical_query (Union[Q, QCombination, None], optional): Query | QueryCombination. Defaults to None.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: count of documents
        """
        if getattr(self._collection, "count_documents"):
            return await self._make_query(
                "count_documents",
                logical_query or query,
                session=session,
                logical=bool(logical_query),
            )
        return await self._make_query(
            "count",
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )

    async def count_documents(
        self,
        logical_query: Union[Q, QCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        """count_documents query

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query | QueryCombination. Defaults to None.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: count of documents
        """
        return await self.count(logical_query, session, **query)

    async def insert_one(
        self, session: Optional[ClientSession] = None, **query
    ) -> ObjectId:
        """insert one document

        Args:
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            ObjectId: created document _id
        """
        obj = self.odm_manager.document.parse_obj(query)
        data = await self._make_query(
            "insert_one",
            obj._mongo_query_data,
            session=session,
        )
        return data.inserted_id

    async def insert_many(
        self,
        data: List,
        session: Optional[ClientSession] = None,
        ordered: bool = True,
        bypass_document_validation: bool = False,
        write_concern: Optional[WriteConcern] = None,
    ) -> int:
        """insert many documents

        Args:
            data (List): List of dict or Documents
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: count inserted ids
        """
        parse_obj = self.odm_manager.document.parse_obj
        query = [
            (
                parse_obj(obj)._mongo_query_data
                if isinstance(obj, dict)
                else obj._mongo_query_data
            )
            for obj in data
        ]
        r = await self._make_query(
            "insert_many",
            query,
            session=session,
            ordered=ordered,
            bypass_document_validation=bypass_document_validation,
            write_concern=write_concern,
        )
        return len(r.inserted_ids)

    async def delete_one(
        self,
        logical_query: Union[Q, QCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        """delete one document

        Args:
            logical_query (Union[Q, QCombination, None], optional): Query|QueryCombination. Defaults to None.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: deleted documents count
        """
        r = await self._make_query(
            "delete_one",
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )
        return r.deleted_count

    async def delete_many(
        self,
        logical_query: Union[Q, QCombination, None] = None,
        session: Optional[ClientSession] = None,
        **query,
    ) -> int:
        """delete many document

        Args:
            logical_query (Union[Q, QCombination, None], optional): Query|QueryCombination. Defaults to None.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: deleted documents count
        """
        r = await self._make_query(
            "delete_many",
            logical_query or query,
            session=session,
            logical=bool(logical_query),
        )
        return r.deleted_count

    async def distinct(
        self, field: str, session: Optional[ClientSession] = None, **query
    ) -> list:
        """wrapper for pymongo distinct

        Args:
            field (str): distinct field
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            list: list of distinct values
        """
        query = self._validate_query_data(query)
        method = getattr(self._collection, "distinct")
        return await method(key=field, filter=query, session=session)

    async def find_one(
        self,
        logical_query: Union[Q, QCombination, None] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        session: Optional[ClientSession] = None,
        sort: Optional[int] = None,
        with_relations_objects: bool = False,
        **query,
    ) -> Optional["Document"]:
        """find one document

        Args:
            logical_query (Union[Q, QCombination, None], optional): Query | LogicalCombination. Defaults to None.
            session (Optional[ClientSession], optional): motor session. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): iterable from sort fielda. Defaults to None.
            sort (Optional[int], optional): sort value -1 or 1. Defaults to None.

        Returns:
            Optional[Document]: Document instance or None
        """
        sort, sort_fields = sort_validation(sort, sort_fields)
        data = await self._make_query(
            "find_one",
            logical_query or query,
            logical=bool(logical_query),
            sort=[(field, sort or 1) for field in sort_fields] if sort_fields else None,
            session=session,
        )
        if data is not None:
            obj = self.odm_manager.document.from_bson(data)
            if with_relations_objects and self.odm_manager.relation_manager:
                obj = await self.odm_manager.relation_manager.map_relation_for_single(
                    obj
                )
            return obj
        return None

    async def _find(
        self,
        logical_query: Union[Q, QCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        batch_size: Optional[int] = None,
        hint: Optional[str] = None,
        allow_disk_use: bool = False,
        **query,
    ) -> AsyncGenerator:
        sort, sort_fields_parsed = sort_validation(sort, sort_fields)

        async def context():
            if bool(logical_query):
                query_params = self._check_query_args(logical_query)
            else:
                query_params = self._validate_query_data(query)
            find_cursor_method = getattr(self._collection, "find")
            if batch_size is not None:
                cursor = find_cursor_method(
                    query_params,
                    session=session,
                    batch_size=batch_size,
                    hint=hint,
                )
            else:
                cursor = find_cursor_method(query_params, session=session, hint=hint)
            if skip_rows is not None:
                cursor = cursor.skip(skip_rows)
            if limit_rows:
                cursor = cursor.limit(limit_rows)
            if sort:
                cursor.sort([(field, sort or 1) for field in sort_fields_parsed])
            if allow_disk_use:
                cursor.allow_disk_use(True)
            async for doc in cursor:
                yield self.odm_manager.document.from_bson(doc)

        return context()

    async def find(
        self,
        logical_query: Union[Q, QCombination, None] = None,
        skip_rows: Optional[int] = None,
        limit_rows: Optional[int] = None,
        session: Optional[ClientSession] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        with_relations_objects: bool = False,
        batch_size: Optional[int] = None,
        hint: Optional[str] = None,
        allow_disk_use: bool = False,
        **query,
    ) -> FindResult:
        """find method

        Args:
            logical_query (Union[Query, LogicalCombination, None], optional): Query|LogicalCombunation. Defaults to None.
            skip_rows (Optional[int], optional): skip rows for pagination. Defaults to None.
            limit_rows (Optional[int], optional): limit rows. Defaults to None.
            session (Optional[ClientSession], optional): pymongo session. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): iterable from sort fielda. Defaults to None.
            sort (Optional[int], optional): sort value -1 or 1. Defaults to None.

        Returns:
            FindResult: Motordantic FindResult
        """
        result = await self._find(
            logical_query,
            skip_rows,
            limit_rows,
            session,
            sort_fields,
            sort,
            batch_size=batch_size,
            hint=hint,
            allow_disk_use=allow_disk_use,
            **query,
        )
        data = [doc async for doc in result]
        if with_relations_objects and self.odm_manager.relation_manager:
            data = await self.odm_manager.relation_manager.map_relation_for_array(data)
        return FindResult(self.odm_manager.document, data)

    def _prepare_update_data(self, **fields) -> tuple:
        """prepare and validate query data for update queries"""

        if not any("__set" in f for f in fields):
            raise MotordanticValidationError("not fields for updating!")
        query_params = {}
        set_values = {}
        for name, value in fields.items():
            if name.endswith("__set"):
                name = name.replace("__set", "")
                data = self._validate_query_data({name: value})
                set_values.update(data)
            else:
                query_params.update({name: value})
        return query_params, set_values

    async def _update(
        self,
        method: str,
        query: Dict,
        upsert: bool = True,
        session: Optional[ClientSession] = None,
    ) -> int:
        """innert method for update

        Args:
            method (str): one of update_many or update_one
            query (Dict): update query
            upsert (bool, optional): upsert option. Defaults to True.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: updated documents count
        """
        query, set_values = self._prepare_update_data(**query)
        r = await self._make_query(
            method, query, {"$set": set_values}, upsert=upsert, session=session
        )
        return r.modified_count

    async def update_one(
        self, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        """update one document

        Args:
            upsert (bool, optional): pymongo upsert. Defaults to False.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: updated documents count
        """
        return await self._update("update_one", query, upsert=upsert, session=session)

    async def update_many(
        self, upsert: bool = False, session: Optional[ClientSession] = None, **query
    ) -> int:
        """update many document

        Args:
            upsert (bool, optional): pymongo upsert. Defaults to False.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            int: updated documents count
        """
        return await self._update("update_many", query, upsert=upsert, session=session)

    async def _find_with_replacement_or_with_update(
        self,
        operation: str,
        projection_fields: Optional[list] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Union[Dict, "Document", None]:
        """base method for find_with_<operation>

        Args:
            operation (str): operation name
            projection_fields (Optional[list], optional): prejection. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): sort fields. Defaults to None.
            sort (Optional[int], optional): -1 or 1. Defaults to None.
            upsert (bool, optional): True/False. Defaults to False.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            Union[Dict, 'Document']: Document or Dict or None
        """
        filter_, set_values = self._prepare_update_data(**query)
        return_document = ReturnDocument.AFTER
        replacement = query.pop("replacement", None)

        projection = {f: True for f in projection_fields} if projection_fields else None
        extra_params = {
            "return_document": return_document,
            "projection": projection,
            "upsert": upsert,
            "session": session,
        }
        if sort_fields:
            extra_params["sort"] = [(field, sort or 1) for field in sort_fields]

        if replacement:
            extra_params["replacement"] = replacement

        data = await self._make_query(
            operation, filter_, set_values={"$set": set_values}, **extra_params
        )
        if projection:
            return {
                field: value for field, value in data.items() if field in projection
            }
        if data:
            return self.odm_manager.document.from_bson(data)
        return None

    async def find_one_and_update(
        self,
        projection_fields: Optional[list] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Union[Dict, "Document", None]:
        """find one and update

        Args:
            operation (str): operation name
            projection_fields (Optional[list], optional): prejection. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): sort fields. Defaults to None.
            sort (Optional[int], optional): -1 or 1. Defaults to None.
            upsert (bool, optional): True/False. Defaults to False.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            Union[Dict, 'Document']: Document or Dict or None
        """
        return await self._find_with_replacement_or_with_update(
            "find_one_and_update",
            projection_fields=projection_fields,
            sort_fields=(
                [(field, sort or 1) for field in sort_fields] if sort_fields else None
            ),
            sort=sort,
            upsert=upsert,
            session=session,
            **query,
        )

    async def find_and_replace(
        self,
        replacement: Union[dict, Any],
        projection_fields: Optional[list] = None,
        sort_fields: Optional[Union[Tuple, List]] = None,
        sort: Optional[int] = None,
        upsert: bool = False,
        session: Optional[ClientSession] = None,
        **query,
    ) -> Union[Dict, "Document", None]:
        """find one and replace

        Args:
            operation (str): operation name
            projection_fields (Optional[list], optional): prejection. Defaults to None.
            sort_fields (Optional[Union[Tuple, List]], optional): sort fields. Defaults to None.
            sort (Optional[int], optional): -1 or 1. Defaults to None.
            upsert (bool, optional): True/False. Defaults to False.
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            Union[Dict, 'Document']: Document or Dict or None
        """
        if not isinstance(replacement, dict):
            replacement = replacement.query_data
        return await self._find_with_replacement_or_with_update(
            "find_and_replace",
            projection_fields=projection_fields,
            sort_fields=(
                [(field, sort) for field in sort_fields] if sort_fields else None
            ),
            sort=sort,
            upsert=upsert,
            session=session,
            replacement=replacement,
            **query,
        )

    async def _motor_aggreggate_call(
        self, data: list, session: Optional[ClientSession], allow_disk_use: bool = False
    ) -> AsyncIterable:
        async def context():
            aggregate_cursor = getattr(self._collection, "aggregate")

            async for row in aggregate_cursor(
                data, session=session, allowDiskUse=allow_disk_use
            ):
                yield row

        return context()

    def from_bson(self, row: RawBSONDocument) -> dict:
        return bson_decode(row.raw)

    async def raw_aggregate(
        self,
        data: List[Dict[Any, Any]],
        session: Optional[ClientSession] = None,
        allow_disk_use: bool = False,
    ) -> list:
        """raw aggregation query

        Args:
            data (List[Dict[Any, Any]]): aggregation query
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Returns:
            list: aggregation result
        """
        result = await self._motor_aggreggate_call(data, session, allow_disk_use)
        return [self.from_bson(row) async for row in result]

    async def _aggregate(self, *args, **query) -> SimpleAggregateResult:
        """main aggregate method

        Raises:
            MongoValidationError: miss aggregation or group_by

        Returns:
            dict: aggregation result
        """
        session = query.pop("session", None)
        aggregation = query.pop("aggregation", None)
        group_by = query.pop("group_by", None)
        if not aggregation and not group_by:
            raise MotordanticIndexError("miss aggregation or group_by")
        if isinstance(aggregation, Iterable):
            aggregate_query = {}
            for agg in aggregation:
                aggregate_query.update(agg._aggregate_query(self.odm_manager.document))
        elif aggregation is not None:
            aggregate_query = aggregation._aggregate_query(self.odm_manager.document)
        else:
            aggregate_query = {}
        if group_by:
            group_by = group_by_aggregate_generation(group_by)
            aggregate_query.pop("_id", None)
            group_params = {"$group": {"_id": group_by, **aggregate_query}}
        else:
            group_params = {
                "$group": (
                    {"_id": None, **aggregate_query}
                    if "_id" not in aggregate_query
                    else aggregate_query
                )
            }
        data = [
            {
                "$match": (
                    self._validate_query_data(query)
                    if not args
                    else self._check_query_args(*args)
                )
            },
            group_params,
        ]

        async_result = await self._motor_aggreggate_call(data, session)
        result = [self.from_bson(row) async for row in async_result]
        if not result:
            return SimpleAggregateResult(self.odm_manager.document, {})
        result_data = {}
        for r in result:
            name = generate_name_field(r.pop("_id"))
            result_data.update({name: r} if name else r)
        return SimpleAggregateResult(self.odm_manager.document, result_data)

    async def simple_aggregate(self, *args, **kwargs) -> SimpleAggregateResult:
        return await self._aggregate(*args, **kwargs)

    async def aggregate_sum(self, agg_field: str, **query) -> Union[int, float]:
        result_field = self.odm_manager.document.__mapping_query_fields__[agg_field]
        result = await self._aggregate(aggregation=Sum(agg_field), **query)
        return result.data.get(f"{result_field}__sum", 0)

    async def aggregate_max(self, agg_field: str, **query) -> Union[int, float]:
        result = await self._aggregate(aggregation=Max(agg_field), **query)
        result_field = self.odm_manager.document.__mapping_query_fields__[agg_field]
        return result.data.get(f"{result_field}__max", 0)

    async def aggregate_min(self, agg_field: str, **query) -> Union[int, float]:
        result = await self._aggregate(aggregation=Min(agg_field), **query)
        result_field = self.odm_manager.document.__mapping_query_fields__[agg_field]
        return result.data.get(f"{result_field}__min", 0)

    async def aggregate_avg(self, agg_field: str, **query) -> Union[int, float]:
        result = await self._aggregate(aggregation=Avg(agg_field), **query)
        result_field = self.odm_manager.document.__mapping_query_fields__[agg_field]
        return result.data.get(f"{result_field}__avg", 0)

    def _validate_raw_query(
        self, method_name: str, raw_query: Union[Dict, List[Dict], Tuple[Dict]]
    ) -> tuple:
        if (
            "insert" in method_name
            or "replace" in method_name
            or "update" in method_name
        ):
            if isinstance(raw_query, list):
                raw_query = list(map(self._validate_query_data, raw_query))
            elif isinstance(raw_query, dict):
                raw_query = self._validate_query_data(raw_query)
            else:
                params = [
                    query[key] if "$" in key else query
                    for query in raw_query
                    for key in query.keys()
                ]
                map(self._validate_query_data, params)
        parsed_query = raw_query if isinstance(raw_query, tuple) else (raw_query,)
        return parsed_query

    async def raw_query(
        self,
        method_name: str,
        raw_query: Union[Dict[Any, Any], List[Dict[Any, Any]], Tuple[Dict[Any, Any]]],
        session: Optional[ClientSession] = None,
    ) -> Any:
        """pymongo raw query

        Args:
            method_name (str): pymongo method, like insert_one
            raw_query (Union[Dict, List[Dict], Tuple[Dict]]): query data
            session (Optional[ClientSession], optional): motor session. Defaults to None.

        Raises:
            MongoValidationError: raise if invalid data

        Returns:
            Any: pymongo query result
        """
        parsed_query = self._validate_raw_query(method_name, raw_query)
        try:
            query = getattr(self._collection, method_name)
            return await query(*parsed_query, session=session)
        except AttributeError:
            raise MotordanticValidationError("invalid method name")

    async def list_indexes(self, session: Optional[ClientSession] = None) -> dict:
        """get indexes for this collection

        Returns:
            dict: indexes result
        """
        list_indexes_cursor = getattr(self._collection, "list_indexes")
        query = list_indexes_cursor(session=session)
        index_list = await query.to_list(None)
        return_data = {}
        for index in index_list:
            dict_index = dict(index)
            data = {dict_index["name"]: {"key": dict(dict_index["key"])}}
            return_data.update(data)
        return return_data

    async def create_indexes(
        self,
        indexes: List[IndexModel],
        session: Optional[ClientSession] = None,
    ) -> List[str]:
        create_indexes_cursor = getattr(self._collection, "create_index")
        result = []
        for index in indexes:
            index_value = list(index.document["key"].items())
            res = await create_indexes_cursor(
                index_value,
                session=session,
                background=True,
            )
            result.append(res)
        return result

    async def drop_index(
        self, index_name: str, session: Optional[ClientSession] = None
    ) -> str:
        indexes = await self.list_indexes(session)
        if index_name in indexes:
            await self._make_query("drop_index", index_name, session=session)
            return f"{index_name} dropped."
        raise MotordanticIndexError(f"invalid index name - {index_name}")

    async def drop_collection(self, force: bool = False) -> bool:
        """drop collection

        Args:
            force (bool, optional): if u wanna force drop. Defaults to False.

        Returns:
            bool: result message
        """
        if force:
            await self._collection.drop()  # type: ignore
            return True
        value = input(
            f"Are u sure for drop this collection - {self.odm_manager.document.__name__.lower()} (y, n)"  # type: ignore
        )
        if value.lower() == "y":
            await self._collection.drop()  # type: ignore
            return True
        return False
