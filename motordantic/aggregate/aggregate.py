from typing import TYPE_CHECKING, Any, Optional, Dict, List, Union

from ..exceptions import MotordanticValidationError
from ..query import generate_basic_query, Q, QCombination, AggregateResult
from ..utils.pydantic import get_model_fields
from ..session import Session

__all__ = ("Aggregate",)

if TYPE_CHECKING:
    from ..document import Document, DynamicCollectionDocument


class Aggregate(object):
    __slots__ = (
        "document_class",
        "pipeline",
        "collection_name",
        "_querybuilder",
        "options",
    )

    def __init__(
        self,
        document_class: Union["Document", "DynamicCollectionDocument"],
        collection_name: str,
    ):
        self.document_class = document_class
        self.pipeline: List[Dict] = []
        self.options: Dict = {}
        self.collection_name = collection_name
        self._querybuilder = (
            self.document_class.Q(collection_name)  # type: ignore
            if self.document_class._is_dynamic
            else self.document_class.Q()  # type: ignore
        )

    def match(
        self, logical_query: Optional[Union[Q, QCombination]] = None, **query
    ) -> "Aggregate":
        if logical_query is not None:
            query_params = self._querybuilder._check_query_args(logical_query)  # type: ignore
        else:
            query_params = generate_basic_query(
                self.document_class.manager, query, False
            )
        self.pipeline.append({"$match": {**query_params}})
        return self

    def raw_match(self, native_match_query: dict) -> "Aggregate":
        self.pipeline.append({"$match": native_match_query})
        return self

    def add_fields(self, **fields) -> "Aggregate":
        self.pipeline.append({"$addFields": {**fields}})
        return self

    def group(self, _id: Any = None, **group_kwargs) -> "Aggregate":
        self.pipeline.append({"$group": {"_id": _id, **group_kwargs}})
        return self

    def project(self, project_args: dict) -> "Aggregate":
        self.pipeline.append({"$project": {**project_args}})
        return self

    def skip(self, skip: int) -> "Aggregate":
        self.pipeline.append({"$skip": skip})
        return self

    def limit(self, limit: int) -> "Aggregate":
        self.pipeline.append({"$limit": limit})
        return self

    def sort(self, **sort_kwargs) -> "Aggregate":
        self.pipeline.append({"$sort": {**sort_kwargs}})
        return self

    def lookup(
        self,
        from_: "Document",
        local_field: str,
        foreign_field: str,
        as_: Optional[str] = None,
    ) -> "Aggregate":
        if from_._is_dynamic:
            raise ValueError("dynamic collection unsupport lookup")
        model_fields = get_model_fields(self.document_class)
        if local_field not in model_fields and local_field != "_id":
            raise MotordanticValidationError(
                f"field - {local_field} not a field from model: {from_.__name__}"  # type: ignore
            )
        from_fields = get_model_fields(from_)
        if foreign_field not in from_fields and foreign_field != "_id":
            raise MotordanticValidationError(
                f"field - {foreign_field} not a field from model: {self.document_class.__name__}"  # type: ignore
            )
        lookup = {
            "from": from_.get_collection_name(),
            "localField": local_field,
            "foreignField": foreign_field,
        }
        if as_:
            lookup["as"] = as_
        else:
            collection_name = from_.get_collection_name()
            lookup["as"] = (
                f"{collection_name.lower()}es"
                if collection_name.endswith("s")
                else f"{collection_name.lower()}s"
            )
        self.pipeline.append({"$lookup": lookup})
        return self

    def unwind(self, unwind: Union[str, dict]) -> "Aggregate":
        if isinstance(unwind, str) and not unwind.startswith("$"):
            raise MotordanticValidationError("unwind must be startswith $")
        self.pipeline.append({"$unwind": unwind})
        return self

    def facet(self, facet: Dict[str, "Aggregate"]) -> "Aggregate":
        assert any([not isinstance(v, Aggregate) for v in facet.values()])
        self.pipeline.append(
            {"$facet": {name: aggregate.pipeline for name, aggregate in facet.items()}}
        )
        return self

    def out(self, name: str) -> "Aggregate":
        self.pipeline.append({"$out": name})
        return self

    def replace_with(self, value: Union[str, dict]) -> "Aggregate":
        if isinstance(value, str) and not value.startswith("$"):
            raise MotordanticValidationError("string value must be startwith $")
        self.pipeline.append({"$replaceWith": value})
        return self

    def redact(self, redact: Dict[str, Any]) -> "Aggregate":
        self.pipeline.append({"$redact": redact})
        return self

    def replace_root(self, new_root: Union[str, dict]) -> "Aggregate":
        if isinstance(new_root, str) and not new_root.startswith("$"):
            raise MotordanticValidationError("string new_root must be startwith $")
        self.pipeline.append({"$replaceRoot": {"newRoot": new_root}})
        return self

    def sample(self, **sample) -> "Aggregate":
        self.pipeline.append({"$sample": {**sample}})
        return self

    def fill(
        self,
        sort_by: dict,
        output: dict,
        partition_by: Optional[dict] = None,
        partition_by_fields: Optional[list] = None,
    ) -> "Aggregate":
        fill = {
            "sortBy": sort_by,
            "output": output,
        }
        if partition_by:
            fill["partitionBy"] = partition_by
        if partition_by_fields:
            fill["partitionByFields"] = partition_by_fields  # type: ignore
        self.pipeline.append({"$fill": fill})
        return self

    def geo_near(
        self,
        distance_field: str,
        near: dict,
        distance_multiplier: Optional[Union[int, float]] = None,
        include_locs: Optional[str] = None,
        key: Optional[str] = None,
        max_distance: Optional[float] = None,
        min_distance: Optional[float] = None,
        query: Optional[dict] = None,
        spherical: Optional[bool] = None,
    ) -> "Aggregate":
        geo_near: Dict[str, Any] = {"distanceField": distance_field, "near": near}
        if distance_multiplier is not None:
            geo_near["distanceMultiplier"] = distance_multiplier
        if include_locs:
            geo_near["includeLocs"] = include_locs
        if key:
            geo_near["key"] = key
        if max_distance:
            geo_near["maxDistance"] = max_distance
        if min_distance:
            geo_near["maxDistance"] = min_distance
        if query:
            geo_near["query"] = query
        if spherical is not None:
            geo_near["spherical"] = spherical
        self.pipeline.append({"$geoNear": geo_near})
        return self

    def let(self, let: dict) -> "Aggregate":
        self.pipeline.append({"$let": {**let}})
        return self

    def allow_disc_use(self, value: bool) -> "Aggregate":
        self.options["allowDiscUse"] = value
        return self

    def hint(self, index: str) -> "Aggregate":
        self.options["hint"] = index
        return self

    def session(self, session: Session) -> "Aggregate":
        self.options["session"] = session
        return self

    async def result(self) -> AggregateResult:
        result = await self._querybuilder.raw_aggregate(self.pipeline, **self.options)
        return AggregateResult(native_result=result, document_class=self.document_class)

    def result_sync(self) -> AggregateResult:
        qsync = (
            self.document_class.QSync(self.collection_name)  # type: ignore
            if self.document_class._is_dynamic
            else self.document_class.QSync()  # type: ignore
        )
        result = qsync.raw_aggregate(self.pipeline, **self.options)
        return AggregateResult(native_result=result, document_class=self.document_class)
