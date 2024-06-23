import asyncio
from typing import TYPE_CHECKING, List

from .types import RelationTypes

__all__ = ("RelationManager",)

if TYPE_CHECKING:
    from .document import DocType, Document
    from .custom_typing import DictStrList, DocumentType


class RelationManager(object):
    """relation manager for get and set data from to to models instances"""

    __slots__ = ("document_class", "relation_fields")

    def __init__(self, document_class: "Document"):
        self.document_class = document_class
        self.relation_fields = self._get_relation_fields(document_class)

    @classmethod
    def _get_relation_fields(cls, document_class: "Document") -> dict:
        return document_class.__db_refs__ or {}

    def _relation_data_setter(self, document: "DocType", data: dict) -> "DocType":
        """data setter

        Args:
            document (Document): mongo model insatance
            data (dict): relations objects map

        Returns:
            Document: updated mongo model
        """
        for field, relation_info in self.relation_fields.items():
            relation_attr = getattr(document, field)
            if not relation_attr:
                continue
            if relation_info.relation_type == RelationTypes.ARRAY:
                relation_value = []
                for rel in relation_attr:
                    v = data[field].get(str(rel.db_ref.id))
                    if v:
                        relation_value.append(v)
            else:
                relation_value = data[field].get(str(relation_attr.db_ref.id))
            setattr(document, field, relation_value)
        return document

    async def _get_relation_objects_by_model_class(
        self, field: str, document_class: "DocumentType", ids: list
    ) -> dict:
        result = await document_class.Q.find(_id__in=ids, with_relations_objects=True)
        return {field: {str(o._id): o for o in result}}

    async def get_relation_objects(self, pre_relation: "DictStrList") -> dict:
        futures = []
        for field, ids in pre_relation.items():
            document_class = self.relation_fields[field].document_class
            futures.append(
                asyncio.ensure_future(
                    self._get_relation_objects_by_model_class(
                        field, document_class, ids
                    )
                )
            )

        relation_objects = {}
        for _, field_relation_result in enumerate(
            asyncio.as_completed(futures), start=1
        ):
            relation_objects.update(await field_relation_result)
        return relation_objects

    def _get_pre_relation(self, document_instances: List["DocType"]) -> "DictStrList":
        pre_relation: "DictStrList" = {field: [] for field in self.relation_fields}
        for document_instance in document_instances:
            for field in self.relation_fields:
                attr = getattr(document_instance, field)
                if isinstance(attr, list):
                    ids = tuple(row.db_ref.id for row in attr)
                else:
                    ids = (attr.db_ref.id,) if attr else tuple()
                if ids:
                    pre_relation[field].extend(ids)
        return {f: list(set(items)) for f, items in pre_relation.items()}

    async def map_relation_for_single(self, document_instance: "DocType") -> "DocType":
        """map relation data to mongo model instanc

        Args:
            document_instances (List[Document]): list of instances from Document

        Returns:
            Document: mapped mongo model
        """
        pre_relation: "DictStrList" = self._get_pre_relation([document_instance])
        relation_objects = await self.get_relation_objects(pre_relation)
        return self._relation_data_setter(document_instance, relation_objects)

    async def map_relation_for_array(self, result: List) -> List["Document"]:
        """map relations data for _find method list result

        Args:
            result (List): _find query result converted to list

        Returns:
            List: mapped list
        """
        pre_relation: "DictStrList" = self._get_pre_relation(result)
        relation_objects = await self.get_relation_objects(pre_relation)
        generated_result = [
            self._relation_data_setter(r, relation_objects) for r in result
        ]
        return generated_result
