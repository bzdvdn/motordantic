from typing import TYPE_CHECKING, Dict, Any
from dataclasses import dataclass

from ..exceptions import MotordanticValidationError
from ..utils.pydantic import get_model_fields

__all__ = ("Sum", "Avg", "Min", "Count", "Max")

if TYPE_CHECKING:
    from ..document import Document


class BaseAggregationOperation(object):
    """Abstract class for Aggregation"""

    __slots__ = ("field", "_operation")

    _operation: str

    def __init__(self, field: str):
        self.field = field

    @property
    def operation(self) -> str:
        if not self._operation:
            raise NotImplementedError("implement _operation")
        return self._operation

    def _validate_field(self, document_class: "Document"):
        # if self.field in document_class.__motordantic_extra_fields__.values():
        #     return None
        model_fields = get_model_fields(document_class)
        if self.field not in model_fields and self.field != "_id":
            raise MotordanticValidationError(
                f'invalid field "{self.field}" for this model, field must be one of {list(model_fields.keys())}'
            )

    def _aggregate_query(self, document_class: "Document") -> dict:
        self._validate_field(document_class)
        query_field = document_class.__mapping_query_fields__[self.field]
        query = {
            f"{query_field}__{self.operation}": {f"${self.operation}": f"${self.field}"}
        }
        return query


class Sum(BaseAggregationOperation):
    """
    Simple sum aggregation

    generated query: {'field__sum': {'$sum': 'field'}}

    return: {'field__sum': <value>}
    """

    _operation: str = "sum"


class Max(BaseAggregationOperation):
    """
    Simple max aggregation

    generated query: {'field__max': {'$max': 'field'}}

    return: {'field__max': <value>}
    """

    _operation: str = "max"


class Min(BaseAggregationOperation):
    """
    Simple min aggregation

    generated query: {'field__min': {'$min': 'field'}}

    return: {'field__min': <value>}
    """

    _operation: str = "min"


class Avg(BaseAggregationOperation):
    """
    Simple avg aggregation

    generated query: {'field__avg': {'$avg': 'field'}}

    return: {'field__avg': <value>}
    """

    _operation: str = "avg"


class Count(BaseAggregationOperation):
    """
    Simple Count aggregation

    generated query:
        - if field = _id
            {
                '_id': None
                'count': {'$sum': 1},
            }
        - else
            {S
                '_id': 'field'
                'count': {'$sum': 1},
            }
    return: {'_id': <value>, 'count': value}
    """

    _operation: str = "count"

    def _aggregate_query(self, document_class: "Document") -> dict:
        self._validate_field(document_class)
        query_field = document_class.__mapping_query_fields__[self.field]
        query = {
            "_id": f"${query_field}" if self.field != "_id" else None,
            f"count": {"$sum": 1},
        }
        return query


@dataclass
class Bucket(object):
    group_by: str
    boundaries: list
    default: str
    output: Dict[str, Any]

    def to_mongo(self) -> Dict[str, Any]:
        bucket = {
            "groupBy": self.group_by,
            "boundaries": self.boundaries,
            "default": self.default,
            "output": self.output,
        }
        return bucket
