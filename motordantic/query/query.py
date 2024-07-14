import copy
from typing import TYPE_CHECKING, Union

from .extra import ExtraQueryMapper

from ..validation import validate_field_value, validate_object_id


__all__ = (
    "Q",
    "QCombination",
)


if TYPE_CHECKING:
    from ..manager import ODMManager, DynamicCollectionODMManager
    from .builder import Builder


class BaseQuery(object):
    __slots__ = ("_builder", "method_name")

    def __init__(self, builder: "Builder", method_name: str):
        self._builder = builder
        self.method_name = method_name

    def __call__(self, *args, **kwargs):
        method = getattr(self._builder, self.method_name)
        return method(*args, **kwargs)


class Query(BaseQuery):
    def __getattr__(self, method_name: str) -> "Query":
        return Query(self._builder, method_name)


def _validate_query_data(builder: "Builder", query: dict) -> dict:
    return builder._validate_query_data(query)


class QNodeVisitor(object):
    """Base visitor class for visiting Q-object nodes in a query tree."""

    def prepare_combination(
        self, combination: "QCombination"
    ) -> Union["QCombination", dict]:
        """Called by QCombination objects."""
        return combination

    def visit_query(self, query: "Q") -> Union["Q", dict]:
        """Called by (New)Q objects."""
        return query


class SimplificationVisitor(QNodeVisitor):
    def __init__(self, builder: "Builder"):
        self.builder = builder

    def prepare_combination(
        self, combination: "QCombination"
    ) -> Union["QCombination", dict]:
        if combination.operation == combination.AND:
            # The simplification only applies to 'simple' queries
            if all(isinstance(node, Q) for node in combination.children):
                queries = [n.query for n in combination.children]
                query = self._query_conjunction(queries)
                return {"$and": query}

        return combination

    def _query_conjunction(self, queries):
        """Merges query dicts - effectively &ing them together."""
        combined_query = []
        for query in queries:
            query = _validate_query_data(self.builder, query)
            combined_query.append(copy.deepcopy(query))
        return combined_query


class QCompilerVisitor(QNodeVisitor):
    """Compiles the nodes in a query tree to a PyMongo-compatible query
    dictionary.
    """

    def __init__(self, builder: "Builder"):
        self.builder = builder

    def prepare_combination(
        self, combination: "QCombination"
    ) -> Union["QCombination", dict]:
        operator = "$and"
        if combination.operation == combination.OR:
            operator = "$or"
        return {operator: combination.children}

    def visit_query(self, query: "Q") -> Union["Q", dict]:
        data = _validate_query_data(self.builder, query.query)
        return data


class QNode(object):
    """Base class for nodes in query trees."""

    AND = 0
    OR = 1

    def to_query(self, builder: "Builder") -> dict:
        query = self.accept(SimplificationVisitor(builder))
        if not isinstance(query, dict):
            query = query.accept(QCompilerVisitor(builder))
        return query

    def accept(self, visitor):
        raise NotImplementedError

    def _combine(self, other, operation):
        """Combine this node with another node into a QCombination
        object.
        """
        # If the other Q() is empty, ignore it and just use `self`.
        if getattr(other, "empty", True):
            return self

        # Or if this Q is empty, ignore it and just use `other`.
        if self.empty:
            return other

        return QCombination(operation, [self, other])

    @property
    def empty(self):
        return False

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)


class QCombination(QNode):
    def __init__(self, operation, children):
        self.operation = operation
        self.children = []
        for node in children:
            # If the child is a combination of the same type, we can merge its
            # children directly into this combinations children
            if isinstance(node, QCombination) and node.operation == operation:
                self.children += node.children
            else:
                self.children.append(node)

    def __repr__(self):
        op = " & " if self.operation is self.AND else " | "
        return "(%s)" % op.join([repr(node) for node in self.children])

    def __bool__(self):
        return bool(self.children)

    def accept(self, visitor) -> Union["QCombination", dict]:
        for i in range(len(self.children)):
            if isinstance(self.children[i], QNode):
                self.children[i] = self.children[i].accept(visitor)

        return visitor.prepare_combination(self)

    @property
    def empty(self):
        return not bool(self.children)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.operation == other.operation
            and self.children == other.children
        )


class Q(QNode):
    """A simple query object, used in a query tree to build up more complex
    query structures.
    """

    def __init__(self, **query):
        self.query = query

    def __repr__(self):
        return "Q(**%s)" % repr(self.query)

    def __bool__(self):
        return bool(self.query)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.query == other.query

    def accept(self, visit: "QNodeVisitor") -> Union["Q", dict]:
        return visit.visit_query(self)

    @property
    def empty(self) -> bool:
        return not bool(self.query)


def generate_basic_query(
    manager: Union["ODMManager", "DynamicCollectionODMManager"],
    query: dict,
    with_validate_document_fields: bool = True,
) -> dict:
    query_params: dict = {}
    for query_field, value in query.items():
        field, *extra_params = query_field.split("__")
        inners, extra_params = manager._parse_extra_params(extra_params)
        if with_validate_document_fields and not manager._validate_field(field):
            continue
        query_field_name = manager.document.__mapping_query_fields__[field]
        extra = ExtraQueryMapper(manager.document, field).query(extra_params, value)
        if extra:
            value = extra[field]
        elif field == "_id":
            value = validate_object_id(manager.document, value)
        else:
            value = (
                validate_field_value(manager.document, field, value)
                if not inners
                else value
            )
        if inners:
            query_field_name = f'{query_field_name}.{".".join(i for i in inners)}'
        if (
            extra
            and query_field_name in query_params
            and ("__gt" in query_field or "__lt" in query_field)
        ):
            query_params[query_field_name].update(value)
        else:
            query_params[query_field_name] = value
    return query_params
