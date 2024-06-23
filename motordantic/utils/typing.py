import sys
from typing import Dict, Optional, Type, Any, _eval_type, ForwardRef  # type: ignore


def resolve_annotations(
    raw_annotations: Dict[str, Type[Any]], module_name: Optional[str]
) -> Dict[str, Type[Any]]:
    """
    Partially taken from typing.get_type_hints.

    Resolve string or ForwardRef annotations into type objects if possible.
    """
    base_globals: Optional[Dict[str, Any]] = None
    if module_name:
        try:
            module = sys.modules[module_name]
        except KeyError:
            # happens occasionally, see https://github.com/pydantic/pydantic/issues/2363
            pass
        else:
            base_globals = module.__dict__

    annotations = {}
    for name, value in raw_annotations.items():
        if isinstance(value, str):
            if (3, 10) > sys.version_info >= (3, 9, 8) or sys.version_info >= (
                3,
                10,
                1,
            ):
                value = ForwardRef(value, is_argument=False, is_class=True)  # type: ignore
            else:
                value = ForwardRef(value, is_argument=False)
        try:
            value = _eval_type(value, base_globals, None)
        except NameError:
            # this is ok, it can be fixed with update_forward_refs
            pass
        annotations[name] = value
    return annotations
