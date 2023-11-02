import copy
import datetime
from typing import Dict, Optional, TypeVar, Callable, Any, Tuple, Union, Type

from dbt.common.exceptions import DbtConfigError, RecursionError

K_T = TypeVar("K_T")
V_T = TypeVar("V_T")


def filter_null_values(input: Dict[K_T, Optional[V_T]]) -> Dict[K_T, V_T]:
    return {k: v for k, v in input.items() if v is not None}


class AttrDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def merge(*args):
    if len(args) == 0:
        return None

    if len(args) == 1:
        return args[0]

    lst = list(args)
    last = lst.pop(len(lst) - 1)

    return _merge(merge(*lst), last)


def _merge(a, b):
    to_return = a.copy()
    to_return.update(b)
    return to_return


# http://stackoverflow.com/questions/20656135/python-deep-merge-dictionary-data
def deep_merge(*args):
    """
    >>> dbt.common.utils.deep_merge({'a': 1, 'b': 2, 'c': 3}, {'a': 2}, {'a': 3, 'b': 1})  # noqa
    {'a': 3, 'b': 1, 'c': 3}
    """
    if len(args) == 0:
        return None

    if len(args) == 1:
        return copy.deepcopy(args[0])

    lst = list(args)
    last = copy.deepcopy(lst.pop(len(lst) - 1))

    return _deep_merge(deep_merge(*lst), last)


def _deep_merge(destination, source):
    if isinstance(source, dict):
        for key, value in source.items():
            deep_merge_item(destination, key, value)
        return destination


def deep_merge_item(destination, key, value):
    if isinstance(value, dict):
        node = destination.setdefault(key, {})
        destination[key] = deep_merge(node, value)
    elif isinstance(value, tuple) or isinstance(value, list):
        if key in destination:
            destination[key] = list(value) + list(destination[key])
        else:
            destination[key] = value
    else:
        destination[key] = value


def _deep_map_render(
    func: Callable[[Any, Tuple[Union[str, int], ...]], Any],
    value: Any,
    keypath: Tuple[Union[str, int], ...],
) -> Any:
    atomic_types: Tuple[Type[Any], ...] = (int, float, str, type(None), bool, datetime.date)

    ret: Any

    if isinstance(value, list):
        ret = [_deep_map_render(func, v, (keypath + (idx,))) for idx, v in enumerate(value)]
    elif isinstance(value, dict):
        ret = {k: _deep_map_render(func, v, (keypath + (str(k),))) for k, v in value.items()}
    elif isinstance(value, atomic_types):
        ret = func(value, keypath)
    else:
        container_types: Tuple[Type[Any], ...] = (list, dict)
        ok_types = container_types + atomic_types
        raise DbtConfigError(
            "in _deep_map_render, expected one of {!r}, got {!r}".format(ok_types, type(value))
        )

    return ret


def deep_map_render(func: Callable[[Any, Tuple[Union[str, int], ...]], Any], value: Any) -> Any:
    """This function renders a nested dictionary derived from a yaml
    file. It is used to render dbt_project.yml, profiles.yml, and
    schema files.

    It maps the function func() onto each non-container value in 'value'
    recursively, returning a new value. As long as func does not manipulate
    the value, then deep_map_render will also not manipulate it.

    value should be a value returned by `yaml.safe_load` or `json.load` - the
    only expected types are list, dict, native python number, str, NoneType,
    and bool.

    func() will be called on numbers, strings, Nones, and booleans. Its first
    parameter will be the value, and the second will be its keypath, an
    iterable over the __getitem__ keys needed to get to it.

    :raises: If there are cycles in the value, raises a
        dbt.exceptions.RecursionException
    """
    try:
        return _deep_map_render(func, value, ())
    except RuntimeError as exc:
        if "maximum recursion depth exceeded" in str(exc):
            raise RecursionError("Cycle detected in deep_map_render")
        raise
