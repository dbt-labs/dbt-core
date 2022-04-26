import collections
import concurrent.futures
import copy
import datetime
import decimal
import functools
import hashlib
import itertools
import jinja2
import json
import os
import requests
from tarfile import ReadError
import time
from pathlib import PosixPath, WindowsPath

from contextlib import contextmanager
from dbt.exceptions import ConnectionException
from dbt.events.functions import fire_event
from dbt.events.types import RetryExternalCall, RecordRetryException
from dbt import flags
from enum import Enum
from typing_extensions import Protocol
from typing import (
    Tuple,
    Type,
    Any,
    Optional,
    TypeVar,
    Dict,
    Union,
    Callable,
    List,
    Iterator,
    Mapping,
    Iterable,
    AbstractSet,
    Set,
    Sequence,
)

import dbt.exceptions

DECIMALS: Tuple[Type[Any], ...]
try:
    import cdecimal  # typing: ignore
except ImportError:
    DECIMALS = (decimal.Decimal,)
else:
    DECIMALS = (decimal.Decimal, cdecimal.Decimal)


class ExitCodes(int, Enum):
    Success = 0
    ModelError = 1
    UnhandledError = 2


def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
    return None


def get_profile_from_project(project):
    target_name = project.get("target", {})
    profile = project.get("outputs", {}).get(target_name, {})
    return profile


def get_model_name_or_none(model):
    if model is None:
        name = "<None>"

    elif isinstance(model, str):
        name = model
    elif isinstance(model, dict):
        name = model.get("alias", model.get("name"))
    elif hasattr(model, "alias"):
        name = model.alias
    elif hasattr(model, "name"):
        name = model.name
    else:
        name = str(model)
    return name


MACRO_PREFIX = "dbt_macro__"
DOCS_PREFIX = "dbt_docs__"


def get_dbt_macro_name(name):
    if name is None:
        raise dbt.exceptions.InternalException("Got None for a macro name!")
    return f"{MACRO_PREFIX}{name}"


def get_dbt_docs_name(name):
    if name is None:
        raise dbt.exceptions.InternalException("Got None for a doc name!")
    return f"{DOCS_PREFIX}{name}"


def get_materialization_macro_name(materialization_name, adapter_type=None, with_prefix=True):
    if adapter_type is None:
        adapter_type = "default"
    name = f"materialization_{materialization_name}_{adapter_type}"
    return get_dbt_macro_name(name) if with_prefix else name


def get_docs_macro_name(docs_name, with_prefix=True):
    return get_dbt_docs_name(docs_name) if with_prefix else docs_name


def get_test_macro_name(test_name, with_prefix=True):
    name = f"test_{test_name}"
    return get_dbt_macro_name(name) if with_prefix else name


def split_path(path):
    return path.split(os.sep)


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
    >>> dbt.utils.deep_merge({'a': 1, 'b': 2, 'c': 3}, {'a': 2}, {'a': 3, 'b': 1})  # noqa
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
    atomic_types: Tuple[Type[Any], ...] = (int, float, str, type(None), bool)

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
        raise dbt.exceptions.DbtConfigError(
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
            raise dbt.exceptions.RecursionException("Cycle detected in deep_map_render")
        raise


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def get_pseudo_test_path(node_name, source_path):
    "schema tests all come from schema.yml files. fake a source sql file"
    source_path_parts = split_path(source_path)
    source_path_parts.pop()  # ignore filename
    suffix = ["{}.sql".format(node_name)]
    pseudo_path_parts = source_path_parts + suffix
    return os.path.join(*pseudo_path_parts)


def get_pseudo_hook_path(hook_name):
    path_parts = ["hooks", "{}.sql".format(hook_name)]
    return os.path.join(*path_parts)


def md5(string):
    return hashlib.md5(string.encode("utf-8")).hexdigest()


def get_hash(model):
    return hashlib.md5(model.unique_id.encode("utf-8")).hexdigest()


def get_hashed_contents(model):
    return hashlib.md5(model.raw_sql.encode("utf-8")).hexdigest()


def flatten_nodes(dep_list):
    return list(itertools.chain.from_iterable(dep_list))


class memoized:
    """Decorator. Caches a function's return value each time it is called. If
    called later with the same arguments, the cached value is returned (not
    reevaluated).

    Taken from https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize"""

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        if not isinstance(args, collections.abc.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        value = self.func(*args)
        self.cache[args] = value
        return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


K_T = TypeVar("K_T")
V_T = TypeVar("V_T")


def filter_null_values(input: Dict[K_T, Optional[V_T]]) -> Dict[K_T, V_T]:
    return {k: v for k, v in input.items() if v is not None}


def add_ephemeral_model_prefix(s: str) -> str:
    return "__dbt__cte__{}".format(s)


def timestring() -> str:
    """Get the current datetime as an RFC 3339-compliant string"""
    # isoformat doesn't include the mandatory trailing 'Z' for UTC.
    return datetime.datetime.utcnow().isoformat() + "Z"


class JSONEncoder(json.JSONEncoder):
    """A 'custom' json encoder that does normal json encoder things, but also
    handles `Decimal`s and `Undefined`s. Decimals can lose precision because
    they get converted to floats. Undefined's are serialized to an empty string
    """

    def default(self, obj):
        if isinstance(obj, DECIMALS):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        if isinstance(obj, jinja2.Undefined):
            return ""
        if hasattr(obj, "to_dict"):
            # if we have a to_dict we should try to serialize the result of
            # that!
            return obj.to_dict(omit_none=True)
        return super().default(obj)


class ForgivingJSONEncoder(JSONEncoder):
    def default(self, obj):
        # let dbt's default JSON encoder handle it if possible, fallback to
        # str()
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


class Translator:
    def __init__(self, aliases: Mapping[str, str], recursive: bool = False):
        self.aliases = aliases
        self.recursive = recursive

    def translate_mapping(self, kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        for key, value in kwargs.items():
            canonical_key = self.aliases.get(key, key)
            if canonical_key in result:
                dbt.exceptions.raise_duplicate_alias(kwargs, self.aliases, canonical_key)
            result[canonical_key] = self.translate_value(value)
        return result

    def translate_sequence(self, value: Sequence[Any]) -> List[Any]:
        return [self.translate_value(v) for v in value]

    def translate_value(self, value: Any) -> Any:
        if self.recursive:
            if isinstance(value, Mapping):
                return self.translate_mapping(value)
            elif isinstance(value, (list, tuple)):
                return self.translate_sequence(value)
        return value

    def translate(self, value: Mapping[str, Any]) -> Dict[str, Any]:
        try:
            return self.translate_mapping(value)
        except RuntimeError as exc:
            if "maximum recursion depth exceeded" in str(exc):
                raise dbt.exceptions.RecursionException(
                    "Cycle detected in a value passed to translate!"
                )
            raise


def translate_aliases(
    kwargs: Dict[str, Any],
    aliases: Dict[str, str],
    recurse: bool = False,
) -> Dict[str, Any]:
    """Given a dict of keyword arguments and a dict mapping aliases to their
    canonical values, canonicalize the keys in the kwargs dict.

    If recurse is True, perform this operation recursively.

    :returns: A dict containing all the values in kwargs referenced by their
        canonical key.
    :raises: `AliasException`, if a canonical key is defined more than once.
    """
    translator = Translator(aliases, recurse)
    return translator.translate(kwargs)


# Note that this only affects hologram json validation.
# It has no effect on mashumaro serialization.
def restrict_to(*restrictions):
    """Create the metadata for a restricted dataclass field"""
    return {"restrict": list(restrictions)}


def coerce_dict_str(value: Any) -> Optional[Dict[str, Any]]:
    """For annoying mypy reasons, this helper makes dealing with nested dicts
    easier. You get either `None` if it's not a Dict[str, Any], or the
    Dict[str, Any] you expected (to pass it to dbtClassMixin.from_dict(...)).
    """
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return value
    else:
        return None


def _coerce_decimal(value):
    if isinstance(value, DECIMALS):
        return float(value)
    return value


def lowercase(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    else:
        return value.lower()


# some types need to make constants available to the jinja context as
# attributes, and regular properties only work with objects. maybe this should
# be handled by the RelationProxy?


class classproperty(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, obj, objtype):
        return self.func(objtype)


def format_bytes(num_bytes):
    for unit in ["Bytes", "KB", "MB", "GB", "TB", "PB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f} {unit}"
        num_bytes /= 1024.0

    num_bytes *= 1024.0
    return f"{num_bytes:3.1f} {unit}"


def format_rows_number(rows_number):
    for unit in ["", "k", "m", "b", "t"]:
        if abs(rows_number) < 1000.0:
            return f"{rows_number:3.1f}{unit}".strip()
        rows_number /= 1000.0

    rows_number *= 1000.0
    return f"{rows_number:3.1f}{unit}".strip()


class ConnectingExecutor(concurrent.futures.Executor):
    def submit_connected(self, adapter, conn_name, func, *args, **kwargs):
        def connected(conn_name, func, *args, **kwargs):
            with self.connection_named(adapter, conn_name):
                return func(*args, **kwargs)

        return self.submit(connected, conn_name, func, *args, **kwargs)


# a little concurrent.futures.Executor for single-threaded mode
class SingleThreadedExecutor(ConnectingExecutor):
    def submit(*args, **kwargs):
        # this basic pattern comes from concurrent.futures.Executor itself,
        # but without handling the `fn=` form.
        if len(args) >= 2:
            self, fn, *args = args
        elif not args:
            raise TypeError(
                "descriptor 'submit' of 'SingleThreadedExecutor' object needs " "an argument"
            )
        else:
            raise TypeError(
                "submit expected at least 1 positional argument, " "got %d" % (len(args) - 1)
            )
        fut = concurrent.futures.Future()
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)
        return fut

    @contextmanager
    def connection_named(self, adapter, name):
        yield


class MultiThreadedExecutor(
    ConnectingExecutor,
    concurrent.futures.ThreadPoolExecutor,
):
    @contextmanager
    def connection_named(self, adapter, name):
        with adapter.connection_named(name):
            yield


class ThreadedArgs(Protocol):
    single_threaded: bool


class HasThreadingConfig(Protocol):
    args: ThreadedArgs
    threads: Optional[int]


def executor(config: HasThreadingConfig) -> ConnectingExecutor:
    if config.args.single_threaded:
        return SingleThreadedExecutor()
    else:
        return MultiThreadedExecutor(max_workers=config.threads)


def fqn_search(root: Dict[str, Any], fqn: List[str]) -> Iterator[Dict[str, Any]]:
    """Iterate into a nested dictionary, looking for keys in the fqn as levels.
    Yield the level config.
    """
    yield root

    for level in fqn:
        level_config = root.get(level, None)
        if not isinstance(level_config, dict):
            break
        # This used to do a 'deepcopy',
        # but it didn't seem to be necessary
        yield level_config
        root = level_config


StringMap = Mapping[str, Any]
StringMapList = List[StringMap]
StringMapIter = Iterable[StringMap]


class MultiDict(Mapping[str, Any]):
    """Implement the mapping protocol using a list of mappings. The most
    recently added mapping "wins".
    """

    def __init__(self, sources: Optional[StringMapList] = None) -> None:
        super().__init__()
        self.sources: StringMapList

        if sources is None:
            self.sources = []
        else:
            self.sources = sources

    def add_from(self, sources: StringMapIter):
        self.sources.extend(sources)

    def add(self, source: StringMap):
        self.sources.append(source)

    def _keyset(self) -> AbstractSet[str]:
        # return the set of keys
        keys: Set[str] = set()
        for entry in self._itersource():
            keys.update(entry)
        return keys

    def _itersource(self) -> StringMapIter:
        return reversed(self.sources)

    def __iter__(self) -> Iterator[str]:
        # we need to avoid duplicate keys
        return iter(self._keyset())

    def __len__(self):
        return len(self._keyset())

    def __getitem__(self, name: str) -> Any:
        for entry in self._itersource():
            if name in entry:
                return entry[name]
        raise KeyError(name)

    def __contains__(self, name) -> bool:
        return any((name in entry for entry in self._itersource()))


def _connection_exception_retry(fn, max_attempts: int, attempt: int = 0):
    """Attempts to run a function that makes an external call, if the call fails
    on a Requests exception or decompression issue (ReadError), it will be tried
    up to 5 more times.  All exceptions that Requests explicitly raises inherit from
    requests.exceptions.RequestException.  See https://github.com/dbt-labs/dbt-core/issues/4579
    for context on this decompression issues specifically.
    """
    try:
        return fn()
    except (
        requests.exceptions.RequestException,
        ReadError,
    ) as exc:
        if attempt <= max_attempts - 1:
            fire_event(RecordRetryException(exc=exc))
            fire_event(RetryExternalCall(attempt=attempt, max=max_attempts))
            time.sleep(1)
            return _connection_exception_retry(fn, max_attempts, attempt + 1)
        else:
            raise ConnectionException("External connection exception occurred: " + str(exc))


# This is used to serialize the args in the run_results and in the logs.
# We do this separately because there are a few fields that don't serialize,
# i.e. PosixPath, WindowsPath, and types. It also includes args from both
# cli args and flags, which is more complete than just the cli args.
# If new args are added that are false by default (particularly in the
# global options) they should be added to the 'default_false_keys' list.
def args_to_dict(args):
    var_args = vars(args).copy()
    # update the args with the flags, which could also come from environment
    # variables or user_config
    flag_dict = flags.get_flag_dict()
    var_args.update(flag_dict)
    dict_args = {}
    # remove args keys that clutter up the dictionary
    for key in var_args:
        if key == "cls":
            continue
        if var_args[key] is None:
            continue
        # TODO: add more default_false_keys
        default_false_keys = (
            "debug",
            "full_refresh",
            "fail_fast",
            "warn_error",
            "single_threaded",
            "log_cache_events",
            "store_failures",
            "use_experimental_parser",
        )
        if key in default_false_keys and var_args[key] is False:
            continue
        if key == "vars" and var_args[key] == "{}":
            continue
        # this was required for a test case
        if isinstance(var_args[key], PosixPath) or isinstance(var_args[key], WindowsPath):
            var_args[key] = str(var_args[key])
        dict_args[key] = var_args[key]
    return dict_args
