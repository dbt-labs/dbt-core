import abc
from functools import wraps
from typing import Callable, Optional, Any, FrozenSet, Dict

from dbt.deprecations import warn, renamed_method


Decorator = Callable[[Any], Callable]


def available_function(func: Callable) -> Callable:
    """A decorator to indicate that a method on the adapter will be
    exposed to the database wrapper, and will be available at parse and run
    time.
    """
    func._is_available_ = True  # type: ignore
    return func


def available_deprecated(
    supported_name: str,
    parse_replacement: Optional[Callable] = None
) -> Decorator:
    """A decorator that marks a function as available, but also prints a
    deprecation warning. Use like

    @available_deprecated('my_new_method')
    def my_old_method(self, arg):
        args = compatability_shim(arg)
        return self.my_new_method(*args)

    @available_deprecated('my_new_slow_method', lambda *a, **k: (0, ''))
    def my_old_slow_method(self, arg):
        args = compatibility_shim(arg)
        return self.my_new_slow_method(*args)

    To make `adapter.my_old_method` available but also print out a warning on
    use directing users to `my_new_method`.

    The optional parse_replacement, if provided, will provide a parse-time
    replacement for the actual method (see `available_parse`).
    """
    def wrapper(func):
        func_name = func.__name__
        renamed_method(func_name, supported_name)

        @wraps(func)
        def inner(*args, **kwargs):
            warn('adapter:{}'.format(func_name))
            return func(*args, **kwargs)

        if parse_replacement:
            available_function = available_parse(parse_replacement)
        return available_function(inner)
    return wrapper


def available_parse(parse_replacement: Callable) -> Decorator:
    """A decorator factory to indicate that a method on the adapter will be
    exposed to the database wrapper, and will be stubbed out at parse time with
    the given function.

    @available_parse()
    def my_method(self, a, b):
        if something:
            return None
        return big_expensive_db_query()

    @available_parse(lambda *args, **args: {})
    def my_other_method(self, a, b):
        x = {}
        x.update(big_expensive_db_query())
        return x
    """
    def inner(func):
        func._parse_replacement_ = parse_replacement
        available(func)
        return func
    return inner


class available:
    def __new__(cls, func: Callable) -> Callable:
        return available_function(func)

    @classmethod
    def parse(cls, parse_replacement: Callable) -> Decorator:
        return available_parse(parse_replacement)

    @classmethod
    def deprecated(
        cls, supported_name: str, parse_replacement: Optional[Callable] = None
    ) -> Decorator:
        return available_deprecated(supported_name, parse_replacement)

    @classmethod
    def parse_none(cls, func: Callable) -> Callable:
        wrapper = available_parse(lambda *a, **k: None)
        return wrapper(func)

    @classmethod
    def parse_list(cls, func: Callable) -> Callable:
        wrapper = available_parse(lambda *a, **k: [])
        return wrapper(func)

# available.deprecated = available_deprecated
# available.parse = available_parse
# available.parse_none = available_parse(lambda *a, **k: None)
# available.parse_list = available_parse(lambda *a, **k: [])


class AdapterMeta(abc.ABCMeta):
    def __new__(mcls, name, bases, namespace, **kwargs):
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)

        # this is very much inspired by ABCMeta's own implementation

        # dict mapping the method name to whether the model name should be
        # injected into the arguments. All methods in here are exposed to the
        # context.
        available = set()
        replacements = {}

        # collect base class data first
        for base in bases:
            available.update(getattr(base, '_available_', set()))
            replacements.update(getattr(base, '_parse_replacements_', set()))

        # override with local data if it exists
        for name, value in namespace.items():
            if getattr(value, '_is_available_', False):
                available.add(name)
            parse_replacement = getattr(value, '_parse_replacement_', None)
            if parse_replacement is not None:
                replacements[name] = parse_replacement

        cls._available_: FrozenSet[str] = frozenset(available)
        # should this be a namedtuple so it will be immutable like _available_?
        cls._parse_replacements_: Dict[str, Callable] = replacements
        return cls
