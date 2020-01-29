import abc
from functools import wraps
from typing import Callable, Optional, Any, FrozenSet, Dict, Set

from dbt.deprecations import warn, renamed_method


Decorator = Callable[[Any], Callable]


class _Available:
    def __call__(self, func: Callable) -> Callable:
        func._is_available_ = True  # type: ignore
        return func

    def parse(self, parse_replacement: Callable) -> Decorator:
        """A decorator factory to indicate that a method on the adapter will be
        exposed to the database wrapper, and will be stubbed out at parse time
        with the given function.

        @available.parse()
        def my_method(self, a, b):
            if something:
                return None
            return big_expensive_db_query()

        @available.parse(lambda *args, **args: {})
        def my_other_method(self, a, b):
            x = {}
            x.update(big_expensive_db_query())
            return x
        """
        def inner(func):
            func._parse_replacement_ = parse_replacement
            return self(func)
        return inner

    def deprecated(
        self, supported_name: str, parse_replacement: Optional[Callable] = None
    ) -> Decorator:
        """A decorator that marks a function as available, but also prints a
        deprecation warning. Use like

        @available.deprecated('my_new_method')
        def my_old_method(self, arg):
            args = compatability_shim(arg)
            return self.my_new_method(*args)

        @available.deprecated('my_new_slow_method', lambda *a, **k: (0, ''))
        def my_old_slow_method(self, arg):
            args = compatibility_shim(arg)
            return self.my_new_slow_method(*args)

        To make `adapter.my_old_method` available but also print out a warning
        on use directing users to `my_new_method`.

        The optional parse_replacement, if provided, will provide a parse-time
        replacement for the actual method (see `available.parse`).
        """
        def wrapper(func):
            func_name = func.__name__
            renamed_method(func_name, supported_name)

            @wraps(func)
            def inner(*args, **kwargs):
                warn('adapter:{}'.format(func_name))
                return func(*args, **kwargs)

            if parse_replacement:
                available_function = self.parse(parse_replacement)
            else:
                available_function = self
            return available_function(inner)
        return wrapper

    def parse_none(self, func: Callable) -> Callable:
        wrapper = self.parse(lambda *a, **k: None)
        return wrapper(func)

    def parse_list(self, func: Callable) -> Callable:
        wrapper = self.parse(lambda *a, **k: [])
        return wrapper(func)


available = _Available()


class AdapterMeta(abc.ABCMeta):
    _available_: FrozenSet[str]
    _parse_replacements_: Dict[str, Callable]

    def __new__(mcls, name, bases, namespace, **kwargs):
        # mypy does not like the `**kwargs`. But `ABCMeta` itself takes
        # `**kwargs` in its argspec here (and passes them to `type.__new__`.
        # I'm not sure there is any benefit to it after poking around a bit,
        # but having it doesn't hurt on the python side (and omitting it could
        # hurt for obscure metaclass reasons, for all I know)
        cls = abc.ABCMeta.__new__(  # type: ignore
            mcls, name, bases, namespace, **kwargs
        )

        # this is very much inspired by ABCMeta's own implementation

        # dict mapping the method name to whether the model name should be
        # injected into the arguments. All methods in here are exposed to the
        # context.
        available: Set[str] = set()
        replacements: Dict[str, Any] = {}

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

        cls._available_ = frozenset(available)
        # should this be a namedtuple so it will be immutable like _available_?
        cls._parse_replacements_ = replacements
        return cls
