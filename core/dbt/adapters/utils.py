from typing import Mapping, Sequence, Any, Dict, List
from dbt.adapters.exceptions import DuplicateAliasError


class Translator:
    def __init__(self, aliases: Mapping[str, str], recursive: bool = False) -> None:
        self.aliases = aliases
        self.recursive = recursive

    def translate_mapping(self, kwargs: Mapping[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        for key, value in kwargs.items():
            canonical_key = self.aliases.get(key, key)
            if canonical_key in result:
                raise DuplicateAliasError(kwargs, self.aliases, canonical_key)
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
                raise RecursionError("Cycle detected in a value passed to translate!")
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
    :raises: `AliasError`, if a canonical key is defined more than once.
    """
    translator = Translator(aliases, recurse)
    return translator.translate(kwargs)


# some types need to make constants available to the jinja context as
# attributes, and regular properties only work with objects. maybe this should
# be handled by the RelationProxy?


class classproperty(object):
    def __init__(self, func) -> None:
        self.func = func

    def __get__(self, obj, objtype):
        return self.func(objtype)
