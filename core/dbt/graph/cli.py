# special support for CLI argument parsing.
from dbt import flags
import itertools
from dbt.clients.yaml_helper import yaml, Loader, Dumper  # noqa: F401

from typing import Dict, List, Optional, Tuple, Any, Union

from dbt.contracts.selection import SelectorDefinition, SelectorFile
from dbt.exceptions import InternalException, ValidationException

from .selector_spec import (
    SelectionUnion,
    SelectionSpec,
    SelectionIntersection,
    SelectionDifference,
    SelectionCriteria,
    IndirectSelection,
)

INTERSECTION_DELIMITER = ","

DEFAULT_INCLUDES: List[str] = ["fqn:*", "source:*", "exposure:*", "metric:*"]
DEFAULT_EXCLUDES: List[str] = []


def parse_union(
    components: List[str],
    expect_exists: bool,
    indirect_selection: IndirectSelection = IndirectSelection.Eager,
) -> SelectionUnion:
    # turn ['a b', 'c'] -> ['a', 'b', 'c']
    raw_specs = itertools.chain.from_iterable(r.split(" ") for r in components)
    union_components: List[SelectionSpec] = []

    # ['a', 'b', 'c,d'] -> union('a', 'b', intersection('c', 'd'))
    for raw_spec in raw_specs:
        intersection_components: List[SelectionSpec] = [
            SelectionCriteria.from_single_spec(part, indirect_selection=indirect_selection)
            for part in raw_spec.split(INTERSECTION_DELIMITER)
        ]
        union_components.append(
            SelectionIntersection(
                components=intersection_components,
                expect_exists=expect_exists,
                raw=raw_spec,
            )
        )
    return SelectionUnion(
        components=union_components,
        expect_exists=False,
        raw=components,
    )


def parse_union_from_default(
    raw: Optional[List[str]],
    default: List[str],
    indirect_selection: IndirectSelection = IndirectSelection.Eager,
) -> SelectionUnion:
    components: List[str]
    expect_exists: bool
    if raw is None:
        return parse_union(
            components=default, expect_exists=False, indirect_selection=indirect_selection
        )
    else:
        return parse_union(
            components=raw, expect_exists=True, indirect_selection=indirect_selection
        )


def parse_difference(
    include: Optional[List[str]], exclude: Optional[List[str]]
) -> SelectionDifference:

    included = parse_union_from_default(
        include, DEFAULT_INCLUDES, indirect_selection=IndirectSelection(flags.INDIRECT_SELECTION)
    )
    excluded = parse_union_from_default(
        exclude, DEFAULT_EXCLUDES, indirect_selection=IndirectSelection.Eager
    )
    return SelectionDifference(components=[included, excluded])


RawDefinition = Union[str, Dict[str, Any]]


def _get_list_dicts(dct: Dict[str, Any], key: str) -> List[RawDefinition]:
    result: List[RawDefinition] = []
    if key not in dct:
        raise InternalException(f"Expected to find key {key} in dict, only found {list(dct)}")
    values = dct[key]
    if not isinstance(values, list):
        raise ValidationException(f'Invalid value for key "{key}". Expected a list.')
    for value in values:
        if isinstance(value, dict):
            for value_key in value:
                if not isinstance(value_key, str):
                    raise ValidationException(
                        f'Expected all keys to "{key}" dict to be strings, '
                        f'but "{value_key}" is a "{type(value_key)}"'
                    )
            result.append(value)
        elif isinstance(value, str):
            result.append(value)
        else:
            raise ValidationException(
                f'Invalid value type {type(value)} in key "{key}", expected '
                f"dict or str (value: {value})."
            )

    return result


def _parse_exclusions(definition) -> Optional[SelectionSpec]:
    exclusions = _get_list_dicts(definition, "exclude")
    parsed_exclusions = [parse_from_definition(excl) for excl in exclusions]
    if len(parsed_exclusions) == 1:
        return parsed_exclusions[0]
    elif len(parsed_exclusions) > 1:
        return SelectionUnion(components=parsed_exclusions, raw=exclusions)
    else:
        return None


def _parse_include_exclude_subdefs(
    definitions: List[RawDefinition],
) -> Tuple[List[SelectionSpec], Optional[SelectionSpec]]:
    include_parts: List[SelectionSpec] = []
    diff_arg: Optional[SelectionSpec] = None

    for definition in definitions:
        if isinstance(definition, dict) and "exclude" in definition:
            # do not allow multiple exclude: defs at the same level
            if diff_arg is not None:
                yaml_sel_cfg = yaml.dump(definition)
                raise ValidationException(
                    f"You cannot provide multiple exclude arguments to the "
                    f"same selector set operator:\n{yaml_sel_cfg}"
                )
            diff_arg = _parse_exclusions(definition)
        else:
            include_parts.append(parse_from_definition(definition))

    return (include_parts, diff_arg)


def parse_union_definition(definition: Dict[str, Any]) -> SelectionSpec:
    union_def_parts = _get_list_dicts(definition, "union")
    include, exclude = _parse_include_exclude_subdefs(union_def_parts)

    union = SelectionUnion(components=include)

    if exclude is None:
        union.raw = definition
        return union
    else:
        return SelectionDifference(components=[union, exclude], raw=definition)


def parse_intersection_definition(definition: Dict[str, Any]) -> SelectionSpec:
    intersection_def_parts = _get_list_dicts(definition, "intersection")
    include, exclude = _parse_include_exclude_subdefs(intersection_def_parts)
    intersection = SelectionIntersection(components=include)

    if exclude is None:
        intersection.raw = definition
        return intersection
    else:
        return SelectionDifference(components=[intersection, exclude], raw=definition)


def parse_dict_definition(definition: Dict[str, Any]) -> SelectionSpec:
    diff_arg: Optional[SelectionSpec] = None
    if len(definition) == 1:
        key = list(definition)[0]
        value = definition[key]
        if not isinstance(key, str):
            raise ValidationException(
                f'Expected definition key to be a "str", got one of type ' f'"{type(key)}" ({key})'
            )
        dct = {
            "method": key,
            "value": value,
        }
    elif "method" in definition and "value" in definition:
        dct = definition
        if "exclude" in definition:
            diff_arg = _parse_exclusions(definition)
            dct = {k: v for k, v in dct.items() if k != "exclude"}
    else:
        raise ValidationException(
            f'Expected either 1 key or else "method" '
            f'and "value" keys, but got {list(definition)}'
        )

    # if key isn't a valid method name, this will raise
    base = SelectionCriteria.selection_criteria_from_dict(definition, dct)
    if diff_arg is None:
        return base
    else:
        return SelectionDifference(components=[base, diff_arg])


def parse_from_definition(definition: RawDefinition, rootlevel=False) -> SelectionSpec:

    if (
        isinstance(definition, dict)
        and ("union" in definition or "intersection" in definition)
        and rootlevel
        and len(definition) > 1
    ):
        keys = ",".join(definition.keys())
        raise ValidationException(
            f"Only a single 'union' or 'intersection' key is allowed "
            f"in a root level selector definition; found {keys}."
        )
    if isinstance(definition, str):
        return SelectionCriteria.from_single_spec(definition)
    elif "union" in definition:
        return parse_union_definition(definition)
    elif "intersection" in definition:
        return parse_intersection_definition(definition)
    elif isinstance(definition, dict):
        return parse_dict_definition(definition)
    else:
        raise ValidationException(
            f"Expected to find union, intersection, str or dict, instead "
            f"found {type(definition)}: {definition}"
        )


def parse_from_selectors_definition(
    source: SelectorFile,
) -> Dict[str, Dict[str, Union[SelectionSpec, bool]]]:
    result: Dict[str, Dict[str, Union[SelectionSpec, bool]]] = {}
    selector: SelectorDefinition
    for selector in source.selectors:
        result[selector.name] = {
            "default": selector.default,
            "definition": parse_from_definition(selector.definition, rootlevel=True),
        }
    return result
