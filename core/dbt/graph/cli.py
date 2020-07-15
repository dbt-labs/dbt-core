# special support for CLI argument parsing.
import itertools

from typing import (
    List, Optional
)

from .selector_spec import (
    SelectionUnion,
    SelectionSpec,
    SelectionIntersection,
    SelectionDifference,
    SelectionCriteria,
)

INTERSECTION_DELIMITER = ','

DEFAULT_INCLUDES: List[str] = ['fqn:*', 'source:*']
DEFAULT_EXCLUDES: List[str] = []
DATA_TEST_SELECTOR: str = 'test_type:data'
SCHEMA_TEST_SELECTOR: str = 'test_type:schema'


def parse_union(
    components: List[str], expect_exists: bool
) -> SelectionUnion:
    # turn ['a b', 'c'] -> ['a', 'b', 'c']
    raw_specs = itertools.chain.from_iterable(
        r.split(' ') for r in components
    )
    union_components: List[SelectionSpec] = []

    # ['a', 'b', 'c,d'] -> union('a', 'b', intersection('c', 'd'))
    for raw_spec in raw_specs:
        intersection_components: List[SelectionSpec] = [
            SelectionCriteria.from_single_spec(part)
            for part in raw_spec.split(INTERSECTION_DELIMITER)
        ]
        union_components.append(SelectionIntersection(
            components=intersection_components,
            expect_exists=expect_exists,
            raw=raw_spec,
        ))

    return SelectionUnion(
        components=union_components,
        expect_exists=False,
        raw=components,
    )


def parse_union_from_default(
    raw: Optional[List[str]], default: List[str]
) -> SelectionUnion:
    components: List[str]
    expect_exists: bool
    if raw is None:
        return parse_union(components=default, expect_exists=False)
    else:
        return parse_union(components=raw, expect_exists=True)


def parse_difference(
    include: Optional[List[str]], exclude: Optional[List[str]]
) -> SelectionDifference:
    included = parse_union_from_default(include, DEFAULT_INCLUDES)
    excluded = parse_union_from_default(exclude, DEFAULT_EXCLUDES)
    return SelectionDifference(components=[included, excluded])


def parse_test_selectors(
    data: bool, schema: bool, base: SelectionSpec
) -> SelectionSpec:
    union_components = []

    if data:
        union_components.append(
            SelectionCriteria.from_single_spec(DATA_TEST_SELECTOR)
        )
    if schema:
        union_components.append(
            SelectionCriteria.from_single_spec(SCHEMA_TEST_SELECTOR)
        )

    intersect_with: SelectionSpec
    if not union_components:
        return base
    elif len(union_components) == 1:
        intersect_with = union_components[0]
    else:  # data and schema tests
        intersect_with = SelectionUnion(
            components=union_components,
            expect_exists=True,
            raw=[DATA_TEST_SELECTOR, SCHEMA_TEST_SELECTOR],
        )

    return SelectionIntersection(
        components=[base, intersect_with], expect_exists=True
    )
