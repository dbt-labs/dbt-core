from dbt.graph import (
    cli,
    SelectionUnion,
    SelectionIntersection,
    SelectionDifference,
    SelectionCriteria,
)
from dbt.graph.selector_methods import MethodName
import textwrap
import yaml

from dbt.contracts.selection import SelectorFile


def parse_file(txt: str) -> SelectorFile:
    txt = textwrap.dedent(txt)
    dct = yaml.safe_load(txt)
    sf = SelectorFile.from_dict(dct)
    return sf


class Union:
    def __init__(self, *args):
        self.components = args

    def __str__(self):
        return f"Union(components={self.components})"

    def __repr__(self):
        return f"Union(components={self.components!r})"

    def __eq__(self, other):
        if not isinstance(other, SelectionUnion):
            return False

        return all(mine == theirs for mine, theirs in zip(self.components, other.components))


class Intersection:
    def __init__(self, *args):
        self.components = args

    def __str__(self):
        return f"Intersection(components={self.components})"

    def __repr__(self):
        return f"Intersection(components={self.components!r})"

    def __eq__(self, other):
        if not isinstance(other, SelectionIntersection):
            return False

        return all(mine == theirs for mine, theirs in zip(self.components, other.components))


class Difference:
    def __init__(self, *args):
        self.components = args

    def __str__(self):
        return f"Difference(components={self.components})"

    def __repr__(self):
        return f"Difference(components={self.components!r})"

    def __eq__(self, other):
        if not isinstance(other, SelectionDifference):
            return False

        return all(mine == theirs for mine, theirs in zip(self.components, other.components))


class Criteria:
    def __init__(self, method, value, **kwargs):
        self.method = method
        self.value = value
        self.kwargs = kwargs

    def __str__(self):
        return f"Criteria(method={self.method}, value={self.value}, **{self.kwargs})"

    def __repr__(self):
        return f"Criteria(method={self.method!r}, value={self.value!r}, **{self.kwargs!r})"

    def __eq__(self, other):
        if not isinstance(other, SelectionCriteria):
            return False
        return (
            self.method == other.method
            and self.value == other.value
            and all(getattr(other, k) == v for k, v in self.kwargs.items())
        )


def test_parse_simple():
    sf = parse_file(
        """\
        selectors:
          - name: tagged_foo
            description: Selector for foo-tagged models
            definition:
              tag: foo
    """
    )

    assert len(sf.selectors) == 1
    assert sf.selectors[0].description == "Selector for foo-tagged models"
    parsed = cli.parse_from_selectors_definition(sf)
    assert len(parsed) == 1
    assert "tagged_foo" in parsed
    assert (
        Criteria(
            method=MethodName.Tag,
            method_arguments=[],
            value="foo",
            children=False,
            parents=False,
            childrens_parents=False,
            children_depth=None,
            parents_depth=None,
        )
        == parsed["tagged_foo"]["definition"]
    )


def test_parse_simple_childrens_parents():
    sf = parse_file(
        """\
        selectors:
          - name: tagged_foo
            definition:
              method: tag
              value: foo
              childrens_parents: True
    """
    )

    assert len(sf.selectors) == 1
    parsed = cli.parse_from_selectors_definition(sf)
    assert len(parsed) == 1
    assert "tagged_foo" in parsed
    assert (
        Criteria(
            method=MethodName.Tag,
            method_arguments=[],
            value="foo",
            children=False,
            parents=False,
            childrens_parents=True,
            children_depth=None,
            parents_depth=None,
        )
        == parsed["tagged_foo"]["definition"]
    )


def test_parse_simple_arguments_with_modifiers():
    sf = parse_file(
        """\
        selectors:
          - name: configured_view
            definition:
              method: config.materialized
              value: view
              parents: True
              children: True
              children_depth: 2
    """
    )

    assert len(sf.selectors) == 1
    parsed = cli.parse_from_selectors_definition(sf)
    assert len(parsed) == 1
    assert "configured_view" in parsed
    assert (
        Criteria(
            method=MethodName.Config,
            method_arguments=["materialized"],
            value="view",
            children=True,
            parents=True,
            childrens_parents=False,
            children_depth=2,
            parents_depth=None,
        )
        == parsed["configured_view"]["definition"]
    )


def test_parse_union():
    sf = parse_file(
        """\
        selectors:
            - name: views-or-foos
              definition:
                union:
                  - method: config.materialized
                    value: view
                  - tag: foo
    """
    )
    assert len(sf.selectors) == 1
    parsed = cli.parse_from_selectors_definition(sf)
    assert "views-or-foos" in parsed
    assert (
        Union(
            Criteria(method=MethodName.Config, value="view", method_arguments=["materialized"]),
            Criteria(method=MethodName.Tag, value="foo", method_arguments=[]),
        )
        == parsed["views-or-foos"]["definition"]
    )


def test_parse_intersection():
    sf = parse_file(
        """\
        selectors:
            - name: views-and-foos
              definition:
                intersection:
                  - method: config.materialized
                    value: view
                  - tag: foo
    """
    )
    assert len(sf.selectors) == 1
    parsed = cli.parse_from_selectors_definition(sf)

    assert "views-and-foos" in parsed
    assert (
        Intersection(
            Criteria(method=MethodName.Config, value="view", method_arguments=["materialized"]),
            Criteria(method=MethodName.Tag, value="foo", method_arguments=[]),
        )
        == parsed["views-and-foos"]["definition"]
    )


def test_parse_union_excluding():
    sf = parse_file(
        """\
        selectors:
            - name: views-or-foos-not-bars
              definition:
                union:
                  - method: config.materialized
                    value: view
                  - tag: foo
                  - exclude:
                    - tag: bar
    """
    )
    assert len(sf.selectors) == 1
    parsed = cli.parse_from_selectors_definition(sf)
    assert "views-or-foos-not-bars" in parsed
    assert (
        Difference(
            Union(
                Criteria(
                    method=MethodName.Config, value="view", method_arguments=["materialized"]
                ),
                Criteria(method=MethodName.Tag, value="foo", method_arguments=[]),
            ),
            Criteria(method=MethodName.Tag, value="bar", method_arguments=[]),
        )
        == parsed["views-or-foos-not-bars"]["definition"]
    )


def test_parse_yaml_complex():
    sf = parse_file(
        """\
        selectors:
            - name: test_name
              definition:
                union:
                - intersection:
                  - tag: foo
                  - tag: bar
                  - union:
                    - package: snowplow
                    - config.materialized: incremental
                - union:
                  - path: "models/snowplow/marketing/custom_events.sql"
                  - fqn: "snowplow.marketing"
                - intersection:
                  - resource_type: seed
                  - package: snowplow
                  - exclude:
                    - country_codes
                    - intersection:
                      - tag: baz
                      - config.materialized: ephemeral
            - name: weeknights
              definition:
                union:
                - tag: nightly
                - tag:weeknights_only
        """
    )

    assert len(sf.selectors) == 2
    parsed = cli.parse_from_selectors_definition(sf)
    assert "test_name" in parsed
    assert "weeknights" in parsed
    assert (
        Union(
            Criteria(method=MethodName.Tag, value="nightly"),
            Criteria(method=MethodName.Tag, value="weeknights_only"),
        )
        == parsed["weeknights"]["definition"]
    )

    assert (
        Union(
            Intersection(
                Criteria(method=MethodName.Tag, value="foo"),
                Criteria(method=MethodName.Tag, value="bar"),
                Union(
                    Criteria(method=MethodName.Package, value="snowplow"),
                    Criteria(
                        method=MethodName.Config,
                        value="incremental",
                        method_arguments=["materialized"],
                    ),
                ),
            ),
            Union(
                Criteria(
                    method=MethodName.Path, value="models/snowplow/marketing/custom_events.sql"
                ),
                Criteria(method=MethodName.FQN, value="snowplow.marketing"),
            ),
            Difference(
                Intersection(
                    Criteria(method=MethodName.ResourceType, value="seed"),
                    Criteria(method=MethodName.Package, value="snowplow"),
                ),
                Union(
                    Criteria(method=MethodName.FQN, value="country_codes"),
                    Intersection(
                        Criteria(method=MethodName.Tag, value="baz"),
                        Criteria(
                            method=MethodName.Config,
                            value="ephemeral",
                            method_arguments=["materialized"],
                        ),
                    ),
                ),
            ),
        )
        == parsed["test_name"]["definition"]
    )


def test_parse_selection():
    sf = parse_file(
        """\
        selectors:
            - name: default
              definition:
                union:
                  - tag: foo
                  - tag: bar
            - name: inherited
              definition:
                method: selector
                value: default
    """
    )
    assert len(sf.selectors) == 2
    parsed = cli.parse_from_selectors_definition(sf)
    assert "default" in parsed
    assert "inherited" in parsed
    assert (
        Union(
            Criteria(method=MethodName.Tag, value="foo"),
            Criteria(method=MethodName.Tag, value="bar"),
        )
        == parsed["default"]["definition"]
    )
    assert (
        Union(
            Criteria(method=MethodName.Tag, value="foo"),
            Criteria(method=MethodName.Tag, value="bar"),
        )
        == parsed["inherited"]["definition"]
    )


def test_parse_selection_with_exclusion():
    sf = parse_file(
        """\
        selectors:
            - name: default
              definition:
                union:
                  - tag: foo
                  - tag: bar
            - name: inherited
              definition:
                union:
                  - method: selector
                    value: default
                  - exclude:
                    - tag: bar
    """
    )
    assert len(sf.selectors) == 2
    parsed = cli.parse_from_selectors_definition(sf)
    assert "default" in parsed
    assert "inherited" in parsed
    assert (
        Union(
            Criteria(method=MethodName.Tag, value="foo"),
            Criteria(method=MethodName.Tag, value="bar"),
        )
        == parsed["default"]["definition"]
    )
    assert (
        Difference(
            Union(
                Union(
                    Criteria(method=MethodName.Tag, value="foo"),
                    Criteria(method=MethodName.Tag, value="bar"),
                )
            ),
            Criteria(method=MethodName.Tag, value="bar"),
        )
        == parsed["inherited"]["definition"]
    )
