from dataclasses import replace

import pytest

from dbt.adapters.base import BaseRelation
from dbt.contracts.relation import RelationType


@pytest.mark.parametrize(
    "relation_type,result",
    [
        (RelationType.View, True),
        (RelationType.External, False),
    ],
)
def test_can_be_renamed(relation_type, result):
    my_relation = BaseRelation.create(type=relation_type)
    my_relation = replace(my_relation, renameable_relations=frozenset({RelationType.View}))
    assert my_relation.can_be_renamed is result


def test_can_be_renamed_default():
    my_relation = BaseRelation.create(type=RelationType.View)
    assert my_relation.can_be_renamed is False


@pytest.mark.parametrize(
    "relation_type,result",
    [
        (RelationType.View, True),
        (RelationType.External, False),
    ],
)
def test_can_be_replaced(relation_type, result):
    my_relation = BaseRelation.create(type=relation_type)
    my_relation = replace(my_relation, replaceable_relations=frozenset({RelationType.View}))
    assert my_relation.can_be_replaced is result


def test_can_be_replaced_default():
    my_relation = BaseRelation.create(type=RelationType.View)
    assert my_relation.can_be_replaced is False


@pytest.mark.parametrize(
    "limit,expected_result",
    [
        (None, '"test_database"."test_schema"."test_identifier"'),
        (
            0,
            '(select * from "test_database"."test_schema"."test_identifier" where false limit 0) _dbt_limit_subq',
        ),
        (
            1,
            '(select * from "test_database"."test_schema"."test_identifier" limit 1) _dbt_limit_subq',
        ),
    ],
)
def test_render_limited(limit, expected_result):
    my_relation = BaseRelation.create(
        database="test_database",
        schema="test_schema",
        identifier="test_identifier",
        limit=limit,
    )
    actual_result = my_relation.render_limited()
    assert actual_result == expected_result
    assert str(my_relation) == expected_result
