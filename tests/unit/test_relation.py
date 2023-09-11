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
    assert my_relation.can_be_renamed is result
