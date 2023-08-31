from dataclasses import replace

import pytest

from dbt.adapters.base import BaseRelation
from dbt.contracts.relation import RelationType


@pytest.mark.parametrize(
    "relation_type,result",
    [
        (RelationType.View, True),
        (RelationType.Table, False),
    ],
)
def test_can_be_renamed(relation_type, result):
    my_relation = BaseRelation.create(type=relation_type)
    my_relation = replace(my_relation, renameable_relations=[RelationType.View])
    assert my_relation.can_be_renamed is result
