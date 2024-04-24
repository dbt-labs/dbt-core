from dbt.adapters.postgres.relation import PostgresRelation
from dbt.contracts.relation import RelationType


def test_renameable_relation():
    relation = PostgresRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_table",
        type=RelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
        {
            RelationType.View,
            RelationType.Table,
        }
    )
