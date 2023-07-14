import pytest

from dbt.contracts.relation import RelationType

from dbt.adapters.postgres.relation import PostgresRelation


@pytest.fixture(scope="class")
def my_materialized_view(project) -> PostgresRelation:
    return PostgresRelation.create(
        identifier="my_materialized_view",
        schema=project.test_schema,
        database=project.database,
        type=RelationType.MaterializedView,
    )


@pytest.fixture(scope="class")
def my_view(project) -> PostgresRelation:
    return PostgresRelation.create(
        identifier="my_view",
        schema=project.test_schema,
        database=project.database,
        type=RelationType.View,
    )


@pytest.fixture(scope="class")
def my_table(project) -> PostgresRelation:
    return PostgresRelation.create(
        identifier="my_table",
        schema=project.test_schema,
        database=project.database,
        type=RelationType.Table,
    )


@pytest.fixture(scope="class")
def my_seed(project) -> PostgresRelation:
    return PostgresRelation.create(
        identifier="my_seed",
        schema=project.test_schema,
        database=project.database,
        type=RelationType.Table,
    )
