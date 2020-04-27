import yaml
from .util import (
    assert_has_threads,
    get_querier,
    ProjectDefinition,
)


def test_rpc_test_threads(
    project_root, profiles_root, postgres_profile, unique_schema
):
    schema_yaml = {
        'version': 2,
        'models': [{
            'name': 'my_model',
            'columns': [
                {
                    'name': 'id',
                    'tests': ['not_null', 'unique'],
                },
            ],
        }],
    }
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
            'schema.yml': yaml.safe_dump(schema_yaml)}
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )
    with querier_ctx as querier:
        # first run dbt to get the model built
        querier.async_wait_for_result(querier.run())

        results = querier.async_wait_for_result(querier.test(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('test --threads=7')
        )
        assert_has_threads(results, 7)
