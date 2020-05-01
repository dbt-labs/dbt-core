from .util import (
    assert_has_threads,
    get_querier,
    ProjectDefinition,
)


def test_rpc_compile_threads(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'}
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )
    with querier_ctx as querier:
        results = querier.async_wait_for_result(querier.compile(threads=5))
        assert_has_threads(results, 5)

        results = querier.async_wait_for_result(
            querier.cli_args('compile --threads=7')
        )
        assert_has_threads(results, 7)
