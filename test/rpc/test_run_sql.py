import pytest

from .util import (
    get_querier,
    ProjectDefinition,
)


@pytest.mark.supported('any')
def test_rpc_run_sql_nohang(
    project_root, profiles_root, dbt_profile, unique_schema
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
        querier.async_wait_for_result(querier.run_sql('select 1 as id'))


@pytest.mark.supported('snowflake')
def test_snowflake_rpc_run_sql_keepalive_nohang(
    project_root, profiles_root, dbt_profile, unique_schema
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
        target='keepalives',
    )
    with querier_ctx as querier:
        querier.async_wait_for_result(querier.run_sql('select 1 as id'))
