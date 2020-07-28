from concurrent.futures import ThreadPoolExecutor, as_completed
import pytest

from .util import (
    get_querier,
    ProjectDefinition,
)


def _compile_poll_for_result(querier, id: int):
    sql = f'select {id} as id'
    resp = querier.compile_sql(
        request_id=id, sql=sql, name=f'query_{id}'
    )
    compile_sql_result = querier.async_wait_for_result(resp)
    assert compile_sql_result['results'][0]['compiled_sql'] == sql


@pytest.mark.supported('postgres')
def test_rpc_compile_sql_concurrency(
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
        values = {}
        with ThreadPoolExecutor(max_workers=10) as tpe:
            for id in range(20):
                fut = tpe.submit(_compile_poll_for_result, querier, id)
                values[fut] = id
            for fut in as_completed(values):
                fut.result()
