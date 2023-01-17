from typing import List, Optional
import pytest

from dbt.tests.util import run_dbt


_DEFAULT_CHANGE_RELATION_TYPE_MODEL = """
{{ config(materialized=var('materialized')) }}

select '{{ var("materialized") }}' as materialization

{% if var('materialized') == 'incremental' and is_incremental() %}
    where 'abc' != (select max(materialization) from {{ this }})
{% endif %}
"""


class BaseChangeRelationTypeValidator:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_mc_modelface.sql": _DEFAULT_CHANGE_RELATION_TYPE_MODEL}

    def _run_and_check_materialization(self, materialization, extra_args: Optional[List] = None):
        run_args = ["run", "--vars", f"materialized: {materialization}"]
        if extra_args:
            run_args.extend(extra_args)
        results = run_dbt(run_args)
        assert results[0].node.config.materialized == materialization
        assert len(results) == 1

    def test_changing_materialization_changes_relation_type(self, project):
        self._run_and_check_materialization("view")
        self._run_and_check_materialization("table")
        self._run_and_check_materialization("view")
        self._run_and_check_materialization("incremental")
        self._run_and_check_materialization("table", extra_args=["--full-refresh"])


class TestChangeRelationTypes(BaseChangeRelationTypeValidator):
    pass
