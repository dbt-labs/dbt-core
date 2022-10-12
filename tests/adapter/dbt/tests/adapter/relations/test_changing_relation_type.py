

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
        return {
            "model_mc_modelface.sql": _DEFAULT_CHANGE_RELATION_TYPE_MODEL
        }

    def _run_and_check(self, materialization):
        results = run_dbt(["run", '--vars', f'materialized: {materialization}'])
        assert results[0].node.config.materialized == materialization
        assert len(results) == 1

    def test_changing_materialization_changes_relation_type(self, project):
        self._run_and_check('view')
        self._run_and_check('table')
        self._run_and_check('view')
        self._run_and_check('incremental')


class TestChangeRelationTypes(BaseChangeRelationTypeValidator):
    pass
