import pytest
from dbt.tests.util import run_dbt, check_relations_equal
from collections import namedtuple


models__delete_insert_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

seeds__expected_delete_insert_incremental_predicates_csv = """id,msg,color
1,hey,blue
2,goodbye,red
2,yo,green
3,anyway,purple
"""

ResultHolder = namedtuple(
    "ResultHolder",
    [
        "seed_count",
        "model_count",
        "seed_rows",
        "inc_test_model_count",
        "opt_model_count",
        "relation",
    ],
)


class BaseIncrementalPredicates:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_delete_insert_incremental_predicates.csv": seeds__expected_delete_insert_incremental_predicates_csv
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["id != 2"],
                "+incremental_strategy": "delete+insert",
            }
        }

    def update_incremental_model(self, incremental_model):
        """update incremental model after the seed table has been updated"""
        model_result_set = run_dbt(["run", "--select", incremental_model])
        return len(model_result_set)

    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):

        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}.{}".format(project.test_schema, seed)
        # project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )

    def check_scenario_correctness(self, expected_fields, test_case_fields, project):
        """Invoke assertions to verify correct build functionality"""
        # 1. test seed(s) should build afresh
        assert expected_fields.seed_count == test_case_fields.seed_count
        # 2. test model(s) should build afresh
        assert expected_fields.model_count == test_case_fields.model_count
        # 3. seeds should have intended row counts post update
        assert expected_fields.seed_rows == test_case_fields.seed_rows
        # 4. incremental test model(s) should be updated
        assert expected_fields.inc_test_model_count == test_case_fields.inc_test_model_count
        # 5. extra incremental model(s) should be built; optional since
        #   comparison may be between an incremental model and seed
        if expected_fields.opt_model_count and test_case_fields.opt_model_count:
            assert expected_fields.opt_model_count == test_case_fields.opt_model_count
        # 6. result table should match intended result set (itself a relation)
        check_relations_equal(
            project.adapter, [expected_fields.relation, test_case_fields.relation]
        )

    def get_expected_fields(self, relation, seed_rows, opt_model_count=None):
        return ResultHolder(
            seed_count=1,
            model_count=1,
            inc_test_model_count=1,
            seed_rows=seed_rows,
            opt_model_count=opt_model_count,
            relation=relation,
        )

    # no unique_key test
    def test__incremental_predicates(self, project):
        """seed should match model after two incremental runs"""

        expected_fields = self.get_expected_fields(
            relation="expected_delete_insert_incremental_predicates", seed_rows=4
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="expected_delete_insert_incremental_predicates",
            incremental_model="delete_insert_incremental_predicates",
            update_sql_file=None,
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)


class TestIncrementalPredicatesDeleteInsert(BaseIncrementalPredicates):
    pass


class TestPredicatesDeleteInsert(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+predicates": ["id != 2"], "+incremental_strategy": "delete+insert"}}
