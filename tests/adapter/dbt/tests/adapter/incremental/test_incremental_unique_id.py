import pytest
from dbt.tests.util import run_dbt, check_relations_equal
from dbt.contracts.results import RunStatus
from collections import namedtuple
from pathlib import Path

models__trinary_unique_key_list_sql = """
-- a multi-argument unique key list should see overwriting on rows in the model
--   where all unique key fields apply

{{
    config(
        materialized='incremental',
        unique_key=['state', 'county', 'city']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__nontyped_trinary_unique_key_list_sql = """
-- a multi-argument unique key list should see overwriting on rows in the model
--   where all unique key fields apply
--   N.B. needed for direct comparison with seed

{{
    config(
        materialized='incremental',
        unique_key=['state', 'county', 'city']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__unary_unique_key_list_sql = """
-- a one argument unique key list should result in overwritting semantics for
--   that one matching field

{{
    config(
        materialized='incremental',
        unique_key=['state']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__not_found_unique_key_sql = """
-- a model with a unique key not found in the table itself will error out

{{
    config(
        materialized='incremental',
        unique_key='thisisnotacolumn'
    )
}}

select
    *
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__empty_unique_key_list_sql = """
-- model with empty list unique key should build normally

{{
    config(
        materialized='incremental',
        unique_key=[]
    )
}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__no_unique_key_sql = """
-- no specified unique key should cause no special build behavior

{{
    config(
        materialized='incremental'
    )
}}

select
    *
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__empty_str_unique_key_sql = """
-- ensure model with empty string unique key should build normally

{{
    config(
        materialized='incremental',
        unique_key=''
    )
}}

select
    *
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__str_unique_key_sql = """
-- a unique key with a string should trigger to overwrite behavior when
--   the source has entries in conflict (i.e. more than one row per unique key
--   combination)

{{
    config(
        materialized='incremental',
        unique_key='state'
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__duplicated_unary_unique_key_list_sql = """
{{
    config(
        materialized='incremental',
        unique_key=['state', 'state']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

models__not_found_unique_key_list_sql = """
-- a unique key list with any element not in the model itself should error out

{{
    config(
        materialized='incremental',
        unique_key=['state', 'thisisnotacolumn']
    )
}}

select * from {{ ref('seed') }}

"""

models__expected__one_str__overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston','2020-02-12'
union all
select 'NJ','Mercer','Trenton','2022-01-01'
union all
select 'NY','Kings','Brooklyn','2021-04-02'
union all
select 'NY','New York','Manhattan','2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia','2021-05-21'

"""

models__expected__unique_key_list__inplace_overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston','2020-02-12'
union all
select 'NJ','Mercer','Trenton','2022-01-01'
union all
select 'NY','Kings','Brooklyn','2021-04-02'
union all
select 'NY','New York','Manhattan','2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia','2021-05-21'

"""

seeds__duplicate_insert_sql = """
-- Insert statement which when applied to seed.csv triggers the inplace
--   overwrite strategy of incremental models. Seed and incremental model
--   diverge.

-- insert new row, which should not be in incremental model
--  with primary or first three columns unique
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('CT','Hartford','Hartford','2022-02-14');

"""

seeds__seed_csv = """state,county,city,last_visit_date
CT,Hartford,Hartford,2020-09-23
MA,Suffolk,Boston,2020-02-12
NJ,Mercer,Trenton,2022-01-01
NY,Kings,Brooklyn,2021-04-02
NY,New York,Manhattan,2021-04-01
PA,Philadelphia,Philadelphia,2021-05-21
"""

seeds__add_new_rows_sql = """
-- Insert statement which when applied to seed.csv sees incremental model
--   grow in size while not (necessarily) diverging from the seed itself.

-- insert two new rows, both of which should be in incremental model
--   with any unique columns
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('WA','King','Seattle','2022-02-01');

insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('CA','Los Angeles','Los Angeles','2022-02-01');

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


class BaseIncrementalUniqueKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": models__expected__one_str__overwrite_sql,
                "unique_key_list__inplace_overwrite.sql": models__expected__unique_key_list__inplace_overwrite_sql,
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": seeds__duplicate_insert_sql,
            "seed.csv": seeds__seed_csv,
            "add_new_rows.sql": seeds__add_new_rows_sql,
        }

    def update_incremental_model(self, incremental_model):
        """update incremental model after the seed table has been updated"""
        model_result_set = run_dbt(["run", "--select", incremental_model])
        return len(model_result_set)

    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):
        """build a test case and return values for assertions
        [INFO] Models must be in place to test incremental model
        construction and merge behavior. Database touches are side
        effects to extract counts (which speak to health of unique keys)."""
        # idempotently create some number of seeds and incremental models'''

        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}.{}".format(project.test_schema, seed)
        project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
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
            seed_rows=seed_rows,
            inc_test_model_count=1,
            opt_model_count=opt_model_count,
            relation=relation,
        )

    def fail_to_build_inc_missing_unique_key_column(self, incremental_model_name):
        """should pass back error state when trying build an incremental
        model whose unique key or keylist includes a column missing
        from the incremental model"""
        seed_count = len(run_dbt(["seed", "--select", "seed", "--full-refresh"]))  # noqa:F841
        # unique keys are not applied on first run, so two are needed
        run_dbt(
            ["run", "--select", incremental_model_name, "--full-refresh"],
            expect_pass=True,
        )
        run_result = run_dbt(
            ["run", "--select", incremental_model_name], expect_pass=False
        ).results[0]

        return run_result.status, run_result.message

    # no unique_key test
    def test__no_unique_keys(self, project):
        """with no unique keys, seed and model should match"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=8)
        test_case_fields = self.get_test_fields(
            project, seed="seed", incremental_model="no_unique_key", update_sql_file="add_new_rows"
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    # unique_key as str tests
    def test__empty_str_unique_key(self, project):
        """with empty string for unique key, seed and model should match"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=8)
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="empty_str_unique_key",
            update_sql_file="add_new_rows",
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__one_unique_key(self, project):
        """with one unique key, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="one_str__overwrite", seed_rows=7, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="str_unique_key",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("one_str__overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__bad_unique_key(self, project):
        """expect compilation error from unique key not being a column"""

        (status, exc) = self.fail_to_build_inc_missing_unique_key_column(
            incremental_model_name="not_found_unique_key"
        )

        assert status == RunStatus.Error
        assert "thisisnotacolumn" in exc.lower()

    # test unique_key as list
    def test__empty_unique_key_list(self, project):
        """with no unique keys, seed and model should match"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=8)
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="empty_unique_key_list",
            update_sql_file="add_new_rows",
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__unary_unique_key_list(self, project):
        """with one unique key, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="unique_key_list__inplace_overwrite", seed_rows=7, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="unary_unique_key_list",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("unique_key_list__inplace_overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__duplicated_unary_unique_key_list(self, project):
        """with two of the same unique key, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="unique_key_list__inplace_overwrite", seed_rows=7, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="duplicated_unary_unique_key_list",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("unique_key_list__inplace_overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__trinary_unique_key_list(self, project):
        """with three unique keys, model will overwrite existing row"""

        expected_fields = self.get_expected_fields(
            relation="unique_key_list__inplace_overwrite", seed_rows=7, opt_model_count=1
        )
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="trinary_unique_key_list",
            update_sql_file="duplicate_insert",
            opt_model_count=self.update_incremental_model("unique_key_list__inplace_overwrite"),
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__trinary_unique_key_list_no_update(self, project):
        """even with three unique keys, adding distinct rows to seed does not
        cause seed and model to diverge"""

        expected_fields = self.get_expected_fields(relation="seed", seed_rows=8)
        test_case_fields = self.get_test_fields(
            project,
            seed="seed",
            incremental_model="nontyped_trinary_unique_key_list",
            update_sql_file="add_new_rows",
        )
        self.check_scenario_correctness(expected_fields, test_case_fields, project)

    def test__bad_unique_key_list(self, project):
        """expect compilation error from unique key not being a column"""

        (status, exc) = self.fail_to_build_inc_missing_unique_key_column(
            incremental_model_name="not_found_unique_key_list"
        )

        assert status == RunStatus.Error
        assert "thisisnotacolumn" in exc.lower()


class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
    pass
