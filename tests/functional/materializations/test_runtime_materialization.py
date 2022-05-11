import pytest

from dbt.tests.util import run_dbt, check_relations_equal, check_table_does_not_exist

models__view_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ this.schema }}.seed

{% if is_incremental() %}
    {% do exceptions.raise_compiler_error("is_incremental() evaluated to True in a view") %}
{% endif %}

"""

models__incremental_sql = """
{{
  config(
    materialized = "incremental"
  )
}}

select * from {{ this.schema }}.seed

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}

"""

models__materialized_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

{% if is_incremental() %}
    {% do exceptions.raise_compiler_error("is_incremental() evaluated to True in a table") %}
{% endif %}

"""

seeds__seed_csv = """id,first_name,last_name,email,gender,ip_address
1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
6,Jacqueline,Griffin,jgriffin5@t.co,Female,16.13.192.220
7,Wanda,Arnold,warnold6@google.nl,Female,232.116.150.64
8,Craig,Ortiz,cortiz7@sciencedaily.com,Male,199.126.106.13
9,Gary,Day,gday8@nih.gov,Male,35.81.68.186
10,Rose,Wright,rwright9@yahoo.co.jp,Female,236.82.178.100
"""

invalidate_incremental_sql = """
insert into {schema}.incremental (first_name, last_name, email, gender, ip_address) values
    ('Hank', 'Hund', 'hank@yahoo.com', 'Male', '101.239.70.175');
"""

update_sql = """
-- create a view on top of the models
create view {schema}.dependent_view as (

    select count(*) from {schema}.materialized
    union all
    select count(*) from {schema}.view
    union all
    select count(*) from {schema}.incremental

);

insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (101, 'Michael', 'Perez', 'mperez0@chronoengine.com', 'Male', '106.239.70.175');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (102, 'Shawn', 'Mccoy', 'smccoy1@reddit.com', 'Male', '24.165.76.182');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (103, 'Kathleen', 'Payne', 'kpayne2@cargocollective.com', 'Female', '113.207.168.106');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (104, 'Jimmy', 'Cooper', 'jcooper3@cargocollective.com', 'Male', '198.24.63.114');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (105, 'Katherine', 'Rice', 'krice4@typepad.com', 'Female', '36.97.186.238');
"""

create_view__dbt_tmp_sql = """
create view {schema}.view__dbt_tmp as (
    select 1 as id
);
"""

create_view__dbt_backup_sql = """
create view {schema}.view__dbt_backup as (
    select 1 as id
);
"""

create_incremental__dbt_tmp_sql = """
create table {schema}.incremental__dbt_tmp as (
    select 1 as id
);
"""


@pytest.fixture(scope="class")
def models():
    return {
        "view.sql": models__view_sql,
        "incremental.sql": models__incremental_sql,
        "materialized.sql": models__materialized_sql,
    }


@pytest.fixture(scope="class")
def seeds():
    return {"seed.csv": seeds__seed_csv}


@pytest.fixture(scope="class", autouse=True)
def setup(project):
    run_dbt(["seed"])


class TestRuntimeMaterialization:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            }
        }

    def test_full_refresh(
        self,
        project,
    ):
        # initial full-refresh should have no effect
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "view", "incremental", "materialized"])

        # adds one record to the incremental model. full-refresh should truncate then re-run
        project.run_sql(invalidate_incremental_sql)
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 3
        check_relations_equal(project.adapter, ["seed", "incremental"])

        project.run_sql(update_sql)

        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "view", "incremental", "materialized"])

    def test_delete_dbt_tmp_relation(
        self,
        project,
    ):
        # This creates a __dbt_tmp view - make sure it doesn't interfere with the dbt run
        project.run_sql(create_view__dbt_tmp_sql)
        results = run_dbt(["run", "--model", "view"])
        assert len(results) == 1

        check_table_does_not_exist(project.adapter, "view__dbt_tmp")
        check_relations_equal(project.adapter, ["seed", "view"])

        # Again, but with a __dbt_backup view
        project.run_sql(create_view__dbt_backup_sql)
        results = run_dbt(["run", "--model", "view"])
        assert len(results) == 1

        check_table_does_not_exist(project.adapter, "view__dbt_backup")
        check_relations_equal(project.adapter, ["seed", "view"])

        # Again, but against the incremental materialization
        results = run_dbt(["run", "--model", "incremental"])
        project.run_sql(create_incremental__dbt_tmp_sql)
        assert len(results) == 1

        results = run_dbt(["run", "--model", "incremental", "--full-refresh"])
        assert len(results) == 1

        check_table_does_not_exist(project.adapter, "incremental__dbt_tmp")
        check_relations_equal(project.adapter, ["seed", "incremental"])


# Run same tests with models configured with full_refresh
class TestRuntimeMaterializationWithConfig(TestRuntimeMaterialization):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
            "models": {"full_refresh": True},
        }
