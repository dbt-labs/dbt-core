import pytest

from dbt.tests.util import run_dbt

models__sample_model_sql = """
select * from {{ source("raw", "sample_seed") }}
"""

models__schema_yml = """
version: 2
sources:
  - name: raw
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: sample_seed
        columns:
          - name: email
            tests:
              - not_null:
                  severity: "{{ 'error' if var('strict', false) else 'warn' }}"
models:
  - name: sample_model
    columns:
      - name: email
        tests:
          - not_null:
              severity: "{{ 'error' if var('strict', false) else 'warn' }}"
"""

seeds__sample_seed_csv = """id,first_name,last_name,email,gender,ip_address,updated_at
1,Judith,Kennedy,jkennedy0@phpbb.com,Female,54.60.24.128,2015-12-24 12:19:28
2,Arthur,Kelly,akelly1@eepurl.com,Male,62.56.24.215,2015-10-28 16:22:15
3,Rachel,Moreno,rmoreno2@msu.edu,Female,31.222.249.23,2016-04-05 02:05:30
4,Ralph,Turner,rturner3@hp.com,Male,157.83.76.114,2016-08-08 00:06:51
5,Laura,Gonzales,lgonzales4@howstuffworks.com,Female,30.54.105.168,2016-09-01 08:25:38
6,Katherine,Lopez,null,Female,169.138.46.89,2016-08-30 18:52:11
7,Jeremy,Hamilton,jhamilton6@mozilla.org,Male,231.189.13.133,2016-07-17 02:09:46
8,Heather,Rose,hrose7@goodreads.com,Female,87.165.201.65,2015-12-29 22:03:56
9,Gregory,Kelly,gkelly8@trellian.com,Male,154.209.99.7,2016-03-24 21:18:16
10,Rachel,Lopez,rlopez9@themeforest.net,Female,237.165.82.71,2016-08-20 15:44:49
11,Donna,Welch,dwelcha@shutterfly.com,Female,103.33.110.138,2016-02-27 01:41:48
12,Russell,Lawrence,rlawrenceb@qq.com,Male,189.115.73.4,2016-06-11 03:07:09
13,Michelle,Montgomery,mmontgomeryc@scientificamerican.com,Female,243.220.95.82,2016-06-18 16:27:19
14,Walter,Castillo,null,Male,71.159.238.196,2016-10-06 01:55:44
15,Robin,Mills,rmillse@vkontakte.ru,Female,172.190.5.50,2016-10-31 11:41:21
16,Raymond,Holmes,rholmesf@usgs.gov,Male,148.153.166.95,2016-10-03 08:16:38
17,Gary,Bishop,gbishopg@plala.or.jp,Male,161.108.182.13,2016-08-29 19:35:20
18,Anna,Riley,arileyh@nasa.gov,Female,253.31.108.22,2015-12-11 04:34:27
19,Sarah,Knight,sknighti@foxnews.com,Female,222.220.3.177,2016-09-26 00:49:06
20,Phyllis,Fox,pfoxj@creativecommons.org,Female,163.191.232.95,2016-08-21 10:35:19
"""


tests__sample_test_sql = """
{{ config(severity='error' if var('strict', false) else 'warn') }}
select * from {{ ref("sample_model") }} where email is null
"""


@pytest.fixture(scope="class")
def models():
    return {
        "sample_model.sql": models__sample_model_sql,
        "schema.yml": models__schema_yml
    }


@pytest.fixture(scope="class")
def seeds():
    return {"sample_seed.csv": seeds__sample_seed_csv}


@pytest.fixture(scope="class")
def tests():
    return {"null_email.sql": tests__sample_test_sql}


@pytest.fixture(scope="class")
def project_config_update():
    return {
        'config-version': 2,
        'seed-paths': ['seeds'],
        'test-paths': ['tests'],
        "seeds": {
            "quote_columns": False,
        },
    }


class TestSeverity:
    @pytest.fixture(scope="class", autouse=True)
    def seed_and_run(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def test_generic_default(self, project):
        results = run_dbt(['test', '--select', 'test_type:generic'])
        assert len(results) == 2
        assert all([r.status == 'warn' for r in results])
        assert all([r.failures == 2 for r in results])

    def test_generic_strict(self, project):
        results = run_dbt(['test', '--select', 'test_type:generic', "--vars", '{"strict": True}'], expect_pass=False)
        assert len(results) == 2
        assert all([r.status == 'fail' for r in results])
        assert all([r.failures == 2 for r in results])

    def test_singular_default(self, project):
        results = run_dbt(['test', '--select', 'test_type:singular'])
        assert len(results) == 1
        assert all([r.status == 'warn' for r in results])
        assert all([r.failures == 2 for r in results])

    def test_singular_strict(self, project):
        results = run_dbt(['test', '--select', 'test_type:singular', "--vars", '{"strict": True}'], expect_pass=False)
        assert len(results) == 1
        assert all([r.status == 'fail' for r in results])
        assert all([r.failures == 2 for r in results])
