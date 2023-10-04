import pytest
from dbt.tests.util import run_dbt, write_file, get_manifest, get_artifact
from dbt.exceptions import DuplicateResourceNameError, YamlParseDictError, ParsingError

my_model_sql = """
SELECT
a+b as c,
concat(string_a, string_b) as string_c,
not_testing, date_a,
{{ dbt.string_literal(type_numeric()) }} as macro_call,
{{ dbt.string_literal(var('my_test')) }} as var_call,
{{ dbt.string_literal(env_var('MY_TEST', 'default')) }} as env_var_call,
{{ dbt.string_literal(invocation_id) }} as invocation_id
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id
"""

my_model_a_sql = """
SELECT
1 as a,
1 as id,
2 as not_testing,
'a' as string_a,
DATE '2020-01-02' as date_a
"""

my_model_b_sql = """
SELECT
2 as b,
1 as id,
2 as c,
'b' as string_b
"""

test_my_model_yml = """
unit:
  - model: my_model
    tests:
      - name: test_my_model
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, a: 1}
          - input: ref('my_model_b')
            rows:
              - {id: 1, b: 2}
              - {id: 2, b: 2}
        expect:
          rows:
            - {c: 2}

      - name: test_my_model_empty
        given:
          - input: ref('my_model_a')
            rows: []
          - input: ref('my_model_b')
            rows:
              - {id: 1, b: 2}
              - {id: 2, b: 2}
        expect:
          rows: []
      - name: test_my_model_overrides
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, a: 1}
          - input: ref('my_model_b')
            rows:
              - {id: 1, b: 2}
              - {id: 2, b: 2}
        overrides:
          macros:
            type_numeric: override
            invocation_id: 123
          vars:
            my_test: var_override
          env_vars:
            MY_TEST: env_var_override
        expect:
          rows:
            - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}
      - name: test_my_model_string_concat
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, string_a: a}
          - input: ref('my_model_b')
            rows:
              - {id: 1, string_b: b}
        expect:
          rows:
            - {string_c: ab}
        config:
           tags: test_this
"""

datetime_test = """
      - name: test_my_model_datetime
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, date_a: "2020-01-01"}
          - input: ref('my_model_b')
            rows:
              - {id: 1}
        expect:
          rows:
            - {date_a: "2020-01-01"}
"""


class TestUnitTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def test_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Test select by test name
        results = run_dbt(["unit-test", "--select", "test_name:test_my_model_string_concat"])
        assert len(results) == 1

        # Select, method not specified
        results = run_dbt(["unit-test", "--select", "test_my_model_overrides"])
        assert len(results) == 1

        # Select using tag
        results = run_dbt(["unit-test", "--select", "tag:test_this"])
        assert len(results) == 1

        # Partial parsing... remove test
        write_file(test_my_model_yml, project.project_root, "models", "test_my_model.yml")
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 4

        # Partial parsing... put back removed test
        write_file(
            test_my_model_yml + datetime_test, project.project_root, "models", "test_my_model.yml"
        )
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        manifest = get_manifest(project.project_root)
        assert len(manifest.unit_tests) == 5
        # Every unit test has a depends_on to the model it tests
        for unit_test_definition in manifest.unit_tests.values():
            assert unit_test_definition.depends_on.nodes[0] == "model.test.my_model"

        # We should have a UnitTestNode for every test, plus two input models for each test
        unit_test_manifest = get_artifact(
            project.project_root, "target", "unit_test_manifest.json"
        )
        assert len(unit_test_manifest["nodes"]) == 15

        # Check for duplicate unit test name
        write_file(
            test_my_model_yml + datetime_test + datetime_test,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["unit-test", "--select", "my_model"])


test_my_model_csv_yml = """
unit:
  - model: my_model
    tests:
      - name: test_my_model
        given:
          - input: ref('my_model_a')
            format: csv
            rows: |
              id,a
              1,1
          - input: ref('my_model_b')
            format: csv
            rows: |
              id,b
              1,2
              2,2
        expect:
          format: csv
          rows: |
            c
            2

      - name: test_my_model_empty
        given:
          - input: ref('my_model_a')
            rows: []
          - input: ref('my_model_b')
            format: csv
            rows: |
              id,b
              1,2
              2,2
        expect:
          rows: []
      - name: test_my_model_overrides
        given:
          - input: ref('my_model_a')
            format: csv
            rows: |
              id,a
              1,1
          - input: ref('my_model_b')
            format: csv
            rows: |
              id,b
              1,2
              2,2
        overrides:
          macros:
            type_numeric: override
            invocation_id: 123
          vars:
            my_test: var_override
          env_vars:
            MY_TEST: env_var_override
        expect:
          rows:
            - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}
      - name: test_my_model_string_concat
        given:
          - input: ref('my_model_a')
            format: csv
            rows: |
              id,string_a
              1,a
          - input: ref('my_model_b')
            format: csv
            rows: |
              id,string_b
              1,b
        expect:
          format: csv
          rows: |
            string_c
            ab
        config:
           tags: test_this
"""

datetime_test_invalid_format = """
      - name: test_my_model_datetime
        given:
          - input: ref('my_model_a')
            format: xxxx
            rows:
              - {id: 1, date_a: "2020-01-01"}
          - input: ref('my_model_b')
            rows:
              - {id: 1}
        expect:
          rows:
            - {date_a: "2020-01-01"}
"""

datetime_test_invalid_format2 = """
      - name: test_my_model_datetime
        given:
          - input: ref('my_model_a')
            format: csv
            rows:
              - {id: 1, date_a: "2020-01-01"}
          - input: ref('my_model_b')
            rows:
              - {id: 1}
        expect:
          rows:
            - {date_a: "2020-01-01"}
"""


class TestUnitTestsWithInlineCSV:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_csv_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def test_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Check error with invalid format
        write_file(
            test_my_model_csv_yml + datetime_test_invalid_format,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(YamlParseDictError):
            results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)

        # Check error with format not matching rows
        write_file(
            test_my_model_csv_yml + datetime_test_invalid_format2,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
