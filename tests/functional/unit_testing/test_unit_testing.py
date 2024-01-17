import pytest
from dbt.tests.util import (
    run_dbt,
    write_file,
    get_manifest,
)
from dbt.contracts.results import NodeStatus
from dbt.exceptions import DuplicateResourceNameError, ParsingError
from fixtures import (
    my_model_sql,
    my_model_vars_sql,
    my_model_a_sql,
    my_model_b_sql,
    test_my_model_yml,
    datetime_test,
    my_incremental_model_sql,
    event_sql,
    test_my_model_incremental_yml,
    test_my_model_yml_invalid,
)


class TestUnitTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_vars_sql,
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
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        results = run_dbt(["build", "--select", "my_model"], expect_pass=False)
        assert len(results) == 6
        for result in results:
            if result.node.unique_id == "model.test.my_model":
                result.status == NodeStatus.Skipped

        # Test select by test name
        results = run_dbt(["test", "--select", "test_name:test_my_model_string_concat"])
        assert len(results) == 1

        # Select, method not specified
        results = run_dbt(["test", "--select", "test_my_model_overrides"])
        assert len(results) == 1

        # Select using tag
        results = run_dbt(["test", "--select", "tag:test_this"])
        assert len(results) == 1

        # Partial parsing... remove test
        write_file(test_my_model_yml, project.project_root, "models", "test_my_model.yml")
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 4

        # Partial parsing... put back removed test
        write_file(
            test_my_model_yml + datetime_test, project.project_root, "models", "test_my_model.yml"
        )
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        manifest = get_manifest(project.project_root)
        assert len(manifest.unit_tests) == 5
        # Every unit test has a depends_on to the model it tests
        for unit_test_definition in manifest.unit_tests.values():
            assert unit_test_definition.depends_on.nodes[0] == "model.test.my_model"

        # Check for duplicate unit test name
        # this doesn't currently pass with partial parsing because of the root problem
        # described in https://github.com/dbt-labs/dbt-core/issues/8982
        write_file(
            test_my_model_yml + datetime_test + datetime_test,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["run", "--no-partial-parse", "--select", "my_model"])


class TestUnitTestIncrementalModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "events.sql": event_sql,
            "test_my_incremental_model.yml": test_my_model_incremental_yml,
        }

    def test_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        # Select by model name
        results = run_dbt(["test", "--select", "my_incremental_model"], expect_pass=True)
        assert len(results) == 2


my_new_model = """
select
my_favorite_seed.id,
a + b as c
from {{ ref('my_favorite_seed') }} as my_favorite_seed
inner join {{ ref('my_favorite_model') }} as my_favorite_model
on my_favorite_seed.id = my_favorite_model.id
"""

my_favorite_model = """
select
2 as id,
3 as b
"""

seed_my_favorite_seed = """id,a
1,5
2,4
3,3
4,2
5,1
"""

schema_yml_explicit_seed = """
unit_tests:
  - name: t
    model: my_new_model
    given:
      - input: ref('my_favorite_seed')
        rows:
          - {id: 1, a: 10}
      - input: ref('my_favorite_model')
        rows:
          - {id: 1, b: 2}
    expect:
      rows:
        - {id: 1, c: 12}
"""

schema_yml_implicit_seed = """
unit_tests:
  - name: t
    model: my_new_model
    given:
      - input: ref('my_favorite_seed')
      - input: ref('my_favorite_model')
        rows:
          - {id: 1, b: 2}
    expect:
      rows:
        - {id: 1, c: 7}
"""

schema_yml_nonexistent_seed = """
unit_tests:
  - name: t
    model: my_new_model
    given:
      - input: ref('my_second_favorite_seed')
      - input: ref('my_favorite_model')
        rows:
          - {id: 1, b: 2}
    expect:
      rows:
        - {id: 1, c: 7}
"""


class TestUnitTestExplicitSeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_favorite_seed.csv": seed_my_favorite_seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_new_model.sql": my_new_model,
            "my_favorite_model.sql": my_favorite_model,
            "schema.yml": schema_yml_explicit_seed,
        }

    def test_explicit_seed(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # Select by model name
        results = run_dbt(["test", "--select", "my_new_model"], expect_pass=True)
        assert len(results) == 1


class TestUnitTestImplicitSeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_favorite_seed.csv": seed_my_favorite_seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_new_model.sql": my_new_model,
            "my_favorite_model.sql": my_favorite_model,
            "schema.yml": schema_yml_implicit_seed,
        }

    def test_implicit_seed(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # Select by model name
        results = run_dbt(["test", "--select", "my_new_model"], expect_pass=True)
        assert len(results) == 1


class TestUnitTestNonexistentSeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_favorite_seed.csv": seed_my_favorite_seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_new_model.sql": my_new_model,
            "my_favorite_model.sql": my_favorite_model,
            "schema.yml": schema_yml_nonexistent_seed,
        }

    def test_nonexistent_seed(self, project):
        with pytest.raises(
            ParsingError, match="Unable to find seed 'test.my_second_favorite_seed' for unit tests"
        ):
            run_dbt(["test", "--select", "my_new_model"], expect_pass=False)


class TestUnitTestInvalidInputConfiguration:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_yml_invalid,
        }

    def test_invalid_input_configuration(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        run_dbt(["test"], expect_pass=False)
