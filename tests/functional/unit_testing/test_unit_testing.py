import os
from unittest import mock

import pytest
from fixtures import (  # noqa: F401
    datetime_test,
    event_sql,
    external_package,
    external_package__accounts_seed_csv,
    my_incremental_model_sql,
    my_model_a_sql,
    my_model_b_sql,
    my_model_sql,
    my_model_vars_sql,
    test_my_model_incremental_yml_basic,
    test_my_model_incremental_yml_no_override,
    test_my_model_incremental_yml_no_this_input,
    test_my_model_incremental_yml_wrong_override,
    test_my_model_yml,
    test_my_model_yml_invalid,
    test_my_model_yml_invalid_ref,
    top_level_domains_sql,
    valid_emails_sql,
)

from dbt.contracts.results import NodeStatus
from dbt.exceptions import DuplicateResourceNameError, ParsingError
from dbt.plugins.manifest import ModelNodeArgs, PluginNodes
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import (
    file_exists,
    get_manifest,
    read_file,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)
from tests.unit.utils import normalize


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

        results = run_dbt(
            ["build", "--select", "my_model", "--resource-types", "model unit_test"],
            expect_pass=False,
        )
        assert len(results) == 6
        for result in results:
            if result.node.unique_id == "model.test.my_model":
                result.status == NodeStatus.Skipped

        # Run build command but specify no unit tests
        results = run_dbt(
            ["build", "--select", "my_model", "--exclude-resource-types", "unit_test"],
            expect_pass=True,
        )
        assert len(results) == 1

        # Exclude unit tests with environment variable
        os.environ["DBT_EXCLUDE_RESOURCE_TYPES"] = "unit_test"
        results = run_dbt(["build", "--select", "my_model"], expect_pass=True)
        assert len(results) == 1

        del os.environ["DBT_EXCLUDE_RESOURCE_TYPES"]

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


class TestUnitTestIncrementalModelBasic:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "events.sql": event_sql,
            "schema.yml": test_my_model_incremental_yml_basic,
        }

    def test_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        # Select by model name
        results = run_dbt(["test", "--select", "my_incremental_model"], expect_pass=True)
        assert len(results) == 2


class TestUnitTestIncrementalModelNoOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "events.sql": event_sql,
            "schema.yml": test_my_model_incremental_yml_no_override,
        }

    def test_no_override(self, project):
        with pytest.raises(
            ParsingError,
            match="Boolean override for 'is_incremental' must be provided for unit test 'incremental_false' in model 'my_incremental_model'",
        ):
            run_dbt(["parse"])


class TestUnitTestIncrementalModelWrongOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "events.sql": event_sql,
            "schema.yml": test_my_model_incremental_yml_wrong_override,
        }

    def test_str_override(self, project):
        with pytest.raises(
            ParsingError,
            match="Boolean override for 'is_incremental' must be provided for unit test 'incremental_false' in model 'my_incremental_model'",
        ):
            run_dbt(["parse"])


class TestUnitTestIncrementalModelNoThisInput:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "events.sql": event_sql,
            "schema.yml": test_my_model_incremental_yml_no_this_input,
        }

    def test_no_this_input(self, project):
        with pytest.raises(
            ParsingError,
            match="Unit test 'incremental_true' for incremental model 'my_incremental_model' must have a 'this' input",
        ):
            run_dbt(["parse"])


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

        # A data type in a given row is incorrect, and we'll get a runtime error
        run_dbt(["test"], expect_pass=False)

        # Test invalid model ref. Parsing error InvalidUnitTestGivenInput
        write_file(
            test_my_model_yml_invalid_ref, project.project_root, "models", "test_my_model.yml"
        )
        results = run_dbt(["test"], expect_pass=False)
        result = results.results[0]
        assert "not found in the manifest" in result.message


unit_test_ext_node_yml = """
unit_tests:
  - name: unit_test_ext_node
    model: valid_emails
    given:
      - input: ref('external_package', 'external_model')
        rows:
          - {user_id: 1, email: cool@example.com,     email_top_level_domain: example.com}
          - {user_id: 2, email: cool@unknown.com,     email_top_level_domain: unknown.com}
          - {user_id: 3, email: badgmail.com,         email_top_level_domain: gmail.com}
          - {user_id: 4, email: missingdot@gmailcom,  email_top_level_domain: gmail.com}
      - input: ref('top_level_domains')
        rows:
          - {tld: example.com}
          - {tld: gmail.com}
    expect:
      rows:
        - {user_id: 1, is_valid_email_address: true}
        - {user_id: 2, is_valid_email_address: false}
        - {user_id: 3, is_valid_email_address: true}
        - {user_id: 4, is_valid_email_address: true}
"""


class TestUnitTestExternalPackageNode:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root, external_package):  # noqa: F811
        write_project_files(project_root, "external_package", external_package)

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "external_package"}]}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "top_level_domains.sql": top_level_domains_sql,
            "valid_emails.sql": valid_emails_sql,
            "unit_test_ext_node.yml": unit_test_ext_node_yml,
        }

    def test_unit_test_ext_nodes(
        self,
        project,
    ):
        # `deps` to install the external package
        run_dbt(["deps"], expect_pass=True)
        # `seed` need so a table exists for `external_model` to point to
        run_dbt(["seed"], expect_pass=True)
        # `run` needed to ensure `top_level_domains` exists in database for column getting step
        run_dbt(["run"], expect_pass=True)
        results = run_dbt(["test", "--select", "valid_emails"], expect_pass=True)
        assert len(results) == 1


class TestUnitTestExternalProjectNode:
    @pytest.fixture(scope="class")
    def external_model_node(self, unique_schema):
        return ModelNodeArgs(
            name="external_model",
            package_name="external_package",
            identifier="external_node_seed",
            schema=unique_schema,
        )

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"external_node_seed.csv": external_package__accounts_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "top_level_domains.sql": top_level_domains_sql,
            "valid_emails.sql": valid_emails_sql,
            "unit_test_ext_node.yml": unit_test_ext_node_yml,
        }

    @mock.patch("dbt.plugins.get_plugin_manager")
    def test_unit_test_ext_nodes(
        self,
        get_plugin_manager,
        project,
        external_model_node,
    ):
        # initial plugin - one external model
        external_nodes = PluginNodes()
        external_nodes.add_model(external_model_node)
        get_plugin_manager.return_value.get_nodes.return_value = external_nodes

        # `seed` need so a table exists for `external_model` to point to
        run_dbt(["seed"], expect_pass=True)
        # `run` needed to ensure `top_level_domains` exists in database for column getting step
        run_dbt(["run"], expect_pass=True)
        results = run_dbt(["test", "--select", "valid_emails"], expect_pass=True)
        assert len(results) == 1


subfolder_model_a_sql = """select 1 as id, 'blue' as color"""

subfolder_model_b_sql = """
select
    id,
    color
from {{ ref('model_a') }}
"""

subfolder_my_model_yml = """
unit_tests:
  - name: my_unit_test
    model: model_b
    given:
      - input: ref('model_a')
        rows:
          - { id: 1, color: 'blue' }
    expect:
      rows:
        - { id: 1, color: 'red' }
"""


class TestUnitTestSubfolderPath:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "subfolder": {
                "model_a.sql": subfolder_model_a_sql,
                "model_b.sql": subfolder_model_b_sql,
                "my_model.yml": subfolder_my_model_yml,
            }
        }

    def test_subfolder_unit_test(self, project):
        results, output = run_dbt_and_capture(["build"], expect_pass=False)

        # Test that input fixture doesn't overwrite the original model
        assert (
            read_file("target/compiled/test/models/subfolder/model_a.sql").strip()
            == subfolder_model_a_sql.strip()
        )

        # Test that correct path is written in logs
        assert (
            normalize(
                "target/compiled/test/models/subfolder/my_model.yml/models/subfolder/my_unit_test.sql"
            )
            in output
        )
        assert file_exists(
            normalize(
                "target/compiled/test/models/subfolder/my_model.yml/models/subfolder/my_unit_test.sql"
            )
        )
