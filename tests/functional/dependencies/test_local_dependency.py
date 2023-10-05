import os
import pytest
import json
import shutil
import yaml

# todo: make self.unique_schema to fixture

from pathlib import Path
from unittest import mock

import dbt.semver
import dbt.config
import dbt.exceptions

from dbt.tests.util import check_relations_equal, run_dbt, run_dbt_and_capture
from tests.functional.utils import up_one

models__dep_source = """
{# If our dependency source didn't exist, this would be an errror #}
select * from {{ source('seed_source', 'seed') }}
"""

models__my_configured_model = """
{{
    config(schema='configured')
}}
select * from {{ ref('model_to_import') }}
"""

models__my_model = """
select * from {{ ref('model_to_import') }}
"""

models__source_override_model = """
{# If our source override didn't take, this would be an errror #}
select * from {{ source('my_source', 'my_table') }}
"""

models__iterate = """
{% for x in no_such_dependency.no_such_method() %}
{% endfor %}
"""

models__hooks_actual = """
select * from {{ var('test_create_table') }}
union all
select * from {{ var('test_create_second_table') }}
"""

models__hooks_expected = """
{# surely there is a better way to do this! #}

{% for _ in range(1, 5) %}
select {{ loop.index }} as id
{% if not loop.last %}union all{% endif %}
{% endfor %}
"""

properties__schema_yml = """
version: 2
sources:
  - name: my_source
    schema: "{{ var('schema_override', target.schema) }}"
    tables:
      - name: my_table
        identifier: seed_subpackage_generate_alias_name
"""

macros__macro_sql = """
{# This macro also exists in the dependency -dbt should be fine with that #}
{% macro some_overridden_macro() -%}
999
{%- endmacro %}
"""

macros__macro_override_schema_sql = """
{% macro generate_schema_name(schema_name, node) -%}

    {{ schema_name }}_{{ node.schema }}_macro

{%- endmacro %}
"""


class BaseDependencyTest(object):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": macros__macro_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dep_source_model.sql": models__dep_source,
            "my_configured_model.sql": models__my_configured_model,
            "my_model.sql": models__my_model,
            "source_override_model.sql": models__source_override_model,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "schema.yml": properties__schema_yml,
        }

    @pytest.fixture(scope="class", autouse=True)
    def modify_schema_fqn(self, project):
        schema_fqn = "{}.{}".format(
            project.database,
            project.test_schema,
        )
        schema_fqn_configured = "{}.{}".format(
            project.database,
            project.test_schema + "_configured",
        )

        project.created_schemas.append(schema_fqn)
        project.created_schemas.append(schema_fqn_configured)

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project, modify_schema_fqn):
        shutil.copytree(
            project.test_dir / Path("local_dependency"),
            project.project_root / Path("local_dependency"),
        )

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "local_dependency"}]}


class TestSimpleDependency(BaseDependencyTest):
    def test_local_dependency(self, project):
        run_dbt(["deps"])
        run_dbt(["seed"])
        results = run_dbt()
        assert len(results) == 5

        assert {r.node.schema for r in results} == {
            project.test_schema,
            project.test_schema + "_configured",
        }

        base_schema_nodes = [r.node for r in results if r.node.schema == project.test_schema]
        assert len(base_schema_nodes) == 4

        check_relations_equal(
            project.adapter,
            [
                f"{project.test_schema}.source_override_model",
                f"{project.test_schema}.seed_subpackage_generate_alias_name",
            ],
        )
        check_relations_equal(
            project.adapter,
            [
                f"{project.test_schema}.dep_source_model",
                f"{project.test_schema}.seed_subpackage_generate_alias_name",
            ],
        )

    def test_no_dependency_paths(self, project):
        run_dbt(["deps"])
        run_dbt(["seed"])

        # prove dependency does not exist as model in project
        dep_path = os.path.join("models_local", "model_to_import.sql")
        results = run_dbt(
            ["run", "--models", f"+{dep_path}"],
        )
        assert len(results) == 0

        # prove model can run when importing that dependency
        local_path = Path("models") / "my_model.sql"
        results = run_dbt(
            ["run", "--models", f"+{local_path}"],
        )
        assert len(results) == 2


class TestSimpleDependencyRelativePath(BaseDependencyTest):
    def test_local_dependency_relative_path(self, project):
        last_dir = Path(project.project_root).name
        with up_one():
            _, stdout = run_dbt_and_capture(["deps", "--project-dir", last_dir])
            assert (
                "Installed from <local @ local_dependency>" in stdout
            ), "Test output didn't contain expected string"


class TestMissingDependency(object):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "iterate.sql": models__iterate,
        }

    def test_missing_dependency(self, project):
        # dbt should raise a runtime exception
        with pytest.raises(dbt.exceptions.DbtRuntimeError):
            run_dbt(["compile"])


class TestSimpleDependencyWithSchema(BaseDependencyTest):
    def dbt_vargs(self, schema):
        # we can't add this to the config because Sources don't respect dbt_project.yml
        vars_arg = yaml.safe_dump({"schema_override": "dbt_test_{}_macro".format(schema)})
        return ["--vars", vars_arg]

    def project_config(self):
        return {
            "models": {
                "schema": "dbt_test",
            },
            "seeds": {
                "schema": "dbt_test",
            },
        }

    @mock.patch("dbt.config.project.get_installed_version")
    def test_local_dependency_out_of_date(self, mock_get, project):
        mock_get.return_value = dbt.semver.VersionSpecifier.from_version_string("0.0.1")
        run_dbt(["deps"] + self.dbt_vargs(project.test_schema))
        # check seed
        with pytest.raises(dbt.exceptions.DbtProjectError) as exc:
            run_dbt(["seed"] + self.dbt_vargs(project.test_schema))
        assert "--no-version-check" in str(exc.value)
        # check run too
        with pytest.raises(dbt.exceptions.DbtProjectError) as exc:
            run_dbt(["run"] + self.dbt_vargs(project.test_schema))
        assert "--no-version-check" in str(exc.value)

    @mock.patch("dbt.config.project.get_installed_version")
    def test_local_dependency_out_of_date_no_check(self, mock_get):
        mock_get.return_value = dbt.semver.VersionSpecifier.from_version_string("0.0.1")
        run_dbt(["deps"])
        run_dbt(["seed", "--no-version-check"])
        results = run_dbt(["run", "--no-version-check"])
        assert len(results) == 5


class TestSimpleDependencyNoVersionCheckConfig(BaseDependencyTest):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "schema": "dbt_test",
            },
            "seeds": {
                "schema": "dbt_test",
            },
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "config": {
                "send_anonymous_usage_stats": False,
                "version_check": False,
            }
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": macros__macro_override_schema_sql}

    @mock.patch("dbt.config.project.get_installed_version")
    def test_local_dependency_out_of_date_no_check(self, mock_get, project):
        # we can't add this to the config because Sources don't respect dbt_project.yml
        base_schema = "dbt_test_{}_macro".format(project.test_schema)
        vars_arg = yaml.safe_dump(
            {
                "schema_override": base_schema,
            }
        )

        mock_get.return_value = dbt.semver.VersionSpecifier.from_version_string("0.0.1")
        run_dbt(["deps", "--vars", vars_arg])
        run_dbt(["seed", "--vars", vars_arg])
        results = run_dbt(["run", "--vars", vars_arg])
        len(results) == 5


class TestSimpleDependencyHooks(BaseDependencyTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__hooks_actual,
            "expected.sql": models__hooks_expected,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # these hooks should run first, so nothing to drop
        return {
            "on-run-start": [
                "drop table if exists {{ var('test_create_table') }}",
                "drop table if exists {{ var('test_create_second_table') }}",
            ]
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [{"local": "early_hook_dependency"}, {"local": "late_hook_dependency"}]
        }

    @pytest.fixture(scope="class")
    def prepare_dependencies(self, project):
        shutil.copytree(
            project.test_dir / Path("early_hook_dependency"),
            project.project_root / Path("early_hook_dependency"),
        )
        shutil.copytree(
            project.test_dir / Path("late_hook_dependency"),
            project.project_root / Path("late_hook_dependency"),
        )

    def test_hook_dependency(self, prepare_dependencies, project):
        cli_vars = json.dumps(
            {
                "test_create_table": '"{}"."hook_test"'.format(project.test_schema),
                "test_create_second_table": '"{}"."hook_test_2"'.format(project.test_schema),
            }
        )

        run_dbt(["deps", "--vars", cli_vars])
        results = run_dbt(["run", "--vars", cli_vars])
        assert len(results) == 2
        check_relations_equal(project.adapter, ["actual", "expected"])


class TestSimpleDependencyDuplicateName(BaseDependencyTest):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self):
        pass  # do not copy local dependency automatically

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "duplicate_dependency"}]}

    @pytest.fixture(scope="class")
    def prepare_dependencies(self, project):
        shutil.copytree(
            project.test_dir / Path("duplicate_dependency"),
            project.project_root / Path("duplicate_dependency"),
        )

    def test_local_dependency_same_name(self, prepare_dependencies, project):
        with pytest.raises(dbt.exceptions.DependencyError):
            run_dbt(["deps"], expect_pass=False)

    def test_local_dependency_same_name_sneaky(self, prepare_dependencies, project):
        shutil.copytree("duplicate_dependency", "./dbt_packages/duplicate_dependency")
        with pytest.raises(dbt.exceptions.CompilationError):
            run_dbt(["compile"])

        # needed to avoid compilation errors from duplicate package names in test autocleanup
        run_dbt(["clean"])
