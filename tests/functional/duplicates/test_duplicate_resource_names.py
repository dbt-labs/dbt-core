from collections import defaultdict

import pytest

import dbt.deprecations as deprecations
from dbt.exceptions import DuplicateResourceNameError
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import run_dbt

# Test resources with duplicate names but different database aliases
model_sql = """
select 1 as id, 'test' as name
"""

seed_csv = """
id,value
1,test
2,another
"""


macros_sql = """
{% macro generate_alias_name(custom_alias_name, node) -%}
    {{ node.name }}_{{ node.resource_type }}
{%- endmacro %}
"""

versioned_model_yml = """
models:
  - name: same_name
    versions:
      - v: 1
"""

local_dependency__dbt_project_yml = """

name: 'local_dep'
version: '1.0'
"""


@pytest.fixture(scope="class")
def set_up_deprecations():
    deprecations.reset_deprecations()
    assert deprecations.active_deprecations == defaultdict(int)


class BaseTestDuplicateNames:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "same_name.sql": model_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "same_name.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generate_alias_name.sql": macros_sql,
        }


class TestDuplicateNamesRequireUniqueResourceNamesTrue(BaseTestDuplicateNames):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_unique_project_resource_names": True,
            }
        }

    def test_duplicate_names_with_flag_enabled(self, project):
        """When require_unique_project_resource_names is True, duplicate unversioned names should raise DuplicateResourceNameError"""
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["parse"])


class TestDuplicateNamesRequireUniqueResourceNamesTrueDifferentPackages(BaseTestDuplicateNames):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_unique_project_resource_names": True,
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        # Seed in local dep instead
        return {}

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "local_dependency"}]}

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        local_dependency_files = {
            "dbt_project.yml": local_dependency__dbt_project_yml,
            "seeds": {"same_name.csv": seed_csv},
        }
        write_project_files(project_root, "local_dependency", local_dependency_files)

    def test_duplicate_names_with_flag_enabled_different_packages(self, project):
        run_dbt(["deps"])
        # Behavior flag is true, however, the seed is in a different package.
        # No error is raised when encountering duplicate names between different packages.
        manifest = run_dbt(["parse"])

        assert len(manifest.nodes) == 2
        assert "model.test.same_name" in manifest.nodes
        assert "seed.local_dep.same_name" in manifest.nodes


class TestDuplicateNamesRequireUniqueResourceNamesFalse(BaseTestDuplicateNames):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_unique_project_resource_names": False,
            }
        }

    def test_duplicate_names_with_flag_disabled(self, project, set_up_deprecations):
        """When require_unique_project_resource_names is False, duplicate unversioned names should be allowed (continue behavior)"""
        manifest = run_dbt(["parse"])

        assert (
            manifest.nodes["model.test.same_name"].name
            == manifest.nodes["seed.test.same_name"].name
        )

        assert "duplicate-name-distinct-node-types-deprecation" in deprecations.active_deprecations


class TestDuplicateNamesDefaultBehavior(TestDuplicateNamesRequireUniqueResourceNamesFalse):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}


class TestDuplicateNamesDifferentResourceTypesVersionedUnversioned(BaseTestDuplicateNames):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "same_name.sql": model_sql,
            "schema.yml": versioned_model_yml,
        }

    def test_duplicate_names_versioned_unversioned(self, project):
        # DuplicateVersionedUnversionedError is not raised because parsing fails upstream.
        # However, parsing still fails with an AssertionError because versioning is attempted on a non-model node.
        with pytest.raises(AssertionError):
            run_dbt(["parse"])
