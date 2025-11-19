import pytest

from dbt.exceptions import DuplicateResourceNameError
from dbt.tests.util import run_dbt

# Test resources with duplicate names
model_sql = """
select 1 as id, 'test' as name
"""

seed_csv = """
id,value
1,test
2,another
"""


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


class TestDuplicateNamesRequireUniqueProjectResourceNamesTrue(BaseTestDuplicateNames):

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


class TestDuplicateNamesRequireUniqueProjectResourceNamesFalse(BaseTestDuplicateNames):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_unique_project_resource_names": False,
            }
        }

    def test_duplicate_names_with_flag_disabled(self, project):
        """When require_unique_project_resource_names is False, duplicate unversioned names should be allowed (continue behavior)"""
        manifest = run_dbt(["parse"])

        assert (
            manifest.nodes["model.test.same_name"].name
            == manifest.nodes["seed.test.same_name"].name
        )


class TestDuplicateNamesDefaultBehavior(TestDuplicateNamesRequireUniqueProjectResourceNamesFalse):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}
