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


class TestDuplicateNamesRequireUnambiguousRefFlagTrue(BaseTestDuplicateNames):

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_unambiguous_ref": True,
            }
        }

    def test_duplicate_names_with_flag_enabled(self, project):
        """When require_unambiguous_ref is True, duplicate unversioned names should raise DuplicateResourceNameError"""
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["parse"])


class TestDuplicateNamesRequireUnambiguousRefFlagFalse(BaseTestDuplicateNames):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "require_unambiguous_ref": False,
            }
        }

    def test_duplicate_names_with_flag_disabled(self, project):
        """When require_unambiguous_ref is False, duplicate unversioned names should be allowed (continue behavior)"""
        manifest = run_dbt(["parse"])

        assert (
            manifest.nodes["model.test.same_name"].name
            == manifest.nodes["seed.test.same_name"].name
        )


class TestDuplicateNamesDefaultBehavior(TestDuplicateNamesRequireUnambiguousRefFlagFalse):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}
