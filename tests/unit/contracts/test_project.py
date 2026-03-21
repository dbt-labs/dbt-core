import pytest

from dbt.contracts.project import PackageConfig, Project
from dbt_common.dataclass_schema import ValidationError
from tests.unit.utils import ContractTestCase


class TestProject(ContractTestCase):
    ContractType = Project

    def test_minimal(self):
        dct = {
            "name": "test",
            "version": "1.0",
            "profile": "test",
            "project-root": "/usr/src/app",
            "config-version": 2,
        }
        project = self.ContractType(
            name="test",
            version="1.0",
            profile="test",
            project_root="/usr/src/app",
            config_version=2,
        )
        self.assert_from_dict(project, dct)

    def test_invalid_name(self):
        dct = {
            "name": "log",
            "version": "1.0",
            "profile": "test",
            "project-root": "/usr/src/app",
            "config-version": 2,
        }
        with self.assertRaises(ValidationError):
            self.ContractType.validate(dct)


class TestPackageConfigValidation:
    def test_missing_version_key_raises_validation_error(self):
        """When version key is omitted entirely from a hub package, raise ValidationError, not KeyError."""
        data = {"packages": [{"package": "dbt-labs/dbt_utils"}]}
        with pytest.raises(ValidationError, match="missing the version"):
            PackageConfig.validate(data)

    def test_empty_version_raises_validation_error(self):
        """When version key exists but is empty, raise ValidationError."""
        data = {"packages": [{"package": "dbt-labs/dbt_utils", "version": ""}]}
        with pytest.raises(ValidationError, match="missing the version"):
            PackageConfig.validate(data)

    def test_valid_package_passes(self):
        """A well-formed hub package should pass validation."""
        data = {"packages": [{"package": "dbt-labs/dbt_utils", "version": ">=1.0.0"}]}
        PackageConfig.validate(data)
