from .utils import ContractTestCase

from dbt.dataclass_schema import ValidationError

from dbt.contracts.project import Project


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
