from .utils import ContractTestCase

from hologram import ValidationError

from dbt.contracts.project import ProjectV1

class TestProjectV1(ContractTestCase):
    ContractType = ProjectV1

    def test_minimal(self):
        dct = {
            'name': 'test',
            'version': '1.0',
            'profile': 'test',
            'project-root': '/usr/src/app',
        }
        project = ProjectV1(
            name='test',
            version='1.0',
            profile='test',
            project_root='/usr/src/app',
        )
        self.assert_from_dict(project, dct)

    def test_invalid_name(self):
        dct = {
            'name': 'log',
            'version': '1.0',
            'profile': 'test',
            'project-root': '/usr/src/app',
        }
        with self.assertRaises(ValidationError):
            ProjectV1.from_dict(dct)
