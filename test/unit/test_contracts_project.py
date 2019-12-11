from .utils import ContractTestCase

from hologram import ValidationError

from dbt.contracts.project import Project

class TestProject(ContractTestCase):
    ContractType = Project

    def test_minimal(self):
        dct = {
            'name': 'test',
            'version': '1.0',
            'profile': 'test',
            'project-root': '/usr/src/app',
        }
        project = Project(
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
            Project.from_dict(dct)
