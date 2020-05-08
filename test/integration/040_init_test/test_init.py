
from test.integration.base import DBTIntegrationTest, use_profile
import os
import shutil
import yaml


class TestInit(DBTIntegrationTest):
    def tearDown(self):
        project_name = self.get_project_name()

        if os.path.exists(project_name):
            shutil.rmtree(project_name)

        super().tearDown()

    def get_project_name(self):
        return "my_project_{}".format(self.unique_schema())

    @property
    def schema(self):
        return "init_040"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_init_task(self):
        project_name = self.get_project_name()
        self.run_dbt(['init', project_name])

        assert os.path.exists(project_name)
        project_file = os.path.join(project_name, 'dbt_project.yml')
        assert os.path.exists(project_file)
        with open(project_file) as fp:
            project_data = yaml.safe_load(fp.read())

        assert 'config-version' in project_data
        assert project_data['config-version'] == 2

        git_dir = os.path.join(project_name, '.git')
        assert not os.path.exists(git_dir)
