
from test.integration.base import DBTIntegrationTest, use_profile
import os
import shutil


class TestInit(DBTIntegrationTest):
    def tearDown(self):
        project_name = self.get_project_name()

        if os.path.exists(project_name):
            shutil.rmtree(project_name)

        DBTIntegrationTest.tearDown(self)

    def get_project_name(self):
        return "my_project_{}".format(self.unique_schema())

    @property
    def schema(self):
        return "init_040"

    @property
    def models(self):
        return "test/integration/040_init_test/models"

    @use_profile('postgres')
    def test_init_task(self):
        project_name = self.get_project_name()
        self.run_dbt(['init', project_name])

        dir_exists = os.path.exists(project_name)
        project_file = os.path.join(project_name, 'dbt_project.yml')
        project_file_exists = os.path.exists(project_file)

        self.assertTrue(dir_exists)
        self.assertTrue(project_file_exists)
