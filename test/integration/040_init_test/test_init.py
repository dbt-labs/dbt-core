import os
import shutil
from unittest import mock
from unittest.mock import Mock, call

import click

from test.integration.base import DBTIntegrationTest, use_profile


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
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_postgres_init_task_in_project(self, mock_prompt, mock_confirm):
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')
        manager.confirm.side_effect = ["y"]
        manager.prompt.side_effect = [
            1,
            4,
            "localhost",
            5432,
            "test_user",
            "test_password",
            "test_db",
            "test_schema",
        ]
        self.run_dbt(['init', '--profiles-dir', 'dbt-profile'], profiles_dir=False)
        manager.assert_has_calls([
            call.confirm('The profile test already exists in /Users/niall.woodward/.dbt/profiles.yml. Continue and overwrite it?'),
            call.prompt("Which database would you like to use?\n[1] postgres\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)\n\nEnter a number", type=click.INT),
            call.prompt('threads (1 or more)', default=1, hide_input=False, type=click.INT),
            call.prompt('host (hostname for the instance)', default=None, hide_input=False, type=None),
            call.prompt('port', default=5432, hide_input=False, type=click.INT),
            call.prompt('user (dev username)', default=None, hide_input=False, type=None),
            call.prompt('pass (dev password)', default=None, hide_input=True, type=None),
            call.prompt('dbname (default database that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('schema (default schema that dbt will build objects in)', default=None, hide_input=False, type=None)
        ])
