from test.integration.base import DBTIntegrationTest,  use_profile
import os
import re
import yaml

import pytest


class TestDebug(DBTIntegrationTest):
    @property
    def schema(self):
        return 'dbt_debug_049'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return self.dir('models')

    def postgres_profile(self):
        profile = super(TestDebug, self).postgres_profile()
        profile['test']['outputs'].update({
            'nopass': {
                'type': 'postgres',
                'threads': 4,
                'host': self.database_host,
                'port': 5432,
                'user': 'root',
                # 'pass': 'password',
                'dbname': 'dbt',
                'schema': self.unique_schema()
            },
            'wronguser': {
                'type': 'postgres',
                'threads': 4,
                'host': self.database_host,
                'port': 5432,
                'user': 'notmyuser',
                'pass': 'notmypassword',
                'dbname': 'dbt',
                'schema': self.unique_schema()
            }
        })
        return profile

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    def assertGotValue(self, linepat, result):
        found = False
        output = self.capsys.readouterr().out
        for line in output.split('\n'):
            if linepat.match(line):
                found = True
                self.assertIn(result, line, 'result "{}" not found in "{}" line'.format(result, linepat))
        self.assertTrue(found, 'linepat {} not found in stdout: {}'.format(linepat, output))

    @use_profile('postgres')
    def test_postgres_ok(self):
        self.run_dbt(['debug'])
        self.assertNotIn('ERROR', self.capsys.readouterr().out)

    @use_profile('postgres')
    def test_postgres_nopass(self):
        self.run_dbt(['debug', '--target', 'nopass'])
        self.assertGotValue(re.compile(r'\s+profiles\.yml file'), 'ERROR invalid')

    @use_profile('postgres')
    def test_postgres_wronguser(self):
        self.run_dbt(['debug', '--target', 'wronguser'])
        self.assertGotValue(re.compile(r'\s+Connection test'), 'ERROR')


class TestDebugInvalidProject(DBTIntegrationTest):
    @property
    def schema(self):
        return 'dbt_debug_049'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return self.dir('models')

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    @use_profile('postgres')
    def test_postgres_badproject(self):
        # load a special project that is an error
        self.use_default_project(overrides={
            'invalid-key': 'not a valid key so this is bad project',
        })
        self.run_dbt(['debug', '--profile', 'test'])
        splitout = self.capsys.readouterr().out.split('\n')
        for line in splitout:
            if line.strip().startswith('dbt_project.yml file'):
                self.assertIn('ERROR invalid', line)
            elif line.strip().startswith('profiles.yml file'):
                self.assertNotIn('ERROR invalid', line)

    @use_profile('postgres')
    def test_postgres_not_found_project_dir(self):
        self.run_dbt(['debug', '--project-dir', 'nopass'])
        splitout = self.capsys.readouterr().out.split('\n')
        for line in splitout:
            if line.strip().startswith('dbt_project.yml file'):
                self.assertIn('ERROR not found', line)

    @use_profile('postgres')
    def test_postgres_invalid_project_outside_current_dir(self):
        # create a dbt_project.yml
        project_config = {
            'invalid-key': 'not a valid key in this project'
        }
        os.makedirs('custom', exist_ok=True)
        with open("custom/dbt_project.yml", 'w') as f:
            yaml.safe_dump(project_config, f, default_flow_style=True)
        self.run_dbt(['debug', '--project-dir', 'custom'])
        splitout = self.capsys.readouterr().out.split('\n')
        for line in splitout:
            if line.strip().startswith('dbt_project.yml file'):
                self.assertIn('ERROR invalid', line)
