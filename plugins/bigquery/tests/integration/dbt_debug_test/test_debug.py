from tests.integration.base import DBTIntegrationTest,  use_profile
import os
import re
import yaml

import pytest


class TestDebug(DBTIntegrationTest):
    @property
    def schema(self):
        return 'dbt_debug'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return self.dir('models')

    @use_profile('bigquery')
    def test_bigquery_ok(self):
        self.run_dbt(['debug'])
        self.assertNotIn('ERROR', self.capsys.readouterr().out)

class TestDebugProfileVariable(TestDebug):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'profile': '{{ "te" ~ "st" }}'
        }
