from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, FakeArgs

from dbt.task.test import TestTask
from dbt.project import read_project
import os


class TestThreadCount(DBTIntegrationTest):

    @property
    def project_config(self):
        return {}

    @property
    def profile_config(self):
        return {
            'threads': 2,
        }

    @property
    def schema(self):
        return "thread_tests_031"

    @property
    def models(self):
        return "test/integration/031_thread_count_test/models"

    @attr(type='postgres')
    def test_postgres_threading_8x(self):
        self.use_profile('postgres')

        results = self.run_dbt(args=['run', '--threads', '16'])
        self.assertTrue(len(results), 20)
