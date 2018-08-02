import json
import os

from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, use_profile


class TestLateBindingView(DBTIntegrationTest):
    @property
    def schema(self):
        return 'late_binding_view_034'

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('test/integration/034_redshift_test', path)
        )

    @property
    def models(self):
        return self.dir("models")

    @use_profile('redshift')
    def test__late_binding_view_query(self):
        self.use_default_project({"data-paths": [self.dir("seed")]})

        self.assertEqual(len(self.run_dbt(["seed"])), 1)
        self.assertEqual(len(self.run_dbt()), 1)
        # remove the table. Use 'cascade' here so that if late-binding views
        # didn't work as advertised, the following dbt run will fail.
        drop = 'drop table if exists {}.seed cascade'.format(
            self.unique_schema()
        )
        self.run_sql(drop)
        self.assertEqual(len(self.run_dbt()), 1)
