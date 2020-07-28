from test.integration.base import DBTIntegrationTest, use_profile
import copy
import os
import shutil

import pytest

class TestDeferState(DBTIntegrationTest):
    @property
    def schema(self):
        return "defer_state_062"

    @property
    def models(self):
        return "models"

    def setUp(self):
        self.other_schema = None
        super().setUp()
        self._created_schemas.add(self.other_schema)

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'test': {
                    'quote_columns': True,
                }
            }
        }

    def get_profile(self, adapter_type):
        if self.other_schema is None:
            self.other_schema = self.unique_schema() + '_other'
            if self.adapter_type == 'snowflake':
                self.other_schema = self.other_schema.upper()
        profile = super().get_profile(adapter_type)
        default_name = profile['test']['target']
        profile['test']['outputs']['otherschema'] = copy.deepcopy(profile['test']['outputs'][default_name])
        profile['test']['outputs']['otherschema']['schema'] = self.other_schema
        return profile

    def copy_state(self):
        assert not os.path.exists('state')
        os.makedirs('state')
        shutil.copyfile('target/manifest.json', 'state/manifest.json')

    def run_and_defer(self):
        results = self.run_dbt(['seed'])
        assert len(results) == 1
        results = self.run_dbt(['run'])
        assert len(results) == 2

        # copy files over from the happy times when we had a good target
        self.copy_state()

        # no state, still fails
        self.run_dbt(['run', '--target', 'otherschema'], expect_pass=False)

        # with state it should work though
        results = self.run_dbt(['run', '-m', 'view_model', '--state', 'state', '--defer', '--target', 'otherschema'])
        assert self.other_schema not in results[0].node.injected_sql
        assert self.unique_schema() in results[0].node.injected_sql
        assert len(results) == 1

    def run_switchdirs_defer(self):
        results = self.run_dbt(['seed'])
        assert len(results) == 1
        results = self.run_dbt(['run'])
        assert len(results) == 2

        # copy files over from the happy times when we had a good target
        self.copy_state()

        self.use_default_project({'source-paths': ['changed_models']})
        # the sql here is just wrong, so it should fail
        self.run_dbt(
            ['run', '-m', 'view_model', '--state', 'state', '--defer', '--target', 'otherschema'],
            expect_pass=False,
        )
        # but this should work since we just use the old happy model
        self.run_dbt(
            ['run', '-m', 'table_model', '--state', 'state', '--defer', '--target', 'otherschema'],
            expect_pass=True,
        )

        self.use_default_project({'source-paths': ['changed_models_bad']})
        # this should fail because the table model refs a broken ephemeral
        # model, which it should see
        self.run_dbt(
            ['run', '-m', 'table_model', '--state', 'state', '--defer', '--target', 'otherschema'],
            expect_pass=False,
        )

    @use_profile('postgres')
    def test_postgres_state_changetarget(self):
        self.run_and_defer()
        # these should work without --defer!
        self.run_dbt(['test'])
        self.run_dbt(['snapshot'])
        # make sure these commands don't work with --defer
        with pytest.raises(SystemExit):
            self.run_dbt(['seed', '--defer'])

        with pytest.raises(SystemExit):
            self.run_dbt(['test', '--defer'])
        with pytest.raises(SystemExit):
            self.run_dbt(['snapshot', '--defer'])

    @use_profile('postgres')
    def test_postgres_stat_changedir(self):
        self.run_switchdirs_defer()

    @use_profile('snowflake')
    def test_snowflake_state_changetarget(self):
        self.run_and_defer()

    @use_profile('redshift')
    def test_redshift_state_changetarget(self):
        self.run_and_defer()

    @use_profile('bigquery')
    def test_bigquery_state_changetarget(self):
        self.run_and_defer()

