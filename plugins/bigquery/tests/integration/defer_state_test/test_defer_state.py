from tests.integration.base import DBTIntegrationTest, use_profile
import copy
import json
import os
import shutil

import pytest


class TestDeferState(DBTIntegrationTest):
    @property
    def schema(self):
        return "defer_state"

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
                    'quote_columns': False,
                }
            }
        }

    def get_profile(self, adapter_type):
        if self.other_schema is None:
            self.other_schema = self.unique_schema() + '_other'
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
        assert not any(r.node.deferred for r in results)
        results = self.run_dbt(['run'])
        assert len(results) == 2
        assert not any(r.node.deferred for r in results)
        results = self.run_dbt(['test'])
        assert len(results) == 2

        # copy files over from the happy times when we had a good target
        self.copy_state()

        # test tests first, because run will change things
        # no state, wrong schema, failure.
        self.run_dbt(['test', '--target', 'otherschema'], expect_pass=False)

        # no state, run also fails
        self.run_dbt(['run', '--target', 'otherschema'], expect_pass=False)

        # defer test, it succeeds
        results = self.run_dbt(['test', '-m', 'view_model+', '--state', 'state', '--defer', '--target', 'otherschema'])

        # with state it should work though
        results = self.run_dbt(['run', '-m', 'view_model', '--state', 'state', '--defer', '--target', 'otherschema'])
        assert self.other_schema not in results[0].node.compiled_sql
        assert self.unique_schema() in results[0].node.compiled_sql

        with open('target/manifest.json') as fp:
            data = json.load(fp)
        assert data['nodes']['seed.test.seed']['deferred']

        assert len(results) == 1

    @use_profile('bigquery')
    def test_bigquery_state_changetarget(self):
        self.run_and_defer()
