import unittest

import os
import dbt.project


class ProjectTest(unittest.TestCase):
    def setUp(self):
        self.profiles = {
            'test': {
                'outputs': {
                    'test': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': 'dbt_test'
                    }
                },
                'target': 'test'
            }
        }
        self.cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }

    def test_profile_validate_success(self):
        # Make sure we can instantiate + validate a valid profile

        project = dbt.project.Project(
            cfg=self.cfg,
            profiles=self.profiles,
            profiles_dir=None
        )

        project.validate()

    def test_profile_validate_missing(self):
        del self.profiles['test']['outputs']['test']['schema']

        project = dbt.project.Project(
            cfg=self.cfg,
            profiles=self.profiles,
            profiles_dir=None
        )

        message = r'.*schema.* is a required property.*'
        with self.assertRaisesRegexp(dbt.project.DbtProjectError, message):
            project.validate()

    def test_profile_validate_extra(self):
        self.profiles['test']['outputs']['test']['foo'] = 'bar'

        project = dbt.project.Project(
            cfg=self.cfg,
            profiles=self.profiles,
            profiles_dir=None
        )

        message = r'.*not allowed.*foo.* was unexpected.*'
        with self.assertRaisesRegexp(dbt.project.DbtProjectError, message):
            project.validate()

    def test_profile_validate_missing_and_extra(self):
        del self.profiles['test']['outputs']['test']['schema']
        self.profiles['test']['outputs']['test']['foo'] = 'bar'

        project = dbt.project.Project(
            cfg=self.cfg,
            profiles=self.profiles,
            profiles_dir=None
        )

        unrecognized = r'not allowed.*foo.* was unexpected'
        extra = r'schema.* is a required property'
        # fun with regexp ordering: want both, don't care about order
        message = '.*({0}.*{1}|{1}.*{0}).*'.format(unrecognized, extra)
        with self.assertRaisesRegexp(dbt.project.DbtProjectError, message):
            project.validate()
