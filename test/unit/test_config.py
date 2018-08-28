import os
import unittest
import yaml

import dbt.config

if os.name == 'nt':
    TMPDIR = 'c:/Windows/TEMP'
else:
    TMPDIR = '/tmp'

model_config = {
    'my_package_name': {
        'enabled': True,
        'adwords': {
            'adwords_ads': {
                'materialized': 'table',
                'enabled': True,
                'schema': 'analytics'
            }
        },
        'snowplow': {
            'snowplow_sessions': {
                'sort': 'timestamp',
                'materialized': 'incremental',
                'dist': 'user_id',
                'sql_where': 'created_at > (select max(created_at) from {{ this }})',
                'unique_key': 'id'
            },
            'base': {
                'snowplow_events': {
                    'sort': ['timestamp', 'userid'],
                    'materialized': 'table',
                    'sort_type': 'interleaved',
                    'dist': 'userid'
                }
            }
        }
    }
}

model_fqns = [
    ['my_package_name', 'snowplow', 'snowplow_sessions'],
    ['my_package_name', 'snowplow', 'base', 'snowplow_events'],
    ['my_package_name', 'adwords', 'adwords_ads']
]


class ConfigTest(unittest.TestCase):

    def set_up_empty_config(self):
        profiles_path = '{}/profiles.yml'.format(TMPDIR)

        with open(profiles_path, 'w') as f:
            f.write(yaml.dump({}))

    def set_up_config_options(self, **kwargs):
        profiles_path = '{}/profiles.yml'.format(TMPDIR)

        config = {
            'config': kwargs
        }

        with open(profiles_path, 'w') as f:
            f.write(yaml.dump(config))

    def tearDown(self):
        profiles_path = '{}/profiles.yml'.format(TMPDIR)

        try:
            os.remove(profiles_path)
        except:
            pass

    def test__implicit_opt_in(self):
        self.set_up_empty_config()
        config = dbt.config.read_config(TMPDIR)
        self.assertTrue(dbt.config.send_anonymous_usage_stats(config))

    def test__explicit_opt_out(self):
        self.set_up_config_options(send_anonymous_usage_stats=False)
        config = dbt.config.read_config(TMPDIR)
        self.assertFalse(dbt.config.send_anonymous_usage_stats(config))

    def test__explicit_opt_in(self):
        self.set_up_config_options(send_anonymous_usage_stats=True)
        config = dbt.config.read_config(TMPDIR)
        self.assertTrue(dbt.config.send_anonymous_usage_stats(config))

    def test__implicit_colors(self):
        self.set_up_empty_config()
        config = dbt.config.read_config(TMPDIR)
        self.assertTrue(dbt.config.colorize_output(config))

    def test__explicit_opt_out(self):
        self.set_up_config_options(use_colors=False)
        config = dbt.config.read_config(TMPDIR)
        self.assertFalse(dbt.config.colorize_output(config))

    def test__explicit_opt_in(self):
        self.set_up_config_options(use_colors=True)
        config = dbt.config.read_config(TMPDIR)
        self.assertTrue(dbt.config.colorize_output(config))

    def test__no_unused_resource_config_paths(self):
        resource_config = {'models': model_config, 'seeds': {}}
        resource_config_paths = dbt.config.get_project_resource_config_paths(
            resource_config)
        resource_fqns = {'models': model_fqns}
        self.assertTrue(len(dbt.config.get_unused_resource_config_paths(
            resource_config_paths, resource_fqns)) == 0)

    def test__unused_resource_config_paths(self):
        resource_config = {'models': model_config['my_package_name'], 'seeds': {}}
        resource_config_paths = dbt.config.get_project_resource_config_paths(
            resource_config)
        resource_fqns = {'models': model_fqns}
        self.assertFalse(len(dbt.config.get_unused_resource_config_paths(
            resource_config_paths, resource_fqns)) == 0)
