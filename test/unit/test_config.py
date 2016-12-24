import os
import unittest
import yaml

import dbt.config

if os.name == 'nt':
    TMPDIR = 'c:/Windows/TEMP'
else:
    TMPDIR = '/tmp'

class ConfigTest(unittest.TestCase):

    def set_up_empty_config(self):
        profiles_path = '{}/profiles.yml'.format(TMPDIR)

        with open(profiles_path, 'w') as f:
            f.write(yaml.dump({}))

    def set_up_config_options(self, send_anonymous_usage_stats=False):
        profiles_path = '{}/profiles.yml'.format(TMPDIR)

        with open(profiles_path, 'w') as f:
            f.write(yaml.dump({
                'config': {
                    'send_anonymous_usage_stats': send_anonymous_usage_stats
                }
            }))

    def tearDown(self):
        profiles_path = '{}/profiles.yml'.format(TMPDIR)

        try:
            os.remove(profiles_path)
        except:
            pass

    def test__implicit_opt_in(self):
        self.set_up_empty_config()
        self.assertTrue(dbt.config.send_anonymous_usage_stats(TMPDIR))

    def test__explicit_opt_out(self):
        self.set_up_config_options(send_anonymous_usage_stats=False)
        self.assertFalse(dbt.config.send_anonymous_usage_stats(TMPDIR))

    def test__explicit_opt_in(self):
        self.set_up_config_options(send_anonymous_usage_stats=True)
        self.assertTrue(dbt.config.send_anonymous_usage_stats(TMPDIR))
