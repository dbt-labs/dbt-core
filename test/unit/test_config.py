import os
import unittest
import yaml

import dbt.config

class ConfigTest(unittest.TestCase):

    def set_up_config_options(self, send_anonymous_usage_stats=False):
        with open('/tmp/profiles.yml', 'w') as f:
            f.write(yaml.dump({
                'config': {
                    'send_anonymous_usage_stats': send_anonymous_usage_stats
                }
            }))

    def tearDown(self):
        try:
            os.remove('/tmp/profiles.yml')
        except:
            pass

    def test__implicit_not_opted_out(self):
        self.assertTrue(dbt.config.send_anonymous_usage_stats('/tmp'))

    def test__explicit_opt_out(self):
        self.set_up_config_options(send_anonymous_usage_stats=False)
        self.assertFalse(dbt.config.send_anonymous_usage_stats('/tmp'))

    def test__explicit_opt_in(self):
        self.set_up_config_options(send_anonymous_usage_stats=True)
        self.assertTrue(dbt.config.send_anonymous_usage_stats('/tmp'))
