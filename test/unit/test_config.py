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
