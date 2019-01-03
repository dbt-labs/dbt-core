import os
import tempfile
import unittest

import mock
import yaml

from dbt import main
import dbt.tracking
import dbt.ui.printer


class FakeArgs(object):
    def __init__(self, profiles_dir):
        self.profiles_dir = profiles_dir
        self.profile = 'test'


@mock.patch('dbt.ui.printer.use_colors')
@mock.patch('dbt.tracking.do_not_track')
@mock.patch('dbt.tracking.initialize_tracking')
class TestInitializeConfig(unittest.TestCase):
    def setUp(self):
        self.base_dir = tempfile.mkdtemp()
        self.profiles_path = os.path.join(self.base_dir, 'profiles.yml')
        self.args = FakeArgs(self.base_dir)

    def _base_config(self):
        return {
            'test': {
                'outputs': {
                    'default': {
                        'type': 'postgres',
                        'host': 'test',
                        'port': 5555,
                        'user': 'db_user',
                        'pass': 'db_pass',
                        'dbname': 'dbname',
                        'schema': 'schema',
                        },
                },
                'target': 'default',
            }
        }

    def set_up_empty_config(self):
        with open(self.profiles_path, 'w') as f:
            f.write(yaml.dump(self._base_config()))

    def set_up_config_options(self, **kwargs):
        config = self._base_config()
        config.update(config=kwargs)

        with open(self.profiles_path, 'w') as f:
            f.write(yaml.dump(config))

    def tearDown(self):
        try:
            shutil.rmtree(self.base_dir)
        except:
            pass

    def test__implicit_missing(self, initialize_tracking, do_not_track, use_colors):
        main.initialize_config_values(self.args)

        initialize_tracking.assert_called_once_with(self.base_dir)
        do_not_track.assert_not_called()
        use_colors.assert_called_once_with()

    def test__implicit_opt_in_colors(self, initialize_tracking, do_not_track, use_colors):
        self.set_up_empty_config()
        main.initialize_config_values(self.args)

        initialize_tracking.assert_called_once_with(self.base_dir)
        do_not_track.assert_not_called()
        use_colors.assert_called_once_with()

    def test__explicit_opt_out(self, initialize_tracking, do_not_track, use_colors):
        self.set_up_config_options(send_anonymous_usage_stats=False)
        main.initialize_config_values(self.args)

        initialize_tracking.assert_not_called()
        do_not_track.assert_called_once_with()
        use_colors.assert_called_once_with()

    def test__explicit_opt_in(self, initialize_tracking, do_not_track, use_colors):
        self.set_up_config_options(send_anonymous_usage_stats=True)
        main.initialize_config_values(self.args)

        initialize_tracking.assert_called_once_with(self.base_dir)
        do_not_track.assert_not_called()
        use_colors.assert_called_once_with()

    def test__explicit_no_colors(self, initialize_tracking, do_not_track, use_colors):
        self.set_up_config_options(use_colors=False)
        main.initialize_config_values(self.args)

        initialize_tracking.assert_called_once_with(self.base_dir)
        do_not_track.assert_not_called()
        use_colors.assert_not_called()

    def test__explicit_opt_in(self, initialize_tracking, do_not_track, use_colors):
        self.set_up_config_options(use_colors=True)
        main.initialize_config_values(self.args)

        initialize_tracking.assert_called_once_with(self.base_dir)
        do_not_track.assert_not_called()
        use_colors.assert_called_once_with()
