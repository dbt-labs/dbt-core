import unittest
from unittest.mock import patch, MagicMock

import dbt.main
import dbt.version


class VersionTest(unittest.TestCase):

    @patch("dbt.version.__version__", "0.10.0")
    @patch('dbt.version._get_dbt_plugins_info', autospec=True)
    @patch('dbt.version.requests.get')
    def test_versions_equal(self, mock_get, mock_get_dbt_plugins_info):
        mock_get.return_value.json.return_value = {
            'info': {'version': '0.10.0'}
        }
        mock_get_dbt_plugins_info.return_value = [
            ('dbt-postgres', '0.10.0'),
            ('dbt-redshift', '0.10.0'),
            ('dbt-bigquery', '0.10.0'),
            ('dbt-snowflake', '0.10.0')
        ]

        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "installed version: 0.10.0\n" \
            "   latest version: 0.10.0\n\n" \
            "Up to date!\n\n" \
            "Plugins:\n" \
            "  - dbt-postgres: 0.10.0\n" \
            "  - dbt-redshift: 0.10.0\n" \
            "  - dbt-bigquery: 0.10.0\n" \
            "  - dbt-snowflake: 0.10.0\n"

        self.assertEqual(latest_version, installed_version)
        self.assertEqual(latest_version, installed_version)
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    @patch("dbt.version.__version__", "0.10.2-a1")
    @patch('dbt.version._get_dbt_plugins_info', autospec=True)
    @patch('dbt.version.requests.get')
    def test_installed_version_greater(self, mock_get, mock_get_dbt_plugins_info):
        mock_get.return_value.json.return_value = {
            'info': {'version': '0.10.1'}
        }
        mock_get_dbt_plugins_info.return_value = [
            ('dbt-postgres', '0.10.0'),
            ('dbt-redshift', '0.10.0'),
            ('dbt-bigquery', '0.10.0'),
            ('dbt-snowflake', '0.10.0')
        ]
        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "installed version: 0.10.2-a1\n" \
            "   latest version: 0.10.1\n\n" \
            "Your version of dbt is ahead of the latest release!\n\n" \
            "Plugins:\n" \
            "  - dbt-postgres: 0.10.0\n" \
            "  - dbt-redshift: 0.10.0\n" \
            "  - dbt-bigquery: 0.10.0\n" \
            "  - dbt-snowflake: 0.10.0\n"

        assert installed_version > latest_version
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    @patch("dbt.version.__version__", "0.9.5")
    @patch('dbt.version._get_dbt_plugins_info', autospec=True)
    @patch('dbt.version.requests.get')
    def test_installed_version_lower(self, mock_get, mock_get_dbt_plugins_info):
        mock_get.return_value.json.return_value = {
            'info': {'version': '0.10.0'}
        }
        mock_get_dbt_plugins_info.return_value = [
            ('dbt-postgres', '0.10.0'),
            ('dbt-redshift', '0.10.0'),
            ('dbt-bigquery', '0.10.0'),
            ('dbt-snowflake', '0.10.0')
        ]
        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "installed version: 0.9.5\n" \
            "   latest version: 0.10.0\n\n" \
            "Your version of dbt is out of date! " \
            "You can find instructions for upgrading here:\n" \
            "https://docs.getdbt.com/docs/installation\n\n" \
            "Plugins:\n" \
            "  - dbt-postgres: 0.10.0\n" \
            "  - dbt-redshift: 0.10.0\n" \
            "  - dbt-bigquery: 0.10.0\n" \
            "  - dbt-snowflake: 0.10.0\n"

        assert installed_version < latest_version
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    # suppress having version info printed to the screen during tests.
    @patch('sys.stderr')
    @patch('dbt.version.requests.get')
    def test_dbt_version_flag(self, mock_get, stderr):
        mock_get.return_value.json.return_value = {
            'info': {'version': '0.10.1'}
        }

        with self.assertRaises(SystemExit) as exc:
            dbt.main.handle_and_check(['--version'])
        self.assertEqual(exc.exception.code, 0)

    @patch('glob.glob', autospec=True)
    @patch('importlib.util.find_spec', autospec=True)
    def test_get_adapter_plugin_names(self, mock_find_spec, mock_glob):
        mock_submodule = unittest.mock.MagicMock()
        mock_find_spec.return_value = mock_submodule
        mock_submodule.submodule_search_locations = ['/tmp/dbt/adapters']

        def glob_side_effect(path: str) -> str:
            return [
                path.replace('*', 'postgres'),
                path.replace('*', 'snowflake'),
            ]
        mock_glob.side_effect = glob_side_effect
        self.assertEqual(
            list(dbt.version._get_adapter_plugin_names()),
            ['postgres', 'snowflake'],
        )

        mock_find_spec.assert_called_once_with('dbt.adapters')

    @patch('glob.glob', autospec=True)
    @patch('importlib.util.find_spec', autospec=True)
    def test_get_adapter_plugin_names(self, mock_find_spec, mock_glob):
        mock_submodule = unittest.mock.MagicMock()
        mock_find_spec.return_value = mock_submodule
        mock_submodule.submodule_search_locations = [
            '/tmp/dbt/plugins/postgres/dbt/adapters/', '/tmp/dbt/plugins/snowflake/dbt/adapters/'
        ]

        def glob_side_effect(path: str) -> str:
            assert 'postgres' in path or 'snowflake' in path
            if 'postgres' in path:
                return [path.replace('*', 'postgres')]
            elif 'snowflake' in path:
                return [path.replace('*', 'snowflake')]
        mock_glob.side_effect = glob_side_effect
        self.assertEqual(
            list(dbt.version._get_adapter_plugin_names()),
            ['postgres', 'snowflake'],
        )

        mock_find_spec.assert_called_once_with('dbt.adapters')

    @patch('dbt.version._get_adapter_plugin_names', autospec=True)
    @patch('importlib.import_module', autospec=True)
    def test_get_dbt_plugins_info_with_version_info(
        self, mock_mod, mock_get_plugin_names
    ):
        mock_get_plugin_names.return_value = ['postgres', 'snowflake']
        mod_version = unittest.mock.Mock()
        mock_mod.return_value = mod_version
        mod_version.version = '1.0'
        self.assertEqual(
            list(dbt.version._get_dbt_plugins_info()),
            [('postgres', '1.0'), ('snowflake', '1.0')]
        )
        mock_get_plugin_names.assert_called_once_with()
        mock_mod.assert_has_calls([
            unittest.mock.call('dbt.adapters.postgres.__version__'),
            unittest.mock.call('dbt.adapters.snowflake.__version__')
        ])
