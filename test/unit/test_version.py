from mock import patch, MagicMock
import unittest

import dbt.main
import dbt.version
import sys


class VersionTest(unittest.TestCase):

    @patch("dbt.version.__version__", "0.10.0")
    def test_versions_equal(self):

        dbt.version.get_remote_version_file_contents = MagicMock(
            return_value="""
                [bumpversion]
                current_version = 0.10.0
                commit = True
                tag = True

                [bumpversion:file:setup.py]

                [bumpversion:file:dbt/version.py]
            """)

        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "installed version: 0.10.0\n" \
            "   latest version: 0.10.0\n\n" \
            "Up to date!"

        self.assertEqual(latest_version, installed_version)
        self.assertEqual(latest_version, installed_version)
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    @patch("dbt.version.__version__", "0.10.2-a1")
    def test_installed_version_greater(self):
        dbt.version.get_remote_version_file_contents = MagicMock(
            return_value="""
                [bumpversion]
                current_version = 0.10.1
                commit = True
                tag = True

                [bumpversion:file:setup.py]

                [bumpversion:file:dbt/version.py]
            """)

        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "installed version: 0.10.2-a1\n" \
            "   latest version: 0.10.1\n\n" \
            "Your version of dbt is ahead of the latest release!"

        assert installed_version > latest_version
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    @patch("dbt.version.__version__", "0.9.5")
    def test_installed_version_lower(self):
        dbt.version.get_remote_version_file_contents = MagicMock(
            return_value="""
                [bumpversion]
                current_version = 0.10.0
                commit = True
                tag = True

                [bumpversion:file:setup.py]

                [bumpversion:file:dbt/version.py]
            """)

        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "installed version: 0.9.5\n" \
            "   latest version: 0.10.0\n\n" \
            "Your version of dbt is out of date! " \
            "You can find instructions for upgrading here:\n" \
            "https://docs.getdbt.com/docs/installation"

        assert installed_version < latest_version
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    # suppress having version info printed to the screen during tests.
    @patch('sys.stderr')
    def test_dbt_version_flag(self, stderr):
        dbt.version.get_remote_version_file_contents = MagicMock(
            return_value="""
                [bumpversion]
                current_version = 0.10.1
                commit = True
                tag = True

                [bumpversion:file:setup.py]

                [bumpversion:file:dbt/version.py]
            """)

        with self.assertRaises(SystemExit) as exc:
            dbt.main.handle_and_check(['--version'])
        self.assertEqual(exc.exception.code, 0)

