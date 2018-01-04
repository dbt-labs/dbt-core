from mock import patch, MagicMock
import unittest

import dbt.version


class VersionTest(unittest.TestCase):

    @patch("dbt.version.__version__", "1.1.1")
    def test_versions_equal(self):

        dbt.version.get_remote_version_file_contents = MagicMock(
            return_value="""
                [bumpversion]
                current_version = 1.1.1
                commit = True
                tag = True

                [bumpversion:file:setup.py]

                [bumpversion:file:dbt/version.py]
            """)

        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "Installed version: 1.1.1\n" \
            "Current version: 1.1.1\n" \
            "Up to date!"

        assert isinstance(latest_version, dbt.version.Version)
        assert isinstance(installed_version, dbt.version.Version)
        self.assertTrue(latest_version.is_latest)
        self.assertFalse(installed_version.is_latest)
        self.assertEqual(latest_version, installed_version)
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    @patch("dbt.version.__version__", "1.12.1")
    def test_installed_version_greater(self):
        dbt.version.get_remote_version_file_contents = MagicMock(
            return_value="""
                [bumpversion]
                current_version = 1.1.12
                commit = True
                tag = True

                [bumpversion:file:setup.py]

                [bumpversion:file:dbt/version.py]
            """)

        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "Installed version: 1.12.1\n" \
            "Current version: 1.1.12\n" \
            "Your version is ahead!"

        assert installed_version > latest_version
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)

    @patch("dbt.version.__version__", "1.10.1a")
    def test_installed_version_lower(self):
        dbt.version.get_remote_version_file_contents = MagicMock(
            return_value="""
                [bumpversion]
                current_version = 2.0.113a
                commit = True
                tag = True

                [bumpversion:file:setup.py]

                [bumpversion:file:dbt/version.py]
            """)

        latest_version = dbt.version.get_latest_version()
        installed_version = dbt.version.get_installed_version()
        version_information = dbt.version.get_version_information()

        expected_version_information = "Installed version: 1.10.1a\n" \
            "Current version: 2.0.113a\n" \
            "Your version of dbt is out of date!\n" \
            "\tYou can find instructions for upgrading here:\n" \
            "\thttps://docs.getdbt.com/docs/installation"

        assert installed_version < latest_version
        self.assertMultiLineEqual(version_information,
                                  expected_version_information)
