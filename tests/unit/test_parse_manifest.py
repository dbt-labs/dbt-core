import unittest
from unittest import mock
from unittest.mock import patch, MagicMock
from argparse import Namespace

from .utils import config_from_parts_or_dicts, normalize

from dbt.contracts.files import SourceFile, FileHash, FilePath
from dbt.contracts.graph.manifest import Manifest, ManifestStateCheck
from dbt.parser import manifest
from dbt.parser.manifest import ManifestLoader
from dbt.config import RuntimeConfig
from dbt.flags import set_from_args


class MatchingHash(FileHash):
    def __init__(self):
        return super().__init__("", "")

    def __eq__(self, other):
        return True


class MismatchedHash(FileHash):
    def __init__(self):
        return super().__init__("", "")

    def __eq__(self, other):
        return False


class TestLoader(unittest.TestCase):
    def setUp(self):
        profile_data = {
            "target": "test",
            "quoting": {},
            "outputs": {
                "test": {
                    "type": "postgres",
                    "host": "localhost",
                    "schema": "analytics",
                    "user": "test",
                    "pass": "test",
                    "dbname": "test",
                    "port": 1,
                }
            },
        }

        root_project = {
            "name": "root",
            "version": "0.1",
            "profile": "test",
            "project-root": normalize("/usr/src/app"),
            "config-version": 2,
        }

        self.root_project_config = config_from_parts_or_dicts(
            project=root_project, profile=profile_data, cli_vars='{"test_schema_name": "foo"}'
        )
        self.parser = mock.MagicMock()

        # Create the Manifest.state_check patcher
        @patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
        def _mock_state_check(self):
            all_projects = self.all_projects
            return ManifestStateCheck(
                vars_hash=FileHash.from_contents("vars"),
                project_hashes={name: FileHash.from_contents(name) for name in all_projects},
                profile_hash=FileHash.from_contents("profile"),
            )

        self.load_state_check = patch(
            "dbt.parser.manifest.ManifestLoader.build_manifest_state_check"
        )
        self.mock_state_check = self.load_state_check.start()
        self.mock_state_check.side_effect = _mock_state_check

        self.loader = manifest.ManifestLoader(
            self.root_project_config, {"root": self.root_project_config}
        )

    def _new_manifest(self):
        state_check = ManifestStateCheck(MatchingHash(), MatchingHash, [])
        manifest = Manifest({}, {}, {}, {}, {}, {}, [], {})
        manifest.state_check = state_check
        return manifest

    def _mismatched_file(self, searched, name):
        return self._new_file(searched, name, False)

    def _matching_file(self, searched, name):
        return self._new_file(searched, name, True)

    def _new_file(self, searched, name, match):
        if match:
            checksum = MatchingHash()
        else:
            checksum = MismatchedHash()
        path = FilePath(
            searched_path=normalize(searched),
            relative_path=normalize(name),
            project_root=normalize(self.root_project_config.project_root),
        )
        return SourceFile(path=path, checksum=checksum)


class TestPartialParse(unittest.TestCase):
    @patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
    @patch("dbt.parser.manifest.os.path.exists")
    @patch("dbt.parser.manifest.open")
    def test_partial_parse_file_path(self, patched_open, patched_os_exist, patched_state_check):
        mock_project = MagicMock(RuntimeConfig)
        mock_project.project_target_path = "mock_target_path"
        patched_os_exist.return_value = True
        set_from_args(Namespace(), {})
        ManifestLoader(mock_project, {})
        # by default we use the project_target_path
        patched_open.assert_called_with("mock_target_path/partial_parse.msgpack", "rb")
        set_from_args(Namespace(partial_parse_file_path="specified_partial_parse_path"), {})
        ManifestLoader(mock_project, {})
        # if specified in flags, we use the specified path
        patched_open.assert_called_with("specified_partial_parse_path", "rb")
