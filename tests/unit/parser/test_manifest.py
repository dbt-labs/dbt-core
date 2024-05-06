from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.flags import set_from_args
from dbt.parser.manifest import ManifestLoader


@pytest.fixture
def mock_project():
    mock_project = MagicMock(RuntimeConfig)
    mock_project.cli_vars = {}
    mock_project.args = MagicMock()
    mock_project.args.profile = "test"
    mock_project.args.target = "test"
    mock_project.project_env_vars = {}
    mock_project.profile_env_vars = {}
    mock_project.project_target_path = "mock_target_path"
    mock_project.credentials = MagicMock()
    return mock_project


class TestPartialParse:
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

    def test_profile_hash_change(self, mock_project):
        # This test validate that the profile_hash is updated when the connection keys change
        profile_hash = "750bc99c1d64ca518536ead26b28465a224be5ffc918bf2a490102faa5a1bcf5"
        mock_project.credentials.connection_info.return_value = "test"
        set_from_args(Namespace(), {})
        manifest = ManifestLoader(mock_project, {})
        assert manifest.manifest.state_check.profile_hash.checksum == profile_hash
        mock_project.credentials.connection_info.return_value = "test1"
        manifest = ManifestLoader(mock_project, {})
        assert manifest.manifest.state_check.profile_hash.checksum != profile_hash


class TestFailedPartialParse:
    @patch("dbt.tracking.track_partial_parser")
    @patch("dbt.tracking.active_user")
    @patch("dbt.parser.manifest.PartialParsing")
    @patch("dbt.parser.manifest.ManifestLoader.read_manifest_for_partial_parse")
    @patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
    def test_partial_parse_safe_update_project_parser_files_partially(
        self,
        patched_state_check,
        patched_read_manifest_for_partial_parse,
        patched_partial_parsing,
        patched_active_user,
        patched_track_partial_parser,
    ):
        mock_instance = MagicMock()
        mock_instance.skip_parsing.return_value = False
        mock_instance.get_parsing_files.side_effect = KeyError("Whoopsie!")
        patched_partial_parsing.return_value = mock_instance

        mock_project = MagicMock(RuntimeConfig)
        mock_project.project_target_path = "mock_target_path"

        mock_saved_manifest = MagicMock(Manifest)
        mock_saved_manifest.files = {}
        patched_read_manifest_for_partial_parse.return_value = mock_saved_manifest

        set_from_args(Namespace(), {})
        loader = ManifestLoader(mock_project, {})
        loader.safe_update_project_parser_files_partially({})

        patched_track_partial_parser.assert_called_once()
        exc_info = patched_track_partial_parser.call_args[0][0]
        assert "traceback" in exc_info
        assert "exception" in exc_info
        assert "code" in exc_info
        assert "location" in exc_info
        assert "full_reparse_reason" in exc_info
        assert "KeyError: 'Whoopsie!'" == exc_info["exception"]
        assert isinstance(exc_info["code"], str) or isinstance(exc_info["code"], type(None))
