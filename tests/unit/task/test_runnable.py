from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dbt.cli.flags import Flags
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.state import PreviousState
from dbt.exceptions import DbtRuntimeError
from dbt.task.runnable import GraphRunnableTask


class MockGraphRunnableTask(GraphRunnableTask):
    """Mock implementation of GraphRunnableTask for testing."""

    def run(self):
        pass

    def get_runner_type(self, _):
        pass

    def get_node_selector(self):
        pass


class TestGraphRunnableTaskStateErrors:
    """Test error messages when state manifest is not found."""

    def test_get_previous_state_with_missing_manifest_shows_proper_path(self):
        """
        Test that when a manifest is not found in the state path,
        the error message shows the actual file path instead of
        the object representation.
        """
        # Setup mocks
        mock_config = MagicMock(spec=RuntimeConfig)
        mock_config.target_path = "/test/target"
        mock_config.project_root = "/test/project"

        mock_manifest = MagicMock(spec=Manifest)

        mock_args = MagicMock(spec=Flags)
        mock_args.state = Path("does/not/exist")
        mock_args.defer = True
        mock_args.defer_state = None

        # Create task instance
        task = MockGraphRunnableTask(args=mock_args, config=mock_config, manifest=mock_manifest)

        # Create a PreviousState with no manifest
        mock_state = MagicMock(spec=PreviousState)
        mock_state.manifest = None
        mock_state.state_path = Path("does/not/exist")
        mock_state.project_root = Path("/test/path")

        # Set the task's previous_defer_state
        task.previous_defer_state = mock_state

        # Assert that the error message contains the full path
        with pytest.raises(DbtRuntimeError) as exc_info:
            task._get_previous_state()

        error_message = str(exc_info.value)
        # The error should contain the full path, not the object representation
        assert "/test/path/does/not/exist" in error_message
        assert "object at 0x" not in error_message
        assert "<dbt.contracts.state.PreviousState" not in error_message
        assert "Could not find manifest in --state path:" in error_message
