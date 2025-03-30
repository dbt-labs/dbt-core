import sys
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.sql import SQLConnectionManager
from dbt.config.runtime import RuntimeConfig
from dbt.context.providers import generate_runtime_macro_context
from dbt.contracts.graph.manifest import ManifestStateCheck
from dbt.mp_context import get_mp_context
from dbt.parser.manifest import ManifestLoader

if sys.version_info < (3, 9):
    from typing import Generator
else:
    from collections.abc import Generator


@pytest.fixture
def mock_connection_manager() -> MagicMock:
    mock_connection_manager = MagicMock(SQLConnectionManager)
    mock_connection_manager.set_query_header = lambda query_header_context: None
    return mock_connection_manager


@pytest.fixture
def mock_adapter(mock_connection_manager: MagicMock) -> MagicMock:
    mock_adapter = MagicMock(PostgresAdapter)
    mock_adapter.connections = mock_connection_manager
    mock_adapter.clear_macro_resolver = MagicMock()
    return mock_adapter


@pytest.fixture
def postgres_adapter(
    mocker: MockerFixture, runtime_config: RuntimeConfig
) -> Generator[PostgresAdapter, None, None]:
    register_adapter(runtime_config, get_mp_context())
    adapter = get_adapter(runtime_config)
    assert isinstance(adapter, PostgresAdapter)

    mocker.patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check").return_value = (
        ManifestStateCheck()
    )
    manifest = ManifestLoader.load_macros(
        runtime_config,
        adapter.connections.set_query_header,
        base_macros_only=True,
    )

    adapter.set_macro_resolver(manifest)
    adapter.set_macro_context_generator(generate_runtime_macro_context)

    yield adapter
    adapter.cleanup_connections()
    reset_adapters()
