from unittest.mock import MagicMock

import pytest

from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.sql import SQLConnectionManager


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
