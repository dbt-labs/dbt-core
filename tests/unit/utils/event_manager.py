import pytest
from pytest_mock import MockerFixture

from dbt_common.events.event_manager import EventManager


@pytest.fixture
def mock_global_event_manager(mocker: MockerFixture) -> None:
    """Mocks the global _EVENT_MANAGER so that unit tests can safely modify it without worry about other tests."""
    mocker.patch("dbt_common.events.event_manager_client._EVENT_MANAGER", EventManager())
