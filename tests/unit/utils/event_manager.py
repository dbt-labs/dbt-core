import pytest

from dbt_common.events.event_manager_client import cleanup_event_logger


@pytest.fixture(autouse=True)
def always_clean_event_manager() -> None:
    cleanup_event_logger()
