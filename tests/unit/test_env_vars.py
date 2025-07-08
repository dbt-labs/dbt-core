import os
from unittest import mock

from dbt.deprecations import EnvironmentVariableNamespaceDeprecation as EVND
from dbt.deprecations import active_deprecations
from dbt.env_vars import validate_engine_env_vars
from dbt.events.types import EnvironmentVariableNamespaceDeprecation
from dbt_common.events.event_manager_client import add_callback_to_manager
from tests.utils import EventCatcher


@mock.patch.dict(
    os.environ,
    {
        "DBT_ENGINE_PARTIAL_PARSE": "False",
        "DBT_ENGINE_MY_CUSTOM_ENV_VAR_FOR_TESTING": "True",
    },
)
def test_validate_engine_env_vars():
    event_catcher = EventCatcher(event_to_catch=EnvironmentVariableNamespaceDeprecation)
    add_callback_to_manager(event_catcher.catch)

    validate_engine_env_vars()
    # If it's zero, then we _failed_ to notice the deprecation instance (and we should look why the custom engine env var wasn't noticed)
    # If it's more than one, then we're getting too many deprecation instances (and we should check what the other env vars identified were)
    assert active_deprecations[EVND().name] == 1
    assert (
        "DBT_ENGINE_MY_CUSTOM_ENV_VAR_FOR_TESTING" == event_catcher.caught_events[0].data.env_var
    )
