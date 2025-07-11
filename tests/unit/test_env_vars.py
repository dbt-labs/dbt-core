import os
from unittest import mock

from dbt.deprecations import EnvironmentVariableNamespaceDeprecation as EVND
from dbt.deprecations import active_deprecations
from dbt.env_vars import KNOWN_ENGINE_ENV_VARS, validate_engine_env_vars
from dbt.events.types import EnvironmentVariableNamespaceDeprecation
from dbt.tests.util import safe_set_invocation_context
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
    safe_set_invocation_context()
    event_catcher = EventCatcher(event_to_catch=EnvironmentVariableNamespaceDeprecation)
    add_callback_to_manager(event_catcher.catch)

    validate_engine_env_vars()
    # If it's zero, then we _failed_ to notice the deprecation instance (and we should look why the custom engine env var wasn't noticed)
    # If it's more than one, then we're getting too many deprecation instances (and we should check what the other env vars identified were)
    assert active_deprecations[EVND().name] == 1
    assert (
        "DBT_ENGINE_MY_CUSTOM_ENV_VAR_FOR_TESTING" == event_catcher.caught_events[0].data.env_var
    )


def test_engine_env_vars_with_old_names_has_not_increased():
    engine_env_vars_with_old_names = sum(
        1 for env_var in KNOWN_ENGINE_ENV_VARS if env_var.old_name is not None
    )

    # This failing means we either:
    # 1. incorrectly created a new engine environment variable without using the `DBT_ENGINE` prefix
    # 2. we've identified, and added, an existing but previously unknown engine env var to the _ADDITIONAL_ENGINE_ENV_VARS list.
    # 3. we've _removed_ an existing engine env var with an old name (unlikely)
    #
    # In the case of (1), we should correct the new engine environent variable name
    # In the case of (2), we should increase the number here.
    # In the case of (3), we should decrease the number here.
    assert (
        engine_env_vars_with_old_names == 65
    ), "We've added a new engine env var _without_ using the new naming scheme"
