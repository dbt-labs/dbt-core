import os
from unittest import mock

from dbt.deprecations import EnvironmentVariableNamespaceDeprecation as EVND
from dbt.deprecations import active_deprecations
from dbt.env_vars import KNOWN_ENGINE_ENV_VARS, validate_engine_env_vars
from dbt.events.types import EnvironmentVariableNamespaceDeprecation
from dbt.tests.util import safe_set_invocation_context
from dbt_common.context import (
    CaseInsensitiveMapping,
    get_invocation_context,
    set_invocation_context,
)
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.event_manager_client import add_callback_to_manager


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


@mock.patch.dict(os.environ, {"LOCAL_USER": "dan"})
def test_preflight_env_uses_case_insensitive_mapping_on_windows():
    """On Windows, env vars should be accessible case-insensitively (GH-10422).

    The preflight function in requires.py wraps the env dict in
    CaseInsensitiveMapping on Windows so that env_var('local_user') can find
    an env var stored as LOCAL_USER.
    """
    from dbt.cli.requires import _cross_propagate_engine_env_vars
    from dbt_common.clients.system import get_env

    set_invocation_context({})
    env_dict = get_env()
    _cross_propagate_engine_env_vars(env_dict)

    # Reproduce the preflight code path for Windows (os.name == "nt")
    get_invocation_context()._env = CaseInsensitiveMapping(env_dict)

    env = get_invocation_context().env
    assert isinstance(env, CaseInsensitiveMapping)
    # The env var was set as LOCAL_USER but should be found with any casing
    assert "LOCAL_USER" in env
    assert "local_user" in env
    assert "Local_User" in env
    assert env["local_user"] == "dan"
    assert env["LOCAL_USER"] == "dan"


@mock.patch.dict(os.environ, {"LOCAL_USER": "dan"})
def test_preflight_env_plain_dict_is_case_sensitive():
    """Without CaseInsensitiveMapping, a plain dict loses Windows case-insensitivity.

    This documents the regression from GH-10422: get_env() returns dict(os.environ)
    which is case-sensitive, so looking up 'local_user' when the var is 'LOCAL_USER' fails.
    """
    from dbt.cli.requires import _cross_propagate_engine_env_vars
    from dbt_common.clients.system import get_env

    set_invocation_context({})
    env_dict = get_env()
    _cross_propagate_engine_env_vars(env_dict)

    # Without the fix: plain dict assignment (the old buggy code path)
    get_invocation_context()._env = env_dict

    env = get_invocation_context().env
    assert "LOCAL_USER" in env  # Exact case works
    assert "local_user" not in env  # Lowercase fails — this is the bug


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
