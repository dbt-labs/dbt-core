import os
from unittest import mock

from dbt.deprecations import (
    CustomKeyInConfigDeprecation,
    CustomKeyInObjectDeprecation,
    GenericJSONSchemaValidationDeprecation,
)
from dbt.jsonschemas import validate_model_config
from dbt.tests.util import safe_set_invocation_context
from dbt_common.context import get_invocation_context
from dbt_common.events.event_manager_client import add_callback_to_manager
from tests.utils import EventCatcher


class TestValidateModelConfigNoError:
    @mock.patch.dict(os.environ, {"DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS": "True"})
    def test_validate_model_config_no_error(self):
        safe_set_invocation_context()
        get_invocation_context().uses_adapter("snowflake")
        caught_events = []
        add_callback_to_manager(caught_events.append)

        config = {
            "enabled": True,
        }
        validate_model_config(config, "test.yml")
        assert len(caught_events) == 0

    @mock.patch.dict(os.environ, {"DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS": "True"})
    def test_validate_model_config_error(self):
        safe_set_invocation_context()
        get_invocation_context().uses_adapter("snowflake")
        ckiod_catcher = EventCatcher(CustomKeyInObjectDeprecation)
        ckicd_catcher = EventCatcher(CustomKeyInConfigDeprecation)
        gjsvd_catcher = EventCatcher(GenericJSONSchemaValidationDeprecation)
        add_callback_to_manager(ckiod_catcher.catch)
        add_callback_to_manager(ckicd_catcher.catch)
        add_callback_to_manager(gjsvd_catcher.catch)

        config = {
            "non_existent_config": True,  # this config key doesn't exist
            "docs": {
                "show": True,  # this is a valid config key
                "color": "red",  # this is an invalid config key, as it should be `node_color`
            },
        }

        validate_model_config(config, "test.yml")

        assert len(ckiod_catcher.caught_events) == 1
        assert ckiod_catcher.caught_events[0].data.key == "color"
        assert len(ckicd_catcher.caught_events) == 1
        assert ckicd_catcher.caught_events[0].data.key == "non_existent_config"
        assert len(gjsvd_catcher.caught_events) == 0
