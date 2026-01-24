import pytest

from dbt.deprecations import (
    CustomKeyInConfigDeprecation,
    CustomKeyInObjectDeprecation,
    GenericJSONSchemaValidationDeprecation,
    active_deprecations,
    reset_deprecations,
)
from dbt.jsonschemas.jsonschemas import (
    jsonschema_validate,
    resources_schema,
    validate_model_config,
)
from dbt.tests.util import safe_set_invocation_context
from dbt_common.context import get_invocation_context
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.event_manager_client import add_callback_to_manager


class TestValidateModelConfigNoError:
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


class TestValidateJsonSchema:
    @pytest.fixture(scope="class")
    def model_bigquery_alias_config_contents(self):
        return {
            "models": [
                {
                    "name": "model_1",
                    "config": {
                        "dataset": "dataset_1",
                        "project": "project_1",
                    },
                }
            ],
        }

    def test_validate_json_schema_no_error_aliases(self, model_bigquery_alias_config_contents):
        reset_deprecations()

        safe_set_invocation_context()
        get_invocation_context().uses_adapter("bigquery")

        jsonschema_validate(resources_schema(), model_bigquery_alias_config_contents, "test.yml")
        assert active_deprecations == {}

    def test_validate_json_schema_has_error_aliases(self, model_bigquery_alias_config_contents):
        reset_deprecations()

        safe_set_invocation_context()
        # Set to adapter that doesn't support aliases specified
        get_invocation_context().uses_adapter("snowflake")

        jsonschema_validate(resources_schema(), model_bigquery_alias_config_contents, "test.yml")
        assert active_deprecations == {"custom-key-in-config-deprecation": 2}
