import os
from typing import List
from unittest import mock

import pytest

from dbt.jsonschemas import validate_model_config
from dbt_common.events.event_manager_client import add_callback_to_manager


class TestValidateModelConfig:

    @pytest.fixture(scope="function")
    def caught_events(self):
        caught_events = []
        add_callback_to_manager(caught_events.append)
        return caught_events

    @mock.patch.dict(os.environ, {"DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS": "True"})
    def test_validate_model_config_no_error(self, caught_events: List):
        config = {
            "enabled": True,
        }
        validate_model_config(config, "test.yml")
        assert len(caught_events) == 0

    @mock.patch.dict(os.environ, {"DBT_ENV_PRIVATE_RUN_JSONSCHEMA_VALIDATIONS": "True"})
    def test_validate_model_config_error(self, caught_events: List):
        config = {
            "non_existent_config": True,
        }
        validate_model_config(config, "test.yml")
        assert len(caught_events) == 1
