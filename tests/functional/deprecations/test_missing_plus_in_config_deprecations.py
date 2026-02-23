from unittest import mock

import pytest

from dbt.events.types import MissingPlusPrefixDeprecation
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher


@mock.patch("dbt.jsonschemas.jsonschemas._JSONSCHEMA_SUPPORTED_ADAPTERS", {"postgres"})
class TestEmptyConfig:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": None}

    def test_no_warning(self, project):
        event_catcher = EventCatcher(MissingPlusPrefixDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0


@mock.patch("dbt.jsonschemas.jsonschemas._JSONSCHEMA_SUPPORTED_ADAPTERS", {"postgres"})
class TestEmptyNestedDir:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"nested_dir": None}}

    def test_no_warning(self, project):
        event_catcher = EventCatcher(MissingPlusPrefixDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0


@mock.patch("dbt.jsonschemas.jsonschemas._JSONSCHEMA_SUPPORTED_ADAPTERS", {"postgres"})
class TestValidConfigKey:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"docs": None}}

    def test_raises_warning(self, project):
        event_catcher = EventCatcher(MissingPlusPrefixDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1


@mock.patch("dbt.jsonschemas.jsonschemas._JSONSCHEMA_SUPPORTED_ADAPTERS", {"postgres"})
class TestValidPlusPrefixConfigKeyWithNonPlusPrefixProperty:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+docs": {"show": True}}}

    def test_no_warning(self, project):
        event_catcher = EventCatcher(MissingPlusPrefixDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0


@mock.patch("dbt.jsonschemas.jsonschemas._JSONSCHEMA_SUPPORTED_ADAPTERS", {"postgres"})
class TestValidPlusPrefixConfigKeyWithPlusPrefixInNestedDir:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "nested_dir_l1": {
                    "nested_dir_l2": {"+enabled": True},
                },
            },
        }

    def test_no_warning(self, project):
        event_catcher = EventCatcher(MissingPlusPrefixDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0


@mock.patch("dbt.jsonschemas.jsonschemas._JSONSCHEMA_SUPPORTED_ADAPTERS", {"postgres"})
class TestValidConfigKeyWithNonPlusPrefixInNestedDir:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "nested_dir_l1": {
                    "nested_dir_l2": {"enabled": True},
                },
            },
        }

    def test_raises_warning(self, project):
        event_catcher = EventCatcher(MissingPlusPrefixDeprecation)
        run_dbt(["parse", "--no-partial-parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1
