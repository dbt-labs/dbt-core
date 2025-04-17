import yaml
from pytest_mock import MockerFixture

from dbt.deprecations import GenericJSONSchemaValidationDeprecation
from dbt.tests.util import run_dbt, write_file
from tests.utils import EventCatcher


class TestProjectJsonschemaValidatedOnlyOnce:
    """Ensure that the dbt_project.yml file is validated only once, even if it is 'loaded' multiple times"""

    def test_project(self, project, mocker: MockerFixture) -> None:
        mocked_jsonschema_validate = mocker.patch("dbt.config.project.jsonschema_validate")
        run_dbt(["parse"])
        assert mocked_jsonschema_validate.call_count == 1


class TestGenericJsonSchemaValidationDeprecation:
    """Ensure that the generic jsonschema validation deprecation can be fired"""

    def test_project(self, project, project_root: str) -> None:

        # `name` was already required prior to this deprecation, so this deprecation doesn't
        # really add anything. However, this test shows that jsonschema validation issues raise
        # deprecation warnings via the catchall `GenericJSONSchemaValidationDeprecation`
        project_missing_name = {
            "profile": "test",
            "flags": {"send_anonymous_usage_stats": False},
        }
        write_file(yaml.safe_dump(project_missing_name), project_root, "dbt_project.yml")
        event_catcher = EventCatcher(GenericJSONSchemaValidationDeprecation)

        try:
            run_dbt(["parse"], callbacks=[event_catcher.catch], expect_pass=False)
        except:  # noqa: E722
            pass

        assert len(event_catcher.caught_events) == 1
        assert "'name' is a required property at top level" in event_catcher.caught_events[0].info.msg  # type: ignore
