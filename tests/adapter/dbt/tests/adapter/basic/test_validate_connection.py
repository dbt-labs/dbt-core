import dbt.task.debug


class BaseValidateConnection:
    def test_validate_connection(self, project, dbt_profile_data):

        dbt.task.debug.DebugTask.validate_connection(
            dbt_profile_data["test"]["outputs"]["default"]
        )


class TestValidateConnection(BaseValidateConnection):
    pass
