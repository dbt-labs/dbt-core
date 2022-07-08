import pytest
import os
from dbt.tests.util import (
    run_dbt_and_capture,
    get_manifest,
    relation_from_name,
    write_file,
    get_connection,
)
from dbt.context.base import BaseContext  # diff_of_two_dicts only

TEST_USER_ENV_VARS = ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"]

class BaseGrants:
    
    # TODO: this will be different on different adapters
    def format_grant_log_line(relation, user_name):
        return f"grant select on {relation} to {user_name};"
    
    @pytest.fixture(scope="class", autouse=True)
    def get_test_users(self, project):
        test_users = []
        for env_var in TEST_USER_ENV_VARS:
            user_name = os.getenv(env_var)
            if user_name:
                test_users.append(user_name)
        return test_users

    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            _, grant_table = adapter.execute(show_grant_sql, fetch=True)
            actual_grants = adapter.standardize_grants_dict(grant_table)
        return actual_grants
        
    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        # need a case-insensitive comparison
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = BaseContext.diff_of_two_dicts(actual_grants, expected_grants)
        diff_b = BaseContext.diff_of_two_dicts(expected_grants, actual_grants)
        assert diff_a == diff_b == {}
