import os
import pytest
import yaml

from dbt.tests.util import (
    check_table_does_exist,
    run_dbt
)
from tests.functional.run_operations.fixtures import (
    happy_macros_sql,
    sad_macros_sql,
    model_sql
)

class TestOperations:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}
    
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "happy_macros.sql": happy_macros_sql,
            "sad_macros.sql": sad_macros_sql
        }

    @pytest.fixture(scope="class")
    def dbt_profile_data(self, unique_schema):
        return {
            "config": {"send_anonymous_usage_stats": False},
            "test": {
                "outputs": {
                    "default": {
                        "type": "postgres",
                        "threads": 4,
                        "host": "localhost",
                        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                        "user": os.getenv("POSTGRES_TEST_USER", "root"),
                        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
                        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                        "schema": unique_schema,
                    },
                    "noaccess": {
                        "type": "postgres",
                        "threads": 4,
                        "host": "localhost",
                        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
                        "user": 'noaccess',
                        "pass": 'password',
                        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                        'schema': unique_schema
                    }
                },
                "target": "default",
            },
        }

    def run_operation(self, macro, expect_pass=True, extra_args=None, **kwargs):
        args = ['run-operation', macro]
        if kwargs:
            args.extend(('--args', yaml.safe_dump(kwargs)))
        if extra_args:
            args.extend(extra_args)
        return run_dbt(args, expect_pass=expect_pass)
    
    def test_macro_noargs(self, project):
        self.run_operation('no_args')
        check_table_does_exist(project.adapter, 'no_args')

    def test_macro_args(self, project):
        self.run_operation('table_name_args', table_name='my_fancy_table')
        check_table_does_exist(project.adapter, 'my_fancy_table')
    
    def test_macro_exception(self, project):
        self.run_operation('syntax_error', False)
    
    def test_macro_missing(self, project):
        self.run_operation('this_macro_does_not_exist', False)
    
    def test_cannot_connect(self, project):
        self.run_operation('no_args',
                           extra_args=['--target', 'noaccess'],
                           expect_pass=False)
    
    def test_vacuum(self, project):
        run_dbt(['run'])
        # this should succeed
        self.run_operation('vacuum', table_name='model')
    
    def test_vacuum_ref(self, project):
        run_dbt(['run'])
        # this should succeed
        self.run_operation('vacuum_ref', ref_target='model')

    def test_select(self, project):
        self.run_operation('select_something', name='world')

    def test_access_graph(self, project):
        self.run_operation('log_graph')

    def test_print(self, project):
        # Tests that calling the `print()` macro does not cause an exception
        self.run_operation('print_something')