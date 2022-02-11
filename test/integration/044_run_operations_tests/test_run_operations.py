from test.integration.base import DBTIntegrationTest, use_profile
import yaml


class TestOperations(DBTIntegrationTest):
    @property
    def schema(self):
        return "run_operations_044"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ['macros'],
        }

    def run_operation(self, macro, expect_pass=True, extra_args=None, **kwargs):
        args = ['run-operation', macro]
        if kwargs:
            args.extend(('--args', yaml.safe_dump(kwargs)))
        if extra_args:
            args.extend(extra_args)
        return self.run_dbt(args, expect_pass=expect_pass)

    @use_profile('postgres')
    def test__postgres_macro_noargs(self):
        self.run_operation('no_args')
        self.assertTableDoesExist('no_args')

    @use_profile('postgres')
    def test__postgres_macro_args(self):
        self.run_operation('table_name_args', table_name='my_fancy_table')
        self.assertTableDoesExist('my_fancy_table')

    @use_profile('postgres')
    def test__postgres_macro_exception(self):
        self.run_operation('syntax_error', False)

    @use_profile('postgres')
    def test__postgres_macro_missing(self):
        self.run_operation('this_macro_does_not_exist', False)

    @use_profile('postgres')
    def test__postgres_cannot_connect(self):
        self.run_operation('no_args',
                           extra_args=['--target', 'noaccess'],
                           expect_pass=False)

    @use_profile('postgres')
    def test__postgres_vacuum(self):
        self.run_dbt(['run'])
        # this should succeed
        self.run_operation('vacuum', table_name='model')

    @use_profile('postgres')
    def test__postgres_vacuum_ref(self):
        self.run_dbt(['run'])
        # this should succeed
        self.run_operation('vacuum_ref', ref_target='model')

    @use_profile('postgres')
    def test__postgres_select(self):
        self.run_operation('select_something', name='world')

    @use_profile('postgres')
    def test__postgres_access_graph(self):
        self.run_operation('log_graph')

    @use_profile('postgres')
    def test__postgres_print(self):
        # Tests that calling the `print()` macro does not cause an exception
        self.run_operation('print_something')
