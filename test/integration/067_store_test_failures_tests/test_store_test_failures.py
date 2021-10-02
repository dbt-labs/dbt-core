from test.integration.base import DBTIntegrationTest, use_profile


class TestStoreTestFailures(DBTIntegrationTest):
    @property
    def schema(self):
        return "test_store_test_failures_067"

    def tearDown(self):
        test_audit_schema = self.unique_schema() + "_dbt_test__audit"
        with self.adapter.connection_named('__test'):
            self._drop_schema_named(self.default_database, test_audit_schema)

        super().tearDown()

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "test-paths": ["tests"],
            "seeds": {
                "quote_columns": False,
                "test": {
                    "expected": self.column_type_overrides()
                },
            },
        }

    def column_type_overrides(self):
        return {}

    def run_tests_store_one_failure(self):
        test_audit_schema = self.unique_schema() + "_dbt_test__audit"

        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.run_dbt(["test"], expect_pass=False)

        # one test is configured with store_failures: true, make sure it worked
        self.assertTablesEqual("unique_problematic_model_id", "expected_unique_problematic_model_id", test_audit_schema)

    def run_tests_store_failures_and_assert(self):
        test_audit_schema = self.unique_schema() + "_dbt_test__audit"

        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        # make sure this works idempotently for all tests
        self.run_dbt(["test", "--store-failures"], expect_pass=False)
        results = self.run_dbt(["test", "--store-failures"], expect_pass=False)

        # compare test results
        actual = [(r.status, r.failures) for r in results]
        expected = [('pass', 0), ('pass', 0), ('pass', 0), ('pass', 0),
                    ('fail', 2), ('fail', 2), ('fail', 2), ('fail', 10)]
        self.assertEqual(sorted(actual), sorted(expected))

        # compare test results stored in database
        self.assertTablesEqual("failing_test", "expected_failing_test", test_audit_schema)
        self.assertTablesEqual("not_null_problematic_model_id", "expected_not_null_problematic_model_id", test_audit_schema)
        self.assertTablesEqual("unique_problematic_model_id", "expected_unique_problematic_model_id", test_audit_schema)
        self.assertTablesEqual("accepted_values_problematic_mo_c533ab4ca65c1a9dbf14f79ded49b628", "expected_accepted_values", test_audit_schema)


class PostgresTestStoreTestFailures(TestStoreTestFailures):

    @property
    def schema(self):
        return "067"  # otherwise too long + truncated

    def column_type_overrides(self):
        return {
            "expected_unique_problematic_model_id": {
                "+column_types": {
                    "n_records": "bigint",
                },
            },
            "expected_accepted_values": {
                "+column_types": {
                    "n_records": "bigint",
                },
            },
        }

    @use_profile('postgres')
    def test__postgres__store_and_assert(self):
        self.run_tests_store_one_failure()
        self.run_tests_store_failures_and_assert()
