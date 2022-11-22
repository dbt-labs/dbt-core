import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

from tests.functional.store_test_failures_tests.fixtures import (
    seeds__people,
    seeds__expected_accepted_values,
    seeds__expected_failing_test,
    seeds__expected_not_null_problematic_model_id,
    seeds__expected_unique_problematic_model_id,
    properties__schema_yml,
    models__problematic_model,
    models__fine_model,
    models__file_model_but_with_a_no_good_very_long_name,
    tests__failing_test,
    tests__passing_test,
)


class TestStoreTestFailures:
    @pytest.fixture(scope="function")
    def clean_up(self, project):
        yield
        with project.adapter.connection_named('__test'):
            test_audit_schema = project.test_schema + "_dbt_test__aud"
            relation = project.adapter.Relation.create(database=project.database, schema=test_audit_schema)
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(database=project.database, schema=project.test_schema)
            project.adapter.drop_schema(relation)

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": seeds__people,
            "expected_accepted_values.csv": seeds__expected_accepted_values,
            "expected_failing_test.csv": seeds__expected_failing_test,
            "expected_not_null_problematic_model_id.csv":
                seeds__expected_not_null_problematic_model_id,
            "expected_unique_problematic_model_id.csv":
                seeds__expected_unique_problematic_model_id,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing_test.sql": tests__failing_test,
            "passing_test.sql": tests__passing_test,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": properties__schema_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fine_model.sql": models__fine_model,
            "fine_model_but_with_a_no_good_very_long_name.sql":
                models__file_model_but_with_a_no_good_very_long_name,
            "problematic_model.sql": models__problematic_model,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "test": self.column_type_overrides(),
            },
            "tests": {
                "+schema": "dbt_test__aud"  # max char limit
            }
        }

    def column_type_overrides(self):
        return {}

    def run_tests_store_one_failure(self, project):
        test_audit_schema = project.test_schema + "_dbt_test__aud"

        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["test"], expect_pass=False)

        # one test is configured with store_failures: true, make sure it worked
        check_relations_equal(
            project.adapter,
            [
                f"{test_audit_schema}.unique_problematic_model_id",
                "expected_unique_problematic_model_id"
            ]
        )

    def run_tests_store_failures_and_assert(self, project):
        test_audit_schema = project.test_schema + "_dbt_test__aud"
        project.created_schemas.append(test_audit_schema)

        run_dbt(["seed"])
        run_dbt(["run"])
        # make sure this works idempotently for all tests
        run_dbt(["test", "--store-failures"], expect_pass=False)
        results = run_dbt(["test", "--store-failures"], expect_pass=False)

        # compare test results
        actual = [(r.status, r.failures) for r in results]
        expected = [('pass', 0), ('pass', 0), ('pass', 0), ('pass', 0),
                    ('fail', 2), ('fail', 2), ('fail', 2), ('fail', 10)]
        assert sorted(actual) == sorted(expected)

        # compare test results stored in database
        check_relations_equal(
            project.adapter,
            [
                f"{test_audit_schema}.failing_test",
                "expected_failing_test"
            ]
        )
        check_relations_equal(
            project.adapter,
            [
                f"{test_audit_schema}.not_null_problematic_model_id",
                "expected_not_null_problematic_model_id"
            ]
        )
        check_relations_equal(
            project.adapter,
            [
                f"{test_audit_schema}.unique_problematic_model_id",
                "expected_unique_problematic_model_id"
            ]
        )
        check_relations_equal(
            project.adapter,
            [
                f"{test_audit_schema}.accepted_values_problemat"
                "ic_mo_c533ab4ca65c1a9dbf14f79ded49b628",
                "expected_accepted_values"
            ]
        )


class TestStoreTestFailures(TestStoreTestFailures):
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

    def test__store_and_assert(self, project, clean_up):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)
