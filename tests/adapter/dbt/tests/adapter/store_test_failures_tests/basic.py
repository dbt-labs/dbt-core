from collections import namedtuple

import pytest

from dbt.contracts.results import TestStatus
from dbt.tests.util import run_dbt, check_relation_types

from dbt.tests.adapter.store_test_failures_tests._files import (
    MODEL__CHIPMUNKS,
    SEED__CHIPMUNKS,
    TEST__NONE_FALSE,
    TEST__TABLE_FALSE,
    TEST__TABLE_TRUE,
    TEST__TABLE_UNSET,
    TEST__UNSET_FALSE,
    TEST__UNSET_TRUE,
    TEST__UNSET_UNSET,
    TEST__VIEW_FALSE,
    TEST__VIEW_TRUE,
    TEST__VIEW_UNSET,
    TEST__VIEW_UNSET_PASS,
)


TestResult = namedtuple("TestResult", ["name", "status", "type"])


class StoreTestFailuresAsBase:
    seed_table: str = "chipmunks_stage"
    model_table: str = "chipmunks"
    audit_schema_suffix: str = "dbt_test__audit"

    audit_schema: str

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, project):
        # the seed doesn't get touched, load it once
        run_dbt(["seed"])
        yield

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, project, setup_class):
        # make sure the model is always right
        run_dbt(["run"])

        # the name of the audit schema doesn't change in a class, but this doesn't run at the class level
        self.audit_schema = f"{project.test_schema}_{self.audit_schema_suffix}"
        yield

    @pytest.fixture(scope="function", autouse=True)
    def teardown_method(self, project):
        yield

        # clear out the audit schema after each test case
        with project.adapter.connection_named("__test"):
            audit_schema = project.adapter.Relation.create(
                database=project.database, schema=self.audit_schema
            )
            project.adapter.drop_schema(audit_schema)

    @pytest.fixture(scope="class")
    def seeds(self):
        return {f"{self.seed_table}.csv": SEED__CHIPMUNKS}

    @pytest.fixture(scope="class")
    def models(self):
        return {f"{self.model_table}.sql": MODEL__CHIPMUNKS}

    def row_count(self, project, relation_name: str) -> int:
        """
        Return the row count for the relation.

        Args:
            project: the project fixture
            relation_name: the name of the relation

        Returns:
            the row count as an integer
        """
        sql = f"select count(*) from {self.audit_schema}.{relation_name}"
        try:
            return project.run_sql(sql, fetch="one")[0]
        # this is the error we catch and re-raise in BaseAdapter
        except BaseException:
            return 0


class StoreTestFailuresAsInteractions(StoreTestFailuresAsBase):
    """
    These scenarios test interactions between `store_failures` and `store_failures_as` at the model level.
    Granularity (e.g. setting one at the project level and another at the model level) is not considered.

    Test Scenarios:

    - If `store_failures_as = "view"` and `store_failures = True`, then store the failures in a view.
    - If `store_failures_as = "view"` and `store_failures = False`, then store the failures in a view.
    - If `store_failures_as = "view"` and `store_failures` is not set, then store the failures in a view.
    - If `store_failures_as = "table"` and `store_failures = True`, then store the failures in a table.
    - If `store_failures_as = "table"` and `store_failures = False`, then store the failures in a table.
    - If `store_failures_as = "table"` and `store_failures` is not set, then store the failures in a table.
    - If `store_failures_as` is not set and `store_failures = True`, then store the failures in a table.
    - If `store_failures_as` is not set and `store_failures = False`, then do not store the failures.
    - If `store_failures_as` is not set and `store_failures` is not set, then do not store the failures.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "view_unset_pass.sql": TEST__VIEW_UNSET_PASS,  # control
            "view_true.sql": TEST__VIEW_TRUE,
            "view_false.sql": TEST__VIEW_FALSE,
            "view_unset.sql": TEST__VIEW_UNSET,
            "table_true.sql": TEST__TABLE_TRUE,
            "table_false.sql": TEST__TABLE_FALSE,
            "table_unset.sql": TEST__TABLE_UNSET,
            "unset_true.sql": TEST__UNSET_TRUE,
            "unset_false.sql": TEST__UNSET_FALSE,
            "unset_unset.sql": TEST__UNSET_UNSET,
        }

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            TestResult("view_unset_pass", TestStatus.Pass, "view"),  # control
            TestResult("view_true", TestStatus.Fail, "view"),
            TestResult("view_false", TestStatus.Fail, "view"),
            TestResult("view_unset", TestStatus.Fail, "view"),
            TestResult("table_true", TestStatus.Fail, "table"),
            TestResult("table_false", TestStatus.Fail, "table"),
            TestResult("table_unset", TestStatus.Fail, "table"),
            TestResult("unset_true", TestStatus.Fail, "table"),
            TestResult("unset_false", TestStatus.Fail, None),
            TestResult("unset_unset", TestStatus.Fail, None),
        }

        # run the tests
        results = run_dbt(["test"], expect_pass=False)

        # show that the statuses are what we expect
        actual = {(result.node.name, result.status) for result in results}
        expected = {(result.name, result.status) for result in expected_results}
        assert actual == expected

        # show that the results are persisted in the correct database objects
        check_relation_types(
            project.adapter, {result.name: result.type for result in expected_results}
        )


class StoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsBase):
    """
    These scenarios test that `store_failures_as` at the model level takes precedence over `store_failures`
    at the project level.

    Test Scenarios:

    - If `store_failures = False` in the project and `store_failures_as = "view"` in the model,
    then store the failures in a view.
    - If `store_failures = False` in the project and `store_failures_as = "table"` in the model,
    then store the failures in a table.
    - If `store_failures = False` in the project and `store_failures_as` is not set,
    then do not store the failures.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "results_view.sql": TEST__VIEW_UNSET,
            "results_table.sql": TEST__TABLE_UNSET,
            "results_unset.sql": TEST__UNSET_UNSET,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"tests": {"store_failures": False}}

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            TestResult("results_view", TestStatus.Fail, "view"),
            TestResult("results_table", TestStatus.Fail, "table"),
            TestResult("results_unset", TestStatus.Fail, None),
        }

        # run the tests
        results = run_dbt(["test"], expect_pass=False)

        # show that the statuses are what we expect
        actual = {(result.node.name, result.status) for result in results}
        expected = {(result.name, result.status) for result in expected_results}
        assert actual == expected

        # show that the results are persisted in the correct database objects
        check_relation_types(
            project.adapter, {result.name: result.type for result in expected_results}
        )


class StoreTestFailuresAsProjectLevelView(StoreTestFailuresAsBase):
    """
    These scenarios test that `store_failures_as` at the project level takes precedence over `store_failures`
    at the model level.

    Additionally, the fourth scenario demonstrates how to turn off `store_failures` at the model level
    when `store_failures_as` is used at the project level.

    Test Scenarios:

    - If `store_failures_as = "view"` in the project and `store_failures = False` in the model,
    then store the failures in a view.
    - If `store_failures_as = "view"` in the project and `store_failures = True` in the model,
    then store the failures in a view.
    - If `store_failures_as = "view"` in the project and `store_failures` is not set,
    then store the failures in a view.
    - If `store_failures_as = "view"` in the project and `store_failures = False` in the model
    and `store_failures_as = None` in the model, then do not store the failures.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "results_true.sql": TEST__VIEW_TRUE,
            "results_false.sql": TEST__VIEW_FALSE,
            "results_unset.sql": TEST__VIEW_UNSET,
            "results_turn_off.sql": TEST__NONE_FALSE,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"tests": {"store_failures_as": "view"}}

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            TestResult("results_true", TestStatus.Fail, "view"),
            TestResult("results_false", TestStatus.Fail, "view"),
            TestResult("results_unset", TestStatus.Fail, "view"),
            TestResult("results_turn_off", TestStatus.Fail, None),
        }

        # run the tests
        results = run_dbt(["test"], expect_pass=False)

        # show that the statuses are what we expect
        actual = {(result.node.name, result.status) for result in results}
        expected = {(result.name, result.status) for result in expected_results}
        assert actual == expected

        # show that the results are persisted in the correct database objects
        check_relation_types(
            project.adapter, {result.name: result.type for result in expected_results}
        )
