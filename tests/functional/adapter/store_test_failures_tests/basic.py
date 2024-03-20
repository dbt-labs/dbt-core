from collections import namedtuple
from typing import Set

import pytest

from dbt.artifacts.schemas.results import TestStatus
from dbt.tests.util import run_dbt, check_relation_types

from tests.functional.adapter.store_test_failures_tests import _files


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
        return {f"{self.seed_table}.csv": _files.SEED__CHIPMUNKS}

    @pytest.fixture(scope="class")
    def models(self):
        return {f"{self.model_table}.sql": _files.MODEL__CHIPMUNKS}

    def run_and_assert(
        self, project, expected_results: Set[TestResult], expect_pass: bool = False
    ) -> None:
        """
        Run `dbt test` and assert the results are the expected results

        Args:
            project: the `project` fixture; needed since we invoke `run_dbt`
            expected_results: the expected results of the tests as instances of TestResult
            expect_pass: passed directly into `run_dbt`; this is only needed if all expected results are tests that pass

        Returns:
            the row count as an integer
        """
        # run the tests
        results = run_dbt(["test"], expect_pass=expect_pass)

        # show that the statuses are what we expect
        actual = {(result.node.name, result.status) for result in results}
        expected = {(result.name, result.status) for result in expected_results}
        assert actual == expected

        # show that the results are persisted in the correct database objects
        check_relation_types(
            project.adapter, {result.name: result.type for result in expected_results}
        )


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
    - If `store_failures_as = "ephemeral"` and `store_failures = True`, then do not store the failures.
    - If `store_failures_as = "ephemeral"` and `store_failures = False`, then do not store the failures.
    - If `store_failures_as = "ephemeral"` and `store_failures` is not set, then do not store the failures.
    - If `store_failures_as` is not set and `store_failures = True`, then store the failures in a table.
    - If `store_failures_as` is not set and `store_failures = False`, then do not store the failures.
    - If `store_failures_as` is not set and `store_failures` is not set, then do not store the failures.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "view_unset_pass.sql": _files.TEST__VIEW_UNSET_PASS,  # control
            "view_true.sql": _files.TEST__VIEW_TRUE,
            "view_false.sql": _files.TEST__VIEW_FALSE,
            "view_unset.sql": _files.TEST__VIEW_UNSET,
            "table_true.sql": _files.TEST__TABLE_TRUE,
            "table_false.sql": _files.TEST__TABLE_FALSE,
            "table_unset.sql": _files.TEST__TABLE_UNSET,
            "ephemeral_true.sql": _files.TEST__EPHEMERAL_TRUE,
            "ephemeral_false.sql": _files.TEST__EPHEMERAL_FALSE,
            "ephemeral_unset.sql": _files.TEST__EPHEMERAL_UNSET,
            "unset_true.sql": _files.TEST__UNSET_TRUE,
            "unset_false.sql": _files.TEST__UNSET_FALSE,
            "unset_unset.sql": _files.TEST__UNSET_UNSET,
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
            TestResult("ephemeral_true", TestStatus.Fail, None),
            TestResult("ephemeral_false", TestStatus.Fail, None),
            TestResult("ephemeral_unset", TestStatus.Fail, None),
            TestResult("unset_true", TestStatus.Fail, "table"),
            TestResult("unset_false", TestStatus.Fail, None),
            TestResult("unset_unset", TestStatus.Fail, None),
        }
        self.run_and_assert(project, expected_results)


class StoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsBase):
    """
    These scenarios test that `store_failures_as` at the model level takes precedence over `store_failures`
    at the project level.

    Test Scenarios:

    - If `store_failures = False` in the project and `store_failures_as = "view"` in the model,
    then store the failures in a view.
    - If `store_failures = False` in the project and `store_failures_as = "table"` in the model,
    then store the failures in a table.
    - If `store_failures = False` in the project and `store_failures_as = "ephemeral"` in the model,
    then do not store the failures.
    - If `store_failures = False` in the project and `store_failures_as` is not set,
    then do not store the failures.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "results_view.sql": _files.TEST__VIEW_UNSET,
            "results_table.sql": _files.TEST__TABLE_UNSET,
            "results_ephemeral.sql": _files.TEST__EPHEMERAL_UNSET,
            "results_unset.sql": _files.TEST__UNSET_UNSET,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"data_tests": {"store_failures": False}}

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            TestResult("results_view", TestStatus.Fail, "view"),
            TestResult("results_table", TestStatus.Fail, "table"),
            TestResult("results_ephemeral", TestStatus.Fail, None),
            TestResult("results_unset", TestStatus.Fail, None),
        }
        self.run_and_assert(project, expected_results)


class StoreTestFailuresAsProjectLevelView(StoreTestFailuresAsBase):
    """
    These scenarios test that `store_failures_as` at the project level takes precedence over `store_failures`
    at the model level.

    Test Scenarios:

    - If `store_failures_as = "view"` in the project and `store_failures = False` in the model,
    then store the failures in a view.
    - If `store_failures_as = "view"` in the project and `store_failures = True` in the model,
    then store the failures in a view.
    - If `store_failures_as = "view"` in the project and `store_failures` is not set,
    then store the failures in a view.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "results_true.sql": _files.TEST__VIEW_TRUE,
            "results_false.sql": _files.TEST__VIEW_FALSE,
            "results_unset.sql": _files.TEST__VIEW_UNSET,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"data_tests": {"store_failures_as": "view"}}

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            TestResult("results_true", TestStatus.Fail, "view"),
            TestResult("results_false", TestStatus.Fail, "view"),
            TestResult("results_unset", TestStatus.Fail, "view"),
        }
        self.run_and_assert(project, expected_results)


class StoreTestFailuresAsProjectLevelEphemeral(StoreTestFailuresAsBase):
    """
    This scenario tests that `store_failures_as` at the project level takes precedence over `store_failures`
    at the model level. In particular, setting `store_failures_as = "ephemeral"` at the project level
    turns off `store_failures` regardless of the setting of `store_failures` anywhere. Turning `store_failures`
    back on at the model level requires `store_failures_as` to be set at the model level.

    Test Scenarios:

    - If `store_failures_as = "ephemeral"` in the project and `store_failures = True` in the project,
    then do not store the failures.
    - If `store_failures_as = "ephemeral"` in the project and `store_failures = True` in the project and the model,
    then do not store the failures.
    - If `store_failures_as = "ephemeral"` in the project and `store_failures_as = "view"` in the model,
    then store the failures in a view.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "results_unset.sql": _files.TEST__UNSET_UNSET,
            "results_true.sql": _files.TEST__UNSET_TRUE,
            "results_view.sql": _files.TEST__VIEW_UNSET,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"data_tests": {"store_failures_as": "ephemeral", "store_failures": True}}

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            TestResult("results_unset", TestStatus.Fail, None),
            TestResult("results_true", TestStatus.Fail, None),
            TestResult("results_view", TestStatus.Fail, "view"),
        }
        self.run_and_assert(project, expected_results)


class StoreTestFailuresAsGeneric(StoreTestFailuresAsBase):
    """
    This tests that `store_failures_as` works with generic tests.
    Test Scenarios:

    - If `store_failures_as = "view"` is used with the `not_null` test in the model, then store the failures in a view.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{self.model_table}.sql": _files.MODEL__CHIPMUNKS,
            "schema.yml": _files.SCHEMA_YML,
        }

    def test_tests_run_successfully_and_are_stored_as_expected(self, project):
        expected_results = {
            # `store_failures` unset, `store_failures_as = "view"`
            TestResult("not_null_chipmunks_name", TestStatus.Pass, "view"),
            # `store_failures = False`, `store_failures_as = "table"`
            TestResult(
                "accepted_values_chipmunks_name__alvin__simon__theodore", TestStatus.Fail, "table"
            ),
            # `store_failures = True`, `store_failures_as = "view"`
            TestResult("not_null_chipmunks_shirt", TestStatus.Fail, "view"),
        }
        self.run_and_assert(project, expected_results)


class StoreTestFailuresAsExceptions(StoreTestFailuresAsBase):
    """
    This tests that `store_failures_as` raises exceptions in appropriate scenarios.
    Test Scenarios:

    - If `store_failures_as = "error"`, a helpful exception is raised.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "store_failures_as_error.sql": _files.TEST__ERROR_UNSET,
        }

    def test_tests_run_unsuccessfully_and_raise_appropriate_exception(self, project):
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        result = results[0]
        assert "Compilation Error" in result.message
        assert "'error' is not a valid value" in result.message
        assert "Accepted values are: ['ephemeral', 'table', 'view']" in result.message
