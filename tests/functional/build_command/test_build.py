import pytest

from dbt.tests.util import run_dbt
from tests.functional.build_command.fixtures import (
    models__model_0_sql,
    models__model_1_sql,
    models__model_2_sql,
    models__model_3_sql,
    models__model_99_sql,
    models__test_yml,
    models_circular_relationship__test_yml,
    models_failing__model_1_sql,
    models_failing_tests__tests_yml,
    models_interdependent__model_a_sql,
    models_interdependent__model_b_null_sql,
    models_interdependent__model_b_sql,
    models_interdependent__model_c_sql,
    models_interdependent__test_yml,
    models_simple_blocking__model_a_sql,
    models_simple_blocking__model_b_sql,
    models_simple_blocking__test_yml,
    models_triple_blocking__test_yml,
    seeds__country_csv,
    snapshots__snap_0,
    snapshots__snap_1,
    snapshots__snap_99,
    unit_tests__yml,
)


class TestBuildBase:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"countries.csv": seeds__country_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snap_0.sql": snapshots__snap_0,
            "snap_1.sql": snapshots__snap_1,
            "snap_99.sql": snapshots__snap_99,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }


class TestPassingBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_0.sql": models__model_0_sql,
            "model_1.sql": models__model_1_sql,
            "model_2.sql": models__model_2_sql,
            "model_3.sql": models__model_3_sql,
            "model_99.sql": models__model_99_sql,
            "test.yml": models__test_yml + unit_tests__yml,
        }

    def test_build_happy_path(self, project):
        run_dbt(["build"])


class TestFailingBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_0.sql": models__model_0_sql,
            "model_1.sql": models_failing__model_1_sql,
            "model_2.sql": models__model_2_sql,
            "model_3.sql": models__model_3_sql,
            "model_99.sql": models__model_99_sql,
            "test.yml": models__test_yml + unit_tests__yml,
        }

    def test_failing_test_skips_downstream(self, project):
        results = run_dbt(["build"], expect_pass=False)
        assert len(results) == 14
        actual = [str(r.status) for r in results]
        expected = ["error"] * 1 + ["skipped"] * 6 + ["pass"] * 2 + ["success"] * 5

        assert sorted(actual) == sorted(expected)


class TestFailingTestsBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_0.sql": models__model_0_sql,
            "model_1.sql": models__model_1_sql,
            "model_2.sql": models__model_2_sql,
            "model_99.sql": models__model_99_sql,
            "test.yml": models_failing_tests__tests_yml,
        }

    def test_failing_test_skips_downstream(self, project):
        results = run_dbt(["build"], expect_pass=False)
        assert len(results) == 13
        actual = [str(r.status) for r in results]
        expected = ["fail"] + ["skipped"] * 6 + ["pass"] * 2 + ["success"] * 4
        assert sorted(actual) == sorted(expected)


class TestCircularRelationshipTestsBuild(TestBuildBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_0.sql": models__model_0_sql,
            "model_1.sql": models__model_1_sql,
            "model_99.sql": models__model_99_sql,
            "test.yml": models_circular_relationship__test_yml,
        }

    def test_circular_relationship_test_success(self, project):
        """Ensure that tests that refer to each other's model don't create
        a circular dependency."""
        results = run_dbt(["build"])
        actual = [str(r.status) for r in results]
        expected = ["success"] * 7 + ["pass"] * 2

        assert sorted(actual) == sorted(expected)


class TestSimpleBlockingTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models_simple_blocking__model_a_sql,
            "model_b.sql": models_simple_blocking__model_b_sql,
            "test.yml": models_simple_blocking__test_yml,
        }

    def test_simple_blocking_test(self, project):
        """Ensure that a failed test on model_a always blocks model_b"""
        results = run_dbt(["build"], expect_pass=False)
        actual = [r.status for r in results]
        expected = ["success", "fail", "skipped"]
        assert sorted(actual) == sorted(expected)


class TestInterdependentModels:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"countries.csv": seeds__country_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models_interdependent__model_a_sql,
            "model_b.sql": models_interdependent__model_b_sql,
            "model_c.sql": models_interdependent__model_c_sql,
            "test.yml": models_interdependent__test_yml,
        }

    def test_interdependent_models(self, project):
        results = run_dbt(["build"])
        assert len(results) == 16


class TestInterdependentModelsFail:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"countries.csv": seeds__country_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models_interdependent__model_a_sql,
            "model_b.sql": models_interdependent__model_b_null_sql,
            "model_c.sql": models_interdependent__model_c_sql,
            "test.yml": models_interdependent__test_yml,
        }

    def test_interdependent_models_fail(self, project):
        results = run_dbt(["build"], expect_pass=False)
        assert len(results) == 16

        actual = [str(r.status) for r in results]
        expected = ["error"] * 4 + ["skipped"] * 7 + ["pass"] * 2 + ["success"] * 3
        assert sorted(actual) == sorted(expected)


class TestDownstreamSelection:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models_simple_blocking__model_a_sql,
            "model_b.sql": models_simple_blocking__model_b_sql,
            "test.yml": models_simple_blocking__test_yml,
        }

    def test_downstream_selection(self, project):
        """Ensure that selecting test+ does not select model_a's other children"""
        # fails with "Got 1 result, configured to fail if != 0"
        # model_a is defined as select null as id
        results = run_dbt(["build", "--select", "model_a not_null_model_a_id+"], expect_pass=False)
        assert len(results) == 2


class TestLimitedUpstreamSelection:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": models_interdependent__model_a_sql,
            "model_b.sql": models_interdependent__model_b_sql,
            "model_c.sql": models_interdependent__model_c_sql,
            "test.yml": models_triple_blocking__test_yml,
        }

    def test_limited_upstream_selection(self, project):
        """Ensure that selecting 1+model_c only selects up to model_b (+ tests of both)"""
        # Fails with "relation "test17005969872609282880_test_build.model_a" does not exist"
        results = run_dbt(["build", "--select", "1+model_c"], expect_pass=False)
        assert len(results) == 4
