import pytest

import dbt.exceptions
from dbt.tests.util import (
    check_table_does_exist,
    check_table_does_not_exist,
    run_dbt,
    write_config_file
)
from tests.functional.exit_codes.fixtures import (
    BaseConfigProject,
    snapshots_bad_sql,
    snapshots_good_sql,
    data_seed_bad_csv,
    data_seed_good_csv
)


class TestExitCodes(BaseConfigProject):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"g.sql": snapshots_good_sql}

    def test_postgres_exit_code_run_succeed(self, project):
        results = run_dbt(['run', '--model', 'good'])
        assert len(results) == 1
        check_table_does_exist(project.adapter, 'good')

    def test_postgres_exit_code_run_fail(self, project):
        results = run_dbt(['run', '--model', 'bad'], expect_pass=False)
        assert len(results) == 1
        check_table_does_not_exist(project.adapter, 'bad')

    def test_postgres_schema_test_pass(self, project):
        results = run_dbt(['run', '--model', 'good'])
        assert len(results) == 1

        results = run_dbt(['test', '--model', 'good'])
        assert len(results) == 1

    def test_postgres_schema_test_fail(self, project):
        results = run_dbt(['run', '--model', 'dupe'])
        assert len(results) == 1

        results = run_dbt(['test', '--model', 'dupe'], expect_pass=False)
        assert len(results) == 1

    def test_postgres_compile(self, project):
        results = run_dbt(['compile'])
        assert len(results) == 7

    def test_postgres_snapshot_pass(self, project):
        results = run_dbt(['snapshot'])
        assert len(results) == 1
        check_table_does_exist(project.adapter, 'good_snapshot')


class TestExitCodesSnapshotFail(BaseConfigProject):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"b.sql": snapshots_bad_sql}

    def test_postgres_snapshot_fail(self, project):
        results = run_dbt(['run', '--model', 'good'])
        assert len(results) == 1

        results = run_dbt(['snapshot'], expect_pass=False)
        assert len(results) == 1
        check_table_does_not_exist(project.adapter, 'good_snapshot')


class TestExitCodesDeps:
    @pytest.fixture(scope="class")
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'dbt/1.0.0',
                }
            ]
        }

    def test_postgres_deps(self, project, packages_config):
        write_config_file(packages_config, project.project_root, "packages.yml")

        results = run_dbt(['deps'])
        assert results is None


class TestExitCodesDepsFail:
    @pytest.fixture(scope="class")
    def packages_config(self):
        return {
            "packages": [
                {
                    'git': 'https://github.com/dbt-labs/dbt-integration-project',
                    'revision': 'bad-branch',
                },
            ]
        }

    def test_postgres_deps_fail(self, project, packages_config):
        write_config_file(packages_config, project.project_root, "packages.yml")

        with pytest.raises(dbt.exceptions.InternalException) as exc:
            run_dbt(['deps'])
        expected_msg = "Error checking out spec='bad-branch'"
        assert expected_msg in str(exc.value)


class TestExitCodesSeed:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"good.csv": data_seed_good_csv}

    def test_postgres_seed(self, project):
        results = run_dbt(['seed'])
        assert len(results) == 1


class TestExitCodesSeedFail:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"bad.csv": data_seed_bad_csv}

    def test_postgres_seed(self, project):
        run_dbt(['seed'], expect_pass=False)
