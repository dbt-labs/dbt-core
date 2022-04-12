import pytest
import os
from dbt.exceptions import CompilationException, ValidationException
from dbt.tests.util import run_dbt, check_table_does_exist, check_table_does_not_exist


def model(materialized, unique_schema=None):
    return f"""
    {{{{
      config(
        materialized = "{materialized}",
        schema = {f'"{unique_schema}"' if unique_schema is not None else "None"}
      )
    }}}}
    SELECT * FROM (
    VALUES (1, 'one'),
     (2, 'two'),
     (3, 'three')
    ) AS t (num,letter)
    """


class Base:
    materialized = "table"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model(self.materialized),
            "model_b.sql": model(self.materialized),
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
            "user": os.getenv("POSTGRES_TEST_USER", "root"),
            "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
            "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
        }


class TestMissingConfiguration(Base):
    def test_should_raise_exception(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )
        with pytest.raises(CompilationException):
            run_dbt(["--warn-error", "manage"])

        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

    def test_should_not_delete_anything(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )
        run_dbt(["manage"])

        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")


class TestUnmanagedSchema(TestMissingConfiguration):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "schema": "some_other_schema",
                    "prune-models": "drop",
                }
            ]
        }


class TestEmptyConfiguration(TestMissingConfiguration):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {"managed-schemas": []}


class TestWarn(TestMissingConfiguration):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "prune-models": "warn",
                }
            ]
        }


class TestDrop(Base):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "prune-models": "drop",
                }
            ]
        }

    def test(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )
        run_dbt(["manage"])

        check_table_does_not_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")


class TestDropView(TestDrop):
    materialized = "view"


class TestSkip(Base):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "prune-models": "skip",
                }
            ]
        }

    def test_should_not_raise_exception(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )

        run_dbt(["--warn-error", "manage"])

        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

    def test_should_not_delete_anything(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )
        run_dbt(["manage"])

        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")


class TestDefaultAction(TestSkip):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                }
            ]
        }


class TestCustomSchema(Base):
    custom_schema = "custom"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model(self.materialized, self.custom_schema),
            "model_b.sql": model(self.materialized, self.custom_schema),
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "prune-models": "drop",
                    "schema": self.custom_schema,
                }
            ]
        }

    def test(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, f"{self._generate_schema_name(project)}.model_a")
        check_table_does_exist(project.adapter, f"{self._generate_schema_name(project)}.model_b")

        project.update_models(
            {
                "model_a.sql": model(self.materialized),
                "model_b.sql": model(self.materialized, self.custom_schema),
            }
        )
        run_dbt(["manage"])

        check_table_does_not_exist(
            project.adapter, f"{self._generate_schema_name(project)}.model_a"
        )
        check_table_does_not_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, f"{self._generate_schema_name(project)}.model_b")

    def _generate_schema_name(self, project):
        return f"{project.test_schema}_{self.custom_schema}"


class TestDuplicateConfiguration(Base):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "prune-models": "drop",
                },
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "prune-models": "warn",
                },
            ]
        }

    def test(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )
        with pytest.raises(ValidationException):
            run_dbt(["manage"])

        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")
