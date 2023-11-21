import pytest
from dbt.exceptions import ParsingError, YamlParseDictError
from dbt.tests.util import run_dbt, write_file
from fixtures import (
    my_model_sql,
    my_model_a_sql,
    my_model_b_sql,
    test_my_model_csv_yml,
    datetime_test,
    datetime_test_invalid_format_key,
    datetime_test_invalid_csv_values,
    test_my_model_file_csv_yml,
    test_my_model_fixture_csv,
    test_my_model_a_fixture_csv,
    test_my_model_b_fixture_csv,
    test_my_model_basic_fixture_csv,
    test_my_model_a_numeric_fixture_csv,
    test_my_model_a_empty_fixture_csv,
    test_my_model_concat_fixture_csv,
    test_my_model_mixed_csv_yml,
    test_my_model_missing_csv_yml,
    test_my_model_duplicate_csv_yml,
)


class TestUnitTestsWithInlineCSV:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_csv_yml + datetime_test,
        }

    def test_unit_test(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Check error with invalid format key
        write_file(
            test_my_model_csv_yml + datetime_test_invalid_format_key,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(YamlParseDictError):
            results = run_dbt(["test", "--select", "my_model"], expect_pass=False)

        # Check error with csv format defined but dict on rows
        write_file(
            test_my_model_csv_yml + datetime_test_invalid_csv_values,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["test", "--select", "my_model"], expect_pass=False)


class TestUnitTestsWithFileCSV:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_file_csv_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "fixtures": {
                "test_my_model_fixture.csv": test_my_model_fixture_csv,
                "test_my_model_a_fixture.csv": test_my_model_a_fixture_csv,
                "test_my_model_b_fixture.csv": test_my_model_b_fixture_csv,
                "test_my_model_basic_fixture.csv": test_my_model_basic_fixture_csv,
                "test_my_model_a_numeric_fixture.csv": test_my_model_a_numeric_fixture_csv,
                "test_my_model_a_empty_fixture.csv": test_my_model_a_empty_fixture_csv,
                "test_my_model_concat_fixture.csv": test_my_model_concat_fixture_csv,
            }
        }

    def test_unit_test(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Check error with invalid format key
        write_file(
            test_my_model_file_csv_yml + datetime_test_invalid_format_key,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(YamlParseDictError):
            results = run_dbt(["test", "--select", "my_model"], expect_pass=False)

        # Check error with csv format defined but dict on rows
        write_file(
            test_my_model_file_csv_yml + datetime_test_invalid_csv_values,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["test", "--select", "my_model"], expect_pass=False)


class TestUnitTestsWithMixedCSV:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_mixed_csv_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "fixtures": {
                "test_my_model_fixture.csv": test_my_model_fixture_csv,
                "test_my_model_a_fixture.csv": test_my_model_a_fixture_csv,
                "test_my_model_b_fixture.csv": test_my_model_b_fixture_csv,
                "test_my_model_basic_fixture.csv": test_my_model_basic_fixture_csv,
                "test_my_model_a_numeric_fixture.csv": test_my_model_a_numeric_fixture_csv,
                "test_my_model_a_empty_fixture.csv": test_my_model_a_empty_fixture_csv,
                "test_my_model_concat_fixture.csv": test_my_model_concat_fixture_csv,
            }
        }

    def test_unit_test(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Check error with invalid format key
        write_file(
            test_my_model_mixed_csv_yml + datetime_test_invalid_format_key,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(YamlParseDictError):
            results = run_dbt(["test", "--select", "my_model"], expect_pass=False)

        # Check error with csv format defined but dict on rows
        write_file(
            test_my_model_mixed_csv_yml + datetime_test_invalid_csv_values,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["test", "--select", "my_model"], expect_pass=False)


class TestUnitTestsMissingCSVFile:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_missing_csv_yml,
        }

    def test_missing(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        expected_error = "Could not find fixture file fake_fixture for unit test"
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert expected_error in results[0].message


class TestUnitTestsDuplicateCSVFile:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_duplicate_csv_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "fixtures": {
                "one-folder": {
                    "test_my_model_basic_fixture.csv": test_my_model_basic_fixture_csv,
                },
                "another-folder": {
                    "test_my_model_basic_fixture.csv": test_my_model_basic_fixture_csv,
                },
            }
        }

    def test_duplicate(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        expected_error = "Found multiple fixture files named test_my_model_basic_fixture"
        assert expected_error in results[0].message
