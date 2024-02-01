import pytest
from dbt.exceptions import ParsingError, YamlParseDictError, DuplicateResourceNameError
from dbt.tests.util import run_dbt, write_file, rm_file
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

        manifest = run_dbt(["parse"])  # Note: this manifest is deserialized from msgpack
        fixture = manifest.fixtures["fixture.test.test_my_model_a_fixture"]
        fixture_source_file = manifest.files[fixture.file_id]
        assert fixture_source_file.fixture == "fixture.test.test_my_model_a_fixture"
        assert fixture_source_file.unit_tests == [
            "unit_test.test.my_model.test_my_model_string_concat"
        ]

        # Select by model name
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Check partial parsing remove fixture file
        rm_file(project.project_root, "tests", "fixtures", "test_my_model_a_fixture.csv")
        with pytest.raises(
            ParsingError,
            match="File not found for fixture 'test_my_model_a_fixture' in unit tests",
        ):
            run_dbt(["test", "--select", "my_model"], expect_pass=False)
        # put back file and check that it works
        write_file(
            test_my_model_a_fixture_csv,
            project.project_root,
            "tests",
            "fixtures",
            "test_my_model_a_fixture.csv",
        )
        results = run_dbt(["test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5
        # Now update file
        write_file(
            test_my_model_a_fixture_csv + "2,2",
            project.project_root,
            "tests",
            "fixtures",
            "test_my_model_a_fixture.csv",
        )
        manifest = run_dbt(["parse"])
        fixture = manifest.fixtures["fixture.test.test_my_model_a_fixture"]
        fixture_source_file = manifest.files[fixture.file_id]
        assert "2,2" in fixture_source_file.contents
        assert fixture.rows == [{"id": "1", "string_a": "a"}, {"id": "2", "string_a": "2"}]

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
        with pytest.raises(ParsingError):
            run_dbt(["run"])


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
        with pytest.raises(DuplicateResourceNameError):
            run_dbt(["run"])
