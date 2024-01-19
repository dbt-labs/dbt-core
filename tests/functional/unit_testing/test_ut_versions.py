import pytest
from dbt.tests.util import run_dbt, get_unique_ids_in_results, write_file
from dbt.exceptions import YamlParseDictError, ParsingError

from tests.functional.unit_testing.fixtures import (
    my_model_versioned_yml,
    test_my_model_all_versions_yml,
    test_my_model_exclude_versions_yml,
    test_my_model_include_versions_yml,
    test_my_model_include_exclude_versions_yml,
    test_my_model_include_unversioned_yml,
    my_model_v1_sql,
    my_model_v2_sql,
    my_model_v3_sql,
    my_model_a_sql,
    my_model_b_sql,
    my_model_sql,
)


# test with no version specified, then add an exclude version, then switch
# to include version and make sure the right unit tests are generated for each
class TestVersions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "my_model_v1.sql": my_model_v1_sql,
            "my_model_v2.sql": my_model_v2_sql,
            "my_model_v3.sql": my_model_v3_sql,
            "schema.yml": my_model_versioned_yml,
            "unit_tests.yml": test_my_model_all_versions_yml,
        }

    def test_versions(self, project):
        results = run_dbt(["run"])
        assert len(results) == 5

        # "my_model" has three versions: 1, 2, 3
        # There is a single unit_test which doesn't specify a version,
        # so it should run for all versions.
        results = run_dbt(["test"])
        assert len(results) == 3
        unique_ids = get_unique_ids_in_results(results)
        expected_ids = [
            "unit_test.test.my_model.test_my_model_v1",
            "unit_test.test.my_model.test_my_model_v2",
            "unit_test.test.my_model.test_my_model_v3",
        ]
        assert sorted(expected_ids) == sorted(unique_ids)

        # with an exclude version specified, should create a separate unit test
        # for each version except the excluded version (v2)
        write_file(
            test_my_model_exclude_versions_yml, project.project_root, "models", "unit_tests.yml"
        )
        results = run_dbt(["run"])
        assert len(results) == 5

        results = run_dbt(["test"])
        assert len(results) == 2
        unique_ids = get_unique_ids_in_results(results)
        # v2 model should be excluded
        expected_ids = [
            "unit_test.test.my_model.test_my_model_v1",
            "unit_test.test.my_model.test_my_model_v3",
        ]
        assert sorted(expected_ids) == sorted(unique_ids)

        # test with an include version specified, should create a single unit test for
        # only the version specified (2)
        write_file(
            test_my_model_include_versions_yml, project.project_root, "models", "unit_tests.yml"
        )

        results = run_dbt(["run"])
        assert len(results) == 5

        results = run_dbt(["test"])
        assert len(results) == 1
        unique_ids = get_unique_ids_in_results(results)
        # v2 model should be only one included
        expected_ids = [
            "unit_test.test.my_model.test_my_model_v2",
        ]
        assert sorted(expected_ids) == sorted(unique_ids)


# test with an include and exclude version specified, should raise an error
class TestIncludeExcludeSpecified:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "my_model_v1.sql": my_model_v1_sql,
            "my_model_v2.sql": my_model_v2_sql,
            "my_model_v3.sql": my_model_v3_sql,
            "schema.yml": my_model_versioned_yml,
            "unit_tests.yml": test_my_model_include_exclude_versions_yml,
        }

    def test_include_exclude_specified(self, project):
        with pytest.raises(YamlParseDictError):
            run_dbt(["parse"])


# test with an include for an unversioned model, should error
class TestIncludeUnversioned:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "my_model.sql": my_model_sql,
            "unit_tests.yml": test_my_model_include_unversioned_yml,
        }

    def test_include_unversioned(self, project):
        with pytest.raises(ParsingError):
            run_dbt(["parse"])


# test with no version specified in the schema file and use selection logic for a specific version

# test specifying the fixture version with {{ ref(name, version) }}

# test changing the model versions and getting an error for the unit test referencing an old version
