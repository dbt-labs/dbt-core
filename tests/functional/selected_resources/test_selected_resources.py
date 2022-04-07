import pytest
from dbt.tests.util import run_dbt
from tests.functional.selected_resources.fixtures import (
    on_run_start_macro_assert_selected_models_expected_list,
    my_model1,
    my_model2,
    my_snapshot,
)


@pytest.fixture(scope="class")
def macros():
    return {
        "assert_selected_models_expected_list.sql": on_run_start_macro_assert_selected_models_expected_list,
    }


@pytest.fixture(scope="class")
def models():
    return {"model1.sql": my_model1, "model2.sql": my_model2}


@pytest.fixture(scope="class")
def snapshots():
    return {
        "my_snapshot.sql": my_snapshot,
    }


@pytest.fixture(scope="class")
def project_config_update():
    return {
        "on-run-start": "{{ assert_selected_models_expected_list(var('expected_list',None)) }}",
    }


@pytest.fixture
def build_all(project):
    run_dbt(["build"])


@pytest.mark.usefixtures("build_all")
class TestSelectedResources:
    def test_selected_resources_build_selector(self, project):
        results = run_dbt(
            [
                "build",
                "--select",
                "model1+",
                "--vars",
                '{"expected_list": ["model.test.model1", "model.test.model2", "snapshot.test.cc_all_snapshot"]}',
            ]
        )
        assert results[0].status == "success"

    def test_selected_resources_build_selector_subgraph(self, project):
        results = run_dbt(
            [
                "build",
                "--select",
                "model2+",
                "--vars",
                '{"expected_list": ["model.test.model2", "snapshot.test.cc_all_snapshot"]}',
            ]
        )
        assert results[0].status == "success"

    def test_selected_resources_run(self, project):
        results = run_dbt(
            [
                "run",
                "--select",
                "model1+",
                "--vars",
                '{"expected_list": ["model.test.model2", "model.test.model1"]}',
            ]
        )
        assert results[0].status == "success"

    def test_selected_resources_build_no_selector(self, project):
        results = run_dbt(
            [
                "build",
                "--vars",
                '{"expected_list": ["model.test.model1", "model.test.model2", "snapshot.test.cc_all_snapshot"]}',
            ]
        )
        assert results[0].status == "success"

    def test_selected_resources_build_no_model(self, project):
        results = run_dbt(
            [
                "build",
                "--select",
                "model_that_does_not_exist",
                "--vars",
                '{"expected_list": []}',
            ]
        )
        assert not results

    def test_selected_resources_test_no_model(self, project):
        results = run_dbt(["test", "--select", "model1+", "--vars", '{"expected_list": []}'])
        assert not results
