import pytest

from dbt.tests.util import run_dbt, get_manifest
from tests.functional.exposures.fixtures import (
    models_sql,
    second_model_sql,
    simple_exposure_yml,
)


class TestBasicExposures:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "exposure.yml": simple_exposure_yml,
            "model.sql": models_sql,
            "second_model.sql": second_model_sql,
        }

    def test_names_with_spaces(self, project):
        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        exposure_ids = list(manifest.exposures.keys())
        expected_exposure_ids = [
            "exposure.test.simple_exposure",
            "exposure.test.notebook_exposure",
        ]
        assert exposure_ids == expected_exposure_ids
        assert manifest.exposures["exposure.test.simple_exposure"].label == "simple exposure label"
