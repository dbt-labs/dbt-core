import pytest
import pathlib
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    write_file,
    rm_file,
    read_file,
)
from dbt.exceptions import DuplicateVersionedUnversionedError

model_one_sql = """
select 1 as fun
"""

model_one_downstream_sql = """
select fun from {{ ref('model_one') }}
"""

models_versions_schema_yml = """

models:
    - name: model_one
      description: "The first model"
      versions:
        - v: 1
        - v: 2
"""

models_versions_defined_in_schema_yml = """
models:
    - name: model_one
      description: "The first model"
      versions:
        - v: 1
        - v: 2
          defined_in: model_one_different
"""

models_versions_updated_schema_yml = """
models:
    - name: model_one
      latest_version: 1
      description: "The first model"
      versions:
        - v: 1
        - v: 2
          defined_in: model_one_different
"""

model_two_sql = """
select 1 as notfun
"""


class TestVersionedModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one_v1.sql": model_one_sql,
            "model_one.sql": model_one_sql,
            "model_one_downstream.sql": model_one_downstream_sql,
            "schema.yml": models_versions_schema_yml,
        }

    def test_pp_versioned_models(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        manifest = get_manifest(project.project_root)
        model_one_node = manifest.nodes["model.test.model_one.v1"]
        assert not model_one_node.is_latest_version
        model_two_node = manifest.nodes["model.test.model_one.v2"]
        assert model_two_node.is_latest_version
        # assert unpinned ref points to latest version
        model_one_downstream_node = manifest.nodes["model.test.model_one_downstream"]
        assert model_one_downstream_node.depends_on.nodes == ["model.test.model_one.v2"]

        # update schema.yml block - model_one is now 'defined_in: model_one_different'
        rm_file(project.project_root, "models", "model_one.sql")
        write_file(model_one_sql, project.project_root, "models", "model_one_different.sql")
        write_file(
            models_versions_defined_in_schema_yml, project.project_root, "models", "schema.yml"
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3

        # update versions schema.yml block - latest_version from 2 to 1
        write_file(
            models_versions_updated_schema_yml, project.project_root, "models", "schema.yml"
        )
        # This is where the test was failings in a CI run with:
        # relation \"test..._test_partial_parsing.model_one_downstream\" does not exist
        # because in core/dbt/include/global_project/macros/materializations/models/view/view.sql
        # "existing_relation" didn't actually exist by the time it gets to the rename of the
        # existing relation.
        (pathlib.Path(project.project_root) / "log_output").mkdir(parents=True, exist_ok=True)
        results = run_dbt(
            ["--partial-parse", "--log-format-file", "json", "--log-path", "log_output", "run"]
        )
        assert len(results) == 3

        manifest = get_manifest(project.project_root)
        model_one_node = manifest.nodes["model.test.model_one.v1"]
        assert model_one_node.is_latest_version
        model_two_node = manifest.nodes["model.test.model_one.v2"]
        assert not model_two_node.is_latest_version
        # assert unpinned ref points to latest version
        model_one_downstream_node = manifest.nodes["model.test.model_one_downstream"]
        assert model_one_downstream_node.depends_on.nodes == ["model.test.model_one.v1"]

        # assert unpinned ref to latest-not-max version yields an "FYI" info-level log
        log_output = read_file("log_output", "dbt.log").replace("\n", " ").replace("\\n", " ")
        assert "UnpinnedRefNewVersionAvailable" in log_output

        # update versioned model
        write_file(model_two_sql, project.project_root, "models", "model_one_different.sql")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 3
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 3

        # create a new model_one in model_one.sql and re-parse
        write_file(model_one_sql, project.project_root, "models", "model_one.sql")
        with pytest.raises(DuplicateVersionedUnversionedError):
            run_dbt(["parse"])
