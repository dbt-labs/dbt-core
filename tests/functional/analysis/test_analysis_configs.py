import pytest

from dbt.tests.util import get_manifest, run_dbt

model_sql = """
select 1 as id
"""

analysis_sql = """
select * from {{ ref('my_model') }}
"""

analysis_enabled_true_sql = """
{{ config(enabled=true) }}
select * from {{ ref('my_model') }}
"""

second_analysis_sql = """
select 2 as id
"""

schema_yml = """
version: 2

analyses:
  - name: my_analysis
    description: "This is my analysis"
"""

schema_disabled_yml = """
version: 2

analyses:
  - name: my_analysis
    description: "This is my analysis"
    config:
      enabled: false
"""

schema_enabled_yml = """
version: 2

analyses:
  - name: my_analysis
    description: "This is my analysis"
    config:
      enabled: true
"""


# Test: project-level analyses +enabled: false disables all analyses
class TestAnalysisEnabledConfigProjectLevel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "my_analysis.sql": analysis_sql,
            "second_analysis.sql": second_analysis_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"analyses": {"+enabled": False}}

    def test_project_level_disabled(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "analysis.test.my_analysis" not in manifest.nodes
        assert "analysis.test.second_analysis" not in manifest.nodes
        assert "analysis.test.my_analysis" in [
            n.unique_id for n in manifest.disabled.values() for n in n
        ]


# Test: path-based config disables analyses in a specific subdirectory
class TestAnalysisPathConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "subdir": {"my_analysis.sql": analysis_sql},
            "second_analysis.sql": second_analysis_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"analyses": {"test": {"subdir": {"+enabled": False}}}}

    def test_path_based_disabled(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "analysis.test.my_analysis" not in manifest.nodes
        assert "analysis.test.second_analysis" in manifest.nodes


# Test: in-file config(enabled=true) overrides project-level disabled
class TestAnalysisInFileConfigOverridesProject:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "my_analysis.sql": analysis_enabled_true_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"analyses": {"+enabled": False}}

    def test_in_file_config_overrides_project(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "analysis.test.my_analysis" in manifest.nodes


# Test: YAML-level config disabled works
class TestAnalysisYamlLevelConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "my_analysis.sql": analysis_sql,
            "schema.yml": schema_disabled_yml,
        }

    def test_yaml_level_disabled(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "analysis.test.my_analysis" not in manifest.nodes


# Test: YAML-level enabled overrides project-level disabled
class TestAnalysisConfigInheritance:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": model_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "my_analysis.sql": analysis_sql,
            "schema.yml": schema_enabled_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"analyses": {"+enabled": False}}

    def test_yaml_overrides_project(self, project):
        run_dbt(["parse"])
        manifest = get_manifest(project.project_root)
        assert "analysis.test.my_analysis" in manifest.nodes
