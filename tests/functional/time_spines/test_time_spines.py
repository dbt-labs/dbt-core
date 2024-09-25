from typing import Set

import pytest

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.semantic_manifest import SemanticManifest
from dbt.exceptions import ParsingError
from dbt.tests.util import get_manifest
from dbt_semantic_interfaces.type_enums import TimeGranularity
from tests.functional.time_spines.fixtures import (
    metricflow_time_spine_second_sql,
    metricflow_time_spine_sql,
    models_people_sql,
    semantic_model_people_yml,
    time_spine_missing_custom_column_yml,
    time_spine_missing_granularity_yml,
    time_spine_missing_standard_column_yml,
    valid_time_spines_yml,
)


class TestValidTimeSpines:
    """Tests that YAML using current time spine configs parses as expected."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "metricflow_time_spine_second.sql": metricflow_time_spine_second_sql,
            "time_spines.yml": valid_time_spines_yml,
            "semantic_model_people.yml": semantic_model_people_yml,
            "people.sql": models_people_sql,
        }

    def test_time_spines(self, project):
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert isinstance(result.result, Manifest)

        manifest = get_manifest(project.project_root)
        assert manifest

        # Test that models and columns are set as expected
        time_spine_models = {
            id.split(".")[-1]: node for id, node in manifest.nodes.items() if node.time_spine
        }
        day_model_name = "metricflow_time_spine"
        second_model_name = "metricflow_time_spine_second"
        day_column_name = "date_day"
        second_column_name = "ts_second"
        model_names_to_col_names = {
            day_model_name: day_column_name,
            second_model_name: second_column_name,
        }
        model_names_to_granularities = {
            day_model_name: TimeGranularity.DAY,
            second_model_name: TimeGranularity.SECOND,
        }
        assert len(time_spine_models) == 2
        expected_time_spine_aliases = {second_model_name, day_model_name}
        assert set(time_spine_models.keys()) == expected_time_spine_aliases
        for model in time_spine_models.values():
            assert (
                model.time_spine.standard_granularity_column
                == model_names_to_col_names[model.name]
            )
            if model.name == day_model_name:
                assert len(model.time_spine.custom_granularities) == 2
                assert {
                    custom_granularity.name
                    for custom_granularity in model.time_spine.custom_granularities
                } == {"retail_month", "martian_year"}
                for custom_granularity in model.time_spine.custom_granularities:
                    if custom_granularity.name == "martian_year":
                        assert custom_granularity.column_name == "martian__year_xyz"
            else:
                assert len(model.time_spine.custom_granularities) == 0
            assert len(model.columns) > 0
            assert (
                list(model.columns.values())[0].granularity
                == model_names_to_granularities[model.name]
            )

        # Test that project configs are set as expected in semantic manifest
        semantic_manifest = SemanticManifest(manifest)
        assert semantic_manifest.validate()
        project_config = semantic_manifest._get_pydantic_semantic_manifest().project_configuration
        # Legacy config
        assert len(project_config.time_spine_table_configurations) == 1
        legacy_time_spine_config = project_config.time_spine_table_configurations[0]
        assert legacy_time_spine_config.column_name == day_column_name
        assert legacy_time_spine_config.location.replace('"', "").split(".")[-1] == day_model_name
        assert legacy_time_spine_config.grain == TimeGranularity.DAY
        # Current configs
        assert len(project_config.time_spines) == 2
        sl_time_spine_aliases: Set[str] = set()
        for sl_time_spine in project_config.time_spines:
            alias = sl_time_spine.node_relation.alias
            sl_time_spine_aliases.add(alias)
            assert sl_time_spine.primary_column.name == model_names_to_col_names[alias]
            assert (
                sl_time_spine.primary_column.time_granularity
                == model_names_to_granularities[alias]
            )
        assert sl_time_spine_aliases == expected_time_spine_aliases


class TestValidLegacyTimeSpine:
    """Tests that YAML using only legacy time spine config parses as expected."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "semantic_model_people.yml": semantic_model_people_yml,
            "people.sql": models_people_sql,
        }

    def test_time_spines(self, project):
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert result.success
        assert isinstance(result.result, Manifest)

        manifest = get_manifest(project.project_root)
        assert manifest

        # Test that project configs are set as expected in semantic manifest
        semantic_manifest = SemanticManifest(manifest)
        assert semantic_manifest.validate()
        project_config = semantic_manifest._get_pydantic_semantic_manifest().project_configuration
        # Legacy config
        assert len(project_config.time_spine_table_configurations) == 1
        legacy_time_spine_config = project_config.time_spine_table_configurations[0]
        assert legacy_time_spine_config.column_name == "date_day"
        assert (
            legacy_time_spine_config.location.replace('"', "").split(".")[-1]
            == "metricflow_time_spine"
        )
        assert legacy_time_spine_config.grain == TimeGranularity.DAY
        # Current configs
        assert len(project_config.time_spines) == 0


class TestMissingTimeSpine:
    """Tests that YAML with semantic models but no time spines errors."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "semantic_model_people.yml": semantic_model_people_yml,
            "people.sql": models_people_sql,
        }

    def test_time_spines(self, project):
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert isinstance(result.exception, ParsingError)
        assert (
            "The semantic layer requires a time spine model with granularity DAY or smaller"
            in result.exception.msg
        )


class TestTimeSpineStandardColumnMissing:
    """Tests that YAML with time spine standard granularity column not in model errors."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "semantic_model_people.yml": semantic_model_people_yml,
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "metricflow_time_spine_second.sql": metricflow_time_spine_second_sql,
            "time_spines.yml": time_spine_missing_standard_column_yml,
        }

    def test_time_spines(self, project):
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert isinstance(result.exception, ParsingError)
        assert (
            "Time spine standard granularity column must be defined on the model."
            in result.exception.msg
        )


class TestTimeSpineCustomColumnMissing:
    """Tests that YAML with time spine custom granularity column not in model errors."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "semantic_model_people.yml": semantic_model_people_yml,
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "metricflow_time_spine_second.sql": metricflow_time_spine_second_sql,
            "time_spines.yml": time_spine_missing_custom_column_yml,
        }

    def test_time_spines(self, project):
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert isinstance(result.exception, ParsingError)
        assert (
            "Time spine custom granularity columns do not exist in the model."
            in result.exception.msg
        )


class TestTimeSpineGranularityMissing:
    """Tests that YAML with time spine column without granularity errors."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "semantic_model_people.yml": semantic_model_people_yml,
            "people.sql": models_people_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
            "metricflow_time_spine_second.sql": metricflow_time_spine_second_sql,
            "time_spines.yml": time_spine_missing_granularity_yml,
        }

    def test_time_spines(self, project):
        runner = dbtRunner()
        result = runner.invoke(["parse"])
        assert isinstance(result.exception, ParsingError)
        assert (
            "Time spine standard granularity column must have a granularity defined."
            in result.exception.msg
        )
