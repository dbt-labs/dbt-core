from typing import List, Optional, Set

from dbt import deprecations
from dbt.constants import (
    LEGACY_TIME_SPINE_GRANULARITY,
    LEGACY_TIME_SPINE_MODEL_NAME,
    MINIMUM_REQUIRED_TIME_SPINE_GRANULARITY,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.events.types import ArtifactWritten, SemanticValidationFailure
from dbt.exceptions import ParsingError
from dbt.flags import get_flags
from dbt_common.clients.system import write_file
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_semantic_interfaces.implementations.metric import PydanticMetric
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.implementations.time_spine import (
    PydanticTimeSpine,
    PydanticTimeSpineCustomGranularityColumn,
    PydanticTimeSpinePrimaryColumn,
)
from dbt_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration as LegacyTimeSpine,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    ValidationError,
    ValidationIssueContext,
)


class SemanticManifest:
    def __init__(self, manifest: Manifest) -> None:
        self.manifest = manifest

    def validate(self) -> bool:

        # TODO: Enforce this check.
        # if self.manifest.metrics and not self.manifest.semantic_models:
        #    fire_event(
        #        SemanticValidationFailure(
        #            msg="Metrics require semantic models, but none were found."
        #        ),
        #        EventLevel.ERROR,
        #    )
        #    return False

        if not self.manifest.metrics or not self.manifest.semantic_models:
            return True

        semantic_manifest = self._get_pydantic_semantic_manifest()
        validator = SemanticManifestValidator[PydanticSemanticManifest]()
        validation_results = validator.validate_semantic_manifest(semantic_manifest)
        validation_result_errors = list(validation_results.errors)

        metrics_using_old_params: Set[str] = set()
        for metric in semantic_manifest.metrics or []:
            for field in ("window", "grain_to_date"):
                type_params_field_value = getattr(metric.type_params, field)
                # Warn that the old type_params structure has been deprecated.
                if type_params_field_value:
                    metrics_using_old_params.add(metric.name)
        if metrics_using_old_params:
            if get_flags().require_nested_cumulative_type_params is False:
                deprecations.warn(
                    "mf-cumulative-type-params-deprecation",
                )
            else:
                names = ", ".join(metrics_using_old_params)
                validation_result_errors.append(
                    ValidationError(
                        context=ValidationIssueContext(
                            # We don't have the file context at this point.
                            file_context=FileContext(),
                            object_name=names,
                            object_type="metric",
                        ),
                        message=f"Cumulative fields `type_params.window` and `type_params.grain_to_date` should be nested under `type_params.cumulative_type_params.window` and `type_params.cumulative_type_params.grain_to_date`. Invalid metrics: {names}. See documentation on behavior changes: https://docs.getdbt.com/reference/global-configs/behavior-changes.",
                    )
                )

        time_spines = semantic_manifest.project_configuration.time_spines
        legacy_time_spines = (
            semantic_manifest.project_configuration.time_spine_table_configurations
        )
        # If the time spine contains a day grain then it is functionally equivalent to the legacy time spine.
        time_spines_contain_day = any(
            c for c in time_spines if c.primary_column.time_granularity == TimeGranularity.DAY
        )
        if (
            get_flags().require_yaml_configuration_for_mf_time_spines is False
            and legacy_time_spines
            and not time_spines_contain_day
        ):
            deprecations.warn(
                "mf-timespine-without-yaml-configuration",
            )

        for warning in validation_results.warnings:
            fire_event(SemanticValidationFailure(msg=warning.message))

        for error in validation_result_errors:
            fire_event(SemanticValidationFailure(msg=error.message), EventLevel.ERROR)

        return not validation_result_errors

    def write_json_to_file(self, file_path: str):
        semantic_manifest = self._get_pydantic_semantic_manifest()
        json = semantic_manifest.json()
        write_file(file_path, json)
        fire_event(ArtifactWritten(artifact_type=self.__class__.__name__, artifact_path=file_path))

    def _get_pydantic_semantic_manifest(self) -> PydanticSemanticManifest:
        pydantic_time_spines: List[PydanticTimeSpine] = []
        minimum_time_spine_granularity: Optional[TimeGranularity] = None
        has_legacy_time_spine_with_config: bool = False
        for node in self.manifest.nodes.values():
            if not (isinstance(node, ModelNode) and node.time_spine):
                continue
            time_spine = node.time_spine
            standard_granularity_column = None
            for column in node.columns.values():
                if column.name == time_spine.standard_granularity_column:
                    standard_granularity_column = column
                    break
            # Assertions needed for type checking
            if not standard_granularity_column:
                raise ParsingError(
                    "Expected to find time spine standard granularity column in model columns, but did not. "
                    "This should have been caught in YAML parsing."
                )
            if not standard_granularity_column.granularity:
                raise ParsingError(
                    "Expected to find granularity set for time spine standard granularity column, but did not. "
                    "This should have been caught in YAML parsing."
                )
            pydantic_time_spine = PydanticTimeSpine(
                node_relation=PydanticNodeRelation(
                    alias=node.alias,
                    schema_name=node.schema,
                    database=node.database,
                    relation_name=node.relation_name,
                ),
                primary_column=PydanticTimeSpinePrimaryColumn(
                    name=time_spine.standard_granularity_column,
                    time_granularity=standard_granularity_column.granularity,
                ),
                custom_granularities=[
                    PydanticTimeSpineCustomGranularityColumn(
                        name=custom_granularity.name, column_name=custom_granularity.column_name
                    )
                    for custom_granularity in time_spine.custom_granularities
                ],
            )
            pydantic_time_spines.append(pydantic_time_spine)
            if pydantic_time_spine.node_relation.relation_name == LEGACY_TIME_SPINE_MODEL_NAME:
                has_legacy_time_spine_with_config = True
            if (
                not minimum_time_spine_granularity
                or standard_granularity_column.granularity.to_int()
                < minimum_time_spine_granularity.to_int()
            ):
                minimum_time_spine_granularity = standard_granularity_column.granularity

        project_config = PydanticProjectConfiguration(
            time_spine_table_configurations=[], time_spines=pydantic_time_spines
        )
        pydantic_semantic_manifest = PydanticSemanticManifest(
            metrics=[], semantic_models=[], project_configuration=project_config
        )

        for semantic_model in self.manifest.semantic_models.values():
            pydantic_semantic_manifest.semantic_models.append(
                PydanticSemanticModel.parse_obj(semantic_model.to_dict())
            )

        for metric in self.manifest.metrics.values():
            pydantic_semantic_manifest.metrics.append(PydanticMetric.parse_obj(metric.to_dict()))

        for saved_query in self.manifest.saved_queries.values():
            pydantic_semantic_manifest.saved_queries.append(
                PydanticSavedQuery.parse_obj(saved_query.to_dict())
            )

        if self.manifest.semantic_models:
            if not has_legacy_time_spine_with_config:
                legacy_time_spine_model = self.manifest.ref_lookup.find(
                    LEGACY_TIME_SPINE_MODEL_NAME, None, None, self.manifest
                )
                if legacy_time_spine_model:
                    if (
                        not minimum_time_spine_granularity
                        or LEGACY_TIME_SPINE_GRANULARITY.to_int()
                        < minimum_time_spine_granularity.to_int()
                    ):
                        minimum_time_spine_granularity = LEGACY_TIME_SPINE_GRANULARITY

            # If no time spines have been configured at DAY or smaller AND legacy time spine model does not exist, error.
            if (
                not minimum_time_spine_granularity
                or minimum_time_spine_granularity.to_int()
                > MINIMUM_REQUIRED_TIME_SPINE_GRANULARITY.to_int()
            ):
                raise ParsingError(
                    "The semantic layer requires a time spine model with granularity DAY or smaller in the project, "
                    "but none was found. Guidance on creating this model can be found on our docs site "
                    "(https://docs.getdbt.com/docs/build/metricflow-time-spine)."
                )

            # For backward compatibility: if legacy time spine exists, include it in the manifest.
            if legacy_time_spine_model:
                legacy_time_spine = LegacyTimeSpine(
                    location=legacy_time_spine_model.relation_name,
                    column_name="date_day",
                    grain=LEGACY_TIME_SPINE_GRANULARITY,
                )
                pydantic_semantic_manifest.project_configuration.time_spine_table_configurations = [
                    legacy_time_spine
                ]

        return pydantic_semantic_manifest
