from dbt.parser.schemas import YamlReader, SchemaParser
from dbt.parser.common import YamlBlock
from dbt.node_types import NodeType
from dbt.contracts.graph.unparsed import (
    UnparsedDimension,
    UnparsedDimensionTypeParams,
    UnparsedEntity,
    UnparsedExposure,
    UnparsedGroup,
    UnparsedMeasure,
    UnparsedMetric,
    UnparsedMetricInput,
    UnparsedMetricInputMeasure,
    UnparsedMetricTypeParams,
    UnparsedNonAdditiveDimension,
    UnparsedSemanticModel,
)
from dbt.contracts.graph.nodes import (
    Exposure,
    Group,
    Metric,
    MetricInput,
    MetricInputMeasure,
    MetricTimeWindow,
    MetricTypeParams,
    SemanticModel,
    WhereFilter,
)
from dbt.contracts.graph.semantic_models import (
    Dimension,
    DimensionTypeParams,
    Entity,
    Measure,
    NonAdditiveDimension,
)
from dbt.exceptions import DbtInternalError, YamlParseDictError, JSONValidationError
from dbt.context.providers import generate_parse_exposure, generate_parse_semantic_models

from dbt.contracts.graph.model_config import MetricConfig, ExposureConfig
from dbt.context.context_config import (
    BaseContextConfigGenerator,
    ContextConfigGenerator,
    UnrenderedConfigGenerator,
)
from dbt.clients.jinja import get_rendered
from dbt.dataclass_schema import ValidationError
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from typing import List, Optional, Union


class ExposureParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, NodeType.Exposure.pluralize())
        self.schema_parser = schema_parser
        self.yaml = yaml

    def parse_exposure(self, unparsed: UnparsedExposure):
        package_name = self.project.project_name
        unique_id = f"{NodeType.Exposure}.{package_name}.{unparsed.name}"
        path = self.yaml.path.relative_path

        fqn = self.schema_parser.get_fqn_prefix(path)
        fqn.append(unparsed.name)

        config = self._generate_exposure_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=True,
        )

        config = config.finalize_and_validate()

        unrendered_config = self._generate_exposure_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=False,
        )

        if not isinstance(config, ExposureConfig):
            raise DbtInternalError(
                f"Calculated a {type(config)} for an exposure, but expected an ExposureConfig"
            )

        parsed = Exposure(
            resource_type=NodeType.Exposure,
            package_name=package_name,
            path=path,
            original_file_path=self.yaml.path.original_file_path,
            unique_id=unique_id,
            fqn=fqn,
            name=unparsed.name,
            type=unparsed.type,
            url=unparsed.url,
            meta=unparsed.meta,
            tags=unparsed.tags,
            description=unparsed.description,
            label=unparsed.label,
            owner=unparsed.owner,
            maturity=unparsed.maturity,
            config=config,
            unrendered_config=unrendered_config,
        )
        ctx = generate_parse_exposure(
            parsed,
            self.root_project,
            self.schema_parser.manifest,
            package_name,
        )
        depends_on_jinja = "\n".join("{{ " + line + "}}" for line in unparsed.depends_on)
        get_rendered(depends_on_jinja, ctx, parsed, capture_macros=True)
        # parsed now has a populated refs/sources/metrics

        if parsed.config.enabled:
            self.manifest.add_exposure(self.yaml.file, parsed)
        else:
            self.manifest.add_disabled(self.yaml.file, parsed)

    def _generate_exposure_config(
        self, target: UnparsedExposure, fqn: List[str], package_name: str, rendered: bool
    ):
        generator: BaseContextConfigGenerator
        if rendered:
            generator = ContextConfigGenerator(self.root_project)
        else:
            generator = UnrenderedConfigGenerator(self.root_project)

        # configs with precendence set
        precedence_configs = dict()
        # apply exposure configs
        precedence_configs.update(target.config)

        return generator.calculate_node_config(
            config_call_dict={},
            fqn=fqn,
            resource_type=NodeType.Exposure,
            project_name=package_name,
            base=False,
            patch_config_dict=precedence_configs,
        )

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedExposure.validate(data)
                unparsed = UnparsedExposure.from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)

            self.parse_exposure(unparsed)


class MetricParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, NodeType.Metric.pluralize())
        self.schema_parser = schema_parser
        self.yaml = yaml

    def _get_input_measure(
        self,
        unparsed_input_measure: Union[UnparsedMetricInputMeasure, str],
    ) -> MetricInputMeasure:
        if isinstance(unparsed_input_measure, str):
            return MetricInputMeasure(name=unparsed_input_measure)
        else:
            filter: Optional[WhereFilter] = None
            if unparsed_input_measure.filter is not None:
                filter = WhereFilter(where_sql_template=unparsed_input_measure.filter)

            return MetricInputMeasure(
                name=unparsed_input_measure.name,
                filter=filter,
                alias=unparsed_input_measure.alias,
                join_to_timespine=unparsed_input_measure.join_to_timespine,
                fill_nulls_with=unparsed_input_measure.fill_nulls_with,
            )

    def _get_optional_input_measure(
        self,
        unparsed_input_measure: Optional[Union[UnparsedMetricInputMeasure, str]],
    ) -> Optional[MetricInputMeasure]:
        if unparsed_input_measure is not None:
            return self._get_input_measure(unparsed_input_measure)
        else:
            return None

    def _get_input_measures(
        self,
        unparsed_input_measures: Optional[List[Union[UnparsedMetricInputMeasure, str]]],
    ) -> List[MetricInputMeasure]:
        input_measures: List[MetricInputMeasure] = []
        if unparsed_input_measures is not None:
            for unparsed_input_measure in unparsed_input_measures:
                input_measures.append(self._get_input_measure(unparsed_input_measure))

        return input_measures

    def _get_time_window(
        self,
        unparsed_window: Optional[str],
    ) -> Optional[MetricTimeWindow]:
        if unparsed_window is not None:
            parts = unparsed_window.split(" ")
            if len(parts) != 2:
                raise YamlParseDictError(
                    self.yaml.path,
                    "window",
                    {"window": unparsed_window},
                    f"Invalid window ({unparsed_window}) in cumulative metric. Should be of the form `<count> <granularity>`, "
                    "e.g., `28 days`",
                )

            granularity = parts[1]
            # once we drop python 3.8 this could just be `granularity = parts[0].removesuffix('s')
            if granularity.endswith("s"):
                # months -> month
                granularity = granularity[:-1]
            if granularity not in [item.value for item in TimeGranularity]:
                raise YamlParseDictError(
                    self.yaml.path,
                    "window",
                    {"window": unparsed_window},
                    f"Invalid time granularity {granularity} in cumulative metric window string: ({unparsed_window})",
                )

            count = parts[0]
            if not count.isdigit():
                raise YamlParseDictError(
                    self.yaml.path,
                    "window",
                    {"window": unparsed_window},
                    f"Invalid count ({count}) in cumulative metric window string: ({unparsed_window})",
                )

            return MetricTimeWindow(
                count=int(count),
                granularity=TimeGranularity(granularity),
            )
        else:
            return None

    def _get_metric_input(self, unparsed: Union[UnparsedMetricInput, str]) -> MetricInput:
        if isinstance(unparsed, str):
            return MetricInput(name=unparsed)
        else:
            offset_to_grain: Optional[TimeGranularity] = None
            if unparsed.offset_to_grain is not None:
                offset_to_grain = TimeGranularity(unparsed.offset_to_grain)

            filter: Optional[WhereFilter] = None
            if unparsed.filter is not None:
                filter = WhereFilter(where_sql_template=unparsed.filter)

            return MetricInput(
                name=unparsed.name,
                filter=filter,
                alias=unparsed.alias,
                offset_window=self._get_time_window(unparsed.offset_window),
                offset_to_grain=offset_to_grain,
            )

    def _get_optional_metric_input(
        self,
        unparsed: Optional[Union[UnparsedMetricInput, str]],
    ) -> Optional[MetricInput]:
        if unparsed is not None:
            return self._get_metric_input(unparsed)
        else:
            return None

    def _get_metric_inputs(
        self,
        unparsed_metric_inputs: Optional[List[Union[UnparsedMetricInput, str]]],
    ) -> List[MetricInput]:
        metric_inputs: List[MetricInput] = []
        if unparsed_metric_inputs is not None:
            for unparsed_metric_input in unparsed_metric_inputs:
                metric_inputs.append(self._get_metric_input(unparsed=unparsed_metric_input))

        return metric_inputs

    def _get_metric_type_params(self, type_params: UnparsedMetricTypeParams) -> MetricTypeParams:
        grain_to_date: Optional[TimeGranularity] = None
        if type_params.grain_to_date is not None:
            grain_to_date = TimeGranularity(type_params.grain_to_date)

        return MetricTypeParams(
            measure=self._get_optional_input_measure(type_params.measure),
            numerator=self._get_optional_metric_input(type_params.numerator),
            denominator=self._get_optional_metric_input(type_params.denominator),
            expr=str(type_params.expr) if type_params.expr is not None else None,
            window=self._get_time_window(type_params.window),
            grain_to_date=grain_to_date,
            metrics=self._get_metric_inputs(type_params.metrics),
            # TODO This is a compiled list of measure/numerator/denominator as
            # well as the `input_measures` of included metrics. We're planning
            # on doing this as part of CT-2707
            # input_measures=?,
        )

    def parse_metric(self, unparsed: UnparsedMetric):
        package_name = self.project.project_name
        unique_id = f"{NodeType.Metric}.{package_name}.{unparsed.name}"
        path = self.yaml.path.relative_path

        fqn = self.schema_parser.get_fqn_prefix(path)
        fqn.append(unparsed.name)

        config = self._generate_metric_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=True,
        )

        config = config.finalize_and_validate()

        unrendered_config = self._generate_metric_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=False,
        )

        if not isinstance(config, MetricConfig):
            raise DbtInternalError(
                f"Calculated a {type(config)} for a metric, but expected a MetricConfig"
            )

        filter: Optional[WhereFilter] = None
        if unparsed.filter is not None:
            filter = WhereFilter(where_sql_template=unparsed.filter)

        parsed = Metric(
            resource_type=NodeType.Metric,
            package_name=package_name,
            path=path,
            original_file_path=self.yaml.path.original_file_path,
            unique_id=unique_id,
            fqn=fqn,
            name=unparsed.name,
            description=unparsed.description,
            label=unparsed.label,
            type=MetricType(unparsed.type),
            type_params=self._get_metric_type_params(unparsed.type_params),
            filter=filter,
            meta=unparsed.meta,
            tags=unparsed.tags,
            config=config,
            unrendered_config=unrendered_config,
            group=config.group,
        )

        # if the metric is disabled we do not want it included in the manifest, only in the disabled dict
        if parsed.config.enabled:
            self.manifest.add_metric(self.yaml.file, parsed)
        else:
            self.manifest.add_disabled(self.yaml.file, parsed)

    def _generate_metric_config(
        self, target: UnparsedMetric, fqn: List[str], package_name: str, rendered: bool
    ):
        generator: BaseContextConfigGenerator
        if rendered:
            generator = ContextConfigGenerator(self.root_project)
        else:
            generator = UnrenderedConfigGenerator(self.root_project)

        # configs with precendence set
        precedence_configs = dict()
        # first apply metric configs
        precedence_configs.update(target.config)

        config = generator.calculate_node_config(
            config_call_dict={},
            fqn=fqn,
            resource_type=NodeType.Metric,
            project_name=package_name,
            base=False,
            patch_config_dict=precedence_configs,
        )
        return config

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedMetric.validate(data)
                unparsed = UnparsedMetric.from_dict(data)

            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)
            self.parse_metric(unparsed)


class GroupParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, NodeType.Group.pluralize())
        self.schema_parser = schema_parser
        self.yaml = yaml

    def parse_group(self, unparsed: UnparsedGroup):
        package_name = self.project.project_name
        unique_id = f"{NodeType.Group}.{package_name}.{unparsed.name}"
        path = self.yaml.path.relative_path

        parsed = Group(
            resource_type=NodeType.Group,
            package_name=package_name,
            path=path,
            original_file_path=self.yaml.path.original_file_path,
            unique_id=unique_id,
            name=unparsed.name,
            owner=unparsed.owner,
        )

        self.manifest.add_group(self.yaml.file, parsed)

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedGroup.validate(data)
                unparsed = UnparsedGroup.from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)

            self.parse_group(unparsed)


class SemanticModelParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, "semantic_models")
        self.schema_parser = schema_parser
        self.yaml = yaml

    def _get_dimension_type_params(
        self, unparsed: Optional[UnparsedDimensionTypeParams]
    ) -> Optional[DimensionTypeParams]:
        if unparsed is not None:
            return DimensionTypeParams(
                time_granularity=TimeGranularity(unparsed.time_granularity),
                validity_params=unparsed.validity_params,
            )
        else:
            return None

    def _get_dimensions(self, unparsed_dimensions: List[UnparsedDimension]) -> List[Dimension]:
        dimensions: List[Dimension] = []
        for unparsed in unparsed_dimensions:
            dimensions.append(
                Dimension(
                    name=unparsed.name,
                    type=DimensionType(unparsed.type),
                    description=unparsed.description,
                    label=unparsed.label,
                    is_partition=unparsed.is_partition,
                    type_params=self._get_dimension_type_params(unparsed=unparsed.type_params),
                    expr=unparsed.expr,
                    metadata=None,  # TODO: requires a fair bit of parsing context
                )
            )
        return dimensions

    def _get_entities(self, unparsed_entities: List[UnparsedEntity]) -> List[Entity]:
        entities: List[Entity] = []
        for unparsed in unparsed_entities:
            entities.append(
                Entity(
                    name=unparsed.name,
                    type=EntityType(unparsed.type),
                    description=unparsed.description,
                    label=unparsed.label,
                    role=unparsed.role,
                    expr=unparsed.expr,
                )
            )

        return entities

    def _get_non_additive_dimension(
        self, unparsed: Optional[UnparsedNonAdditiveDimension]
    ) -> Optional[NonAdditiveDimension]:
        if unparsed is not None:
            return NonAdditiveDimension(
                name=unparsed.name,
                window_choice=AggregationType(unparsed.window_choice),
                window_groupings=unparsed.window_groupings,
            )
        else:
            return None

    def _get_measures(self, unparsed_measures: List[UnparsedMeasure]) -> List[Measure]:
        measures: List[Measure] = []
        for unparsed in unparsed_measures:
            measures.append(
                Measure(
                    name=unparsed.name,
                    agg=AggregationType(unparsed.agg),
                    description=unparsed.description,
                    label=unparsed.label,
                    expr=str(unparsed.expr) if unparsed.expr is not None else None,
                    agg_params=unparsed.agg_params,
                    non_additive_dimension=self._get_non_additive_dimension(
                        unparsed.non_additive_dimension
                    ),
                    agg_time_dimension=unparsed.agg_time_dimension,
                )
            )
        return measures

    def parse_semantic_model(self, unparsed: UnparsedSemanticModel):
        package_name = self.project.project_name
        unique_id = f"{NodeType.SemanticModel}.{package_name}.{unparsed.name}"
        path = self.yaml.path.relative_path

        fqn = self.schema_parser.get_fqn_prefix(path)
        fqn.append(unparsed.name)

        parsed = SemanticModel(
            description=unparsed.description,
            label=unparsed.label,
            fqn=fqn,
            model=unparsed.model,
            name=unparsed.name,
            node_relation=None,  # Resolved from the value of "model" after parsing
            original_file_path=self.yaml.path.original_file_path,
            package_name=package_name,
            path=path,
            resource_type=NodeType.SemanticModel,
            unique_id=unique_id,
            entities=self._get_entities(unparsed.entities),
            measures=self._get_measures(unparsed.measures),
            dimensions=self._get_dimensions(unparsed.dimensions),
            defaults=unparsed.defaults,
            primary_entity=unparsed.primary_entity,
        )

        ctx = generate_parse_semantic_models(
            parsed,
            self.root_project,
            self.schema_parser.manifest,
            package_name,
        )

        if parsed.model is not None:
            model_ref = "{{ " + parsed.model + " }}"
            # This sets the "refs" in the SemanticModel from the MetricRefResolver in context/providers.py
            get_rendered(model_ref, ctx, parsed)

        # No ability to disable a semantic model at this time
        self.manifest.add_semantic_model(self.yaml.file, parsed)

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedSemanticModel.validate(data)
                unparsed = UnparsedSemanticModel.from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)

            self.parse_semantic_model(unparsed)
