import json

from dbt.contracts.graph.nodes import (
    Exposure,
    SourceDefinition,
    Metric,
    SavedQuery,
    SemanticModel,
    UnitTestDefinition,
)
from dbt.cli.flags import Flags
from dbt.flags import get_flags
from dbt.graph import ResourceTypeSelector
from dbt.task.base import resource_types_from_args
from dbt.task.runnable import GraphRunnableTask
from dbt.task.test import TestSelector
from dbt.node_types import NodeType
from dbt_common.events.functions import (
    fire_event,
    warn_or_error,
)
from dbt.events.types import NoNodesSelected, ListCmdOut
from dbt_common.exceptions import DbtRuntimeError, DbtInternalError
from dbt_common.events.contextvars import task_contextvars


class ListTask(GraphRunnableTask):
    DEFAULT_RESOURCE_VALUES = frozenset(
        (
            NodeType.Model,
            NodeType.Snapshot,
            NodeType.Seed,
            NodeType.Test,
            NodeType.Source,
            NodeType.Exposure,
            NodeType.Metric,
            NodeType.SavedQuery,
            NodeType.SemanticModel,
            NodeType.Unit,
        )
    )
    ALL_RESOURCE_VALUES = DEFAULT_RESOURCE_VALUES | frozenset((NodeType.Analysis,))
    ALLOWED_KEYS = frozenset(
        (
            "alias",
            "name",
            "package_name",
            "depends_on",
            "tags",
            "config",
            "resource_type",
            "source_name",
            "original_file_path",
            "unique_id",
        )
    )

    def __init__(self, args: Flags, config, manifest) -> None:
        super().__init__(args, config, manifest)
        if self.args.models:
            if self.args.select:
                raise DbtRuntimeError('"models" and "select" are mutually exclusive arguments')
            if self.args.resource_types:
                raise DbtRuntimeError(
                    '"models" and "resource_type" are mutually exclusive ' "arguments"
                )

    def _iterate_selected_nodes(self):
        selector = self.get_node_selector()
        spec = self.get_selection_spec()
        unique_ids = sorted(selector.get_selected(spec))
        if not unique_ids:
            warn_or_error(NoNodesSelected())
            return
        if self.manifest is None:
            raise DbtInternalError("manifest is None in _iterate_selected_nodes")
        for unique_id in unique_ids:
            if unique_id in self.manifest.nodes:
                yield self.manifest.nodes[unique_id]
            elif unique_id in self.manifest.sources:
                yield self.manifest.sources[unique_id]
            elif unique_id in self.manifest.exposures:
                yield self.manifest.exposures[unique_id]
            elif unique_id in self.manifest.metrics:
                yield self.manifest.metrics[unique_id]
            elif unique_id in self.manifest.semantic_models:
                yield self.manifest.semantic_models[unique_id]
            elif unique_id in self.manifest.unit_tests:
                yield self.manifest.unit_tests[unique_id]
            elif unique_id in self.manifest.saved_queries:
                yield self.manifest.saved_queries[unique_id]
            else:
                raise DbtRuntimeError(
                    f'Got an unexpected result from node selection: "{unique_id}"'
                    f"Listing this node type is not yet supported!"
                )

    def generate_selectors(self):
        for node in self._iterate_selected_nodes():
            if node.resource_type == NodeType.Source:
                assert isinstance(node, SourceDefinition)
                # sources are searched for by pkg.source_name.table_name
                source_selector = ".".join([node.package_name, node.source_name, node.name])
                yield f"source:{source_selector}"
            elif node.resource_type == NodeType.Exposure:
                assert isinstance(node, Exposure)
                # exposures are searched for by pkg.exposure_name
                exposure_selector = ".".join([node.package_name, node.name])
                yield f"exposure:{exposure_selector}"
            elif node.resource_type == NodeType.Metric:
                assert isinstance(node, Metric)
                # metrics are searched for by pkg.metric_name
                metric_selector = ".".join([node.package_name, node.name])
                yield f"metric:{metric_selector}"
            elif node.resource_type == NodeType.SavedQuery:
                assert isinstance(node, SavedQuery)
                saved_query_selector = ".".join([node.package_name, node.name])
                yield f"saved_query:{saved_query_selector}"
            elif node.resource_type == NodeType.SemanticModel:
                assert isinstance(node, SemanticModel)
                semantic_model_selector = ".".join([node.package_name, node.name])
                yield f"semantic_model:{semantic_model_selector}"
            elif node.resource_type == NodeType.Unit:
                assert isinstance(node, UnitTestDefinition)
                unit_test_selector = ".".join([node.package_name, node.versioned_name])
                yield f"unit_test:{unit_test_selector}"
            else:
                # everything else is from `fqn`
                yield ".".join(node.fqn)

    def generate_names(self):
        for node in self._iterate_selected_nodes():
            yield node.search_name

    def generate_json(self):
        for node in self._iterate_selected_nodes():
            yield json.dumps(
                {
                    k: v
                    for k, v in node.to_dict(omit_none=False).items()
                    if (
                        k in self.args.output_keys
                        if self.args.output_keys
                        else k in self.ALLOWED_KEYS
                    )
                }
            )

    def generate_paths(self):
        for node in self._iterate_selected_nodes():
            yield node.original_file_path

    def run(self):
        # We set up a context manager here with "task_contextvars" because we
        # we need the project_root in compile_manifest.
        with task_contextvars(project_root=self.config.project_root):
            self.compile_manifest()
            output = self.args.output
            if output == "selector":
                generator = self.generate_selectors
            elif output == "name":
                generator = self.generate_names
            elif output == "json":
                generator = self.generate_json
            elif output == "path":
                generator = self.generate_paths
            else:
                raise DbtInternalError("Invalid output {}".format(output))

            return self.output_results(generator())

    def output_results(self, results):
        """Log, or output a plain, newline-delimited, and ready-to-pipe list of nodes found."""
        for result in results:
            self.node_results.append(result)
            if get_flags().LOG_FORMAT == "json":
                fire_event(ListCmdOut(msg=result))
            else:
                # Cleaner to leave as print than to mutate the logger not to print timestamps.
                print(result)
        return self.node_results

    @property
    def resource_types(self):
        if self.args.models:
            return [NodeType.Model]

        resource_types = resource_types_from_args(
            self.args, set(self.ALL_RESOURCE_VALUES), set(self.DEFAULT_RESOURCE_VALUES)
        )

        return list(resource_types)

    @property
    def selection_arg(self):
        # for backwards compatibility, list accepts both --models and --select,
        # with slightly different behavior: --models implies --resource-type model
        if self.args.models:
            return self.args.models
        else:
            return self.args.select

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        if self.resource_types == [NodeType.Test]:
            return TestSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
            )
        else:
            return ResourceTypeSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
                resource_types=self.resource_types,
                include_empty_nodes=True,
            )

    def interpret_results(self, results):
        # list command should always return 0 as exit code
        return True
