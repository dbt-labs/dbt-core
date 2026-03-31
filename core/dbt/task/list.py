import json
import sys
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

import click

from dbt.cli.flags import Flags
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import (
    Exposure,
    Metric,
    SavedQuery,
    SemanticModel,
    SourceDefinition,
    UnitTestDefinition,
)
from dbt.events.types import NoNodesSelected
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.base import resource_types_from_args
from dbt.task.runnable import GraphRunnableTask
from dbt.utils import JSONEncoder
from dbt_common.events.contextvars import task_contextvars
from dbt_common.events.functions import fire_event, warn_or_error
from dbt_common.events.types import PrintEvent
from dbt_common.exceptions import DbtInternalError, DbtRuntimeError

# dbt brand orange (RGB) plus standard ANSI names for other resource types.
_RESOURCE_TYPE_COLORS: dict = {
    "model": 208,              # xterm-256 orange (#FF8700), closest to dbt brand
    "source": "green",
    "seed": "yellow",
    "snapshot": "magenta",
    "test": 167,      # xterm-256 muted rose-red (#D75F5F)
    "unit_test": 167,
    "exposure": "bright_cyan",
    "metric": "bright_blue",
    "semantic_model": "bright_blue",
    "saved_query": "bright_blue",
}


def _colorize(text: str, resource_type: str) -> str:
    """Apply color to text if stdout is a TTY, otherwise return plain text."""
    if not sys.stdout.isatty():
        return text
    color = _RESOURCE_TYPE_COLORS.get(resource_type)
    if color is None:
        return text
    return click.style(text, fg=color, bold=True)


# Resource types that can have depends_on — show the line even if the list is empty.
# Types omitted here (e.g. Source, Seed) have the line omitted entirely.
_DEPENDS_ON_RESOURCE_TYPES = frozenset(
    (
        NodeType.Model,
        NodeType.Snapshot,
        NodeType.Test,
        NodeType.Analysis,
        NodeType.Exposure,
        NodeType.Metric,
        NodeType.SemanticModel,
        NodeType.SavedQuery,
        NodeType.Unit,
        NodeType.Function,
    )
)

# Only show materialization for models — it's redundant for seeds (always table)
# and snapshots (always table), and not meaningful for other resource types.
_MATERIALIZED_RESOURCE_TYPES = frozenset((NodeType.Model,))


@dataclass
class _ColumnDetail:
    name: str
    data_type: str  # empty string when not set
    description: str  # empty string when not set


@dataclass
class _VerboseRow:
    """Intermediate representation of one node for verbose rendering."""

    # Fixed columns — always present, padded to align across all rows.
    name: str
    resource_type: str
    package_name: str
    original_file_path: str

    # Inline fields — all padded to align across rows. Add new fields here in
    # declaration order; generate_verbose picks them up via INLINE_ATTRS.
    tags: str = ""           # e.g. "[finance,core]" or ""
    materialized: str = ""   # e.g. "table", "view", "" when not applicable

    # Continuation lines — None means omit the line entirely.
    description: Optional[str] = field(default=None)
    depends_on_names: Optional[List[str]] = field(default=None)
    column_details: Optional[List[_ColumnDetail]] = field(default=None)

    # All main-line attributes in render order.
    INLINE_ATTRS = ("name", "resource_type", "package_name", "original_file_path", "tags", "materialized")


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
            NodeType.Function,
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

    def __init__(self, args: Flags, config: RuntimeConfig, manifest: Manifest) -> None:
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
            elif unique_id in self.manifest.functions:
                yield self.manifest.functions[unique_id]
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

    def _get_nested_value(self, data, key_path):
        """Get nested value using dot notation (e.g., 'config.materialized')"""
        keys = key_path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def generate_json(self):
        for node in self._iterate_selected_nodes():
            node_dict = node.to_dict(omit_none=False)

            if self.args.output_keys:
                # Handle both nested and regular keys
                result = {}
                for key in self.args.output_keys:
                    if "." in key:
                        # Handle nested key (e.g., 'config.materialized')
                        value = self._get_nested_value(node_dict, key)
                        if value is not None:
                            result[key] = value
                    else:
                        # Handle regular key
                        if key in node_dict:
                            result[key] = node_dict[key]
            else:
                # Use default allowed keys
                result = {k: v for k, v in node_dict.items() if k in self.ALLOWED_KEYS}

            yield json.dumps(result, cls=JSONEncoder)

    def generate_paths(self) -> Iterator[str]:
        for node in self._iterate_selected_nodes():
            yield node.original_file_path

    def _short_dep_name(self, unique_id: str, current_package: str) -> str:
        """Return a human-readable dep name.

        Same-package deps use bare name; cross-package deps use ``pkg.name``
        to preserve full context without repeating the resource-type prefix.
        """
        parts = unique_id.split(".")
        # unique_id format: resource_type.package_name.name[.version]
        if len(parts) >= 3:
            pkg, name = parts[1], parts[2]
            return name if pkg == current_package else f"{pkg}.{name}"
        return unique_id

    def _build_verbose_row(self, node, show_columns: bool) -> _VerboseRow:
        tags = getattr(node, "tags", [])
        tags_str = f"[{','.join(tags)}]" if tags else ""

        mat = ""
        if node.resource_type in _MATERIALIZED_RESOURCE_TYPES:
            config = getattr(node, "config", None)
            if config:
                mat = getattr(config, "materialized", "") or ""

        depends_on_names: Optional[List[str]] = None
        if node.resource_type in _DEPENDS_ON_RESOURCE_TYPES:
            dep_nodes = getattr(node, "depends_on", None)
            node_ids = getattr(dep_nodes, "nodes", []) if dep_nodes else []
            depends_on_names = [
                self._short_dep_name(uid, node.package_name) for uid in node_ids
            ]

        column_details: Optional[List[_ColumnDetail]] = None
        if show_columns:
            cols = getattr(node, "columns", None)
            if cols is not None:
                column_details = [
                    _ColumnDetail(
                        name=col.name,
                        data_type=col.data_type or "",
                        description=col.description or "",
                    )
                    for col in cols.values()
                ]

        raw_description = getattr(node, "description", None)
        description = raw_description if raw_description else None

        return _VerboseRow(
            name=node.name,
            resource_type=node.resource_type.value,
            package_name=node.package_name,
            original_file_path=node.original_file_path,
            tags=tags_str,
            materialized=mat,
            depends_on_names=depends_on_names,
            column_details=column_details,
            description=description,
        )

    def generate_verbose(self, show_columns: bool = False) -> Iterator[str]:
        rows = [
            self._build_verbose_row(node, show_columns)
            for node in self._iterate_selected_nodes()
        ]
        if not rows:
            return

        # Two-pass render: compute max width for every inline column across all rows.
        col_widths = [
            max(len(getattr(row, a)) for row in rows)
            for a in _VerboseRow.INLINE_ATTRS
        ]

        for row in rows:
            parts = []
            for a, w in zip(_VerboseRow.INLINE_ATTRS, col_widths):
                val = getattr(row, a)
                if a == "name":
                    # Colorize the text only, then pad with plain spaces so
                    # escape codes don't bleed into the whitespace.
                    parts.append(_colorize(val, row.resource_type) + " " * (w - len(val)))
                else:
                    parts.append(val.ljust(w))
            yield "  ".join(parts).rstrip()

            if row.description is not None:
                lines = row.description.splitlines()
                yield f"  description: {lines[0]}"
                for line in lines[1:]:
                    yield f"    {line}"

            if row.depends_on_names is not None:
                deps = ', '.join(row.depends_on_names) if row.depends_on_names else '[]'
                yield f"  depends_on: {deps}"

            if row.column_details is not None:
                if not row.column_details:
                    yield "  columns: []"
                else:
                    yield "  columns:"
                    name_w = max(len(c.name) for c in row.column_details)
                    type_w = max(len(c.data_type) for c in row.column_details)
                    for col in row.column_details:
                        prefix = f"    {col.name.ljust(name_w)}  {col.data_type.ljust(type_w)}"
                        if not col.description:
                            yield prefix.rstrip()
                        else:
                            desc_lines = col.description.splitlines()
                            yield f"{prefix}  {desc_lines[0]}"
                            hang = " " * (len(prefix) + 2)
                            for desc_line in desc_lines[1:]:
                                yield f"{hang}{desc_line}"

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
            elif output == "verbose":
                show_columns = getattr(self.args, "columns", False)
                generator = lambda: self.generate_verbose(show_columns=show_columns)
            else:
                raise DbtInternalError("Invalid output {}".format(output))

            return self.output_results(generator())

    def output_results(self, results):
        """Log, or output a plain, newline-delimited, and ready-to-pipe list of nodes found."""
        for result in results:
            self.node_results.append(result)
            # No formatting, still get to stdout when --quiet is used
            fire_event(PrintEvent(msg=result))
        return self.node_results

    @property
    def resource_types(self) -> List[NodeType]:
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

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=self.resource_types,
            include_empty_nodes=True,
            selectors=self.config.selectors,
        )

    def interpret_results(self, results):
        # list command should always return 0 as exit code
        return True
