from typing import Any, Dict, Iterable, List, Optional, Union

from jinja2.nodes import Call, Const

from dbt.clients.jinja import get_rendered
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import (
    Exposure,
    Macro,
    ManifestNode,
    Metric,
    SavedQuery,
    SemanticModel,
    SourceDefinition,
)
from dbt_common.clients.jinja import parse


def _is_doc_call(node: Any) -> bool:
    return (
        isinstance(node, Call)
        and hasattr(node, "node")
        and hasattr(node, "args")
        and hasattr(node.node, "name")
        and node.node.name == "doc"
    )


def _resolve_doc_unique_id(node: Any, manifest: Manifest, node_package: str) -> Optional[str]:
    # Only Const is statically resolvable to a block name at parse time.
    #    * It does have a "value" attribute but mypy is unconvinced so the hasattr is to make it extra happy.
    # Filter out Const values to avoid raising an unhandled exception attempting
    # to statically parse other jinja expression nodes (ie Concat, CondExpr)
    doc_args = [arg.value for arg in node.args if isinstance(arg, Const) and hasattr(arg, "value")]

    if len(doc_args) == 1:
        package, name = None, doc_args[0]
    elif len(doc_args) == 2:
        package, name = doc_args
    else:
        return None

    if not manifest.metadata.project_name:
        return None

    resolved_doc = manifest.resolve_doc(
        name, package, manifest.metadata.project_name, node_package
    )
    return resolved_doc.unique_id if resolved_doc else None


def _get_doc_blocks(description: str, manifest: Manifest, node_package: str) -> List[str]:
    ast = parse(description)
    doc_blocks: List[str] = []

    if not hasattr(ast, "body"):
        return doc_blocks

    for statement in ast.body:
        for node in statement.nodes:
            if not _is_doc_call(node):
                continue
            unique_id = _resolve_doc_unique_id(node, manifest, node_package)
            if unique_id:
                doc_blocks.append(unique_id)

    return doc_blocks


def _render_if_set(items: Iterable[Any], context: Dict[str, Any]) -> None:
    for item in items:
        if item.description:
            item.description = get_rendered(item.description, context)


def _render_description_and_columns(
    context: Dict[str, Any],
    obj: Union[ManifestNode, SourceDefinition],
    manifest: Manifest,
) -> None:
    obj.doc_blocks = _get_doc_blocks(obj.description, manifest, obj.package_name)
    obj.description = get_rendered(obj.description, context)
    for column in obj.columns.values():
        column.doc_blocks = _get_doc_blocks(column.description, manifest, obj.package_name)
        column.description = get_rendered(column.description, context)


# node and column descriptions
def _process_docs_for_node(
    context: Dict[str, Any],
    node: ManifestNode,
    manifest: Manifest,
) -> None:
    _render_description_and_columns(context, node, manifest)


# source and table descriptions, column descriptions
def _process_docs_for_source(
    context: Dict[str, Any],
    source: SourceDefinition,
    manifest: Manifest,
) -> None:
    _render_description_and_columns(context, source, manifest)
    source.source_description = get_rendered(source.source_description, context)


# macro argument descriptions
def _process_docs_for_macro(context: Dict[str, Any], macro: Macro) -> None:
    macro.description = get_rendered(macro.description, context)
    for arg in macro.arguments:
        arg.description = get_rendered(arg.description, context)


# exposure descriptions
def _process_docs_for_exposure(context: Dict[str, Any], exposure: Exposure) -> None:
    exposure.description = get_rendered(exposure.description, context)


def _process_docs_for_metrics(context: Dict[str, Any], metric: Metric) -> None:
    metric.description = get_rendered(metric.description, context)


def _process_docs_for_semantic_model(
    context: Dict[str, Any], semantic_model: SemanticModel
) -> None:
    if semantic_model.description:
        semantic_model.description = get_rendered(semantic_model.description, context)
    _render_if_set(semantic_model.dimensions, context)
    _render_if_set(semantic_model.measures, context)
    _render_if_set(semantic_model.entities, context)


def _process_docs_for_saved_query(context: Dict[str, Any], saved_query: SavedQuery) -> None:
    if saved_query.description:
        saved_query.description = get_rendered(saved_query.description, context)
