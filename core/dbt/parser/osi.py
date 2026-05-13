import os
import time
from pathlib import Path
from typing import Dict, List, Tuple

from metricflow.converters.osi_to_msi import OSIToMSIConverter

from dbt.constants import OSI_DIRECTORY_NAME, SUPPORTED_OSI_VERSIONS
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import Metric, ModelNode, SemanticModel
from dbt.events.types import MFConverterIssue
from dbt.exceptions import ParsingError
from dbt.node_types import NodeType
from dbt_common.events.functions import fire_event


def _scan_osi_directory(project_root: str) -> List[Path]:
    osi_dir = os.path.join(project_root, OSI_DIRECTORY_NAME)
    if not os.path.isdir(osi_dir):
        return []
    return sorted(Path(osi_dir) / f for f in os.listdir(osi_dir) if f.endswith(".json"))


def _build_model_lookup(manifest: Manifest) -> Dict[Tuple[str, str, str], ModelNode]:
    return {
        (
            (node.alias or node.name).lower(),
            node.schema.lower(),
            (node.database or "").lower(),
        ): node
        for node in manifest.nodes.values()
        if isinstance(node, ModelNode)
    }


def load_osi_into_manifest(
    project_root: str,
    package_name: str,
    manifest: Manifest,
) -> None:
    files = _scan_osi_directory(project_root)
    if not files:
        return

    from metricflow.converters.models import OSIDocument

    model_lookup = _build_model_lookup(manifest)

    for path in files:
        try:
            doc = OSIDocument.parse_raw(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ParsingError(f"Failed to parse OSI file '{path}': {exc}") from exc

        if doc.version not in SUPPORTED_OSI_VERSIONS:
            raise ParsingError(
                f"OSI file '{path}' uses unsupported version '{doc.version}'. "
                f"Supported versions: {sorted(SUPPORTED_OSI_VERSIONS)}"
            )

        result = OSIToMSIConverter().convert(doc)

        for issue in result.issues:
            fire_event(
                MFConverterIssue(
                    issue_type=issue.issue_type.value,
                    element_name=issue.element_name,
                    converter_name="OSIToMSIConverter",
                )
            )

        rel_path = os.path.join(OSI_DIRECTORY_NAME, path.name)
        original_file_path = str(path)
        now = time.time()

        for pydantic_sm in result.output.semantic_models:
            nr = pydantic_sm.node_relation
            key = (
                (nr.alias or "").lower(),
                (nr.schema_name or "").lower(),
                (nr.database or "").lower(),
            )
            matched = model_lookup.get(key)
            if matched is None:
                table_ref = ".".join(filter(None, [nr.database, nr.schema_name, nr.alias]))
                raise ParsingError(
                    f"OSI file '{path}' contains dataset '{pydantic_sm.name}' "
                    f"({table_ref}) that does not match any dbt model in this project. "
                    f"Each OSI dataset must reference a table managed by a dbt model."
                )

            unique_id = f"semantic_model.{package_name}.{pydantic_sm.name}"
            if unique_id in manifest.semantic_models:
                raise ParsingError(
                    f"OSI file '{path}' defines semantic model '{pydantic_sm.name}' "
                    f"which conflicts with an existing semantic model in this project."
                )

            d = pydantic_sm.dict()
            d.pop("config", None)
            d.pop("metadata", None)
            d.update(
                {
                    "resource_type": NodeType.SemanticModel,
                    "package_name": package_name,
                    "path": rel_path,
                    "original_file_path": original_file_path,
                    "unique_id": unique_id,
                    "fqn": [package_name, pydantic_sm.name],
                    "model": f"ref('{matched.name}')",
                    "node_relation": None,
                    "refs": [{"name": matched.name, "package": None, "version": None}],
                    "created_at": now,
                }
            )
            manifest.semantic_models[unique_id] = SemanticModel.from_dict(d)

        for pydantic_metric in result.output.metrics:
            unique_id = f"metric.{package_name}.{pydantic_metric.name}"
            if unique_id in manifest.metrics:
                raise ParsingError(
                    f"OSI file '{path}' defines metric '{pydantic_metric.name}' "
                    f"which conflicts with an existing metric in this project."
                )

            d = pydantic_metric.dict()
            d.pop("config", None)
            d.pop("metadata", None)
            d.update(
                {
                    "resource_type": NodeType.Metric,
                    "package_name": package_name,
                    "path": rel_path,
                    "original_file_path": original_file_path,
                    "unique_id": unique_id,
                    "fqn": [package_name, pydantic_metric.name],
                    "description": pydantic_metric.description or "",
                    "label": pydantic_metric.label or pydantic_metric.name,
                    "created_at": now,
                }
            )
            manifest.metrics[unique_id] = Metric.from_dict(d)
