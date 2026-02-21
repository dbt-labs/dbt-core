"""Parse dbt YAML files and extract meta.rationale blocks.

Walks a dbt project directory, finds all YAML schema files, and extracts
rationale metadata from supported resource types (models, metrics,
semantic_models, sources, exposures, tests).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from dbt_rationale.schema import RATIONALE_RESOURCE_TYPES


@dataclass
class RationaleEntry:
    """A single rationale block extracted from a dbt YAML file."""
    resource_type: str
    resource_name: str
    file_path: str
    rationale: Dict[str, Any]
    # For sources, capture parent source name
    parent_name: Optional[str] = None


@dataclass
class ParseResult:
    """Result of parsing a dbt project for rationale blocks."""
    entries: List[RationaleEntry] = field(default_factory=list)
    # Resources that exist but have no rationale
    uncovered: List[Dict[str, str]] = field(default_factory=list)
    # Files that failed to parse
    errors: List[Dict[str, str]] = field(default_factory=list)
    # Total resource count (with and without rationale)
    total_resources: int = 0


def find_yaml_files(project_path: str) -> List[Path]:
    """Find all YAML files in a dbt project that may contain schema definitions.

    Looks for *.yml and *.yaml files, excluding dbt_project.yml,
    packages.yml, profiles.yml, and anything under target/ or dbt_packages/.
    """
    project = Path(project_path)
    skip_dirs = {"target", "dbt_packages", "dbt_modules", ".git", "__pycache__", "logs"}
    skip_files = {"dbt_project.yml", "packages.yml", "profiles.yml", "dependencies.yml"}

    yaml_files = []
    for root, dirs, files in os.walk(project):
        # Prune directories we don't want to traverse
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for fname in files:
            if fname in skip_files:
                continue
            if fname.endswith((".yml", ".yaml")):
                yaml_files.append(Path(root) / fname)

    return sorted(yaml_files)


def _extract_rationale_from_meta(meta: Any) -> Optional[Dict[str, Any]]:
    """Extract the rationale dict from a meta field, if present."""
    if not isinstance(meta, dict):
        return None
    return meta.get("rationale")


def _extract_from_resource_list(
    resources: List[Dict[str, Any]],
    resource_type: str,
    file_path: str,
    parent_name: Optional[str] = None,
) -> tuple:
    """Extract rationale entries from a list of resource definitions.

    Returns (entries, uncovered) tuple.
    """
    entries = []
    uncovered = []

    for resource in resources:
        if not isinstance(resource, dict):
            continue

        name = resource.get("name", "<unnamed>")

        # Check both top-level meta and config.meta
        rationale = None
        for meta_source in [
            resource.get("meta"),
            resource.get("config", {}).get("meta") if isinstance(resource.get("config"), dict) else None,
        ]:
            rationale = _extract_rationale_from_meta(meta_source)
            if rationale is not None:
                break

        if rationale is not None:
            entries.append(RationaleEntry(
                resource_type=resource_type,
                resource_name=name,
                file_path=str(file_path),
                rationale=rationale,
                parent_name=parent_name,
            ))
        else:
            uncovered.append({
                "resource_type": resource_type,
                "resource_name": name,
                "file_path": str(file_path),
            })

    return entries, uncovered


def parse_yaml_file(file_path: Path) -> tuple:
    """Parse a single YAML file for rationale blocks.

    Returns (entries, uncovered, error_or_none).
    """
    entries = []
    uncovered = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return entries, uncovered, {"file": str(file_path), "error": str(e)}
    except OSError as e:
        return entries, uncovered, {"file": str(file_path), "error": str(e)}

    if not isinstance(content, dict):
        return entries, uncovered, None

    # Process each supported resource type
    for resource_type in RATIONALE_RESOURCE_TYPES:
        resources = content.get(resource_type)
        if not isinstance(resources, list):
            continue

        if resource_type == "sources":
            # Sources have a nested structure: sources[].tables[]
            for source in resources:
                if not isinstance(source, dict):
                    continue
                source_name = source.get("name", "<unnamed>")

                # Check source-level rationale
                source_rationale = None
                for meta_source in [
                    source.get("meta"),
                    source.get("config", {}).get("meta") if isinstance(source.get("config"), dict) else None,
                ]:
                    source_rationale = _extract_rationale_from_meta(meta_source)
                    if source_rationale is not None:
                        break

                if source_rationale is not None:
                    entries.append(RationaleEntry(
                        resource_type="sources",
                        resource_name=source_name,
                        file_path=str(file_path),
                        rationale=source_rationale,
                    ))
                else:
                    uncovered.append({
                        "resource_type": "sources",
                        "resource_name": source_name,
                        "file_path": str(file_path),
                    })

                # Check table-level rationale
                tables = source.get("tables", [])
                if isinstance(tables, list):
                    t_entries, t_uncovered = _extract_from_resource_list(
                        tables, "sources.table", str(file_path), parent_name=source_name
                    )
                    entries.extend(t_entries)
                    uncovered.extend(t_uncovered)
        else:
            r_entries, r_uncovered = _extract_from_resource_list(
                resources, resource_type, str(file_path)
            )
            entries.extend(r_entries)
            uncovered.extend(r_uncovered)

    return entries, uncovered, None


def parse_project(project_path: str) -> ParseResult:
    """Parse an entire dbt project for rationale blocks.

    Args:
        project_path: Path to the root of the dbt project.

    Returns:
        ParseResult with all extracted rationale entries, uncovered resources,
        and any file-level parse errors.
    """
    result = ParseResult()
    yaml_files = find_yaml_files(project_path)

    for yaml_file in yaml_files:
        entries, uncovered, error = parse_yaml_file(yaml_file)
        result.entries.extend(entries)
        result.uncovered.extend(uncovered)
        if error:
            result.errors.append(error)

    result.total_resources = len(result.entries) + len(result.uncovered)
    return result
