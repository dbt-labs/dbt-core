from collections import Counter

from dbt.artifacts.resources.types import NodeType
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note


def drop_unknown_resource_types(manifest: dict) -> dict:
    """Drop nodes of unknown resource types, e.g. dbt v2 checks."""
    known = {t.value for t in NodeType}
    dropped: Counter = Counter()

    nodes = manifest.get("nodes") or {}
    for unique_id, node in list(nodes.items()):
        if isinstance(node, dict) and node.get("resource_type") not in known:
            dropped[node.get("resource_type")] += 1
            del nodes[unique_id]

    disabled = manifest.get("disabled") or {}
    for unique_id, node_list in list(disabled.items()):
        kept = []
        for node in node_list:
            if isinstance(node, dict) and node.get("resource_type") not in known:
                dropped[node.get("resource_type")] += 1
            else:
                kept.append(node)
        if kept:
            disabled[unique_id] = kept
        else:
            del disabled[unique_id]

    for resource_type, count in dropped.items():
        fire_event(
            Note(
                msg=f"{count} node(s) of resource type '{resource_type}' in the manifest are "
                "not being used because this version of dbt doesn't support them."
            )
        )
    return manifest
