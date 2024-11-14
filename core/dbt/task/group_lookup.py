from typing import AbstractSet, Dict, Optional, Union

from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import Group

_node_id_to_group_name_map: Dict[str, str] = {}
_group_name_to_group_map: Dict[str, Group] = {}


def init(manifest: Optional[Manifest], selected_ids: AbstractSet[str]) -> None:
    if not manifest:
        return

    if not manifest.groups:
        return

    if not hasattr(manifest, "group_map"):
        manifest.build_group_map()

    _every_group_name_to_group_map = {v.name: v for v in manifest.groups.values()}

    for group_name, node_ids in manifest.group_map.items():
        for node_id in node_ids:
            # only add node to lookup if it's selected
            if node_id in selected_ids:
                _node_id_to_group_name_map[node_id] = group_name

                # only add group to lookup if it's not already there and if node is selected
                if group_name not in _group_name_to_group_map:
                    _group_name_to_group_map[group_name] = _every_group_name_to_group_map[
                        group_name
                    ]


def get(node_id: str) -> Optional[Dict[str, Union[str, Dict[str, str]]]]:
    group_name = _node_id_to_group_name_map.get(node_id)

    if group_name is None:
        return None

    group = _group_name_to_group_map.get(group_name)

    if group is None:
        return None

    return group.to_logging_dict()
