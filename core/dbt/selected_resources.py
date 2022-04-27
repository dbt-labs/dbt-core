from typing import Set, Any

SELECTED_RESOURCES = []


def set_selected_resources(selected_resources: Set[Any]) -> None:
    global SELECTED_RESOURCES
    SELECTED_RESOURCES = list(selected_resources)
