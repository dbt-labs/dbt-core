from typing import List, Any

SELECTED_RESOURCES = []


def set_selected_resources(selected_resources: List[Any]) -> None:
    global SELECTED_RESOURCES
    SELECTED_RESOURCES = list(selected_resources)
