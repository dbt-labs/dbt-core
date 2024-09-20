from typing import Optional

# these are just exports, they need "noqa" so flake8 will not complain.
from .manager import PluginManager, dbt_hook, dbtPlugin  # noqa

PLUGIN_MANAGER: Optional[PluginManager] = None


def set_up_plugin_manager(project_name: str):
    global PLUGIN_MANAGER
    PLUGIN_MANAGER = PluginManager.from_modules(project_name)


def get_plugin_manager(project_name: str) -> PluginManager:
    global PLUGIN_MANAGER
    if not PLUGIN_MANAGER:
        set_up_plugin_manager(project_name)

    assert PLUGIN_MANAGER
    return PLUGIN_MANAGER
