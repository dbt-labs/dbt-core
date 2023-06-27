from typing import Optional

from .manager import PluginManager

# these are just exports, they need "noqa" so flake8 will not complain.
from .manager import dbtPlugin, dbt_hook  # noqa

from dbt.config import Project


PLUGIN_MANAGER: Optional[PluginManager] = None


def set_up_plugin_manager(project: Project):
    global PLUGIN_MANAGER
    PLUGIN_MANAGER = PluginManager.from_modules(project)


def get_plugin_manager(project: Project) -> PluginManager:
    global PLUGIN_MANAGER
    if not PLUGIN_MANAGER:
        set_up_plugin_manager(project)

    assert PLUGIN_MANAGER
    return PLUGIN_MANAGER
