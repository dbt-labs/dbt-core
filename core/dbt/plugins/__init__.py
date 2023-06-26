from typing import Optional

from dbt.exceptions import DbtInternalError

from .manager import PluginManager

# these are just exports, they need "noqa" so flake8 will not complain.
from .manager import dbtPlugin, dbt_hook  # noqa

PLUGIN_MANAGER: Optional[PluginManager] = None


def setup_plugin_manager():
    global PLUGIN_MANAGER
    PLUGIN_MANAGER = PluginManager()


def get_plugin_manager() -> PluginManager:
    global PLUGIN_MANAGER
    if not PLUGIN_MANAGER:
        raise DbtInternalError("get_plugin_manager called before plugin manager is set!")
    return PLUGIN_MANAGER
