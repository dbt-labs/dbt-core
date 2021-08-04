from importlib import import_module
from os import getenv
from dbt.logger import GLOBAL_LOGGER as logger
from .storage_adapter_type import StorageAdapterType
from typing import cast

# TODO:
# ensure configured adapter has the correct module signature
# provide better not-ready error messagin

# get configured storage adapter
_module_to_load = getenv(
    "DBT_STORAGE_ADAPTER", "dbt.adapters.internal_storage.local_filesystem"
)

# load module if it exists
try:
    _adapter = cast(StorageAdapterType, import_module(_module_to_load))
    logger.debug(f"Storage adapter {_module_to_load} loaded")
except ModuleNotFoundError:
    logger.error(f"Storage adapter {_module_to_load} not found")

# run ready check
if not _adapter.ready_check():
    logger.error(f"Storage Adapter{_module_to_load} not ready")
else:
    adapter = _adapter
