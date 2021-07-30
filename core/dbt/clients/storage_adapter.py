from importlib import import_module
from dbt.adapters.internal_storage.local_filesystem import Adapter as configured_adapter

DEFAULT_STORAGE_ADAPTER = "dbt.adapters.internal_storage.local_filesystem"
DEFAULT_STORAGE_ADAPTER_NAME = "local_filesystem_storage_adapter"

import_module(DEFAULT_STORAGE_ADAPTER)


class StorageAdapter():
    def __init__(self) -> None:
        # TODO:
        # get the config
        # ensure configured adapter exsists
        # import configured adapter
        # ensure configured adapter has the correct class signature
        # ensure configured adapter can access it's datastore
        pass

    # Dynamically generated class attribute that is the
    # configured storage adapter
    adapter = type(
        DEFAULT_STORAGE_ADAPTER_NAME,
        (configured_adapter,),
        dict()
    )
