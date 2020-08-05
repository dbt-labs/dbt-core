"""A collection of performance-enhancing functions that have to know just a
little bit too much to go anywhere else.
"""
from dbt.adapters.factory import get_adapter
from dbt.parser.manifest import load_manifest
from dbt.contracts.graph.manifest import Manifest
from dbt.config import RuntimeConfig


def get_full_manifest(
    config: RuntimeConfig,
    *,
    reset: bool = False,
) -> Manifest:
    """Load the full manifest, using the adapter's internal manifest if it
    exists to skip parsing internal (dbt + plugins) macros a second time.

    Also, make sure that we force-laod the adapter's manifest, so it gets
    attached to the adapter for any methods that need it.
    """
    adapter = get_adapter(config)  # type: ignore
    if reset:
        config.clear_dependencies()
        adapter.clear_macro_manifest()

    internal: Manifest = adapter.load_macro_manifest()

    return load_manifest(
        config,
        internal,
        adapter.connections.set_query_header,
    )
