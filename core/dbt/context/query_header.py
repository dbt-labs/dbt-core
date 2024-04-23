from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.context.manifest import ManifestContext
from dbt.contracts.graph.manifest import Manifest


class QueryHeaderContext(ManifestContext):
    def __init__(self, config: AdapterRequiredConfig, manifest: Manifest) -> None:
        super().__init__(config, manifest, config.project_name)


def generate_query_header_context(config: AdapterRequiredConfig, manifest: Manifest):
    ctx = QueryHeaderContext(config, manifest)
    return ctx.to_dict()
