from typing import List

from dbt.clients.jinja import MacroStack
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.graph.manifest import Manifest


from .configured import ConfiguredContext
from .macros import MacroNamespaceBuilder


class ManifestContext(ConfiguredContext):
    """The Macro context has everything in the target context, plus the macros
    in the manifest.

    The given macros can override any previous context values, which will be
    available as if they were accessed relative to the package name.
    """
    def __init__(
        self,
        config: AdapterRequiredConfig,
        manifest: Manifest,
        search_package: str,
    ) -> None:
        super().__init__(config)
        self.manifest = manifest
        self.search_package = search_package
        self.macro_stack = MacroStack()
        builder = self._get_namespace_builder()
        self.namespace = builder.build_namespace(
            self.manifest.macros.values(),
            self._ctx,
        )

    def _get_namespace_builder(self) -> MacroNamespaceBuilder:
        # avoid an import loop
        from dbt.adapters.factory import get_adapter_package_names
        internal_packages: List[str] = get_adapter_package_names(
            self.config.credentials.type
        )
        return MacroNamespaceBuilder(
            self.config.project_name,
            self.search_package,
            self.macro_stack,
            internal_packages,
            None,
        )

    def to_dict(self):
        dct = super().to_dict()
        dct.update(self.namespace)
        return dct


class QueryHeaderContext(ManifestContext):
    def __init__(
        self, config: AdapterRequiredConfig, manifest: Manifest
    ) -> None:
        super().__init__(config, manifest, config.project_name)


def generate_query_header_context(
    config: AdapterRequiredConfig, manifest: Manifest
):
    ctx = QueryHeaderContext(config, manifest)
    return ctx.to_dict()
