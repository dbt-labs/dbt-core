from collections import ChainMap
from typing import List, Mapping

from dbt.clients.jinja import MacroStack
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.context.macro_resolver import TestMacroNamespace
from .base import contextproperty


from .configured import ConfiguredContext
from .virtual_macros import VirtualMacroNamespaceBuilder, VirtualMacroNamespace


class ManifestContext(ConfiguredContext):
    """The Macro context has everything in the target context, plus the macros
    in the manifest.

    The given macros can override any previous context values, which will be
    available as if they were accessed relative to the package name.
    """

    # subclasses are QueryHeaderContext and ProviderContext
    def __init__(
        self,
        config: AdapterRequiredConfig,
        manifest: Manifest,
        search_package: str,
    ) -> None:
        super().__init__(config)
        self.manifest = manifest
        # this is the package of the node for which this context was built
        self.search_package = search_package
        self.macro_stack = MacroStack()
        # This namespace is used by the BaseDatabaseWrapper in jinja rendering.
        # The namespace is passed to it when it's constructed. It expects
        # to be able to do: namespace.get_from_package(..)
        self.namespace = self._build_namespace()  # HEAVY

    def _build_namespace(self) -> VirtualMacroNamespace:
        # this takes all the macros in the manifest and adds them
        # to the MacroNamespaceBuilder stored in self.namespace
        builder = self._get_namespace_builder()
        return builder.build_namespace(self.manifest.get_macros_by_package(), self._ctx)

    def _get_namespace_builder(self) -> VirtualMacroNamespaceBuilder:
        # avoid an import loop
        from dbt.adapters.factory import get_adapter_package_names

        internal_packages: List[str] = get_adapter_package_names(self.config.credentials.type)
        return VirtualMacroNamespaceBuilder(
            self.config.project_name,
            self.search_package,
            self.macro_stack,
            internal_packages,
            None,
        )

    # This does not use the Mashumaro code
    def to_dict(self):
        dct = super().to_dict()
        # This moves all of the macros in the 'namespace' into top level
        # keys in the manifest dictionary
        if isinstance(self.namespace, TestMacroNamespace):
            dct.update(self.namespace.local_namespace)
            dct.update(self.namespace.project_namespace)
            return dct
        else:
            cm = ChainMap(self.namespace, dct)
            cm.maps.insert(0, {"context": cm})
            self.namespace.ctx = cm
            self._ctx = cm
            return cm

    @contextproperty()
    def context_macro_stack(self):
        return self.macro_stack


class QueryHeaderContext(ManifestContext):
    def __init__(self, config: AdapterRequiredConfig, manifest: Manifest) -> None:
        super().__init__(config, manifest, config.project_name)


def generate_query_header_context(config: AdapterRequiredConfig, manifest: Manifest):
    ctx = QueryHeaderContext(config, manifest)
    return ctx.to_dict()
