from collections import ChainMap
from typing import List

from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.clients.jinja import MacroStack
from dbt.context.macro_resolver import TestMacroNamespace
from dbt.contracts.graph.manifest import Manifest

from .base import contextproperty
from .configured import ConfiguredContext
from .macros import MacroNamespace, MacroNamespaceBuilder


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
        self.namespace = self._build_namespace()

    def _build_namespace(self) -> MacroNamespace:
        # this takes all the macros in the manifest and adds them
        # to the MacroNamespaceBuilder stored in self.namespace
        builder = self._get_namespace_builder()
        return builder.build_namespace(self.manifest.get_macros_by_package(), self._ctx)  # type: ignore

    def _get_namespace_builder(self) -> MacroNamespaceBuilder:
        # avoid an import loop
        from dbt.adapters.factory import get_adapter_package_names

        internal_packages: List[str] = get_adapter_package_names(self.config.credentials.type)
        return MacroNamespaceBuilder(
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
            # The following is a performance optimization which creates a "virtual"
            # copy of dct, updated with the values in namespace. The result is
            # called cm and by using a ChainMap it avoids the very large cost of
            # iterating every value of namespace.
            cm = ChainMap(self.namespace, dct)

            # These next lines repair certain assumptions which were important
            # before the performance optimization:
            # 1. The context has a key called 'context' whose value is a reference
            #    to the full context.
            # 2. That self.namespace has the updated version of dct as its context,
            #    which is used for macro context later.
            # 3. That self._ctx is the updated version of dct rather than just dct
            cm.maps.insert(0, {"context": cm})
            self.namespace.ctx = cm
            self._ctx = cm

            return cm

    @contextproperty()
    def context_macro_stack(self):
        return self.macro_stack
