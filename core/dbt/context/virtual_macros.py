from collections import ChainMap
from typing import Any, Dict, Optional, List, Iterator, Mapping

from dbt.clients.jinja import MacroGenerator, MacroStack
from dbt.contracts.graph.nodes import Macro
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME
from dbt.exceptions import DuplicateMacroNameError, PackageNotFoundForMacroError


class VirtualMacroNamespace(ChainMap):
    def __init__(
        self,
        ctx: Dict[str, Any],
        node,
        thread_ctx,
        search_dicts: List[Mapping[str, Any]],
    ):
        self.ctx = ctx
        self.node = node
        self.thread_ctx = thread_ctx
        super().__init__(*search_dicts)

    def __iter__(self):
        i = super().__iter__()
        return i

    def __len__(self):
        l = super().__len__()
        return l

    def __getitem__(self, key: str):
        value = super().__getitem__(key)
        if isinstance(value, Macro):
            return MacroGenerator(value, self.ctx, self.node, self.thread_ctx)
        elif isinstance(value, Mapping):
            return VirtualMacroNamespace(self.ctx, self.node, self.thread_ctx, [value])

    def get_from_package(self, package_name: Optional[str], name: str) -> Optional[MacroGenerator]:
        if package_name is None:
            return self.get(name)
        elif package_name in self:
            return self[package_name].get(name)

        raise PackageNotFoundForMacroError(package_name)


class VirtualMacroNamespaceBuilder:
    def __init__(
        self,
        root_package: str,
        search_package: str,
        thread_ctx: MacroStack,
        internal_packages: List[str],
        node: Optional[Any] = None,
    ) -> None:
        self.root_package = root_package
        self.search_package = search_package
        self.thread_ctx = thread_ctx
        self.internal_packages = internal_packages  # order significant
        self.node = node

    def build_namespace(
        self, macros_by_package: Mapping[str, Mapping[str, Macro]], ctx: Dict[str, Any]
    ) -> VirtualMacroNamespace:

        internals = ChainMap(
            *[
                macros_by_package[package_name]
                for package_name in self.internal_packages
                if package_name in macros_by_package
            ]
        )

        # The virtual namespace will attempt to resolve names into either macros
        # or sub-namespaces by checking the dictionaries in the following list
        # in order.
        search_dicts = [
            macros_by_package[self.search_package]
            if self.search_package in macros_by_package
            else {},
            macros_by_package[self.root_package] if self.root_package in macros_by_package else {},
            {k: v for k, v in macros_by_package.items() if k not in self.internal_packages},
            {
                GLOBAL_PROJECT_NAME: internals
            },  # Macros from internal packages are available within the 'dbt' namespace.
            internals,
        ]

        return VirtualMacroNamespace(ctx, self.node, self.thread_ctx, search_dicts)
