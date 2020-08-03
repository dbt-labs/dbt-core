from typing import (
    Any, Dict, Iterable, Union, Optional, List, Iterator, Mapping, Set
)

from dbt.clients.jinja import MacroGenerator, MacroStack
from dbt.contracts.graph.parsed import ParsedMacro
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME
from dbt.exceptions import (
    raise_duplicate_macro_name, raise_compiler_error
)


FlatNamespace = Dict[str, MacroGenerator]
NamespaceMember = Union[FlatNamespace, MacroGenerator]
FullNamespace = Dict[str, NamespaceMember]


class MacroNamespace(Mapping):
    def __init__(
        self,
        global_namespace: FlatNamespace,
        local_namespace: FlatNamespace,
        global_project_namespace: FlatNamespace,
        packages: Dict[str, FlatNamespace],
    ):
        self.global_namespace: FlatNamespace = global_namespace
        self.local_namespace: FlatNamespace = local_namespace
        self.packages: Dict[str, FlatNamespace] = packages
        self.global_project_namespace: FlatNamespace = global_project_namespace

    def _search_order(self) -> Iterable[Union[FullNamespace, FlatNamespace]]:
        yield self.local_namespace
        yield self.global_namespace
        yield self.packages
        yield {
            GLOBAL_PROJECT_NAME: self.global_project_namespace,
        }
        yield self.global_project_namespace

    def _keys(self) -> Set[str]:
        keys: Set[str] = set()
        for search in self._search_order():
            keys.update(search)
        return keys

    def __iter__(self) -> Iterator[str]:
        for key in self._keys():
            yield key

    def __len__(self):
        return len(self._keys())

    def __getitem__(self, key: str) -> NamespaceMember:
        for dct in self._search_order():
            if key in dct:
                return dct[key]
        raise KeyError(key)

    def get_from_package(
        self, package_name: Optional[str], name: str
    ) -> Optional[MacroGenerator]:
        pkg: FlatNamespace
        if package_name is None:
            return self.get(name)
        elif package_name == GLOBAL_PROJECT_NAME:
            return self.global_project_namespace.get(name)
        elif package_name in self.packages:
            return self.packages[package_name].get(name)
        else:
            raise_compiler_error(
                f"Could not find package '{package_name}'"
            )


class MacroNamespaceBuilder:
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
        self.internal_package_names = set(internal_packages)
        self.internal_package_names_order = internal_packages
        self.globals: FlatNamespace = {}
        self.locals: FlatNamespace = {}
        self.internal_packages: Dict[str, FlatNamespace] = {}
        self.packages: Dict[str, FlatNamespace] = {}
        self.thread_ctx = thread_ctx
        self.node = node

    def _add_macro_to(
        self,
        heirarchy: Dict[str, FlatNamespace],
        macro: ParsedMacro,
        macro_func: MacroGenerator,
    ):
        if macro.package_name in heirarchy:
            namespace = heirarchy[macro.package_name]
        else:
            namespace = {}
            heirarchy[macro.package_name] = namespace

        if macro.name in namespace:
            raise_duplicate_macro_name(
                macro_func.macro, macro, macro.package_name
            )
        heirarchy[macro.package_name][macro.name] = macro_func

    def add_macro(self, macro: ParsedMacro, ctx: Dict[str, Any]):
        macro_name: str = macro.name

        macro_func: MacroGenerator = MacroGenerator(
            macro, ctx, self.node, self.thread_ctx
        )

        # internal macros (from plugins) will be processed separately from
        # project macros, so store them in a different place
        if macro.package_name in self.internal_package_names:
            self._add_macro_to(self.internal_packages, macro, macro_func)
        else:
            self._add_macro_to(self.packages, macro, macro_func)

            if macro.package_name == self.search_package:
                self.locals[macro_name] = macro_func
            elif macro.package_name == self.root_package:
                self.globals[macro_name] = macro_func

    def add_macros(self, macros: Iterable[ParsedMacro], ctx: Dict[str, Any]):
        for macro in macros:
            self.add_macro(macro, ctx)

    def build_namespace(
        self, macros: Iterable[ParsedMacro], ctx: Dict[str, Any]
    ) -> MacroNamespace:
        self.add_macros(macros, ctx)

        # Iterate in reverse-order and overwrite: the packages that are first
        # in the list are the ones we want to "win".
        global_project_namespace: FlatNamespace = {}
        for pkg in reversed(self.internal_package_names_order):
            if pkg in self.internal_packages:
                global_project_namespace.update(self.internal_packages[pkg])

        return MacroNamespace(
            global_namespace=self.globals,
            local_namespace=self.locals,
            global_project_namespace=global_project_namespace,
            packages=self.packages,
        )
