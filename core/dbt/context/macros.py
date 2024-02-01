from typing import Any, Dict, Iterable, Union, Optional, List, Iterator, Mapping, Set

from dbt.clients.jinja import MacroGenerator, MacroStack
from dbt.contracts.graph.nodes import Macro
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME
from dbt.exceptions import DuplicateMacroNameError, PackageNotFoundForMacroError


FlatNamespace = Dict[str, MacroGenerator]
NamespaceMember = Union[FlatNamespace, MacroGenerator]
FullNamespace = Dict[str, NamespaceMember]


# The point of this class is to collect the various macros
# and provide the ability to flatten them into the ManifestContexts
# that are created for jinja, so that macro calls can be resolved.
# Creates special iterators and _keys methods to flatten the lists.
# When this class is created it has a static 'local_namespace' which
# depends on the package of the node, so it only works for one
# particular local package at a time for "flattening" into a context.
# 'get_by_package' should work for any macro.
class MacroNamespace(Mapping):
    def __init__(
        self,
        global_namespace: FlatNamespace,  # root package macros
        local_namespace: FlatNamespace,  # packages for *this* node
        global_project_namespace: FlatNamespace,  # internal packages
        packages: Dict[str, FlatNamespace],  # non-internal packages
    ):
        self.global_namespace: FlatNamespace = global_namespace
        self.local_namespace: FlatNamespace = local_namespace
        self.packages: Dict[str, FlatNamespace] = packages
        self.global_project_namespace: FlatNamespace = global_project_namespace

    def _search_order(self) -> Iterable[Union[FullNamespace, FlatNamespace]]:
        yield self.local_namespace  # local package
        yield self.global_namespace  # root package
        # TODO CT-211
        yield self.packages  # type: ignore[misc] # non-internal packages
        yield {
            # TODO CT-211
            GLOBAL_PROJECT_NAME: self.global_project_namespace,  # type: ignore[misc] # dbt
        }
        yield self.global_project_namespace  # other internal project besides dbt

    # provides special keys method for MacroNamespace iterator
    # returns keys from local_namespace, global_namespace, packages,
    # global_project_namespace
    def _keys(self) -> Set[str]:
        keys: Set[str] = set()
        for search in self._search_order():
            keys.update(search)
        return keys

    # special iterator using special keys
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

    def get_from_package(self, package_name: Optional[str], name: str) -> Optional[MacroGenerator]:
        if package_name is None:
            return self.get(name)
        elif package_name == GLOBAL_PROJECT_NAME:
            return self.global_project_namespace.get(name)
        elif package_name in self.packages:
            return self.packages[package_name].get(name)
        else:
            raise PackageNotFoundForMacroError(package_name)


# This class builds the MacroNamespace by adding macros to
# internal_packages or packages, and locals/globals.
# Call 'build_namespace' to return a MacroNamespace.
# This is used by ManifestContext (and subclasses)
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
        # internal packages comes from get_adapter_package_names
        self.internal_package_names = set(internal_packages)
        self.internal_package_names_order = internal_packages
        # macro_func is added here if in root package, since
        # the root package acts as a "global" namespace, overriding
        # everything else except local external package macro calls
        self.globals: FlatNamespace = {}
        # macro_func is added here if it's the package for this node
        self.locals: FlatNamespace = {}
        # Create a dictionary of [package name][macro name] =
        #     MacroGenerator object which acts like a function
        self.internal_packages: Dict[str, FlatNamespace] = {}
        self.packages: Dict[str, FlatNamespace] = {}
        self.thread_ctx = thread_ctx
        self.node = node

    def _add_macro_to(
        self,
        hierarchy: Dict[str, FlatNamespace],
        macro: Macro,
        macro_func: MacroGenerator,
    ):
        if macro.package_name in hierarchy:
            namespace = hierarchy[macro.package_name]
        else:
            namespace = {}
            hierarchy[macro.package_name] = namespace

        if macro.name in namespace:
            raise DuplicateMacroNameError(macro_func.macro, macro, macro.package_name)
        hierarchy[macro.package_name][macro.name] = macro_func

    def add_macro(self, macro: Macro, ctx: Dict[str, Any]) -> None:
        macro_name: str = macro.name

        # MacroGenerator is in clients/jinja.py
        # a MacroGenerator object is a callable object that will
        # execute the MacroGenerator.__call__ function
        macro_func: MacroGenerator = MacroGenerator(macro, ctx, self.node, self.thread_ctx)

        # internal macros (from plugins) will be processed separately from
        # project macros, so store them in a different place
        if macro.package_name in self.internal_package_names:
            self._add_macro_to(self.internal_packages, macro, macro_func)
        else:
            # if it's not an internal package
            self._add_macro_to(self.packages, macro, macro_func)
            # add to locals if it's the package this node is in
            if macro.package_name == self.search_package:
                self.locals[macro_name] = macro_func
            # add to globals if it's in the root package
            elif macro.package_name == self.root_package:
                self.globals[macro_name] = macro_func

    def add_macros(self, macros: Iterable[Macro], ctx: Dict[str, Any]) -> None:
        for macro in macros:
            self.add_macro(macro, ctx)

    def build_namespace(
        self, macros_by_package: Dict[str, Dict[str, Macro]], ctx: Dict[str, Any]
    ) -> MacroNamespace:
        for package in macros_by_package.values():
            self.add_macros(package.values(), ctx)

        # Iterate in reverse-order and overwrite: the packages that are first
        # in the list are the ones we want to "win".
        global_project_namespace: FlatNamespace = {}
        for pkg in reversed(self.internal_package_names_order):
            if pkg in self.internal_packages:
                # add the macros pointed to by this package name
                global_project_namespace.update(self.internal_packages[pkg])

        return MacroNamespace(
            global_namespace=self.globals,  # root package macros
            local_namespace=self.locals,  # packages for *this* node
            global_project_namespace=global_project_namespace,  # internal packages
            packages=self.packages,  # non internal_packages
        )
