from typing import Dict, MutableMapping, Optional
from dbt.contracts.graph.nodes import Macro
from dbt.exceptions import DuplicateMacroNameError, PackageNotFoundForMacroError
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME
from dbt.clients.jinja import MacroGenerator

MacroNamespace = Dict[str, Macro]


# This class builds the MacroResolver by adding macros
# to various categories for finding macros in the right order,
# so that higher precedence macros are found first.
# This functionality is also provided by the MacroNamespace,
# but the intention is to eventually replace that class.
# This enables us to get the macro unique_id without
# processing every macro in the project.
# Note: the root project macros override everything in the
# dbt internal projects. External projects (dependencies) will
# use their own macros first, then pull from the root project
# followed by dbt internal projects.
class MacroResolver:
    def __init__(
        self,
        macros: MutableMapping[str, Macro],
        root_project_name: str,
        internal_package_names,
    ) -> None:
        self.root_project_name = root_project_name
        self.macros = macros
        # internal packages comes from get_adapter_package_names
        self.internal_package_names = internal_package_names

        # To be filled in from macros.
        self.internal_packages: Dict[str, MacroNamespace] = {}
        self.packages: Dict[str, MacroNamespace] = {}
        self.root_package_macros: MacroNamespace = {}

        # add the macros to internal_packages, packages, and root packages
        self.add_macros()
        self._build_internal_packages_namespace()
        self._build_macros_by_name()

    def _build_internal_packages_namespace(self) -> None:
        # Iterate in reverse-order and overwrite: the packages that are first
        # in the list are the ones we want to "win".
        self.internal_packages_namespace: MacroNamespace = {}
        for pkg in reversed(self.internal_package_names):
            if pkg in self.internal_packages:
                # Turn the internal packages into a flat namespace
                self.internal_packages_namespace.update(self.internal_packages[pkg])

    # search order:
    # local_namespace (package of particular node), not including
    #    the internal packages or the root package
    #    This means that within an extra package, it uses its own macros
    # root package namespace
    # non-internal packages (that aren't local or root)
    # dbt internal packages
    def _build_macros_by_name(self) -> None:
        macros_by_name = {}

        # all internal packages (already in the right order)
        for macro in self.internal_packages_namespace.values():
            macros_by_name[macro.name] = macro

        # non-internal packages
        for fnamespace in self.packages.values():
            for macro in fnamespace.values():
                macros_by_name[macro.name] = macro

        # root package macros
        for macro in self.root_package_macros.values():
            macros_by_name[macro.name] = macro

        self.macros_by_name = macros_by_name

    def _add_macro_to(
        self,
        package_namespaces: Dict[str, MacroNamespace],
        macro: Macro,
    ) -> None:
        if macro.package_name in package_namespaces:
            namespace = package_namespaces[macro.package_name]
        else:
            namespace = {}
            package_namespaces[macro.package_name] = namespace

        if macro.name in namespace:
            raise DuplicateMacroNameError(macro, macro, macro.package_name)
        package_namespaces[macro.package_name][macro.name] = macro

    def add_macro(self, macro: Macro) -> None:
        macro_name: str = macro.name

        # internal macros (from plugins) will be processed separately from
        # project macros, so store them in a different place
        if macro.package_name in self.internal_package_names:
            self._add_macro_to(self.internal_packages, macro)
        else:
            # if it's not an internal package
            self._add_macro_to(self.packages, macro)
            # add to root_package_macros if it's in the root package
            if macro.package_name == self.root_project_name:
                self.root_package_macros[macro_name] = macro

    def add_macros(self) -> None:
        for macro in self.macros.values():
            self.add_macro(macro)

    def get_macro(self, local_package, macro_name) -> Optional[Macro]:
        local_package_macros = {}
        # If the macro is explicitly prefixed with an internal namespace
        # (e.g. 'dbt.some_macro'), look there first
        if local_package in self.internal_package_names:
            local_package_macros = self.internal_packages[local_package]
        # If the macro is explicitly prefixed with a different package name
        # (e.g. 'dbt_utils.some_macro'), look there first
        if local_package not in self.internal_package_names and local_package in self.packages:
            local_package_macros = self.packages[local_package]
        # First: search the specified package for this macro
        if macro_name in local_package_macros:
            return local_package_macros[macro_name]
        # Now look up in the standard search order
        if macro_name in self.macros_by_name:
            return self.macros_by_name[macro_name]
        return None

    def get_macro_id(self, local_package, macro_name) -> Optional[str]:
        macro = self.get_macro(local_package, macro_name)
        if macro is None:
            return None
        else:
            return macro.unique_id


# Currently this is just used by test processing in the schema
# parser (in connection with the MacroResolver). Future work
# will extend the use of these classes to other parsing areas.
# One of the features of this class compared to the MacroNamespace
# is that you can limit the number of macros provided to the
# context dictionary in the 'to_dict' manifest method.
class TestMacroNamespace:
    def __init__(self, macro_resolver, ctx, node, thread_ctx, depends_on_macros):
        self.macro_resolver = macro_resolver
        self.ctx = ctx
        self.node = node  # can be none
        self.thread_ctx = thread_ctx
        self.local_namespace = {}
        self.project_namespace = {}
        if depends_on_macros:
            dep_macros = []
            self.recursively_get_depends_on_macros(depends_on_macros, dep_macros)
            for macro_unique_id in dep_macros:
                if macro_unique_id in self.macro_resolver.macros:
                    # Split up the macro unique_id to get the project_name
                    (_, project_name, macro_name) = macro_unique_id.split(".")
                    # Save the plain macro_name in the local_namespace
                    macro = self.macro_resolver.macros[macro_unique_id]
                    macro_gen = MacroGenerator(
                        macro,
                        self.ctx,
                        self.node,
                        self.thread_ctx,
                    )
                    self.local_namespace[macro_name] = macro_gen
                    # We also need the two part macro name
                    if project_name not in self.project_namespace:
                        self.project_namespace[project_name] = {}
                    self.project_namespace[project_name][macro_name] = macro_gen

    def recursively_get_depends_on_macros(self, depends_on_macros, dep_macros):
        for macro_unique_id in depends_on_macros:
            if macro_unique_id in dep_macros:
                continue
            dep_macros.append(macro_unique_id)
            if macro_unique_id in self.macro_resolver.macros:
                macro = self.macro_resolver.macros[macro_unique_id]
                if macro.depends_on.macros:
                    self.recursively_get_depends_on_macros(macro.depends_on.macros, dep_macros)

    def get_from_package(self, package_name: Optional[str], name: str) -> Optional[MacroGenerator]:
        macro = None
        if package_name is None:
            macro = self.macro_resolver.macros_by_name.get(name)
        elif package_name == GLOBAL_PROJECT_NAME:
            macro = self.macro_resolver.internal_packages_namespace.get(name)
        elif package_name in self.macro_resolver.packages:
            macro = self.macro_resolver.packages[package_name].get(name)
        else:
            raise PackageNotFoundForMacroError(package_name)
        if not macro:
            return None
        macro_func = MacroGenerator(macro, self.ctx, self.node, self.thread_ctx)
        return macro_func
