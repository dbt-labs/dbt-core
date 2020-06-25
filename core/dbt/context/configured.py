from typing import Any, Dict, Iterable, Union, Optional, List

from dbt.clients.jinja import MacroGenerator, MacroStack
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedMacro
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME
from dbt.node_types import NodeType
from dbt.utils import MultiDict

from dbt.context.base import contextproperty, Var
from dbt.context.target import TargetContext
from dbt.exceptions import raise_duplicate_macro_name


class ConfiguredContext(TargetContext):
    config: AdapterRequiredConfig

    def __init__(
        self, config: AdapterRequiredConfig
    ) -> None:
        super().__init__(config, config.cli_vars)

    @contextproperty
    def project_name(self) -> str:
        return self.config.project_name


class FQNLookup:
    def __init__(self, package_name: str):
        self.package_name = package_name
        self.fqn = [package_name]
        self.resource_type = NodeType.Model


class ConfiguredVar(Var):
    def __init__(
        self,
        context: Dict[str, Any],
        config: AdapterRequiredConfig,
        project_name: str,
    ):
        super().__init__(context, config.cli_vars)
        self.config = config
        self.project_name = project_name

    def __call__(self, var_name, default=Var._VAR_NOTSET):
        my_config = self.config.load_dependencies()[self.project_name]

        # cli vars > active project > local project
        if var_name in self.config.cli_vars:
            return self.config.cli_vars[var_name]

        if self.config.config_version == 2 and my_config.config_version == 2:
            adapter_type = self.config.credentials.type
            lookup = FQNLookup(self.project_name)
            active_vars = self.config.vars.vars_for(lookup, adapter_type)
            all_vars = MultiDict([active_vars])

            if self.config.project_name != my_config.project_name:
                all_vars.add(my_config.vars.vars_for(lookup, adapter_type))

            if var_name in all_vars:
                return all_vars[var_name]

        if default is not Var._VAR_NOTSET:
            return default

        return self.get_missing_var(var_name)


class SchemaYamlContext(ConfiguredContext):
    def __init__(self, config, project_name: str):
        super().__init__(config)
        self._project_name = project_name

    @contextproperty
    def var(self) -> ConfiguredVar:
        return ConfiguredVar(
            self._ctx, self.config, self._project_name
        )


FlatNamespace = Dict[str, MacroGenerator]
NamespaceMember = Union[FlatNamespace, MacroGenerator]
FullNamespace = Dict[str, NamespaceMember]


class MacroNamespace:
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

    def get_macro_dict(self) -> FullNamespace:
        root_namespace: FullNamespace = {}

        # add everything in the 'dbt' namespace to the root namespace
        # overwriting any duplicates. Iterate in reverse-order because the
        # packages that are first in the list are the ones we want to "win".
        global_project_namespace = {}
        for pkg in reversed(self.internal_package_names_order):
            macros = self.internal_packages.get(pkg, {})
            global_project_namespace.update(macros)
            # these can then be overwitten by globals/locals
            root_namespace.update(macros)

        root_namespace[GLOBAL_PROJECT_NAME] = global_project_namespace
        root_namespace.update(self.packages)
        root_namespace.update(self.globals)
        root_namespace.update(self.locals)

        return root_namespace


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

    def _get_namespace(self):
        # avoid an import loop
        from dbt.adapters.factory import get_adapter_package_names
        internal_packages = get_adapter_package_names(
            self.config.credentials.type
        )
        return MacroNamespace(
            self.config.project_name,
            self.search_package,
            self.macro_stack,
            internal_packages,
            None,
        )

    def get_macros(self) -> Dict[str, Any]:
        nsp = self._get_namespace()
        nsp.add_macros(self.manifest.macros.values(), self._ctx)
        return nsp.get_macro_dict()

    def to_dict(self) -> Dict[str, Any]:
        dct = super().to_dict()
        dct.update(self.get_macros())
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


def generate_schema_yml(
    config: AdapterRequiredConfig, project_name: str
) -> Dict[str, Any]:
    ctx = SchemaYamlContext(config, project_name)
    return ctx.to_dict()
