from typing import Any, Dict

from dbt.contracts.connection import AdapterRequiredConfig
from dbt.node_types import NodeType
from dbt.utils import MultiDict

from dbt.context.base import contextproperty, Var
from dbt.context.target import TargetContext


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
        self._config = config
        self._project_name = project_name

    def __call__(self, var_name, default=Var._VAR_NOTSET):
        my_config = self._config.load_dependencies()[self._project_name]

        # cli vars > active project > local project
        if var_name in self._config.cli_vars:
            return self._config.cli_vars[var_name]

        if self._config.config_version == 2 and my_config.config_version == 2:
            adapter_type = self._config.credentials.type
            lookup = FQNLookup(self._project_name)
            active_vars = self._config.vars.vars_for(lookup, adapter_type)
            all_vars = MultiDict([active_vars])

            if self._config.project_name != my_config.project_name:
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


def generate_schema_yml(
    config: AdapterRequiredConfig, project_name: str
) -> Dict[str, Any]:
    ctx = SchemaYamlContext(config, project_name)
    return ctx.to_dict()
