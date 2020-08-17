from copy import deepcopy
from dataclasses import dataclass
from typing import List, Iterator, Dict, Any, TypeVar, Union

from dbt.config import RuntimeConfig, Project
from dbt.contracts.graph.model_config import BaseConfig, get_config_for
from dbt.exceptions import InternalException
from dbt.legacy_config_updater import ConfigUpdater, IsFQNResource
from dbt.node_types import NodeType
from dbt.utils import fqn_search


@dataclass
class ModelParts(IsFQNResource):
    fqn: List[str]
    resource_type: NodeType
    package_name: str


class LegacyContextConfig:
    def __init__(
        self,
        active_project: RuntimeConfig,
        own_project: Project,
        fqn: List[str],
        node_type: NodeType,
    ):
        self._config = None
        self._active_project: RuntimeConfig = active_project
        self._own_project: Project = own_project

        self._model = ModelParts(
            fqn=fqn,
            resource_type=node_type,
            package_name=self._own_project.project_name,
        )

        self._updater = ConfigUpdater(active_project.credentials.type)

        # the config options defined within the model
        self.in_model_config: Dict[str, Any] = {}

    def get_default(self) -> Dict[str, Any]:
        defaults = {"enabled": True, "materialized": "view"}

        if self._model.resource_type == NodeType.Seed:
            defaults['materialized'] = 'seed'
        elif self._model.resource_type == NodeType.Snapshot:
            defaults['materialized'] = 'snapshot'

        if self._model.resource_type == NodeType.Test:
            defaults['severity'] = 'ERROR'

        return defaults

    def build_config_dict(self, base: bool = False) -> Dict[str, Any]:
        defaults = self.get_default()
        active_config = self.load_config_from_active_project()

        if self._active_project.project_name == self._own_project.project_name:
            cfg = self._updater.merge(
                defaults, active_config, self.in_model_config
            )
        else:
            own_config = self.load_config_from_own_project()

            cfg = self._updater.merge(
                defaults, own_config, self.in_model_config, active_config
            )

        return cfg

    def _translate_adapter_aliases(self, config: Dict[str, Any]):
        return self._active_project.credentials.translate_aliases(config)

    def update_in_model_config(self, config: Dict[str, Any]) -> None:
        config = self._translate_adapter_aliases(config)
        self._updater.update_into(self.in_model_config, config)

    def load_config_from_own_project(self) -> Dict[str, Any]:
        return self._updater.get_project_config(self._model, self._own_project)

    def load_config_from_active_project(self) -> Dict[str, Any]:
        return self._updater.get_project_config(
            self._model,
            self._active_project,
        )


T = TypeVar('T', bound=BaseConfig)


class ContextConfigGenerator:
    def __init__(self, active_project: RuntimeConfig):
        self._active_project = active_project

    def get_node_project(self, project_name: str):
        if project_name == self._active_project.project_name:
            return self._active_project
        dependencies = self._active_project.load_dependencies()
        if project_name not in dependencies:
            raise InternalException(
                f'Project name {project_name} not found in dependencies '
                f'(found {list(dependencies)})'
            )
        return dependencies[project_name]

    def _project_configs(
        self, project: Project, fqn: List[str], resource_type: NodeType
    ) -> Iterator[Dict[str, Any]]:
        if resource_type == NodeType.Seed:
            model_configs = project.seeds
        elif resource_type == NodeType.Snapshot:
            model_configs = project.snapshots
        elif resource_type == NodeType.Source:
            model_configs = project.sources
        else:
            model_configs = project.models
        for level_config in fqn_search(model_configs, fqn):
            result = {}
            for key, value in level_config.items():
                if key.startswith('+'):
                    result[key[1:]] = deepcopy(value)
                elif not isinstance(value, dict):
                    result[key] = deepcopy(value)

            yield result

    def _active_project_configs(
        self, fqn: List[str], resource_type: NodeType
    ) -> Iterator[Dict[str, Any]]:
        return self._project_configs(self._active_project, fqn, resource_type)

    def _update_from_config(
        self, result: T, partial: Dict[str, Any], validate: bool = False
    ) -> T:
        translated = self._active_project.credentials.translate_aliases(
            partial
        )
        return result.update_from(
            translated,
            self._active_project.credentials.type,
            validate=validate
        )

    def calculate_node_config(
        self,
        config_calls: List[Dict[str, Any]],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        base: bool,
    ) -> BaseConfig:
        own_config = self.get_node_project(project_name)
        # defaults, own_config, config calls, active_config (if != own_config)
        config_cls = get_config_for(resource_type, base=base)
        # Calculate the defaults. We don't want to validate the defaults,
        # because it might be invalid in the case of required config members
        # (such as on snapshots!)
        result = config_cls.from_dict({}, validate=False)

        project_configs = self._project_configs(own_config, fqn, resource_type)
        for fqn_config in project_configs:
            result = self._update_from_config(result, fqn_config)

        for config_call in config_calls:
            result = self._update_from_config(result, config_call)

        if own_config.project_name != self._active_project.project_name:
            for fqn_config in self._active_project_configs(fqn, resource_type):
                result = self._update_from_config(result, fqn_config)

        # this is mostly impactful in the snapshot config case
        return result.finalize_and_validate()


class ContextConfig:
    def __init__(
        self,
        active_project: RuntimeConfig,
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
    ) -> None:
        self._config_calls: List[Dict[str, Any]] = []
        self._cfg_source = ContextConfigGenerator(active_project)
        self._fqn = fqn
        self._resource_type = resource_type
        self._project_name = project_name

    def update_in_model_config(self, opts: Dict[str, Any]) -> None:
        self._config_calls.append(opts)

    def build_config_dict(self, base: bool = False) -> Dict[str, Any]:
        return self._cfg_source.calculate_node_config(
            config_calls=self._config_calls,
            fqn=self._fqn,
            resource_type=self._resource_type,
            project_name=self._project_name,
            base=base,
        ).to_dict()


ContextConfigType = Union[LegacyContextConfig, ContextConfig]
