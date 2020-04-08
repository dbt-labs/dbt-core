from dataclasses import dataclass
from typing import Dict, Any, List

from dbt.legacy_config_updater import ConfigUpdater, IsFQNResource
from dbt.node_types import NodeType
from dbt.config import RuntimeConfig, Project


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
        self.active_project: RuntimeConfig = active_project
        self.own_project: Project = own_project

        self.model = ModelParts(
            fqn=fqn,
            resource_type=node_type,
            package_name=self.own_project.project_name,
        )

        self.updater = ConfigUpdater(active_project.credentials.type)

        # the config options defined within the model
        self.in_model_config: Dict[str, Any] = {}

    def get_default(self) -> Dict[str, Any]:
        defaults = {"enabled": True, "materialized": "view"}

        if self.model.resource_type == NodeType.Seed:
            defaults['materialized'] = 'seed'
        elif self.model.resource_type == NodeType.Snapshot:
            defaults['materialized'] = 'snapshot'

        if self.model.resource_type == NodeType.Test:
            defaults['severity'] = 'ERROR'

        return defaults

    def build_config_dict(self) -> Dict[str, Any]:
        defaults = self.get_default()
        active_config = self.load_config_from_active_project()

        if self.active_project.project_name == self.own_project.project_name:
            cfg = self.updater.merge(
                defaults, active_config, self.in_model_config
            )
        else:
            own_config = self.load_config_from_own_project()

            cfg = self.updater.merge(
                defaults, own_config, self.in_model_config, active_config
            )

        return cfg

    def _translate_adapter_aliases(self, config: Dict[str, Any]):
        return self.active_project.credentials.translate_aliases(config)

    def update_in_model_config(self, config: Dict[str, Any]) -> None:
        config = self._translate_adapter_aliases(config)
        self.updater.update_into(self.in_model_config, config)

    def load_config_from_own_project(self) -> Dict[str, Any]:
        return self.updater.get_project_config(self.model, self.own_project)

    def load_config_from_active_project(self) -> Dict[str, Any]:
        return self.updater.get_project_config(self.model, self.active_project)
