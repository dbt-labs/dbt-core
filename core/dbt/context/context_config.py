from abc import abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Generic, Iterator, List, Optional, Type, TypeVar

from dbt import hooks
from dbt.adapters.factory import get_config_class_by_name
from dbt.config import IsFQNResource, Project, RuntimeConfig
from dbt.contracts.graph.model_config import get_config_for
from dbt.exceptions import SchemaConfigError
from dbt.flags import get_flags
from dbt.node_types import NodeType
from dbt.utils import fqn_search
from dbt_common.contracts.config.base import BaseConfig, merge_config_dicts
from dbt_common.dataclass_schema import ValidationError
from dbt_common.exceptions import DbtInternalError


@dataclass
class ModelParts(IsFQNResource):
    fqn: List[str]
    resource_type: NodeType
    package_name: str


T = TypeVar("T")  # any old type
C = TypeVar("C", bound=BaseConfig)


def fix_hooks(config_dict: Dict[str, Any]):
    """Given a config dict that may have `pre-hook`/`post-hook` keys,
    convert it from the yucky maybe-a-string, maybe-a-dict to a dict.
    """
    # Like most of parsing, this is a horrible hack :(
    for key in hooks.ModelHookType:
        if key in config_dict:
            config_dict[key] = [hooks.get_hook_dict(h) for h in config_dict[key]]


class BaseConfigGenerator(Generic[T]):
    def __init__(self, active_project: RuntimeConfig):
        self._active_project = active_project

    def get_node_project_config(self, project_name: str):
        if project_name == self._active_project.project_name:
            return self._active_project
        dependencies = self._active_project.load_dependencies()
        if project_name not in dependencies:
            raise DbtInternalError(
                f"Project name {project_name} not found in dependencies "
                f"(found {list(dependencies)})"
            )
        return dependencies[project_name]

    def _project_configs(
        self, project: Project, fqn: List[str], resource_type: NodeType
    ) -> Iterator[Dict[str, Any]]:
        resource_configs = self.get_resource_configs(project, resource_type)
        for level_config in fqn_search(resource_configs, fqn):
            result = {}
            for key, value in level_config.items():
                if key.startswith("+"):
                    result[key[1:].strip()] = deepcopy(value)
                elif not isinstance(value, dict):
                    result[key] = deepcopy(value)

            yield result

    def _active_project_configs(
        self, fqn: List[str], resource_type: NodeType
    ) -> Iterator[Dict[str, Any]]:
        return self._project_configs(self._active_project, fqn, resource_type)

    def combine_config_dicts(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        patch_config_dict: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """This method takes resource configs from the project, the model (if applicable),
        and the patch, and combines them into one config dictionary."""

        project_config = self.get_node_project_config(project_name)
        config_cls = get_config_for(resource_type)

        # creates "default" config object. Unrendered config starts with
        # empty dictionary, rendered config starts with to_dict() from empty config object.
        config_dict = self.initial_result(config_cls)

        # Update with project configs
        project_configs = self._project_configs(project_config, fqn, resource_type)
        for fqn_config in project_configs:
            config_dict = self._update_from_config(config_cls, config_dict, fqn_config)

        # Update with schema file configs (patches)
        if patch_config_dict:
            config_dict = self._update_from_config(config_cls, config_dict, patch_config_dict)

        # Update with config dictionary from sql files (config_call_dict)
        config_dict = self._update_from_config(config_cls, config_dict, config_call_dict)

        # If this is not the root project, update with configs from root project
        if project_config.project_name != self._active_project.project_name:
            for fqn_config in self._active_project_configs(fqn, resource_type):
                config_dict = self._update_from_config(config_cls, config_dict, fqn_config)

        return config_dict

    @abstractmethod
    def get_resource_configs(
        self, project: Project, resource_type: NodeType
    ) -> Dict[str, Any]: ...

    @abstractmethod
    def _update_from_config(
        self, config_cls: Type[BaseConfig], result_dict: Dict[str, Any], partial: Dict[str, Any]
    ) -> Dict[str, Any]: ...

    @abstractmethod
    def initial_result(self, config_cls: Type[BaseConfig]) -> Dict[str, Any]: ...

    @abstractmethod
    def generate_node_config(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        patch_config_dict: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]: ...


class RenderedConfigGenerator(BaseConfigGenerator[C]):
    """This class produces the config dictionary used to create the resource config."""

    def __init__(self, active_project: RuntimeConfig):
        self._active_project = active_project

    def get_resource_configs(self, project: Project, resource_type: NodeType) -> Dict[str, Any]:
        if resource_type == NodeType.Seed:
            resource_configs = project.seeds
        elif resource_type == NodeType.Snapshot:
            resource_configs = project.snapshots
        elif resource_type == NodeType.Source:
            resource_configs = project.sources
        elif resource_type == NodeType.Test:
            resource_configs = project.data_tests
        elif resource_type == NodeType.Metric:
            resource_configs = project.metrics
        elif resource_type == NodeType.SemanticModel:
            resource_configs = project.semantic_models
        elif resource_type == NodeType.SavedQuery:
            resource_configs = project.saved_queries
        elif resource_type == NodeType.Exposure:
            resource_configs = project.exposures
        elif resource_type == NodeType.Unit:
            resource_configs = project.unit_tests
        else:
            resource_configs = project.models
        return resource_configs

    def initial_result(self, config_cls: Type[BaseConfig]) -> Dict[str, Any]:
        # Produce a dictionary with config defaults.
        result = config_cls.from_dict({}).to_dict()
        return result

    def _update_from_config(
        self, config_cls: Type[BaseConfig], result_dict: Dict[str, Any], partial: Dict[str, Any]
    ) -> Dict[str, Any]:
        translated = self._active_project.credentials.translate_aliases(partial)
        translated = self.translate_hook_names(translated)

        adapter_type = self._active_project.credentials.type
        adapter_config_cls = get_config_class_by_name(adapter_type)

        # The "update_from" method in BaseConfig merges dictionaries using MergeBehavior
        updated = config_cls.update_from(result_dict, translated, adapter_config_cls)
        return updated

    def translate_hook_names(self, project_dict):
        # This is a kind of kludge because the fix for #6411 specifically allowed misspelling
        # the hook field names in dbt_project.yml, which only ever worked because we didn't
        # run validate on the dbt_project configs.
        if "pre_hook" in project_dict:
            project_dict["pre-hook"] = project_dict.pop("pre_hook")
        if "post_hook" in project_dict:
            project_dict["post-hook"] = project_dict.pop("post_hook")
        return project_dict

    # RenderedConfigGenerator. Validation is performed, and a config object is returned.
    def generate_node_config(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        patch_config_dict: Optional[dict] = None,
    ) -> Dict[str, Any]:

        config_cls = get_config_for(resource_type)
        config_dict = self.combine_config_dicts(
            config_call_dict=config_call_dict,
            fqn=fqn,
            resource_type=resource_type,
            project_name=project_name,
            patch_config_dict=patch_config_dict,
        )
        fix_hooks(config_dict)
        try:
            config_cls.validate(config_dict)
            config_obj = config_cls.from_dict(config_dict)
            return config_obj
        except ValidationError as exc:
            # we got a ValidationError - probably bad types in config()
            config_obj = config_cls.from_dict(config_dict)
            raise SchemaConfigError(exc, node=config_obj) from exc


class UnrenderedConfigGenerator(BaseConfigGenerator[Dict[str, Any]]):
    """This class produces the unrendered_config dictionary in the resource."""

    def get_resource_configs(self, project: Project, resource_type: NodeType) -> Dict[str, Any]:
        """Get configs for this resource_type from the project's unrendered config"""
        unrendered = project.unrendered.project_dict
        if resource_type == NodeType.Seed:
            resource_configs = unrendered.get("seeds")
        elif resource_type == NodeType.Snapshot:
            resource_configs = unrendered.get("snapshots")
        elif resource_type == NodeType.Source:
            resource_configs = unrendered.get("sources")
        elif resource_type == NodeType.Test:
            resource_configs = unrendered.get("data_tests")
        elif resource_type == NodeType.Metric:
            resource_configs = unrendered.get("metrics")
        elif resource_type == NodeType.SemanticModel:
            resource_configs = unrendered.get("semantic_models")
        elif resource_type == NodeType.SavedQuery:
            resource_configs = unrendered.get("saved_queries")
        elif resource_type == NodeType.Exposure:
            resource_configs = unrendered.get("exposures")
        elif resource_type == NodeType.Unit:
            resource_configs = unrendered.get("unit_tests")
        else:
            resource_configs = unrendered.get("models")
        if resource_configs is None:
            return {}
        else:
            return resource_configs

    # UnrenderedConfigGenerator. No validation is performed and a dictionary is returned.
    def generate_node_config(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        patch_config_dict: Optional[dict] = None,
    ) -> Dict[str, Any]:

        result = self.combine_config_dicts(
            config_call_dict=config_call_dict,
            fqn=fqn,
            resource_type=resource_type,
            project_name=project_name,
            patch_config_dict=patch_config_dict,
        )
        return result

    def initial_result(self, config_cls: Type[BaseConfig]) -> Dict[str, Any]:
        # We don't want the config defaults here, just the configs which have
        # actually been set.
        return {}

    def _update_from_config(
        self,
        config_cls: Type[BaseConfig],
        result_dict: Dict[str, Any],
        partial: Dict[str, Any],
    ) -> Dict[str, Any]:
        translated = self._active_project.credentials.translate_aliases(partial)
        result_dict.update(translated)
        return result_dict


class ConfigBuilder:
    """This object is included in various jinja contexts in order to collect the _config_call_dicts
    and the _unrendered_config_call dicts from the config calls in sql files.
    It is then used to run "build_config_dict" which calls the rendered or unrendered
    config generators and returns a config dictionary."""

    def __init__(
        self,
        active_project: RuntimeConfig,
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
    ) -> None:
        self._config_call_dict: Dict[str, Any] = {}
        self._unrendered_config_call_dict: Dict[str, Any] = {}
        self._active_project = active_project
        self._fqn = fqn
        self._resource_type = resource_type
        self._project_name = project_name

    def add_config_call(self, opts: Dict[str, Any]) -> None:
        dct = self._config_call_dict
        merge_config_dicts(dct, opts)

    def add_unrendered_config_call(self, opts: Dict[str, Any]) -> None:
        # Cannot perform complex merge behaviours on unrendered configs as they may not be appropriate types.
        self._unrendered_config_call_dict.update(opts)

    def build_config_dict(
        self,
        rendered: bool = True,
        patch_config_dict: Optional[dict] = None,
    ) -> Dict[str, Any]:
        if rendered:
            config_generator = RenderedConfigGenerator(self._active_project)  # type: ignore[var-annotated]
            config_call_dict = self._config_call_dict
        else:  # unrendered
            config_generator = UnrenderedConfigGenerator(self._active_project)  # type: ignore[assignment]

            # preserve legacy behaviour - using unreliable (potentially rendered) _config_call_dict
            if get_flags().state_modified_compare_more_unrendered_values is False:
                config_call_dict = self._config_call_dict
            else:
                # Prefer _config_call_dict if it is available and _unrendered_config_call_dict is not,
                # as _unrendered_config_call_dict is unreliable for non-sql nodes (e.g. no jinja config block rendered for python models, etc)
                if self._config_call_dict and not self._unrendered_config_call_dict:
                    config_call_dict = self._config_call_dict
                else:
                    config_call_dict = self._unrendered_config_call_dict

        config = config_generator.generate_node_config(
            config_call_dict=config_call_dict,
            fqn=self._fqn,
            resource_type=self._resource_type,
            project_name=self._project_name,
            patch_config_dict=patch_config_dict,
        )
        if isinstance(config, BaseConfig):
            return config.to_dict(omit_none=True)
        else:
            return config
