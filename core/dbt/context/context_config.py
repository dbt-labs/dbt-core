from abc import abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from typing import List, Iterator, Dict, Any, TypeVar, Generic, Optional

from dbt.config import RuntimeConfig, Project, IsFQNResource
from dbt.contracts.graph.model_config import BaseConfig, get_config_for, _listify
from dbt.exceptions import DbtInternalError
from dbt.node_types import NodeType
from dbt.utils import fqn_search


@dataclass
class ModelParts(IsFQNResource):
    fqn: List[str]
    resource_type: NodeType
    package_name: str


T = TypeVar("T")  # any old type
C = TypeVar("C", bound=BaseConfig)


class ConfigSource:
    def __init__(self, project):
        self.project = project

    def get_config_dict(self, resource_type: NodeType):
        ...


class UnrenderedConfig(ConfigSource):
    def __init__(self, project: Project):
        self.project = project

    def get_config_dict(self, resource_type: NodeType) -> Dict[str, Any]:
        unrendered = self.project.unrendered.project_dict
        if resource_type == NodeType.Seed:
            model_configs = unrendered.get("seeds")
        elif resource_type == NodeType.Snapshot:
            model_configs = unrendered.get("snapshots")
        elif resource_type == NodeType.Source:
            model_configs = unrendered.get("sources")
        elif resource_type == NodeType.Test:
            model_configs = unrendered.get("tests")
        elif resource_type == NodeType.Metric:
            model_configs = unrendered.get("metrics")
        elif resource_type == NodeType.SemanticModel:
            model_configs = unrendered.get("semantic_models")
        elif resource_type == NodeType.SavedQuery:
            model_configs = unrendered.get("saved_queries")
        elif resource_type == NodeType.Exposure:
            model_configs = unrendered.get("exposures")
        else:
            model_configs = unrendered.get("models")
        if model_configs is None:
            return {}
        else:
            return model_configs


class RenderedConfig(ConfigSource):
    def __init__(self, project: Project):
        self.project = project

    def get_config_dict(self, resource_type: NodeType) -> Dict[str, Any]:
        if resource_type == NodeType.Seed:
            model_configs = self.project.seeds
        elif resource_type == NodeType.Snapshot:
            model_configs = self.project.snapshots
        elif resource_type == NodeType.Source:
            model_configs = self.project.sources
        elif resource_type == NodeType.Test:
            model_configs = self.project.tests
        elif resource_type == NodeType.Metric:
            model_configs = self.project.metrics
        elif resource_type == NodeType.SemanticModel:
            model_configs = self.project.semantic_models
        elif resource_type == NodeType.SavedQuery:
            model_configs = self.project.saved_queries
        elif resource_type == NodeType.Exposure:
            model_configs = self.project.exposures
        else:
            model_configs = self.project.models
        return model_configs


class BaseContextConfigGenerator(Generic[T]):
    def __init__(self, active_project: RuntimeConfig):
        self._active_project = active_project

    def get_config_source(self, project: Project) -> ConfigSource:
        return RenderedConfig(project)

    def get_node_project(self, project_name: str):
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
        src = self.get_config_source(project)
        model_configs = src.get_config_dict(resource_type)
        for level_config in fqn_search(model_configs, fqn):
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

    @abstractmethod
    def _update_from_config(self, result: T, partial: Dict[str, Any], validate: bool = False) -> T:
        ...

    @abstractmethod
    def initial_result(self, resource_type: NodeType, base: bool) -> T:
        ...

    def calculate_node_config(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        base: bool,
        patch_config_dict: Optional[Dict[str, Any]] = None,
    ) -> BaseConfig:
        own_config = self.get_node_project(project_name)

        result = self.initial_result(resource_type=resource_type, base=base)

        project_configs = self._project_configs(own_config, fqn, resource_type)
        for fqn_config in project_configs:
            result = self._update_from_config(result, fqn_config)

        # When schema files patch config, it has lower precedence than
        # config in the models (config_call_dict), so we add the patch_config_dict
        # before the config_call_dict
        if patch_config_dict:
            result = self._update_from_config(result, patch_config_dict)

        # config_calls are created in the 'experimental' model parser and
        # the ParseConfigObject (via add_config_call)
        result = self._update_from_config(result, config_call_dict)

        if own_config.project_name != self._active_project.project_name:
            for fqn_config in self._active_project_configs(fqn, resource_type):
                result = self._update_from_config(result, fqn_config)

        # this is mostly impactful in the snapshot config case
        # TODO CT-211
        return result  # type: ignore[return-value]

    @abstractmethod
    def calculate_node_config_dict(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        base: bool,
        patch_config_dict: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ...


class ContextConfigGenerator(BaseContextConfigGenerator[C]):
    def __init__(self, active_project: RuntimeConfig):
        self._active_project = active_project

    def get_config_source(self, project: Project) -> ConfigSource:
        return RenderedConfig(project)

    def initial_result(self, resource_type: NodeType, base: bool) -> C:
        # defaults, own_config, config calls, active_config (if != own_config)
        config_cls = get_config_for(resource_type, base=base)
        # Calculate the defaults. We don't want to validate the defaults,
        # because it might be invalid in the case of required config members
        # (such as on snapshots!)
        result = config_cls.from_dict({})
        return result

    def _update_from_config(self, result: C, partial: Dict[str, Any], validate: bool = False) -> C:
        translated = self._active_project.credentials.translate_aliases(partial)
        translated = self.translate_hook_names(translated)
        updated = result.update_from(
            translated, self._active_project.credentials.type, validate=validate
        )
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

    def calculate_node_config_dict(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        base: bool,
        patch_config_dict: Optional[dict] = None,
    ) -> Dict[str, Any]:
        config = self.calculate_node_config(
            config_call_dict=config_call_dict,
            fqn=fqn,
            resource_type=resource_type,
            project_name=project_name,
            base=base,
            patch_config_dict=patch_config_dict,
        )
        finalized = config.finalize_and_validate()
        return finalized.to_dict(omit_none=True)


class UnrenderedConfigGenerator(BaseContextConfigGenerator[Dict[str, Any]]):
    def get_config_source(self, project: Project) -> ConfigSource:
        return UnrenderedConfig(project)

    def calculate_node_config_dict(
        self,
        config_call_dict: Dict[str, Any],
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
        base: bool,
        patch_config_dict: Optional[dict] = None,
    ) -> Dict[str, Any]:
        # TODO CT-211
        return self.calculate_node_config(
            config_call_dict=config_call_dict,
            fqn=fqn,
            resource_type=resource_type,
            project_name=project_name,
            base=base,
            patch_config_dict=patch_config_dict,
        )  # type: ignore[return-value]

    def initial_result(self, resource_type: NodeType, base: bool) -> Dict[str, Any]:
        return {}

    def _update_from_config(
        self,
        result: Dict[str, Any],
        partial: Dict[str, Any],
        validate: bool = False,
    ) -> Dict[str, Any]:
        translated = self._active_project.credentials.translate_aliases(partial)
        result.update(translated)
        return result


class ContextConfig:
    def __init__(
        self,
        active_project: RuntimeConfig,
        fqn: List[str],
        resource_type: NodeType,
        project_name: str,
    ) -> None:
        self._config_call_dict: Dict[str, Any] = {}
        self._active_project = active_project
        self._fqn = fqn
        self._resource_type = resource_type
        self._project_name = project_name

    def add_config_call(self, opts: Dict[str, Any]) -> None:
        dct = self._config_call_dict
        self._add_config_call(dct, opts)

    @classmethod
    def _add_config_call(cls, config_call_dict, opts: Dict[str, Any]) -> None:
        # config_call_dict is already encountered configs, opts is new
        # This mirrors code in _merge_field_value in model_config.py which is similar but
        # operates on config objects.
        for k, v in opts.items():
            # MergeBehavior for post-hook and pre-hook is to collect all
            # values, instead of overwriting
            if k in BaseConfig.mergebehavior["append"]:
                if not isinstance(v, list):
                    v = [v]
                if k in config_call_dict:  # should always be a list here
                    config_call_dict[k].extend(v)
                else:
                    config_call_dict[k] = v

            elif k in BaseConfig.mergebehavior["update"]:
                if not isinstance(v, dict):
                    raise DbtInternalError(f"expected dict, got {v}")
                if k in config_call_dict and isinstance(config_call_dict[k], dict):
                    config_call_dict[k].update(v)
                else:
                    config_call_dict[k] = v
            elif k in BaseConfig.mergebehavior["dict_key_append"]:
                if not isinstance(v, dict):
                    raise DbtInternalError(f"expected dict, got {v}")
                if k in config_call_dict:  # should always be a dict
                    for key, value in v.items():
                        extend = False
                        # This might start with a +, to indicate we should extend the list
                        # instead of just clobbering it
                        if key.startswith("+"):
                            extend = True
                        if key in config_call_dict[k] and extend:
                            # extend the list
                            config_call_dict[k][key].extend(_listify(value))
                        else:
                            # clobber the list
                            config_call_dict[k][key] = _listify(value)
                else:
                    # This is always a dictionary
                    config_call_dict[k] = v
                    # listify everything
                    for key, value in config_call_dict[k].items():
                        config_call_dict[k][key] = _listify(value)
            else:
                config_call_dict[k] = v

    def build_config_dict(
        self,
        base: bool = False,
        *,
        rendered: bool = True,
        patch_config_dict: Optional[dict] = None,
    ) -> Dict[str, Any]:
        if rendered:
            # TODO CT-211
            src = ContextConfigGenerator(self._active_project)  # type: ignore[var-annotated]
        else:
            # TODO CT-211
            src = UnrenderedConfigGenerator(self._active_project)  # type: ignore[assignment]

        return src.calculate_node_config_dict(
            config_call_dict=self._config_call_dict,
            fqn=self._fqn,
            resource_type=self._resource_type,
            project_name=self._project_name,
            base=base,
            patch_config_dict=patch_config_dict,
        )
