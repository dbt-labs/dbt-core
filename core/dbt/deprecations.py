import abc
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, DefaultDict, Dict, List, Optional

import dbt.tracking
from dbt.events import types as core_types
from dbt.flags import get_flags
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.events.functions import fire_event, warn_or_error
from dbt_common.events.types import Note


class DBTDeprecation:
    _name: ClassVar[Optional[str]] = None
    _event: ClassVar[Optional[str]] = None
    _is_preview: ClassVar[bool] = False

    @property
    def name(self) -> str:
        if self._name is not None:
            return self._name
        raise NotImplementedError("name not implemented for {}".format(self))

    def track_deprecation_warn(self) -> None:
        if dbt.tracking.active_user is not None:
            dbt.tracking.track_deprecation_warn({"deprecation_name": self.name})

    @property
    def event(self) -> abc.ABCMeta:
        if self._event is not None:
            module_path = core_types
            class_name = self._event

            try:
                return getattr(module_path, class_name)
            except AttributeError:
                msg = f"Event Class `{class_name}` is not defined in `{module_path}`"
                raise NameError(msg)
        raise NotImplementedError("event not implemented for {}".format(self._event))

    def preview(self, base_event: abc.ABCMeta) -> None:
        note_event = Note(msg=base_event.message())  # type: ignore
        fire_event(note_event)

    def show(self, *args, **kwargs) -> None:
        if self._is_preview:
            base_event = self.event(**kwargs)
            self.preview(base_event)
        else:
            flags = get_flags()
            if self.name not in active_deprecations or flags.show_all_deprecations:
                event = self.event(**kwargs)
                warn_or_error(event)
                self.track_deprecation_warn()

            active_deprecations[self.name] += 1


class PackageRedirectDeprecation(DBTDeprecation):
    _name = "package-redirect"
    _event = "PackageRedirectDeprecation"


class PackageInstallPathDeprecation(DBTDeprecation):
    _name = "install-packages-path"
    _event = "PackageInstallPathDeprecation"


# deprecations with a pattern of `project-config-*` for the name are not hardcoded
# they are called programatically via the pattern below
class ConfigSourcePathDeprecation(DBTDeprecation):
    _name = "project-config-source-paths"
    _event = "ConfigSourcePathDeprecation"


class ConfigDataPathDeprecation(DBTDeprecation):
    _name = "project-config-data-paths"
    _event = "ConfigDataPathDeprecation"


class ConfigLogPathDeprecation(DBTDeprecation):
    _name = "project-config-log-path"
    _event = "ConfigLogPathDeprecation"


class ConfigTargetPathDeprecation(DBTDeprecation):
    _name = "project-config-target-path"
    _event = "ConfigTargetPathDeprecation"


def renamed_method(old_name: str, new_name: str):
    class AdapterDeprecationWarning(DBTDeprecation):
        _name = "adapter:{}".format(old_name)
        _event = "AdapterDeprecationWarning"

    dep = AdapterDeprecationWarning()
    deprecations_list.append(dep)
    deprecations[dep.name] = dep


class MetricAttributesRenamed(DBTDeprecation):
    _name = "metric-attr-renamed"
    _event = "MetricAttributesRenamed"


class ExposureNameDeprecation(DBTDeprecation):
    _name = "exposure-name"
    _event = "ExposureNameDeprecation"


class CollectFreshnessReturnSignature(DBTDeprecation):
    _name = "collect-freshness-return-signature"
    _event = "CollectFreshnessReturnSignature"


class ProjectFlagsMovedDeprecation(DBTDeprecation):
    _name = "project-flags-moved"
    _event = "ProjectFlagsMovedDeprecation"


class PackageMaterializationOverrideDeprecation(DBTDeprecation):
    _name = "package-materialization-override"
    _event = "PackageMaterializationOverrideDeprecation"


class ResourceNamesWithSpacesDeprecation(DBTDeprecation):
    _name = "resource-names-with-spaces"
    _event = "ResourceNamesWithSpacesDeprecation"


class SourceFreshnessProjectHooksNotRun(DBTDeprecation):
    _name = "source-freshness-project-hooks"
    _event = "SourceFreshnessProjectHooksNotRun"


class MFTimespineWithoutYamlConfigurationDeprecation(DBTDeprecation):
    _name = "mf-timespine-without-yaml-configuration"
    _event = "MFTimespineWithoutYamlConfigurationDeprecation"


class MFCumulativeTypeParamsDeprecation(DBTDeprecation):
    _name = "mf-cumulative-type-params-deprecation"
    _event = "MFCumulativeTypeParamsDeprecation"


class MicrobatchMacroOutsideOfBatchesDeprecation(DBTDeprecation):
    _name = "microbatch-macro-outside-of-batches-deprecation"
    _event = "MicrobatchMacroOutsideOfBatchesDeprecation"


class GenericJSONSchemaValidationDeprecation(DBTDeprecation):
    _name = "generic-json-schema-validation-deprecation"
    _event = "GenericJSONSchemaValidationDeprecation"


class UnexpectedJinjaBlockDeprecation(DBTDeprecation):
    _name = "unexpected-jinja-block-deprecation"
    _event = "UnexpectedJinjaBlockDeprecation"


class DuplicateYAMLKeysDeprecation(DBTDeprecation):
    _name = "duplicate-yaml-keys-deprecation"
    _event = "DuplicateYAMLKeysDeprecation"


class CustomTopLevelKeyDeprecation(DBTDeprecation):
    _name = "custom-top-level-key-deprecation"
    _event = "CustomTopLevelKeyDeprecation"


class CustomKeyInConfigDeprecation(DBTDeprecation):
    _name = "custom-key-in-config-deprecation"
    _event = "CustomKeyInConfigDeprecation"


class CustomKeyInObjectDeprecation(DBTDeprecation):
    _name = "custom-key-in-object-deprecation"
    _event = "CustomKeyInObjectDeprecation"


class WEOInlcudeExcludeDeprecation(DBTDeprecation):
    _name = "weo-include-exclude-deprecation"
    _event = "WEOIncludeExcludeDeprecation"


class CustomOutputPathInSourceFreshnessDeprecation(DBTDeprecation):
    _name = "custom-output-path-in-source-freshness-deprecation"
    _event = "CustomOutputPathInSourceFreshnessDeprecation"


class PropertyMovedToConfigDeprecation(DBTDeprecation):
    _name = "property-moved-to-config-deprecation"
    _event = "PropertyMovedToConfigDeprecation"


class ModelParamUsageDeprecation(DBTDeprecation):
    _name = "model-param-usage-deprecation"
    _event = "ModelParamUsageDeprecation"


def renamed_env_var(old_name: str, new_name: str):
    class EnvironmentVariableRenamed(DBTDeprecation):
        _name = f"environment-variable-renamed:{old_name}"
        _event = "EnvironmentVariableRenamed"

    dep = EnvironmentVariableRenamed()
    deprecations_list.append(dep)
    deprecations[dep.name] = dep

    def cb():
        dep.show(old_name=old_name, new_name=new_name)

    return cb


def warn(name: str, *args, **kwargs) -> None:
    if name not in deprecations:
        # this should (hopefully) never happen
        raise RuntimeError("Error showing deprecation warning: {}".format(name))

    deprecations[name].show(*args, **kwargs)


def buffer(name: str, *args, **kwargs):
    def show_callback():
        deprecations[name].show(*args, **kwargs)

    buffered_deprecations.append(show_callback)


def show_deprecations_summary() -> None:
    summaries: List[Dict[str, Any]] = []
    for deprecation, occurrences in active_deprecations.items():
        deprecation_event = deprecations[deprecation].event()
        summaries.append(
            DeprecationSummary(
                event_name=deprecation_event.__name__,
                event_code=deprecation_event.code(),
                occurrences=occurrences,
            ).to_msg_dict()
        )

    if len(summaries) > 0:
        show_all_hint = not get_flags().show_all_deprecations
        warn_or_error(
            core_types.DeprecationsSummary(summaries=summaries, show_all_hint=show_all_hint)
        )


# these are globally available
# since modules are only imported once, active_deprecations is a singleton

active_deprecations: DefaultDict[str, int] = defaultdict(int)

deprecations_list: List[DBTDeprecation] = [
    PackageRedirectDeprecation(),
    PackageInstallPathDeprecation(),
    ConfigSourcePathDeprecation(),
    ConfigDataPathDeprecation(),
    ExposureNameDeprecation(),
    ConfigLogPathDeprecation(),
    ConfigTargetPathDeprecation(),
    CollectFreshnessReturnSignature(),
    ProjectFlagsMovedDeprecation(),
    PackageMaterializationOverrideDeprecation(),
    ResourceNamesWithSpacesDeprecation(),
    SourceFreshnessProjectHooksNotRun(),
    MFTimespineWithoutYamlConfigurationDeprecation(),
    MFCumulativeTypeParamsDeprecation(),
    MicrobatchMacroOutsideOfBatchesDeprecation(),
    GenericJSONSchemaValidationDeprecation(),
    UnexpectedJinjaBlockDeprecation(),
    DuplicateYAMLKeysDeprecation(),
    CustomTopLevelKeyDeprecation(),
    CustomKeyInConfigDeprecation(),
    CustomKeyInObjectDeprecation(),
    CustomOutputPathInSourceFreshnessDeprecation(),
    PropertyMovedToConfigDeprecation(),
    ModelParamUsageDeprecation(),
    WEOInlcudeExcludeDeprecation(),
]

deprecations: Dict[str, DBTDeprecation] = {d.name: d for d in deprecations_list}

buffered_deprecations: List[Callable] = []


def reset_deprecations():
    active_deprecations.clear()


def fire_buffered_deprecations():
    [dep_fn() for dep_fn in buffered_deprecations]
    buffered_deprecations.clear()


@dataclass
class DeprecationSummary(dbtClassMixin):
    event_name: str
    event_code: str
    occurrences: int

    def to_msg_dict(self) -> Dict[str, Any]:
        return {
            "event_name": self.event_name,
            "event_code": self.event_code,
            "occurrences": self.occurrences,
        }
