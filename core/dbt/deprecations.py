import abc
from dataclasses import dataclass
from typing import Callable, ClassVar, Dict, List, Optional, Set

import dbt.tracking
from dbt.events import types as core_types
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event, warn_or_error


class DBTDeprecation:
    _name: ClassVar[Optional[str]] = None
    _event: ClassVar[Optional[str]] = None

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

    def show(self, *args, **kwargs) -> None:
        if self.name not in active_deprecations:
            event = self.event(**kwargs)
            warn_or_error(event)
            self.track_deprecation_warn()
            active_deprecations.add(self.name)


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


# these are globally available
# since modules are only imported once, active_deprecations is a singleton

active_deprecations: Set[str] = set()

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
]

deprecations: Dict[str, DBTDeprecation] = {d.name: d for d in deprecations_list}

buffered_deprecations: List[Callable] = []


def reset_deprecations():
    active_deprecations.clear()


def fire_buffered_deprecations():
    [dep_fn() for dep_fn in buffered_deprecations]
    buffered_deprecations.clear()


@dataclass
class ManagedDeprecationWarning:
    deprecation_warning: DBTDeprecation
    occurances: int = 0
    only_show_once: bool = True

    def show(self, fire_as_error: bool = False, *args, **kwargs) -> None:
        # We don't use the `show` built into the DBTDeprecation class,
        # because we want to handle gating whether the warning should is
        # fired or not, and we want to track the number of times the warning
        # has been shown.
        if not self.only_show_once or self.occurances == 0:
            event = self.deprecation_warning.event(*args, **kwargs)

            # This is for maintaining current functionality wherein certain deprecations
            # allow for force firing as an error outside of warn_or_error.
            if fire_as_error:
                fire_event(event, level=EventLevel.ERROR)
            else:
                warn_or_error(event)

            self.deprecation_warning.track_deprecation_warn()

        self.occurances += 1
