import abc
from typing import Optional, Set, List, Dict, ClassVar
from types import ModuleType

import dbt.tracking

from dbt.common.events import types as common_types
from dbt.events import types as core_types


class DBTDeprecation:
    _name: ClassVar[Optional[str]] = None
    _event: ClassVar[Optional[str]] = None
    _event_module: ClassVar[Optional[ModuleType]] = common_types

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
            module_path = self._event_module
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
            dbt.common.events.functions.warn_or_error(event)
            self.track_deprecation_warn()
            active_deprecations.add(self.name)


class DBTCoreDeprecation(DBTDeprecation):
    _event_module = core_types


class PackageRedirectDeprecation(DBTCoreDeprecation):
    _name = "package-redirect"
    _event = "PackageRedirectDeprecation"


class PackageInstallPathDeprecation(DBTCoreDeprecation):
    _name = "install-packages-path"
    _event = "PackageInstallPathDeprecation"


class ConfigSourcePathDeprecation(DBTCoreDeprecation):
    _name = "project-config-source-paths"
    _event = "ConfigSourcePathDeprecation"


class ConfigDataPathDeprecation(DBTCoreDeprecation):
    _name = "project-config-data-paths"
    _event = "ConfigDataPathDeprecation"


def renamed_method(old_name: str, new_name: str):
    class AdapterDeprecationWarning(DBTCoreDeprecation):
        _name = "adapter:{}".format(old_name)
        _event = "AdapterDeprecationWarning"

    dep = AdapterDeprecationWarning()
    deprecations_list.append(dep)
    deprecations[dep.name] = dep


class MetricAttributesRenamed(DBTCoreDeprecation):
    _name = "metric-attr-renamed"
    _event = "MetricAttributesRenamed"


class ExposureNameDeprecation(DBTCoreDeprecation):
    _name = "exposure-name"
    _event = "ExposureNameDeprecation"


class ConfigLogPathDeprecation(DBTCoreDeprecation):
    _name = "project-config-log-path"
    _event = "ConfigLogPathDeprecation"


class ConfigTargetPathDeprecation(DBTCoreDeprecation):
    _name = "project-config-target-path"
    _event = "ConfigTargetPathDeprecation"


class CollectFreshnessReturnSignature(DBTCoreDeprecation):
    _name = "collect-freshness-return-signature"
    _event = "CollectFreshnessReturnSignature"


def renamed_env_var(old_name: str, new_name: str):
    class EnvironmentVariableRenamed(DBTCoreDeprecation):
        _name = f"environment-variable-renamed:{old_name}"
        _event = "EnvironmentVariableRenamed"

    dep = EnvironmentVariableRenamed()
    deprecations_list.append(dep)
    deprecations[dep.name] = dep

    def cb():
        dep.show(old_name=old_name, new_name=new_name)

    return cb


def warn(name, *args, **kwargs):
    if name not in deprecations:
        # this should (hopefully) never happen
        raise RuntimeError("Error showing deprecation warning: {}".format(name))

    deprecations[name].show(*args, **kwargs)


# these are globally available
# since modules are only imported once, active_deprecations is a singleton

active_deprecations: Set[str] = set()

deprecations_list: List[DBTDeprecation] = [
    PackageRedirectDeprecation(),
    PackageInstallPathDeprecation(),
    ConfigSourcePathDeprecation(),
    ConfigDataPathDeprecation(),
    MetricAttributesRenamed(),
    ExposureNameDeprecation(),
    ConfigLogPathDeprecation(),
    ConfigTargetPathDeprecation(),
    CollectFreshnessReturnSignature(),
]

deprecations: Dict[str, DBTDeprecation] = {d.name: d for d in deprecations_list}


def reset_deprecations():
    active_deprecations.clear()
