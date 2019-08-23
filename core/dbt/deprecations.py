from typing import Optional, Set, List, Dict, ClassVar

import dbt.exceptions
from dbt import ui

import dbt.tracking


class DBTDeprecation:
    _name: ClassVar[Optional[str]] = None
    _description: ClassVar[Optional[str]] = None

    @property
    def name(self) -> str:
        if self._name is not None:
            return self._name
        raise NotImplementedError("name not implemented for {}".format(self))

    def track_deprecation_warn(self) -> None:
        if dbt.tracking.active_user is not None:
            dbt.tracking.track_deprecation_warn({"deprecation_name": self.name})

    @property
    def description(self) -> str:
        if self._description is not None:
            return self._description
        raise NotImplementedError("description not implemented for {}".format(self))

    def show(self, *args, **kwargs) -> None:
        if self.name not in active_deprecations:
            desc = self.description.format(**kwargs)
            msg = ui.line_wrap_message(desc, prefix="Deprecated functionality\n\n")
            dbt.exceptions.warn_or_error(msg, log_fmt=ui.warning_tag("{}"))
            self.track_deprecation_warn()
            active_deprecations.add(self.name)


class PackageRedirectDeprecation(DBTDeprecation):
    _name = "package-redirect"
    _description = """\
    The `{old_name}` package is deprecated in favor of `{new_name}`. Please update
    your `packages.yml` configuration to use `{new_name}` instead.
    """


class PackageInstallPathDeprecation(DBTDeprecation):
    _name = "install-packages-path"
    _description = """\
    The default package install path has changed from `dbt_modules` to `dbt_packages`.
    Please update `clean-targets` in `dbt_project.yml` and check `.gitignore` as well.
    Or, set `packages-install-path: dbt_modules` if you'd like to keep the current value.
    """


class ConfigPathDeprecation(DBTDeprecation):
    _description = """\
    The `{deprecated_path}` config has been renamed to `{exp_path}`.
    Please update your `dbt_project.yml` configuration to reflect this change.
    """


class ConfigSourcePathDeprecation(ConfigPathDeprecation):
    _name = "project-config-source-paths"


class ConfigDataPathDeprecation(ConfigPathDeprecation):
    _name = "project-config-data-paths"


_adapter_renamed_description = """\
The adapter function `adapter.{old_name}` is deprecated and will be removed in
a future release of dbt. Please use `adapter.{new_name}` instead.

Documentation for {new_name} can be found here:

    https://docs.getdbt.com/docs/adapter
"""


def renamed_method(old_name: str, new_name: str):
    class AdapterDeprecationWarning(DBTDeprecation):
        _name = "adapter:{}".format(old_name)
        _description = _adapter_renamed_description.format(old_name=old_name, new_name=new_name)

    dep = AdapterDeprecationWarning()
    deprecations_list.append(dep)
    deprecations[dep.name] = dep


def warn(name, *args, **kwargs):
    if name not in deprecations:
        # this should (hopefully) never happen
        raise RuntimeError("Error showing deprecation warning: {}".format(name))

    deprecations[name].show(*args, **kwargs)


# these are globally available
# since modules are only imported once, active_deprecations is a singleton

active_deprecations: Set[str] = set()

deprecations_list: List[DBTDeprecation] = [
    ConfigSourcePathDeprecation(),
    ConfigDataPathDeprecation(),
    PackageInstallPathDeprecation(),
    PackageRedirectDeprecation(),
]

deprecations: Dict[str, DBTDeprecation] = {d.name: d for d in deprecations_list}


def reset_deprecations():
    active_deprecations.clear()
