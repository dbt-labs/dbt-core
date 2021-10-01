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
        raise NotImplementedError(
            'name not implemented for {}'.format(self)
        )

    def track_deprecation_warn(self) -> None:
        if dbt.tracking.active_user is not None:
            dbt.tracking.track_deprecation_warn({
                "deprecation_name": self.name
            })

    @property
    def description(self) -> str:
        if self._description is not None:
            return self._description
        raise NotImplementedError(
            'description not implemented for {}'.format(self)
        )

    def show(self, *args, **kwargs) -> None:
        if self.name not in active_deprecations:
            desc = self.description.format(**kwargs)
            msg = ui.line_wrap_message(
                desc, prefix='* Deprecation Warning: '
            )
            dbt.exceptions.warn_or_error(msg)
            self.track_deprecation_warn()
            active_deprecations.add(self.name)


class DispatchPackagesDeprecation(DBTDeprecation):
    _name = 'dispatch-packages'
    _description = '''\
    The "packages" argument of adapter.dispatch() has been deprecated.
    Use the "macro_namespace" argument instead.

    Raised during dispatch for: {macro_name}

    For more information, see:

    https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch
    '''


class PackageRedirectDeprecation(DBTDeprecation):
    _name = 'package-redirect'
    _description = '''\
    The `{old_name}` package is deprecated in favor of `{new_name}`. Please update
    your `packages.yml` configuration to use `{new_name}` instead.
    '''


_adapter_renamed_description = """\
The adapter function `adapter.{old_name}` is deprecated and will be removed in
a future release of dbt. Please use `adapter.{new_name}` instead.

Documentation for {new_name} can be found here:

    https://docs.getdbt.com/docs/adapter
"""


def renamed_method(old_name: str, new_name: str):

    class AdapterDeprecationWarning(DBTDeprecation):
        _name = 'adapter:{}'.format(old_name)
        _description = _adapter_renamed_description.format(old_name=old_name,
                                                           new_name=new_name)

    dep = AdapterDeprecationWarning()
    deprecations_list.append(dep)
    deprecations[dep.name] = dep


def warn(name, *args, **kwargs):
    if name not in deprecations:
        # this should (hopefully) never happen
        raise RuntimeError(
            "Error showing deprecation warning: {}".format(name)
        )

    deprecations[name].show(*args, **kwargs)


# these are globally available
# since modules are only imported once, active_deprecations is a singleton

active_deprecations: Set[str] = set()

deprecations_list: List[DBTDeprecation] = [
    DispatchPackagesDeprecation(),
    PackageRedirectDeprecation()
]

deprecations: Dict[str, DBTDeprecation] = {
    d.name: d for d in deprecations_list
}


def reset_deprecations():
    active_deprecations.clear()
