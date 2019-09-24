from typing import Optional, Set, List, Dict, ClassVar

import dbt.links
import dbt.exceptions
import dbt.flags


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
            dbt.exceptions.warn_or_error(
                "* Deprecation Warning: {}\n".format(desc)
            )
            active_deprecations.add(self.name)


class DBTRepositoriesDeprecation(DBTDeprecation):
    _name = "repositories"

    _description = """
    The dbt_project.yml configuration option 'repositories' is
  deprecated. Please place dependencies in the `packages.yml` file instead.
  The 'repositories' option will be removed in a future version of dbt.

  For more information, see: https://docs.getdbt.com/docs/package-management

  # Example packages.yml contents:

{recommendation}
  """.lstrip()


class GenerateSchemaNameSingleArgDeprecated(DBTDeprecation):
    _name = 'generate-schema-name-single-arg'

    _description = '''As of dbt v0.14.0, the `generate_schema_name` macro
  accepts a second "node" argument. The one-argument form of `generate_schema_name`
  is deprecated, and will become unsupported in a future release.

  For more information, see:
    https://docs.getdbt.com/v0.14/docs/upgrading-to-014
  '''  # noqa


class MaterializationReturnDeprecation(DBTDeprecation):
    _name = 'materialization-return'

    _description = '''
    The materialization ("{materialization}") did not explicitly return a list
    of relations to add to the cache. By default the target relation will be
    added, but this behavior will be removed in a future version of dbt.

  For more information, see:
  https://docs.getdbt.com/v0.15/docs/creating-new-materializations#section-6-returning-relations
    '''.lstrip()


class NotADictionaryDeprecation(DBTDeprecation):
    _name = 'not-a-dictionary'

    _description = '''
    The object ("{obj}") was used as a dictionary. In a future version of dbt
    this capability will be removed from objects of this type.
    '''.lstrip()


_adapter_renamed_description = """\
The adapter function `adapter.{old_name}` is deprecated and will be removed in
 a future release of dbt. Please use `adapter.{new_name}` instead.
 Documentation for {new_name} can be found here:
 https://docs.getdbt.com/docs/adapter"""


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
    DBTRepositoriesDeprecation(),
    GenerateSchemaNameSingleArgDeprecated(),
    MaterializationReturnDeprecation(),
    NotADictionaryDeprecation(),
]

deprecations: Dict[str, DBTDeprecation] = {
    d.name: d for d in deprecations_list
}


def reset_deprecations():
    active_deprecations.clear()
