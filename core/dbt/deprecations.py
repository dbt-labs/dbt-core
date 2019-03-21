from dbt.logger import GLOBAL_LOGGER as logger
import dbt.links
import dbt.flags


class DBTDeprecation(object):
    name = None
    description = None

    def show(self, *args, **kwargs):
        if self.name not in active_deprecations:
            desc = self.description.format(**kwargs)
            dbt.exceptions.warn_or_error(
                "* Deprecation Warning: {}\n".format(desc)
            )
            active_deprecations.add(self.name)


class DBTRepositoriesDeprecation(DBTDeprecation):
    name = "repositories"
    description = """The dbt_project.yml configuration option 'repositories' is
  deprecated. Please place dependencies in the `packages.yml` file instead.
  The 'repositories' option will be removed in a future version of dbt.

  For more information, see: https://docs.getdbt.com/docs/package-management

  # Example packages.yml contents:

{recommendation}
  """


class SqlWhereDeprecation(DBTDeprecation):
    name = "sql_where"
    description = """\
The `sql_where` option for incremental models is deprecated and will be
  removed in a future release. Check the docs for more information

  {}
  """.format(dbt.links.IncrementalDocs)


class SeedDropExistingDeprecation(DBTDeprecation):
    name = 'drop-existing'
    description = """The --drop-existing argument to `dbt seed` has been
  deprecated. Please use --full-refresh instead. The --drop-existing option
  will be removed in a future version of dbt."""


_adapter_renamed_description = """\
The adapter function `adapter.{old_name}` is deprecated and will be removed in
 a future release of dbt. Please use `adapter.{new_name}` instead.
 Documentation for {new_name} can be found here:
 https://docs.getdbt.com/reference#adapter"""


def renamed_method(old_name, new_name):
    class AdapterDeprecationWarning(DBTDeprecation):
        name = 'adapter:{}'.format(old_name)
        description = _adapter_renamed_description.format(old_name=old_name,
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

active_deprecations = set()

deprecations_list = [
    DBTRepositoriesDeprecation(),
    SeedDropExistingDeprecation(),
    SqlWhereDeprecation(),
]

deprecations = {d.name: d for d in deprecations_list}


def reset_deprecations():
    active_deprecations.clear()
