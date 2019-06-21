import dbt.links
import dbt.flags


class DBTDeprecation:
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


class GenerateSchemaNameSingleArgDeprecated(DBTDeprecation):
    name = 'generate-schema-name-single-arg'
    description = '''As of dbt v0.14.0, the `generate_schema_name` macro
  accepts a second "node" argument. The one-argument form of `generate_schema_name`
  is deprecated, and will become unsupported in a future release.

  For more information, see:
    https://docs.getdbt.com/v0.14/docs/upgrading-to-014
  '''  # noqa


class ArchiveDeprecated(DBTDeprecation):
    name = 'archives'
    description = '''As of dbt v0.14.0, the `dbt archive` command is renamed to
  `dbt snapshot` and "archives" are "snapshots". The `dbt archive` command will
  be removed in a future release.

  For more information, see:
    https://docs.getdbt.com/v0.14/docs/upgrading-to-014
  '''


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
    GenerateSchemaNameSingleArgDeprecated(),
    ArchiveDeprecated(),
]

deprecations = {d.name: d for d in deprecations_list}


def reset_deprecations():
    active_deprecations.clear()
