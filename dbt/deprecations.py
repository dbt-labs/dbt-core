from dbt.logger import GLOBAL_LOGGER as logger


class DBTDeprecation(object):
    name = None
    description = None

    def show(self, *args, **kwargs):
        if self.name not in active_deprecations:
            desc = self.description.format(**kwargs)
            logger.info("* Deprecation Warning: {}\n".format(desc))
            active_deprecations.add(self.name)


class DBTRunTargetDeprecation(DBTDeprecation):
    name = 'run-target'
    description = """profiles.yml configuration option 'run-target' is
    deprecated. Please use 'target' instead. The 'run-target' option will be
    removed (in favor of 'target') in DBT version 0.7.0"""


class DBTInvalidPackageName(DBTDeprecation):
    name = 'invalid-package-name'
    description = """The package name '{package_name}' is not valid. Package
    names must only contain letters and underscores. Packages with invalid
    names will fail to compile in DBT version 0.7.0"""


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
    DBTRunTargetDeprecation(),
    DBTInvalidPackageName()
]

deprecations = {d.name: d for d in deprecations_list}


def reset_deprecations():
    active_deprecations.clear()
