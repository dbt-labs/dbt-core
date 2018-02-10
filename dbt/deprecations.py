from dbt.logger import GLOBAL_LOGGER as logger


class DBTDeprecation(object):
    name = None
    description = None

    def show(self, *args, **kwargs):
        if self.name not in active_deprecations:
            desc = self.description.format(**kwargs)
            logger.info("* Deprecation Warning: {}\n".format(desc))
            active_deprecations.add(self.name)

# Leaving this as an example. Make sure to add new ones to deprecations_list
#       - Connor
#
# class DBTRunTargetDeprecation(DBTDeprecation):
#     name = 'run-target'
#     description = """profiles.yml configuration option 'run-target' is
#     deprecated. Please use 'target' instead. The 'run-target' option will be
#     removed (in favor of 'target') in DBT version 0.7.0"""


class SeedDropExistingDeprecation(DBTDeprecation):
    name = 'drop-existing'
    description = """The --drop-existing argument has been deprecated. Please
  use --full-refresh instead. The --drop-existing option will be removed in a
  future version of dbt."""


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
    SeedDropExistingDeprecation()
]

deprecations = {d.name: d for d in deprecations_list}


def reset_deprecations():
    active_deprecations.clear()
