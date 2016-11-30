

class DBTDeprecation(object):
    name = None
    description = None

    def show(self):
        if self.name not in active_deprecations:
            print("* Deprecation Warning: {}".format(self.description))
            active_deprecations.add(self.name)


class DBTRunTargetDeprecation(DBTDeprecation):
    name = 'run-target'
    description = """profiles.yml configuration option 'run-target' is deprecated. Please use 'target' instead. 
  The 'run-target' option will be removed (in favor of 'target') in DBT version 0.6.0"""


def warn(name):
    if name not in deprecations:
        # this should (hopefully) never happen
        raise RuntimeError("Error showing deprecation warning: {}".format(name))

    deprecations[name].show()


# these are globally available
# since modules are only imported once, active_deprecations is a singleton

active_deprecations = set()

deprecations_list = [
    DBTRunTargetDeprecation()
]

deprecations = {d.name : d for d in deprecations_list}

def reset_deprecations():
    active_deprecations.clear()
