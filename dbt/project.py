import os.path
import yaml
import pprint
import copy
import sys
import hashlib
import re
import dbt.deprecations

default_project_cfg = {
    'source-paths': ['models'],
    'macro-paths': ['macros'],
    'data-paths': ['data'],
    'test-paths': ['test'],
    'target-path': 'target',
    'clean-targets': ['target'],
    'outputs': {'default': {}},
    'target': 'default',
    'models': {},
    'profile': None,
    'repositories': [],
    'modules-path': 'dbt_modules'
}

default_profiles = {}

default_profiles_dir = os.path.join(os.path.expanduser('~'), '.dbt')


class DbtProjectError(Exception):
    def __init__(self, message, project):
        self.project = project
        super(DbtProjectError, self).__init__(message)


class Project(object):

    def __init__(self, cfg, profiles, profiles_dir, profile_to_load=None):
        self.cfg = default_project_cfg.copy()
        self.cfg.update(cfg)
        self.profiles = default_profiles.copy()
        self.profiles.update(profiles)
        self.profiles_dir = profiles_dir
        self.profile_to_load = profile_to_load

        # load profile from dbt_config.yml if cli arg isn't supplied
        if self.profile_to_load is None and self.cfg['profile'] is not None:
            self.profile_to_load = self.cfg['profile']

        if self.profile_to_load is None:
            raise DbtProjectError(
                "No profile was supplied in the dbt_project.yml file, or the "
                "command line", self)

        if self.profile_to_load in self.profiles:
            self.cfg.update(self.profiles[self.profile_to_load])
        else:
            raise DbtProjectError(
                "Could not find profile named '{}'"
                .format(self.profile_to_load), self)

    def __str__(self):
        return pprint.pformat({'project': self.cfg, 'profiles': self.profiles})

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        return self.cfg.__getitem__(key)

    def __contains__(self, key):
        return self.cfg.__contains__(key)

    def __setitem__(self, key, value):
        return self.cfg.__setitem__(key, value)

    def get(self, key, default=None):
        return self.cfg.get(key, default)

    def handle_deprecations(self):
        if 'run-target' in self.cfg:
            dbt.deprecations.warn('run-target')
            self.cfg['target'] = self.cfg['run-target']

        if not self.is_valid_package_name():
            dbt.deprecations.warn(
                'invalid-package-name', package_name=self['name'])

    def is_valid_package_name(self):
        if re.match(r"^[^\d\W]\w*\Z", self['name']):
            return True
        else:
            return False

    def run_environment(self):
        target_name = self.cfg['target']
        return self.cfg['outputs'][target_name]

    def get_target(self):
        ctx = self.context().get('env').copy()
        ctx['name'] = self.cfg['target']
        return ctx

    def context(self):
        target_cfg = self.run_environment()
        filtered_target = copy.deepcopy(target_cfg)
        filtered_target.pop('pass', None)
        return {'env': filtered_target}

    def validate(self):
        self.handle_deprecations()

        target_cfg = self.run_environment()
        target_name = self.cfg['target']

        package_name = self.cfg.get('name', None)
        package_version = self.cfg.get('version', None)

        if package_name is None or package_version is None:
            raise DbtProjectError(
                "Project name and version is not provided", self)

        required_keys = ['host', 'user', 'pass', 'schema', 'type',
                         'dbname', 'port']
        for key in required_keys:
            if key not in target_cfg or len(str(target_cfg[key])) == 0:
                raise DbtProjectError(
                    "Expected project configuration '{}' was not supplied"
                    .format(key), self)

    def hashed_name(self):
        if self.cfg.get("name", None) is None:
            return None

        project_name = self['name']
        return hashlib.md5(project_name.encode('utf-8')).hexdigest()


def read_profiles(profiles_dir=None):
    if profiles_dir is None:
        profiles_dir = default_profiles_dir

    profiles = {}
    paths = [
        os.path.join(profiles_dir, 'profiles.yml')
    ]
    for path in paths:
        if os.path.isfile(path):
            with open(path, 'r') as f:
                m = yaml.safe_load(f)
                valid_profiles = {k: v for (k, v) in m.items()
                                  if k != 'config'}
                profiles.update(valid_profiles)

    return profiles


def read_project(filename, profiles_dir=None, validate=True,
                 profile_to_load=None):
    if profiles_dir is None:
        profiles_dir = default_profiles_dir

    with open(filename, 'r') as f:
        project_cfg = yaml.safe_load(f)
        project_cfg['project-root'] = os.path.dirname(
            os.path.abspath(filename))
        profiles = read_profiles(profiles_dir)
        proj = Project(project_cfg, profiles, profiles_dir, profile_to_load)

        if validate:
            proj.validate()
        return proj
