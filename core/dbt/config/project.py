from copy import deepcopy
import hashlib
import os
import pprint

from dbt.clients.system import resolve_path_from_base
from dbt.clients.system import path_exists
from dbt.clients.system import load_file_contents
from dbt.clients.yaml_helper import load_yaml_text
from dbt.exceptions import DbtProjectError
from dbt.exceptions import RecursionException
from dbt.exceptions import SemverException
from dbt.exceptions import ValidationException
from dbt.exceptions import warn_or_error
from dbt.semver import VersionSpecifier
from dbt.semver import versions_compatible
from dbt.version import get_installed_version
from dbt.ui import printer
from dbt.utils import deep_map
from dbt.utils import parse_cli_vars
from dbt.parser.source_config import SourceConfig

from dbt.contracts.project import Project as ProjectContract
from dbt.contracts.project import PackageConfig

from .renderer import ConfigRenderer


UNUSED_RESOURCE_CONFIGURATION_PATH_MESSAGE = """\
WARNING: Configuration paths exist in your dbt_project.yml file which do not \
apply to any resources.
There are {} unused configuration paths:\n{}
"""


INVALID_VERSION_ERROR = """\
This version of dbt is not supported with the '{package}' package.
  Installed version of dbt: {installed}
  Required version of dbt for '{package}': {version_spec}
Check the requirements for the '{package}' package, or run dbt again with \
--no-version-check
"""


IMPOSSIBLE_VERSION_ERROR = """\
The package version requirement can never be satisfied for the '{package}
package.
  Required versions of dbt for '{package}': {version_spec}
Check the requirements for the '{package}' package, or run dbt again with \
--no-version-check
"""


def _list_if_none(value):
    if value is None:
        value = []
    return value


def _dict_if_none(value):
    if value is None:
        value = {}
    return value


def _list_if_none_or_string(value):
    value = _list_if_none(value)
    if isinstance(value, str):
        return [value]
    return value


def _load_yaml(path):
    contents = load_file_contents(path)
    return load_yaml_text(contents)


def _get_config_paths(config, path=(), paths=None):
    if paths is None:
        paths = set()

    for key, value in config.items():
        if isinstance(value, dict):
            if key in SourceConfig.ConfigKeys:
                if path not in paths:
                    paths.add(path)
            else:
                _get_config_paths(value, path + (key,), paths)
        else:
            if path not in paths:
                paths.add(path)

    return frozenset(paths)


def _is_config_used(path, fqns):
    if fqns:
        for fqn in fqns:
            if len(path) <= len(fqn) and fqn[:len(path)] == path:
                return True
    return False


def package_data_from_root(project_root):
    package_filepath = resolve_path_from_base(
        'packages.yml', project_root
    )

    if path_exists(package_filepath):
        packages_dict = _load_yaml(package_filepath)
    else:
        packages_dict = None
    return packages_dict


def package_config_from_data(packages_data):
    if packages_data is None:
        packages_data = {'packages': []}

    try:
        packages = PackageConfig(**packages_data)
    except ValidationException as e:
        raise DbtProjectError('Invalid package config: {}'.format(str(e)))
    return packages


def _parse_versions(versions):
    """Parse multiple versions as read from disk. The versions value may be any
    one of:
        - a single version string ('>0.12.1')
        - a single string specifying multiple comma-separated versions
            ('>0.11.1,<=0.12.2')
        - an array of single-version strings (['>0.11.1', '<=0.12.2'])

    Regardless, this will return a list of VersionSpecifiers
    """
    if isinstance(versions, str):
        versions = versions.split(',')
    return [VersionSpecifier.from_version_string(v) for v in versions]


class Project:
    def __init__(self, project_name, version, project_root, profile_name,
                 source_paths, macro_paths, data_paths, test_paths,
                 analysis_paths, docs_paths, target_path, snapshot_paths,
                 clean_targets, log_path, modules_path, quoting, models,
                 on_run_start, on_run_end, archive, seeds, dbt_version,
                 packages):
        self.project_name = project_name
        self.version = version
        self.project_root = project_root
        self.profile_name = profile_name
        self.source_paths = source_paths
        self.macro_paths = macro_paths
        self.data_paths = data_paths
        self.test_paths = test_paths
        self.analysis_paths = analysis_paths
        self.docs_paths = docs_paths
        self.target_path = target_path
        self.snapshot_paths = snapshot_paths
        self.clean_targets = clean_targets
        self.log_path = log_path
        self.modules_path = modules_path
        self.quoting = quoting
        self.models = models
        self.on_run_start = on_run_start
        self.on_run_end = on_run_end
        self.archive = archive
        self.seeds = seeds
        self.dbt_version = dbt_version
        self.packages = packages

    @staticmethod
    def _preprocess(project_dict):
        """Pre-process certain special keys to convert them from None values
        into empty containers, and to turn strings into arrays of strings.
        """
        handlers = {
            ('archive',): _list_if_none,
            ('on-run-start',): _list_if_none_or_string,
            ('on-run-end',): _list_if_none_or_string,
        }

        for k in ('models', 'seeds'):
            handlers[(k,)] = _dict_if_none
            handlers[(k, 'vars')] = _dict_if_none
            handlers[(k, 'pre-hook')] = _list_if_none_or_string
            handlers[(k, 'post-hook')] = _list_if_none_or_string
        handlers[('seeds', 'column_types')] = _dict_if_none

        def converter(value, keypath):
            if keypath in handlers:
                handler = handlers[keypath]
                return handler(value)
            else:
                return value

        return deep_map(converter, project_dict)

    @classmethod
    def from_project_config(cls, project_dict, packages_dict=None):
        """Create a project from its project and package configuration, as read
        by yaml.safe_load().

        :param project_dict dict: The dictionary as read from disk
        :param packages_dict Optional[dict]: If it exists, the packages file as
            read from disk.
        :raises DbtProjectError: If the project is missing or invalid, or if
            the packages file exists and is invalid.
        :returns Project: The project, with defaults populated.
        """
        try:
            project_dict = cls._preprocess(project_dict)
        except RecursionException:
            raise DbtProjectError(
                'Cycle detected: Project input has a reference to itself',
                project=project_dict
            )
        # just for validation.
        try:
            ProjectContract(**project_dict)
        except ValidationException as e:
            raise DbtProjectError(str(e))

        # name/version are required in the Project definition, so we can assume
        # they are present
        name = project_dict['name']
        version = project_dict['version']
        # this is added at project_dict parse time and should always be here
        # once we see it.
        project_root = project_dict['project-root']
        # this is only optional in the sense that if it's not present, it needs
        # to have been a cli argument.
        profile_name = project_dict.get('profile')
        # these are optional
        source_paths = project_dict.get('source-paths', ['models'])
        macro_paths = project_dict.get('macro-paths', ['macros'])
        data_paths = project_dict.get('data-paths', ['data'])
        test_paths = project_dict.get('test-paths', ['test'])
        analysis_paths = project_dict.get('analysis-paths', [])
        docs_paths = project_dict.get('docs-paths', source_paths[:])
        target_path = project_dict.get('target-path', 'target')
        snapshot_paths = project_dict.get('snapshot-paths', ['snapshots'])
        # should this also include the modules path by default?
        clean_targets = project_dict.get('clean-targets', [target_path])
        log_path = project_dict.get('log-path', 'logs')
        modules_path = project_dict.get('modules-path', 'dbt_modules')
        # in the default case we'll populate this once we know the adapter type
        quoting = project_dict.get('quoting', {})

        models = project_dict.get('models', {})
        on_run_start = project_dict.get('on-run-start', [])
        on_run_end = project_dict.get('on-run-end', [])
        archive = project_dict.get('archive', [])
        seeds = project_dict.get('seeds', {})
        dbt_raw_version = project_dict.get('require-dbt-version', '>=0.0.0')

        try:
            dbt_version = _parse_versions(dbt_raw_version)
        except SemverException as e:
            raise DbtProjectError(str(e))

        packages = package_config_from_data(packages_dict)

        project = cls(
            project_name=name,
            version=version,
            project_root=project_root,
            profile_name=profile_name,
            source_paths=source_paths,
            macro_paths=macro_paths,
            data_paths=data_paths,
            test_paths=test_paths,
            analysis_paths=analysis_paths,
            docs_paths=docs_paths,
            target_path=target_path,
            snapshot_paths=snapshot_paths,
            clean_targets=clean_targets,
            log_path=log_path,
            modules_path=modules_path,
            quoting=quoting,
            models=models,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            archive=archive,
            seeds=seeds,
            dbt_version=dbt_version,
            packages=packages
        )
        # sanity check - this means an internal issue
        project.validate()
        return project

    def __str__(self):
        cfg = self.to_project_config(with_packages=True)
        return pprint.pformat(cfg)

    def __eq__(self, other):
        if not (isinstance(other, self.__class__) and
                isinstance(self, other.__class__)):
            return False
        return self.to_project_config(with_packages=True) == \
            other.to_project_config(with_packages=True)

    def to_project_config(self, with_packages=False):
        """Return a dict representation of the config that could be written to
        disk with `yaml.safe_dump` to get this configuration.

        :param with_packages bool: If True, include the serialized packages
            file in the root.
        :returns dict: The serialized profile.
        """
        result = deepcopy({
            'name': self.project_name,
            'version': self.version,
            'project-root': self.project_root,
            'profile': self.profile_name,
            'source-paths': self.source_paths,
            'macro-paths': self.macro_paths,
            'data-paths': self.data_paths,
            'test-paths': self.test_paths,
            'analysis-paths': self.analysis_paths,
            'docs-paths': self.docs_paths,
            'target-path': self.target_path,
            'snapshot-paths': self.snapshot_paths,
            'clean-targets': self.clean_targets,
            'log-path': self.log_path,
            'quoting': self.quoting,
            'models': self.models,
            'on-run-start': self.on_run_start,
            'on-run-end': self.on_run_end,
            'archive': self.archive,
            'seeds': self.seeds,
            'require-dbt-version': [
                v.to_version_string() for v in self.dbt_version
            ],
        })
        if with_packages:
            result.update(self.packages.serialize())
        return result

    def validate(self):
        try:
            ProjectContract(**self.to_project_config())
        except ValidationException as exc:
            raise DbtProjectError(str(exc))

    @classmethod
    def from_project_root(cls, project_root, cli_vars):
        """Create a project from a root directory. Reads in dbt_project.yml and
        packages.yml, if it exists.

        :param project_root str: The path to the project root to load.
        :raises DbtProjectError: If the project is missing or invalid, or if
            the packages file exists and is invalid.
        :returns Project: The project, with defaults populated.
        """
        project_root = os.path.normpath(project_root)
        project_yaml_filepath = os.path.join(project_root, 'dbt_project.yml')

        # get the project.yml contents
        if not path_exists(project_yaml_filepath):
            raise DbtProjectError(
                'no dbt_project.yml found at expected path {}'
                .format(project_yaml_filepath)
            )

        if isinstance(cli_vars, str):
            cli_vars = parse_cli_vars(cli_vars)
        renderer = ConfigRenderer(cli_vars)

        project_dict = _load_yaml(project_yaml_filepath)
        rendered_project = renderer.render_project(project_dict)
        rendered_project['project-root'] = project_root
        packages_dict = package_data_from_root(project_root)
        return cls.from_project_config(rendered_project, packages_dict)

    @classmethod
    def from_current_directory(cls, cli_vars):
        return cls.from_project_root(os.getcwd(), cli_vars)

    @classmethod
    def from_args(cls, args):
        return cls.from_current_directory(getattr(args, 'vars', '{}'))

    def hashed_name(self):
        return hashlib.md5(self.project_name.encode('utf-8')).hexdigest()

    def get_resource_config_paths(self):
        """Return a dictionary with 'seeds' and 'models' keys whose values are
        lists of lists of strings, where each inner list of strings represents
        a configured path in the resource.
        """
        return {
            'models': _get_config_paths(self.models),
            'seeds': _get_config_paths(self.seeds),
        }

    def get_unused_resource_config_paths(self, resource_fqns, disabled):
        """Return a list of lists of strings, where each inner list of strings
        represents a type + FQN path of a resource configuration that is not
        used.
        """
        disabled_fqns = frozenset(tuple(fqn) for fqn in disabled)
        resource_config_paths = self.get_resource_config_paths()
        unused_resource_config_paths = []
        for resource_type, config_paths in resource_config_paths.items():
            used_fqns = resource_fqns.get(resource_type, frozenset())
            fqns = used_fqns | disabled_fqns

            for config_path in config_paths:
                if not _is_config_used(config_path, fqns):
                    unused_resource_config_paths.append(
                        (resource_type,) + config_path
                    )
        return unused_resource_config_paths

    def warn_for_unused_resource_config_paths(self, resource_fqns, disabled):
        unused = self.get_unused_resource_config_paths(resource_fqns, disabled)
        if len(unused) == 0:
            return

        msg = UNUSED_RESOURCE_CONFIGURATION_PATH_MESSAGE.format(
            len(unused),
            '\n'.join('- {}'.format('.'.join(u)) for u in unused)
        )
        warn_or_error(msg, log_fmt=printer.yellow('{}'))

    def validate_version(self):
        """Ensure this package works with the installed version of dbt."""
        installed = get_installed_version()
        if not versions_compatible(*self.dbt_version):
            msg = IMPOSSIBLE_VERSION_ERROR.format(
                package=self.project_name,
                version_spec=[
                    x.to_version_string() for x in self.dbt_version
                ]
            )
            raise DbtProjectError(msg)

        if not versions_compatible(installed, *self.dbt_version):
            msg = INVALID_VERSION_ERROR.format(
                package=self.project_name,
                installed=installed.to_version_string(),
                version_spec=[
                    x.to_version_string() for x in self.dbt_version
                ]
            )
            raise DbtProjectError(msg)
