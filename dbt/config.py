import os.path
import os
from copy import deepcopy
import hashlib
import pprint

import dbt.exceptions
import dbt.clients.yaml_helper
import dbt.clients.system
import dbt.utils
from dbt.contracts.connection import Connection, create_credentials
from dbt.contracts.project import Project as ProjectContract, Configuration, \
    PackageConfig, ProfileConfig
from dbt.exceptions import DbtProjectError, DbtProfileError
from dbt.context.common import env_var, Var
from dbt import compat
from dbt.adapters.factory import get_relation_class_by_name

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import DBTConfigKeys
import dbt.ui.printer

DEFAULT_THREADS = 1
DEFAULT_SEND_ANONYMOUS_USAGE_STATS = True
DEFAULT_USE_COLORS = True
DEFAULT_PROFILES_DIR = os.path.join(os.path.expanduser('~'), '.dbt')
PROFILES_DIR = os.path.expanduser(
        os.environ.get('DBT_PROFILES_DIR', DEFAULT_PROFILES_DIR)
        )

INVALID_PROFILE_MESSAGE = """
dbt encountered an error while trying to read your profiles.yml file.

{error_string}
"""


NO_SUPPLIED_PROFILE_ERROR = """\
dbt cannot run because no profile was specified for this dbt project.
To specify a profile for this project, add a line like the this to
your dbt_project.yml file:

profile: [profile name]

Here, [profile name] should be replaced with a profile name
defined in your profiles.yml file. You can find profiles.yml here:

{profiles_file}/profiles.yml
""".format(profiles_file=PROFILES_DIR)


UNUSED_RESOURCE_CONFIGURATION_PATH_MESSAGE = """\
WARNING: Configuration paths exist in your dbt_project.yml file which do not \
apply to any resources.
There are {} unused configuration paths:\n{}
"""


def read_profile(profiles_dir):
    path = os.path.join(profiles_dir, 'profiles.yml')

    contents = None
    if os.path.isfile(path):
        try:
            contents = dbt.clients.system.load_file_contents(path, strip=False)
            return dbt.clients.yaml_helper.load_yaml_text(contents)
        except dbt.exceptions.ValidationException as e:
            msg = INVALID_PROFILE_MESSAGE.format(error_string=e)
            raise dbt.exceptions.ValidationException(msg)

    return {}


def read_profiles(profiles_dir=None):
    """This is only used in main, for some error handling"""
    if profiles_dir is None:
        profiles_dir = PROFILES_DIR

    raw_profiles = read_profile(profiles_dir)

    if raw_profiles is None:
        profiles = {}
    else:
        profiles = {k: v for (k, v) in raw_profiles.items() if k != 'config'}

    return profiles


def read_config(profiles_dir):
    profile = read_profile(profiles_dir)
    if profile is None:
        return {}
    else:
        return profile.get('config', {})


def send_anonymous_usage_stats(config):
    return config.get('send_anonymous_usage_stats', True)


def colorize_output(config):
    return config.get('use_colors', True)


class ConfigRenderer(object):
    """A renderer provides configuration rendering for a given set of cli
    variables and a render type.
    """
    def __init__(self, cli_vars):
        self.context = {'env_var': env_var}
        self.context['var'] = Var(None, self.context, cli_vars)

    @staticmethod
    def _is_hook_or_model_vars_path(keypath):
        if not keypath:
            return False

        first = keypath[0]
        # run hooks
        if first in {'on-run-start', 'on-run-end'}:
            return True
        # models have two things to avoid
        if first in {'seeds', 'models'}:
            # model-level hooks
            if 'pre-hook' in keypath or 'post-hook' in keypath:
                return True
            # model-level 'vars' declarations
            if 'vars' in keypath:
                return True

        return False

    def _render_project_entry(self, value, keypath):
        """Render an entry, in case it's jinja. This is meant to be passed to
        dbt.utils.deep_map.

        If the parsed entry is a string and has the name 'port', this will
        attempt to cast it to an int, and on failure will return the parsed
        string.

        :param value Any: The value to potentially render
        :param key str: The key to convert on.
        :return Any: The rendered entry.
        """
        # hooks should be treated as raw sql, they'll get rendered later.
        # Same goes for 'vars' declarations inside 'models'/'seeds'.
        if self._is_hook_or_model_vars_path(keypath):
            return value

        return self.render_value(value)

    def render_value(self, value, keypath=None):
        # keypath is ignored.
        # if it wasn't read as a string, ignore it
        if not isinstance(value, compat.basestring):
            return value

        return dbt.clients.jinja.get_rendered(value, self.context)

    def _render_profile_data(self, value, keypath):
        result = self.render_value(value)
        if len(keypath) == 1 and keypath[-1] == 'port':
            try:
                result = int(result)
            except ValueError:
                # let the validator or connection handle this
                pass
        return result

    def render(self, as_parsed):
        return dbt.utils.deep_map(self.render_value, as_parsed)

    def render_project(self, as_parsed):
        """Render the parsed data, returning a new dict (or whatever was read).
        """
        return dbt.utils.deep_map(self._render_project_entry, as_parsed)

    def render_profile_data(self, as_parsed):
        """Render the chosen profile entry, as it was parsed."""
        return dbt.utils.deep_map(self._render_profile_data, as_parsed)


class Project(object):
    def __init__(self, project_name, version, project_root, profile_name,
                 source_paths, macro_paths, data_paths, test_paths,
                 analysis_paths, docs_paths, target_path, clean_targets,
                 log_path, modules_path, quoting, models, on_run_start,
                 on_run_end, archive, seeds, packages):
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
        self.clean_targets = clean_targets
        self.log_path = log_path
        self.modules_path = modules_path
        self.quoting = quoting
        self.models = models
        self.on_run_start = on_run_start
        self.on_run_end = on_run_end
        self.archive = archive
        self.seeds = seeds
        self.packages = packages

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
        # just for validation.
        try:
            ProjectContract(**project_dict)
        except dbt.exceptions.ValidationException as e:
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
            clean_targets=clean_targets,
            log_path=log_path,
            modules_path=modules_path,
            quoting=quoting,
            models=models,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            archive=archive,
            seeds=seeds,
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
            'clean-targets': self.clean_targets,
            'log-path': self.log_path,
            'quoting': self.quoting,
            'models': self.models,
            'on-run-start': self.on_run_start,
            'on-run-end': self.on_run_end,
            'archive': self.archive,
            'seeds': self.seeds,
        })
        if with_packages:
            result.update(self.packages.serialize())
        return result

    def validate(self):
        try:
            ProjectContract(**self.to_project_config())
        except dbt.exceptions.ValidationException as exc:
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
        if not dbt.clients.system.path_exists(project_yaml_filepath):
            raise DbtProjectError(
                'no dbt_project.yml found at expected path {}'
                .format(project_yaml_filepath)
            )

        if isinstance(cli_vars, compat.basestring):
            cli_vars = dbt.utils.parse_cli_vars(cli_vars)
        renderer = ConfigRenderer(cli_vars)

        project_dict = _load_yaml(project_yaml_filepath)
        rendered_project = renderer.render_project(project_dict)
        rendered_project['project-root'] = project_root
        packages_dict = package_data_from_root(project_root)
        return cls.from_project_config(rendered_project, packages_dict)

    @classmethod
    def from_current_directory(cls, cli_vars):
        return cls.from_project_root(os.getcwd(), cli_vars)

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
        logger.info(dbt.ui.printer.yellow(msg))


class Profile(object):
    def __init__(self, profile_name, target_name, send_anonymous_usage_stats,
                 use_colors, threads, credentials):
        self.profile_name = profile_name
        self.target_name = target_name
        self.send_anonymous_usage_stats = send_anonymous_usage_stats
        self.use_colors = use_colors
        self.threads = threads
        self.credentials = credentials

    def to_profile_info(self, serialize_credentials=False):
        """Unlike to_project_config, this dict is not a mirror of any existing
        on-disk data structure. It's used when creating a new profile from an
        existing one.

        :param serialize_credentials bool: If True, serialize the credentials.
            Otherwise, the Credentials object will be copied.
        :returns dict: The serialized profile.
        """
        result = {
            'profile_name': self.profile_name,
            'target_name': self.target_name,
            'send_anonymous_usage_stats': self.send_anonymous_usage_stats,
            'use_colors': self.use_colors,
            'threads': self.threads,
            'credentials': self.credentials.incorporate(),
        }
        if serialize_credentials:
            result['credentials'] = result['credentials'].serialize()
        return result

    def __str__(self):
        return pprint.pformat(self.to_profile_info())

    def __eq__(self, other):
        if not (isinstance(other, self.__class__) and
                isinstance(self, other.__class__)):
            return False
            return False
        return self.to_profile_info() == other.to_profile_info()

    def validate(self):
        try:
            ProfileConfig(**self.to_profile_info(serialize_credentials=True))
        except dbt.exceptions.ValidationException as exc:
            raise DbtProfileError(str(exc))

    @staticmethod
    def _credentials_from_profile(profile, profile_name, target_name):
        # credentials carry their 'type' in their actual type, not their
        # attributes. We do want this in order to pick our Credentials class.
        if 'type' not in profile:
            raise DbtProfileError(
                'required field "type" not found in profile {} and target {}'
                .format(profile_name, target_name))

        typename = profile.pop('type')
        try:
            credentials = create_credentials(typename, profile)
        except dbt.exceptions.RuntimeException as e:
            raise DbtProfileError(
                'Credentials in profile "{}", target "{}" invalid: {}'
                .format(profile_name, target_name, str(e))
            )
        return credentials

    @staticmethod
    def _pick_profile_name(args_profile_name, project_profile_name=None):
        profile_name = project_profile_name
        if args_profile_name is not None:
            profile_name = args_profile_name
        if profile_name is None:
            raise DbtProjectError(NO_SUPPLIED_PROFILE_ERROR)
        return profile_name

    @staticmethod
    def _get_profile_data(profile, profile_name, target_name):
        if 'outputs' not in profile:
            raise DbtProfileError(
                "outputs not specified in profile '{}'".format(profile_name)
            )
        outputs = profile['outputs']

        if target_name not in outputs:
            outputs = '\n'.join(' - {}'.format(output)
                                for output in outputs)
            msg = ("The profile '{}' does not have a target named '{}'. The "
                   "valid target names for this profile are:\n{}"
                   .format(profile_name, target_name, outputs))
            raise DbtProfileError(msg, result_type='invalid_target')
        profile_data = outputs[target_name]
        return profile_data

    @classmethod
    def from_credentials(cls, credentials, threads, profile_name, target_name,
                         user_cfg=None):
        """Create a profile from an existing set of Credentials and the
        remaining information.

        :param credentials Credentials: The credentials for this profile.
        :param threads int: The number of threads to use for connections.
        :param profile_name str: The profile name used for this profile.
        :param target_name str: The target name used for this profile.
        :param user_cfg Optional[dict]: The user-level config block from the
            raw profiles, if specified.
        :raises DbtProfileError: If the profile is invalid.
        :returns Profile: The new Profile object.
        """
        if user_cfg is None:
            user_cfg = {}
        send_anonymous_usage_stats = user_cfg.get(
            'send_anonymous_usage_stats',
            DEFAULT_SEND_ANONYMOUS_USAGE_STATS
        )
        use_colors = user_cfg.get(
            'use_colors',
            DEFAULT_USE_COLORS
        )
        profile = cls(
            profile_name=profile_name,
            target_name=target_name,
            send_anonymous_usage_stats=send_anonymous_usage_stats,
            use_colors=use_colors,
            threads=threads,
            credentials=credentials
        )
        profile.validate()
        return profile

    @classmethod
    def _render_profile(cls, raw_profile, profile_name, target_override,
                        cli_vars):
        """This is a containment zone for the hateful way we're rendering
        profiles.
        """
        renderer = ConfigRenderer(cli_vars=cli_vars)

        # rendering profiles is a bit complex. Two constraints cause trouble:
        # 1) users should be able to use environment/cli variables to specify
        #    the target in their profile.
        # 2) Missing environment/cli variables in profiles/targets that don't
        #    end up getting selected should not cause errors.
        # so first we'll just render the target name, then we use that rendered
        # name to extract a profile that we can render.
        if target_override is not None:
            target_name = target_override
        elif 'target' in raw_profile:
            # render the target if it was parsed from yaml
            target_name = renderer.render_value(raw_profile['target'])
        else:
            raise DbtProfileError(
                "target not specified in profile '{}'".format(profile_name)
            )

        raw_profile_data = cls._get_profile_data(
            raw_profile, profile_name, target_name
        )

        profile_data = renderer.render_profile_data(raw_profile_data)
        return target_name, profile_data

    @classmethod
    def from_raw_profile_info(cls, raw_profile, profile_name, cli_vars,
                              user_cfg=None, target_override=None,
                              threads_override=None):
        """Create a profile from its raw profile information.

         (this is an intermediate step, mostly useful for unit testing)

        :param raw_profile dict: The profile data for a single profile, from
            disk as yaml and its values rendered with jinja.
        :param profile_name str: The profile name used.
        :param cli_vars dict: The command-line variables passed as arguments,
            as a dict.
        :param user_cfg Optional[dict]: The global config for the user, if it
            was present.
        :param target_override Optional[str]: The target to use, if provided on
            the command line.
        :param threads_override Optional[str]: The thread count to use, if
            provided on the command line.
        :raises DbtProfileError: If the profile is invalid or missing, or the
            target could not be found
        :returns Profile: The new Profile object.
        """
        # user_cfg is not rendered since it only contains booleans.
        # TODO: should it be, and the values coerced to bool?
        target_name, profile_data = cls._render_profile(
            raw_profile, profile_name, target_override, cli_vars
        )

        # valid connections never include the number of threads, but it's
        # stored on a per-connection level in the raw configs
        threads = profile_data.pop('threads', DEFAULT_THREADS)
        if threads_override is not None:
            threads = threads_override

        credentials = cls._credentials_from_profile(
            profile_data, profile_name, target_name
        )
        return cls.from_credentials(
            credentials=credentials,
            profile_name=profile_name,
            target_name=target_name,
            threads=threads,
            user_cfg=user_cfg
        )

    @classmethod
    def from_raw_profiles(cls, raw_profiles, profile_name, cli_vars,
                          target_override=None, threads_override=None):
        """
        :param raw_profiles dict: The profile data, from disk as yaml.
        :param profile_name str: The profile name to use.
        :param cli_vars dict: The command-line variables passed as arguments,
            as a dict.
        :param target_override Optional[str]: The target to use, if provided on
            the command line.
        :param threads_override Optional[str]: The thread count to use, if
            provided on the command line.
        :raises DbtProjectError: If there is no profile name specified in the
            project or the command line arguments
        :raises DbtProfileError: If the profile is invalid or missing, or the
            target could not be found
        :returns Profile: The new Profile object.
        """
        if profile_name not in raw_profiles:
            raise DbtProjectError(
                "Could not find profile named '{}'".format(profile_name)
            )

        # First, we've already got our final decision on profile name, and we
        # don't render keys, so we can pluck that out
        raw_profile = raw_profiles[profile_name]

        user_cfg = raw_profiles.get('config')

        return cls.from_raw_profile_info(
            raw_profile=raw_profile,
            profile_name=profile_name,
            cli_vars=cli_vars,
            user_cfg=user_cfg,
            target_override=target_override,
            threads_override=threads_override,
        )

    @classmethod
    def from_args(cls, args, project_profile_name=None, cli_vars=None):
        """Given the raw profiles as read from disk and the name of the desired
        profile if specified, return the profile component of the runtime
        config.

        :param args argparse.Namespace: The arguments as parsed from the cli.
        :param cli_vars dict: The command-line variables passed as arguments,
            as a dict.
        :param project_profile_name Optional[str]: The profile name, if
            specified in a project.
        :raises DbtProjectError: If there is no profile name specified in the
            project or the command line arguments, or if the specified profile
            is not found
        :raises DbtProfileError: If the profile is invalid or missing, or the
            target could not be found.
        :returns Profile: The new Profile object.
        """
        if cli_vars is None:
            cli_vars = dbt.utils.parse_cli_vars(getattr(args, 'vars', '{}'))

        threads_override = getattr(args, 'threads', None)
        # TODO(jeb): is it even possible for this to not be set?
        profiles_dir = getattr(args, 'profiles_dir', PROFILES_DIR)
        target_override = getattr(args, 'target', None)
        raw_profiles = read_profile(profiles_dir)
        profile_name = cls._pick_profile_name(args.profile,
                                              project_profile_name)

        return cls.from_raw_profiles(
            raw_profiles=raw_profiles,
            profile_name=profile_name,
            cli_vars=cli_vars,
            target_override=target_override,
            threads_override=threads_override
        )


def package_config_from_data(packages_data):
    if packages_data is None:
        packages_data = {'packages': []}

    try:
        packages = PackageConfig(**packages_data)
    except dbt.exceptions.ValidationException as e:
        raise DbtProjectError('Invalid package config: {}'.format(str(e)))
    return packages


def package_data_from_root(project_root):
    package_filepath = dbt.clients.system.resolve_path_from_base(
        'packages.yml', project_root
    )

    if dbt.clients.system.path_exists(package_filepath):
        packages_dict = _load_yaml(package_filepath)
    else:
        packages_dict = None
    return packages_dict


def package_config_from_root(project_root):
    packages_dict = package_data_from_root(project_root)
    return package_config_from_data(packages_dict)


class RuntimeConfig(Project, Profile):
    """The runtime configuration, as constructed from its components. There's a
    lot because there is a lot of stuff!
    """
    def __init__(self, project_name, version, project_root, source_paths,
                 macro_paths, data_paths, test_paths, analysis_paths,
                 docs_paths, target_path, clean_targets, log_path,
                 modules_path, quoting, models, on_run_start, on_run_end,
                 archive, seeds, profile_name, target_name,
                 send_anonymous_usage_stats, use_colors, threads, credentials,
                 packages, args):
        # 'vars'
        self.args = args
        self.cli_vars = dbt.utils.parse_cli_vars(getattr(args, 'vars', '{}'))
        # 'project'
        Project.__init__(
            self,
            project_name=project_name,
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
            clean_targets=clean_targets,
            log_path=log_path,
            modules_path=modules_path,
            quoting=quoting,
            models=models,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            archive=archive,
            seeds=seeds,
            packages=packages,
        )
        # 'profile'
        Profile.__init__(
            self,
            profile_name=profile_name,
            target_name=target_name,
            send_anonymous_usage_stats=send_anonymous_usage_stats,
            use_colors=use_colors,
            threads=threads,
            credentials=credentials
        )
        self.validate()

    @classmethod
    def from_parts(cls, project, profile, args):
        """Instantiate a RuntimeConfig from its components.

        :param profile Profile: A parsed dbt Profile.
        :param project Project: A parsed dbt Project.
        :param args argparse.Namespace: The parsed command-line arguments.
        :returns RuntimeConfig: The new configuration.
        """
        quoting = deepcopy(
            get_relation_class_by_name(profile.credentials.type)
            .DEFAULTS['quote_policy']
        )
        quoting.update(project.quoting)
        return cls(
            project_name=project.project_name,
            version=project.version,
            project_root=project.project_root,
            source_paths=project.source_paths,
            macro_paths=project.macro_paths,
            data_paths=project.data_paths,
            test_paths=project.test_paths,
            analysis_paths=project.analysis_paths,
            docs_paths=project.docs_paths,
            target_path=project.target_path,
            clean_targets=project.clean_targets,
            log_path=project.log_path,
            modules_path=project.modules_path,
            quoting=quoting,
            models=project.models,
            on_run_start=project.on_run_start,
            on_run_end=project.on_run_end,
            archive=project.archive,
            seeds=project.seeds,
            packages=project.packages,
            profile_name=profile.profile_name,
            target_name=profile.target_name,
            send_anonymous_usage_stats=profile.send_anonymous_usage_stats,
            use_colors=profile.use_colors,
            threads=profile.threads,
            credentials=profile.credentials,
            args=args
        )

    def new_project(self, project_root):
        """Given a new project root, read in its project dictionary, supply the
        existing project's profile info, and create a new project file.

        :param project_root str: A filepath to a dbt project.
        :raises DbtProfileError: If the profile is invalid.
        :raises DbtProjectError: If project is missing or invalid.
        :returns RuntimeConfig: The new configuration.
        """
        # copy profile
        profile = Profile(**self.to_profile_info())
        profile.validate()
        # load the new project and its packages. Don't pass cli variables.
        project = Project.from_project_root(project_root, {})

        cfg = self.from_parts(
            project=project,
            profile=profile,
            args=deepcopy(self.args),
        )
        # force our quoting back onto the new project.
        cfg.quoting = deepcopy(self.quoting)
        return cfg

    def serialize(self):
        """Serialize the full configuration to a single dictionary. For any
        instance that has passed validate() (which happens in __init__), it
        matches the Configuration contract.

        Note that args are not serialized.

        :returns dict: The serialized configuration.
        """
        result = self.to_project_config(with_packages=True)
        result.update(self.to_profile_info(serialize_credentials=True))
        result['cli_vars'] = deepcopy(self.cli_vars)
        return result

    def __str__(self):
        return pprint.pformat(self.serialize())

    def validate(self):
        """Validate the configuration against its contract.

        :raises DbtProjectError: If the configuration fails validation.
        """
        try:
            Configuration(**self.serialize())
        except dbt.exceptions.ValidationException as e:
            raise DbtProjectError(str(e))

    @classmethod
    def from_args(cls, args):
        """Given arguments, read in dbt_project.yml from the current directory,
        read in packages.yml if it exists, and use them to find the profile to
        load.

        :param args argparse.Namespace: The arguments as parsed from the cli.
        :raises DbtProjectError: If the project is invalid or missing.
        :raises DbtProfileError: If the profile is invalid or missing.
        :raises ValidationException: If the cli variables are invalid.
        """
        cli_vars = dbt.utils.parse_cli_vars(getattr(args, 'vars', '{}'))

        # build the project and read in packages.yml
        project = Project.from_current_directory(cli_vars)

        # build the profile
        profile = Profile.from_args(
            args=args,
            project_profile_name=project.profile_name,
            cli_vars=cli_vars
        )

        return cls.from_parts(
            project=project,
            profile=profile,
            args=args
        )


def _load_yaml(path):
    contents = dbt.clients.system.load_file_contents(path)
    return dbt.clients.yaml_helper.load_yaml_text(contents)


def _get_config_paths(config, path=(), paths=None):
    if paths is None:
        paths = set()

    for key, value in config.items():
        if isinstance(value, dict):
            if key in DBTConfigKeys:
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
