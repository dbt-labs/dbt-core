import itertools
import os
from copy import deepcopy
from dataclasses import dataclass, fields
from pathlib import Path
from typing import (
    Dict, Any, Optional, Mapping, Iterator, Iterable, Tuple, List, MutableSet,
    Type
)

from .profile import Profile
from .project import Project
from .renderer import DbtProjectYamlRenderer, ProfileRenderer
from .utils import parse_cli_vars
from dbt import flags
from dbt import tracking
from dbt.adapters.factory import get_relation_class_by_name, get_include_paths
from dbt.helper_types import FQNPath, PathSet
from dbt.context.base import generate_base_context
from dbt.context.target import generate_target_context
from dbt.contracts.connection import AdapterRequiredConfig, Credentials
from dbt.contracts.graph.manifest import ManifestMetadata
from dbt.contracts.relation import ComponentName
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.ui import warning_tag

from dbt.contracts.project import Configuration, UserConfig
from dbt.exceptions import (
    RuntimeException,
    DbtProfileError,
    DbtProjectError,
    validator_error_message,
    warn_or_error,
    raise_compiler_error
)

from dbt.dataclass_schema import ValidationError


def _project_quoting_dict(
    proj: Project, profile: Profile
) -> Dict[ComponentName, bool]:
    src: Dict[str, Any] = profile.credentials.translate_aliases(proj.quoting)
    result: Dict[ComponentName, bool] = {}
    for key in ComponentName:
        if key in src:
            value = src[key]
            if isinstance(value, bool):
                result[key] = value
    return result


@dataclass
class RuntimeConfig(Project, Profile, AdapterRequiredConfig):
    args: Any
    profile_name: str
    cli_vars: Dict[str, Any]
    dependencies: Optional[Mapping[str, 'RuntimeConfig']] = None

    def __post_init__(self):
        self.validate()

    @classmethod
    def from_parts(
        cls,
        project: Project,
        profile: Profile,
        args: Any,
        dependencies: Optional[Mapping[str, 'RuntimeConfig']] = None,
    ) -> 'RuntimeConfig':
        """Instantiate a RuntimeConfig from its components.

        :param profile: A parsed dbt Profile.
        :param project: A parsed dbt Project.
        :param args: The parsed command-line arguments.
        :returns RuntimeConfig: The new configuration.
        """
        quoting: Dict[str, Any] = (
            get_relation_class_by_name(profile.credentials.type)
            .get_default_quote_policy()
            .replace_dict(_project_quoting_dict(project, profile))
        ).to_dict(omit_none=True)

        cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, 'vars', '{}'))

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
            asset_paths=project.asset_paths,
            target_path=project.target_path,
            snapshot_paths=project.snapshot_paths,
            clean_targets=project.clean_targets,
            log_path=project.log_path,
            modules_path=project.modules_path,
            quoting=quoting,
            models=project.models,
            on_run_start=project.on_run_start,
            on_run_end=project.on_run_end,
            dispatch=project.dispatch,
            seeds=project.seeds,
            snapshots=project.snapshots,
            dbt_version=project.dbt_version,
            packages=project.packages,
            manifest_selectors=project.manifest_selectors,
            selectors=project.selectors,
            query_comment=project.query_comment,
            sources=project.sources,
            tests=project.tests,
            vars=project.vars,
            config_version=project.config_version,
            unrendered=project.unrendered,
            profile_name=profile.profile_name,
            target_name=profile.target_name,
            user_config=profile.user_config,
            threads=profile.threads,
            credentials=profile.credentials,
            args=args,
            cli_vars=cli_vars,
            dependencies=dependencies,
        )

    def new_project(self, project_root: str) -> 'RuntimeConfig':
        """Given a new project root, read in its project dictionary, supply the
        existing project's profile info, and create a new project file.

        :param project_root: A filepath to a dbt project.
        :raises DbtProfileError: If the profile is invalid.
        :raises DbtProjectError: If project is missing or invalid.
        :returns: The new configuration.
        """
        # copy profile
        profile = Profile(**self.to_profile_info())
        profile.validate()

        # load the new project and its packages. Don't pass cli variables.
        renderer = DbtProjectYamlRenderer(generate_target_context(profile, {}))

        project = Project.from_project_root(
            project_root,
            renderer,
            verify_version=bool(flags.VERSION_CHECK),
        )

        cfg = self.from_parts(
            project=project,
            profile=profile,
            args=deepcopy(self.args),
        )
        # force our quoting back onto the new project.
        cfg.quoting = deepcopy(self.quoting)
        return cfg

    def serialize(self) -> Dict[str, Any]:
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

    def validate(self):
        """Validate the configuration against its contract.

        :raises DbtProjectError: If the configuration fails validation.
        """
        try:
            Configuration.validate(self.serialize())
        except ValidationError as e:
            raise DbtProjectError(validator_error_message(e)) from e

    @classmethod
    def _get_rendered_profile(
        cls,
        args: Any,
        profile_renderer: ProfileRenderer,
        profile_name: Optional[str],
    ) -> Profile:
        return Profile.render_from_args(
            args, profile_renderer, profile_name
        )

    @classmethod
    def collect_parts(
        cls: Type['RuntimeConfig'], args: Any
    ) -> Tuple[Project, Profile]:
        # profile_name from the project
        project_root = args.project_dir if args.project_dir else os.getcwd()
        version_check = bool(flags.VERSION_CHECK)
        partial = Project.partial_load(
            project_root,
            verify_version=version_check
        )

        # build the profile using the base renderer and the one fact we know
        cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, 'vars', '{}'))
        profile_renderer = ProfileRenderer(generate_base_context(cli_vars))
        profile_name = partial.render_profile_name(profile_renderer)

        profile = cls._get_rendered_profile(
            args, profile_renderer, profile_name
        )

        # get a new renderer using our target information and render the
        # project
        ctx = generate_target_context(profile, cli_vars)
        project_renderer = DbtProjectYamlRenderer(ctx)
        project = partial.render(project_renderer)
        return (project, profile)

    @classmethod
    def from_args(cls, args: Any) -> 'RuntimeConfig':
        """Given arguments, read in dbt_project.yml from the current directory,
        read in packages.yml if it exists, and use them to find the profile to
        load.

        :param args: The arguments as parsed from the cli.
        :raises DbtProjectError: If the project is invalid or missing.
        :raises DbtProfileError: If the profile is invalid or missing.
        :raises ValidationException: If the cli variables are invalid.
        """
        project, profile = cls.collect_parts(args)

        return cls.from_parts(
            project=project,
            profile=profile,
            args=args,
        )

    def get_metadata(self) -> ManifestMetadata:
        return ManifestMetadata(
            project_id=self.hashed_name(),
            adapter_type=self.credentials.type
        )

    def _get_v2_config_paths(
        self,
        config,
        path: FQNPath,
        paths: MutableSet[FQNPath],
    ) -> PathSet:
        for key, value in config.items():
            if isinstance(value, dict) and not key.startswith('+'):
                self._get_v2_config_paths(value, path + (key,), paths)
            else:
                paths.add(path)
        return frozenset(paths)

    def _get_config_paths(
        self,
        config: Dict[str, Any],
        path: FQNPath = (),
        paths: Optional[MutableSet[FQNPath]] = None,
    ) -> PathSet:
        if paths is None:
            paths = set()

        for key, value in config.items():
            if isinstance(value, dict) and not key.startswith('+'):
                self._get_v2_config_paths(value, path + (key,), paths)
            else:
                paths.add(path)
        return frozenset(paths)

    def get_resource_config_paths(self) -> Dict[str, PathSet]:
        """Return a dictionary with resource type keys whose values are
        lists of lists of strings, where each inner list of strings represents
        a configured path in the resource.
        """
        return {
            'models': self._get_config_paths(self.models),
            'seeds': self._get_config_paths(self.seeds),
            'snapshots': self._get_config_paths(self.snapshots),
            'sources': self._get_config_paths(self.sources),
            'tests': self._get_config_paths(self.tests),
        }

    def get_unused_resource_config_paths(
        self,
        resource_fqns: Mapping[str, PathSet],
        disabled: PathSet,
    ) -> List[FQNPath]:
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

    def warn_for_unused_resource_config_paths(
        self,
        resource_fqns: Mapping[str, PathSet],
        disabled: PathSet,
    ) -> None:
        unused = self.get_unused_resource_config_paths(resource_fqns, disabled)
        if len(unused) == 0:
            return

        msg = UNUSED_RESOURCE_CONFIGURATION_PATH_MESSAGE.format(
            len(unused),
            '\n'.join('- {}'.format('.'.join(u)) for u in unused)
        )

        warn_or_error(msg, log_fmt=warning_tag('{}'))

    def load_dependencies(self) -> Mapping[str, 'RuntimeConfig']:
        if self.dependencies is None:
            all_projects = {self.project_name: self}
            internal_packages = get_include_paths(self.credentials.type)
            # raise exception if fewer installed packages than in packages.yml
            count_packages_specified = len(self.packages.packages)  # type: ignore
            count_packages_installed = len(tuple(self._get_project_directories()))
            if count_packages_specified > count_packages_installed:
                raise_compiler_error(
                    f'dbt found {count_packages_specified} package(s) '
                    f'specified in packages.yml, but only '
                    f'{count_packages_installed} package(s) installed '
                    f'in {self.modules_path}. Run "dbt deps" to '
                    f'install package dependencies.'
                )
            project_paths = itertools.chain(
                internal_packages,
                self._get_project_directories()
            )
            for project_name, project in self.load_projects(project_paths):
                if project_name in all_projects:
                    raise_compiler_error(
                        f'dbt found more than one package with the name '
                        f'"{project_name}" included in this project. Package '
                        f'names must be unique in a project. Please rename '
                        f'one of these packages.'
                    )
                all_projects[project_name] = project
            self.dependencies = all_projects
        return self.dependencies

    def clear_dependencies(self):
        self.dependencies = None

    def load_projects(
        self, paths: Iterable[Path]
    ) -> Iterator[Tuple[str, 'RuntimeConfig']]:
        for path in paths:
            try:
                project = self.new_project(str(path))
            except DbtProjectError as e:
                raise DbtProjectError(
                    f'Failed to read package: {e}',
                    result_type='invalid_project',
                    path=path,
                ) from e
            else:
                yield project.project_name, project

    def _get_project_directories(self) -> Iterator[Path]:
        root = Path(self.project_root) / self.modules_path

        if root.exists():
            for path in root.iterdir():
                if path.is_dir() and not path.name.startswith('__'):
                    yield path


class UnsetCredentials(Credentials):
    def __init__(self):
        super().__init__('', '')

    @property
    def type(self):
        return None

    @property
    def unique_field(self):
        return None

    def connection_info(self, *args, **kwargs):
        return {}

    def _connection_keys(self):
        return ()


class UnsetConfig(UserConfig):
    def __getattribute__(self, name):
        if name in {f.name for f in fields(UserConfig)}:
            raise AttributeError(
                f"'UnsetConfig' object has no attribute {name}"
            )

    def __post_serialize__(self, dct):
        return {}


class UnsetProfile(Profile):
    def __init__(self):
        self.credentials = UnsetCredentials()
        self.user_config = UnsetConfig()
        self.profile_name = ''
        self.target_name = ''
        self.threads = -1

    def to_target_dict(self):
        return {}

    def __getattribute__(self, name):
        if name in {'profile_name', 'target_name', 'threads'}:
            raise RuntimeException(
                f'Error: disallowed attribute "{name}" - no profile!'
            )

        return Profile.__getattribute__(self, name)


@dataclass
class UnsetProfileConfig(RuntimeConfig):
    """This class acts a lot _like_ a RuntimeConfig, except if your profile is
    missing, any access to profile members results in an exception.
    """

    def __post_init__(self):
        # instead of futzing with InitVar overrides or rewriting __init__, just
        # `del` the attrs we don't want  users touching.
        del self.profile_name
        del self.target_name
        # don't call super().__post_init__(), as that calls validate(), and
        # this object isn't very valid

    def __getattribute__(self, name):
        # Override __getattribute__ to check that the attribute isn't 'banned'.
        if name in {'profile_name', 'target_name'}:
            raise RuntimeException(
                f'Error: disallowed attribute "{name}" - no profile!'
            )

        # avoid every attribute access triggering infinite recursion
        return RuntimeConfig.__getattribute__(self, name)

    def to_target_dict(self):
        # re-override the poisoned profile behavior
        return {}

    @classmethod
    def from_parts(
        cls,
        project: Project,
        profile: Profile,
        args: Any,
        dependencies: Optional[Mapping[str, 'RuntimeConfig']] = None,
    ) -> 'RuntimeConfig':
        """Instantiate a RuntimeConfig from its components.

        :param profile: Ignored.
        :param project: A parsed dbt Project.
        :param args: The parsed command-line arguments.
        :returns RuntimeConfig: The new configuration.
        """
        cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, 'vars', '{}'))

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
            asset_paths=project.asset_paths,
            target_path=project.target_path,
            snapshot_paths=project.snapshot_paths,
            clean_targets=project.clean_targets,
            log_path=project.log_path,
            modules_path=project.modules_path,
            quoting=project.quoting,  # we never use this anyway.
            models=project.models,
            on_run_start=project.on_run_start,
            on_run_end=project.on_run_end,
            dispatch=project.dispatch,
            seeds=project.seeds,
            snapshots=project.snapshots,
            dbt_version=project.dbt_version,
            packages=project.packages,
            manifest_selectors=project.manifest_selectors,
            selectors=project.selectors,
            query_comment=project.query_comment,
            sources=project.sources,
            tests=project.tests,
            vars=project.vars,
            config_version=project.config_version,
            unrendered=project.unrendered,
            profile_name='',
            target_name='',
            user_config=UnsetConfig(),
            threads=getattr(args, 'threads', 1),
            credentials=UnsetCredentials(),
            args=args,
            cli_vars=cli_vars,
            dependencies=dependencies,
        )

    @classmethod
    def _get_rendered_profile(
        cls,
        args: Any,
        profile_renderer: ProfileRenderer,
        profile_name: Optional[str],
    ) -> Profile:
        try:
            profile = Profile.render_from_args(
                args, profile_renderer, profile_name
            )
        except (DbtProjectError, DbtProfileError) as exc:
            logger.debug(
                'Profile not loaded due to error: {}', exc, exc_info=True
            )
            logger.info(
                'No profile "{}" found, continuing with no target',
                profile_name
            )
            # return the poisoned form
            profile = UnsetProfile()
            # disable anonymous usage statistics
            tracking.disable_tracking()
        return profile

    @classmethod
    def from_args(cls: Type[RuntimeConfig], args: Any) -> 'RuntimeConfig':
        """Given arguments, read in dbt_project.yml from the current directory,
        read in packages.yml if it exists, and use them to find the profile to
        load.

        :param args: The arguments as parsed from the cli.
        :raises DbtProjectError: If the project is invalid or missing.
        :raises DbtProfileError: If the profile is invalid or missing.
        :raises ValidationException: If the cli variables are invalid.
        """
        project, profile = cls.collect_parts(args)
        if not isinstance(profile, UnsetProfile):
            # if it's a real profile, return a real config
            cls = RuntimeConfig

        return cls.from_parts(
            project=project,
            profile=profile,
            args=args
        )


UNUSED_RESOURCE_CONFIGURATION_PATH_MESSAGE = """\
Configuration paths exist in your dbt_project.yml file which do not \
apply to any resources.
There are {} unused configuration paths:
{}
"""


def _is_config_used(path, fqns):
    if fqns:
        for fqn in fqns:
            if len(path) <= len(fqn) and fqn[:len(path)] == path:
                return True
    return False
