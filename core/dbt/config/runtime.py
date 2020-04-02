from copy import deepcopy
from dataclasses import dataclass, fields
import os
from typing import Dict, Any, Type

from .profile import Profile
from .project import Project
from .renderer import ConfigRenderer
from dbt import tracking
from dbt.adapters.factory import get_relation_class_by_name
from dbt.context.base import generate_base_context
from dbt.context.target import generate_target_context
from dbt.contracts.connection import AdapterRequiredConfig, Credentials
from dbt.contracts.graph.manifest import ManifestMetadata
from dbt.contracts.project import Configuration, UserConfig
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import DbtProjectError, RuntimeException, DbtProfileError
from dbt.exceptions import validator_error_message
from dbt.utils import parse_cli_vars

from hologram import ValidationError


@dataclass
class RuntimeConfig(Project, Profile, AdapterRequiredConfig):
    args: Any
    cli_vars: Dict[str, Any]

    def __post_init__(self):
        self.validate()

    @classmethod
    def from_parts(
        cls, project: Project, profile: Profile, args: Any,
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
            .replace_dict(project.quoting)
        ).to_dict()

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
            target_path=project.target_path,
            snapshot_paths=project.snapshot_paths,
            clean_targets=project.clean_targets,
            log_path=project.log_path,
            modules_path=project.modules_path,
            quoting=quoting,
            models=project.models,
            on_run_start=project.on_run_start,
            on_run_end=project.on_run_end,
            seeds=project.seeds,
            snapshots=project.snapshots,
            dbt_version=project.dbt_version,
            packages=project.packages,
            query_comment=project.query_comment,
            profile_name=profile.profile_name,
            target_name=profile.target_name,
            config=profile.config,
            threads=profile.threads,
            credentials=profile.credentials,
            args=args,
            cli_vars=cli_vars,
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
        renderer = ConfigRenderer(generate_target_context(profile, {}))

        project = Project.from_project_root(project_root, renderer)

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
            Configuration.from_dict(self.serialize())
        except ValidationError as e:
            raise DbtProjectError(validator_error_message(e)) from e

        if getattr(self.args, 'version_check', False):
            self.validate_version()

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
        # profile_name from the project
        partial = Project.partial_load(os.getcwd())

        # build the profile using the base renderer and the one fact we know
        cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, 'vars', '{}'))
        renderer = ConfigRenderer(generate_base_context(cli_vars=cli_vars))
        profile_name = partial.render_profile_name(renderer)
        profile = Profile.render_from_args(
            args, renderer, profile_name
        )

        # get a new renderer using our target information and render the
        # project
        renderer = ConfigRenderer(generate_target_context(profile, cli_vars))
        project = partial.render(renderer)

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


class UnsetCredentials(Credentials):
    def __init__(self):
        super().__init__('', '')

    @property
    def type(self):
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

    def to_dict(self):
        return {}


class UnsetProfile(Profile):
    def __init__(self):
        self.credentials = UnsetCredentials()
        self.config = UnsetConfig()
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
        cls, project: Project, profile: Any, args: Any,
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
            target_path=project.target_path,
            snapshot_paths=project.snapshot_paths,
            clean_targets=project.clean_targets,
            log_path=project.log_path,
            modules_path=project.modules_path,
            quoting=project.quoting,  # we never use this anyway.
            models=project.models,
            on_run_start=project.on_run_start,
            on_run_end=project.on_run_end,
            seeds=project.seeds,
            snapshots=project.snapshots,
            dbt_version=project.dbt_version,
            packages=project.packages,
            query_comment=project.query_comment,
            profile_name='',
            target_name='',
            config=UnsetConfig(),
            threads=getattr(args, 'threads', 1),
            credentials=UnsetCredentials(),
            args=args,
            cli_vars=cli_vars,
        )

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
        # profile_name from the project
        partial = Project.partial_load(os.getcwd())

        # build the profile using the base renderer and the one fact we know
        cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, 'vars', '{}'))
        renderer = ConfigRenderer(generate_base_context(cli_vars=cli_vars))
        profile_name = partial.render_profile_name(renderer)

        try:
            profile = Profile.render_from_args(
                args, renderer, profile_name
            )
            cls = RuntimeConfig  # we can return a real runtime config, do that
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
            tracking.do_not_track()

        # get a new renderer using our target information and render the
        # project
        renderer = ConfigRenderer(generate_target_context(profile, cli_vars))
        project = partial.render(renderer)

        return cls.from_parts(
            project=project,
            profile=profile,
            args=args,
        )
