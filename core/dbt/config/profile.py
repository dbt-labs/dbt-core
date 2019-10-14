import os

from hologram import ValidationError

from dbt.clients.system import load_file_contents
from dbt.clients.yaml_helper import load_yaml_text
from dbt.contracts.project import ProfileConfig, UserConfig
from dbt.exceptions import DbtProfileError
from dbt.exceptions import DbtProjectError
from dbt.exceptions import ValidationException
from dbt.exceptions import RuntimeException
from dbt.exceptions import validator_error_message
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import parse_cli_vars

from .renderer import ConfigRenderer

DEFAULT_THREADS = 1
DEFAULT_PROFILES_DIR = os.path.join(os.path.expanduser('~'), '.dbt')
PROFILES_DIR = os.path.expanduser(
    os.getenv('DBT_PROFILES_DIR', DEFAULT_PROFILES_DIR)
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


def read_profile(profiles_dir):
    path = os.path.join(profiles_dir, 'profiles.yml')

    contents = None
    if os.path.isfile(path):
        try:
            contents = load_file_contents(path, strip=False)
            return load_yaml_text(contents)
        except ValidationException as e:
            msg = INVALID_PROFILE_MESSAGE.format(error_string=e)
            raise ValidationException(msg) from e

    return {}


def read_user_config(directory):
    try:
        user_cfg = None
        profile = read_profile(directory)
        if profile:
            user_cfg = profile.get('config', {})
        return UserConfig.from_dict(user_cfg)
    except (RuntimeException, ValidationError):
        return UserConfig()


class Profile:
    def __init__(self, profile_name, target_name, config, threads,
                 credentials):
        self.profile_name = profile_name
        self.target_name = target_name
        if isinstance(config, dict):
            config = UserConfig.from_dict(config)
        self.config = config
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
            'config': self.config.to_dict(),
            'threads': self.threads,
            'credentials': self.credentials,
        }
        if serialize_credentials:
            result['credentials'] = result['credentials'].to_dict()
        return result

    def __str__(self):
        return str(self.to_profile_info())

    def __eq__(self, other):
        if not (isinstance(other, self.__class__) and
                isinstance(self, other.__class__)):
            return False
        return self.to_profile_info() == other.to_profile_info()

    def validate(self):
        try:
            if self.credentials:
                self.credentials.to_dict(validate=True)
            ProfileConfig.from_dict(
                self.to_profile_info(serialize_credentials=True)
            )
        except ValidationError as exc:
            raise DbtProfileError(validator_error_message(exc)) from exc

    @staticmethod
    def _credentials_from_profile(profile, profile_name, target_name):
        # avoid an import cycle
        from dbt.adapters.factory import load_plugin
        # credentials carry their 'type' in their actual type, not their
        # attributes. We do want this in order to pick our Credentials class.
        if 'type' not in profile:
            raise DbtProfileError(
                'required field "type" not found in profile {} and target {}'
                .format(profile_name, target_name))

        typename = profile.pop('type')
        try:
            cls = load_plugin(typename)
            credentials = cls.from_dict(profile)
        except (RuntimeException, ValidationError) as e:
            msg = str(e) if isinstance(e, RuntimeException) else e.message
            raise DbtProfileError(
                'Credentials in profile "{}", target "{}" invalid: {}'
                .format(profile_name, target_name, msg)
            ) from e

        return credentials

    @staticmethod
    def pick_profile_name(args_profile_name, project_profile_name=None):
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

        :param credentials dict: The credentials dict for this profile.
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
        config = UserConfig.from_dict(user_cfg)

        profile = cls(
            profile_name=profile_name,
            target_name=target_name,
            config=config,
            threads=threads,
            credentials=credentials
        )
        profile.validate()
        return profile

    @classmethod
    def render_profile(cls, raw_profile, profile_name, target_override,
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
            target_name = 'default'
            logger.debug(
                "target not specified in profile '{}', using '{}'"
                .format(profile_name, target_name)
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
        target_name, profile_data = cls.render_profile(
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
    def from_args(cls, args, project_profile_name=None):
        """Given the raw profiles as read from disk and the name of the desired
        profile if specified, return the profile component of the runtime
        config.

        :param args argparse.Namespace: The arguments as parsed from the cli.
        :param project_profile_name Optional[str]: The profile name, if
            specified in a project.
        :raises DbtProjectError: If there is no profile name specified in the
            project or the command line arguments, or if the specified profile
            is not found
        :raises DbtProfileError: If the profile is invalid or missing, or the
            target could not be found.
        :returns Profile: The new Profile object.
        """
        cli_vars = parse_cli_vars(getattr(args, 'vars', '{}'))
        threads_override = getattr(args, 'threads', None)
        target_override = getattr(args, 'target', None)
        raw_profiles = read_profile(args.profiles_dir)
        profile_name = cls.pick_profile_name(args.profile,
                                             project_profile_name)

        return cls.from_raw_profiles(
            raw_profiles=raw_profiles,
            profile_name=profile_name,
            cli_vars=cli_vars,
            target_override=target_override,
            threads_override=threads_override
        )
