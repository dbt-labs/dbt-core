# coding=utf-8
import os
import platform
import sys
from typing import Optional, Dict, Any, List

from dbt.events.functions import fire_event
from dbt.events.types import OpenCommand
from dbt import flags
import dbt.clients.system
import dbt.exceptions
from dbt.adapters.factory import get_adapter, register_adapter
from dbt.config import Project, Profile
from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
from dbt.config.utils import parse_cli_vars
from dbt.clients.yaml_helper import load_yaml_text
from dbt.links import ProfileConfigDocs
from dbt.ui import green, red
from dbt.events.format import pluralize
from dbt.version import get_installed_version

from dbt.task.base import BaseTask, get_nearest_project_dir

PROFILE_DIR_MESSAGE = """To view your profiles.yml file, run:

{open_cmd} {profiles_dir}"""

ONLY_PROFILE_MESSAGE = """
A `dbt_project.yml` file was not found in this directory.
Using the only profile `{}`.
""".lstrip()

MULTIPLE_PROFILE_MESSAGE = """
A `dbt_project.yml` file was not found in this directory.
dbt found the following profiles:
{}

To debug one of these profiles, run:
dbt debug --profile [profile-name]
""".lstrip()

COULD_NOT_CONNECT_MESSAGE = """
dbt was unable to connect to the specified database.
The database returned the following error:

  >{err}

Check your database credentials and try again. For more information, visit:
{url}
""".lstrip()

MISSING_PROFILE_MESSAGE = """
dbt looked for a profiles.yml file in {path}, but did
not find one. For more information on configuring your profile, consult the
documentation:

{url}
""".lstrip()

FILE_NOT_FOUND = "file not found"


class QueryCommentedProfile(Profile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_comment = None


class DebugTask(BaseTask):
    def __init__(self, args, config):
        super().__init__(args, config)
        self.profiles_dir = flags.PROFILES_DIR
        self.profile_path = os.path.join(self.profiles_dir, "profiles.yml")
        try:
            self.project_dir = get_nearest_project_dir(self.args)
        except dbt.exceptions.Exception:
            # we probably couldn't find a project directory. Set project dir
            # to whatever was given, or default to the current directory.
            if args.project_dir:
                self.project_dir = args.project_dir
            else:
                self.project_dir = os.getcwd()
        self.project_path = os.path.join(self.project_dir, "dbt_project.yml")
        self.cli_vars = parse_cli_vars(getattr(self.args, "vars", "{}"))

        # set by _load_*
        self.profile: Optional[Profile] = None
        self.profile_fail_details = ""
        self.raw_profile_data: Optional[Dict[str, Any]] = None
        self.profile_name: Optional[str] = None
        self.project: Optional[Project] = None
        self.project_fail_details = ""
        self.any_failure = False
        self.messages: List[str] = []

    @property
    def project_profile(self):
        if self.project is None:
            return None
        return self.project.profile_name

    def path_info(self):
        open_cmd = dbt.clients.system.open_dir_cmd()
        fire_event(OpenCommand(open_cmd=open_cmd, profiles_dir=self.profiles_dir))

    def run(self):
        if self.args.config_dir:
            self.path_info()
            return not self.any_failure

        version = get_installed_version().to_version_string(skip_matcher=True)
        print("dbt version: {}".format(version))
        print("python version: {}".format(sys.version.split()[0]))
        print("python path: {}".format(sys.executable))
        print("os info: {}".format(platform.platform()))
        print("Using profiles.yml file at {}".format(self.profile_path))
        print("Using dbt_project.yml file at {}".format(self.project_path))
        print("")
        self.test_configuration()
        self.test_dependencies()
        self.test_connection()

        if self.any_failure:
            print(red(f"{(pluralize(len(self.messages), 'check'))} failed:"))
        else:
            print(green("All checks passed!"))

        for message in self.messages:
            print(message)
            print("")

        return not self.any_failure

    def interpret_results(self, results):
        return results

    def _load_project(self):
        if not os.path.exists(self.project_path):
            self.project_fail_details = FILE_NOT_FOUND
            return red("ERROR not found")

        renderer = DbtProjectYamlRenderer(self.profile, self.cli_vars)

        try:
            self.project = Project.from_project_root(
                self.project_dir,
                renderer,
                verify_version=flags.VERSION_CHECK,
            )
        except dbt.exceptions.DbtConfigError as exc:
            self.project_fail_details = str(exc)
            return red("ERROR invalid")

        return green("OK found and valid")

    def _profile_found(self):
        if not self.raw_profile_data:
            return red("ERROR not found")
        assert self.raw_profile_data is not None
        if self.profile_name in self.raw_profile_data:
            return green("OK found")
        else:
            return red("ERROR not found")

    def _target_found(self):
        requirements = self.raw_profile_data and self.profile_name and self.target_name
        if not requirements:
            return red("ERROR not found")
        # mypy appeasement, we checked just above
        assert self.raw_profile_data is not None
        assert self.profile_name is not None
        assert self.target_name is not None
        if self.profile_name not in self.raw_profile_data:
            return red("ERROR not found")
        profiles = self.raw_profile_data[self.profile_name]["outputs"]
        if self.target_name not in profiles:
            return red("ERROR not found")
        return green("OK found")

    def _choose_profile_names(self) -> Optional[List[str]]:
        project_profile: Optional[str] = None
        if os.path.exists(self.project_path):
            try:
                partial = Project.partial_load(
                    os.path.dirname(self.project_path),
                    verify_version=bool(flags.VERSION_CHECK),
                )
                renderer = DbtProjectYamlRenderer(None, self.cli_vars)
                project_profile = partial.render_profile_name(renderer)
            except dbt.exceptions.DbtProjectError:
                pass

        args_profile: Optional[str] = getattr(self.args, "profile", None)

        try:
            return [Profile.pick_profile_name(args_profile, project_profile)]
        except dbt.exceptions.DbtConfigError:
            pass
        # try to guess

        profiles = []
        if self.raw_profile_data:
            profiles = [k for k in self.raw_profile_data if k != "config"]
            if project_profile is None:
                self.messages.append("Could not load dbt_project.yml")
            elif len(profiles) == 0:
                self.messages.append("The profiles.yml has no profiles")
            elif len(profiles) == 1:
                self.messages.append(ONLY_PROFILE_MESSAGE.format(profiles[0]))
            else:
                self.messages.append(
                    MULTIPLE_PROFILE_MESSAGE.format("\n".join(" - {}".format(o) for o in profiles))
                )
        return profiles

    def _choose_target_name(self, profile_name: str):
        has_raw_profile = (
            self.raw_profile_data is not None and profile_name in self.raw_profile_data
        )

        if not has_raw_profile:
            return None

        # mypy appeasement, we checked just above
        assert self.raw_profile_data is not None
        raw_profile = self.raw_profile_data[profile_name]

        renderer = ProfileRenderer(self.cli_vars)

        target_name, _ = Profile.render_profile(
            raw_profile=raw_profile,
            profile_name=profile_name,
            target_override=getattr(self.args, "target", None),
            renderer=renderer,
        )
        return target_name

    def _load_profile(self):
        if not os.path.exists(self.profile_path):
            self.profile_fail_details = FILE_NOT_FOUND
            self.messages.append(
                MISSING_PROFILE_MESSAGE.format(path=self.profile_path, url=ProfileConfigDocs)
            )
            self.any_failure = True
            return red("ERROR not found")

        try:
            raw_profile_data = load_yaml_text(
                dbt.clients.system.load_file_contents(self.profile_path)
            )
        except Exception:
            pass  # we'll report this when we try to load the profile for real
        else:
            if isinstance(raw_profile_data, dict):
                self.raw_profile_data = raw_profile_data

        profile_errors = []
        profile_names = self._choose_profile_names()
        renderer = ProfileRenderer(self.cli_vars)
        for profile_name in profile_names:
            try:
                profile: Profile = QueryCommentedProfile.render_from_args(
                    self.args, renderer, profile_name
                )
            except dbt.exceptions.DbtConfigError as exc:
                profile_errors.append(str(exc))
            else:
                if len(profile_names) == 1:
                    # if a profile was specified, set it on the task
                    self.target_name = self._choose_target_name(profile_name)
                    self.profile = profile

        if profile_errors:
            self.profile_fail_details = "\n\n".join(profile_errors)
            return red("ERROR invalid")
        return green("OK found and valid")

    def test_git(self):
        try:
            dbt.clients.system.run_cmd(os.getcwd(), ["git", "--help"])
        except dbt.exceptions.ExecutableError as exc:
            self.messages.append("Error from git --help: {!s}".format(exc))
            self.any_failure = True
            return red("ERROR")
        return green("OK found")

    def test_dependencies(self):
        print("Required dependencies:")
        print(" - git [{}]".format(self.test_git()))
        print("")

    def test_configuration(self):
        profile_status = self._load_profile()
        project_status = self._load_project()
        print("Configuration:")
        print("  profiles.yml file [{}]".format(profile_status))
        print("  dbt_project.yml file [{}]".format(project_status))
        # skip profile stuff if we can't find a profile name
        if self.profile_name is not None:
            print("  profile: {} [{}]".format(self.profile_name, self._profile_found()))
            print("  target: {} [{}]".format(self.target_name, self._target_found()))
        print("")
        self._log_project_fail()
        self._log_profile_fail()

    def _log_project_fail(self):
        if not self.project_fail_details:
            return

        self.any_failure = True
        if self.project_fail_details == FILE_NOT_FOUND:
            return
        msg = (
            f"Project loading failed for the following reason:"
            f"\n{self.project_fail_details}"
            f"\n"
        )
        self.messages.append(msg)

    def _log_profile_fail(self):
        if not self.profile_fail_details:
            return

        self.any_failure = True
        if self.profile_fail_details == FILE_NOT_FOUND:
            return
        msg = (
            f"Profile loading failed for the following reason:"
            f"\n{self.profile_fail_details}"
            f"\n"
        )
        self.messages.append(msg)

    @staticmethod
    def attempt_connection(profile):
        """Return a string containing the error message, or None if there was
        no error.
        """
        register_adapter(profile)
        adapter = get_adapter(profile)
        try:
            with adapter.connection_named("debug"):
                adapter.debug_query()
        except Exception as exc:
            return COULD_NOT_CONNECT_MESSAGE.format(
                err=str(exc),
                url=ProfileConfigDocs,
            )

        return None

    def _connection_result(self):
        result = self.attempt_connection(self.profile)
        if result is not None:
            self.messages.append(result)
            self.any_failure = True
            return red("ERROR")
        return green("OK connection ok")

    def test_connection(self):
        if not self.profile:
            return
        print("Connection:")
        for k, v in self.profile.credentials.connection_info():
            print("  {}: {}".format(k, v))
        print("  Connection test: [{}]".format(self._connection_result()))
        print("")

    @classmethod
    def validate_connection(cls, target_dict):
        """Validate a connection dictionary. On error, raises a DbtConfigError."""
        target_name = "test"
        # make a fake profile that we can parse
        profile_data = {
            "outputs": {
                target_name: target_dict,
            },
        }
        # this will raise a DbtConfigError on failure
        profile = Profile.from_raw_profile_info(
            raw_profile=profile_data,
            profile_name="",
            target_override=target_name,
            renderer=ProfileRenderer({}),
        )
        result = cls.attempt_connection(profile)
        if result is not None:
            raise dbt.exceptions.DbtProfileError(result, result_type="connection_failure")
