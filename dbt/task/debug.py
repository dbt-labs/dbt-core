# coding=utf-8
import os
import platform
import pprint
import sys

from dbt.logger import GLOBAL_LOGGER as logger
import dbt.clients.system
import dbt.config
import dbt.utils
import dbt.exceptions
from dbt.links import ProfileConfigDocs
from dbt.adapters.factory import get_adapter
from dbt.version import get_installed_version
from dbt.config import Project, Profile
from dbt.clients.yaml_helper import load_yaml_text
from dbt.ui.printer import green, red

from dbt.task.base_task import BaseTask

PROFILE_DIR_MESSAGE = """To view your profiles.yml file, run:

{open_cmd} {profiles_dir}"""

ONLY_PROFILE_MESSAGE = '''
A `dbt_project.yml` file was not found in this directory.
Using the only profile `{}`.
'''.lstrip()

MULTIPLE_PROFILE_MESSAGE = '''
A `dbt_project.yml` file was not found in this directory.
dbt found the following profiles:
{}

To debug one of these profiles, run:
dbt debug --profile [profile-name]
'''.lstrip()

COULD_NOT_CONNECT_MESSAGE = '''
dbt was unable to connect to the specified database.
The database returned the following error:

  >{err}

Check your database credentials and try again. For more information, visit:
{url}
'''.lstrip()


MISSING_PROFILE_MESSAGE = '''
dbt looked for a profiles.yml file in {path}, but did
not find one. For more information on configuring your profile, consult the
documentation:

{url}
'''.lstrip()

FILE_NOT_FOUND = 'file not found'


class DebugTask(BaseTask):
    def __init__(self, args, config=None):
        super(DebugTask, self).__init__(args, config)
        self.profiles_dir = getattr(self.args, 'profiles_dir',
                                    dbt.config.PROFILES_DIR)
        self.profile_path = os.path.join(self.profiles_dir, 'profiles.yml')
        self.project_path = os.path.join(os.getcwd(), 'dbt_project.yml')
        self.cli_vars = dbt.utils.parse_cli_vars(
            getattr(self.args, 'vars', '{}')
        )

        # set by _load_*
        self.profile = None
        self.profile_fail_details = ''
        self.raw_profile_data = None
        self.profile_name = None
        self.project = None
        self.project_fail_details = ''
        self.messages = []

    @property
    def project_profile(self):
        if self.project is None:
            return None
        return self.project.profile_name

    def path_info(self):
        open_cmd = dbt.clients.system.open_dir_cmd()

        message = PROFILE_DIR_MESSAGE.format(
            open_cmd=open_cmd,
            profiles_dir=self.profiles_dir
        )

        logger.info(message)

    def run(self):
        if self.args.config_dir:
            self.path_info()
            return

        version = get_installed_version().to_version_string(skip_matcher=True)
        print('dbt version: {}'.format(version))
        print('python version: {}'.format(sys.version.split()[0]))
        print('python path: {}'.format(sys.executable))
        print('os info: {}'.format(platform.platform()))
        print('Using profiles.yml file at {}'.format(self.profile_path))
        print('')
        self.test_configuration()
        self.test_dependencies()
        self.test_connection()

        for message in self.messages:
            print(message)
            print('')

    def _load_project(self):
        if not os.path.exists(self.project_path):
            self.project_fail_details = FILE_NOT_FOUND
            return red('✗ not found')

        try:
            self.project = Project.from_current_directory(self.cli_vars)
        except dbt.exceptions.DbtConfigError as exc:
            self.project_fail_details = str(exc)
            return red('✗ invalid')

        return green('✓ found and valid')

    def _profile_found(self):
        if not self.raw_profile_data:
            return red('✗ not found')
        if self.profile_name in self.raw_profile_data:
            return green('✓ found')
        else:
            return red('✗ not found')

    def _target_found(self):
        requirements = (self.raw_profile_data and self.profile_name and
                        self.target_name)
        if not requirements:
            return red('✗ not found')
        if self.profile_name not in self.raw_profile_data:
            return red('✗ not found')
        profiles = self.raw_profile_data[self.profile_name]['outputs']
        if self.target_name not in profiles:
            return red('✗ not found')
        return green('✓ found')

    def _choose_profile_name(self):
        assert self.project or self.project_fail_details, \
            '_load_project() required'

        project_profile = None
        if self.project:
            project_profile = self.project.profile_name

        args_profile = getattr(self.args, 'profile', None)

        try:
            return Profile.pick_profile_name(args_profile, project_profile)
        except dbt.exceptions.DbtConfigError:
            pass
        # try to guess

        if self.raw_profile_data:
            profiles = [k for k in self.raw_profile_data if k != 'config']
            if len(profiles) == 0:
                self.messages.append('The profiles.yml has no profiles')
            elif len(profiles) == 1:
                self.messages.append(ONLY_PROFILE_MESSAGE.format(profiles[0]))
                return profiles[0]
            else:
                self.messages.append(MULTIPLE_PROFILE_MESSAGE.format(
                    '\n'.join(' - {}'.format(o) for o in profiles)
                ))
        return None

    def _choose_target_name(self):
        has_raw_profile = (self.raw_profile_data and self.profile_name and
                           self.profile_name in self.raw_profile_data)
        if has_raw_profile:
            raw_profile = self.raw_profile_data[self.profile_name]

            target_name, _ = Profile.render_profile(
                raw_profile, self.profile_name,
                getattr(self.args, 'target', None), self.cli_vars
            )
            return target_name
        return None

    def _load_profile(self):
        if not os.path.exists(self.profile_path):
            self.profile_fail_details = FILE_NOT_FOUND
            self.messages.append(MISSING_PROFILE_MESSAGE.format(
                path=self.profile_path, url=ProfileConfigDocs
            ))
            return red('✗ not found')

        try:
            raw_profile_data = load_yaml_text(
                dbt.clients.system.load_file_contents(self.profile_path)
            )
        except Exception:
            pass  # we'll report this when we try to load the profile for real
        else:
            if isinstance(raw_profile_data, dict):
                self.raw_profile_data = raw_profile_data

        self.profile_name = self._choose_profile_name()
        self.target_name = self._choose_target_name()
        try:
            self.profile = Profile.from_args(self.args, self.profile_name,
                                             self.cli_vars)
        except dbt.exceptions.DbtConfigError as exc:
            self.profile_fail_details = str(exc)
            return red('✗ invalid')

        return green('✓ found and valid')

    def test_git(self):
        try:
            dbt.clients.system.run_cmd(os.getcwd(), ['git', '--help'])
        except dbt.exceptions.ExecutableError as exc:
            self.messages.append('Error from git --help: {!s}'.format(exc))
            return red('✗ error')
        return green('✓ found')

    def test_dependencies(self):
        print('Required dependencies:')
        print(' - git [{}]'.format(self.test_git()))
        print('')

    def test_configuration(self):
        project_status = self._load_project()
        profile_status = self._load_profile()
        print('Configuration:')
        print('  profiles.yml file [{}]'.format(profile_status))
        print('  dbt_project.yml file [{}]'.format(project_status))
        # skip profile stuff if we can't find a profile name
        if self.profile_name is not None:
            print('  profile: {} [{}]'.format(self.profile_name,
                                              self._profile_found()))
            print('  target: {} [{}]'.format(self.target_name,
                                             self._target_found()))
        print('')
        self._log_project_fail()
        self._log_profile_fail()

    def _log_project_fail(self):
        if not self.project_fail_details:
            return
        if self.project_fail_details == FILE_NOT_FOUND:
            return
        print('Project loading failed for the following reason:')
        print(self.project_fail_details)
        print('')

    def _log_profile_fail(self):
        if not self.profile_fail_details:
            return
        if self.profile_fail_details == FILE_NOT_FOUND:
            return
        if self.profile_name is None:
            return  # we expect an error (no profile provided)
        print('Profile loading failed for the following reason:')
        print(self.profile_fail_details)
        print('')

    def _connection_result(self):
        adapter = get_adapter(self.profile)
        try:
            adapter.execute('select 1 as id')
        except Exception as exc:
            self.messages.append(COULD_NOT_CONNECT_MESSAGE.format(
                err=str(exc),
                url=ProfileConfigDocs
            ))
            return red('✗ error')
        return green('✓ connection ok')

    def test_connection(self):
        if not self.profile:
            return
        print('Connection:')
        for k, v in self.profile.credentials.connection_info():
            print('  {}: {}'.format(k, v))
        print('  Connection test: {}'.format(self._connection_result()))
        print('')
