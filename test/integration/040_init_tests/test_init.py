import os
import shutil
from unittest import mock
from unittest.mock import Mock, call
from pathlib import Path

import click

from test.integration.base import DBTIntegrationTest, use_profile


class TestInit(DBTIntegrationTest):
    def tearDown(self):
        project_name = self.get_project_name()

        if os.path.exists(project_name):
            shutil.rmtree(project_name)

        super().tearDown()

    def get_project_name(self):
        return 'my_project_{}'.format(self.unique_schema())

    @property
    def schema(self):
        return 'init_040'

    @property
    def models(self):
        return 'models'

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_postgres_init_task_in_project_with_existing_profiles_yml(self, mock_prompt, mock_confirm):
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')
        manager.confirm.side_effect = ["y"]
        manager.prompt.side_effect = [
            1,
            'localhost',
            5432,
            'test_user',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]

        self.run_dbt(['init'])

        manager.assert_has_calls([
            call.confirm(f"The profile test already exists in {os.path.join(self.test_root_dir, 'profiles.yml')}. Continue and overwrite it?"),
            call.prompt("Which database would you like to use?\n[1] postgres\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)\n\nEnter a number", type=click.INT),
            call.prompt('host (hostname for the instance)', default=None, hide_input=False, type=None),
            call.prompt('port', default=5432, hide_input=False, type=click.INT),
            call.prompt('user (dev username)', default=None, hide_input=False, type=None),
            call.prompt('pass (dev password)', default=None, hide_input=True, type=None),
            call.prompt('dbname (default database that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('schema (default schema that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('threads (1 or more)', default=1, hide_input=False, type=click.INT),
        ])

        with open(os.path.join(self.test_root_dir, 'profiles.yml'), 'r') as f:
            assert f.read() == """config:
  send_anonymous_usage_stats: false
test:
  outputs:
    dev:
      dbname: test_db
      host: localhost
      pass: test_password
      port: 5432
      schema: test_schema
      threads: 4
      type: postgres
      user: test_user
  target: dev
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    @mock.patch.object(Path, 'exists', autospec=True)
    def test_postgres_init_task_in_project_without_existing_profiles_yml(self, exists, mock_prompt, mock_confirm):

        def exists_side_effect(path):
            # Override responses on specific files, default to 'real world' if not overriden
            return {
                'profiles.yml': False
            }.get(path.name, os.path.exists(path))

        exists.side_effect = exists_side_effect
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.prompt.side_effect = [
            1,
            'localhost',
            5432,
            'test_user',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]

        self.run_dbt(['init'])

        manager.assert_has_calls([
            call.prompt("Which database would you like to use?\n[1] postgres\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)\n\nEnter a number", type=click.INT),
            call.prompt('host (hostname for the instance)', default=None, hide_input=False, type=None),
            call.prompt('port', default=5432, hide_input=False, type=click.INT),
            call.prompt('user (dev username)', default=None, hide_input=False, type=None),
            call.prompt('pass (dev password)', default=None, hide_input=True, type=None),
            call.prompt('dbname (default database that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('schema (default schema that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('threads (1 or more)', default=1, hide_input=False, type=click.INT)
        ])

        with open(os.path.join(self.test_root_dir, 'profiles.yml'), 'r') as f:
            assert f.read() == """test:
  outputs:
    dev:
      dbname: test_db
      host: localhost
      pass: test_password
      port: 5432
      schema: test_schema
      threads: 4
      type: postgres
      user: test_user
  target: dev
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    @mock.patch.object(Path, 'exists', autospec=True)
    def test_postgres_init_task_in_project_without_existing_profiles_yml_or_profile_template(self, exists, mock_prompt, mock_confirm):

        def exists_side_effect(path):
            # Override responses on specific files, default to 'real world' if not overriden
            return {
                'profiles.yml': False,
                'profile_template.yml': False,
            }.get(path.name, os.path.exists(path))

        exists.side_effect = exists_side_effect
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')
        manager.prompt.side_effect = [
            1,
        ]
        self.run_dbt(['init'])
        manager.assert_has_calls([
            call.prompt("Which database would you like to use?\n[1] postgres\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)\n\nEnter a number", type=click.INT),
        ])

        with open(os.path.join(self.test_root_dir, 'profiles.yml'), 'r') as f:
            assert f.read() == """test:
  outputs:

    dev:
      type: postgres
      threads: [1 or more]
      host: [host]
      port: [port]
      user: [dev_username]
      pass: [dev_password]
      dbname: [dbname]
      schema: [dev_schema]

    prod:
      type: postgres
      threads: [1 or more]
      host: [host]
      port: [port]
      user: [prod_username]
      pass: [prod_password]
      dbname: [dbname]
      schema: [prod_schema]

  target: dev
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    @mock.patch.object(Path, 'exists', autospec=True)
    def test_postgres_init_task_in_project_with_profile_template_without_existing_profiles_yml(self, exists, mock_prompt, mock_confirm):

        def exists_side_effect(path):
            # Override responses on specific files, default to 'real world' if not overriden
            return {
                'profiles.yml': False,
            }.get(path.name, os.path.exists(path))
        exists.side_effect = exists_side_effect

        with open("profile_template.yml", 'w') as f:
            f.write("""fixed:
  type: postgres
  threads: 4
  host: localhost
  dbname: my_db
  schema: my_schema
prompts:
  port:
    hint: 'The port (for integer test purposes)'
    type: int
    default: 5432
  user:
    hint: 'Your username'
  pass:
    hint: 'Your password'
    hide_input: true""")

        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')
        manager.prompt.side_effect = [
            5432,
            'test_username',
            'test_password'
        ]
        self.run_dbt(['init'])
        manager.assert_has_calls([
            call.prompt('port (The port (for integer test purposes))', default=5432, hide_input=False, type=click.INT),
            call.prompt('user (Your username)', default=None, hide_input=False, type=None),
            call.prompt('pass (Your password)', default=None, hide_input=True, type=None)
        ])

        with open(os.path.join(self.test_root_dir, 'profiles.yml'), 'r') as f:
            assert f.read() == """test:
  outputs:
    dev:
      dbname: my_db
      host: localhost
      pass: test_password
      port: 5432
      schema: my_schema
      threads: 4
      type: postgres
      user: test_username
  target: dev
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_postgres_init_task_in_project_with_invalid_profile_template(self, mock_prompt, mock_confirm):
        """Test that when an invalid profile_template.yml is provided in the project,
        init command falls back to the target's profile_template.yml"""

        with open("profile_template.yml", 'w') as f:
            f.write("""invalid template""")

        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')
        manager.confirm.side_effect = ["y"]
        manager.prompt.side_effect = [
            1,
            'localhost',
            5432,
            'test_username',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]

        self.run_dbt(['init'])

        manager.assert_has_calls([
            call.confirm(f"The profile test already exists in {os.path.join(self.test_root_dir, 'profiles.yml')}. Continue and overwrite it?"),
            call.prompt("Which database would you like to use?\n[1] postgres\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)\n\nEnter a number", type=click.INT),
            call.prompt('host (hostname for the instance)', default=None, hide_input=False, type=None),
            call.prompt('port', default=5432, hide_input=False, type=click.INT),
            call.prompt('user (dev username)', default=None, hide_input=False, type=None),
            call.prompt('pass (dev password)', default=None, hide_input=True, type=None),
            call.prompt('dbname (default database that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('schema (default schema that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('threads (1 or more)', default=1, hide_input=False, type=click.INT)
        ])

        with open(os.path.join(self.test_root_dir, 'profiles.yml'), 'r') as f:
            assert f.read() == """config:
  send_anonymous_usage_stats: false
test:
  outputs:
    dev:
      dbname: test_db
      host: localhost
      pass: test_password
      port: 5432
      schema: test_schema
      threads: 4
      type: postgres
      user: test_username
  target: dev
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_postgres_init_task_outside_of_project(self, mock_prompt, mock_confirm):
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')

        # Start by removing the dbt_project.yml so that we're not in an existing project
        os.remove('dbt_project.yml')

        project_name = self.get_project_name()
        manager.prompt.side_effect = [
            project_name,
            1,
            'localhost',
            5432,
            'test_username',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]
        self.run_dbt(['init'])
        manager.assert_has_calls([
            call.prompt('What is the desired project name?'),
            call.prompt("Which database would you like to use?\n[1] postgres\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)\n\nEnter a number", type=click.INT),
            call.prompt('host (hostname for the instance)', default=None, hide_input=False, type=None),
            call.prompt('port', default=5432, hide_input=False, type=click.INT),
            call.prompt('user (dev username)', default=None, hide_input=False, type=None),
            call.prompt('pass (dev password)', default=None, hide_input=True, type=None),
            call.prompt('dbname (default database that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('schema (default schema that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('threads (1 or more)', default=1, hide_input=False, type=click.INT),
        ])

        with open(os.path.join(self.test_root_dir, 'profiles.yml'), 'r') as f:
            assert f.read() == f"""config:
  send_anonymous_usage_stats: false
{project_name}:
  outputs:
    dev:
      dbname: test_db
      host: localhost
      pass: test_password
      port: 5432
      schema: test_schema
      threads: 4
      type: postgres
      user: test_username
  target: dev
test:
  outputs:
    default2:
      dbname: dbt
      host: localhost
      pass: password
      port: 5432
      schema: {self.unique_schema()}
      threads: 4
      type: postgres
      user: root
    noaccess:
      dbname: dbt
      host: localhost
      pass: password
      port: 5432
      schema: {self.unique_schema()}
      threads: 4
      type: postgres
      user: noaccess
  target: default2
"""

        with open(os.path.join(self.test_root_dir, project_name, 'dbt_project.yml'), 'r') as f:
            assert f.read() == f"""
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: '{project_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: '{project_name}'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/ directory
# as tables. These settings can be overridden in the individual model files
# using the `{{{{ config(...) }}}}` macro.
models:
  {project_name}:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_postgres_init_with_provided_project_name(self, mock_prompt, mock_confirm):
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')

        # Start by removing the dbt_project.yml so that we're not in an existing project
        os.remove('dbt_project.yml')

        manager.prompt.side_effect = [
            1,
            'localhost',
            5432,
            'test_username',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]

        # Provide project name through the init command.
        project_name = self.get_project_name()
        self.run_dbt(['init', project_name])
        manager.assert_has_calls([
            call.prompt("Which database would you like to use?\n[1] postgres\n\n(Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)\n\nEnter a number", type=click.INT),
            call.prompt('host (hostname for the instance)', default=None, hide_input=False, type=None),
            call.prompt('port', default=5432, hide_input=False, type=click.INT),
            call.prompt('user (dev username)', default=None, hide_input=False, type=None),
            call.prompt('pass (dev password)', default=None, hide_input=True, type=None),
            call.prompt('dbname (default database that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('schema (default schema that dbt will build objects in)', default=None, hide_input=False, type=None),
            call.prompt('threads (1 or more)', default=1, hide_input=False, type=click.INT),
        ])

        with open(os.path.join(self.test_root_dir, 'profiles.yml'), 'r') as f:
            assert f.read() == f"""config:
  send_anonymous_usage_stats: false
{project_name}:
  outputs:
    dev:
      dbname: test_db
      host: localhost
      pass: test_password
      port: 5432
      schema: test_schema
      threads: 4
      type: postgres
      user: test_username
  target: dev
test:
  outputs:
    default2:
      dbname: dbt
      host: localhost
      pass: password
      port: 5432
      schema: {self.unique_schema()}
      threads: 4
      type: postgres
      user: root
    noaccess:
      dbname: dbt
      host: localhost
      pass: password
      port: 5432
      schema: {self.unique_schema()}
      threads: 4
      type: postgres
      user: noaccess
  target: default2
"""

        with open(os.path.join(self.test_root_dir, project_name, 'dbt_project.yml'), 'r') as f:
            assert f.read() == f"""
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: '{project_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: '{project_name}'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/ directory
# as tables. These settings can be overridden in the individual model files
# using the `{{{{ config(...) }}}}` macro.
models:
  {project_name}:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_postgres_init_skip_profile_setup(self, mock_prompt, mock_confirm):
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')

        # Start by removing the dbt_project.yml so that we're not in an existing project
        os.remove('dbt_project.yml')

        project_name = self.get_project_name()
        manager.prompt.side_effect = [
            project_name,
            1,
            'localhost',
            5432,
            'test_username',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]

        # provide project name through the ini command
        self.run_dbt(['init', '-s'])
        manager.assert_has_calls([
          call.prompt('What is the desired project name?')
        ])

        with open(os.path.join(self.test_root_dir, project_name, 'dbt_project.yml'), 'r') as f:
            assert f.read() == f"""
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: '{project_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: '{project_name}'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/ directory
# as tables. These settings can be overridden in the individual model files
# using the `{{{{ config(...) }}}}` macro.
models:
  {project_name}:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
"""

    @use_profile('postgres')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_postgres_init_provided_project_name_and_skip_profile_setup(self, mock_prompt, mock_confirm):
        manager = Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')

        # Start by removing the dbt_project.yml so that we're not in an existing project
        os.remove('dbt_project.yml')

        manager.prompt.side_effect = [
            1,
            'localhost',
            5432,
            'test_username',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]

        # provide project name through the ini command
        project_name = self.get_project_name()
        self.run_dbt(['init', project_name, '-s'])
        manager.assert_not_called()

        with open(os.path.join(self.test_root_dir, project_name, 'dbt_project.yml'), 'r') as f:
            assert f.read() == f"""
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: '{project_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: '{project_name}'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/ directory
# as tables. These settings can be overridden in the individual model files
# using the `{{{{ config(...) }}}}` macro.
models:
  {project_name}:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
"""
