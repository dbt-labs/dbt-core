import pathlib

from test_profile_dir import environ

from dbt.cli.main import dbtRunner

jinjaesque_password = "no{{jinja{%re{#ndering"

profile_with_jinjaesque_password = f"""test:
  outputs:
    default:
      dbname: my_db
      host: localhost
      password: {jinjaesque_password}
      port: 12345
      schema: dummy
      threads: 4
      type: postgres
      user: peter.webb
  target: default
"""

profile_with_env_password = """test:
  outputs:
    default:
      dbname: my_db
      host: localhost
      password: "{{ env_var('DBT_PASSWORD') }}"
      port: 12345
      schema: dummy
      threads: 4
      type: postgres
      user: peter.webb
  target: default
"""


class TestProfileParsing:
    def write_profiles_yml(self, profiles_root, content) -> None:
        with open(pathlib.Path(profiles_root, "profiles.yml"), "w") as profiles_yml:
            profiles_yml.write(content)

    def test_password_not_jinja_rendered_when_invalid(self, project, profiles_root) -> None:
        """Verifies that passwords that contain Jinja control characters, but which are
        not valid Jinja, do not cause errors."""
        self.write_profiles_yml(profiles_root, profile_with_jinjaesque_password)

        events = []
        result = dbtRunner(callbacks=[events.append]).invoke(["parse"])
        assert result.success

        for e in events:
            assert "no{{jinja{%re{#ndering" not in e.info.msg

    def test_password_jinja_rendered_when_valid(self, project, profiles_root) -> None:
        """Verifies that a password value that is valid Jinja is rendered as such,
        and that it doesn't cause problems if the resulting value looks like Jinja"""
        self.write_profiles_yml(profiles_root, profile_with_env_password)

        events = []
        with environ({"DBT_PASSWORD": jinjaesque_password}):
            result = dbtRunner(callbacks=[events.append]).invoke(["parse"])

        assert result.success
        assert project.adapter.config.credentials.password == jinjaesque_password
