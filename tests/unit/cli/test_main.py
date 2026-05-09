import click

from dbt.cli.flags import command_args
from dbt.cli.main import cli
from dbt.cli.types import Command


class TestCLI:
    def _all_commands(self, group=cli, result=set()):
        for command in group.commands.values():
            result.add(command)
            if isinstance(command, click.Group):
                self._all_commands(command, result)
                continue
        return result

    def test_commands_have_docstrings(self):
        for command in self._all_commands():
            assert command.__doc__ is not None

    # TODO:  This isn't the ideal way to test params as
    # they will be tested as many times as they are used as decorators.
    # This is inefficent (obvs)
    def test_unhidden_params_have_help_texts(self):
        def run_test(command):
            for param in command.params:
                # arguments can't have help text
                if not isinstance(param, click.Argument) and not param.hidden:
                    assert param.help is not None
            if type(command) is click.Group:
                for command in command.commands.values():
                    run_test(command)

        run_test(cli)

    def test_param_names_match_envvars(self):
        def run_test(command):
            for param in command.params:
                # deprecated params are named "deprecated_x" and do not need to have
                # a parallel name like "DBT_"
                if isinstance(param.envvar, list):
                    envvar = param.envvar[0]
                else:
                    envvar = param.envvar

                if envvar is not None and "deprecated_" not in param.name:
                    assert ("DBT_" + param.name.upper() == envvar) or (
                        "DBT_ENGINE_" + param.name.upper() == envvar
                    )
            if type(command) is click.Group:
                for command in command.commands.values():
                    run_test(command)

        run_test(cli)

    def test_commands_in_enum_and_dict(self):
        for command in self._all_commands(cli):
            if isinstance(command, click.Group):
                continue
            cmd = Command.from_str(command.name)
            command_args(cmd)

    def test_dunder_main_module_importable(self):
        # Regression test for dbt-labs/dbt-core#11013:
        # `python -m dbt.cli` must work, which requires dbt/cli/__main__.py
        # to exist and be importable.
        import importlib

        module = importlib.import_module("dbt.cli.__main__")
        assert module.cli is cli
