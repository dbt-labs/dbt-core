from dbt.cli.main import cli
import click


class TestCLI:
    def test_commands_have_docstrings(self):
        def run_test(commands):
            for _, command in commands.items():
                if type(command) is click.core.Command:
                    assert command.__doc__ is not None
                if type(command) is click.core.Group:
                    run_test(command.commands)

        run_test(cli.commands)

    def test_params_have_help_texts(self):
        def run_test(commands):
            for _, command in commands.items():
                if type(command) is click.core.Command:
                    for param in command.params:
                        assert param.help is not None
                if type(command) is click.core.Group:
                    run_test(command.commands)

        run_test(cli.commands)
