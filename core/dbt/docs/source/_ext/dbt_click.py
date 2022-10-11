import click
import click.types as click_t
import dbt.cli.option_types as dbt_t
from docutils import nodes
from docutils.parsers.rst import Directive
import traceback
import typing as t


PARAM_TYPE_MAP = {
    click_t.BoolParamType: lambda _: "boolean",
    click_t.Choice: lambda c: f"choice: {c.choices}",
    click_t.IntParamType: lambda _: "int",
    click_t.Path: lambda _: "path",
    click_t.StringParamType: lambda _: "string",
    dbt_t.YAML: lambda _: "YAML",
}


def format_command(cmd) -> nodes.section:
    cmd_name = cmd.name.replace("-", "_")
    section = nodes.section(
        "",
        nodes.title(text=f"Command: {cmd_name}"),
        ids=[cmd_name],
        names=[cmd_name],
    )
    section.extend(format_params(cmd))
    return section


def format_params(cmd) -> t.List[nodes.section]:
    lines = []
    for param in cmd.params:
        uid = f"{cmd.name}|{param.name}"
        param_section = nodes.section(
            "",
            nodes.title(text=param.name),
            ids=[uid],
            names=[uid],
        )

        get_type_str = PARAM_TYPE_MAP.get(type(param.type), lambda _: "unknown")
        type_str = get_type_str(param.type)

        param_section.append(nodes.paragraph(text=f"Type: {type_str}"))
        param_section.append(nodes.paragraph(text=param.help))
        lines.append(param_section)
    return lines


def load_module(module_path: str, error) -> t.Union[click.Command, click.Group]:
    try:
        module_name, attr_name = module_path.split(":", 1)
    except ValueError:  # noqa
        raise error(f'"{module_path}" is not of format "module:parser"')

    try:
        mod = __import__(module_name, globals(), locals(), [attr_name])
    except Exception:  # noqa
        raise error(
            f'Failed to import "{attr_name}" from "{module_name}". '
            f"The following exception was raised:\n{traceback.format_exc()}"
        )

    if not hasattr(mod, attr_name):
        raise error(f'Module "{module_name}" has no attribute "{attr_name}"')

    parser = getattr(mod, attr_name)

    if not isinstance(parser, (click.Command, click.Group)):
        raise error(
            f'"{type(parser)}" of type "{module_path}" is not click.Command'
            ' or click.Group."click.BaseCommand"'
        )
    return parser


class DBTClick(Directive):
    has_content = False
    required_arguments = 1

    def run(self):
        section = nodes.section(
            "",
            ids=["dbt-section"],
            names=["dbt-section"],
        )
        cmds = self._get_commands(self.arguments[0])
        for cmd in cmds:
            command_section = format_command(cmd)
            section.extend(command_section)
        return [section]

    def _get_commands(self, module: str) -> t.List[click.Command]:
        click_group = load_module(module, self.error)
        if type(click_group) is not click.Group:
            raise self.error('Type "click.Group" not supported in dbt_click extension')
        cmd_strs = [cmd for cmd in click_group.commands]
        cmd_strs.sort()
        cmds = []
        for cmd_str in cmd_strs:
            cmd = click_group.commands.get(cmd_str)
            if cmd is not None:
                cmds.append(cmd)
        return cmds


def setup(app) -> t.Dict[str, t.Any]:
    app.add_directive("dbt_click", DBTClick)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
