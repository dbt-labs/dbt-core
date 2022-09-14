import click
import yaml
from pathlib import Path, PurePath
from click import ParamType


class YAML(ParamType):
    """The Click YAML type. Converts YAML strings into objects."""

    name = "YAML"

    def convert(self, value, param, ctx):
        # assume non-string values are a problem
        if not isinstance(value, str):
            self.fail(f"Cannot load YAML from type {type(value)}", param, ctx)
        try:
            return yaml.load(value, Loader=yaml.Loader)
        except yaml.parser.ParserError:
            self.fail(f"String '{value}' is not valid YAML", param, ctx)


args = click.option(
    "--args",
    help="Supply arguments to the macro. This dictionary will be mapped to the keyword arguments defined in the selected macro. This argument should be a YAML string, eg. '{my_variable: my_value}'",
    type=YAML(),
)

browser = click.option(
    "--browser/--no-browser",
    help="Wether or not to open a local web browser after starting the server",
    default=True,
)

cache_selected_only = click.option(
    "--cache-selected-only/--no-cache-selected-only",
    help="Pre cache database objects relevant to selected resource only.",
    default=False,
)

compile_docs = click.option(
    "--compile/--no-compile",
    help="Wether or not to run 'dbt compile' as part of docs generation",
    default=True,
)

compile_parse = click.option(
    "--compile/--no-compile",
    help="TODO: No help text currently available",
    default=True,
)

config_dir = click.option(
    "--config-dir",
    help="If specified, DBT will show path information for this project",
    type=click.STRING,
)

debug = click.option(
    "--debug/--no-debug",
    "-d/ ",
    help="Display debug logging during dbt execution. Useful for debugging and making bug reports.",
    default=False,
)

defer = click.option(
    "--defer/--no-defer",
    help="If set, defer to the state variable for resolving unselected nodes.",
    default=True,
)

event_buffer_size = click.option(
    "--event-buffer-size",
    help="Sets the max number of events to buffer in EVENT_HISTORY.",
    default=100000,
    type=click.INT,
)

exclude = click.option("--exclude", help="Specify the nodes to exclude.")

fail_fast = click.option(
    "--fail-fast/--no-fail-fast", "-x/ ", help="Stop execution on first failure.", default=False
)

full_refresh = click.option(
    "--full-refresh",
    help="If specified, dbt will drop incremental models and fully-recalculate the incremental table from the model definition.",
    is_flag=True,
)

indirect_selection = click.option(
    "--indirect_selection",
    help="Select all tests that are adjacent to selected resources, even if they those resources have been explicitly selected.",
    type=click.Choice(["eager", "cautious"], case_sensitive=False),
    default="eager",
)

log_format = click.option(
    "--log-format",
    help="Specify the log format, overriding the command's default.",
    type=click.Choice(["text", "json", "default"], case_sensitive=False),
    default="default",
)

log_path = click.option(
    "--log-path",
    help="Configure the 'log-path'. Only applies this setting for the current run. Overrides the 'DBT_LOG_PATH' if it is set.",
    type=click.Path(),
)

models = click.option("-m", "-s", help="Specify the nodes to include.", multiple=True)

output = click.option(
    "--output",
    help="TODO: No current help text",
    type=click.Choice(["json", "name", "path", "selector"], case_sensitive=False),
    default="name",
)

ouptut_keys = click.option(
    "--output-keys",
    help="TODO: No current help text",
    default=False,
)

output_path = click.option(
    "--output",
    "-o",
    help="Specify the output path for the json report. By default, outputs to 'target/sources.json'",
    type=click.Path(file_okay=True, dir_okay=False, writable=True),
    default=PurePath.joinpath(Path.cwd(), "target/sources.json"),
)

parse_only = click.option(
    "--parse-only",
    help="TODO:  No help text currently available",
    is_flag=True,
)

partial_parse = click.option(
    "--partial-parse/--no-partial-parse",
    help="Allow for partial parsing by looking for and writing to a pickle file in the target directory. This overrides the user configuration file.",
    default=True,
)

port = click.option(
    "--port", help="Specify the port number for the docs server", default=8080, type=click.INT
)

print = click.option(
    "--print/--no-print", help="Output all {{ print() }} macro calls.", default=True
)

printer_width = click.option(
    "--printer_width", help="Sets the width of terminal output", type=click.INT, default=80
)

profile = click.option(
    "--profile",
    help="Which profile to load. Overrides setting in dbt_project.yml.",
)

profiles_dir = click.option(
    "--profiles-dir",
    help=f"Which directory to look in for the profiles.yml file. Default = {PurePath.joinpath(Path.home(), '.dbt')}",
    default=PurePath.joinpath(Path.home(), ".dbt"),
    type=click.Path(
        exists=True,
    ),
)

project_dir = click.option(
    "--project-dir",
    help="Which directory to look in for the dbt_project.yml file. Default is the current working directory and its parents.",
    default=Path.cwd(),
    type=click.Path(exists=True),
)

quiet = click.option(
    "--quiet/--no-quiet",
    help="Suppress all non-error logging to stdout. Does not affect {{ print() }} macro calls.",
    default=False,
)

record_timing = click.option(
    "-r",
    "--record-timing-info",
    help="When this option is passed, dbt will output low-level timing stats to the specified file. Example: `--record-timing-info output.profile`",
    is_flag=True,
    default=False,
)

resource_type = click.option(
    "--resource-type",
    help="TODO: No current help text",
    type=click.Choice(
        [
            "metric",
            "source",
            "analysis",
            "model",
            "test",
            "exposure",
            "snapshot",
            "seed",
            "default",
            "all",
        ],
        case_sensitive=False,
    ),
    default="default",
)

selector = click.option("--selector", help="The selector name to use, as defined in selectors.yml")

send_anonymous_usage_stats = click.option(
    "--anonymous-usage-stats/--no-anonymous-usage-stats",
    help="Send anonymous usage stats to dbt Labs.",
    default=True,
)

show = click.option(
    "--show",
    help="Show a sample of the loaded data in the terminal",
    default=False,
)

skip_profile_setup = click.option(
    "--skip-profile-setup",
    "-s",
    help="Skip interative profile setup.",
    default=False,
)

state = click.option(
    "--state",
    help="If set, use the given directory as the source for json files to compare with this project.",
)

static_parser = click.option(
    "--static-parser/--no-static-parser", help="Use the static parser.", default=True
)

store_failures = click.option(
    "--store-failures", help="Store test results (failing rows) in the database", default=False
)

target = click.option("-t", "--target", help="Which target to load for the given profile")

target_path = click.option(
    "--target-path",
    help="Configure the 'target-path'. Only applies this setting for the current run. Overrides the 'DBT_TARGET_PATH' if it is set.",
    type=click.Path(),
)

threads = click.option(
    "--threads",
    help="Specify number of threads to use while executing models. Overrides settings in profiles.yml.",
    default=1,
    type=click.INT,
)

use_colors = click.option(
    "--use-colors/--no-use-colors",
    help="Output is colorized by default and may also be set in a profile or at the command line.",
    default=True,
)

use_experimental_parser = click.option(
    "--use-experimental-parser/--no-use-experimental-parser",
    help="Enable experimental parsing features.",
    default=False,
)

vars = click.option(
    "--vars",
    help="Supply variables to the project. This argument overrides variables defined in your dbt_project.yml file. This argument should be a YAML string, eg. '{my_variable: my_value}'",
    type=YAML(),
)

version = click.option("--version", help="Show version information", is_flag=True, default=False)

version_check = click.option(
    "--version-check/--no-version-check",
    help="Ensure dbt's version matches the one specified in the dbt_project.yml file ('require-dbt-version')",
    default=True,
)

warn_error = click.option(
    "--warn-error/--no-warn-error",
    help="If dbt would normally warn, instead raise an exception. Examples include --models that selects nothing, deprecations, configurations with no associated models, invalid test configurations, and missing sources/refs in tests.",
    default=False,
)

write_json = click.option(
    "--write-json/--no-write-json",
    help="Writing the manifest and run_results.json files to disk",
    default=True,
)

write_manifest = click.option(
    "--write-manifest/--no-write-manifest",
    help="TODO: No help text currently available",
    default=True,
)
