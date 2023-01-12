from dataclasses import dataclass
from dbt.ui import line_wrap_message, warning_tag, red, green, yellow
from dbt.constants import MAXIMUM_SEED_SIZE_NAME, PIN_PACKAGE_URL
from dbt.events.base_types import (
    DynamicLevel,
    NoFile,
    DebugLevel,
    InfoLevel,
    WarnLevel,
    ErrorLevel,
    Cache,
    AdapterEventStringFunctor,
    EventStringFunctor,
    EventLevel,
)
from dbt.events.format import format_fancy_output_line, pluralize

# The generated classes quote the included message classes, requiring the following lines
from dbt.events.proto_types import EventInfo, RunResultMsg, ListOfStrings  # noqa
from dbt.events.proto_types import NodeInfo, ReferenceKeyMsg, TimingInfoMsg  # noqa
from dbt.events import proto_types as pt

from dbt.node_types import NodeType


# The classes in this file represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. classes extend superclasses that indicate what
# destinations they are intended for, which mypy uses to enforce
# that the necessary methods are defined.


# Event codes have prefixes which follow this table
#
# | Code |     Description     |
# |:----:|:-------------------:|
# | A    | Pre-project loading |
# | D    | Deprecations        |
# | E    | DB adapter          |
# | I    | Project parsing     |
# | M    | Deps generation     |
# | Q    | Node execution      |
# | W    | Node testing        |
# | Z    | Misc                |
# | T    | Test only           |
#
# The basic idea is that event codes roughly translate to the natural order of running a dbt task


def format_adapter_message(name, base_msg, args) -> str:
    # only apply formatting if there are arguments to format.
    # avoids issues like "dict: {k: v}".format() which results in `KeyError 'k'`
    msg = base_msg if len(args) == 0 else base_msg.format(*args)
    return f"{name} adapter: {msg}"


# =======================================================
# A - Pre-project loading
# =======================================================


@dataclass
class MainReportVersion(InfoLevel, pt.MainReportVersion):  # noqa
    def code(self):
        return "A001"

    def message(self):
        return f"Running with dbt{self.version}"


@dataclass
class MainReportArgs(DebugLevel, pt.MainReportArgs):  # noqa
    def code(self):
        return "A002"

    def message(self):
        return f"running dbt with arguments {str(self.args)}"


@dataclass
class MainTrackingUserState(DebugLevel, pt.MainTrackingUserState):
    def code(self):
        return "A003"

    def message(self):
        return f"Tracking: {self.user_state}"


@dataclass
class MergedFromState(DebugLevel, pt.MergedFromState):
    def code(self):
        return "A004"

    def message(self) -> str:
        return f"Merged {self.num_merged} items from state (sample: {self.sample})"


@dataclass
class MissingProfileTarget(InfoLevel, pt.MissingProfileTarget):
    def code(self):
        return "A005"

    def message(self) -> str:
        return f"target not specified in profile '{self.profile_name}', using '{self.target_name}'"


# Skipped A006, A007


@dataclass
class InvalidOptionYAML(ErrorLevel, pt.InvalidOptionYAML):
    def code(self):
        return "A008"

    def message(self) -> str:
        return f"The YAML provided in the --{self.option_name} argument is not valid."


@dataclass
class LogDbtProjectError(ErrorLevel, pt.LogDbtProjectError):
    def code(self):
        return "A009"

    def message(self) -> str:
        msg = "Encountered an error while reading the project:"
        if self.exc:
            msg += f"  ERROR: {str(self.exc)}"
        return msg


# Skipped A010


@dataclass
class LogDbtProfileError(ErrorLevel, pt.LogDbtProfileError):
    def code(self):
        return "A011"

    def message(self) -> str:
        msg = "Encountered an error while reading profiles:\n" f"  ERROR: {str(self.exc)}"
        if self.profiles:
            msg += "Defined profiles:\n"
            for profile in self.profiles:
                msg += f" - {profile}"
        else:
            msg += "There are no profiles defined in your profiles.yml file"

        msg += """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""
        return msg


@dataclass
class StarterProjectPath(DebugLevel, pt.StarterProjectPath):
    def code(self):
        return "A017"

    def message(self) -> str:
        return f"Starter project path: {self.dir}"


@dataclass
class ConfigFolderDirectory(InfoLevel, pt.ConfigFolderDirectory):
    def code(self):
        return "A018"

    def message(self) -> str:
        return f"Creating dbt configuration folder at {self.dir}"


@dataclass
class NoSampleProfileFound(InfoLevel, pt.NoSampleProfileFound):
    def code(self):
        return "A019"

    def message(self) -> str:
        return f"No sample profile found for {self.adapter}."


@dataclass
class ProfileWrittenWithSample(InfoLevel, pt.ProfileWrittenWithSample):
    def code(self):
        return "A020"

    def message(self) -> str:
        return (
            f"Profile {self.name} written to {self.path} "
            "using target's sample configuration. Once updated, you'll be able to "
            "start developing with dbt."
        )


@dataclass
class ProfileWrittenWithTargetTemplateYAML(InfoLevel, pt.ProfileWrittenWithTargetTemplateYAML):
    def code(self):
        return "A021"

    def message(self) -> str:
        return (
            f"Profile {self.name} written to {self.path} using target's "
            "profile_template.yml and your supplied values. Run 'dbt debug' to "
            "validate the connection."
        )


@dataclass
class ProfileWrittenWithProjectTemplateYAML(InfoLevel, pt.ProfileWrittenWithProjectTemplateYAML):
    def code(self):
        return "A022"

    def message(self) -> str:
        return (
            f"Profile {self.name} written to {self.path} using project's "
            "profile_template.yml and your supplied values. Run 'dbt debug' to "
            "validate the connection."
        )


@dataclass
class SettingUpProfile(InfoLevel, pt.SettingUpProfile):
    def code(self):
        return "A023"

    def message(self) -> str:
        return "Setting up your profile."


@dataclass
class InvalidProfileTemplateYAML(InfoLevel, pt.InvalidProfileTemplateYAML):
    def code(self):
        return "A024"

    def message(self) -> str:
        return "Invalid profile_template.yml in project."


@dataclass
class ProjectNameAlreadyExists(InfoLevel, pt.ProjectNameAlreadyExists):
    def code(self):
        return "A025"

    def message(self) -> str:
        return f"A project called {self.name} already exists here."


@dataclass
class ProjectCreated(InfoLevel, pt.ProjectCreated):
    def code(self):
        return "A026"

    def message(self) -> str:
        return f"""
Your new dbt project "{self.project_name}" was created!

For more information on how to configure the profiles.yml file,
please consult the dbt documentation here:

  {self.docs_url}

One more thing:

Need help? Don't hesitate to reach out to us via GitHub issues or on Slack:

  {self.slack_url}

Happy modeling!
"""


# =======================================================
# D - Deprecations
# =======================================================


@dataclass
class PackageRedirectDeprecation(WarnLevel, pt.PackageRedirectDeprecation):  # noqa
    def code(self):
        return "D001"

    def message(self):
        description = (
            f"The `{self.old_name}` package is deprecated in favor of `{self.new_name}`. Please "
            f"update your `packages.yml` configuration to use `{self.new_name}` instead."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


@dataclass
class PackageInstallPathDeprecation(WarnLevel, pt.PackageInstallPathDeprecation):  # noqa
    def code(self):
        return "D002"

    def message(self):
        description = """\
        The default package install path has changed from `dbt_modules` to `dbt_packages`.
        Please update `clean-targets` in `dbt_project.yml` and check `.gitignore` as well.
        Or, set `packages-install-path: dbt_modules` if you'd like to keep the current value.
        """
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


@dataclass
class ConfigSourcePathDeprecation(WarnLevel, pt.ConfigSourcePathDeprecation):  # noqa
    def code(self):
        return "D003"

    def message(self):
        description = (
            f"The `{self.deprecated_path}` config has been renamed to `{self.exp_path}`."
            "Please update your `dbt_project.yml` configuration to reflect this change."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


@dataclass
class ConfigDataPathDeprecation(WarnLevel, pt.ConfigDataPathDeprecation):  # noqa
    def code(self):
        return "D004"

    def message(self):
        description = (
            f"The `{self.deprecated_path}` config has been renamed to `{self.exp_path}`."
            "Please update your `dbt_project.yml` configuration to reflect this change."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


@dataclass
class AdapterDeprecationWarning(WarnLevel, pt.AdapterDeprecationWarning):  # noqa
    def code(self):
        return "D005"

    def message(self):
        description = (
            f"The adapter function `adapter.{self.old_name}` is deprecated and will be removed in "
            f"a future release of dbt. Please use `adapter.{self.new_name}` instead. "
            f"\n\nDocumentation for {self.new_name} can be found here:"
            f"\n\nhttps://docs.getdbt.com/docs/adapter"
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


@dataclass
class MetricAttributesRenamed(WarnLevel, pt.MetricAttributesRenamed):  # noqa
    def code(self):
        return "D006"

    def message(self):
        description = (
            "dbt-core v1.3 renamed attributes for metrics:"
            "\n  'sql'              -> 'expression'"
            "\n  'type'             -> 'calculation_method'"
            "\n  'type: expression' -> 'calculation_method: derived'"
            f"\nPlease remove them from the metric definition of metric '{self.metric_name}'"
            "\nRelevant issue here: https://github.com/dbt-labs/dbt-core/issues/5849"
        )

        return warning_tag(f"Deprecated functionality\n\n{description}")


@dataclass
class ExposureNameDeprecation(WarnLevel, pt.ExposureNameDeprecation):  # noqa
    def code(self):
        return "D007"

    def message(self):
        description = (
            "Starting in v1.3, the 'name' of an exposure should contain only letters, "
            "numbers, and underscores. Exposures support a new property, 'label', which may "
            f"contain spaces, capital letters, and special characters. {self.exposure} does not "
            "follow this pattern. Please update the 'name', and use the 'label' property for a "
            "human-friendly title. This will raise an error in a future version of dbt-core."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


@dataclass
class InternalDeprecation(WarnLevel, pt.InternalDeprecation):
    def code(self):
        return "D008"

    def message(self):
        extra_reason = ""
        if self.reason:
            extra_reason = f"\n{self.reason}"
        msg = (
            f"`{self.name}` is deprecated and will be removed in dbt-core version {self.version}\n\n"
            f"Adapter maintainers can resolve this deprecation by {self.suggested_action}. {extra_reason}"
        )
        return warning_tag(msg)


# =======================================================
# E - DB Adapter
# =======================================================


@dataclass
class AdapterEventDebug(DebugLevel, AdapterEventStringFunctor, pt.AdapterEventDebug):  # noqa
    def code(self):
        return "E001"

    def message(self):
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class AdapterEventInfo(InfoLevel, AdapterEventStringFunctor, pt.AdapterEventInfo):  # noqa
    def code(self):
        return "E002"

    def message(self):
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class AdapterEventWarning(WarnLevel, AdapterEventStringFunctor, pt.AdapterEventWarning):  # noqa
    def code(self):
        return "E003"

    def message(self):
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class AdapterEventError(ErrorLevel, AdapterEventStringFunctor, pt.AdapterEventError):  # noqa
    def code(self):
        return "E004"

    def message(self):
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class NewConnection(DebugLevel, pt.NewConnection):
    def code(self):
        return "E005"

    def message(self) -> str:
        return f"Acquiring new {self.conn_type} connection '{self.conn_name}'"


@dataclass
class ConnectionReused(DebugLevel, pt.ConnectionReused):
    def code(self):
        return "E006"

    def message(self) -> str:
        return f"Re-using an available connection from the pool (formerly {self.conn_name})"


@dataclass
class ConnectionLeftOpenInCleanup(DebugLevel, pt.ConnectionLeftOpenInCleanup):
    def code(self):
        return "E007"

    def message(self) -> str:
        return f"Connection '{self.conn_name}' was left open."


@dataclass
class ConnectionClosedInCleanup(DebugLevel, pt.ConnectionClosedInCleanup):
    def code(self):
        return "E008"

    def message(self) -> str:
        return f"Connection '{self.conn_name}' was properly closed."


@dataclass
class RollbackFailed(DebugLevel, pt.RollbackFailed):  # noqa
    def code(self):
        return "E009"

    def message(self) -> str:
        return f"Failed to rollback '{self.conn_name}'"


# TODO: can we combine this with ConnectionClosed?
@dataclass
class ConnectionClosed(DebugLevel, pt.ConnectionClosed):
    def code(self):
        return "E010"

    def message(self) -> str:
        return f"On {self.conn_name}: Close"


# TODO: can we combine this with ConnectionLeftOpen?
@dataclass
class ConnectionLeftOpen(DebugLevel, pt.ConnectionLeftOpen):
    def code(self):
        return "E011"

    def message(self) -> str:
        return f"On {self.conn_name}: No close available on handle"


@dataclass
class Rollback(DebugLevel, pt.Rollback):
    def code(self):
        return "E012"

    def message(self) -> str:
        return f"On {self.conn_name}: ROLLBACK"


@dataclass
class CacheMiss(DebugLevel, pt.CacheMiss):
    def code(self):
        return "E013"

    def message(self) -> str:
        return (
            f'On "{self.conn_name}": cache miss for schema '
            f'"{self.database}.{self.schema}", this is inefficient'
        )


@dataclass
class ListRelations(DebugLevel, pt.ListRelations):
    def code(self):
        return "E014"

    def message(self) -> str:
        return f"with database={self.database}, schema={self.schema}, relations={self.relations}"


@dataclass
class ConnectionUsed(DebugLevel, pt.ConnectionUsed):
    def code(self):
        return "E015"

    def message(self) -> str:
        return f'Using {self.conn_type} connection "{self.conn_name}"'


@dataclass
class SQLQuery(DebugLevel, pt.SQLQuery):
    def code(self):
        return "E016"

    def message(self) -> str:
        return f"On {self.conn_name}: {self.sql}"


@dataclass
class SQLQueryStatus(DebugLevel, pt.SQLQueryStatus):
    def code(self):
        return "E017"

    def message(self) -> str:
        return f"SQL status: {self.status} in {self.elapsed} seconds"


@dataclass
class SQLCommit(DebugLevel, pt.SQLCommit):
    def code(self):
        return "E018"

    def message(self) -> str:
        return f"On {self.conn_name}: COMMIT"


@dataclass
class ColTypeChange(DebugLevel, pt.ColTypeChange):
    def code(self):
        return "E019"

    def message(self) -> str:
        return f"Changing col type from {self.orig_type} to {self.new_type} in table {self.table}"


@dataclass
class SchemaCreation(DebugLevel, pt.SchemaCreation):
    def code(self):
        return "E020"

    def message(self) -> str:
        return f'Creating schema "{self.relation}"'


@dataclass
class SchemaDrop(DebugLevel, pt.SchemaDrop):
    def code(self):
        return "E021"

    def message(self) -> str:
        return f'Dropping schema "{self.relation}".'


@dataclass
class CacheAction(DebugLevel, Cache, pt.CacheAction):
    def code(self):
        return "E022"

    def message(self):
        if self.action == "add_link":
            return f"adding link, {self.ref_key} references {self.ref_key_2}"
        elif self.action == "add_relation":
            return f"adding relation: {str(self.ref_key)}"
        elif self.action == "drop_missing_relation":
            return f"dropped a nonexistent relationship: {str(self.ref_key)}"
        elif self.action == "drop_cascade":
            return f"drop {self.ref_key} is cascading to {self.ref_list}"
        elif self.action == "drop_relation":
            return f"Dropping relation: {self.ref_key}"
        elif self.action == "update_reference":
            return (
                f"updated reference from {self.ref_key} -> {self.ref_key_3} to "
                f"{self.ref_key_2} -> {self.ref_key_3}"
            )
        elif self.action == "temporary_relation":
            return f"old key {self.ref_key} not found in self.relations, assuming temporary"
        elif self.action == "rename_relation":
            return f"Renaming relation {self.ref_key} to {self.ref_key_2}"
        elif self.action == "uncached_relation":
            return (
                f"{self.ref_key_2} references {str(self.ref_key)} "
                f"but {self.ref_key.database}.{self.ref_key.schema}"
                "is not in the cache, skipping assumed external relation"
            )
        else:
            return f"{self.ref_key}"


# Skipping E023, E024, E025, E026, E027, E028, E029, E030


@dataclass
class CacheDumpGraph(DebugLevel, Cache, pt.CacheDumpGraph):
    def code(self):
        return "E031"

    def message(self) -> str:
        return f"{self.before_after} {self.action} : {self.dump}"


# Skipping E032, E033, E034


@dataclass
class AdapterImportError(InfoLevel, pt.AdapterImportError):
    def code(self):
        return "E035"

    def message(self) -> str:
        return f"Error importing adapter: {self.exc}"


@dataclass
class PluginLoadError(DebugLevel, pt.PluginLoadError):  # noqa
    def code(self):
        return "E036"

    def message(self):
        return f"{self.exc_info}"


@dataclass
class NewConnectionOpening(DebugLevel, pt.NewConnectionOpening):
    def code(self):
        return "E037"

    def message(self) -> str:
        return f"Opening a new connection, currently in state {self.connection_state}"


@dataclass
class CodeExecution(DebugLevel, pt.CodeExecution):
    def code(self):
        return "E038"

    def message(self) -> str:
        return f"On {self.conn_name}: {self.code_content}"


@dataclass
class CodeExecutionStatus(DebugLevel, pt.CodeExecutionStatus):
    def code(self):
        return "E039"

    def message(self) -> str:
        return f"Execution status: {self.status} in {self.elapsed} seconds"


@dataclass
class CatalogGenerationError(WarnLevel, pt.CatalogGenerationError):
    def code(self):
        return "E040"

    def message(self) -> str:
        return f"Encountered an error while generating catalog: {self.exc}"


@dataclass
class WriteCatalogFailure(ErrorLevel, pt.WriteCatalogFailure):
    def code(self):
        return "E041"

    def message(self) -> str:
        return (
            f"dbt encountered {self.num_exceptions} failure{(self.num_exceptions != 1) * 's'} "
            "while writing the catalog"
        )


@dataclass
class CatalogWritten(InfoLevel, pt.CatalogWritten):
    def code(self):
        return "E042"

    def message(self) -> str:
        return f"Catalog written to {self.path}"


@dataclass
class CannotGenerateDocs(InfoLevel, pt.CannotGenerateDocs):
    def code(self):
        return "E043"

    def message(self) -> str:
        return "compile failed, cannot generate docs"


@dataclass
class BuildingCatalog(InfoLevel, pt.BuildingCatalog):
    def code(self):
        return "E044"

    def message(self) -> str:
        return "Building catalog"


@dataclass
class DatabaseErrorRunningHook(InfoLevel, pt.DatabaseErrorRunningHook):
    def code(self):
        return "E045"

    def message(self) -> str:
        return f"Database error while running {self.hook_type}"


@dataclass
class HooksRunning(InfoLevel, pt.HooksRunning):
    def code(self):
        return "E046"

    def message(self) -> str:
        plural = "hook" if self.num_hooks == 1 else "hooks"
        return f"Running {self.num_hooks} {self.hook_type} {plural}"


@dataclass
class FinishedRunningStats(InfoLevel, pt.FinishedRunningStats):
    def code(self):
        return "E047"

    def message(self) -> str:
        return f"Finished running {self.stat_line}{self.execution} ({self.execution_time:0.2f}s)."


# =======================================================
# I - Project parsing
# =======================================================


@dataclass
class ParseCmdOut(InfoLevel, pt.ParseCmdOut):
    def code(self):
        return "I001"

    def message(self) -> str:
        return self.msg


# Skipping I002, I003, I004, I005, I006, I007, I008, I009, I010


@dataclass
class GenericTestFileParse(DebugLevel, pt.GenericTestFileParse):
    def code(self):
        return "I011"

    def message(self) -> str:
        return f"Parsing {self.path}"


@dataclass
class MacroFileParse(DebugLevel, pt.MacroFileParse):
    def code(self):
        return "I012"

    def message(self) -> str:
        return f"Parsing {self.path}"


# Skipping I013


@dataclass
class PartialParsingErrorProcessingFile(DebugLevel, pt.PartialParsingErrorProcessingFile):
    def code(self):
        return "I014"

    def message(self) -> str:
        return f"Partial parsing exception processing file {self.file}"


# Skipped I015


@dataclass
class PartialParsingError(DebugLevel, pt.PartialParsingError):
    def code(self):
        return "I016"

    def message(self) -> str:
        return f"PP exception info: {self.exc_info}"


@dataclass
class PartialParsingSkipParsing(DebugLevel, pt.PartialParsingSkipParsing):
    def code(self):
        return "I017"

    def message(self) -> str:
        return "Partial parsing enabled, no changes found, skipping parsing"


# Skipped I018, I019, I020, I021, I022, I023


@dataclass
class UnableToPartialParse(InfoLevel, pt.UnableToPartialParse):
    def code(self):
        return "I024"

    def message(self) -> str:
        return f"Unable to do partial parsing because {self.reason}"


# Skipped I025, I026, I026, I027


@dataclass
class PartialParsingNotEnabled(DebugLevel, pt.PartialParsingNotEnabled):
    def code(self):
        return "I028"

    def message(self) -> str:
        return "Partial parsing not enabled"


@dataclass
class ParsedFileLoadFailed(DebugLevel, pt.ParsedFileLoadFailed):  # noqa
    def code(self):
        return "I029"

    def message(self) -> str:
        return f"Failed to load parsed file from disk at {self.path}: {self.exc}"


# Skipped I030


@dataclass
class StaticParserCausedJinjaRendering(DebugLevel, pt.StaticParserCausedJinjaRendering):
    def code(self):
        return "I031"

    def message(self) -> str:
        return f"1605: jinja rendering because of STATIC_PARSER flag. file: {self.path}"


# TODO: Experimental/static parser uses these for testing and some may be a good use case for
#       the `TestLevel` logger once we implement it.  Some will probably stay `DebugLevel`.
@dataclass
class UsingExperimentalParser(DebugLevel, pt.UsingExperimentalParser):
    def code(self):
        return "I032"

    def message(self) -> str:
        return f"1610: conducting experimental parser sample on {self.path}"


@dataclass
class SampleFullJinjaRendering(DebugLevel, pt.SampleFullJinjaRendering):
    def code(self):
        return "I033"

    def message(self) -> str:
        return f"1611: conducting full jinja rendering sample on {self.path}"


@dataclass
class StaticParserFallbackJinjaRendering(DebugLevel, pt.StaticParserFallbackJinjaRendering):
    def code(self):
        return "I034"

    def message(self) -> str:
        return f"1602: parser fallback to jinja rendering on {self.path}"


@dataclass
class StaticParsingMacroOverrideDetected(DebugLevel, pt.StaticParsingMacroOverrideDetected):
    def code(self):
        return "I035"

    def message(self) -> str:
        return f"1601: detected macro override of ref/source/config in the scope of {self.path}"


@dataclass
class StaticParserSuccess(DebugLevel, pt.StaticParserSuccess):
    def code(self):
        return "I036"

    def message(self) -> str:
        return f"1699: static parser successfully parsed {self.path}"


@dataclass
class StaticParserFailure(DebugLevel, pt.StaticParserFailure):
    def code(self):
        return "I037"

    def message(self) -> str:
        return f"1603: static parser failed on {self.path}"


@dataclass
class ExperimentalParserSuccess(DebugLevel, pt.ExperimentalParserSuccess):
    def code(self):
        return "I038"

    def message(self) -> str:
        return f"1698: experimental parser successfully parsed {self.path}"


@dataclass
class ExperimentalParserFailure(DebugLevel, pt.ExperimentalParserFailure):
    def code(self):
        return "I039"

    def message(self) -> str:
        return f"1604: experimental parser failed on {self.path}"


@dataclass
class PartialParsingEnabled(DebugLevel, pt.PartialParsingEnabled):
    def code(self):
        return "I040"

    def message(self) -> str:
        return (
            f"Partial parsing enabled: "
            f"{self.deleted} files deleted, "
            f"{self.added} files added, "
            f"{self.changed} files changed."
        )


@dataclass
class PartialParsingFile(DebugLevel, pt.PartialParsingFile):
    def code(self):
        return "I041"

    def message(self) -> str:
        return f"Partial parsing: {self.operation} file: {self.file_id}"


# Skipped I042, I043, I044, I045, I046, I047, I048, I049


@dataclass
class InvalidDisabledTargetInTestNode(DebugLevel, pt.InvalidDisabledTargetInTestNode):
    def code(self):
        return "I050"

    def message(self) -> str:
        target_package_string = ""

        if self.target_package != target_package_string:
            target_package_string = f"in package '{self.target_package}' "

        msg = (
            f"{self.resource_type_title} '{self.unique_id}' "
            f"({self.original_file_path}) depends on a {self.target_kind} "
            f"named '{self.target_name}' {target_package_string}which is disabled"
        )

        return warning_tag(msg)


@dataclass
class UnusedResourceConfigPath(WarnLevel, pt.UnusedResourceConfigPath):
    def code(self):
        return "I051"

    def message(self) -> str:
        path_list = "\n".join(f"- {u}" for u in self.unused_config_paths)
        msg = (
            "Configuration paths exist in your dbt_project.yml file which do not "
            "apply to any resources.\n"
            f"There are {len(self.unused_config_paths)} unused configuration paths:\n{path_list}"
        )
        return warning_tag(msg)


@dataclass
class SeedIncreased(WarnLevel, pt.SeedIncreased):
    def code(self):
        return "I052"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file was "
            f"<={MAXIMUM_SEED_SIZE_NAME}, so it has changed"
        )
        return msg


@dataclass
class SeedExceedsLimitSamePath(WarnLevel, pt.SeedExceedsLimitSamePath):
    def code(self):
        return "I053"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size at the same path, dbt "
            f"cannot tell if it has changed: assuming they are the same"
        )
        return msg


@dataclass
class SeedExceedsLimitAndPathChanged(WarnLevel, pt.SeedExceedsLimitAndPathChanged):
    def code(self):
        return "I054"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file was in "
            f"a different location, assuming it has changed"
        )
        return msg


@dataclass
class SeedExceedsLimitChecksumChanged(WarnLevel, pt.SeedExceedsLimitChecksumChanged):
    def code(self):
        return "I055"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file had a "
            f"checksum type of {self.checksum_name}, so it has changed"
        )
        return msg


@dataclass
class UnusedTables(WarnLevel, pt.UnusedTables):
    def code(self):
        return "I056"

    def message(self) -> str:
        msg = [
            "During parsing, dbt encountered source overrides that had no target:",
        ]
        msg += self.unused_tables
        msg.append("")
        return warning_tag("\n".join(msg))


@dataclass
class WrongResourceSchemaFile(WarnLevel, pt.WrongResourceSchemaFile):
    def code(self):
        return "I057"

    def message(self) -> str:
        msg = line_wrap_message(
            f"""\
            '{self.patch_name}' is a {self.resource_type} node, but it is
            specified in the {self.yaml_key} section of
            {self.file_path}.
            To fix this error, place the `{self.patch_name}`
            specification under the {self.plural_resource_type} key instead.
            """
        )
        return warning_tag(msg)


@dataclass
class NoNodeForYamlKey(WarnLevel, pt.NoNodeForYamlKey):
    def code(self):
        return "I058"

    def message(self) -> str:
        msg = (
            f"Did not find matching node for patch with name '{self.patch_name}' "
            f"in the '{self.yaml_key}' section of "
            f"file '{self.file_path}'"
        )
        return warning_tag(msg)


@dataclass
class MacroNotFoundForPatch(WarnLevel, pt.MacroNotFoundForPatch):
    def code(self):
        return "I059"

    def message(self) -> str:
        msg = f'Found patch for macro "{self.patch_name}" which was not found'
        return warning_tag(msg)


@dataclass
class NodeNotFoundOrDisabled(WarnLevel, pt.NodeNotFoundOrDisabled):
    def code(self):
        return "I060"

    def message(self) -> str:
        # this is duplicated logic from exceptions.get_not_found_or_disabled_msg
        # when we convert exceptions to be stuctured maybe it can be combined?
        # convverting the bool to a string since None is also valid
        if self.disabled == "None":
            reason = "was not found or is disabled"
        elif self.disabled == "True":
            reason = "is disabled"
        else:
            reason = "was not found"

        target_package_string = ""

        if self.target_package is not None:
            target_package_string = f"in package '{self.target_package}' "

        msg = (
            f"{self.resource_type_title} '{self.unique_id}' "
            f"({self.original_file_path}) depends on a {self.target_kind} "
            f"named '{self.target_name}' {target_package_string}which {reason}"
        )

        return warning_tag(msg)


@dataclass
class JinjaLogWarning(WarnLevel, pt.JinjaLogWarning):
    def code(self):
        return "I061"

    def message(self) -> str:
        return self.msg


# =======================================================
# M - Deps generation
# =======================================================


@dataclass
class GitSparseCheckoutSubdirectory(DebugLevel, pt.GitSparseCheckoutSubdirectory):
    def code(self):
        return "M001"

    def message(self) -> str:
        return f"  Subdirectory specified: {self.subdir}, using sparse checkout."


@dataclass
class GitProgressCheckoutRevision(DebugLevel, pt.GitProgressCheckoutRevision):
    def code(self):
        return "M002"

    def message(self) -> str:
        return f"  Checking out revision {self.revision}."


@dataclass
class GitProgressUpdatingExistingDependency(DebugLevel, pt.GitProgressUpdatingExistingDependency):
    def code(self):
        return "M003"

    def message(self) -> str:
        return f"Updating existing dependency {self.dir}."


@dataclass
class GitProgressPullingNewDependency(DebugLevel, pt.GitProgressPullingNewDependency):
    def code(self):
        return "M004"

    def message(self) -> str:
        return f"Pulling new dependency {self.dir}."


@dataclass
class GitNothingToDo(DebugLevel, pt.GitNothingToDo):
    def code(self):
        return "M005"

    def message(self) -> str:
        return f"Already at {self.sha}, nothing to do."


@dataclass
class GitProgressUpdatedCheckoutRange(DebugLevel, pt.GitProgressUpdatedCheckoutRange):
    def code(self):
        return "M006"

    def message(self) -> str:
        return f"  Updated checkout from {self.start_sha} to {self.end_sha}."


@dataclass
class GitProgressCheckedOutAt(DebugLevel, pt.GitProgressCheckedOutAt):
    def code(self):
        return "M007"

    def message(self) -> str:
        return f"  Checked out at {self.end_sha}."


@dataclass
class RegistryProgressGETRequest(DebugLevel, pt.RegistryProgressGETRequest):
    def code(self):
        return "M008"

    def message(self) -> str:
        return f"Making package registry request: GET {self.url}"


@dataclass
class RegistryProgressGETResponse(DebugLevel, pt.RegistryProgressGETResponse):
    def code(self):
        return "M009"

    def message(self) -> str:
        return f"Response from registry: GET {self.url} {self.resp_code}"


@dataclass
class SelectorReportInvalidSelector(InfoLevel, pt.SelectorReportInvalidSelector):
    def code(self):
        return "M010"

    def message(self) -> str:
        return (
            f"The '{self.spec_method}' selector specified in {self.raw_spec} is "
            f"invalid. Must be one of [{self.valid_selectors}]"
        )


@dataclass
class JinjaLogInfo(InfoLevel, EventStringFunctor, pt.JinjaLogInfo):
    def code(self):
        return "M011"

    def message(self) -> str:
        # This is for the log method used in macros so msg cannot be built here
        return self.msg


@dataclass
class JinjaLogDebug(DebugLevel, EventStringFunctor, pt.JinjaLogDebug):
    def code(self):
        return "M012"

    def message(self) -> str:
        # This is for the log method used in macros so msg cannot be built here
        return self.msg


@dataclass
class DepsNoPackagesFound(InfoLevel, pt.DepsNoPackagesFound):
    def code(self):
        return "M013"

    def message(self) -> str:
        return "Warning: No packages were found in packages.yml"


@dataclass
class DepsStartPackageInstall(InfoLevel, pt.DepsStartPackageInstall):
    def code(self):
        return "M014"

    def message(self) -> str:
        return f"Installing {self.package_name}"


@dataclass
class DepsInstallInfo(InfoLevel, pt.DepsInstallInfo):
    def code(self):
        return "M015"

    def message(self) -> str:
        return f"  Installed from {self.version_name}"


@dataclass
class DepsUpdateAvailable(InfoLevel, pt.DepsUpdateAvailable):
    def code(self):
        return "M016"

    def message(self) -> str:
        return f"  Updated version available: {self.version_latest}"


@dataclass
class DepsUpToDate(InfoLevel, pt.DepsUpToDate):
    def code(self):
        return "M017"

    def message(self) -> str:
        return "  Up to date!"


@dataclass
class DepsListSubdirectory(InfoLevel, pt.DepsListSubdirectory):
    def code(self):
        return "M018"

    def message(self) -> str:
        return f"   and subdirectory {self.subdirectory}"


@dataclass
class DepsNotifyUpdatesAvailable(InfoLevel, pt.DepsNotifyUpdatesAvailable):
    def code(self):
        return "M019"

    def message(self) -> str:
        return f"Updates available for packages: {self.packages.value} \
                \nUpdate your versions in packages.yml, then run dbt deps"


@dataclass
class RetryExternalCall(DebugLevel, pt.RetryExternalCall):
    def code(self):
        return "M020"

    def message(self) -> str:
        return f"Retrying external call. Attempt: {self.attempt} Max attempts: {self.max}"


@dataclass
class RecordRetryException(DebugLevel, pt.RecordRetryException):
    def code(self):
        return "M021"

    def message(self) -> str:
        return f"External call exception: {self.exc}"


@dataclass
class RegistryIndexProgressGETRequest(DebugLevel, pt.RegistryIndexProgressGETRequest):
    def code(self):
        return "M022"

    def message(self) -> str:
        return f"Making package index registry request: GET {self.url}"


@dataclass
class RegistryIndexProgressGETResponse(DebugLevel, pt.RegistryIndexProgressGETResponse):
    def code(self):
        return "M023"

    def message(self) -> str:
        return f"Response from registry index: GET {self.url} {self.resp_code}"


@dataclass
class RegistryResponseUnexpectedType(DebugLevel, pt.RegistryResponseUnexpectedType):
    def code(self):
        return "M024"

    def message(self) -> str:
        return f"Response was None: {self.response}"


@dataclass
class RegistryResponseMissingTopKeys(DebugLevel, pt.RegistryResponseMissingTopKeys):
    def code(self):
        return "M025"

    def message(self) -> str:
        # expected/actual keys logged in exception
        return f"Response missing top level keys: {self.response}"


@dataclass
class RegistryResponseMissingNestedKeys(DebugLevel, pt.RegistryResponseMissingNestedKeys):
    def code(self):
        return "M026"

    def message(self) -> str:
        # expected/actual keys logged in exception
        return f"Response missing nested keys: {self.response}"


@dataclass
class RegistryResponseExtraNestedKeys(DebugLevel, pt.RegistryResponseExtraNestedKeys):
    def code(self):
        return "M027"

    def message(self) -> str:
        # expected/actual keys logged in exception
        return f"Response contained inconsistent keys: {self.response}"


@dataclass
class DepsSetDownloadDirectory(DebugLevel, pt.DepsSetDownloadDirectory):
    def code(self):
        return "M028"

    def message(self) -> str:
        return f"Set downloads directory='{self.path}'"


@dataclass
class DepsUnpinned(WarnLevel, pt.DepsUnpinned):
    def code(self):
        return "M029"

    def message(self) -> str:
        if self.revision == "HEAD":
            unpinned_msg = "not pinned, using HEAD (default branch)"
        elif self.revision in ("main", "master"):
            unpinned_msg = f'pinned to the "{self.revision}" branch'
        else:
            unpinned_msg = None

        msg = (
            f'The git package "{self.git}" \n\tis {unpinned_msg}.\n\tThis can introduce '
            f"breaking changes into your project without warning!\n\nSee {PIN_PACKAGE_URL}"
        )
        return yellow(f"WARNING: {msg}")


@dataclass
class NoNodesForSelectionCriteria(WarnLevel, pt.NoNodesForSelectionCriteria):
    def code(self):
        return "M030"

    def message(self) -> str:
        return f"The selection criterion '{self.spec_raw}' does not match any nodes"


# =======================================================
# Q - Node execution
# =======================================================


@dataclass
class RunningOperationCaughtError(ErrorLevel, pt.RunningOperationCaughtError):
    def code(self):
        return "Q001"

    def message(self) -> str:
        return f"Encountered an error while running operation: {self.exc}"


@dataclass
class CompileComplete(InfoLevel, pt.CompileComplete):
    def code(self):
        return "Q002"

    def message(self) -> str:
        return "Done."


@dataclass
class FreshnessCheckComplete(InfoLevel, pt.FreshnessCheckComplete):
    def code(self):
        return "Q003"

    def message(self) -> str:
        return "Done."


@dataclass
class SeedHeader(InfoLevel, pt.SeedHeader):
    def code(self):
        return "Q004"

    def message(self) -> str:
        return self.header


@dataclass
class SeedHeaderSeparator(InfoLevel, pt.SeedHeaderSeparator):
    def code(self):
        return "Q005"

    def message(self) -> str:
        return "-" * self.len_header


@dataclass
class SQLRunnerException(DebugLevel, pt.SQLRunnerException):  # noqa
    def code(self):
        return "Q006"

    def message(self) -> str:
        return f"Got an exception: {self.exc}"


@dataclass
class LogTestResult(DynamicLevel, pt.LogTestResult):
    def code(self):
        return "Q007"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR"
            status = red(info)
        elif self.status == "pass":
            info = "PASS"
            status = green(info)
        elif self.status == "warn":
            info = f"WARN {self.num_failures}"
            status = yellow(info)
        else:  # self.status == "fail":
            info = f"FAIL {self.num_failures}"
            status = red(info)
        msg = f"{info} {self.name}"

        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.num_models,
            execution_time=self.execution_time,
        )

    @classmethod
    def status_to_level(cls, status):
        # The statuses come from TestStatus
        level_lookup = {
            "fail": EventLevel.ERROR,
            "pass": EventLevel.INFO,
            "warn": EventLevel.WARN,
            "error": EventLevel.ERROR,
        }
        if status in level_lookup:
            return level_lookup[status]
        else:
            return EventLevel.INFO


# Skipped Q008, Q009, Q010


@dataclass
class LogStartLine(InfoLevel, pt.LogStartLine):  # noqa
    def code(self):
        return "Q011"

    def message(self) -> str:
        msg = f"START {self.description}"
        return format_fancy_output_line(msg=msg, status="RUN", index=self.index, total=self.total)


@dataclass
class LogModelResult(DynamicLevel, pt.LogModelResult):
    def code(self):
        return "Q012"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR creating"
            status = red(self.status.upper())
        else:
            info = "OK created"
            status = green(self.status)

        msg = f"{info} {self.description}"
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )


# Skipped Q013, Q014


@dataclass
class LogSnapshotResult(DynamicLevel, pt.LogSnapshotResult):
    def code(self):
        return "Q015"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR snapshotting"
            status = red(self.status.upper())
        else:
            info = "OK snapshotted"
            status = green(self.status)

        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )


@dataclass
class LogSeedResult(DynamicLevel, pt.LogSeedResult):
    def code(self):
        return "Q016"

    def message(self) -> str:
        if self.status == "error":
            info = "ERROR loading"
            status = red(self.status.upper())
        else:
            info = "OK loaded"
            status = green(self.result_message)
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )


# Skipped Q017


@dataclass
class LogFreshnessResult(DynamicLevel, pt.LogFreshnessResult):
    def code(self):
        return "Q018"

    def message(self) -> str:
        if self.status == "runtime error":
            info = "ERROR"
            status = red(info)
        elif self.status == "error":
            info = "ERROR STALE"
            status = red(info)
        elif self.status == "warn":
            info = "WARN"
            status = yellow(info)
        else:
            info = "PASS"
            status = green(info)
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(
            msg=msg,
            status=status,
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
        )

    @classmethod
    def status_to_level(cls, status):
        # The statuses come from FreshnessStatus
        # TODO should this return EventLevel enum instead?
        level_lookup = {
            "runtime error": EventLevel.ERROR,
            "pass": EventLevel.INFO,
            "warn": EventLevel.WARN,
            "error": EventLevel.ERROR,
        }
        if status in level_lookup:
            return level_lookup[status]
        else:
            return EventLevel.INFO


# Skipped Q019, Q020, Q021


@dataclass
class LogCancelLine(ErrorLevel, pt.LogCancelLine):
    def code(self):
        return "Q022"

    def message(self) -> str:
        msg = f"CANCEL query {self.conn_name}"
        return format_fancy_output_line(msg=msg, status=red("CANCEL"), index=None, total=None)


@dataclass
class DefaultSelector(InfoLevel, pt.DefaultSelector):
    def code(self):
        return "Q023"

    def message(self) -> str:
        return f"Using default selector {self.name}"


@dataclass
class NodeStart(DebugLevel, pt.NodeStart):
    def code(self):
        return "Q024"

    def message(self) -> str:
        return f"Began running node {self.node_info.unique_id}"


@dataclass
class NodeFinished(DebugLevel, pt.NodeFinished):
    def code(self):
        return "Q025"

    def message(self) -> str:
        return f"Finished running node {self.node_info.unique_id}"


@dataclass
class QueryCancelationUnsupported(InfoLevel, pt.QueryCancelationUnsupported):
    def code(self):
        return "Q026"

    def message(self) -> str:
        msg = (
            f"The {self.type} adapter does not support query "
            "cancellation. Some queries may still be "
            "running!"
        )
        return yellow(msg)


@dataclass
class ConcurrencyLine(InfoLevel, pt.ConcurrencyLine):  # noqa
    def code(self):
        return "Q027"

    def message(self) -> str:
        return f"Concurrency: {self.num_threads} threads (target='{self.target_name}')"


# Skipped Q028


@dataclass
class WritingInjectedSQLForNode(DebugLevel, pt.WritingInjectedSQLForNode):
    def code(self):
        return "Q029"

    def message(self) -> str:
        return f'Writing injected SQL for node "{self.node_info.unique_id}"'


@dataclass
class NodeCompiling(DebugLevel, pt.NodeCompiling):
    def code(self):
        return "Q030"

    def message(self) -> str:
        return f"Began compiling node {self.node_info.unique_id}"


@dataclass
class NodeExecuting(DebugLevel, pt.NodeExecuting):
    def code(self):
        return "Q031"

    def message(self) -> str:
        return f"Began executing node {self.node_info.unique_id}"


@dataclass
class LogHookStartLine(InfoLevel, pt.LogHookStartLine):  # noqa
    def code(self):
        return "Q032"

    def message(self) -> str:
        msg = f"START hook: {self.statement}"
        return format_fancy_output_line(
            msg=msg, status="RUN", index=self.index, total=self.total, truncate=True
        )


@dataclass
class LogHookEndLine(InfoLevel, pt.LogHookEndLine):  # noqa
    def code(self):
        return "Q033"

    def message(self) -> str:
        msg = f"OK hook: {self.statement}"
        return format_fancy_output_line(
            msg=msg,
            status=green(self.status),
            index=self.index,
            total=self.total,
            execution_time=self.execution_time,
            truncate=True,
        )


@dataclass
class SkippingDetails(InfoLevel, pt.SkippingDetails):
    def code(self):
        return "Q034"

    def message(self) -> str:
        if self.resource_type in NodeType.refable():
            msg = f"SKIP relation {self.schema}.{self.node_name}"
        else:
            msg = f"SKIP {self.resource_type} {self.node_name}"
        return format_fancy_output_line(
            msg=msg, status=yellow("SKIP"), index=self.index, total=self.total
        )


@dataclass
class NothingToDo(WarnLevel, pt.NothingToDo):
    def code(self):
        return "Q035"

    def message(self) -> str:
        return "Nothing to do. Try checking your model configs and model specification args"


@dataclass
class RunningOperationUncaughtError(ErrorLevel, pt.RunningOperationUncaughtError):
    def code(self):
        return "Q036"

    def message(self) -> str:
        return f"Encountered an error while running operation: {self.exc}"


@dataclass
class EndRunResult(DebugLevel, pt.EndRunResult):
    def code(self):
        return "Q037"

    def message(self) -> str:
        return "Command end result"


@dataclass
class NoNodesSelected(WarnLevel, pt.NoNodesSelected):
    def code(self):
        return "Q038"

    def message(self) -> str:
        return "No nodes selected!"


# =======================================================
# W - Node testing
# =======================================================

# Skipped W001


@dataclass
class CatchableExceptionOnRun(DebugLevel, pt.CatchableExceptionOnRun):  # noqa
    def code(self):
        return "W002"

    def message(self) -> str:
        return str(self.exc)


@dataclass
class InternalErrorOnRun(DebugLevel, pt.InternalErrorOnRun):
    def code(self):
        return "W003"

    def message(self) -> str:
        prefix = f"Internal error executing {self.build_path}"

        internal_error_string = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/dbt-labs/dbt-core
""".strip()

        return f"{red(prefix)}\n" f"{str(self.exc).strip()}\n\n" f"{internal_error_string}"


@dataclass
class GenericExceptionOnRun(ErrorLevel, pt.GenericExceptionOnRun):
    def code(self):
        return "W004"

    def message(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = f"Unhandled error while executing {node_description}"
        return f"{red(prefix)}\n{str(self.exc).strip()}"


@dataclass
class NodeConnectionReleaseError(DebugLevel, pt.NodeConnectionReleaseError):  # noqa
    def code(self):
        return "W005"

    def message(self) -> str:
        return f"Error releasing connection for node {self.node_name}: {str(self.exc)}"


@dataclass
class FoundStats(InfoLevel, pt.FoundStats):
    def code(self):
        return "W006"

    def message(self) -> str:
        return f"Found {self.stat_line}"


# =======================================================
# Z - Misc
# =======================================================


@dataclass
class MainKeyboardInterrupt(InfoLevel, pt.MainKeyboardInterrupt):
    def code(self):
        return "Z001"

    def message(self) -> str:
        return "ctrl-c"


@dataclass
class MainEncounteredError(ErrorLevel, pt.MainEncounteredError):  # noqa
    def code(self):
        return "Z002"

    def message(self) -> str:
        return f"Encountered an error:\n{self.exc}"


@dataclass
class MainStackTrace(ErrorLevel, pt.MainStackTrace):
    def code(self):
        return "Z003"

    def message(self) -> str:
        return self.stack_trace


@dataclass
class SystemErrorRetrievingModTime(ErrorLevel, pt.SystemErrorRetrievingModTime):
    def code(self):
        return "Z004"

    def message(self) -> str:
        return f"Error retrieving modification time for file {self.path}"


@dataclass
class SystemCouldNotWrite(DebugLevel, pt.SystemCouldNotWrite):
    def code(self):
        return "Z005"

    def message(self) -> str:
        return (
            f"Could not write to path {self.path}({len(self.path)} characters): "
            f"{self.reason}\nexception: {self.exc}"
        )


@dataclass
class SystemExecutingCmd(DebugLevel, pt.SystemExecutingCmd):
    def code(self):
        return "Z006"

    def message(self) -> str:
        return f'Executing "{" ".join(self.cmd)}"'


@dataclass
class SystemStdOut(DebugLevel, pt.SystemStdOut):
    def code(self):
        return "Z007"

    def message(self) -> str:
        return f'STDOUT: "{str(self.bmsg)}"'


@dataclass
class SystemStdErr(DebugLevel, pt.SystemStdErr):
    def code(self):
        return "Z008"

    def message(self) -> str:
        return f'STDERR: "{str(self.bmsg)}"'


@dataclass
class SystemReportReturnCode(DebugLevel, pt.SystemReportReturnCode):
    def code(self):
        return "Z009"

    def message(self) -> str:
        return f"command return code={self.returncode}"


@dataclass
class TimingInfoCollected(DebugLevel, pt.TimingInfoCollected):
    def code(self):
        return "Z010"

    def message(self) -> str:
        return f"Timing info for {self.node_info.unique_id} ({self.timing_info.name}): {self.timing_info.started_at} => {self.timing_info.completed_at}"


# This prints the stack trace at the debug level while allowing just the nice exception message
# at the error level - or whatever other level chosen.  Used in multiple places.
@dataclass
class LogDebugStackTrace(DebugLevel, pt.LogDebugStackTrace):  # noqa
    def code(self):
        return "Z011"

    def message(self) -> str:
        return f"{self.exc_info}"


# We don't write "clean" events to the log, because the clean command
# may have removed the log directory.
@dataclass
class CheckCleanPath(InfoLevel, NoFile, pt.CheckCleanPath):
    def code(self):
        return "Z012"

    def message(self) -> str:
        return f"Checking {self.path}/*"


@dataclass
class ConfirmCleanPath(InfoLevel, NoFile, pt.ConfirmCleanPath):
    def code(self):
        return "Z013"

    def message(self) -> str:
        return f"Cleaned {self.path}/*"


@dataclass
class ProtectedCleanPath(InfoLevel, NoFile, pt.ProtectedCleanPath):
    def code(self):
        return "Z014"

    def message(self) -> str:
        return f"ERROR: not cleaning {self.path}/* because it is protected"


@dataclass
class FinishedCleanPaths(InfoLevel, NoFile, pt.FinishedCleanPaths):
    def code(self):
        return "Z015"

    def message(self) -> str:
        return "Finished cleaning all paths."


@dataclass
class OpenCommand(InfoLevel, pt.OpenCommand):
    def code(self):
        return "Z016"

    def message(self) -> str:
        msg = f"""To view your profiles.yml file, run:

{self.open_cmd} {self.profiles_dir}"""

        return msg


@dataclass
class EmptyLine(InfoLevel, pt.EmptyLine):
    def code(self):
        return "Z017"

    def message(self) -> str:
        return ""


@dataclass
class ServingDocsPort(InfoLevel, pt.ServingDocsPort):
    def code(self):
        return "Z018"

    def message(self) -> str:
        return f"Serving docs at {self.address}:{self.port}"


@dataclass
class ServingDocsAccessInfo(InfoLevel, pt.ServingDocsAccessInfo):
    def code(self):
        return "Z019"

    def message(self) -> str:
        return f"To access from your browser, navigate to:  http://localhost:{self.port}"


@dataclass
class ServingDocsExitInfo(InfoLevel, pt.ServingDocsExitInfo):
    def code(self):
        return "Z020"

    def message(self) -> str:
        return "Press Ctrl+C to exit."


@dataclass
class RunResultWarning(WarnLevel, pt.RunResultWarning):
    def code(self):
        return "Z021"

    def message(self) -> str:
        info = "Warning"
        return yellow(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class RunResultFailure(ErrorLevel, pt.RunResultFailure):
    def code(self):
        return "Z022"

    def message(self) -> str:
        info = "Failure"
        return red(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class StatsLine(InfoLevel, pt.StatsLine):
    def code(self):
        return "Z023"

    def message(self) -> str:
        stats_line = "Done. PASS={pass} WARN={warn} ERROR={error} SKIP={skip} TOTAL={total}"
        return stats_line.format(**self.stats)


@dataclass
class RunResultError(ErrorLevel, EventStringFunctor, pt.RunResultError):
    def code(self):
        return "Z024"

    def message(self) -> str:
        # This is the message on the result object, cannot be built here
        return f"  {self.msg}"


@dataclass
class RunResultErrorNoMessage(ErrorLevel, pt.RunResultErrorNoMessage):
    def code(self):
        return "Z025"

    def message(self) -> str:
        return f"  Status: {self.status}"


@dataclass
class SQLCompiledPath(InfoLevel, pt.SQLCompiledPath):
    def code(self):
        return "Z026"

    def message(self) -> str:
        return f"  compiled Code at {self.path}"


@dataclass
class CheckNodeTestFailure(InfoLevel, pt.CheckNodeTestFailure):
    def code(self):
        return "Z027"

    def message(self) -> str:
        msg = f"select * from {self.relation_name}"
        border = "-" * len(msg)
        return f"  See test failures:\n  {border}\n  {msg}\n  {border}"


# FirstRunResultError and AfterFirstRunResultError are just splitting the message from the result
#  object into multiple log lines
# TODO: is this reallly needed?  See printer.py
@dataclass
class FirstRunResultError(ErrorLevel, EventStringFunctor, pt.FirstRunResultError):
    def code(self):
        return "Z028"

    def message(self) -> str:
        return yellow(self.msg)


@dataclass
class AfterFirstRunResultError(ErrorLevel, EventStringFunctor, pt.AfterFirstRunResultError):
    def code(self):
        return "Z029"

    def message(self) -> str:
        return self.msg


@dataclass
class EndOfRunSummary(InfoLevel, pt.EndOfRunSummary):
    def code(self):
        return "Z030"

    def message(self) -> str:
        error_plural = pluralize(self.num_errors, "error")
        warn_plural = pluralize(self.num_warnings, "warning")
        if self.keyboard_interrupt:
            message = yellow("Exited because of keyboard interrupt.")
        elif self.num_errors > 0:
            message = red(f"Completed with {error_plural} and {warn_plural}:")
        elif self.num_warnings > 0:
            message = yellow(f"Completed with {warn_plural}:")
        else:
            message = green("Completed successfully")
        return message


# Skipped Z031, Z032, Z033


@dataclass
class LogSkipBecauseError(ErrorLevel, pt.LogSkipBecauseError):
    def code(self):
        return "Z034"

    def message(self) -> str:
        msg = f"SKIP relation {self.schema}.{self.relation} due to ephemeral model error"
        return format_fancy_output_line(
            msg=msg, status=red("ERROR SKIP"), index=self.index, total=self.total
        )


# Skipped Z035


@dataclass
class EnsureGitInstalled(ErrorLevel, pt.EnsureGitInstalled):
    def code(self):
        return "Z036"

    def message(self) -> str:
        return (
            "Make sure git is installed on your machine. More "
            "information: "
            "https://docs.getdbt.com/docs/package-management"
        )


@dataclass
class DepsCreatingLocalSymlink(DebugLevel, pt.DepsCreatingLocalSymlink):
    def code(self):
        return "Z037"

    def message(self) -> str:
        return "  Creating symlink to local dependency."


@dataclass
class DepsSymlinkNotAvailable(DebugLevel, pt.DepsSymlinkNotAvailable):
    def code(self):
        return "Z038"

    def message(self) -> str:
        return "  Symlinks are not available on this OS, copying dependency."


@dataclass
class DisableTracking(DebugLevel, pt.DisableTracking):
    def code(self):
        return "Z039"

    def message(self) -> str:
        return (
            "Error sending anonymous usage statistics. Disabling tracking for this execution. "
            "If you wish to permanently disable tracking, see: "
            "https://docs.getdbt.com/reference/global-configs#send-anonymous-usage-stats."
        )


@dataclass
class SendingEvent(DebugLevel, pt.SendingEvent):
    def code(self):
        return "Z040"

    def message(self) -> str:
        return f"Sending event: {self.kwargs}"


@dataclass
class SendEventFailure(DebugLevel, pt.SendEventFailure):
    def code(self):
        return "Z041"

    def message(self) -> str:
        return "An error was encountered while trying to send an event"


@dataclass
class FlushEvents(DebugLevel, pt.FlushEvents):
    def code(self):
        return "Z042"

    def message(self) -> str:
        return "Flushing usage events"


@dataclass
class FlushEventsFailure(DebugLevel, pt.FlushEventsFailure):
    def code(self):
        return "Z043"

    def message(self) -> str:
        return "An error was encountered while trying to flush usage events"


@dataclass
class TrackingInitializeFailure(DebugLevel, pt.TrackingInitializeFailure):  # noqa
    def code(self):
        return "Z044"

    def message(self) -> str:
        return "Got an exception trying to initialize tracking"


# this is the message from the result object
@dataclass
class RunResultWarningMessage(WarnLevel, EventStringFunctor, pt.RunResultWarningMessage):
    def code(self):
        return "Z046"

    def message(self) -> str:
        # This is the message on the result object, cannot be formatted in event
        return self.msg
