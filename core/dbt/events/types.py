from abc import ABCMeta, abstractmethod
import argparse
from dataclasses import dataclass
from typing import Any, List, Optional, Dict
from dbt import ui
from dbt.node_types import NodeType
from dbt.events.format import format_fancy_output_line, pluralize


# types to represent log levels

# in preparation for #3977
class TestLevel():
    def level_tag(self) -> str:
        return "test"


class DebugLevel():
    def level_tag(self) -> str:
        return "debug"


class InfoLevel():
    def level_tag(self) -> str:
        return "info"


class WarnLevel():
    def level_tag(self) -> str:
        return "warn"


class ErrorLevel():
    def level_tag(self) -> str:
        return "error"


@dataclass
class ShowException():
    # N.B.:
    # As long as we stick with the current convention of setting the member vars in the
    # `cli_msg` method of subclasses, this is a safe operation.
    # If that ever changes we'll want to reassess.
    def __post_init__(self):
        self.exc_info: Any = True
        self.stack_info: Any = None
        self.extra: Any = None


# The following classes represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. classes extend superclasses that indicate what
# destinations they are intended for, which mypy uses to enforce
# that the necessary methods are defined.


# top-level superclass for all events
class Event(metaclass=ABCMeta):
    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for event")


class CliEventABC(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    @abstractmethod
    def cli_msg(self) -> str:
        raise Exception("cli_msg not implemented for cli event")


class MainKeyboardInterrupt(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "ctrl-c"


@dataclass
class MainEncounteredError(ErrorLevel, CliEventABC):
    e: BaseException

    def cli_msg(self) -> str:
        return f"Encountered an error:\n{str(self.e)}"


@dataclass
class MainStackTrace(DebugLevel, CliEventABC):
    stack_trace: str

    def cli_msg(self) -> str:
        return self.stack_trace


@dataclass
class MainReportVersion(InfoLevel, CliEventABC):
    v: str  # could be VersionSpecifier instead if we resolved some circular imports

    def cli_msg(self):
        return f"Running with dbt{self.v}"


@dataclass
class MainReportArgs(DebugLevel, CliEventABC):
    args: argparse.Namespace

    def cli_msg(self):
        return f"running dbt with arguments {str(self.args)}"


@dataclass
class MainTrackingUserState(DebugLevel, CliEventABC):
    user_state: str

    def cli_msg(self):
        return f"Tracking: {self.user_state}"


class ParsingStart(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Start parsing."


class ParsingCompiling(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Compiling."


class ParsingWritingManifest(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Writing manifest."


class ParsingDone(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Done."


class ManifestDependenciesLoaded(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Dependencies loaded"


class ManifestLoaderCreated(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "ManifestLoader created"


class ManifestLoaded(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Manifest loaded"


class ManifestChecked(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Manifest checked"


class ManifestFlatGraphBuilt(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Flat graph built"


@dataclass
class ReportPerformancePath(InfoLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Performance info: {self.path}"


@dataclass
class GitSparseCheckoutSubdirectory(DebugLevel, CliEventABC):
    subdir: str

    def cli_msg(self) -> str:
        return f"  Subdirectory specified: {self.subdir}, using sparse checkout."


@dataclass
class GitProgressCheckoutRevision(DebugLevel, CliEventABC):
    revision: str

    def cli_msg(self) -> str:
        return f"  Checking out revision {self.revision}."


@dataclass
class GitProgressUpdatingExistingDependency(DebugLevel, CliEventABC):
    dir: str

    def cli_msg(self) -> str:
        return f"Updating existing dependency {self.dir}."


@dataclass
class GitProgressPullingNewDependency(DebugLevel, CliEventABC):
    dir: str

    def cli_msg(self) -> str:
        return f"Pulling new dependency {self.dir}."


@dataclass
class GitNothingToDo(DebugLevel, CliEventABC):
    sha: str

    def cli_msg(self) -> str:
        return f"Already at {self.sha}, nothing to do."


@dataclass
class GitProgressUpdatedCheckoutRange(DebugLevel, CliEventABC):
    start_sha: str
    end_sha: str

    def cli_msg(self) -> str:
        return f"  Updated checkout from {self.start_sha} to {self.end_sha}."


@dataclass
class GitProgressCheckedOutAt(DebugLevel, CliEventABC):
    end_sha: str

    def cli_msg(self) -> str:
        return f"  Checked out at {self.end_sha}."


@dataclass
class RegistryProgressMakingGETRequest(DebugLevel, CliEventABC):
    url: str

    def cli_msg(self) -> str:
        return f"Making package registry request: GET {self.url}"


@dataclass
class RegistryProgressGETResponse(DebugLevel, CliEventABC):
    url: str
    resp_code: int

    def cli_msg(self) -> str:
        return f"Response from registry: GET {self.url} {self.resp_code}"


# TODO this was actually `logger.exception(...)` not `logger.error(...)`
@dataclass
class SystemErrorRetrievingModTime(ErrorLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Error retrieving modification time for file {self.path}"


@dataclass
class SystemCouldNotWrite(DebugLevel, CliEventABC):
    path: str
    reason: str
    exc: Exception

    def cli_msg(self) -> str:
        return (
            f"Could not write to path {self.path}({len(self.path)} characters): "
            f"{self.reason}\nexception: {self.exc}"
        )


@dataclass
class SystemExecutingCmd(DebugLevel, CliEventABC):
    cmd: List[str]

    def cli_msg(self) -> str:
        return f'Executing "{" ".join(self.cmd)}"'


@dataclass
class SystemStdOutMsg(DebugLevel, CliEventABC):
    bmsg: bytes

    def cli_msg(self) -> str:
        return f'STDOUT: "{str(self.bmsg)}"'


@dataclass
class SystemStdErrMsg(DebugLevel, CliEventABC):
    bmsg: bytes

    def cli_msg(self) -> str:
        return f'STDERR: "{str(self.bmsg)}"'


@dataclass
class SystemReportReturnCode(DebugLevel, CliEventABC):
    code: int

    def cli_msg(self) -> str:
        return f"command return code={self.code}"


@dataclass
class SelectorAlertUpto3UnusedNodes(InfoLevel, CliEventABC):
    node_names: List[str]

    def cli_msg(self) -> str:
        summary_nodes_str = ("\n  - ").join(self.node_names[:3])
        and_more_str = (
            f"\n  - and {len(self.node_names) - 3} more" if len(self.node_names) > 4 else ""
        )
        return (
            f"\nSome tests were excluded because at least one parent is not selected. "
            f"Use the --greedy flag to include them."
            f"\n  - {summary_nodes_str}{and_more_str}"
        )


@dataclass
class SelectorAlertAllUnusedNodes(DebugLevel, CliEventABC):
    node_names: List[str]

    def cli_msg(self) -> str:
        debug_nodes_str = ("\n  - ").join(self.node_names)
        return (
            f"Full list of tests that were excluded:"
            f"\n  - {debug_nodes_str}"
        )


@dataclass
class SelectorReportInvalidSelector(InfoLevel, CliEventABC):
    selector_methods: dict
    spec_method: str
    raw_spec: str

    def cli_msg(self) -> str:
        valid_selectors = ", ".join(self.selector_methods)
        return (
            f"The '{self.spec_method}' selector specified in {self.raw_spec} is "
            f"invalid. Must be one of [{valid_selectors}]"
        )


@dataclass
class MacroEventInfo(InfoLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


@dataclass
class MacroEventDebug(DebugLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


@dataclass
class NewConnectionOpening(DebugLevel, CliEventABC):
    connection_state: str

    def cli_msg(self) -> str:
        return f"Opening a new connection, currently in state {self.connection_state}"


class TimingInfoCollected(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "finished collecting timing info"


@dataclass
class MergedFromState(DebugLevel, CliEventABC):
    nbr_merged: int
    sample: List

    def cli_msg(self) -> str:
        return f"Merged {self.nbr_merged} items from state (sample: {self.sample})"


@dataclass
class MissingProfileTarget(InfoLevel, CliEventABC):
    profile_name: str
    target_name: str

    def cli_msg(self) -> str:
        return f"target not specified in profile '{self.profile_name}', using '{self.target_name}'"


@dataclass
class ProfileLoadError(ShowException, DebugLevel, CliEventABC):
    exc: Exception

    def cli_msg(self) -> str:
        return f"Profile not loaded due to error: {self.exc}"


@dataclass
class ProfileNotFound(InfoLevel, CliEventABC):
    profile_name: Optional[str]

    def cli_msg(self) -> str:
        return f'No profile "{self.profile_name}" found, continuing with no target'


class InvalidVarsYAML(ErrorLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "The YAML provided in the --vars argument is not valid.\n"


@dataclass
class CatchRunException(ShowException, DebugLevel, CliEventABC):
    build_path: Any
    exc: Exception

    def cli_msg(self) -> str:
        INTERNAL_ERROR_STRING = """This is an error in dbt. Please try again. If the \
                            error persists, open an issue at https://github.com/dbt-labs/dbt-core
                            """.strip()
        prefix = f'Internal error executing {self.build_path}'
        error = "{prefix}\n{error}\n\n{note}".format(
                prefix=ui.red(prefix),
                error=str(self.exc).strip(),
                note=INTERNAL_ERROR_STRING
        )
        return error


@dataclass
class HandleInternalException(ShowException, DebugLevel, CliEventABC):
    exc: Exception

    def cli_msg(self) -> str:
        return str(self.exc)


@dataclass
class MessageHandleGenericException(ErrorLevel, CliEventABC):
    build_path: str
    unique_id: str
    exc: Exception

    def cli_msg(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = "Unhandled error while executing {}".format(node_description)
        return "{prefix}\n{error}".format(
            prefix=ui.red(prefix),
            error=str(self.exc).strip()
        )


@dataclass
class DetailsHandleGenericException(ShowException, DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ''


@dataclass
class GenericTestFileParse(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Parsing {self.path}"


@dataclass
class MacroFileParse(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Parsing {self.path}"


class PartialParsingFullReparseBecauseOfError(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Partial parsing enabled but an error occurred. Switching to a full re-parse."


@dataclass
class PartialParsingExceptionFile(DebugLevel, CliEventABC):
    file: str

    def cli_msg(self) -> str:
        return f"Partial parsing exception processing file {self.file}"


@dataclass
class PartialParsingFile(DebugLevel, CliEventABC):
    file_dict: Dict

    def cli_msg(self) -> str:
        return f"PP file: {self.file_dict}"


@dataclass
class PartialParsingException(DebugLevel, CliEventABC):
    exc_info: Dict

    def cli_msg(self) -> str:
        return f"PP exception info: {self.exc_info}"


class PartialParsingSkipParsing(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Partial parsing enabled, no changes found, skipping parsing"


class PartialParsingMacroChangeStartFullParse(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Change detected to override macro used during parsing. Starting full parse."


@dataclass
class ManifestWrongMetadataVersion(DebugLevel, CliEventABC):
    version: str

    def cli_msg(self) -> str:
        return ("Manifest metadata did not contain correct version. "
                f"Contained '{self.version}' instead.")


@dataclass
class PartialParsingVersionMismatch(InfoLevel, CliEventABC):
    saved_version: str
    current_version: str

    def cli_msg(self) -> str:
        return ("Unable to do partial parsing because of a dbt version mismatch. "
                f"Saved manifest version: {self.saved_version}. "
                f"Current version: {self.current_version}.")


class PartialParsingFailedBecauseConfigChange(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ("Unable to do partial parsing because config vars, "
                "config profile, or config target have changed")


class PartialParsingFailedBecauseProfileChange(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ("Unable to do partial parsing because profile has changed")


class PartialParsingFailedBecauseNewProjectDependency(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ("Unable to do partial parsing because a project dependency has been added")


class PartialParsingFailedBecauseHashChanged(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ("Unable to do partial parsing because a project config has changed")


class PartialParsingNotEnabled(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ("Partial parsing not enabled")


@dataclass
class ParsedFileLoadFailed(ShowException, DebugLevel, CliEventABC):
    path: str
    exc: Exception

    def cli_msg(self) -> str:
        return f"Failed to load parsed file from disk at {self.path}: {self.exc}"


class PartialParseSaveFileNotFound(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ("Partial parse save file not found. Starting full parse.")


@dataclass
class StaticParserCausedJinjaRendering(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1605: jinja rendering because of STATIC_PARSER flag. file: {self.path}"


# TODO: Experimental/static parser uses these for testing and some may be a good use case for
#       the `TestLevel` logger once we implement it.  Some will probably stay `DebugLevel`.
@dataclass
class UsingExperimentalParser(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1610: conducting experimental parser sample on {self.path}"


@dataclass
class SampleFullJinjaRendering(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1611: conducting full jinja rendering sample on {self.path}"


@dataclass
class StaticParserFallbackJinjaRendering(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1602: parser fallback to jinja rendering on {self.path}"


@dataclass
class StaticParsingMacroOverrideDetected(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1601: detected macro override of ref/source/config in the scope of {self.path}"


@dataclass
class StaticParserSuccess(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1699: static parser successfully parsed {self.path}"


@dataclass
class StaticParserFailure(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1603: static parser failed on {self.path}"


@dataclass
class ExperimentalParserSuccess(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1698: experimental parser successfully parsed {self.path}"


@dataclass
class ExperimentalParserFailure(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"1604: experimental parser failed on {self.path}"


@dataclass
class PartialParsingEnabled(DebugLevel, CliEventABC):
    deleted: int
    added: int
    changed: int

    def cli_msg(self) -> str:
        return (f"Partial parsing enabled: "
                f"{self.deleted} files deleted, "
                f"{self.added} files added, "
                f"{self.changed} files changed.")


@dataclass
class PartialParsingAddedFile(DebugLevel, CliEventABC):
    file_id: str

    def cli_msg(self) -> str:
        return f"Partial parsing: added file: {self.file_id}"


@dataclass
class PartialParsingDeletedFile(DebugLevel, CliEventABC):
    file_id: str

    def cli_msg(self) -> str:
        return f"Partial parsing: deleted file: {self.file_id}"


@dataclass
class PartialParsingUpdatedFile(DebugLevel, CliEventABC):
    file_id: str

    def cli_msg(self) -> str:
        return f"Partial parsing: updated file: {self.file_id}"


@dataclass
class PartialParsingNodeMissingInSourceFile(DebugLevel, CliEventABC):
    source_file: str

    def cli_msg(self) -> str:
        return f"Partial parsing: node not found for source_file {self.source_file}"


@dataclass
class PartialParsingMissingNodes(DebugLevel, CliEventABC):
    file_id: str

    def cli_msg(self) -> str:
        return f"No nodes found for source file {self.file_id}"


@dataclass
class PartialParsingChildMapMissingUniqueID(DebugLevel, CliEventABC):
    unique_id: str

    def cli_msg(self) -> str:
        return f"Partial parsing: {self.unique_id} not found in child_map"


@dataclass
class PartialParsingUpdateSchemaFile(DebugLevel, CliEventABC):
    file_id: str

    def cli_msg(self) -> str:
        return f"Partial parsing: update schema file: {self.file_id}"


@dataclass
class PartialParsingDeletedSource(DebugLevel, CliEventABC):
    unique_id: str

    def cli_msg(self) -> str:
        return f"Partial parsing: deleted source {self.unique_id}"


@dataclass
class PartialParsingDeletedExposure(DebugLevel, CliEventABC):
    unique_id: str

    def cli_msg(self) -> str:
        return f"Partial parsing: deleted exposure {self.unique_id}"


@dataclass
class InvalidDisabledSourceInTestNode(WarnLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return ui.warning_tag(self.msg)


@dataclass
class InvalidRefInTestNode(WarnLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return ui.warning_tag(self.msg)


@dataclass
class RunningOperationCaughtError(ErrorLevel, CliEventABC):
    exc: Exception

    def cli_msg(self) -> str:
        return f'Encountered an error while running operation: {self.exc}'


@dataclass
class RunningOperationUncaughtError(ErrorLevel, CliEventABC):
    exc: Exception

    def cli_msg(self) -> str:
        return f'Encountered an error while running operation: {self.exc}'


class DbtProjectError(ErrorLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Encountered an error while reading the project:"


@dataclass
class DbtProjectErrorException(ErrorLevel, CliEventABC):
    exc: Exception

    def cli_msg(self) -> str:
        return f"  ERROR: {str(self.exc)}"


class DbtProfileError(ErrorLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Encountered an error while reading profiles:"


@dataclass
class DbtProfileErrorException(ErrorLevel, CliEventABC):
    exc: Exception

    def cli_msg(self) -> str:
        return f"  ERROR: {str(self.exc)}"


class ProfileListTitle(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Defined profiles:"


@dataclass
class ListSingleProfile(InfoLevel, CliEventABC):
    profile: str

    def cli_msg(self) -> str:
        return f" - {self.profile}"


class NoDefinedProfiles(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "There are no profiles defined in your profiles.yml file"


class ProfileHelpMessage(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        PROFILES_HELP_MESSAGE = """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""
        return PROFILES_HELP_MESSAGE


@dataclass
class CatchableExceptionOnRun(ShowException, DebugLevel, CliEventABC):
    exc: Exception

    def cli_msg(self) -> str:
        return str(self.exc)


@dataclass
class InternalExceptionOnRun(DebugLevel, CliEventABC):
    build_path: str
    exc: Exception

    def cli_msg(self) -> str:
        prefix = 'Internal error executing {}'.format(self.build_path)

        INTERNAL_ERROR_STRING = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/dbt-labs/dbt-core
""".strip()

        return "{prefix}\n{error}\n\n{note}".format(
            prefix=ui.red(prefix),
            error=str(self.exc).strip(),
            note=INTERNAL_ERROR_STRING
        )


# This prints the stack trace at the debug level while allowing just the nice exception message
# at the error level - or whatever other level chosen.  Used in multiple places.
@dataclass
class PrintDebugStackTrace(ShowException, DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ""


@dataclass
class GenericExceptionOnRun(ErrorLevel, CliEventABC):
    build_path: str
    unique_id: str
    exc: Exception

    def cli_msg(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = "Unhandled error while executing {}".format(node_description)
        return "{prefix}\n{error}".format(
            prefix=ui.red(prefix),
            error=str(self.exc).strip()
        )


@dataclass
class NodeConnectionReleaseError(ShowException, DebugLevel, CliEventABC):
    node_name: str
    exc: Exception

    def cli_msg(self) -> str:
        return ('Error releasing connection for node {}: {!s}'
                .format(self.node_name, self.exc))


@dataclass
class CheckCleanPath(InfoLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Checking {self.path}/*"


@dataclass
class ConfirmCleanPath(InfoLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Cleaned {self.path}/*"


@dataclass
class ProtectedCleanPath(InfoLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"ERROR: not cleaning {self.path}/* because it is protected"


class FinishedCleanPaths(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Finished cleaning all paths."


@dataclass
class OpenCommand(InfoLevel, CliEventABC):
    open_cmd: str
    profiles_dir: str

    def cli_msg(self) -> str:
        PROFILE_DIR_MESSAGE = """To view your profiles.yml file, run:

{open_cmd} {profiles_dir}"""
        message = PROFILE_DIR_MESSAGE.format(
            open_cmd=self.open_cmd,
            profiles_dir=self.profiles_dir
        )

        return message


class DepsNoPackagesFound(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return 'Warning: No packages were found in packages.yml'


@dataclass
class DepsStartPackageInstall(InfoLevel, CliEventABC):
    package: str

    def cli_msg(self) -> str:
        return f"Installing {self.package}"


@dataclass
class DepsInstallInfo(InfoLevel, CliEventABC):
    version_name: str

    def cli_msg(self) -> str:
        return f"  Installed from {self.version_name}"


@dataclass
class DepsUpdateAvailable(InfoLevel, CliEventABC):
    version_latest: str

    def cli_msg(self) -> str:
        return f"  Updated version available: {self.version_latest}"


class DepsUTD(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "  Up to date!"


@dataclass
class DepsListSubdirectory(InfoLevel, CliEventABC):
    subdirectory: str

    def cli_msg(self) -> str:
        return f"   and subdirectory {self.subdirectory}"


@dataclass
class DepsNotifyUpdatesAvailable(InfoLevel, CliEventABC):
    packages: List[str]

    def cli_msg(self) -> str:
        return ('\nUpdates available for packages: {} \
                \nUpdate your versions in packages.yml, then run dbt deps'.format(self.packages))


@dataclass
class DatabaseErrorRunning(InfoLevel, CliEventABC):
    hook_type: str

    def cli_msg(self) -> str:
        return f"Database error while running {self.hook_type}"


class EmptyLine(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ''


@dataclass
class HooksRunning(InfoLevel, CliEventABC):
    num_hooks: int
    hook_type: str

    def cli_msg(self) -> str:
        plural = 'hook' if self.num_hooks == 1 else 'hooks'
        return f"Running {self.num_hooks} {self.hook_type} {plural}"


@dataclass
class HookFinished(InfoLevel, CliEventABC):
    stat_line: str
    execution: str

    def cli_msg(self) -> str:
        return f"Finished running {self.stat_line}{self.execution}."


@dataclass
class WriteCatalogFailure(ErrorLevel, CliEventABC):
    num_exceptions: int

    def cli_msg(self) -> str:
        return (f"dbt encountered {self.num_exceptions} failure{(self.num_exceptions != 1) * 's'} "
                "while writing the catalog")


@dataclass
class CatalogWritten(InfoLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Catalog written to {self.path}"


class CannotGenerateDocs(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "compile failed, cannot generate docs"


class BuildingCatalog(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Building catalog"


class CompileComplete(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Done."


class FreshnessCheckComplete(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Done."


@dataclass
class ServingDocsPort(InfoLevel, CliEventABC):
    address: str
    port: int

    def cli_msg(self) -> str:
        return f"Serving docs at {self.address}:{self.port}"


@dataclass
class ServingDocsAccessInfo(InfoLevel, CliEventABC):
    port: str

    def cli_msg(self) -> str:
        return f"To access from your browser, navigate to:  http://localhost:{self.port}"


class ServingDocsExitInfo(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Press Ctrl+C to exit.\n\n"


@dataclass
class SeedHeader(InfoLevel, CliEventABC):
    header: str

    def cli_msg(self) -> str:
        return self.header


@dataclass
class SeedHeaderSeperator(InfoLevel, CliEventABC):
    len_header: int

    def cli_msg(self) -> str:
        return "-" * self.len_header


@dataclass
class RunResultWarning(WarnLevel, CliEventABC):
    resource_type: str
    node_name: str
    path: str

    def cli_msg(self) -> str:
        info = 'Warning'
        return ui.yellow(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class RunResultFailure(ErrorLevel, CliEventABC):
    resource_type: str
    node_name: str
    path: str

    def cli_msg(self) -> str:
        info = 'Failure'
        return ui.red(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class StatsLine(InfoLevel, CliEventABC):
    stats: Dict

    def cli_msg(self) -> str:
        stats_line = ("\nDone. PASS={pass} WARN={warn} ERROR={error} SKIP={skip} TOTAL={total}")
        return stats_line.format(**self.stats)


@dataclass
class RunResultError(ErrorLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return f"  {self.msg}"


@dataclass
class RunResultErrorNoMessage(ErrorLevel, CliEventABC):
    status: str

    def cli_msg(self) -> str:
        return f"  Status: {self.status}"


@dataclass
class SQLCompiledPath(InfoLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"  compiled SQL at {self.path}"


@dataclass
class CheckNodeTestFailure(InfoLevel, CliEventABC):
    relation_name: str

    def cli_msg(self) -> str:
        msg = f"select * from {self.relation_name}"
        border = '-' * len(msg)
        return f"  See test failures:\n  {border}\n  {msg}\n  {border}"


@dataclass
class FirstRunResultError(ErrorLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return ui.yellow(self.msg)


@dataclass
class AfterFirstRunResultError(ErrorLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


@dataclass
class EndOfRunSummary(InfoLevel, CliEventABC):
    num_errors: int
    num_warnings: int
    keyboard_interrupt: bool = False

    def cli_msg(self) -> str:
        error_plural = pluralize(self.num_errors, 'error')
        warn_plural = pluralize(self.num_warnings, 'warning')
        if self.keyboard_interrupt:
            message = ui.yellow('Exited because of keyboard interrupt.')
        elif self.num_errors > 0:
            message = ui.red("Completed with {} and {}:".format(
                error_plural, warn_plural))
        elif self.num_warnings > 0:
            message = ui.yellow('Completed with {}:'.format(warn_plural))
        else:
            message = ui.green('Completed successfully')
        return message


@dataclass
class PrintStartLine(InfoLevel, CliEventABC):
    description: str
    index: int
    total: int

    def cli_msg(self) -> str:
        msg = "START {self.description}"
        return format_fancy_output_line(msg=msg, status='RUN', index=self.index, total=self.total)


@dataclass
class PrintHookStartLine(InfoLevel, CliEventABC):
    statement: str
    index: int
    total: int
    truncate: bool

    def cli_msg(self) -> str:
        msg = "START hook: {self.statement}"
        return format_fancy_output_line(msg=msg,
                                        status='RUN',
                                        index=self.index,
                                        total=self.total,
                                        truncate=self.truncate)


@dataclass
class PrintHookEndLine(InfoLevel, CliEventABC):
    statement: str
    status: str
    index: int
    total: int
    execution_time: int
    truncate: bool

    def cli_msg(self) -> str:
        msg = 'OK hook: {}'.format(self.statement)
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time,
                                        truncate=self.truncate)


@dataclass
class SkippingDetails(InfoLevel, CliEventABC):
    resource_type: str
    schema: str
    node_name: str
    index: int
    total: int

    def cli_msg(self) -> str:
        if self.resource_type in NodeType.refable():
            msg = f'SKIP relation {self.schema}.{self.node_name}'
        else:
            msg = f'SKIP {self.resource_type} {self.node_name}'
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow('SKIP'),
                                        index=self.index,
                                        total=self.total)


@dataclass
class PrintErrorTestResult(ErrorLevel, CliEventABC):
    name: str
    index: int
    num_models: int
    execution_time: int

    def cli_msg(self) -> str:
        info = "ERROR"
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintPassTestResult(InfoLevel, CliEventABC):
    name: str
    index: int
    num_models: int
    execution_time: int

    def cli_msg(self) -> str:
        info = "PASS"
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintWarnTestResult(WarnLevel, CliEventABC):
    name: str
    index: int
    num_models: int
    execution_time: int
    failures: List[str]

    def cli_msg(self) -> str:
        info = f'WARN {self.failures}'
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintFailureTestResult(ErrorLevel, CliEventABC):
    name: str
    index: int
    num_models: int
    execution_time: int
    failures: List[str]

    def cli_msg(self) -> str:
        info = f'FAIL {self.failures}'
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintSkipBecauseError(ErrorLevel, CliEventABC):
    schema: str
    relation: str
    index: int
    total: int

    def cli_msg(self) -> str:
        msg = f'SKIP relation {self.schema}.{self.relation} due to ephemeral model error'
        return format_fancy_output_line(msg=msg,
                                        status=ui.red('ERROR SKIP'),
                                        index=self.index,
                                        total=self.total)


@dataclass
class PrintModelErrorResultLine(ErrorLevel, CliEventABC):
    description: str
    status: str
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = "ERROR creating"
        msg = f"{info} {self.description}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintModelResultLine(InfoLevel, CliEventABC):
    description: str
    status: str
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = "OK created"
        msg = f"{info} {self.description}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSnapshotErrorResultLine(ErrorLevel, CliEventABC):
    status: str
    description: str
    cfg: Dict
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = 'ERROR snapshotting'
        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSnapshotResultLine(InfoLevel, CliEventABC):
    status: str
    description: str
    cfg: Dict
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = 'OK snapshotted'
        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSeedErrorResultLine(ErrorLevel, CliEventABC):
    status: str
    index: int
    total: int
    execution_time: int
    schema: str
    relation: str

    def cli_msg(self) -> str:
        info = 'ERROR loading'
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSeedResultLine(InfoLevel, CliEventABC):
    status: str
    index: int
    total: int
    execution_time: int
    schema: str
    relation: str

    def cli_msg(self) -> str:
        info = 'OK loaded'
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndErrorLine(ErrorLevel, CliEventABC):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = 'ERROR'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndErrorStaleLine(ErrorLevel, CliEventABC):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = 'ERROR STALE'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndWarnLine(WarnLevel, CliEventABC):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = 'WARN'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndPassLine(InfoLevel, CliEventABC):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def cli_msg(self) -> str:
        info = 'PASS'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintCancelLine(ErrorLevel, CliEventABC):
    conn_name: str

    def cli_msg(self) -> str:
        msg = 'CANCEL query {}'.format(self.conn_name)
        return format_fancy_output_line(msg=msg,
                                        status=ui.red('CANCEL'),
                                        index=None,
                                        total=None)


@dataclass
class DefaultSelector(InfoLevel, CliEventABC):
    name: str

    def cli_msg(self) -> str:
        return f"Using default selector {self.name}"


@dataclass
class NodeStart(DebugLevel, CliEventABC):
    unique_id: str

    def cli_msg(self) -> str:
        return f"Began running node {self.unique_id}"


@dataclass
class NodeFinished(DebugLevel, CliEventABC):
    unique_id: str

    def cli_msg(self) -> str:
        return f"Finished running node {self.unique_id}"


@dataclass
class QueryCancelationUnsupported(InfoLevel, CliEventABC):
    type: str

    def cli_msg(self) -> str:
        msg = (f"The {self.type} adapter does not support query "
               "cancellation. Some queries may still be "
               "running!")
        return ui.yellow(msg)


@dataclass
class ConcurrencyLine(InfoLevel, CliEventABC):
    concurrency_line: str

    def cli_msg(self) -> str:
        return self.concurrency_line


@dataclass
class StarterProjectPath(DebugLevel, CliEventABC):
    dir: str

    def cli_msg(self) -> str:
        return f"Starter project path: {self.dir}"


@dataclass
class ConfigFolderDirectory(InfoLevel, CliEventABC):
    dir: str

    def cli_msg(self) -> str:
        return f"Creating dbt configuration folder at {self.dir}"


@dataclass
class NoSampleProfileFound(InfoLevel, CliEventABC):
    adapter: str

    def cli_msg(self) -> str:
        return f"No sample profile found for {self.adapter}."


@dataclass
class ProfileWrittenWithSample(InfoLevel, CliEventABC):
    name: str
    path: str

    def cli_msg(self) -> str:
        return (f"Profile {self.name} written to {self.path} "
                "using target's sample configuration. Once updated, you'll be able to "
                "start developing with dbt.")


@dataclass
class ProfileWrittenWithTargetTemplateYAML(InfoLevel, CliEventABC):
    name: str
    path: str

    def cli_msg(self) -> str:
        return (f"Profile {self.name} written to {self.path} using target's "
                "profile_template.yml and your supplied values. Run 'dbt debug' to "
                "validate the connection.")


@dataclass
class ProfileWrittenWithProjectTemplateYAML(InfoLevel, CliEventABC):
    name: str
    path: str

    def cli_msg(self) -> str:
        return (f"Profile {self.name} written to {self.path} using project's "
                "profile_template.yml and your supplied values. Run 'dbt debug' to "
                "validate the connection.")


class SettingUpProfile(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Setting up your profile."


class InvalidProfileTemplateYAML(InfoLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Invalid profile_template.yml in project."


@dataclass
class ProjectNameAlreadyExists(InfoLevel, CliEventABC):
    name: str

    def cli_msg(self) -> str:
        return f"A project called {self.name} already exists here."


@dataclass
class GetAddendum(InfoLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


@dataclass
class DepsSetDownloadDirectory(DebugLevel, CliEventABC):
    path: str

    def cli_msg(self) -> str:
        return f"Set downloads directory='{self.path}'"


class EnsureGitInstalled(ErrorLevel, CliEventABC):
    def cli_msg(self) -> str:
        return ('Make sure git is installed on your machine. More '
                'information: '
                'https://docs.getdbt.com/docs/package-management')


class DepsCreatingLocalSymlink(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return '  Creating symlink to local dependency.'


class DepsSymlinkNotAvailable(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return '  Symlinks are not available on this OS, copying dependency.'


@dataclass
class FoundStats(InfoLevel, CliEventABC):
    stat_line: str

    def cli_msg(self) -> str:
        return f"Found {self.stat_line}"


@dataclass
class CompilingNode(DebugLevel, CliEventABC):
    unique_id: str

    def cli_msg(self) -> str:
        return f"Compiling {self.unique_id}"


@dataclass
class WritingInjectedSQLForNode(DebugLevel, CliEventABC):
    unique_id: str

    def cli_msg(self) -> str:
        return f'Writing injected SQL for node "{self.unique_id}"'


class DisableTracking(WarnLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Error sending message, disabling tracking"


@dataclass
class SendingEvent(DebugLevel, CliEventABC):
    kwargs: str

    def cli_msg(self) -> str:
        return f"Sending event: {self.kwargs}"


class SendEventFailure(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "An error was encountered while trying to send an event"


class FlushEvents(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Flushing usage events"


class FlushEventsFailure(DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "An error was encountered while trying to flush usage events"


class TrackingInitializeFailure(ShowException, DebugLevel, CliEventABC):
    def cli_msg(self) -> str:
        return "Got an exception trying to initialize tracking"


@dataclass
class RetryExternalCall(DebugLevel, CliEventABC):
    attempt: int
    max: int

    def cli_msg(self) -> str:
        return f"Retrying external call. Attempt: {self.attempt} Max attempts: {self.max}"


@dataclass
class GeneralWarningMsg(WarnLevel, CliEventABC):
    msg: str
    log_fmt: str

    def cli_msg(self) -> str:
        if self.log_fmt is not None:
            return self.log_fmt.format(self.msg)
        return self.msg


@dataclass
class GeneralWarningException(WarnLevel, CliEventABC):
    exc: Exception
    log_fmt: str

    def cli_msg(self) -> str:
        if self.log_fmt is not None:
            return self.log_fmt.format(str(self.exc))
        return str(self.exc)


# since mypy doesn't run on every file we need to suggest to mypy that every
# class gets instantiated. But we don't actually want to run this code.
# making the conditional `if False` causes mypy to skip it as dead code so
# we need to skirt around that by computing something it doesn't check statically.
#
# TODO remove these lines once we run mypy everywhere.
if 1 == 0:
    MainReportVersion('')
    MainKeyboardInterrupt()
    MainEncounteredError(BaseException(''))
    MainStackTrace('')
    MainReportVersion('')
    MainTrackingUserState('')
    ParsingStart()
    ParsingCompiling()
    ParsingWritingManifest()
    ParsingDone()
    ManifestDependenciesLoaded()
    ManifestLoaderCreated()
    ManifestLoaded()
    ManifestChecked()
    ManifestFlatGraphBuilt()
    ReportPerformancePath(path='')
    GitSparseCheckoutSubdirectory(subdir='')
    GitProgressCheckoutRevision(revision='')
    GitProgressUpdatingExistingDependency(dir='')
    GitProgressPullingNewDependency(dir='')
    GitNothingToDo(sha='')
    GitProgressUpdatedCheckoutRange(start_sha='', end_sha='')
    GitProgressCheckedOutAt(end_sha='')
    SystemErrorRetrievingModTime(path='')
    SystemCouldNotWrite(path='', reason='', exc=Exception(''))
    SystemExecutingCmd(cmd=[''])
    SystemStdOutMsg(bmsg=b'')
    SystemStdErrMsg(bmsg=b'')
    SystemReportReturnCode(code=0)
    SelectorAlertUpto3UnusedNodes(node_names=[])
    SelectorAlertAllUnusedNodes(node_names=[])
    SelectorReportInvalidSelector(selector_methods={'': ''}, spec_method='', raw_spec='')
    MacroEventInfo(msg='')
    MacroEventDebug(msg='')
    NewConnectionOpening(connection_state='')
    TimingInfoCollected()
    MergedFromState(nbr_merged=0, sample=[])
    MissingProfileTarget(profile_name='', target_name='')
    ProfileLoadError(exc=Exception(''))
    ProfileNotFound(profile_name='')
    InvalidVarsYAML()
    GenericTestFileParse(path='')
    MacroFileParse(path='')
    PartialParsingFullReparseBecauseOfError()
    PartialParsingFile(file_dict={})
    PartialParsingException(exc_info={})
    PartialParsingSkipParsing()
    PartialParsingMacroChangeStartFullParse()
    ManifestWrongMetadataVersion(version='')
    PartialParsingVersionMismatch(saved_version='', current_version='')
    PartialParsingFailedBecauseConfigChange()
    PartialParsingFailedBecauseProfileChange()
    PartialParsingFailedBecauseNewProjectDependency()
    PartialParsingFailedBecauseHashChanged()
    ParsedFileLoadFailed(path='', exc=Exception(''))
    PartialParseSaveFileNotFound()
    StaticParserCausedJinjaRendering(path='')
    UsingExperimentalParser(path='')
    SampleFullJinjaRendering(path='')
    StaticParserFallbackJinjaRendering(path='')
    StaticParsingMacroOverrideDetected(path='')
    StaticParserSuccess(path='')
    StaticParserFailure(path='')
    ExperimentalParserSuccess(path='')
    ExperimentalParserFailure(path='')
    PartialParsingEnabled(deleted=0, added=0, changed=0)
    PartialParsingAddedFile(file_id='')
    PartialParsingDeletedFile(file_id='')
    PartialParsingUpdatedFile(file_id='')
    PartialParsingNodeMissingInSourceFile(source_file='')
    PartialParsingMissingNodes(file_id='')
    PartialParsingChildMapMissingUniqueID(unique_id='')
    PartialParsingUpdateSchemaFile(file_id='')
    PartialParsingDeletedSource(unique_id='')
    PartialParsingDeletedExposure(unique_id='')
    InvalidDisabledSourceInTestNode(msg='')
    InvalidRefInTestNode(msg='')
    MessageHandleGenericException(build_path='', unique_id='', exc=Exception(''))
    DetailsHandleGenericException()
    RunningOperationCaughtError(exc=Exception(''))
    RunningOperationUncaughtError(exc=Exception(''))
    DbtProjectError()
    DbtProjectErrorException(exc=Exception(''))
    DbtProfileError()
    DbtProfileErrorException(exc=Exception(''))
    ProfileListTitle()
    ListSingleProfile(profile='')
    NoDefinedProfiles()
    ProfileHelpMessage()
    CatchableExceptionOnRun(exc=Exception(''))
    InternalExceptionOnRun(build_path='', exc=Exception(''))
    GenericExceptionOnRun(build_path='', unique_id='', exc=Exception(''))
    NodeConnectionReleaseError(node_name='', exc=Exception(''))
    CheckCleanPath(path='')
    ConfirmCleanPath(path='')
    ProtectedCleanPath(path='')
    FinishedCleanPaths()
    OpenCommand(open_cmd='', profiles_dir='')
    DepsNoPackagesFound()
    DepsStartPackageInstall(package='')
    DepsInstallInfo(version_name='')
    DepsUpdateAvailable(version_latest='')
    DepsListSubdirectory(subdirectory='')
    DepsNotifyUpdatesAvailable(packages=[])
    DatabaseErrorRunning(hook_type='')
    EmptyLine()
    HooksRunning(num_hooks=0, hook_type='')
    HookFinished(stat_line='', execution='')
    WriteCatalogFailure(num_exceptions=0)
    CatalogWritten(path='')
    CannotGenerateDocs()
    BuildingCatalog()
    CompileComplete()
    FreshnessCheckComplete()
    ServingDocsPort(address='', port=0)
    ServingDocsAccessInfo(port='')
    ServingDocsExitInfo()
    SeedHeader(header='')
    SeedHeaderSeperator(len_header=0)
    RunResultWarning(resource_type='', node_name='', path='')
    RunResultFailure(resource_type='', node_name='', path='')
    StatsLine(stats={})
    RunResultError(msg='')
    RunResultErrorNoMessage(status='')
    SQLCompiledPath(path='')
    CheckNodeTestFailure(relation_name='')
    FirstRunResultError(msg='')
    AfterFirstRunResultError(msg='')
    EndOfRunSummary(num_errors=0, num_warnings=0, keyboard_interrupt=False)
    PrintStartLine(description='', index=0, total=0)
    PrintHookStartLine(statement='', index=0, total=0, truncate=False)
    PrintHookEndLine(statement='', status='', index=0, total=0, execution_time=0, truncate=False)
    SkippingDetails(resource_type='', schema='', node_name='', index=0, total=0)
    PrintErrorTestResult(name='', index=0, num_models=0, execution_time=0)
    PrintPassTestResult(name='', index=0, num_models=0, execution_time=0)
    PrintWarnTestResult(name='', index=0, num_models=0, execution_time=0, failures=[])
    PrintFailureTestResult(name='', index=0, num_models=0, execution_time=0, failures=[])
    PrintSkipBecauseError(schema='', relation='', index=0, total=0)
    PrintModelErrorResultLine(description='', status='', index=0, total=0, execution_time=0)
    PrintModelResultLine(description='', status='', index=0, total=0, execution_time=0)
    PrintSnapshotErrorResultLine(status='',
                                 description='',
                                 cfg={},
                                 index=0,
                                 total=0,
                                 execution_time=0)
    PrintSnapshotResultLine(status='', description='', cfg={}, index=0, total=0, execution_time=0)
    PrintSeedErrorResultLine(status='', index=0, total=0, execution_time=0, schema='', relation='')
    PrintSeedResultLine(status='', index=0, total=0, execution_time=0, schema='', relation='')
    PrintHookEndErrorLine(source_name='', table_name='', index=0, total=0, execution_time=0)
    PrintHookEndErrorStaleLine(source_name='', table_name='', index=0, total=0, execution_time=0)
    PrintHookEndWarnLine(source_name='', table_name='', index=0, total=0, execution_time=0)
    PrintHookEndPassLine(source_name='', table_name='', index=0, total=0, execution_time=0)
    PrintCancelLine(conn_name='')
    DefaultSelector(name='')
    NodeStart(unique_id='')
    NodeFinished(unique_id='')
    QueryCancelationUnsupported(type='')
    ConcurrencyLine(concurrency_line='')
    StarterProjectPath(dir='')
    ConfigFolderDirectory(dir='')
    NoSampleProfileFound(adapter='')
    ProfileWrittenWithSample(name='', path='')
    ProfileWrittenWithTargetTemplateYAML(name='', path='')
    ProfileWrittenWithProjectTemplateYAML(name='', path='')
    SettingUpProfile()
    InvalidProfileTemplateYAML()
    ProjectNameAlreadyExists(name='')
    GetAddendum(msg='')
    DepsSetDownloadDirectory(path='')
    EnsureGitInstalled()
    DepsCreatingLocalSymlink()
    DepsSymlinkNotAvailable()
    FoundStats(stat_line='')
    CompilingNode(unique_id='')
    WritingInjectedSQLForNode(unique_id='')
    DisableTracking()
    SendingEvent(kwargs='')
    SendEventFailure()
    FlushEvents()
    FlushEventsFailure()
    TrackingInitializeFailure()
    RetryExternalCall(attempt=0, max=0)
    GeneralWarningMsg(msg='', log_fmt='')
    GeneralWarningException(exc=Exception(''), log_fmt='')
