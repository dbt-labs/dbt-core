from abc import ABCMeta, abstractmethod
import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, cast, Dict, List, Optional, Set, Union
from dbt.events.stubs import _CachedRelation, AdapterResponse, BaseRelation, _ReferenceKey
from dbt import ui
from dbt.node_types import NodeType
from dbt.events.format import format_fancy_output_line, pluralize
import os


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
    # `message` method of subclasses, this is a safe operation.
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
    # fields that should be on all events with their default implementations
    ts: datetime = datetime.now()
    pid: int = os.getpid()
    # code: int

    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for event")

    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    # Must override yourself
    @abstractmethod
    def message(self) -> str:
        raise Exception("msg not implemented for cli event")


class File(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    def file_msg(self) -> str:
        # returns the event msg unless overriden in the concrete class
        return self.message()


class Cli(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    def cli_msg(self) -> str:
        # returns the event msg unless overriden in the concrete class
        return self.message()


@dataclass
class AdapterEventBase():
    name: str
    raw_msg: str

    def message(self) -> str:
        return f"{self.name} adapter: {self.raw_msg}"


class AdapterEventDebug(DebugLevel, AdapterEventBase, Cli, File, ShowException):
    pass


class AdapterEventInfo(InfoLevel, AdapterEventBase, Cli, File, ShowException):
    pass


class AdapterEventWarning(WarnLevel, AdapterEventBase, Cli, File, ShowException):
    pass


class AdapterEventError(ErrorLevel, AdapterEventBase, Cli, File, ShowException):
    pass


class MainKeyboardInterrupt(InfoLevel, Cli):
    def message(self) -> str:
        return "ctrl-c"


# will log to a file if the file logger is configured
@dataclass
class MainEncounteredError(ErrorLevel, Cli):
    e: BaseException

    def message(self) -> str:
        return f"Encountered an error:\n{str(self.e)}"


@dataclass
class MainStackTrace(DebugLevel, Cli):
    stack_trace: str

    def message(self) -> str:
        return self.stack_trace


@dataclass
class MainReportVersion(InfoLevel, Cli, File):
    v: str  # could be VersionSpecifier instead if we resolved some circular imports

    def message(self):
        return f"Running with dbt{self.v}"


@dataclass
class MainReportArgs(DebugLevel, Cli, File):
    args: argparse.Namespace

    def message(self):
        return f"running dbt with arguments {str(self.args)}"


@dataclass
class MainTrackingUserState(DebugLevel, Cli):
    user_state: str

    def message(self):
        return f"Tracking: {self.user_state}"


class ParsingStart(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Start parsing."


class ParsingCompiling(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Compiling."


class ParsingWritingManifest(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Writing manifest."


class ParsingDone(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Done."


class ManifestDependenciesLoaded(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Dependencies loaded"


class ManifestLoaderCreated(InfoLevel, Cli, File):
    def message(self) -> str:
        return "ManifestLoader created"


class ManifestLoaded(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Manifest loaded"


class ManifestChecked(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Manifest checked"


class ManifestFlatGraphBuilt(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Flat graph built"


@dataclass
class ReportPerformancePath(InfoLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"Performance info: {self.path}"


@dataclass
class GitSparseCheckoutSubdirectory(DebugLevel, Cli, File):
    subdir: str

    def message(self) -> str:
        return f"  Subdirectory specified: {self.subdir}, using sparse checkout."


@dataclass
class GitProgressCheckoutRevision(DebugLevel, Cli, File):
    revision: str

    def message(self) -> str:
        return f"  Checking out revision {self.revision}."


@dataclass
class GitProgressUpdatingExistingDependency(DebugLevel, Cli, File):
    dir: str

    def message(self) -> str:
        return f"Updating existing dependency {self.dir}."


@dataclass
class GitProgressPullingNewDependency(DebugLevel, Cli, File):
    dir: str

    def message(self) -> str:
        return f"Pulling new dependency {self.dir}."


@dataclass
class GitNothingToDo(DebugLevel, Cli, File):
    sha: str

    def message(self) -> str:
        return f"Already at {self.sha}, nothing to do."


@dataclass
class GitProgressUpdatedCheckoutRange(DebugLevel, Cli, File):
    start_sha: str
    end_sha: str

    def message(self) -> str:
        return f"  Updated checkout from {self.start_sha} to {self.end_sha}."


@dataclass
class GitProgressCheckedOutAt(DebugLevel, Cli, File):
    end_sha: str

    def message(self) -> str:
        return f"  Checked out at {self.end_sha}."


@dataclass
class RegistryProgressMakingGETRequest(DebugLevel, Cli, File):
    url: str

    def message(self) -> str:
        return f"Making package registry request: GET {self.url}"


@dataclass
class RegistryProgressGETResponse(DebugLevel, Cli, File):
    url: str
    resp_code: int

    def message(self) -> str:
        return f"Response from registry: GET {self.url} {self.resp_code}"


# TODO this was actually `logger.exception(...)` not `logger.error(...)`
@dataclass
class SystemErrorRetrievingModTime(ErrorLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"Error retrieving modification time for file {self.path}"


@dataclass
class SystemCouldNotWrite(DebugLevel, Cli, File):
    path: str
    reason: str
    exc: Exception

    def message(self) -> str:
        return (
            f"Could not write to path {self.path}({len(self.path)} characters): "
            f"{self.reason}\nexception: {self.exc}"
        )


@dataclass
class SystemExecutingCmd(DebugLevel, Cli, File):
    cmd: List[str]

    def message(self) -> str:
        return f'Executing "{" ".join(self.cmd)}"'


@dataclass
class SystemStdOutMsg(DebugLevel, Cli, File):
    bmsg: bytes

    def message(self) -> str:
        return f'STDOUT: "{str(self.bmsg)}"'


@dataclass
class SystemStdErrMsg(DebugLevel, Cli, File):
    bmsg: bytes

    def message(self) -> str:
        return f'STDERR: "{str(self.bmsg)}"'


@dataclass
class SystemReportReturnCode(DebugLevel, Cli, File):
    code: int

    def message(self) -> str:
        return f"command return code={self.code}"


@dataclass
class SelectorAlertUpto3UnusedNodes(InfoLevel, Cli, File):
    node_names: List[str]

    def message(self) -> str:
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
class SelectorAlertAllUnusedNodes(DebugLevel, Cli, File):
    node_names: List[str]

    def message(self) -> str:
        debug_nodes_str = ("\n  - ").join(self.node_names)
        return (
            f"Full list of tests that were excluded:"
            f"\n  - {debug_nodes_str}"
        )


@dataclass
class SelectorReportInvalidSelector(InfoLevel, Cli, File):
    selector_methods: dict
    spec_method: str
    raw_spec: str

    def message(self) -> str:
        valid_selectors = ", ".join(self.selector_methods)
        return (
            f"The '{self.spec_method}' selector specified in {self.raw_spec} is "
            f"invalid. Must be one of [{valid_selectors}]"
        )


@dataclass
class MacroEventInfo(InfoLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return self.msg


@dataclass
class MacroEventDebug(DebugLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return self.msg


@dataclass
class NewConnection(DebugLevel, Cli, File):
    conn_type: str
    conn_name: str

    def message(self) -> str:
        return f'Acquiring new {self.conn_type} connection "{self.conn_name}"'


@dataclass
class ConnectionReused(DebugLevel, Cli, File):
    conn_name: str

    def message(self) -> str:
        return f"Re-using an available connection from the pool (formerly {self.conn_name})"


@dataclass
class ConnectionLeftOpen(DebugLevel, Cli, File):
    conn_name: Optional[str]

    def message(self) -> str:
        return f"Connection '{self.conn_name}' was left open."


@dataclass
class ConnectionClosed(DebugLevel, Cli, File):
    conn_name: Optional[str]

    def message(self) -> str:
        return f"Connection '{self.conn_name}' was properly closed."


@dataclass
class RollbackFailed(ShowException, DebugLevel, Cli, File):
    conn_name: Optional[str]

    def message(self) -> str:
        return f"Failed to rollback '{self.conn_name}'"


# TODO: can we combine this with ConnectionClosed?
@dataclass
class ConnectionClosed2(DebugLevel, Cli, File):
    conn_name: Optional[str]

    def message(self) -> str:
        return f"On {self.conn_name}: Close"


# TODO: can we combine this with ConnectionLeftOpen?
@dataclass
class ConnectionLeftOpen2(DebugLevel, Cli, File):
    conn_name: Optional[str]

    def message(self) -> str:
        return f"On {self.conn_name}: No close available on handle"


@dataclass
class Rollback(DebugLevel, Cli, File):
    conn_name: Optional[str]

    def message(self) -> str:
        return f"On {self.conn_name}: ROLLBACK"


@dataclass
class CacheMiss(DebugLevel, Cli, File):
    conn_name: Any  # TODO mypy says this is `Callable[[], str]`??  ¯\_(ツ)_/¯
    database: Optional[str]
    schema: str

    def message(self) -> str:
        return (
            f'On "{self.conn_name}": cache miss for schema '
            '"{self.database}.{self.schema}", this is inefficient'
        )


@dataclass
class ListRelations(DebugLevel, Cli, File):
    database: Optional[str]
    schema: str
    relations: List[BaseRelation]

    def message(self) -> str:
        return f"with database={self.database}, schema={self.schema}, relations={self.relations}"


@dataclass
class ConnectionUsed(DebugLevel, Cli, File):
    conn_type: str
    conn_name: Optional[str]

    def message(self) -> str:
        return f'Using {self.conn_type} connection "{self.conn_name}"'


@dataclass
class SQLQuery(DebugLevel, Cli, File):
    conn_name: Optional[str]
    sql: str

    def message(self) -> str:
        return f"On {self.conn_name}: {self.sql}"


@dataclass
class SQLQueryStatus(DebugLevel, Cli, File):
    status: Union[AdapterResponse, str]
    elapsed: float

    def message(self) -> str:
        return f"SQL status: {self.status} in {self.elapsed} seconds"


@dataclass
class SQLCommit(DebugLevel, Cli, File):
    conn_name: str

    def message(self) -> str:
        return f"On {self.conn_name}: COMMIT"


@dataclass
class ColTypeChange(DebugLevel, Cli, File):
    orig_type: str
    new_type: str
    table: str

    def message(self) -> str:
        return f"Changing col type from {self.orig_type} to {self.new_type} in table {self.table}"


@dataclass
class SchemaCreation(DebugLevel, Cli, File):
    relation: BaseRelation

    def message(self) -> str:
        return f'Creating schema "{self.relation}"'


@dataclass
class SchemaDrop(DebugLevel, Cli, File):
    relation: BaseRelation

    def message(self) -> str:
        return f'Dropping schema "{self.relation}".'


# TODO pretty sure this is only ever called in dead code
# see: core/dbt/adapters/cache.py _add_link vs add_link
@dataclass
class UncachedRelation(DebugLevel, Cli, File):
    dep_key: _ReferenceKey
    ref_key: _ReferenceKey

    def message(self) -> str:
        return (
            f"{self.dep_key} references {str(self.ref_key)} "
            "but {self.ref_key.database}.{self.ref_key.schema}"
            "is not in the cache, skipping assumed external relation"
        )


@dataclass
class AddLink(DebugLevel, Cli, File):
    dep_key: _ReferenceKey
    ref_key: _ReferenceKey

    def message(self) -> str:
        return f"adding link, {self.dep_key} references {self.ref_key}"


@dataclass
class AddRelation(DebugLevel, Cli, File):
    relation: _CachedRelation

    def message(self) -> str:
        return f"Adding relation: {str(self.relation)}"


@dataclass
class DropMissingRelation(DebugLevel, Cli, File):
    relation: _ReferenceKey

    def message(self) -> str:
        return f"dropped a nonexistent relationship: {str(self.relation)}"


@dataclass
class DropCascade(DebugLevel, Cli, File):
    dropped: _ReferenceKey
    consequences: Set[_ReferenceKey]

    def message(self) -> str:
        return f"drop {self.dropped} is cascading to {self.consequences}"


@dataclass
class DropRelation(DebugLevel, Cli, File):
    dropped: _ReferenceKey

    def message(self) -> str:
        return f"Dropping relation: {self.dropped}"


@dataclass
class UpdateReference(DebugLevel, Cli, File):
    old_key: _ReferenceKey
    new_key: _ReferenceKey
    cached_key: _ReferenceKey

    def message(self) -> str:
        return f"updated reference from {self.old_key} -> {self.cached_key} to "\
            "{self.new_key} -> {self.cached_key}"


@dataclass
class TemporaryRelation(DebugLevel, Cli, File):
    key: _ReferenceKey

    def message(self) -> str:
        return f"old key {self.key} not found in self.relations, assuming temporary"


@dataclass
class RenameSchema(DebugLevel, Cli, File):
    old_key: _ReferenceKey
    new_key: _ReferenceKey

    def message(self) -> str:
        return f"Renaming relation {self.old_key} to {self.new_key}"


@dataclass
class DumpBeforeAddGraph(DebugLevel, Cli, File):
    graph_func: Callable[[], Dict[str, List[str]]]

    def message(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        # TODO remove when we've upgraded to a mypy version without that bug
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"before adding : {func_returns}"


@dataclass
class DumpAfterAddGraph(DebugLevel, Cli, File):
    graph_func: Callable[[], Dict[str, List[str]]]

    def message(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"after adding: {func_returns}"


@dataclass
class DumpBeforeRenameSchema(DebugLevel, Cli, File):
    graph_func: Callable[[], Dict[str, List[str]]]

    def message(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"before rename: {func_returns}"


@dataclass
class DumpAfterRenameSchema(DebugLevel, Cli, File):
    graph_func: Callable[[], Dict[str, List[str]]]

    def message(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"after rename: {func_returns}"


@dataclass
class AdapterImportError(InfoLevel, Cli, File):
    exc: ModuleNotFoundError

    def message(self) -> str:
        return f"Error importing adapter: {self.exc}"


@dataclass
class PluginLoadError(ShowException, DebugLevel, Cli, File):
    def message(self):
        pass


@dataclass
class NewConnectionOpening(DebugLevel, Cli, File):
    connection_state: str

    def message(self) -> str:
        return f"Opening a new connection, currently in state {self.connection_state}"


class TimingInfoCollected(DebugLevel, Cli, File):
    def message(self) -> str:
        return "finished collecting timing info"


@dataclass
class MergedFromState(DebugLevel, Cli, File):
    nbr_merged: int
    sample: List

    def message(self) -> str:
        return f"Merged {self.nbr_merged} items from state (sample: {self.sample})"


@dataclass
class MissingProfileTarget(InfoLevel, Cli, File):
    profile_name: str
    target_name: str

    def message(self) -> str:
        return f"target not specified in profile '{self.profile_name}', using '{self.target_name}'"


@dataclass
class ProfileLoadError(ShowException, DebugLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return f"Profile not loaded due to error: {self.exc}"


@dataclass
class ProfileNotFound(InfoLevel, Cli, File):
    profile_name: Optional[str]

    def message(self) -> str:
        return f'No profile "{self.profile_name}" found, continuing with no target'


class InvalidVarsYAML(ErrorLevel, Cli, File):
    def message(self) -> str:
        return "The YAML provided in the --vars argument is not valid.\n"


@dataclass
class CatchRunException(ShowException, DebugLevel, Cli, File):
    build_path: Any
    exc: Exception

    def message(self) -> str:
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
class HandleInternalException(ShowException, DebugLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return str(self.exc)


@dataclass
class MessageHandleGenericException(ErrorLevel, Cli, File):
    build_path: str
    unique_id: str
    exc: Exception

    def message(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = "Unhandled error while executing {}".format(node_description)
        return "{prefix}\n{error}".format(
            prefix=ui.red(prefix),
            error=str(self.exc).strip()
        )


@dataclass
class DetailsHandleGenericException(ShowException, DebugLevel, Cli, File):
    def message(self) -> str:
        return ''


@dataclass
class GenericTestFileParse(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"Parsing {self.path}"


@dataclass
class MacroFileParse(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"Parsing {self.path}"


class PartialParsingFullReparseBecauseOfError(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Partial parsing enabled but an error occurred. Switching to a full re-parse."


@dataclass
class PartialParsingExceptionFile(DebugLevel, Cli, File):
    file: str

    def message(self) -> str:
        return f"Partial parsing exception processing file {self.file}"


@dataclass
class PartialParsingFile(DebugLevel, Cli, File):
    file_dict: Dict

    def message(self) -> str:
        return f"PP file: {self.file_dict}"


@dataclass
class PartialParsingException(DebugLevel, Cli, File):
    exc_info: Dict

    def message(self) -> str:
        return f"PP exception info: {self.exc_info}"


class PartialParsingSkipParsing(DebugLevel, Cli, File):
    def message(self) -> str:
        return "Partial parsing enabled, no changes found, skipping parsing"


class PartialParsingMacroChangeStartFullParse(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Change detected to override macro used during parsing. Starting full parse."


class PartialParsingProjectEnvVarsChanged(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Unable to do partial parsing because env vars used in dbt_project.yml have changed"


class PartialParsingProfileEnvVarsChanged(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Unable to do partial parsing because env vars used in profiles.yml have changed"


@dataclass
class PartialParsingDeletedMetric(DebugLevel, Cli, File):
    id: str

    def message(self) -> str:
        return f"Partial parsing: deleted metric {self.id}"


@dataclass
class ManifestWrongMetadataVersion(DebugLevel, Cli, File):
    version: str

    def message(self) -> str:
        return ("Manifest metadata did not contain correct version. "
                f"Contained '{self.version}' instead.")


@dataclass
class PartialParsingVersionMismatch(InfoLevel, Cli, File):
    saved_version: str
    current_version: str

    def message(self) -> str:
        return ("Unable to do partial parsing because of a dbt version mismatch. "
                f"Saved manifest version: {self.saved_version}. "
                f"Current version: {self.current_version}.")


class PartialParsingFailedBecauseConfigChange(InfoLevel, Cli, File):
    def message(self) -> str:
        return ("Unable to do partial parsing because config vars, "
                "config profile, or config target have changed")


class PartialParsingFailedBecauseProfileChange(InfoLevel, Cli, File):
    def message(self) -> str:
        return ("Unable to do partial parsing because profile has changed")


class PartialParsingFailedBecauseNewProjectDependency(InfoLevel, Cli, File):
    def message(self) -> str:
        return ("Unable to do partial parsing because a project dependency has been added")


class PartialParsingFailedBecauseHashChanged(InfoLevel, Cli, File):
    def message(self) -> str:
        return ("Unable to do partial parsing because a project config has changed")


class PartialParsingNotEnabled(DebugLevel, Cli, File):
    def message(self) -> str:
        return ("Partial parsing not enabled")


@dataclass
class ParsedFileLoadFailed(ShowException, DebugLevel, Cli, File):
    path: str
    exc: Exception

    def message(self) -> str:
        return f"Failed to load parsed file from disk at {self.path}: {self.exc}"


class PartialParseSaveFileNotFound(InfoLevel, Cli, File):
    def message(self) -> str:
        return ("Partial parse save file not found. Starting full parse.")


@dataclass
class StaticParserCausedJinjaRendering(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1605: jinja rendering because of STATIC_PARSER flag. file: {self.path}"


# TODO: Experimental/static parser uses these for testing and some may be a good use case for
#       the `TestLevel` logger once we implement it.  Some will probably stay `DebugLevel`.
@dataclass
class UsingExperimentalParser(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1610: conducting experimental parser sample on {self.path}"


@dataclass
class SampleFullJinjaRendering(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1611: conducting full jinja rendering sample on {self.path}"


@dataclass
class StaticParserFallbackJinjaRendering(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1602: parser fallback to jinja rendering on {self.path}"


@dataclass
class StaticParsingMacroOverrideDetected(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1601: detected macro override of ref/source/config in the scope of {self.path}"


@dataclass
class StaticParserSuccess(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1699: static parser successfully parsed {self.path}"


@dataclass
class StaticParserFailure(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1603: static parser failed on {self.path}"


@dataclass
class ExperimentalParserSuccess(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1698: experimental parser successfully parsed {self.path}"


@dataclass
class ExperimentalParserFailure(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"1604: experimental parser failed on {self.path}"


@dataclass
class PartialParsingEnabled(DebugLevel, Cli, File):
    deleted: int
    added: int
    changed: int

    def message(self) -> str:
        return (f"Partial parsing enabled: "
                f"{self.deleted} files deleted, "
                f"{self.added} files added, "
                f"{self.changed} files changed.")


@dataclass
class PartialParsingAddedFile(DebugLevel, Cli, File):
    file_id: str

    def message(self) -> str:
        return f"Partial parsing: added file: {self.file_id}"


@dataclass
class PartialParsingDeletedFile(DebugLevel, Cli, File):
    file_id: str

    def message(self) -> str:
        return f"Partial parsing: deleted file: {self.file_id}"


@dataclass
class PartialParsingUpdatedFile(DebugLevel, Cli, File):
    file_id: str

    def message(self) -> str:
        return f"Partial parsing: updated file: {self.file_id}"


@dataclass
class PartialParsingNodeMissingInSourceFile(DebugLevel, Cli, File):
    source_file: str

    def message(self) -> str:
        return f"Partial parsing: node not found for source_file {self.source_file}"


@dataclass
class PartialParsingMissingNodes(DebugLevel, Cli, File):
    file_id: str

    def message(self) -> str:
        return f"No nodes found for source file {self.file_id}"


@dataclass
class PartialParsingChildMapMissingUniqueID(DebugLevel, Cli, File):
    unique_id: str

    def message(self) -> str:
        return f"Partial parsing: {self.unique_id} not found in child_map"


@dataclass
class PartialParsingUpdateSchemaFile(DebugLevel, Cli, File):
    file_id: str

    def message(self) -> str:
        return f"Partial parsing: update schema file: {self.file_id}"


@dataclass
class PartialParsingDeletedSource(DebugLevel, Cli, File):
    unique_id: str

    def message(self) -> str:
        return f"Partial parsing: deleted source {self.unique_id}"


@dataclass
class PartialParsingDeletedExposure(DebugLevel, Cli, File):
    unique_id: str

    def message(self) -> str:
        return f"Partial parsing: deleted exposure {self.unique_id}"


@dataclass
class InvalidDisabledSourceInTestNode(WarnLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return ui.warning_tag(self.msg)


@dataclass
class InvalidRefInTestNode(WarnLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return ui.warning_tag(self.msg)


@dataclass
class RunningOperationCaughtError(ErrorLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return f'Encountered an error while running operation: {self.exc}'


@dataclass
class RunningOperationUncaughtError(ErrorLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return f'Encountered an error while running operation: {self.exc}'


class DbtProjectError(ErrorLevel, Cli, File):
    def message(self) -> str:
        return "Encountered an error while reading the project:"


@dataclass
class DbtProjectErrorException(ErrorLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return f"  ERROR: {str(self.exc)}"


class DbtProfileError(ErrorLevel, Cli, File):
    def message(self) -> str:
        return "Encountered an error while reading profiles:"


@dataclass
class DbtProfileErrorException(ErrorLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return f"  ERROR: {str(self.exc)}"


class ProfileListTitle(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Defined profiles:"


@dataclass
class ListSingleProfile(InfoLevel, Cli, File):
    profile: str

    def message(self) -> str:
        return f" - {self.profile}"


class NoDefinedProfiles(InfoLevel, Cli, File):
    def message(self) -> str:
        return "There are no profiles defined in your profiles.yml file"


class ProfileHelpMessage(InfoLevel, Cli, File):
    def message(self) -> str:
        PROFILES_HELP_MESSAGE = """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""
        return PROFILES_HELP_MESSAGE


@dataclass
class CatchableExceptionOnRun(ShowException, DebugLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return str(self.exc)


@dataclass
class InternalExceptionOnRun(DebugLevel, Cli, File):
    build_path: str
    exc: Exception

    def message(self) -> str:
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
class PrintDebugStackTrace(ShowException, DebugLevel, Cli, File):
    def message(self) -> str:
        return ""


@dataclass
class GenericExceptionOnRun(ErrorLevel, Cli, File):
    build_path: str
    unique_id: str
    exc: Exception

    def message(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = "Unhandled error while executing {}".format(node_description)
        return "{prefix}\n{error}".format(
            prefix=ui.red(prefix),
            error=str(self.exc).strip()
        )


@dataclass
class NodeConnectionReleaseError(ShowException, DebugLevel, Cli, File):
    node_name: str
    exc: Exception

    def message(self) -> str:
        return ('Error releasing connection for node {}: {!s}'
                .format(self.node_name, self.exc))


@dataclass
class CheckCleanPath(InfoLevel, Cli):
    path: str

    def message(self) -> str:
        return f"Checking {self.path}/*"


@dataclass
class ConfirmCleanPath(InfoLevel, Cli):
    path: str

    def message(self) -> str:
        return f"Cleaned {self.path}/*"


@dataclass
class ProtectedCleanPath(InfoLevel, Cli):
    path: str

    def message(self) -> str:
        return f"ERROR: not cleaning {self.path}/* because it is protected"


class FinishedCleanPaths(InfoLevel, Cli):
    def message(self) -> str:
        return "Finished cleaning all paths."


@dataclass
class OpenCommand(InfoLevel, Cli, File):
    open_cmd: str
    profiles_dir: str

    def message(self) -> str:
        PROFILE_DIR_MESSAGE = """To view your profiles.yml file, run:

{open_cmd} {profiles_dir}"""
        message = PROFILE_DIR_MESSAGE.format(
            open_cmd=self.open_cmd,
            profiles_dir=self.profiles_dir
        )

        return message


class DepsNoPackagesFound(InfoLevel, Cli, File):
    def message(self) -> str:
        return 'Warning: No packages were found in packages.yml'


@dataclass
class DepsStartPackageInstall(InfoLevel, Cli, File):
    package: str

    def message(self) -> str:
        return f"Installing {self.package}"


@dataclass
class DepsInstallInfo(InfoLevel, Cli, File):
    version_name: str

    def message(self) -> str:
        return f"  Installed from {self.version_name}"


@dataclass
class DepsUpdateAvailable(InfoLevel, Cli, File):
    version_latest: str

    def message(self) -> str:
        return f"  Updated version available: {self.version_latest}"


class DepsUTD(InfoLevel, Cli, File):
    def message(self) -> str:
        return "  Up to date!"


@dataclass
class DepsListSubdirectory(InfoLevel, Cli, File):
    subdirectory: str

    def message(self) -> str:
        return f"   and subdirectory {self.subdirectory}"


@dataclass
class DepsNotifyUpdatesAvailable(InfoLevel, Cli, File):
    packages: List[str]

    def message(self) -> str:
        return ('\nUpdates available for packages: {} \
                \nUpdate your versions in packages.yml, then run dbt deps'.format(self.packages))


@dataclass
class DatabaseErrorRunning(InfoLevel, Cli, File):
    hook_type: str

    def message(self) -> str:
        return f"Database error while running {self.hook_type}"


class EmptyLine(InfoLevel, Cli, File):
    def message(self) -> str:
        return ''


@dataclass
class HooksRunning(InfoLevel, Cli, File):
    num_hooks: int
    hook_type: str

    def message(self) -> str:
        plural = 'hook' if self.num_hooks == 1 else 'hooks'
        return f"Running {self.num_hooks} {self.hook_type} {plural}"


@dataclass
class HookFinished(InfoLevel, Cli, File):
    stat_line: str
    execution: str

    def message(self) -> str:
        return f"Finished running {self.stat_line}{self.execution}."


@dataclass
class WriteCatalogFailure(ErrorLevel, Cli, File):
    num_exceptions: int

    def message(self) -> str:
        return (f"dbt encountered {self.num_exceptions} failure{(self.num_exceptions != 1) * 's'} "
                "while writing the catalog")


@dataclass
class CatalogWritten(InfoLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"Catalog written to {self.path}"


class CannotGenerateDocs(InfoLevel, Cli, File):
    def message(self) -> str:
        return "compile failed, cannot generate docs"


class BuildingCatalog(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Building catalog"


class CompileComplete(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Done."


class FreshnessCheckComplete(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Done."


@dataclass
class ServingDocsPort(InfoLevel, Cli, File):
    address: str
    port: int

    def message(self) -> str:
        return f"Serving docs at {self.address}:{self.port}"


@dataclass
class ServingDocsAccessInfo(InfoLevel, Cli, File):
    port: str

    def message(self) -> str:
        return f"To access from your browser, navigate to:  http://localhost:{self.port}"


class ServingDocsExitInfo(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Press Ctrl+C to exit.\n\n"


@dataclass
class SeedHeader(InfoLevel, Cli, File):
    header: str

    def message(self) -> str:
        return self.header


@dataclass
class SeedHeaderSeperator(InfoLevel, Cli, File):
    len_header: int

    def message(self) -> str:
        return "-" * self.len_header


@dataclass
class RunResultWarning(WarnLevel, Cli, File):
    resource_type: str
    node_name: str
    path: str

    def message(self) -> str:
        info = 'Warning'
        return ui.yellow(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class RunResultFailure(ErrorLevel, Cli, File):
    resource_type: str
    node_name: str
    path: str

    def message(self) -> str:
        info = 'Failure'
        return ui.red(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class StatsLine(InfoLevel, Cli, File):
    stats: Dict

    def message(self) -> str:
        stats_line = ("\nDone. PASS={pass} WARN={warn} ERROR={error} SKIP={skip} TOTAL={total}")
        return stats_line.format(**self.stats)


@dataclass
class RunResultError(ErrorLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return f"  {self.msg}"


@dataclass
class RunResultErrorNoMessage(ErrorLevel, Cli, File):
    status: str

    def message(self) -> str:
        return f"  Status: {self.status}"


@dataclass
class SQLCompiledPath(InfoLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"  compiled SQL at {self.path}"


@dataclass
class SQlRunnerException(ShowException, DebugLevel, Cli, File):
    exc: Exception

    def message(self) -> str:
        return f"Got an exception: {self.exc}"


@dataclass
class CheckNodeTestFailure(InfoLevel, Cli, File):
    relation_name: str

    def message(self) -> str:
        msg = f"select * from {self.relation_name}"
        border = '-' * len(msg)
        return f"  See test failures:\n  {border}\n  {msg}\n  {border}"


@dataclass
class FirstRunResultError(ErrorLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return ui.yellow(self.msg)


@dataclass
class AfterFirstRunResultError(ErrorLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return self.msg


@dataclass
class EndOfRunSummary(InfoLevel, Cli, File):
    num_errors: int
    num_warnings: int
    keyboard_interrupt: bool = False

    def message(self) -> str:
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
class PrintStartLine(InfoLevel, Cli, File):
    description: str
    index: int
    total: int

    def message(self) -> str:
        msg = f"START {self.description}"
        return format_fancy_output_line(msg=msg, status='RUN', index=self.index, total=self.total)


@dataclass
class PrintHookStartLine(InfoLevel, Cli, File):
    statement: str
    index: int
    total: int
    truncate: bool

    def message(self) -> str:
        msg = f"START hook: {self.statement}"
        return format_fancy_output_line(msg=msg,
                                        status='RUN',
                                        index=self.index,
                                        total=self.total,
                                        truncate=self.truncate)


@dataclass
class PrintHookEndLine(InfoLevel, Cli, File):
    statement: str
    status: str
    index: int
    total: int
    execution_time: int
    truncate: bool

    def message(self) -> str:
        msg = 'OK hook: {}'.format(self.statement)
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time,
                                        truncate=self.truncate)


@dataclass
class SkippingDetails(InfoLevel, Cli, File):
    resource_type: str
    schema: str
    node_name: str
    index: int
    total: int

    def message(self) -> str:
        if self.resource_type in NodeType.refable():
            msg = f'SKIP relation {self.schema}.{self.node_name}'
        else:
            msg = f'SKIP {self.resource_type} {self.node_name}'
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow('SKIP'),
                                        index=self.index,
                                        total=self.total)


@dataclass
class PrintErrorTestResult(ErrorLevel, Cli, File):
    name: str
    index: int
    num_models: int
    execution_time: int

    def message(self) -> str:
        info = "ERROR"
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintPassTestResult(InfoLevel, Cli, File):
    name: str
    index: int
    num_models: int
    execution_time: int

    def message(self) -> str:
        info = "PASS"
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintWarnTestResult(WarnLevel, Cli, File):
    name: str
    index: int
    num_models: int
    execution_time: int
    failures: List[str]

    def message(self) -> str:
        info = f'WARN {self.failures}'
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintFailureTestResult(ErrorLevel, Cli, File):
    name: str
    index: int
    num_models: int
    execution_time: int
    failures: List[str]

    def message(self) -> str:
        info = f'FAIL {self.failures}'
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintSkipBecauseError(ErrorLevel, Cli, File):
    schema: str
    relation: str
    index: int
    total: int

    def message(self) -> str:
        msg = f'SKIP relation {self.schema}.{self.relation} due to ephemeral model error'
        return format_fancy_output_line(msg=msg,
                                        status=ui.red('ERROR SKIP'),
                                        index=self.index,
                                        total=self.total)


@dataclass
class PrintModelErrorResultLine(ErrorLevel, Cli, File):
    description: str
    status: str
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = "ERROR creating"
        msg = f"{info} {self.description}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintModelResultLine(InfoLevel, Cli, File):
    description: str
    status: str
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = "OK created"
        msg = f"{info} {self.description}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSnapshotErrorResultLine(ErrorLevel, Cli, File):
    status: str
    description: str
    cfg: Dict
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = 'ERROR snapshotting'
        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSnapshotResultLine(InfoLevel, Cli, File):
    status: str
    description: str
    cfg: Dict
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = 'OK snapshotted'
        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSeedErrorResultLine(ErrorLevel, Cli, File):
    status: str
    index: int
    total: int
    execution_time: int
    schema: str
    relation: str

    def message(self) -> str:
        info = 'ERROR loading'
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSeedResultLine(InfoLevel, Cli, File):
    status: str
    index: int
    total: int
    execution_time: int
    schema: str
    relation: str

    def message(self) -> str:
        info = 'OK loaded'
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndErrorLine(ErrorLevel, Cli, File):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = 'ERROR'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndErrorStaleLine(ErrorLevel, Cli, File):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = 'ERROR STALE'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndWarnLine(WarnLevel, Cli, File):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = 'WARN'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndPassLine(InfoLevel, Cli, File):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int

    def message(self) -> str:
        info = 'PASS'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintCancelLine(ErrorLevel, Cli, File):
    conn_name: str

    def message(self) -> str:
        msg = 'CANCEL query {}'.format(self.conn_name)
        return format_fancy_output_line(msg=msg,
                                        status=ui.red('CANCEL'),
                                        index=None,
                                        total=None)


@dataclass
class DefaultSelector(InfoLevel, Cli, File):
    name: str

    def message(self) -> str:
        return f"Using default selector {self.name}"


@dataclass
class NodeStart(DebugLevel, Cli, File):
    unique_id: str

    def message(self) -> str:
        return f"Began running node {self.unique_id}"


@dataclass
class NodeFinished(DebugLevel, Cli, File):
    unique_id: str

    def message(self) -> str:
        return f"Finished running node {self.unique_id}"


@dataclass
class QueryCancelationUnsupported(InfoLevel, Cli, File):
    type: str

    def message(self) -> str:
        msg = (f"The {self.type} adapter does not support query "
               "cancellation. Some queries may still be "
               "running!")
        return ui.yellow(msg)


@dataclass
class ConcurrencyLine(InfoLevel, Cli, File):
    concurrency_line: str

    def message(self) -> str:
        return self.concurrency_line


@dataclass
class StarterProjectPath(DebugLevel, Cli, File):
    dir: str

    def message(self) -> str:
        return f"Starter project path: {self.dir}"


@dataclass
class ConfigFolderDirectory(InfoLevel, Cli, File):
    dir: str

    def message(self) -> str:
        return f"Creating dbt configuration folder at {self.dir}"


@dataclass
class NoSampleProfileFound(InfoLevel, Cli, File):
    adapter: str

    def message(self) -> str:
        return f"No sample profile found for {self.adapter}."


@dataclass
class ProfileWrittenWithSample(InfoLevel, Cli, File):
    name: str
    path: str

    def message(self) -> str:
        return (f"Profile {self.name} written to {self.path} "
                "using target's sample configuration. Once updated, you'll be able to "
                "start developing with dbt.")


@dataclass
class ProfileWrittenWithTargetTemplateYAML(InfoLevel, Cli, File):
    name: str
    path: str

    def message(self) -> str:
        return (f"Profile {self.name} written to {self.path} using target's "
                "profile_template.yml and your supplied values. Run 'dbt debug' to "
                "validate the connection.")


@dataclass
class ProfileWrittenWithProjectTemplateYAML(InfoLevel, Cli, File):
    name: str
    path: str

    def message(self) -> str:
        return (f"Profile {self.name} written to {self.path} using project's "
                "profile_template.yml and your supplied values. Run 'dbt debug' to "
                "validate the connection.")


class SettingUpProfile(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Setting up your profile."


class InvalidProfileTemplateYAML(InfoLevel, Cli, File):
    def message(self) -> str:
        return "Invalid profile_template.yml in project."


@dataclass
class ProjectNameAlreadyExists(InfoLevel, Cli, File):
    name: str

    def message(self) -> str:
        return f"A project called {self.name} already exists here."


@dataclass
class GetAddendum(InfoLevel, Cli, File):
    msg: str

    def message(self) -> str:
        return self.msg


@dataclass
class DepsSetDownloadDirectory(DebugLevel, Cli, File):
    path: str

    def message(self) -> str:
        return f"Set downloads directory='{self.path}'"


class EnsureGitInstalled(ErrorLevel, Cli, File):
    def message(self) -> str:
        return ('Make sure git is installed on your machine. More '
                'information: '
                'https://docs.getdbt.com/docs/package-management')


class DepsCreatingLocalSymlink(DebugLevel, Cli, File):
    def message(self) -> str:
        return '  Creating symlink to local dependency.'


class DepsSymlinkNotAvailable(DebugLevel, Cli, File):
    def message(self) -> str:
        return '  Symlinks are not available on this OS, copying dependency.'


@dataclass
class FoundStats(InfoLevel, Cli, File):
    stat_line: str

    def message(self) -> str:
        return f"Found {self.stat_line}"


@dataclass
class CompilingNode(DebugLevel, Cli, File):
    unique_id: str

    def message(self) -> str:
        return f"Compiling {self.unique_id}"


@dataclass
class WritingInjectedSQLForNode(DebugLevel, Cli, File):
    unique_id: str

    def message(self) -> str:
        return f'Writing injected SQL for node "{self.unique_id}"'


class DisableTracking(WarnLevel, Cli, File):
    def message(self) -> str:
        return "Error sending message, disabling tracking"


@dataclass
class SendingEvent(DebugLevel, Cli):
    kwargs: str

    def message(self) -> str:
        return f"Sending event: {self.kwargs}"


class SendEventFailure(DebugLevel, Cli, File):
    def message(self) -> str:
        return "An error was encountered while trying to send an event"


class FlushEvents(DebugLevel, Cli):
    def message(self) -> str:
        return "Flushing usage events"


class FlushEventsFailure(DebugLevel, Cli):
    def message(self) -> str:
        return "An error was encountered while trying to flush usage events"


class TrackingInitializeFailure(ShowException, DebugLevel, Cli, File):
    def message(self) -> str:
        return "Got an exception trying to initialize tracking"


@dataclass
class RetryExternalCall(DebugLevel, Cli, File):
    attempt: int
    max: int

    def message(self) -> str:
        return f"Retrying external call. Attempt: {self.attempt} Max attempts: {self.max}"


@dataclass
class GeneralWarningMsg(WarnLevel, Cli, File):
    msg: str
    log_fmt: str

    def message(self) -> str:
        if self.log_fmt is not None:
            return self.log_fmt.format(self.msg)
        return self.msg


@dataclass
class GeneralWarningException(WarnLevel, Cli, File):
    exc: Exception
    log_fmt: str

    def message(self) -> str:
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
    def dump_callable():
        return {"": [""]}  # for instantiating `Dump...` methods which take callables.

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
    ReportPerformancePath(path="")
    GitSparseCheckoutSubdirectory(subdir="")
    GitProgressCheckoutRevision(revision="")
    GitProgressUpdatingExistingDependency(dir="")
    GitProgressPullingNewDependency(dir="")
    GitNothingToDo(sha="")
    GitProgressUpdatedCheckoutRange(start_sha="", end_sha="")
    GitProgressCheckedOutAt(end_sha="")
    SystemErrorRetrievingModTime(path="")
    SystemCouldNotWrite(path="", reason="", exc=Exception(""))
    SystemExecutingCmd(cmd=[""])
    SystemStdOutMsg(bmsg=b"")
    SystemStdErrMsg(bmsg=b"")
    SystemReportReturnCode(code=0)
    SelectorReportInvalidSelector(
        selector_methods={"": ""}, spec_method="", raw_spec=""
    )
    MacroEventInfo(msg="")
    MacroEventDebug(msg="")
    NewConnection(conn_type="", conn_name="")
    ConnectionReused(conn_name="")
    ConnectionLeftOpen(conn_name="")
    ConnectionClosed(conn_name="")
    RollbackFailed(conn_name="")
    ConnectionClosed2(conn_name="")
    ConnectionLeftOpen2(conn_name="")
    Rollback(conn_name="")
    CacheMiss(conn_name="", database="", schema="")
    ListRelations(database="", schema="", relations=[])
    ConnectionUsed(conn_type="", conn_name="")
    SQLQuery(conn_name="", sql="")
    SQLQueryStatus(status="", elapsed=0.1)
    SQLCommit(conn_name="")
    ColTypeChange(orig_type="", new_type="", table="")
    SchemaCreation(relation=BaseRelation())
    SchemaDrop(relation=BaseRelation())
    UncachedRelation(
        dep_key=_ReferenceKey(database="", schema="", identifier=""),
        ref_key=_ReferenceKey(database="", schema="", identifier=""),
    )
    AddLink(
        dep_key=_ReferenceKey(database="", schema="", identifier=""),
        ref_key=_ReferenceKey(database="", schema="", identifier=""),
    )
    AddRelation(relation=_CachedRelation())
    DropMissingRelation(relation=_ReferenceKey(database="", schema="", identifier=""))
    DropCascade(
        dropped=_ReferenceKey(database="", schema="", identifier=""),
        consequences={_ReferenceKey(database="", schema="", identifier="")},
    )
    UpdateReference(
        old_key=_ReferenceKey(database="", schema="", identifier=""),
        new_key=_ReferenceKey(database="", schema="", identifier=""),
        cached_key=_ReferenceKey(database="", schema="", identifier=""),
    )
    TemporaryRelation(key=_ReferenceKey(database="", schema="", identifier=""))
    RenameSchema(
        old_key=_ReferenceKey(database="", schema="", identifier=""),
        new_key=_ReferenceKey(database="", schema="", identifier="")
    )
    DumpBeforeAddGraph(dump_callable)
    DumpAfterAddGraph(dump_callable)
    DumpBeforeRenameSchema(dump_callable)
    DumpAfterRenameSchema(dump_callable)
    AdapterImportError(ModuleNotFoundError())
    PluginLoadError()
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
    PartialParsingDeletedMetric('')
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
