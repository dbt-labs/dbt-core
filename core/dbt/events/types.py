import argparse
from dataclasses import dataclass
from dbt.events.stubs import (
    _CachedRelation,
    BaseRelation,
    ParsedModelNode,
    ParsedHookNode,
    _ReferenceKey,
    RunResult
)
from dbt import ui
from dbt.events.base_types import (
    Cli, Event, File, DebugLevel, InfoLevel, WarnLevel, ErrorLevel, ShowException, NodeInfo, Cache
)
from dbt.events.format import format_fancy_output_line, pluralize
from dbt.node_types import NodeType
from typing import Any, Dict, List, Optional, Set, Tuple, TypeVar


# The classes in this file represent the data necessary to describe a
# particular event to both human readable logs, and machine reliable
# event streams. classes extend superclasses that indicate what
# destinations they are intended for, which mypy uses to enforce
# that the necessary methods are defined.


# Type representing Event and all subclasses of Event
T_Event = TypeVar('T_Event', bound=Event)


# Event codes have prefixes which follow this table
#
# | Code |     Description     |
# |:----:|:-------------------:|
# | A    | Pre-project loading |
# | E    | DB adapter          |
# | I    | Project parsing     |
# | M    | Deps generation     |
# | Q    | Node processing     |
# | W    | Node testing        |
# | Y    | Post processing     |
# | Z    | Misc                |
# | T    | Test only           |
#
# The basic idea is that event codes roughly translate to the natural order of running a dbt task

# TODO: remove ingore when this is fixed:
# https://github.com/python/mypy/issues/5374

@dataclass  # type: ignore
class AdapterEventBase(Cli, File):
    name: str
    base_msg: str
    args: Tuple[Any, ...]

    # instead of having this inherit from one of the level classes
    def level_tag(self) -> str:
        raise Exception("level_tag should never be called on AdapterEventBase")

    def message(self) -> str:
        # this class shouldn't be createable, but we can't make it an ABC because of a mypy bug
        if type(self).__name__ == 'AdapterEventBase':
            raise Exception(
                'attempted to create a message for AdapterEventBase which cannot be created'
            )

        # only apply formatting if there are arguments to format.
        # avoids issues like "dict: {k: v}".format() which results in `KeyError 'k'`
        msg = (
            self.base_msg
            if len(self.args) == 0 else
            self.base_msg.format(*self.args)
        )
        return f"{self.name} adapter: {msg}"


@dataclass
class AdapterEventDebug(DebugLevel, AdapterEventBase, ShowException):
    code: str = "E001"
    pass


@dataclass
class AdapterEventInfo(InfoLevel, AdapterEventBase, ShowException):
    code: str = "E002"
    pass


@dataclass
class AdapterEventWarning(WarnLevel, AdapterEventBase, ShowException):
    code: str = "E003"
    pass


@dataclass
class AdapterEventError(ErrorLevel, AdapterEventBase, ShowException):
    code: str = "E004"
    pass


@dataclass
class MainKeyboardInterrupt(InfoLevel, Cli):
    code: str = "Z001"

    def message(self) -> str:
        return "ctrl-c"


# will log to a file if the file logger is configured
@dataclass
class MainEncounteredError(ErrorLevel, Cli):
    e: BaseException
    code: str = "Z002"

    def message(self) -> str:
        return f"Encountered an error:\n{str(self.e)}"

    # overriding default json serialization for this event
    def fields_to_json(self, val: Any) -> Any:
        # equality on BaseException is not good enough of a comparison here
        if isinstance(val, BaseException):
            return str(val)

        return val


@dataclass
class MainStackTrace(DebugLevel, Cli):
    stack_trace: str
    code: str = "Z003"

    def message(self) -> str:
        return self.stack_trace


@dataclass
class MainReportVersion(InfoLevel, Cli, File):
    v: str  # could be VersionSpecifier instead if we resolved some circular imports
    code: str = "A001"

    def message(self):
        return f"Running with dbt{self.v}"


@dataclass
class MainReportArgs(DebugLevel, Cli, File):
    args: argparse.Namespace
    code: str = "A002"

    def message(self):
        return f"running dbt with arguments {str(self.args)}"

    # overriding default json serialization for this event
    def fields_to_json(self, val: Any) -> Any:
        if isinstance(val, argparse.Namespace):
            return str(val)

        return val


@dataclass
class MainTrackingUserState(DebugLevel, Cli, File):
    user_state: str
    code: str = "A003"

    def message(self):
        return f"Tracking: {self.user_state}"


@dataclass
class ParsingStart(InfoLevel, Cli, File):
    code: str = "I001"

    def message(self) -> str:
        return "Start parsing."


@dataclass
class ParsingCompiling(InfoLevel, Cli, File):
    code: str = "I002"

    def message(self) -> str:
        return "Compiling."


@dataclass
class ParsingWritingManifest(InfoLevel, Cli, File):
    code: str = "I003"

    def message(self) -> str:
        return "Writing manifest."


@dataclass
class ParsingDone(InfoLevel, Cli, File):
    code: str = "I004"

    def message(self) -> str:
        return "Done."


@dataclass
class ManifestDependenciesLoaded(InfoLevel, Cli, File):
    code: str = "I005"

    def message(self) -> str:
        return "Dependencies loaded"


@dataclass
class ManifestLoaderCreated(InfoLevel, Cli, File):
    code: str = "I006"

    def message(self) -> str:
        return "ManifestLoader created"


@dataclass
class ManifestLoaded(InfoLevel, Cli, File):
    code: str = "I007"

    def message(self) -> str:
        return "Manifest loaded"


@dataclass
class ManifestChecked(InfoLevel, Cli, File):
    code: str = "I008"

    def message(self) -> str:
        return "Manifest checked"


@dataclass
class ManifestFlatGraphBuilt(InfoLevel, Cli, File):
    code: str = "I009"

    def message(self) -> str:
        return "Flat graph built"


@dataclass
class ReportPerformancePath(InfoLevel, Cli, File):
    path: str
    code: str = "I010"

    def message(self) -> str:
        return f"Performance info: {self.path}"


@dataclass
class GitSparseCheckoutSubdirectory(DebugLevel, Cli, File):
    subdir: str
    code: str = "M001"

    def message(self) -> str:
        return f"  Subdirectory specified: {self.subdir}, using sparse checkout."


@dataclass
class GitProgressCheckoutRevision(DebugLevel, Cli, File):
    revision: str
    code: str = "M002"

    def message(self) -> str:
        return f"  Checking out revision {self.revision}."


@dataclass
class GitProgressUpdatingExistingDependency(DebugLevel, Cli, File):
    dir: str
    code: str = "M003"

    def message(self) -> str:
        return f"Updating existing dependency {self.dir}."


@dataclass
class GitProgressPullingNewDependency(DebugLevel, Cli, File):
    dir: str
    code: str = "M004"

    def message(self) -> str:
        return f"Pulling new dependency {self.dir}."


@dataclass
class GitNothingToDo(DebugLevel, Cli, File):
    sha: str
    code: str = "M005"

    def message(self) -> str:
        return f"Already at {self.sha}, nothing to do."


@dataclass
class GitProgressUpdatedCheckoutRange(DebugLevel, Cli, File):
    start_sha: str
    end_sha: str
    code: str = "M006"

    def message(self) -> str:
        return f"  Updated checkout from {self.start_sha} to {self.end_sha}."


@dataclass
class GitProgressCheckedOutAt(DebugLevel, Cli, File):
    end_sha: str
    code: str = "M007"

    def message(self) -> str:
        return f"  Checked out at {self.end_sha}."


@dataclass
class RegistryProgressMakingGETRequest(DebugLevel, Cli, File):
    url: str
    code: str = "M008"

    def message(self) -> str:
        return f"Making package registry request: GET {self.url}"


@dataclass
class RegistryProgressGETResponse(DebugLevel, Cli, File):
    url: str
    resp_code: int
    code: str = "M009"

    def message(self) -> str:
        return f"Response from registry: GET {self.url} {self.resp_code}"


# TODO this was actually `logger.exception(...)` not `logger.error(...)`
@dataclass
class SystemErrorRetrievingModTime(ErrorLevel, Cli, File):
    path: str
    code: str = "Z004"

    def message(self) -> str:
        return f"Error retrieving modification time for file {self.path}"


@dataclass
class SystemCouldNotWrite(DebugLevel, Cli, File):
    path: str
    reason: str
    exc: Exception
    code: str = "Z005"

    def message(self) -> str:
        return (
            f"Could not write to path {self.path}({len(self.path)} characters): "
            f"{self.reason}\nexception: {self.exc}"
        )

    # overriding default json serialization for this event
    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class SystemExecutingCmd(DebugLevel, Cli, File):
    cmd: List[str]
    code: str = "Z006"

    def message(self) -> str:
        return f'Executing "{" ".join(self.cmd)}"'


@dataclass
class SystemStdOutMsg(DebugLevel, Cli, File):
    bmsg: bytes
    code: str = "Z007"

    def message(self) -> str:
        return f'STDOUT: "{str(self.bmsg)}"'


@dataclass
class SystemStdErrMsg(DebugLevel, Cli, File):
    bmsg: bytes
    code: str = "Z008"

    def message(self) -> str:
        return f'STDERR: "{str(self.bmsg)}"'


@dataclass
class SystemReportReturnCode(DebugLevel, Cli, File):
    returncode: int
    code: str = "Z009"

    def message(self) -> str:
        return f"command return code={self.returncode}"

# TODO remove?? Not called outside of this file


@dataclass
class SelectorAlertUpto3UnusedNodes(InfoLevel, Cli, File):
    node_names: List[str]
    code: str = "I_NEED_A_CODE_5"

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

# TODO remove?? Not called outside of this file


@dataclass
class SelectorAlertAllUnusedNodes(DebugLevel, Cli, File):
    node_names: List[str]
    code: str = "I_NEED_A_CODE_6"

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
    code: str = "M010"

    def message(self) -> str:
        valid_selectors = ", ".join(self.selector_methods)
        return (
            f"The '{self.spec_method}' selector specified in {self.raw_spec} is "
            f"invalid. Must be one of [{valid_selectors}]"
        )


@dataclass
class MacroEventInfo(InfoLevel, Cli, File):
    msg: str
    code: str = "M011"

    def message(self) -> str:
        return self.msg


@dataclass
class MacroEventDebug(DebugLevel, Cli, File):
    msg: str
    code: str = "M012"

    def message(self) -> str:
        return self.msg


@dataclass
class NewConnection(DebugLevel, Cli, File):
    conn_type: str
    conn_name: str
    code: str = "E005"

    def message(self) -> str:
        return f'Acquiring new {self.conn_type} connection "{self.conn_name}"'


@dataclass
class ConnectionReused(DebugLevel, Cli, File):
    conn_name: str
    code: str = "E006"

    def message(self) -> str:
        return f"Re-using an available connection from the pool (formerly {self.conn_name})"


@dataclass
class ConnectionLeftOpen(DebugLevel, Cli, File):
    conn_name: Optional[str]
    code: str = "E007"

    def message(self) -> str:
        return f"Connection '{self.conn_name}' was left open."


@dataclass
class ConnectionClosed(DebugLevel, Cli, File):
    conn_name: Optional[str]
    code: str = "E008"

    def message(self) -> str:
        return f"Connection '{self.conn_name}' was properly closed."


@dataclass
class RollbackFailed(ShowException, DebugLevel, Cli, File):
    conn_name: Optional[str]
    code: str = "E009"

    def message(self) -> str:
        return f"Failed to rollback '{self.conn_name}'"


# TODO: can we combine this with ConnectionClosed?
@dataclass
class ConnectionClosed2(DebugLevel, Cli, File):
    conn_name: Optional[str]
    code: str = "E010"

    def message(self) -> str:
        return f"On {self.conn_name}: Close"


# TODO: can we combine this with ConnectionLeftOpen?
@dataclass
class ConnectionLeftOpen2(DebugLevel, Cli, File):
    conn_name: Optional[str]
    code: str = "E011"

    def message(self) -> str:
        return f"On {self.conn_name}: No close available on handle"


@dataclass
class Rollback(DebugLevel, Cli, File):
    conn_name: Optional[str]
    code: str = "E012"

    def message(self) -> str:
        return f"On {self.conn_name}: ROLLBACK"


@dataclass
class CacheMiss(DebugLevel, Cli, File):
    conn_name: Any  # TODO mypy says this is `Callable[[], str]`??  ¯\_(ツ)_/¯
    database: Optional[str]
    schema: str
    code: str = "E013"

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
    code: str = "E014"

    def message(self) -> str:
        return f"with database={self.database}, schema={self.schema}, relations={self.relations}"


@dataclass
class ConnectionUsed(DebugLevel, Cli, File):
    conn_type: str
    conn_name: Optional[str]
    code: str = "E015"

    def message(self) -> str:
        return f'Using {self.conn_type} connection "{self.conn_name}"'


@dataclass
class SQLQuery(DebugLevel, Cli, File):
    conn_name: Optional[str]
    sql: str
    code: str = "E016"

    def message(self) -> str:
        return f"On {self.conn_name}: {self.sql}"


@dataclass
class SQLQueryStatus(DebugLevel, Cli, File):
    status: str  # could include AdapterResponse if we resolve circular imports
    elapsed: float
    code: str = "E017"

    def message(self) -> str:
        return f"SQL status: {self.status} in {self.elapsed} seconds"


@dataclass
class SQLCommit(DebugLevel, Cli, File):
    conn_name: str
    code: str = "E018"

    def message(self) -> str:
        return f"On {self.conn_name}: COMMIT"


@dataclass
class ColTypeChange(DebugLevel, Cli, File):
    orig_type: str
    new_type: str
    table: str
    code: str = "E019"

    def message(self) -> str:
        return f"Changing col type from {self.orig_type} to {self.new_type} in table {self.table}"


@dataclass
class SchemaCreation(DebugLevel, Cli, File):
    relation: BaseRelation
    code: str = "E020"

    def message(self) -> str:
        return f'Creating schema "{self.relation}"'


@dataclass
class SchemaDrop(DebugLevel, Cli, File):
    relation: BaseRelation
    code: str = "E021"

    def message(self) -> str:
        return f'Dropping schema "{self.relation}".'


# TODO pretty sure this is only ever called in dead code
# see: core/dbt/adapters/cache.py _add_link vs add_link
@dataclass
class UncachedRelation(DebugLevel, Cli, File, Cache):
    dep_key: _ReferenceKey
    ref_key: _ReferenceKey
    code: str = "E022"

    def message(self) -> str:
        return (
            f"{self.dep_key} references {str(self.ref_key)} "
            "but {self.ref_key.database}.{self.ref_key.schema}"
            "is not in the cache, skipping assumed external relation"
        )


@dataclass
class AddLink(DebugLevel, Cli, File, Cache):
    dep_key: _ReferenceKey
    ref_key: _ReferenceKey
    code: str = "E023"

    def message(self) -> str:
        return f"adding link, {self.dep_key} references {self.ref_key}"


@dataclass
class AddRelation(DebugLevel, Cli, File, Cache):
    relation: _CachedRelation
    code: str = "E024"

    def message(self) -> str:
        return f"Adding relation: {str(self.relation)}"

    # overriding default json serialization for this event
    def fields_to_json(self, val: Any) -> Any:
        if isinstance(val, _CachedRelation):
            return str(val)

        return val


@dataclass
class DropMissingRelation(DebugLevel, Cli, File, Cache):
    relation: _ReferenceKey
    code: str = "E025"

    def message(self) -> str:
        return f"dropped a nonexistent relationship: {str(self.relation)}"


@dataclass
class DropCascade(DebugLevel, Cli, File, Cache):
    dropped: _ReferenceKey
    consequences: Set[_ReferenceKey]
    code: str = "E026"

    def message(self) -> str:
        return f"drop {self.dropped} is cascading to {self.consequences}"


@dataclass
class DropRelation(DebugLevel, Cli, File, Cache):
    dropped: _ReferenceKey
    code: str = "E027"

    def message(self) -> str:
        return f"Dropping relation: {self.dropped}"


@dataclass
class UpdateReference(DebugLevel, Cli, File, Cache):
    old_key: _ReferenceKey
    new_key: _ReferenceKey
    cached_key: _ReferenceKey
    code: str = "E028"

    def message(self) -> str:
        return f"updated reference from {self.old_key} -> {self.cached_key} to "\
            "{self.new_key} -> {self.cached_key}"


@dataclass
class TemporaryRelation(DebugLevel, Cli, File, Cache):
    key: _ReferenceKey
    code: str = "E029"

    def message(self) -> str:
        return f"old key {self.key} not found in self.relations, assuming temporary"


@dataclass
class RenameSchema(DebugLevel, Cli, File, Cache):
    old_key: _ReferenceKey
    new_key: _ReferenceKey
    code: str = "E030"

    def message(self) -> str:
        return f"Renaming relation {self.old_key} to {self.new_key}"


@dataclass
class DumpBeforeAddGraph(DebugLevel, Cli, File, Cache):
    # large value. delay not necessary since every debug level message is logged anyway.
    dump: Dict[str, List[str]]
    code: str = "E031"

    def message(self) -> str:
        return f"before adding : {self.dump}"


@dataclass
class DumpAfterAddGraph(DebugLevel, Cli, File, Cache):
    # large value. delay not necessary since every debug level message is logged anyway.
    dump: Dict[str, List[str]]
    code: str = "E032"

    def message(self) -> str:
        return f"after adding: {self.dump}"


@dataclass
class DumpBeforeRenameSchema(DebugLevel, Cli, File, Cache):
    # large value. delay not necessary since every debug level message is logged anyway.
    dump: Dict[str, List[str]]
    code: str = "E033"

    def message(self) -> str:
        return f"before rename: {self.dump}"


@dataclass
class DumpAfterRenameSchema(DebugLevel, Cli, File, Cache):
    # large value. delay not necessary since every debug level message is logged anyway.
    dump: Dict[str, List[str]]
    code: str = "E034"

    def message(self) -> str:
        return f"after rename: {self.dump}"


@dataclass
class AdapterImportError(InfoLevel, Cli, File):
    exc: ModuleNotFoundError
    code: str = "E035"

    def message(self) -> str:
        return f"Error importing adapter: {self.exc}"

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val())

        return val


@dataclass
class PluginLoadError(ShowException, DebugLevel, Cli, File):
    code: str = "E036"

    def message(self):
        pass


@dataclass
class NewConnectionOpening(DebugLevel, Cli, File):
    connection_state: str
    code: str = "E037"

    def message(self) -> str:
        return f"Opening a new connection, currently in state {self.connection_state}"


@dataclass
class TimingInfoCollected(DebugLevel, Cli, File):
    code: str = "Z010"

    def message(self) -> str:
        return "finished collecting timing info"


@dataclass
class MergedFromState(DebugLevel, Cli, File):
    nbr_merged: int
    sample: List
    code: str = "A004"

    def message(self) -> str:
        return f"Merged {self.nbr_merged} items from state (sample: {self.sample})"


@dataclass
class MissingProfileTarget(InfoLevel, Cli, File):
    profile_name: str
    target_name: str
    code: str = "A005"

    def message(self) -> str:
        return f"target not specified in profile '{self.profile_name}', using '{self.target_name}'"


@dataclass
class ProfileLoadError(ShowException, DebugLevel, Cli, File):
    exc: Exception
    code: str = "A006"

    def message(self) -> str:
        return f"Profile not loaded due to error: {self.exc}"

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class ProfileNotFound(InfoLevel, Cli, File):
    profile_name: Optional[str]
    code: str = "A007"

    def message(self) -> str:
        return f'No profile "{self.profile_name}" found, continuing with no target'


@dataclass
class InvalidVarsYAML(ErrorLevel, Cli, File):
    code: str = "A008"

    def message(self) -> str:
        return "The YAML provided in the --vars argument is not valid.\n"


# TODO: Remove? (appears to be uncalled)
@dataclass
class CatchRunException(ShowException, DebugLevel, Cli, File):
    build_path: Any
    exc: Exception
    code: str = "I_NEED_A_CODE_1"

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

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


# TODO: Remove? (appears to be uncalled)
@dataclass
class HandleInternalException(ShowException, DebugLevel, Cli, File):
    exc: Exception
    code: str = "I_NEED_A_CODE_2"

    def message(self) -> str:
        return str(self.exc)

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val

# TODO: Remove? (appears to be uncalled)


@dataclass
class MessageHandleGenericException(ErrorLevel, Cli, File):
    build_path: str
    unique_id: str
    exc: Exception
    code: str = "I_NEED_A_CODE_3"

    def message(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = "Unhandled error while executing {}".format(node_description)
        return "{prefix}\n{error}".format(
            prefix=ui.red(prefix),
            error=str(self.exc).strip()
        )

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val

# TODO: Remove? (appears to be uncalled)


@dataclass
class DetailsHandleGenericException(ShowException, DebugLevel, Cli, File):
    code: str = "I_NEED_A_CODE_4"

    def message(self) -> str:
        return ''


@dataclass
class GenericTestFileParse(DebugLevel, Cli, File):
    path: str
    code: str = "I011"

    def message(self) -> str:
        return f"Parsing {self.path}"


@dataclass
class MacroFileParse(DebugLevel, Cli, File):
    path: str
    code: str = "I012"

    def message(self) -> str:
        return f"Parsing {self.path}"


@dataclass
class PartialParsingFullReparseBecauseOfError(InfoLevel, Cli, File):
    code: str = "I013"

    def message(self) -> str:
        return "Partial parsing enabled but an error occurred. Switching to a full re-parse."


@dataclass
class PartialParsingExceptionFile(DebugLevel, Cli, File):
    file: str
    code: str = "I014"

    def message(self) -> str:
        return f"Partial parsing exception processing file {self.file}"


@dataclass
class PartialParsingFile(DebugLevel, Cli, File):
    file_dict: Dict
    code: str = "I015"

    def message(self) -> str:
        return f"PP file: {self.file_dict}"


@dataclass
class PartialParsingException(DebugLevel, Cli, File):
    exc_info: Dict
    code: str = "I016"

    def message(self) -> str:
        return f"PP exception info: {self.exc_info}"


@dataclass
class PartialParsingSkipParsing(DebugLevel, Cli, File):
    code: str = "I017"

    def message(self) -> str:
        return "Partial parsing enabled, no changes found, skipping parsing"


@dataclass
class PartialParsingMacroChangeStartFullParse(InfoLevel, Cli, File):
    code: str = "I018"

    def message(self) -> str:
        return "Change detected to override macro used during parsing. Starting full parse."


@dataclass
class PartialParsingProjectEnvVarsChanged(InfoLevel, Cli, File):
    code: str = "I019"

    def message(self) -> str:
        return "Unable to do partial parsing because env vars used in dbt_project.yml have changed"


@dataclass
class PartialParsingProfileEnvVarsChanged(InfoLevel, Cli, File):
    code: str = "I020"

    def message(self) -> str:
        return "Unable to do partial parsing because env vars used in profiles.yml have changed"


@dataclass
class PartialParsingDeletedMetric(DebugLevel, Cli, File):
    id: str
    code: str = "I021"

    def message(self) -> str:
        return f"Partial parsing: deleted metric {self.id}"


@dataclass
class ManifestWrongMetadataVersion(DebugLevel, Cli, File):
    version: str
    code: str = "I022"

    def message(self) -> str:
        return ("Manifest metadata did not contain correct version. "
                f"Contained '{self.version}' instead.")


@dataclass
class PartialParsingVersionMismatch(InfoLevel, Cli, File):
    saved_version: str
    current_version: str
    code: str = "I023"

    def message(self) -> str:
        return ("Unable to do partial parsing because of a dbt version mismatch. "
                f"Saved manifest version: {self.saved_version}. "
                f"Current version: {self.current_version}.")


@dataclass
class PartialParsingFailedBecauseConfigChange(InfoLevel, Cli, File):
    code: str = "I024"

    def message(self) -> str:
        return ("Unable to do partial parsing because config vars, "
                "config profile, or config target have changed")


@dataclass
class PartialParsingFailedBecauseProfileChange(InfoLevel, Cli, File):
    code: str = "I025"

    def message(self) -> str:
        return ("Unable to do partial parsing because profile has changed")


@dataclass
class PartialParsingFailedBecauseNewProjectDependency(InfoLevel, Cli, File):
    code: str = "I026"

    def message(self) -> str:
        return ("Unable to do partial parsing because a project dependency has been added")


@dataclass
class PartialParsingFailedBecauseHashChanged(InfoLevel, Cli, File):
    code: str = "I027"

    def message(self) -> str:
        return ("Unable to do partial parsing because a project config has changed")


@dataclass
class PartialParsingNotEnabled(DebugLevel, Cli, File):
    code: str = "I028"

    def message(self) -> str:
        return ("Partial parsing not enabled")


@dataclass
class ParsedFileLoadFailed(ShowException, DebugLevel, Cli, File):
    path: str
    exc: Exception
    code: str = "I029"

    def message(self) -> str:
        return f"Failed to load parsed file from disk at {self.path}: {self.exc}"

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class PartialParseSaveFileNotFound(InfoLevel, Cli, File):
    code: str = "I030"

    def message(self) -> str:
        return ("Partial parse save file not found. Starting full parse.")


@dataclass
class StaticParserCausedJinjaRendering(DebugLevel, Cli, File):
    path: str
    code: str = "I031"

    def message(self) -> str:
        return f"1605: jinja rendering because of STATIC_PARSER flag. file: {self.path}"


# TODO: Experimental/static parser uses these for testing and some may be a good use case for
#       the `TestLevel` logger once we implement it.  Some will probably stay `DebugLevel`.
@dataclass
class UsingExperimentalParser(DebugLevel, Cli, File):
    path: str
    code: str = "I032"

    def message(self) -> str:
        return f"1610: conducting experimental parser sample on {self.path}"


@dataclass
class SampleFullJinjaRendering(DebugLevel, Cli, File):
    path: str
    code: str = "I033"

    def message(self) -> str:
        return f"1611: conducting full jinja rendering sample on {self.path}"


@dataclass
class StaticParserFallbackJinjaRendering(DebugLevel, Cli, File):
    path: str
    code: str = "I034"

    def message(self) -> str:
        return f"1602: parser fallback to jinja rendering on {self.path}"


@dataclass
class StaticParsingMacroOverrideDetected(DebugLevel, Cli, File):
    path: str
    code: str = "I035"

    def message(self) -> str:
        return f"1601: detected macro override of ref/source/config in the scope of {self.path}"


@dataclass
class StaticParserSuccess(DebugLevel, Cli, File):
    path: str
    code: str = "I036"

    def message(self) -> str:
        return f"1699: static parser successfully parsed {self.path}"


@dataclass
class StaticParserFailure(DebugLevel, Cli, File):
    path: str
    code: str = "I037"

    def message(self) -> str:
        return f"1603: static parser failed on {self.path}"


@dataclass
class ExperimentalParserSuccess(DebugLevel, Cli, File):
    path: str
    code: str = "I038"

    def message(self) -> str:
        return f"1698: experimental parser successfully parsed {self.path}"


@dataclass
class ExperimentalParserFailure(DebugLevel, Cli, File):
    path: str
    code: str = "I039"

    def message(self) -> str:
        return f"1604: experimental parser failed on {self.path}"


@dataclass
class PartialParsingEnabled(DebugLevel, Cli, File):
    deleted: int
    added: int
    changed: int
    code: str = "I040"

    def message(self) -> str:
        return (f"Partial parsing enabled: "
                f"{self.deleted} files deleted, "
                f"{self.added} files added, "
                f"{self.changed} files changed.")


@dataclass
class PartialParsingAddedFile(DebugLevel, Cli, File):
    file_id: str
    code: str = "I041"

    def message(self) -> str:
        return f"Partial parsing: added file: {self.file_id}"


@dataclass
class PartialParsingDeletedFile(DebugLevel, Cli, File):
    file_id: str
    code: str = "I042"

    def message(self) -> str:
        return f"Partial parsing: deleted file: {self.file_id}"


@dataclass
class PartialParsingUpdatedFile(DebugLevel, Cli, File):
    file_id: str
    code: str = "I043"

    def message(self) -> str:
        return f"Partial parsing: updated file: {self.file_id}"


@dataclass
class PartialParsingNodeMissingInSourceFile(DebugLevel, Cli, File):
    source_file: str
    code: str = "I044"

    def message(self) -> str:
        return f"Partial parsing: node not found for source_file {self.source_file}"


@dataclass
class PartialParsingMissingNodes(DebugLevel, Cli, File):
    file_id: str
    code: str = "I045"

    def message(self) -> str:
        return f"No nodes found for source file {self.file_id}"


@dataclass
class PartialParsingChildMapMissingUniqueID(DebugLevel, Cli, File):
    unique_id: str
    code: str = "I046"

    def message(self) -> str:
        return f"Partial parsing: {self.unique_id} not found in child_map"


@dataclass
class PartialParsingUpdateSchemaFile(DebugLevel, Cli, File):
    file_id: str
    code: str = "I047"

    def message(self) -> str:
        return f"Partial parsing: update schema file: {self.file_id}"


@dataclass
class PartialParsingDeletedSource(DebugLevel, Cli, File):
    unique_id: str
    code: str = "I048"

    def message(self) -> str:
        return f"Partial parsing: deleted source {self.unique_id}"


@dataclass
class PartialParsingDeletedExposure(DebugLevel, Cli, File):
    unique_id: str
    code: str = "I049"

    def message(self) -> str:
        return f"Partial parsing: deleted exposure {self.unique_id}"


@dataclass
class InvalidDisabledSourceInTestNode(WarnLevel, Cli, File):
    msg: str
    code: str = "I050"

    def message(self) -> str:
        return ui.warning_tag(self.msg)


@dataclass
class InvalidRefInTestNode(WarnLevel, Cli, File):
    msg: str
    code: str = "I051"

    def message(self) -> str:
        return ui.warning_tag(self.msg)


@dataclass
class RunningOperationCaughtError(ErrorLevel, Cli, File):
    exc: Exception
    code: str = "Q001"

    def message(self) -> str:
        return f'Encountered an error while running operation: {self.exc}'

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class RunningOperationUncaughtError(ErrorLevel, Cli, File):
    exc: Exception
    code: str = "FF01"

    def message(self) -> str:
        return f'Encountered an error while running operation: {self.exc}'

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class DbtProjectError(ErrorLevel, Cli, File):
    code: str = "A009"

    def message(self) -> str:
        return "Encountered an error while reading the project:"


@dataclass
class DbtProjectErrorException(ErrorLevel, Cli, File):
    exc: Exception
    code: str = "A010"

    def message(self) -> str:
        return f"  ERROR: {str(self.exc)}"

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class DbtProfileError(ErrorLevel, Cli, File):
    code: str = "A011"

    def message(self) -> str:
        return "Encountered an error while reading profiles:"


@dataclass
class DbtProfileErrorException(ErrorLevel, Cli, File):
    exc: Exception
    code: str = "A012"

    def message(self) -> str:
        return f"  ERROR: {str(self.exc)}"

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class ProfileListTitle(InfoLevel, Cli, File):
    code: str = "A013"

    def message(self) -> str:
        return "Defined profiles:"


@dataclass
class ListSingleProfile(InfoLevel, Cli, File):
    profile: str
    code: str = "A014"

    def message(self) -> str:
        return f" - {self.profile}"


@dataclass
class NoDefinedProfiles(InfoLevel, Cli, File):
    code: str = "A015"

    def message(self) -> str:
        return "There are no profiles defined in your profiles.yml file"


@dataclass
class ProfileHelpMessage(InfoLevel, Cli, File):
    code: str = "A016"

    def message(self) -> str:
        PROFILES_HELP_MESSAGE = """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""
        return PROFILES_HELP_MESSAGE


@dataclass
class CatchableExceptionOnRun(ShowException, DebugLevel, Cli, File):
    exc: Exception
    code: str = "W002"

    def message(self) -> str:
        return str(self.exc)

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class InternalExceptionOnRun(DebugLevel, Cli, File):
    build_path: str
    exc: Exception
    code: str = "W003"

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

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


# This prints the stack trace at the debug level while allowing just the nice exception message
# at the error level - or whatever other level chosen.  Used in multiple places.
@dataclass
class PrintDebugStackTrace(ShowException, DebugLevel, Cli, File):
    code: str = "Z011"

    def message(self) -> str:
        return ""


@dataclass
class GenericExceptionOnRun(ErrorLevel, Cli, File):
    build_path: str
    unique_id: str
    exc: Exception
    code: str = "W004"

    def message(self) -> str:
        node_description = self.build_path
        if node_description is None:
            node_description = self.unique_id
        prefix = "Unhandled error while executing {}".format(node_description)
        return "{prefix}\n{error}".format(
            prefix=ui.red(prefix),
            error=str(self.exc).strip()
        )

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class NodeConnectionReleaseError(ShowException, DebugLevel, Cli, File):
    node_name: str
    exc: Exception
    code: str = "W005"

    def message(self) -> str:
        return ('Error releasing connection for node {}: {!s}'
                .format(self.node_name, self.exc))

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class CheckCleanPath(InfoLevel, Cli):
    path: str
    code: str = "Z012"

    def message(self) -> str:
        return f"Checking {self.path}/*"


@dataclass
class ConfirmCleanPath(InfoLevel, Cli):
    path: str

    code: str = "Z013"

    def message(self) -> str:
        return f"Cleaned {self.path}/*"


@dataclass
class ProtectedCleanPath(InfoLevel, Cli):
    path: str
    code: str = "Z014"

    def message(self) -> str:
        return f"ERROR: not cleaning {self.path}/* because it is protected"


@dataclass
class FinishedCleanPaths(InfoLevel, Cli):
    code: str = "Z015"

    def message(self) -> str:
        return "Finished cleaning all paths."


@dataclass
class OpenCommand(InfoLevel, Cli, File):
    open_cmd: str
    profiles_dir: str
    code: str = "Z016"

    def message(self) -> str:
        PROFILE_DIR_MESSAGE = """To view your profiles.yml file, run:

{open_cmd} {profiles_dir}"""
        message = PROFILE_DIR_MESSAGE.format(
            open_cmd=self.open_cmd,
            profiles_dir=self.profiles_dir
        )

        return message


@dataclass
class DepsNoPackagesFound(InfoLevel, Cli, File):
    code: str = "M013"

    def message(self) -> str:
        return 'Warning: No packages were found in packages.yml'


@dataclass
class DepsStartPackageInstall(InfoLevel, Cli, File):
    package: str
    code: str = "M014"

    def message(self) -> str:
        return f"Installing {self.package}"


@dataclass
class DepsInstallInfo(InfoLevel, Cli, File):
    version_name: str
    code: str = "M015"

    def message(self) -> str:
        return f"  Installed from {self.version_name}"


@dataclass
class DepsUpdateAvailable(InfoLevel, Cli, File):
    version_latest: str
    code: str = "M016"

    def message(self) -> str:
        return f"  Updated version available: {self.version_latest}"


@dataclass
class DepsUTD(InfoLevel, Cli, File):
    code: str = "M017"

    def message(self) -> str:
        return "  Up to date!"


@dataclass
class DepsListSubdirectory(InfoLevel, Cli, File):
    subdirectory: str
    code: str = "M018"

    def message(self) -> str:
        return f"   and subdirectory {self.subdirectory}"


@dataclass
class DepsNotifyUpdatesAvailable(InfoLevel, Cli, File):
    packages: List[str]
    code: str = "M019"

    def message(self) -> str:
        return ('\nUpdates available for packages: {} \
                \nUpdate your versions in packages.yml, then run dbt deps'.format(self.packages))


@dataclass
class DatabaseErrorRunning(InfoLevel, Cli, File):
    hook_type: str
    code: str = "E038"

    def message(self) -> str:
        return f"Database error while running {self.hook_type}"


@dataclass
class EmptyLine(InfoLevel, Cli, File):
    code: str = "Z017"

    def message(self) -> str:
        return ''


@dataclass
class HooksRunning(InfoLevel, Cli, File):
    num_hooks: int
    hook_type: str
    code: str = "E039"

    def message(self) -> str:
        plural = 'hook' if self.num_hooks == 1 else 'hooks'
        return f"Running {self.num_hooks} {self.hook_type} {plural}"


@dataclass
class HookFinished(InfoLevel, Cli, File):
    stat_line: str
    execution: str
    code: str = "E040"

    def message(self) -> str:
        return f"Finished running {self.stat_line}{self.execution}."


@dataclass
class WriteCatalogFailure(ErrorLevel, Cli, File):
    num_exceptions: int
    code: str = "E041"

    def message(self) -> str:
        return (f"dbt encountered {self.num_exceptions} failure{(self.num_exceptions != 1) * 's'} "
                "while writing the catalog")


@dataclass
class CatalogWritten(InfoLevel, Cli, File):
    path: str
    code: str = "E042"

    def message(self) -> str:
        return f"Catalog written to {self.path}"


@dataclass
class CannotGenerateDocs(InfoLevel, Cli, File):
    code: str = "E043"

    def message(self) -> str:
        return "compile failed, cannot generate docs"


@dataclass
class BuildingCatalog(InfoLevel, Cli, File):
    code: str = "E044"

    def message(self) -> str:
        return "Building catalog"


@dataclass
class CompileComplete(InfoLevel, Cli, File):
    code: str = "Q002"

    def message(self) -> str:
        return "Done."


@dataclass
class FreshnessCheckComplete(InfoLevel, Cli, File):
    code: str = "Q003"

    def message(self) -> str:
        return "Done."


@dataclass
class ServingDocsPort(InfoLevel, Cli, File):
    address: str
    port: int
    code: str = "Z018"

    def message(self) -> str:
        return f"Serving docs at {self.address}:{self.port}"


@dataclass
class ServingDocsAccessInfo(InfoLevel, Cli, File):
    port: str
    code: str = "Z019"

    def message(self) -> str:
        return f"To access from your browser, navigate to:  http://localhost:{self.port}"


@dataclass
class ServingDocsExitInfo(InfoLevel, Cli, File):
    code: str = "Z020"

    def message(self) -> str:
        return "Press Ctrl+C to exit.\n\n"


@dataclass
class SeedHeader(InfoLevel, Cli, File):
    header: str
    code: str = "Q004"

    def message(self) -> str:
        return self.header


@dataclass
class SeedHeaderSeperator(InfoLevel, Cli, File):
    len_header: int
    code: str = "Q005"

    def message(self) -> str:
        return "-" * self.len_header


@dataclass
class RunResultWarning(WarnLevel, Cli, File):
    resource_type: str
    node_name: str
    path: str
    code: str = "Z021"

    def message(self) -> str:
        info = 'Warning'
        return ui.yellow(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class RunResultFailure(ErrorLevel, Cli, File):
    resource_type: str
    node_name: str
    path: str
    code: str = "Z022"

    def message(self) -> str:
        info = 'Failure'
        return ui.red(f"{info} in {self.resource_type} {self.node_name} ({self.path})")


@dataclass
class StatsLine(InfoLevel, Cli, File):
    stats: Dict
    code: str = "Z023"

    def message(self) -> str:
        stats_line = ("\nDone. PASS={pass} WARN={warn} ERROR={error} SKIP={skip} TOTAL={total}")
        return stats_line.format(**self.stats)


@dataclass
class RunResultError(ErrorLevel, Cli, File):
    msg: str
    code: str = "Z024"

    def message(self) -> str:
        return f"  {self.msg}"


@dataclass
class RunResultErrorNoMessage(ErrorLevel, Cli, File):
    status: str
    code: str = "Z025"

    def message(self) -> str:
        return f"  Status: {self.status}"


@dataclass
class SQLCompiledPath(InfoLevel, Cli, File):
    path: str
    code: str = "Z026"

    def message(self) -> str:
        return f"  compiled SQL at {self.path}"


@dataclass
class SQlRunnerException(ShowException, DebugLevel, Cli, File):
    exc: Exception
    code: str = "Q006"

    def message(self) -> str:
        return f"Got an exception: {self.exc}"

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class CheckNodeTestFailure(InfoLevel, Cli, File):
    relation_name: str
    code: str = "Z027"

    def message(self) -> str:
        msg = f"select * from {self.relation_name}"
        border = '-' * len(msg)
        return f"  See test failures:\n  {border}\n  {msg}\n  {border}"


@dataclass
class FirstRunResultError(ErrorLevel, Cli, File):
    msg: str
    code: str = "Z028"

    def message(self) -> str:
        return ui.yellow(self.msg)


@dataclass
class AfterFirstRunResultError(ErrorLevel, Cli, File):
    msg: str
    code: str = "Z029"

    def message(self) -> str:
        return self.msg


@dataclass
class EndOfRunSummary(InfoLevel, Cli, File):
    num_errors: int
    num_warnings: int
    keyboard_interrupt: bool = False
    code: str = "Z030"

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
class PrintStartLine(InfoLevel, Cli, File, NodeInfo):
    description: str
    index: int
    total: int
    report_node_data: ParsedModelNode
    code: str = "Z031"

    def message(self) -> str:
        msg = f"START {self.description}"
        return format_fancy_output_line(
            msg=msg,
            status='RUN',
            index=self.index,
            total=self.total
        )


@dataclass
class PrintHookStartLine(InfoLevel, Cli, File, NodeInfo):
    statement: str
    index: int
    total: int
    truncate: bool
    report_node_data: Any  # TODO use ParsedHookNode here
    code: str = "Z032"

    def message(self) -> str:
        msg = f"START hook: {self.statement}"
        return format_fancy_output_line(msg=msg,
                                        status='RUN',
                                        index=self.index,
                                        total=self.total,
                                        truncate=self.truncate)


@dataclass
class PrintHookEndLine(InfoLevel, Cli, File, NodeInfo):
    statement: str
    status: str
    index: int
    total: int
    execution_time: int
    truncate: bool
    report_node_data: Any  # TODO use ParsedHookNode here
    code: str = "Q007"

    def message(self) -> str:
        msg = 'OK hook: {}'.format(self.statement)
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time,
                                        truncate=self.truncate)


@dataclass
class SkippingDetails(InfoLevel, Cli, File, NodeInfo):
    resource_type: str
    schema: str
    node_name: str
    index: int
    total: int
    report_node_data: ParsedModelNode
    code: str = "Z033"

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
class PrintErrorTestResult(ErrorLevel, Cli, File, NodeInfo):
    name: str
    index: int
    num_models: int
    execution_time: int
    report_node_data: ParsedModelNode
    code: str = "Q008"

    def message(self) -> str:
        info = "ERROR"
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintPassTestResult(InfoLevel, Cli, File, NodeInfo):
    name: str
    index: int
    num_models: int
    execution_time: int
    report_node_data: ParsedModelNode
    code: str = "Q009"

    def message(self) -> str:
        info = "PASS"
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintWarnTestResult(WarnLevel, Cli, File, NodeInfo):
    name: str
    index: int
    num_models: int
    execution_time: int
    failures: List[str]
    report_node_data: ParsedModelNode
    code: str = "Q010"

    def message(self) -> str:
        info = f'WARN {self.failures}'
        msg = f"{info} {self.name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow(info),
                                        index=self.index,
                                        total=self.num_models,
                                        execution_time=self.execution_time)


@dataclass
class PrintFailureTestResult(ErrorLevel, Cli, File, NodeInfo):
    name: str
    index: int
    num_models: int
    execution_time: int
    failures: List[str]
    report_node_data: ParsedModelNode
    code: str = "Q011"

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
    code: str = "Z034"

    def message(self) -> str:
        msg = f'SKIP relation {self.schema}.{self.relation} due to ephemeral model error'
        return format_fancy_output_line(msg=msg,
                                        status=ui.red('ERROR SKIP'),
                                        index=self.index,
                                        total=self.total)


@dataclass
class PrintModelErrorResultLine(ErrorLevel, Cli, File, NodeInfo):
    description: str
    status: str
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedModelNode
    code: str = "Z035"

    def message(self) -> str:
        info = "ERROR creating"
        msg = f"{info} {self.description}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintModelResultLine(InfoLevel, Cli, File, NodeInfo):
    description: str
    status: str
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedModelNode
    code: str = "Q012"

    def message(self) -> str:
        info = "OK created"
        msg = f"{info} {self.description}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSnapshotErrorResultLine(ErrorLevel, Cli, File, NodeInfo):
    status: str
    description: str
    cfg: Dict
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedModelNode
    code: str = "Q013"

    def message(self) -> str:
        info = 'ERROR snapshotting'
        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSnapshotResultLine(InfoLevel, Cli, File, NodeInfo):
    status: str
    description: str
    cfg: Dict
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedModelNode
    code: str = "Q014"

    def message(self) -> str:
        info = 'OK snapshotted'
        msg = "{info} {description}".format(info=info, description=self.description, **self.cfg)
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSeedErrorResultLine(ErrorLevel, Cli, File, NodeInfo):
    status: str
    index: int
    total: int
    execution_time: int
    schema: str
    relation: str
    report_node_data: ParsedModelNode
    code: str = "Q015"

    def message(self) -> str:
        info = 'ERROR loading'
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(self.status.upper()),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintSeedResultLine(InfoLevel, Cli, File, NodeInfo):
    status: str
    index: int
    total: int
    execution_time: int
    schema: str
    relation: str
    report_node_data: ParsedModelNode
    code: str = "Q016"

    def message(self) -> str:
        info = 'OK loaded'
        msg = f"{info} seed file {self.schema}.{self.relation}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.green(self.status),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndErrorLine(ErrorLevel, Cli, File, NodeInfo):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedHookNode
    code: str = "Q017"

    def message(self) -> str:
        info = 'ERROR'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndErrorStaleLine(ErrorLevel, Cli, File, NodeInfo):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedHookNode
    code: str = "Q018"

    def message(self) -> str:
        info = 'ERROR STALE'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.red(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndWarnLine(WarnLevel, Cli, File, NodeInfo):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedHookNode
    code: str = "Q019"

    def message(self) -> str:
        info = 'WARN'
        msg = f"{info} freshness of {self.source_name}.{self.table_name}"
        return format_fancy_output_line(msg=msg,
                                        status=ui.yellow(info),
                                        index=self.index,
                                        total=self.total,
                                        execution_time=self.execution_time)


@dataclass
class PrintHookEndPassLine(InfoLevel, Cli, File, NodeInfo):
    source_name: str
    table_name: str
    index: int
    total: int
    execution_time: int
    report_node_data: ParsedHookNode
    code: str = "Q020"

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
    code: str = "Q021"

    def message(self) -> str:
        msg = 'CANCEL query {}'.format(self.conn_name)
        return format_fancy_output_line(msg=msg,
                                        status=ui.red('CANCEL'),
                                        index=None,
                                        total=None)


@dataclass
class DefaultSelector(InfoLevel, Cli, File):
    name: str
    code: str = "Q022"

    def message(self) -> str:
        return f"Using default selector {self.name}"


@dataclass
class NodeStart(DebugLevel, Cli, File, NodeInfo):
    unique_id: str
    report_node_data: ParsedModelNode
    code: str = "Q023"

    def message(self) -> str:
        return f"Began running node {self.unique_id}"


@dataclass
class NodeFinished(DebugLevel, Cli, File, NodeInfo):
    unique_id: str
    report_node_data: ParsedModelNode
    run_result: RunResult
    code: str = "Q024"

    def message(self) -> str:
        return f"Finished running node {self.unique_id}"


@dataclass
class QueryCancelationUnsupported(InfoLevel, Cli, File):
    type: str
    code: str = "Q025"

    def message(self) -> str:
        msg = (f"The {self.type} adapter does not support query "
               "cancellation. Some queries may still be "
               "running!")
        return ui.yellow(msg)


@dataclass
class ConcurrencyLine(InfoLevel, Cli, File):
    concurrency_line: str
    code: str = "Q026"

    def message(self) -> str:
        return self.concurrency_line


@dataclass
class NodeCompiling(DebugLevel, Cli, File, NodeInfo):
    unique_id: str
    report_node_data: ParsedModelNode
    code: str = "Q030"

    def message(self) -> str:
        return f"Began compiling node {self.unique_id}"


@dataclass
class NodeExecuting(DebugLevel, Cli, File, NodeInfo):
    unique_id: str
    report_node_data: ParsedModelNode
    code: str = "Q031"

    def message(self) -> str:
        return f"Began executing node {self.unique_id}"


@dataclass
class StarterProjectPath(DebugLevel, Cli, File):
    dir: str
    code: str = "A017"

    def message(self) -> str:
        return f"Starter project path: {self.dir}"


@dataclass
class ConfigFolderDirectory(InfoLevel, Cli, File):
    dir: str
    code: str = "A018"

    def message(self) -> str:
        return f"Creating dbt configuration folder at {self.dir}"


@dataclass
class NoSampleProfileFound(InfoLevel, Cli, File):
    adapter: str
    code: str = "A019"

    def message(self) -> str:
        return f"No sample profile found for {self.adapter}."


@dataclass
class ProfileWrittenWithSample(InfoLevel, Cli, File):
    name: str
    path: str
    code: str = "A020"

    def message(self) -> str:
        return (f"Profile {self.name} written to {self.path} "
                "using target's sample configuration. Once updated, you'll be able to "
                "start developing with dbt.")


@dataclass
class ProfileWrittenWithTargetTemplateYAML(InfoLevel, Cli, File):
    name: str
    path: str
    code: str = "A021"

    def message(self) -> str:
        return (f"Profile {self.name} written to {self.path} using target's "
                "profile_template.yml and your supplied values. Run 'dbt debug' to "
                "validate the connection.")


@dataclass
class ProfileWrittenWithProjectTemplateYAML(InfoLevel, Cli, File):
    name: str
    path: str
    code: str = "A022"

    def message(self) -> str:
        return (f"Profile {self.name} written to {self.path} using project's "
                "profile_template.yml and your supplied values. Run 'dbt debug' to "
                "validate the connection.")


@dataclass
class SettingUpProfile(InfoLevel, Cli, File):
    code: str = "A023"

    def message(self) -> str:
        return "Setting up your profile."


@dataclass
class InvalidProfileTemplateYAML(InfoLevel, Cli, File):
    code: str = "A024"

    def message(self) -> str:
        return "Invalid profile_template.yml in project."


@dataclass
class ProjectNameAlreadyExists(InfoLevel, Cli, File):
    name: str
    code: str = "A025"

    def message(self) -> str:
        return f"A project called {self.name} already exists here."


@dataclass
class GetAddendum(InfoLevel, Cli, File):
    msg: str
    code: str = "A026"

    def message(self) -> str:
        return self.msg


@dataclass
class DepsSetDownloadDirectory(DebugLevel, Cli, File):
    path: str
    code: str = "A027"

    def message(self) -> str:
        return f"Set downloads directory='{self.path}'"


@dataclass
class EnsureGitInstalled(ErrorLevel, Cli, File):
    code: str = "Z036"

    def message(self) -> str:
        return ('Make sure git is installed on your machine. More '
                'information: '
                'https://docs.getdbt.com/docs/package-management')


@dataclass
class DepsCreatingLocalSymlink(DebugLevel, Cli, File):
    code: str = "Z037"

    def message(self) -> str:
        return '  Creating symlink to local dependency.'


@dataclass
class DepsSymlinkNotAvailable(DebugLevel, Cli, File):
    code: str = "Z038"

    def message(self) -> str:
        return '  Symlinks are not available on this OS, copying dependency.'


@dataclass
class FoundStats(InfoLevel, Cli, File):
    stat_line: str
    code: str = "W006"

    def message(self) -> str:
        return f"Found {self.stat_line}"


# TODO: should this have NodeInfo on it?
@dataclass
class CompilingNode(DebugLevel, Cli, File):
    unique_id: str
    code: str = "Q027"

    def message(self) -> str:
        return f"Compiling {self.unique_id}"


@dataclass
class WritingInjectedSQLForNode(DebugLevel, Cli, File):
    unique_id: str
    code: str = "Q028"

    def message(self) -> str:
        return f'Writing injected SQL for node "{self.unique_id}"'


@dataclass
class DisableTracking(WarnLevel, Cli, File):
    code: str = "Z039"

    def message(self) -> str:
        return "Error sending message, disabling tracking"


@dataclass
class SendingEvent(DebugLevel, Cli, File):
    kwargs: str
    code: str = "Z040"

    def message(self) -> str:
        return f"Sending event: {self.kwargs}"


@dataclass
class SendEventFailure(DebugLevel, Cli, File):
    code: str = "Z041"

    def message(self) -> str:
        return "An error was encountered while trying to send an event"


@dataclass
class FlushEvents(DebugLevel, Cli):
    code: str = "Z042"

    def message(self) -> str:
        return "Flushing usage events"


@dataclass
class FlushEventsFailure(DebugLevel, Cli):
    code: str = "Z043"

    def message(self) -> str:
        return "An error was encountered while trying to flush usage events"


@dataclass
class TrackingInitializeFailure(ShowException, DebugLevel, Cli, File):
    code: str = "Z044"

    def message(self) -> str:
        return "Got an exception trying to initialize tracking"


@dataclass
class RetryExternalCall(DebugLevel, Cli, File):
    attempt: int
    max: int
    code: str = "Z045"

    def message(self) -> str:
        return f"Retrying external call. Attempt: {self.attempt} Max attempts: {self.max}"


@dataclass
class GeneralWarningMsg(WarnLevel, Cli, File):
    msg: str
    log_fmt: str
    code: str = "Z046"

    def message(self) -> str:
        if self.log_fmt is not None:
            return self.log_fmt.format(self.msg)
        return self.msg


@dataclass
class GeneralWarningException(WarnLevel, Cli, File):
    exc: Exception
    log_fmt: str
    code: str = "Z047"

    def message(self) -> str:
        if self.log_fmt is not None:
            return self.log_fmt.format(str(self.exc))
        return str(self.exc)

    def fields_to_json(self, val: Any) -> Any:
        if val == self.exc:
            return str(val)

        return val


@dataclass
class EventBufferFull(WarnLevel, Cli, File):
    code: str = "Z048"

    def message(self) -> str:
        return "Internal event buffer full. Earliest events will be dropped (FIFO)."


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
    DumpBeforeAddGraph(dict())
    DumpAfterAddGraph(dict())
    DumpBeforeRenameSchema(dict())
    DumpAfterRenameSchema(dict())
    AdapterImportError(ModuleNotFoundError())
    PluginLoadError()
    SystemReportReturnCode(returncode=0)
    SelectorAlertUpto3UnusedNodes(node_names=[])
    SelectorAlertAllUnusedNodes(node_names=[])
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
    PartialParsingExceptionFile(file='')
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
    PrintStartLine(description='', index=0, total=0, report_node_data=ParsedModelNode())
    PrintHookStartLine(
        statement='',
        index=0,
        total=0,
        truncate=False,
        report_node_data=ParsedHookNode()
    )
    PrintHookEndLine(
        statement='',
        status='',
        index=0,
        total=0,
        execution_time=0,
        truncate=False,
        report_node_data=ParsedHookNode()
    )
    SkippingDetails(
        resource_type='',
        schema='',
        node_name='',
        index=0,
        total=0,
        report_node_data=ParsedModelNode()
    )
    PrintErrorTestResult(
        name='',
        index=0,
        num_models=0,
        execution_time=0,
        report_node_data=ParsedModelNode()
    )
    PrintPassTestResult(
        name='',
        index=0,
        num_models=0,
        execution_time=0,
        report_node_data=ParsedModelNode()
    )
    PrintWarnTestResult(
        name='',
        index=0,
        num_models=0,
        execution_time=0,
        failures=[],
        report_node_data=ParsedModelNode()
    )
    PrintFailureTestResult(
        name='',
        index=0,
        num_models=0,
        execution_time=0,
        failures=[],
        report_node_data=ParsedModelNode()
    )
    PrintSkipBecauseError(schema='', relation='', index=0, total=0)
    PrintModelErrorResultLine(
        description='',
        status='',
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedModelNode()
    )
    PrintModelResultLine(
        description='',
        status='',
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedModelNode()
    )
    PrintSnapshotErrorResultLine(
        status='',
        description='',
        cfg={},
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedModelNode()
    )
    PrintSnapshotResultLine(
        status='',
        description='',
        cfg={},
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedModelNode()
    )
    PrintSeedErrorResultLine(
        status='',
        index=0,
        total=0,
        execution_time=0,
        schema='',
        relation='',
        report_node_data=ParsedModelNode()
    )
    PrintSeedResultLine(
        status='',
        index=0,
        total=0,
        execution_time=0,
        schema='',
        relation='',
        report_node_data=ParsedModelNode()
    )
    PrintHookEndErrorLine(
        source_name='',
        table_name='',
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedHookNode()
    )
    PrintHookEndErrorStaleLine(
        source_name='',
        table_name='',
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedHookNode()
    )
    PrintHookEndWarnLine(
        source_name='',
        table_name='',
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedHookNode()
    )
    PrintHookEndPassLine(
        source_name='',
        table_name='',
        index=0,
        total=0,
        execution_time=0,
        report_node_data=ParsedHookNode()
    )
    PrintCancelLine(conn_name='')
    DefaultSelector(name='')
    NodeStart(report_node_data=ParsedModelNode(), unique_id='')
    NodeFinished(report_node_data=ParsedModelNode(), unique_id='', run_result=RunResult())
    QueryCancelationUnsupported(type='')
    ConcurrencyLine(concurrency_line='')
    NodeCompiling(report_node_data=ParsedModelNode(), unique_id='')
    NodeExecuting(report_node_data=ParsedModelNode(), unique_id='')
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
    EventBufferFull()
