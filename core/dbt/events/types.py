from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, cast, Dict, List, Optional, Set, Union

from dbt.events.stubs import _CachedRelation, AdapterResponse, BaseRelation, _ReferenceKey

# types to represent log levels


# in preparation for #3977
class TestLevel:
    def level_tag(self) -> str:
        return "test"


class DebugLevel:
    def level_tag(self) -> str:
        return "debug"


class InfoLevel:
    def level_tag(self) -> str:
        return "info"


class WarnLevel:
    def level_tag(self) -> str:
        return "warn"


class ErrorLevel:
    def level_tag(self) -> str:
        return "error"


@dataclass
class ShowException:
    def __post_init__(self):
        self.exc_info: Any = None
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
            f"\n  - and {len(self.node_names) - 3} more"
            if len(self.node_names) > 4
            else ""
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
        return f"Full list of tests that were excluded:\n  - {debug_nodes_str}"


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
class MacroEventDebug(DebugLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


@dataclass
class MacroEventInfo(InfoLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return self.msg


@dataclass
class NewConnection(DebugLevel, CliEventABC):
    conn_type: str
    conn_name: str

    def cli_msg(self) -> str:
        return f'Acquiring new {self.conn_type} connection "{self.conn_name}"'


@dataclass
class ConnectionReused(DebugLevel, CliEventABC):
    conn_name: str

    def cli_msg(self) -> str:
        return f"Re-using an available connection from the pool (formerly {self.conn_name})"


@dataclass
class ConnectionLeftOpen(DebugLevel, CliEventABC):
    conn_name: Optional[str]

    def cli_msg(self) -> str:
        return f"Connection '{self.conn_name}' was left open."


@dataclass
class ConnectionClosed(DebugLevel, CliEventABC):
    conn_name: Optional[str]

    def cli_msg(self) -> str:
        return f"Connection '{self.conn_name}' was properly closed."


@dataclass
class RollbackFailed(ShowException, DebugLevel, CliEventABC):
    conn_name: Optional[str]

    def cli_msg(self) -> str:
        return f"Failed to rollback '{self.conn_name}'"


# TODO: can we combine this with ConnectionClosed?
@dataclass
class ConnectionClosed2(DebugLevel, CliEventABC):
    conn_name: Optional[str]

    def cli_msg(self) -> str:
        return f"On {self.conn_name}: Close"


# TODO: can we combine this with ConnectionLeftOpen?
@dataclass
class ConnectionLeftOpen2(DebugLevel, CliEventABC):
    conn_name: Optional[str]

    def cli_msg(self) -> str:
        return f"On {self.conn_name}: No close available on handle"


@dataclass
class Rollback(DebugLevel, CliEventABC):
    conn_name: Optional[str]

    def cli_msg(self) -> str:
        return f"On {self.conn_name}: ROLLBACK"


@dataclass
class CacheMiss(DebugLevel, CliEventABC):
    conn_name: Any  # TODO mypy says this is `Callable[[], str]`??  ¯\_(ツ)_/¯
    database: Optional[str]
    schema: str

    def cli_msg(self) -> str:
        return (
            f'On "{self.conn_name}": cache miss for schema '
            '"{self.database}.{self.schema}", this is inefficient'
        )


@dataclass
class ListRelations(DebugLevel, CliEventABC):
    database: Optional[str]
    schema: str
    relations: List[BaseRelation]

    def cli_msg(self) -> str:
        return f"with database={self.database}, schema={self.schema}, relations={self.relations}"


@dataclass
class ConnectionUsed(DebugLevel, CliEventABC):
    conn_type: str
    conn_name: Optional[str]

    def cli_msg(self) -> str:
        return f'Using {self.conn_type} connection "{self.conn_name}"'


@dataclass
class SQLQuery(DebugLevel, CliEventABC):
    conn_name: Optional[str]
    sql: str

    def cli_msg(self) -> str:
        return f"On {self.conn_name}: {self.sql}"


@dataclass
class SQLQueryStatus(DebugLevel, CliEventABC):
    status: Union[AdapterResponse, str]
    elapsed: float

    def cli_msg(self) -> str:
        return f"SQL status: {self.status} in {self.elapsed} seconds"


@dataclass
class SQLCommit(DebugLevel, CliEventABC):
    conn_name: str

    def cli_msg(self) -> str:
        return f"On {self.conn_name}: COMMIT"


@dataclass
class ColTypeChange(DebugLevel, CliEventABC):
    orig_type: str
    new_type: str
    table: str

    def cli_msg(self) -> str:
        return f"Changing col type from {self.orig_type} to {self.new_type} in table {self.table}"


@dataclass
class SchemaCreation(DebugLevel, CliEventABC):
    relation: BaseRelation

    def cli_msg(self) -> str:
        return f'Creating schema "{self.relation}"'


@dataclass
class SchemaDrop(DebugLevel, CliEventABC):
    relation: BaseRelation

    def cli_msg(self) -> str:
        return f'Dropping schema "{self.relation}".'


# TODO pretty sure this is only ever called in dead code
# see: core/dbt/adapters/cache.py _add_link vs add_link
@dataclass
class UncachedRelation(DebugLevel, CliEventABC):
    dep_key: _ReferenceKey
    ref_key: _ReferenceKey

    def cli_msg(self) -> str:
        return (
            f"{self.dep_key} references {str(self.ref_key)} "
            "but {self.ref_key.database}.{self.ref_key.schema}"
            "is not in the cache, skipping assumed external relation"
        )


@dataclass
class AddLink(DebugLevel, CliEventABC):
    dep_key: _ReferenceKey
    ref_key: _ReferenceKey

    def cli_msg(self) -> str:
        return f"adding link, {self.dep_key} references {self.ref_key}"


@dataclass
class AddRelation(DebugLevel, CliEventABC):
    relation: _CachedRelation

    def cli_msg(self) -> str:
        return f"Adding relation: {str(self.relation)}"


@dataclass
class DropMissingRelation(DebugLevel, CliEventABC):
    relation: _ReferenceKey

    def cli_msg(self) -> str:
        return f"dropped a nonexistent relationship: {str(self.relation)}"


@dataclass
class DropCascade(DebugLevel, CliEventABC):
    dropped: _ReferenceKey
    consequences: Set[_ReferenceKey]

    def cli_msg(self) -> str:
        return f"drop {self.dropped} is cascading to {self.consequences}"


@dataclass
class DropRelation(DebugLevel, CliEventABC):
    dropped: _ReferenceKey

    def cli_msg(self) -> str:
        return f"Dropping relation: {self.dropped}"


@dataclass
class UpdateReference(DebugLevel, CliEventABC):
    old_key: _ReferenceKey
    new_key: _ReferenceKey
    cached_key: _ReferenceKey

    def cli_msg(self) -> str:
        return f"updated reference from {self.old_key} -> {self.cached_key} to "\
            "{self.new_key} -> {self.cached_key}"


@dataclass
class TemporaryRelation(DebugLevel, CliEventABC):
    key: _ReferenceKey

    def cli_msg(self) -> str:
        return f"old key {self.key} not found in self.relations, assuming temporary"


@dataclass
class RenameSchema(DebugLevel, CliEventABC):
    old_key: _ReferenceKey
    new_key: _ReferenceKey

    def cli_msg(self) -> str:
        return f"Renaming relation {self.old_key} to {self.new_key}"


@dataclass
class DumpBeforeAddGraph(DebugLevel, CliEventABC):
    graph_func: Callable[[], Dict[str, List[str]]]

    def cli_msg(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        # TODO remove when we've upgraded to a mypy version without that bug
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"before adding : {func_returns}"


@dataclass
class DumpAfterAddGraph(DebugLevel, CliEventABC):
    graph_func: Callable[[], Dict[str, List[str]]]

    def cli_msg(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"after adding: {func_returns}"


@dataclass
class DumpBeforeRenameSchema(DebugLevel, CliEventABC):
    graph_func: Callable[[], Dict[str, List[str]]]

    def cli_msg(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"before rename: {func_returns}"


@dataclass
class DumpAfterRenameSchema(DebugLevel, CliEventABC):
    graph_func: Callable[[], Dict[str, List[str]]]

    def cli_msg(self) -> str:
        # workaround for https://github.com/python/mypy/issues/6910
        func_returns = cast(Callable[[], Dict[str, List[str]]], getattr(self, "graph_func"))
        return f"after rename: {func_returns}"


@dataclass
class AdapterImportError(InfoLevel, CliEventABC):
    exc: ModuleNotFoundError

    def cli_msg(self) -> str:
        return f"Error importing adapter: {self.exc}"


@dataclass
class PluginLoadError(ShowException, DebugLevel, CliEventABC):
    def cli_msg(self):
        pass


# since mypy doesn't run on every file we need to suggest to mypy that every
# class gets instantiated. But we don't actually want to run this code.
# making the conditional `if False` causes mypy to skip it as dead code so
# we need to skirt around that by computing something it doesn't check statically.
#
# TODO remove these lines once we run mypy everywhere.

def dump_callable():
    return {"": [""]}  # for instantiating `Dump...` methods which take callables.


if 1 == 0:
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
    SelectorAlertUpto3UnusedNodes(node_names=[])
    SelectorAlertAllUnusedNodes(node_names=[])
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
