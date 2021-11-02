from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Dict
from dbt.ui import warning_tag


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
        return warning_tag(self.msg)


@dataclass
class InvalidRefInTestNode(WarnLevel, CliEventABC):
    msg: str

    def cli_msg(self) -> str:
        return warning_tag(self.msg)


# since mypy doesn't run on every file we need to suggest to mypy that every
# class gets instantiated. But we don't actually want to run this code.
# making the conditional `if False` causes mypy to skip it as dead code so
# we need to skirt around that by computing something it doesn't check statically.
#
# TODO remove these lines once we run mypy everywhere.
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
