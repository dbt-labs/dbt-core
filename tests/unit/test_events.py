# flake8: noqa
from dbt.events.test_types import UnitTestInfo
from dbt.events import AdapterLogger
from dbt.events.functions import event_to_json, LOG_VERSION, event_to_dict
from dbt.events.types import *
from dbt.events.test_types import *

from dbt.events.base_types import (
    BaseEvent,
    DebugLevel,
    WarnLevel,
    InfoLevel,
    ErrorLevel,
    TestLevel,
)
from dbt.events.proto_types import ListOfStrings, NodeInfo, RunResultMsg, ReferenceKeyMsg
from importlib import reload
import dbt.events.functions as event_funcs
import dbt.flags as flags
import inspect
import json
from dbt.contracts.graph.nodes import ModelNode, NodeConfig, DependsOn
from dbt.contracts.files import FileHash
from mashumaro.types import SerializableType
from typing import Generic, TypeVar, Dict
import re

# takes in a class and finds any subclasses for it
def get_all_subclasses(cls):
    all_subclasses = []
    for subclass in cls.__subclasses__():
        if subclass not in [TestLevel, DebugLevel, WarnLevel, InfoLevel, ErrorLevel, DynamicLevel]:
            all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))
    return set(all_subclasses)


class TestAdapterLogger:
    # this interface is documented for adapter maintainers to plug into
    # so we should test that it at the very least doesn't explode.
    def test_basic_adapter_logging_interface(self):
        logger = AdapterLogger("dbt_tests")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.exception("exception message")
        logger.critical("exception message")

    # python loggers allow deferring string formatting via this signature:
    def test_formatting(self):
        logger = AdapterLogger("dbt_tests")
        # tests that it doesn't throw
        logger.debug("hello {}", "world")

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="hello {}", args=("world",))
        assert "hello world" in event.message()

        # tests that it doesn't throw
        logger.debug("1 2 {}", 3)

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="1 2 {}", args=(3,))
        assert "1 2 3" in event.message()

        # tests that it doesn't throw
        logger.debug("boop{x}boop")

        # enters lower in the call stack to test that it formats correctly
        # in this case it's that we didn't attempt to replace anything since there
        # were no args passed after the initial message
        event = AdapterEventDebug(name="dbt_tests", base_msg="boop{x}boop", args=())
        assert "boop{x}boop" in event.message()

        # ensure AdapterLogger and subclasses makes all base_msg members
        # of type string; when someone writes logger.debug(a) where a is
        # any non-string object
        event = AdapterEventDebug(name="dbt_tests", base_msg=[1,2,3], args=(3,))
        assert isinstance(event.base_msg, str)

        event = JinjaLogDebug(msg=[1,2,3])
        assert isinstance(event.msg, str)


class TestEventCodes:

    # checks to see if event codes are duplicated to keep codes singluar and clear.
    # also checks that event codes follow correct namming convention ex. E001
    def test_event_codes(self):
        all_concrete = get_all_subclasses(BaseEvent)
        all_codes = set()

        for event_cls in all_concrete:
            code = event_cls.code(event_cls)
            # must be in the form 1 capital letter, 3 digits
            assert re.match("^[A-Z][0-9]{3}", code)
            # cannot have been used already
            assert (
                code not in all_codes
            ), f"{code} is assigned more than once. Check types.py for duplicates."
            all_codes.add(code)


def MockNode():
    return ModelNode(
        alias="model_one",
        name="model_one",
        database="dbt",
        schema="analytics",
        resource_type=NodeType.Model,
        unique_id="model.root.model_one",
        fqn=["root", "model_one"],
        package_name="root",
        original_file_path="model_one.sql",
        root_path="/usr/src/app",
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict(
            {
                "enabled": True,
                "materialized": "view",
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "vars": {},
                "quoting": {},
                "column_types": {},
                "tags": [],
            }
        ),
        tags=[],
        path="model_one.sql",
        raw_code="",
        description="",
        columns={},
        checksum=FileHash.from_contents(""),
    )


sample_values = [
    # A - pre-project loading
    MainReportVersion(version=""),
    MainReportArgs(args={}),
    MainTrackingUserState(user_state=""),
    MergedFromState(num_merged=0, sample=[]),
    MissingProfileTarget(profile_name="", target_name=""),
    InvalidVarsYAML(),
    DbtProjectError(),
    DbtProjectErrorException(exc=""),
    DbtProfileError(),
    DbtProfileErrorException(exc=""),
    ProfileListTitle(),
    ListSingleProfile(profile=""),
    NoDefinedProfiles(),
    ProfileHelpMessage(),
    StarterProjectPath(dir=""),
    ConfigFolderDirectory(dir=""),
    NoSampleProfileFound(adapter=""),
    ProfileWrittenWithSample(name="", path=""),
    ProfileWrittenWithTargetTemplateYAML(name="", path=""),
    ProfileWrittenWithProjectTemplateYAML(name="", path=""),
    SettingUpProfile(),
    InvalidProfileTemplateYAML(),
    ProjectNameAlreadyExists(name=""),
    ProjectCreated(project_name=""),

    # D - Deprecations ======================
    PackageRedirectDeprecation(old_name="", new_name=""),
    PackageInstallPathDeprecation(),
    ConfigSourcePathDeprecation(deprecated_path="", exp_path=""),
    ConfigDataPathDeprecation(deprecated_path="", exp_path=""),
    AdapterDeprecationWarning(old_name="", new_name=""),
    MetricAttributesRenamed(metric_name=""),
    ExposureNameDeprecation(exposure=""),

    # E - DB Adapter ======================
    AdapterEventDebug(),
    AdapterEventInfo(),
    AdapterEventWarning(),
    AdapterEventError(),
    NewConnection(conn_type="", conn_name=""),
    ConnectionReused(conn_name=""),
    ConnectionLeftOpenInCleanup(conn_name=""),
    ConnectionClosedInCleanup(conn_name=""),
    RollbackFailed(conn_name=""),
    ConnectionClosed(conn_name=""),
    ConnectionLeftOpen(conn_name=""),
    Rollback(conn_name=""),
    CacheMiss(conn_name="", database="", schema=""),
    ListRelations(database="", schema=""),
    ConnectionUsed(conn_type="", conn_name=""),
    SQLQuery(conn_name="", sql=""),
    SQLQueryStatus(status="", elapsed=0.1),
    SQLCommit(conn_name=""),
    ColTypeChange(
        orig_type="", new_type="", table=ReferenceKeyMsg(database="", schema="", identifier="")
    ),
    SchemaCreation(relation=ReferenceKeyMsg(database="", schema="", identifier="")),
    SchemaDrop(relation=ReferenceKeyMsg(database="", schema="", identifier="")),
    UncachedRelation(
        dep_key=ReferenceKeyMsg(database="", schema="", identifier=""),
        ref_key=ReferenceKeyMsg(database="", schema="", identifier=""),
    ),
    AddLink(
        dep_key=ReferenceKeyMsg(database="", schema="", identifier=""),
        ref_key=ReferenceKeyMsg(database="", schema="", identifier=""),
    ),
    AddRelation(relation=ReferenceKeyMsg(database="", schema="", identifier="")),
    DropMissingRelation(relation=ReferenceKeyMsg(database="", schema="", identifier="")),
    DropCascade(
        dropped=ReferenceKeyMsg(database="", schema="", identifier=""),
        consequences=[ReferenceKeyMsg(database="", schema="", identifier="")],
    ),
    DropRelation(dropped=ReferenceKeyMsg()),
    UpdateReference(
        old_key=ReferenceKeyMsg(database="", schema="", identifier=""),
        new_key=ReferenceKeyMsg(database="", schema="", identifier=""),
        cached_key=ReferenceKeyMsg(database="", schema="", identifier=""),
    ),
    TemporaryRelation(key=ReferenceKeyMsg(database="", schema="", identifier="")),
    RenameSchema(
        old_key=ReferenceKeyMsg(database="", schema="", identifier=""),
        new_key=ReferenceKeyMsg(database="", schema="", identifier=""),
    ),
    DumpBeforeAddGraph(dump=dict()),
    DumpAfterAddGraph(dump=dict()),
    DumpBeforeRenameSchema(dump=dict()),
    DumpAfterRenameSchema(dump=dict()),
    AdapterImportError(exc=""),
    PluginLoadError(exc_info=""),
    NewConnectionOpening(connection_state=""),
    CodeExecution(conn_name="", code_content=""),
    CodeExecutionStatus(status="", elapsed=0.1),
    CatalogGenerationError(exc=""),
    WriteCatalogFailure(num_exceptions=0),
    CatalogWritten(path=""),
    CannotGenerateDocs(),
    BuildingCatalog(),
    DatabaseErrorRunningHook(hook_type=""),
    HooksRunning(num_hooks=0, hook_type=""),
    HookFinished(stat_line="", execution="", execution_time=0),

    # I - Project parsing ======================
    ParseCmdStart(),
    ParseCmdCompiling(),
    ParseCmdWritingManifest(),
    ParseCmdDone(),
    ManifestDependenciesLoaded(),
    ManifestLoaderCreated(),
    ManifestLoaded(),
    ManifestChecked(),
    ManifestFlatGraphBuilt(),
    ParseCmdPerfInfoPath(path=""),
    GenericTestFileParse(path=""),
    MacroFileParse(path=""),
    PartialParsingFullReparseBecauseOfError(),
    PartialParsingExceptionFile(file=""),
    PartialParsingFile(file_id=""),
    PartialParsingException(exc_info={}),
    PartialParsingSkipParsing(),
    PartialParsingMacroChangeStartFullParse(),
    PartialParsingProjectEnvVarsChanged(),
    PartialParsingProfileEnvVarsChanged(),
    PartialParsingDeletedMetric(unique_id=""),
    ManifestWrongMetadataVersion(version=""),
    PartialParsingVersionMismatch(saved_version="", current_version=""),
    PartialParsingFailedBecauseConfigChange(),
    PartialParsingFailedBecauseProfileChange(),
    PartialParsingFailedBecauseNewProjectDependency(),
    PartialParsingFailedBecauseHashChanged(),
    PartialParsingNotEnabled(),
    ParsedFileLoadFailed(path="", exc="", exc_info=""),
    PartialParseSaveFileNotFound(),
    StaticParserCausedJinjaRendering(path=""),
    UsingExperimentalParser(path=""),
    SampleFullJinjaRendering(path=""),
    StaticParserFallbackJinjaRendering(path=""),
    StaticParsingMacroOverrideDetected(path=""),
    StaticParserSuccess(path=""),
    StaticParserFailure(path=""),
    ExperimentalParserSuccess(path=""),
    ExperimentalParserFailure(path=""),
    PartialParsingEnabled(deleted=0, added=0, changed=0),
    PartialParsingAddedFile(file_id=""),
    PartialParsingDeletedFile(file_id=""),
    PartialParsingUpdatedFile(file_id=""),
    PartialParsingNodeMissingInSourceFile(file_id=""),
    PartialParsingMissingNodes(file_id=""),
    PartialParsingChildMapMissingUniqueID(unique_id=""),
    PartialParsingUpdateSchemaFile(file_id=""),
    PartialParsingDeletedSource(unique_id=""),
    PartialParsingDeletedExposure(unique_id=""),
    InvalidDisabledTargetInTestNode(
        resource_type_title="",
        unique_id="",
        original_file_path="",
        target_kind="",
        target_name="",
        target_package="",
    ),
    UnusedResourceConfigPath(unused_config_paths=[]),
    SeedIncreased(package_name="", name=""),
    SeedExceedsLimitSamePath(package_name="", name=""),
    SeedExceedsLimitAndPathChanged(package_name="", name=""),
    SeedExceedsLimitChecksumChanged(package_name="", name="", checksum_name=""),
    UnusedTables(unused_tables=[]),
    WrongResourceSchemaFile(patch_name="", resource_type="", file_path="", plural_resource_type=""),
    NoNodeForYamlKey(patch_name="", yaml_key="", file_path=""),
    MacroPatchNotFound(patch_name=""),
    NodeNotFoundOrDisabled(
        original_file_path="",
        unique_id="",
        resource_type_title="",
        target_name="",
        target_kind="",
        target_package="",
        disabled="",
    ),
    JinjaLogWarning(),

    # M - Deps generation ======================

    GitSparseCheckoutSubdirectory(subdir=""),
    GitProgressCheckoutRevision(revision=""),
    GitProgressUpdatingExistingDependency(dir=""),
    GitProgressPullingNewDependency(dir=""),
    GitNothingToDo(sha=""),
    GitProgressUpdatedCheckoutRange(start_sha="", end_sha=""),
    GitProgressCheckedOutAt(end_sha=""),
    RegistryProgressGETRequest(url=""),
    RegistryProgressGETResponse(url="", resp_code=1234),
    SelectorReportInvalidSelector(valid_selectors="", spec_method="", raw_spec=""),
    JinjaLogInfo(msg=""),
    JinjaLogDebug(msg=""),
    DepsNoPackagesFound(),
    DepsStartPackageInstall(package_name=""),
    DepsInstallInfo(version_name=""),
    DepsUpdateAvailable(version_latest=""),
    DepsUpToDate(),
    DepsListSubdirectory(subdirectory=""),
    DepsNotifyUpdatesAvailable(packages=ListOfStrings()),
    RetryExternalCall(attempt=0, max=0),
    RecordRetryException(exc=""),
    RegistryIndexProgressGETRequest(url=""),
    RegistryIndexProgressGETResponse(url="", resp_code=1234),
    RegistryResponseUnexpectedType(response=""),
    RegistryResponseMissingTopKeys(response=""),
    RegistryResponseMissingNestedKeys(response=""),
    RegistryResponseExtraNestedKeys(response=""),
    DepsSetDownloadDirectory(path=""),

    # Q - Node execution ======================

    RunningOperationCaughtError(exc=""),
    CompileComplete(),
    FreshnessCheckComplete(),
    SeedHeader(header=""),
    SeedHeaderSeparator(len_header=0),
    SQLRunnerException(exc=""),
    LogTestResult(
        name="",
        index=0,
        num_models=0,
        execution_time=0,
        num_failures=0,
    ),
    LogStartLine(description="", index=0, total=0, node_info=NodeInfo()),
    LogModelResult(
        description="",
        status="",
        index=0,
        total=0,
        execution_time=0,
    ),
    LogSnapshotResult(
        status="",
        description="",
        cfg={},
        index=0,
        total=0,
        execution_time=0,
    ),
    LogSeedResult(
        status="",
        index=0,
        total=0,
        execution_time=0,
        schema="",
        relation="",
    ),
    LogFreshnessResult(
        source_name="",
        table_name="",
        index=0,
        total=0,
        execution_time=0,
    ),
    LogCancelLine(conn_name=""),
    DefaultSelector(name=""),
    NodeStart(node_info=NodeInfo()),
    NodeFinished(node_info=NodeInfo()),
    QueryCancelationUnsupported(type=""),
    ConcurrencyLine(num_threads=0, target_name=""),
    WritingInjectedSQLForNode(node_info=NodeInfo()),
    NodeCompiling(node_info=NodeInfo()),
    NodeExecuting(node_info=NodeInfo()),
    LogHookStartLine(
        statement="",
        index=0,
        total=0,
    ),
    LogHookEndLine(
        statement="",
        status="",
        index=0,
        total=0,
        execution_time=0,
    ),
    SkippingDetails(
        resource_type="",
        schema="",
        node_name="",
        index=0,
        total=0,
    ),
    NothingToDo(),
    RunningOperationUncaughtError(exc=""),
    EndRunResult(),
    NoNodesSelected(),
    DepsUnpinned(revision="", git=""),
    NoNodesForSelectionCriteria(spec_raw=""),

    # W - Node testing ======================

    CatchableExceptionOnRun(exc=""),
    InternalExceptionOnRun(build_path="", exc=""),
    GenericExceptionOnRun(build_path="", unique_id="", exc=""),
    NodeConnectionReleaseError(node_name="", exc=""),
    FoundStats(stat_line=""),

    # Z - misc ======================

    MainKeyboardInterrupt(),
    MainEncounteredError(exc=""),
    MainStackTrace(stack_trace=""),
    SystemErrorRetrievingModTime(path=""),
    SystemCouldNotWrite(path="", reason="", exc=""),
    SystemExecutingCmd(cmd=[""]),
    SystemStdOutMsg(bmsg=b""),
    SystemStdErrMsg(bmsg=b""),
    SystemReportReturnCode(returncode=0),
    TimingInfoCollected(),
    LogDebugStackTrace(),
    CheckCleanPath(path=""),
    ConfirmCleanPath(path=""),
    ProtectedCleanPath(path=""),
    FinishedCleanPaths(),
    OpenCommand(open_cmd="", profiles_dir=""),
    EmptyLine(),
    ServingDocsPort(address="", port=0),
    ServingDocsAccessInfo(port=""),
    ServingDocsExitInfo(),
    RunResultWarning(resource_type="", node_name="", path=""),
    RunResultFailure(resource_type="", node_name="", path=""),
    StatsLine(stats={"error": 0, "skip": 0, "pass": 0, "warn": 0,"total": 0}),
    RunResultError(msg=""),
    RunResultErrorNoMessage(status=""),
    SQLCompiledPath(path=""),
    CheckNodeTestFailure(relation_name=""),
    FirstRunResultError(msg=""),
    AfterFirstRunResultError(msg=""),
    EndOfRunSummary(num_errors=0, num_warnings=0, keyboard_interrupt=False),
    LogSkipBecauseError(schema="", relation="", index=0, total=0),
    EnsureGitInstalled(),
    DepsCreatingLocalSymlink(),
    DepsSymlinkNotAvailable(),
    DisableTracking(),
    SendingEvent(kwargs=""),
    SendEventFailure(),
    FlushEvents(),
    FlushEventsFailure(),
    TrackingInitializeFailure(),
    RunResultWarningMessage(),

    # T - tests ======================
    IntegrationTestInfo(),
    IntegrationTestDebug(),
    IntegrationTestWarn(),
    IntegrationTestError(),
    IntegrationTestException(),
    UnitTestInfo(),

]




class TestEventJSONSerialization:

    # attempts to test that every event is serializable to json.
    # event types that take `Any` are not possible to test in this way since some will serialize
    # just fine and others won't.
    def test_all_serializable(self):
        all_non_abstract_events = set(
            get_all_subclasses(BaseEvent),
        )
        all_event_values_list = list(map(lambda x: x.__class__, sample_values))
        diff = all_non_abstract_events.difference(set(all_event_values_list))
        assert (
            not diff
        ), f"test is missing concrete values in `sample_values`. Please add the values for the aforementioned event classes"

        # make sure everything in the list is a value not a type
        for event in sample_values:
            assert type(event) != type

        # if we have everything we need to test, try to serialize everything
        for event in sample_values:
            event_dict = event_to_dict(event)
            try:
                event_json = event_to_json(event)
            except Exception as e:
                raise Exception(f"{event} is not serializable to json. Originating exception: {e}")


T = TypeVar("T")
