from dbt import events
from dbt.events.functions import EVENT_HISTORY, fire_event
from dbt.events.test_types import UnitTestInfo
from argparse import Namespace
from dbt.events import AdapterLogger
from dbt.events.functions import event_to_serializable_dict
from dbt.events.types import *
from dbt.events.test_types import *
from dbt.events.base_types import Event, Node
from dbt.events.stubs import _CachedRelation, BaseRelation, _ReferenceKey, ParsedModelNode
import inspect
import json
import datetime
from unittest import TestCase
from dbt.contracts.graph.parsed import (
    ParsedModelNode, NodeConfig, DependsOn, ParsedMacro
)
from dbt.contracts.files import FileHash

# takes in a class and finds any subclasses for it
def get_all_subclasses(cls):
    all_subclasses = []
    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))
    return set(all_subclasses)


class TestAdapterLogger(TestCase):

    def setUp(self):
        pass

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
        self.assertTrue(True)

    # python loggers allow deferring string formatting via this signature:
    def test_formatting(self):
        logger = AdapterLogger("dbt_tests")
        # tests that it doesn't throw
        logger.debug("hello {}", 'world')

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="hello {}", args=('world',))
        self.assertTrue("hello world" in event.message())

        # tests that it doesn't throw
        logger.debug("1 2 {}", 3)

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="1 2 {}", args=(3,))
        self.assertTrue("1 2 3" in event.message())

        # tests that it doesn't throw
        logger.debug("boop{x}boop")

        # enters lower in the call stack to test that it formats correctly
        # in this case it's that we didn't attempt to replace anything since there
        # were no args passed after the initial message
        event = AdapterEventDebug(name="dbt_tests", base_msg="boop{x}boop", args=())
        self.assertTrue("boop{x}boop" in event.message())

class TestEventCodes(TestCase):

    # checks to see if event codes are duplicated to keep codes singluar and clear.
    # also checks that event codes follow correct namming convention ex. E001
    def test_event_codes(self):
        all_concrete = get_all_subclasses(Event)
        all_codes = set()

        for event in all_concrete:
            if not inspect.isabstract(event):
                # must be in the form 1 capital letter, 3 digits
                self.assertTrue('^[A-Z][0-9]{3}', event.code)
                # cannot have been used already
                self.assertFalse(event.code in all_codes, f'{event.code} is assigned more than once. Check types.py for duplicates.')
                all_codes.add(event.code)


class TestEventBuffer(TestCase):

    # ensure events are populated to the buffer exactly once
    def test_buffer_populates(self):
        fire_event(UnitTestInfo(msg="Test Event 1"))
        fire_event(UnitTestInfo(msg="Test Event 2"))
        self.assertTrue(
            EVENT_HISTORY.count(UnitTestInfo(msg='Test Event 1', code='T006')) == 1
        )

    # ensure events drop from the front of the buffer when buffer maxsize is reached
    # TODO commenting out till we can make this not spit out 100k log lines.
    # def test_buffer_FIFOs(self):
    #     for n in range(0,100001):
    #         fire_event(UnitTestInfo(msg=f"Test Event {n}"))
    #     self.assertTrue(
    #         EVENT_HISTORY.count(EventBufferFull(code='Z048')) == 1
    #     )
    #     self.assertTrue(
    #         EVENT_HISTORY.count(UnitTestInfo(msg='Test Event 1', code='T006')) == 0
    #     )

def dump_callable():
    return dict()
    

def MockNode():
    return ParsedModelNode(
        alias='model_one',
        name='model_one',
        database='dbt',
        schema='analytics',
        resource_type=NodeType.Model,
        unique_id='model.root.model_one',
        fqn=['root', 'model_one'],
        package_name='root',
        original_file_path='model_one.sql',
        root_path='/usr/src/app',
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        }),
        tags=[],
        path='model_one.sql',
        raw_sql='',
        description='',
        columns={},
        checksum=FileHash.from_contents(''),
    )


sample_values = [
    MainReportVersion(''),
    MainKeyboardInterrupt(),
    MainEncounteredError(BaseException('')),
    MainStackTrace(''),
    MainTrackingUserState(''),
    ParsingStart(),
    ParsingCompiling(),
    ParsingWritingManifest(),
    ParsingDone(),
    ManifestDependenciesLoaded(),
    ManifestLoaderCreated(),
    ManifestLoaded(),
    ManifestChecked(),
    ManifestFlatGraphBuilt(),
    ReportPerformancePath(path=""),
    GitSparseCheckoutSubdirectory(subdir=""),
    GitProgressCheckoutRevision(revision=""),
    GitProgressUpdatingExistingDependency(dir=""),
    GitProgressPullingNewDependency(dir=""),
    GitNothingToDo(sha=""),
    GitProgressUpdatedCheckoutRange(start_sha="", end_sha=""),
    GitProgressCheckedOutAt(end_sha=""),
    SystemErrorRetrievingModTime(path=""),
    SystemCouldNotWrite(path="", reason="", exc=Exception("")),
    SystemExecutingCmd(cmd=[""]),
    SystemStdOutMsg(bmsg=b""),
    SystemStdErrMsg(bmsg=b""),
    SelectorReportInvalidSelector(
        selector_methods={"": ""}, spec_method="", raw_spec=""
    ),
    MacroEventInfo(msg=""),
    MacroEventDebug(msg=""),
    NewConnection(conn_type="", conn_name=""),
    ConnectionReused(conn_name=""),
    ConnectionLeftOpen(conn_name=""),
    ConnectionClosed(conn_name=""),
    RollbackFailed(conn_name=""),
    ConnectionClosed2(conn_name=""),
    ConnectionLeftOpen2(conn_name=""),
    Rollback(conn_name=""),
    CacheMiss(conn_name="", database="", schema=""),
    ListRelations(database="", schema="", relations=[]),
    ConnectionUsed(conn_type="", conn_name=""),
    SQLQuery(conn_name="", sql=""),
    SQLQueryStatus(status="", elapsed=0.1),
    SQLCommit(conn_name=""),
    ColTypeChange(orig_type="", new_type="", table=""),
    SchemaCreation(relation=BaseRelation()),
    SchemaDrop(relation=BaseRelation()),
    UncachedRelation(
        dep_key=_ReferenceKey(database="", schema="", identifier=""),
        ref_key=_ReferenceKey(database="", schema="", identifier=""),
    ),
    AddLink(
        dep_key=_ReferenceKey(database="", schema="", identifier=""),
        ref_key=_ReferenceKey(database="", schema="", identifier=""),
    ),
    AddRelation(relation=_CachedRelation()),
    DropMissingRelation(relation=_ReferenceKey(database="", schema="", identifier="")),
    DropCascade(
        dropped=_ReferenceKey(database="", schema="", identifier=""),
        consequences={_ReferenceKey(database="", schema="", identifier="")},
    ),
    UpdateReference(
        old_key=_ReferenceKey(database="", schema="", identifier=""),
        new_key=_ReferenceKey(database="", schema="", identifier=""),
        cached_key=_ReferenceKey(database="", schema="", identifier=""),
    ),
    TemporaryRelation(key=_ReferenceKey(database="", schema="", identifier="")),
    RenameSchema(
        old_key=_ReferenceKey(database="", schema="", identifier=""),
        new_key=_ReferenceKey(database="", schema="", identifier="")
    ),
    DumpBeforeAddGraph(dump_callable),
    DumpAfterAddGraph(dump_callable),
    DumpBeforeRenameSchema(dump_callable),
    DumpAfterRenameSchema(dump_callable),
    AdapterImportError(ModuleNotFoundError()),
    PluginLoadError(),
    SystemReportReturnCode(returncode=0),
    SelectorAlertUpto3UnusedNodes(node_names=[]),
    SelectorAlertAllUnusedNodes(node_names=[]),
    NewConnectionOpening(connection_state=''),
    TimingInfoCollected(),
    MergedFromState(nbr_merged=0, sample=[]),
    MissingProfileTarget(profile_name='', target_name=''),
    ProfileLoadError(exc=Exception('')),
    ProfileNotFound(profile_name=''),
    InvalidVarsYAML(),
    GenericTestFileParse(path=''),
    MacroFileParse(path=''),
    PartialParsingFullReparseBecauseOfError(),
    PartialParsingFile(file_dict={}),
    PartialParsingExceptionFile(file=''),
    PartialParsingException(exc_info={}),
    PartialParsingSkipParsing(),
    PartialParsingMacroChangeStartFullParse(),
    ManifestWrongMetadataVersion(version=''),
    PartialParsingVersionMismatch(saved_version='', current_version=''),
    PartialParsingFailedBecauseConfigChange(),
    PartialParsingFailedBecauseProfileChange(),
    PartialParsingFailedBecauseNewProjectDependency(),
    PartialParsingFailedBecauseHashChanged(),
    PartialParsingDeletedMetric(''),
    ParsedFileLoadFailed(path='', exc=Exception('')),
    PartialParseSaveFileNotFound(),
    StaticParserCausedJinjaRendering(path=''),
    UsingExperimentalParser(path=''),
    SampleFullJinjaRendering(path=''),
    StaticParserFallbackJinjaRendering(path=''),
    StaticParsingMacroOverrideDetected(path=''),
    StaticParserSuccess(path=''),
    StaticParserFailure(path=''),
    ExperimentalParserSuccess(path=''),
    ExperimentalParserFailure(path=''),
    PartialParsingEnabled(deleted=0, added=0, changed=0),
    PartialParsingAddedFile(file_id=''),
    PartialParsingDeletedFile(file_id=''),
    PartialParsingUpdatedFile(file_id=''),
    PartialParsingNodeMissingInSourceFile(source_file=''),
    PartialParsingMissingNodes(file_id=''),
    PartialParsingChildMapMissingUniqueID(unique_id=''),
    PartialParsingUpdateSchemaFile(file_id=''),
    PartialParsingDeletedSource(unique_id=''),
    PartialParsingDeletedExposure(unique_id=''),
    InvalidDisabledSourceInTestNode(msg=''),
    InvalidRefInTestNode(msg=''),
    MessageHandleGenericException(build_path='', unique_id='', exc=Exception('')),
    DetailsHandleGenericException(),
    RunningOperationCaughtError(exc=Exception('')),
    RunningOperationUncaughtError(exc=Exception('')),
    DbtProjectError(),
    DbtProjectErrorException(exc=Exception('')),
    DbtProfileError(),
    DbtProfileErrorException(exc=Exception('')),
    ProfileListTitle(),
    ListSingleProfile(profile=''),
    NoDefinedProfiles(),
    ProfileHelpMessage(),
    CatchableExceptionOnRun(exc=Exception('')),
    InternalExceptionOnRun(build_path='', exc=Exception('')),
    GenericExceptionOnRun(build_path='', unique_id='', exc=Exception('')),
    NodeConnectionReleaseError(node_name='', exc=Exception('')),
    CheckCleanPath(path=''),
    ConfirmCleanPath(path=''),
    ProtectedCleanPath(path=''),
    FinishedCleanPaths(),
    OpenCommand(open_cmd='', profiles_dir=''),
    DepsNoPackagesFound(),
    DepsStartPackageInstall(package=''),
    DepsInstallInfo(version_name=''),
    DepsUpdateAvailable(version_latest=''),
    DepsListSubdirectory(subdirectory=''),
    DepsNotifyUpdatesAvailable(packages=[]),
    DatabaseErrorRunning(hook_type=''),
    EmptyLine(),
    HooksRunning(num_hooks=0, hook_type=''),
    HookFinished(stat_line='', execution=''),
    WriteCatalogFailure(num_exceptions=0),
    CatalogWritten(path=''),
    CannotGenerateDocs(),
    BuildingCatalog(),
    CompileComplete(),
    FreshnessCheckComplete(),
    ServingDocsPort(address='', port=0),
    ServingDocsAccessInfo(port=''),
    ServingDocsExitInfo(),
    SeedHeader(header=''),
    SeedHeaderSeperator(len_header=0),
    RunResultWarning(resource_type='', node_name='', path=''),
    RunResultFailure(resource_type='', node_name='', path=''),
    StatsLine(stats={'pass':0, 'warn':0, 'error':0, 'skip':0, 'total':0}),
    RunResultError(msg=''),
    RunResultErrorNoMessage(status=''),
    SQLCompiledPath(path=''),
    CheckNodeTestFailure(relation_name=''),
    FirstRunResultError(msg=''),
    AfterFirstRunResultError(msg=''),
    EndOfRunSummary(num_errors=0, num_warnings=0, keyboard_interrupt=False),
    PrintStartLine(description='', index=0, total=0, report_node_data=MockNode()),
    PrintHookStartLine(statement='', index=0, total=0, truncate=False, report_node_data=MockNode()),
    PrintHookEndLine(statement='', status='', index=0, total=0, execution_time=0, truncate=False, report_node_data=MockNode()),
    SkippingDetails(resource_type='', schema='', node_name='', index=0, total=0, report_node_data=MockNode()),
    PrintErrorTestResult(name='', index=0, num_models=0, execution_time=0, report_node_data=MockNode()),
    PrintPassTestResult(name='', index=0, num_models=0, execution_time=0, report_node_data=MockNode()),
    PrintWarnTestResult(name='', index=0, num_models=0, execution_time=0, failures=[], report_node_data=MockNode()),
    PrintFailureTestResult(name='', index=0, num_models=0, execution_time=0, failures=[], report_node_data=MockNode()),
    PrintSkipBecauseError(schema='', relation='', index=0, total=0),
    PrintModelErrorResultLine(description='', status='', index=0, total=0, execution_time=0, report_node_data=MockNode()),
    PrintModelResultLine(description='', status='', index=0, total=0, execution_time=0, report_node_data=MockNode()),
    PrintSnapshotErrorResultLine(status='',
                                 description='',
                                 cfg={},
                                 index=0,
                                 total=0,
                                 execution_time=0,
                                 report_node_data=MockNode()),
    PrintSnapshotResultLine(status='', description='', cfg={}, index=0, total=0, execution_time=0, report_node_data=MockNode()),
    PrintSeedErrorResultLine(status='', index=0, total=0, execution_time=0, schema='', relation='', report_node_data=MockNode()),
    PrintSeedResultLine(status='', index=0, total=0, execution_time=0, schema='', relation='', report_node_data=MockNode()),
    PrintHookEndErrorLine(source_name='', table_name='', index=0, total=0, execution_time=0, report_node_data=MockNode()),
    PrintHookEndErrorStaleLine(source_name='', table_name='', index=0, total=0, execution_time=0, report_node_data=MockNode()),
    PrintHookEndWarnLine(source_name='', table_name='', index=0, total=0, execution_time=0, report_node_data=MockNode()),
    PrintHookEndPassLine(source_name='', table_name='', index=0, total=0, execution_time=0, report_node_data=MockNode()),
    PrintCancelLine(conn_name=''),
    DefaultSelector(name=''),
    NodeStart(unique_id='', report_node_data=MockNode()),
    NodeCompiling(unique_id='', report_node_data=MockNode()),
    NodeExecuting(unique_id='', report_node_data=MockNode()),
    NodeFinished(unique_id='', report_node_data=MockNode(), run_result=''),
    QueryCancelationUnsupported(type=''),
    ConcurrencyLine(concurrency_line=''),
    StarterProjectPath(dir=''),
    ConfigFolderDirectory(dir=''),
    NoSampleProfileFound(adapter=''),
    ProfileWrittenWithSample(name='', path=''),
    ProfileWrittenWithTargetTemplateYAML(name='', path=''),
    ProfileWrittenWithProjectTemplateYAML(name='', path=''),
    SettingUpProfile(),
    InvalidProfileTemplateYAML(),
    ProjectNameAlreadyExists(name=''),
    GetAddendum(msg=''),
    DepsSetDownloadDirectory(path=''),
    EnsureGitInstalled(),
    DepsCreatingLocalSymlink(),
    DepsSymlinkNotAvailable(),
    FoundStats(stat_line=''),
    CompilingNode(unique_id=''),
    WritingInjectedSQLForNode(unique_id=''),
    DisableTracking(),
    SendingEvent(kwargs=''),
    SendEventFailure(),
    FlushEvents(),
    FlushEventsFailure(),
    TrackingInitializeFailure(),
    RetryExternalCall(attempt=0, max=0),
    GeneralWarningMsg(msg='', log_fmt=''),
    GeneralWarningException(exc=Exception(''), log_fmt=''),
    PartialParsingProfileEnvVarsChanged(),
    AdapterEventDebug('', '', ()),
    AdapterEventInfo('', '', ()),
    AdapterEventWarning('', '', ()),
    AdapterEventError('', '', ()),
    PrintDebugStackTrace(),
    MainReportArgs(Namespace()),
    RegistryProgressMakingGETRequest(''),
    DepsUTD(),
    CatchRunException('', Exception('')),
    HandleInternalException(Exception('')),
    PartialParsingNotEnabled(),
    SQlRunnerException(Exception('')),
    DropRelation(''),
    PartialParsingProjectEnvVarsChanged(),
    RegistryProgressGETResponse('', ''),
    IntegrationTestDebug(''),
    IntegrationTestInfo(''),
    IntegrationTestWarn(''),
    IntegrationTestError(''),
    IntegrationTestException(''),
    EventBufferFull(),
    UnitTestInfo('')
]


class TestEventJSONSerialization(TestCase):

    # attempts to test that every event is serializable to json.
    # event types that take `Any` are not possible to test in this way since some will serialize
    # just fine and others won't.
    def test_all_serializable(self):
        all_non_abstract_events = set(filter(lambda x: not inspect.isabstract(x), get_all_subclasses(Event)))
        all_event_values_list = list(map(lambda x: x.__class__, sample_values))
        diff = all_non_abstract_events.difference(set(all_event_values_list))
        self.assertFalse(diff, f"test is missing concrete values in `sample_values`. Please add the values for the aforementioned event classes")

        # make sure everything in the list is a value not a type
        for event in sample_values:
            self.assertFalse(type(event) == type)

        # if we have everything we need to test, try to serialize everything
        for event in sample_values:
            d = event_to_serializable_dict(event, lambda dt: dt.isoformat(), lambda x: x.message())
            try:
                json.dumps(d)
            except TypeError as e:
                raise Exception(f"{event} is not serializable to json. Originating exception: {e}")
                
