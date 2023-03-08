import re
from typing import TypeVar

from dbt.contracts.results import TimingInfo
from dbt.events import AdapterLogger, test_types, types
from dbt.events.base_types import (
    BaseEvent,
    DebugLevel,
    DynamicLevel,
    ErrorLevel,
    InfoLevel,
    TestLevel,
    WarnLevel,
    msg_from_base_event,
)
from dbt.events.functions import msg_to_dict, msg_to_json
from dbt.flags import set_from_args
from argparse import Namespace


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
        event = types.AdapterEventDebug(name="dbt_tests", base_msg="hello {}", args=("world",))
        assert "hello world" in event.message()

        # tests that it doesn't throw
        logger.debug("1 2 {}", 3)

        # enters lower in the call stack to test that it formats correctly
        event = types.AdapterEventDebug(name="dbt_tests", base_msg="1 2 {}", args=(3,))
        assert "1 2 3" in event.message()

        # tests that it doesn't throw
        logger.debug("boop{x}boop")

        # enters lower in the call stack to test that it formats correctly
        # in this case it's that we didn't attempt to replace anything since there
        # were no args passed after the initial message
        event = types.AdapterEventDebug(name="dbt_tests", base_msg="boop{x}boop", args=())
        assert "boop{x}boop" in event.message()

        # ensure AdapterLogger and subclasses makes all base_msg members
        # of type string; when someone writes logger.debug(a) where a is
        # any non-string object
        event = types.AdapterEventDebug(name="dbt_tests", base_msg=[1, 2, 3], args=(3,))
        assert isinstance(event.base_msg, str)

        event = types.JinjaLogDebug(msg=[1, 2, 3])
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


sample_values = [
    # N.B. Events instantiated here include the module prefix in order to
    # avoid having the entire list twice in the code.
    # A - pre-project loading
    types.MainReportVersion(version=""),
    types.MainReportArgs(args={}),
    types.MainTrackingUserState(user_state=""),
    types.MergedFromState(num_merged=0, sample=[]),
    types.MissingProfileTarget(profile_name="", target_name=""),
    types.InvalidOptionYAML(option_name="vars"),
    types.LogDbtProjectError(),
    types.LogDbtProfileError(),
    types.StarterProjectPath(dir=""),
    types.ConfigFolderDirectory(dir=""),
    types.NoSampleProfileFound(adapter=""),
    types.ProfileWrittenWithSample(name="", path=""),
    types.ProfileWrittenWithTargetTemplateYAML(name="", path=""),
    types.ProfileWrittenWithProjectTemplateYAML(name="", path=""),
    types.SettingUpProfile(),
    types.InvalidProfileTemplateYAML(),
    types.ProjectNameAlreadyExists(name=""),
    types.ProjectCreated(project_name=""),
    # D - Deprecations ======================
    types.PackageRedirectDeprecation(old_name="", new_name=""),
    types.PackageInstallPathDeprecation(),
    types.ConfigSourcePathDeprecation(deprecated_path="", exp_path=""),
    types.ConfigDataPathDeprecation(deprecated_path="", exp_path=""),
    types.AdapterDeprecationWarning(old_name="", new_name=""),
    types.MetricAttributesRenamed(metric_name=""),
    types.ExposureNameDeprecation(exposure=""),
    types.InternalDeprecation(name="", reason="", suggested_action="", version=""),
    # E - DB Adapter ======================
    types.AdapterEventDebug(),
    types.AdapterEventInfo(),
    types.AdapterEventWarning(),
    types.AdapterEventError(),
    types.NewConnection(conn_type="", conn_name=""),
    types.ConnectionReused(conn_name=""),
    types.ConnectionLeftOpenInCleanup(conn_name=""),
    types.ConnectionClosedInCleanup(conn_name=""),
    types.RollbackFailed(conn_name=""),
    types.ConnectionClosed(conn_name=""),
    types.ConnectionLeftOpen(conn_name=""),
    types.Rollback(conn_name=""),
    types.CacheMiss(conn_name="", database="", schema=""),
    types.ListRelations(database="", schema=""),
    types.ConnectionUsed(conn_type="", conn_name=""),
    types.SQLQuery(conn_name="", sql=""),
    types.SQLQueryStatus(status="", elapsed=0.1),
    types.SQLCommit(conn_name=""),
    types.ColTypeChange(
        orig_type="",
        new_type="",
        table=types.ReferenceKeyMsg(database="", schema="", identifier=""),
    ),
    types.SchemaCreation(relation=types.ReferenceKeyMsg(database="", schema="", identifier="")),
    types.SchemaDrop(relation=types.ReferenceKeyMsg(database="", schema="", identifier="")),
    types.CacheAction(
        action="adding_relation",
        ref_key=types.ReferenceKeyMsg(database="", schema="", identifier=""),
        ref_key_2=types.ReferenceKeyMsg(database="", schema="", identifier=""),
    ),
    types.CacheDumpGraph(before_after="before", action="rename", dump=dict()),
    types.AdapterImportError(exc=""),
    types.PluginLoadError(exc_info=""),
    types.NewConnectionOpening(connection_state=""),
    types.CodeExecution(conn_name="", code_content=""),
    types.CodeExecutionStatus(status="", elapsed=0.1),
    types.CatalogGenerationError(exc=""),
    types.WriteCatalogFailure(num_exceptions=0),
    types.CatalogWritten(path=""),
    types.CannotGenerateDocs(),
    types.BuildingCatalog(),
    types.DatabaseErrorRunningHook(hook_type=""),
    types.HooksRunning(num_hooks=0, hook_type=""),
    types.FinishedRunningStats(stat_line="", execution="", execution_time=0),
    # I - Project parsing ======================
    types.InputFileDiffError(category="testing", file_id="my_file"),
    types.InvalidValueForField(field_name="test", field_value="test"),
    types.ValidationWarning(resource_type="model", field_name="access", node_name="my_macro"),
    types.ParsePerfInfoPath(path=""),
    types.GenericTestFileParse(path=""),
    types.MacroFileParse(path=""),
    types.PartialParsingErrorProcessingFile(file=""),
    types.PartialParsingFile(file_id=""),
    types.PartialParsingError(exc_info={}),
    types.PartialParsingSkipParsing(),
    types.UnableToPartialParse(reason="something went wrong"),
    types.StateCheckVarsHash(vars="testing", target="testing", profile="testing"),
    types.PartialParsingNotEnabled(),
    types.ParsedFileLoadFailed(path="", exc="", exc_info=""),
    types.PartialParsingEnabled(deleted=0, added=0, changed=0),
    types.PartialParsingFile(file_id=""),
    types.InvalidDisabledTargetInTestNode(
        resource_type_title="",
        unique_id="",
        original_file_path="",
        target_kind="",
        target_name="",
        target_package="",
    ),
    types.UnusedResourceConfigPath(unused_config_paths=[]),
    types.SeedIncreased(package_name="", name=""),
    types.SeedExceedsLimitSamePath(package_name="", name=""),
    types.SeedExceedsLimitAndPathChanged(package_name="", name=""),
    types.SeedExceedsLimitChecksumChanged(package_name="", name="", checksum_name=""),
    types.UnusedTables(unused_tables=[]),
    types.WrongResourceSchemaFile(
        patch_name="", resource_type="", file_path="", plural_resource_type=""
    ),
    types.NoNodeForYamlKey(patch_name="", yaml_key="", file_path=""),
    types.MacroNotFoundForPatch(patch_name=""),
    types.NodeNotFoundOrDisabled(
        original_file_path="",
        unique_id="",
        resource_type_title="",
        target_name="",
        target_kind="",
        target_package="",
        disabled="",
    ),
    types.JinjaLogWarning(),
    types.JinjaLogInfo(msg=""),
    types.JinjaLogDebug(msg=""),
    # M - Deps generation ======================
    types.GitSparseCheckoutSubdirectory(subdir=""),
    types.GitProgressCheckoutRevision(revision=""),
    types.GitProgressUpdatingExistingDependency(dir=""),
    types.GitProgressPullingNewDependency(dir=""),
    types.GitNothingToDo(sha=""),
    types.GitProgressUpdatedCheckoutRange(start_sha="", end_sha=""),
    types.GitProgressCheckedOutAt(end_sha=""),
    types.RegistryProgressGETRequest(url=""),
    types.RegistryProgressGETResponse(url="", resp_code=1234),
    types.SelectorReportInvalidSelector(valid_selectors="", spec_method="", raw_spec=""),
    types.DepsNoPackagesFound(),
    types.DepsStartPackageInstall(package_name=""),
    types.DepsInstallInfo(version_name=""),
    types.DepsUpdateAvailable(version_latest=""),
    types.DepsUpToDate(),
    types.DepsListSubdirectory(subdirectory=""),
    types.DepsNotifyUpdatesAvailable(packages=types.ListOfStrings()),
    types.RetryExternalCall(attempt=0, max=0),
    types.RecordRetryException(exc=""),
    types.RegistryIndexProgressGETRequest(url=""),
    types.RegistryIndexProgressGETResponse(url="", resp_code=1234),
    types.RegistryResponseUnexpectedType(response=""),
    types.RegistryResponseMissingTopKeys(response=""),
    types.RegistryResponseMissingNestedKeys(response=""),
    types.RegistryResponseExtraNestedKeys(response=""),
    types.DepsSetDownloadDirectory(path=""),
    # Q - Node execution ======================
    types.RunningOperationCaughtError(exc=""),
    types.CompileComplete(),
    types.FreshnessCheckComplete(),
    types.SeedHeader(header=""),
    types.SQLRunnerException(exc=""),
    types.LogTestResult(
        name="",
        index=0,
        num_models=0,
        execution_time=0,
        num_failures=0,
    ),
    types.LogStartLine(description="", index=0, total=0, node_info=types.NodeInfo()),
    types.LogModelResult(
        description="",
        status="",
        index=0,
        total=0,
        execution_time=0,
    ),
    types.LogSnapshotResult(
        status="",
        description="",
        cfg={},
        index=0,
        total=0,
        execution_time=0,
    ),
    types.LogSeedResult(
        status="",
        index=0,
        total=0,
        execution_time=0,
        schema="",
        relation="",
    ),
    types.LogFreshnessResult(
        source_name="",
        table_name="",
        index=0,
        total=0,
        execution_time=0,
    ),
    types.LogCancelLine(conn_name=""),
    types.DefaultSelector(name=""),
    types.NodeStart(node_info=types.NodeInfo()),
    types.NodeFinished(node_info=types.NodeInfo()),
    types.QueryCancelationUnsupported(type=""),
    types.ConcurrencyLine(num_threads=0, target_name=""),
    types.WritingInjectedSQLForNode(node_info=types.NodeInfo()),
    types.NodeCompiling(node_info=types.NodeInfo()),
    types.NodeExecuting(node_info=types.NodeInfo()),
    types.LogHookStartLine(
        statement="",
        index=0,
        total=0,
    ),
    types.LogHookEndLine(
        statement="",
        status="",
        index=0,
        total=0,
        execution_time=0,
    ),
    types.SkippingDetails(
        resource_type="",
        schema="",
        node_name="",
        index=0,
        total=0,
    ),
    types.NothingToDo(),
    types.RunningOperationUncaughtError(exc=""),
    types.EndRunResult(),
    types.NoNodesSelected(),
    types.DepsUnpinned(revision="", git=""),
    types.NoNodesForSelectionCriteria(spec_raw=""),
    # W - Node testing ======================
    types.CatchableExceptionOnRun(exc=""),
    types.InternalErrorOnRun(build_path="", exc=""),
    types.GenericExceptionOnRun(build_path="", unique_id="", exc=""),
    types.NodeConnectionReleaseError(node_name="", exc=""),
    types.FoundStats(stat_line=""),
    # Z - misc ======================
    types.MainKeyboardInterrupt(),
    types.MainEncounteredError(exc=""),
    types.MainStackTrace(stack_trace=""),
    types.SystemCouldNotWrite(path="", reason="", exc=""),
    types.SystemExecutingCmd(cmd=[""]),
    types.SystemStdOut(bmsg=b""),
    types.SystemStdErr(bmsg=b""),
    types.SystemReportReturnCode(returncode=0),
    types.TimingInfoCollected(),
    types.LogDebugStackTrace(),
    types.CheckCleanPath(path=""),
    types.ConfirmCleanPath(path=""),
    types.ProtectedCleanPath(path=""),
    types.FinishedCleanPaths(),
    types.OpenCommand(open_cmd="", profiles_dir=""),
    types.RunResultWarning(resource_type="", node_name="", path=""),
    types.RunResultFailure(resource_type="", node_name="", path=""),
    types.StatsLine(stats={"error": 0, "skip": 0, "pass": 0, "warn": 0, "total": 0}),
    types.RunResultError(msg=""),
    types.RunResultErrorNoMessage(status=""),
    types.SQLCompiledPath(path=""),
    types.CheckNodeTestFailure(relation_name=""),
    types.FirstRunResultError(msg=""),
    types.AfterFirstRunResultError(msg=""),
    types.EndOfRunSummary(num_errors=0, num_warnings=0, keyboard_interrupt=False),
    types.LogSkipBecauseError(schema="", relation="", index=0, total=0),
    types.EnsureGitInstalled(),
    types.DepsCreatingLocalSymlink(),
    types.DepsSymlinkNotAvailable(),
    types.DisableTracking(),
    types.SendingEvent(kwargs=""),
    types.SendEventFailure(),
    types.FlushEvents(),
    types.FlushEventsFailure(),
    types.Formatting(),
    types.TrackingInitializeFailure(),
    types.RunResultWarningMessage(),
    types.DebugCmdOut(),
    types.DebugCmdResult(),
    types.ListCmdOut(),
    types.Note(msg="This is a note."),
    # T - tests ======================
    test_types.IntegrationTestInfo(),
    test_types.IntegrationTestDebug(),
    test_types.IntegrationTestWarn(),
    test_types.IntegrationTestError(),
    test_types.IntegrationTestException(),
    test_types.UnitTestInfo(),
]


class TestEventJSONSerialization:

    # attempts to test that every event is serializable to json.
    # event types that take `Any` are not possible to test in this way since some will serialize
    # just fine and others won't.
    def test_all_serializable(self):
        set_from_args(Namespace(WARN_ERROR=False), None)
        all_non_abstract_events = set(
            get_all_subclasses(BaseEvent),
        )
        all_event_values_list = list(map(lambda x: x.__class__, sample_values))
        diff = all_non_abstract_events.difference(set(all_event_values_list))
        assert (
            not diff
        ), f"{diff}test is missing concrete values in `sample_values`. Please add the values for the aforementioned event classes"

        # make sure everything in the list is a value not a type
        for event in sample_values:
            assert type(event) != type

        # if we have everything we need to test, try to serialize everything
        count = 0
        for event in sample_values:
            msg = msg_from_base_event(event)
            print(f"--- msg: {msg.info.name}")
            # Serialize to dictionary
            try:
                msg_to_dict(msg)
            except Exception as e:
                raise Exception(
                    f"{event} can not be converted to a dict. Originating exception: {e}"
                )
            # Serialize to json
            try:
                msg_to_json(msg)
            except Exception as e:
                raise Exception(f"{event} is not serializable to json. Originating exception: {e}")
            # Serialize to binary
            try:
                bytes(msg)
            except Exception as e:
                raise Exception(
                    f"{event} is not serializable to binary protobuf. Originating exception: {e}"
                )
            count += 1
        print(f"--- Found {count} events")


T = TypeVar("T")


def test_date_serialization():
    ti = TimingInfo("test")
    ti.begin()
    ti.end()
    ti_dict = ti.to_dict()
    assert ti_dict["started_at"].endswith("Z")
    assert ti_dict["completed_at"].endswith("Z")
