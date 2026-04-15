use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, EnumMessage, EnumString};

// TODO: these models should live in dbt-schemas crate. It currently lives in dbt-common because
// EvalArgs is defined here and dbt-common cannot depend on dbt-schemas without creating a cycle.

/// Legacy dbt-core event names supported by Fusion in `warn-error-options` configuration
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    EnumString,
    AsRefStr,
)]
pub enum SupportedLegacyWarnError {
    JinjaLogWarning,
    LogTestResult,
    NothingToDo,
    NoNodesSelected,
    NodeNotFoundOrDisabled,
    DeprecatedModel,
    DeprecatedReference,
    UpcomingReferenceDeprecation,
    SnapshotTimestampWarning,
    PackageRedirectDeprecation,
    DepsUnpinned,
    FreshnessConfigProblem,
    WarnStateTargetEqual,
    WEOIncludeExcludeDeprecation,
}

/// Legacy dbt-core event names that Fusion will eventually support in `warn-error-options` configuration, but does not yet support. These may have a corresponding fusion-native error code providing similar functionality.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    EnumString,
    AsRefStr,
)]
pub enum NotYetSupportedLegacyWarnError {
    AdapterDeprecationWarning,
    AdapterEventDebug,
    AdapterEventError,
    AdapterEventInfo,
    AdapterEventWarning,
    AdapterImportError,
    AdapterRegistered,
    ArgumentsPropertyInGenericTestDeprecation,
    ArtifactUploadError,
    ArtifactUploadSkipped,
    ArtifactUploadSuccess,
    ArtifactWritten,
    BehaviorChangeEvent,
    BuildingCatalog,
    CacheAction,
    CacheDumpGraph,
    CacheMiss,
    CannotGenerateDocs,
    CatalogGenerationError,
    CatalogWritten,
    CatchableExceptionOnRun,
    CheckCleanPath,
    CheckNodeTestFailure,
    CodeExecution,
    CodeExecutionStatus,
    ColTypeChange,
    CollectFreshnessReturnSignature,
    CommandCompleted,
    CompileComplete,
    CompiledNode,
    ConcurrencyLine,
    ConfigFolderDirectory,
    ConfirmCleanPath,
    ConnectionClosed,
    ConnectionClosedInCleanup,
    ConnectionLeftOpen,
    ConnectionLeftOpenInCleanup,
    ConnectionReused,
    ConnectionUsed,
    ConstraintNotEnforced,
    ConstraintNotSupported,
    CustomKeyInConfigDeprecation,
    CustomKeyInObjectDeprecation,
    CustomOutputPathInSourceFreshnessDeprecation,
    CustomTopLevelKeyDeprecation,
    DatabaseErrorRunningHook,
    DebugCmdOut,
    DebugCmdResult,
    DebugLevel,
    DefaultSelector,
    DeprecationsSummary,
    DepsAddPackage,
    DepsCreatingLocalSymlink,
    DepsFoundDuplicatePackage,
    DepsInstallInfo,
    DepsListSubdirectory,
    DepsLockUpdating,
    DepsNoPackagesFound,
    DepsNotifyUpdatesAvailable,
    DepsScrubbedPackageName,
    DepsSetDownloadDirectory,
    DepsStartPackageInstall,
    DepsSymlinkNotAvailable,
    DepsUpToDate,
    DepsUpdateAvailable,
    DisableTracking,
    DuplicateNameDistinctNodeTypesDeprecation,
    DuplicateYAMLKeysDeprecation,
    DynamicLevel,
    EndOfRunSummary,
    EndRunResult,
    EnsureGitInstalled,
    EnvironmentVariableRenamed,
    ErrorLevel,
    EventLevel,
    ExposureNameDeprecation,
    FinishedCleanPaths,
    FinishedRunningStats,
    FlushEvents,
    FlushEventsFailure,
    Formatting,
    FoundStats,
    FreshnessCheckComplete,
    GenericExceptionOnRun,
    GenericJSONSchemaValidationDeprecation,
    GetMetaKeyWarning,
    GitNothingToDo,
    GitProgressCheckedOutAt,
    GitProgressCheckoutRevision,
    GitProgressPullingNewDependency,
    GitProgressUpdatedCheckoutRange,
    GitProgressUpdatingExistingDependency,
    GitSparseCheckoutSubdirectory,
    HooksRunning,
    InfoLevel,
    InputFileDiffError,
    InternalErrorOnRun,
    InternalDeprecation,
    InvalidConcurrentBatchesConfig,
    InvalidDisabledTargetInTestNode,
    InvalidMacroAnnotation,
    InvalidOptionYAML,
    InvalidProfileTemplateYAML,
    InvalidValueForField,
    JinjaLogDebug,
    JinjaLogInfo,
    ListCmdOut,
    ListRelations,
    LogBatchResult,
    LogCancelLine,
    LogDbtProfileError,
    LogDbtProjectError,
    LogDebugStackTrace,
    LogFreshnessResult,
    LogFunctionResult,
    LogHookEndLine,
    LogHookStartLine,
    LogModelResult,
    LogNodeNoOpResult,
    LogNodeResult,
    LogSeedResult,
    LogSkipBecauseError,
    LogSnapshotResult,
    LogStartBatch,
    LogStartLine,
    MacroNotFoundForPatch,
    MainEncounteredError,
    MainKeyboardInterrupt,
    MainReportArgs,
    MainReportVersion,
    MainStackTrace,
    MainTrackingUserState,
    MarkSkippedChildren,
    MicrobatchExecutionDebug,
    MicrobatchModelNoEventTimeInputs,
    MissingArgumentsPropertyInGenericTestDeprecation,
    MissingPlusPrefixDeprecation,
    MissingProfileTarget,
    ModelParamUsageDeprecation,
    ModulesItertoolsUsageDeprecation,
    NewConnection,
    NewConnectionOpening,
    NoNodeForYamlKey,
    NoNodesForSelectionCriteria,
    NoSampleProfileFound,
    NodeCompiling,
    NodeConnectionReleaseError,
    NodeExecuting,
    NodeFinished,
    NodeStart,
    Note,
    OpenCommand,
    PackageInstallPathDeprecation,
    PackageNodeDependsOnRootProjectNode,
    ParseInlineNodeError,
    ParsePerfInfoPath,
    ParsedFileLoadFailed,
    PartialParsingEnabled,
    PartialParsingError,
    PartialParsingErrorProcessingFile,
    PartialParsingFile,
    PartialParsingNotEnabled,
    PartialParsingSkipParsing,
    PluginLoadError,
    PrintEvent,
    ProfileWrittenWithProjectTemplateYAML,
    ProfileWrittenWithSample,
    ProfileWrittenWithTargetTemplateYAML,
    ProjectCreated,
    ProjectNameAlreadyExists,
    PropertyMovedToConfigDeprecation,
    ProtectedCleanPath,
    QueryCancelationUnsupported,
    RecordReplayIssue,
    RecordRetryException,
    RegistryIndexProgressGETRequest,
    RegistryIndexProgressGETResponse,
    RegistryProgressGETRequest,
    RegistryProgressGETResponse,
    RegistryResponseExtraNestedKeys,
    RegistryResponseMissingNestedKeys,
    RegistryResponseMissingTopKeys,
    RegistryResponseUnexpectedType,
    ResourceNamesWithSpacesDeprecation,
    ResourceReport,
    RetryExternalCall,
    Rollback,
    RollbackFailed,
    RunResultError,
    RunResultErrorNoMessage,
    RunResultFailure,
    RunResultWarning,
    RunResultWarningMessage,
    RunningOperationCaughtError,
    RunningOperationUncaughtError,
    SQLCommit,
    SQLCompiledPath,
    SQLQuery,
    SQLQueryStatus,
    SQLRunnerException,
    SchemaCreation,
    SchemaDrop,
    SeedExceedsLimitAndPathChanged,
    SeedExceedsLimitChecksumChanged,
    SeedHeader,
    SelectExcludeIgnoredWithSelectorWarning,
    SelectorReportInvalidSelector,
    SendEventFailure,
    SendingEvent,
    SettingUpProfile,
    ShowNode,
    SkippingDetails,
    SourceOverrideDeprecation,
    SpacesInResourceNameDeprecation,
    StarterProjectPath,
    StateCheckVarsHash,
    StatsLine,
    SystemCouldNotWrite,
    SystemExecutingCmd,
    SystemReportReturnCode,
    SystemStdErr,
    SystemStdOut,
    TimingInfoCollected,
    TrackingInitializeFailure,
    TypeCodeNotFound,
    UnableToPartialParse,
    UnexpectedJinjaBlockDeprecation,
    UnpinnedRefNewVersionAvailable,
    UnsupportedConstraintMaterialization,
    UnusedResourceConfigPath,
    UnversionedBreakingChange,
    WarnLevel,
    WriteCatalogFailure,
    WritingInjectedSQLForNode,
}

/// Legacy dbt-core event names that Fusion will not support in `warn-error-options` configuration, either because they are no longer relevant or can't be implemented in a way that provides value to users.
///
/// NOTE: each variant in this enum should have a message describing why it will not be supported, matching
/// public documentation.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    EnumMessage,
    EnumString,
    AsRefStr,
)]
pub enum WillNotSupportLegacyWarnError {
    #[strum(
        message = "Fusion only supports the newer behavior-change flag, where this case is a hard error."
    )]
    MicrobatchMacroOutsideOfBatchesDeprecation,
    #[strum(
        message = "This warning comes from partial parsing in dbt Core, which Fusion does not support."
    )]
    SeedExceedsLimitSamePath,
    #[strum(
        message = "This warning comes from partial parsing in dbt Core, which Fusion does not support."
    )]
    SeedIncreased,
    #[strum(
        message = "Fusion only supports the newer behavior-change flag, where this case is a hard error."
    )]
    GenerateSchemaNameNullValueDeprecation,
    #[strum(
        message = "Fusion already implements the new semantic layer spec, so this legacy warning no longer applies."
    )]
    GenericSemanticLayerDeprecation,
    #[strum(
        message = "Fusion already implements the new semantic layer spec, so this legacy warning no longer applies."
    )]
    MFCumulativeTypeParamsDeprecation,
    #[strum(
        message = "Fusion already implements the new semantic layer spec, so this legacy warning no longer applies."
    )]
    MFTimespineWithoutYamlConfigurationDeprecation,
    #[strum(
        message = "Fusion already implements the new semantic layer spec, so this legacy warning no longer applies."
    )]
    MetricAttributesRenamed,
    #[strum(
        message = "Fusion already implements the new semantic layer spec, so this legacy warning no longer applies."
    )]
    TimeDimensionsRequireGranularityDeprecation,
    #[strum(
        message = "Fusion already uses the newer source freshness behavior, so this legacy warning does not apply."
    )]
    SourceFreshnessProjectHooksNotRun,
    #[strum(message = "Fusion does not support semantic models, so this warning does not apply.")]
    SemanticValidationFailure,
    #[strum(
        message = "Fusion already validates allowed YAML keys strictly, so this warning would be redundant."
    )]
    ValidationWarning,
    #[strum(
        message = "Fusion already enforces the latest behavior, which prevents packages from overriding built-in materializations."
    )]
    PackageMaterializationOverrideDeprecation,
    #[strum(
        message = "Fusion does not surface this warning by default, which matches current dbt Core behavior."
    )]
    TestsConfigDeprecation,
    #[strum(
        message = "Fusion already errors on this configuration, which matches newer dbt Core behavior."
    )]
    ProjectFlagsMovedDeprecation,
    #[strum(message = "This is now fully deprecated in Fusion.")]
    ConfigSourcePathDeprecation,
    #[strum(message = "This is now fully deprecated in Fusion.")]
    ConfigLogPathDeprecation,
    #[strum(message = "This is now fully deprecated in Fusion.")]
    ConfigTargetPathDeprecation,
    #[strum(message = "This is now fully deprecated in Fusion.")]
    ConfigDataPathDeprecation,
    #[strum(
        message = "Fusion reserves the DBT_ENGINE_ prefix and rejects unknown environment variables that use it."
    )]
    EnvironmentVariableNamespaceDeprecation,
    #[strum(
        message = "Fusion does not allow source overrides, so packages must disable a source explicitly instead."
    )]
    UnusedTables,
    #[strum(message = "Fusion reports this case under NoNodeForYamlKey instead.")]
    WrongResourceSchemaFile,
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn enums_are_not_overlapping() {
        let all_not_yet_supported_names = NotYetSupportedLegacyWarnError::iter()
            .map(|variant| variant.as_ref().to_string())
            .collect::<Vec<_>>();
        let all_will_not_support_names = WillNotSupportLegacyWarnError::iter()
            .map(|variant| variant.as_ref().to_string())
            .collect::<Vec<_>>();

        for supported in SupportedLegacyWarnError::iter() {
            assert!(
                !all_not_yet_supported_names.contains(&supported.as_ref().to_string())
                    && !all_will_not_support_names.contains(&supported.as_ref().to_string()),
                "Variant `{}` of `SupportedLegacyWarnError` should not also be present in `NotYetSupportedLegacyWarnError` or `WillNotSupportLegacyWarnError`",
                supported.as_ref()
            );
        }

        for not_yet_supported in NotYetSupportedLegacyWarnError::iter() {
            assert!(
                !all_will_not_support_names.contains(&not_yet_supported.as_ref().to_string()),
                "Variant `{}` of `NotYetSupportedLegacyWarnError` should not also be present in `WillNotSupportLegacyWarnError`",
                not_yet_supported.as_ref()
            );
        }
    }

    #[test]
    fn will_not_support_legacy_all_have_messages() {
        for variant in WillNotSupportLegacyWarnError::iter() {
            assert!(
                !variant.get_message().unwrap_or("").is_empty(),
                "Variant `{}` of `WillNotSupportLegacyWarnError` must have a non-empty message describing why it will not be supported",
                variant.as_ref()
            );
        }
    }
}
