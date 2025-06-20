use std::{collections::HashMap, fmt::Display};

use dbt_proc_macros::include_frontend_error_codes;
use int_enum::IntEnum;

/// Error codes for the SDF CLI.
///
/// Error codes define the general "semantic type" of a [FsError]. Each error
/// code is a 4-digit number stored as a u16 type.
#[include_frontend_error_codes]
#[repr(u16)]
#[non_exhaustive]
#[derive(Debug, Copy, Clone, Eq, PartialEq, IntEnum)]
pub enum ErrorCode {
    // ----------------- Frontend errors [0, 999] -----------------------------
    //
    // This section contains user-facing error codes originating from the
    // frontend. Frontend error codes occupy the range [0, 999]
    //
    // **NOTE**: this section is auto-synced with
    // [dbt_frontend_common::error::ErrorCode] by way of the
    // `include_frontend_error_codes` macro. **DO NOT** manually add any error
    // codes in this range here, add them to [dbt_frontend_common::error::ErrorCode]
    // instead.

    // ----------------- CLI errors [1000, 8999] ------------------------------
    //
    // This section contains user-facing error codes originating from the CLI.
    // CLI error codes occupy the range [1000, 8999]
    //
    // Define all CLI error codes here.
    /// Default catch-all code for when you're too lazy to specify a proper code
    Generic = 1000,
    IoError = 1001,
    EncodingError = 1002,
    FileIoError = 1003,
    CacheError = 1004,
    InvalidConfig = 1005,
    InvalidPath = 1006,
    InvalidArgument = 1007,
    MissingArgument = 1008,
    InferenceError = 1009,
    InvalidTable = 1010,
    AuthenticationError = 1011,
    MissingClassifiers = 1012,
    SerializationError = 1013,
    RemoteError = 1014,
    ExecutionError = 1015,
    ArrowError = 1016,
    ParquetError = 1017,
    ObjectStoreError = 1018,
    LogicalPlanError = 1019,
    ResourceError = 1020,
    GenericDatafusionError = 1021,
    CyclicDependency = 1022,
    UnsupportedFileFormat = 1023,
    FileNotFound = 1024,
    MissingTable = 1025,
    InvalidType = 1026,
    MergeConflict = 1027,
    MissingSourceLocation = 1028,
    TooManyRows = 1029,
    TableMissingProvider = 1030,
    AmbiguousRenamingSpecification = 1031,
    UndefinedField = 1032,
    DuplicateColumns = 1033,
    MissingWorkspaceFile = 1034,
    InvalidEnvironment = 1035,
    DuplicateEnvironment = 1036,
    UnsupportedWorkspaceEdition = 1037,
    CredentialsError = 1038,
    LintCheckFailed = 1039,
    SubprocessError = 1040,
    FmtError = 1041,
    FunctionDefinitionError = 1042,
    BuildError = 1043,
    UnimplementedFunction = 1044,
    NoTableFoundForPrefix = 1045,

    AmbiguousSourceSchema = 1046,
    UnsupportedLogicalPlanForLocalExecution = 1047,
    DependencyNotFound = 1048,
    UnsupportedFileExtension = 1049,
    SkippedArtifact = 1050,

    // fs db errors
    FailedToCreateDatabase = 1051,
    FailedToRegisterSeedTable = 1052,
    FailedToRegisterExistingTable = 1053,
    FailedToWriteTable = 1054,
    FailedToLookupExistingTable = 1055,

    MissingTargetDirectory = 1056,
    ColumnTypeMismatch = 1058,
    DuplicateConfigKey = 1059,
    UnusedConfigKey = 1060,
    InvalidCsvFormat = 1061,

    /// Error code for when a model tries to reference a disabled ref or source
    DisabledDependency = 1062,

    StaleSource = 1063,

    DisabledModel = 1064,

    // --------------------------------------------------------------------------------------------
    // Jinja
    MacroUnsupportedValueType = 1500,
    JinjaError = 1501,
    MacroSyntaxError = 1502,
    MacroVarNotFound = 1503,
    InvalidSeedValue = 1504,
    MacroUseIllegal = 1505,

    // --------------------------------------------------------------------------------------------
    // Local execution
    SelectorError = 1600,
    NoNodesSelected = 1601,

    // --------------------------------------------------------------------------------------------
    // CLI errors
    NoLongerSupportedOption = 1700,
    NotYetSupportedOption = 1701,
    DeprecatedOption = 1702,

    // --------------------------------------------------------------------------------------------
    // Local execution
    SessionError = 2000,
    UnsupportedLocalExecutionDialect = 2001,

    // --------------------------------------------------------------------------------------------
    // Error parsing an .slt file
    SltParse = 3000,
    SltLimits = 3001,
    SltConfig = 3002,
    SltDatabaseError = 3003,

    // --------------------------------------------------------------------------------------------
    // Lineage
    InvalidLineageSchema = 3500,

    InvalidDialect = 8998,
    RuntimeError = 8999,
    InvalidUserInput = 8997,
    InvalidOptions = 8996,
    OperationCanceled = 8995,

    // -----------------  ---------------------
    // CLI Internal errors [9000, 9899]
    // Everything below this line is an internal error. They will be presented
    // as bugs if surfaced to the user.
    NotSupported = 9000,
    Unknown = 9001,
    Unexpected = 9002,
    NotImplemented = 9003,
    InvalidTableNameInCLI = 9004,
    CoalesceHasOnlyNulls = 9005,
    // ExitRepl is not really an error, but a special error code that is used to
    // signal the repl to exit gracefully:
    ExitRepl = 9006,
    // ----------------- Internal errors from frontend [9900, 9999] -----------
    // This section contains the internal error codes from the frontend.
    //
    // **NOTE**: this section is auto-synced with
    // [dbt_frontend_common::error::ErrorCode] by way of the
    // `include_frontend_error_codes` macro. **DO NOT** manually add any error
    // codes in this range here, add them to [dbt_frontend_common::error::ErrorCode]
    // instead.
}
impl std::hash::Hash for ErrorCode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (*self as u16).hash(state)
    }
}

impl Default for ErrorCode {
    fn default() -> Self {
        Self::Generic
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:04}", *self as u16)
    }
}

impl ErrorCode {
    pub fn is_bug(&self) -> bool {
        (*self as u16) >= (Self::NotSupported as u16)
    }

    pub fn is_frontend(&self) -> bool {
        (*self as u16) < (Self::Generic as u16)
    }
}

impl From<dbt_frontend_common::error::ErrorCode> for ErrorCode {
    fn from(code: dbt_frontend_common::error::ErrorCode) -> Self {
        let frontend_code = code as u16;
        if frontend_code < dbt_frontend_common::error::ErrorCode::NotSupported as u16 {
            Self::try_from(frontend_code).expect("invalid cli error code: {frontend_code}")
        } else {
            // Internal errors map to the 9k range:
            Self::try_from(frontend_code + 9000).expect("invalid cli error code: {frontend_code}")
        }
    }
}
/// General warning handling. Warnings are controlled via -w from the CLI.
///
/// Warnings can be set and unset. They are usually passed as part of EvalArg.
///
/// A warning is active if its key in the Warnings hashmap is defined.
/// The value of the key can be used to provide additional info, for instance
/// for the warning capitalization_identifier:upper, use the error code for
/// capitalization_identifier as key and the string "upper" as value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warnings {
    // todo: better representation, but good enough for now...
    pub values: HashMap<ErrorCode, String>,
}

impl Warnings {
    /// Creates an empty Warning instance.
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Adds an error code to the warnings.
    pub fn with_error_code(mut self, code: ErrorCode) -> Self {
        self.values.insert(code, String::new());
        self
    }

    /// Adds an error code to the warnings with a specified value.
    pub fn with_error_code_and_value(mut self, code: ErrorCode, value: String) -> Self {
        self.values.insert(code, value);
        self
    }

    /// Checks if the warnings is turned on.
    pub fn contains(&self, code: &ErrorCode) -> bool {
        self.values.contains_key(code)
    }

    /// Checks if there are no warnings.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns an iterator over the error codes and their corresponding values in the warnings.
    pub fn iter(&self) -> impl Iterator<Item = (&ErrorCode, &String)> {
        self.values.iter()
    }
}

impl Default for Warnings {
    /// Creates a new Warnings instance with an empty hashmap.
    fn default() -> Self {
        Warnings::new()
    }
}
