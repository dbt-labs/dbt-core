//! Arrow serialization support for telemetry records using serde_arrow.

use super::to_nanos;
use crate::{
    BuildPhase, BuildPhaseInfo, DevInternalInfo, InvocationCloudAttributes, InvocationEvalArgs,
    InvocationInfo, InvocationMetrics, LegacyLogEventInfo, LogEventInfo, LogRecordInfo,
    NodeExecutionStatus, NodeIdentifier, NodeInfo, ProcessInfo, RecordCodeLocation, SeverityNumber,
    SharedPhaseInfo, SpanEndInfo, SpanStartInfo, SpanStatus, StatusCode, TelemetryAttributes,
    TelemetryAttributesType, TelemetryRecord, TelemetryRecordType, UnknownInfo, UpdateInfo,
    WriteArtifactInfo,
};
use arrow::{
    array::Array,
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Field, FieldRef, Schema, TimeUnit},
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::time::SystemTime;
use std::{borrow::Cow, sync::Arc};

// Create sudo impls for defaults on these two enums. This is only necessary
// to make `ArrowTelemetryRecord` derive `Default` automatically, which in turn
// simplifies the conversion from `TelemetryRecord` to `ArrowTelemetryRecord`.
// During conversion we always set the `record_type` & `event_type` fields,
// so default implementations are not used in practice.
#[allow(clippy::derivable_impls)]
impl Default for TelemetryRecordType {
    fn default() -> Self {
        TelemetryRecordType::LogRecord
    }
}

#[allow(clippy::derivable_impls)]
impl Default for TelemetryAttributesType {
    fn default() -> Self {
        TelemetryAttributesType::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArrowTelemetryRecord<'a> {
    pub record_type: TelemetryRecordType,
    pub trace_id: String, // Arrow doesn't support u128 natively, so...
    pub span_id: Option<u64>,
    pub span_name: Option<&'a str>,
    pub parent_span_id: Option<u64>,
    pub start_time_unix_nano: Option<u64>,
    pub end_time_unix_nano: Option<u64>,
    pub time_unix_nano: Option<u64>,
    pub severity_number: u8,
    pub severity_text: &'a str,
    pub body: Option<&'a str>,
    pub status_code: Option<u32>,
    pub status_message: Option<&'a str>,
    pub event_type: TelemetryAttributesType,
    pub attributes: ArrowAttributes<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArrowAttributes<'a> {
    // Process fields
    pub schema_url: Option<&'a str>,
    pub schema_version: Option<u16>,
    pub package: Option<&'a str>,
    pub version: Option<&'a str>,
    pub host_os: Option<&'a str>,
    pub host_arch: Option<&'a str>,
    // Invocation fields
    pub invocation_id: Option<&'a str>,
    pub raw_command: Option<&'a str>,
    // Invocation eval args
    pub command: Option<&'a str>,
    pub profiles_dir: Option<&'a str>,
    pub packages_install_path: Option<&'a str>,
    pub target: Option<&'a str>,
    pub profile: Option<&'a str>,
    pub vars: Option<String>, // owned due to JSON serialization
    pub limit: Option<u64>,
    pub num_threads: Option<u64>,
    pub selector: Option<&'a str>,
    #[serde(borrow)]
    pub select: Option<Cow<'a, [String]>>,
    #[serde(borrow)]
    pub exclude: Option<Cow<'a, [String]>>,
    pub indirect_selection: Option<&'a str>,
    #[serde(borrow)]
    pub output_keys: Option<Cow<'a, [String]>>,
    #[serde(borrow)]
    pub resource_types: Option<Cow<'a, [String]>>,
    #[serde(borrow)]
    pub exclude_resource_types: Option<Cow<'a, [String]>>,
    pub debug: Option<bool>,
    pub log_format: Option<&'a str>,
    pub log_level: Option<&'a str>,
    pub log_path: Option<&'a str>,
    pub target_path: Option<&'a str>,
    pub project_dir: Option<&'a str>,
    pub quiet: Option<bool>,
    pub write_json: Option<bool>,
    pub write_catalog: Option<bool>,
    pub update_deps: Option<bool>,
    pub replay_mode: Option<&'a str>,
    pub replay_path: Option<&'a str>,
    pub static_analysis: Option<&'a str>,
    pub interactive: Option<bool>,
    pub task_cache_url: Option<&'a str>,
    pub run_cache_mode: Option<&'a str>,
    pub show_scans: Option<bool>,
    pub max_depth: Option<u64>,
    pub use_fqtn: Option<bool>,
    pub skip_unreferenced_table_check: Option<bool>,
    pub state: Option<&'a str>,
    pub defer_state: Option<&'a str>,
    pub connection: Option<bool>,
    pub warn_error: Option<bool>,
    pub warn_error_options: Option<String>, // owned due to JSON serialization
    pub version_check: Option<bool>,
    pub defer: Option<bool>,
    pub fail_fast: Option<bool>,
    pub empty: Option<bool>,
    pub sample: Option<&'a str>,
    pub full_refresh: Option<bool>,
    pub favor_state: Option<bool>,
    pub refresh_sources: Option<bool>,
    pub send_anonymous_usage_stats: Option<bool>,
    pub check_all: Option<bool>,
    // Invocation cloud attributes
    pub account_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub job_id: Option<&'a str>,
    pub run_id: Option<&'a str>,
    pub run_reason: Option<&'a str>,
    pub run_reason_category: Option<&'a str>,
    pub run_trigger_category: Option<&'a str>,
    pub project_id: Option<&'a str>,
    // Invocation metrics
    pub total_errors: Option<u64>,
    pub total_warnings: Option<u64>,
    pub autofix_suggestions: Option<u64>,

    // Update fields
    pub update_version: Option<&'a str>,
    pub update_package: Option<&'a str>,
    pub exe_path: Option<&'a str>,
    // Onboarding fields
    pub onboarding_step: Option<&'a str>,
    pub onboarding_invocation_id: Option<&'a str>,
    // Phase fields - BuildPhaseInfo union fields
    pub phase: Option<BuildPhase>,
    pub node_count: Option<u64>,
    // Node fields
    pub unique_id: Option<&'a str>,
    pub fqn: Option<&'a str>,
    pub status: Option<NodeExecutionStatus>,
    pub num_rows: Option<u64>,
    // DevInternal/Unknown fields
    pub dev_name: Option<&'a str>,
    // Location fields (common to multiple variants)
    pub file: Option<&'a str>,
    pub line: Option<u32>,
    pub module_path: Option<&'a str>,
    pub location_target: Option<&'a str>,
    // Log fields
    pub code: Option<u32>,
    pub dbt_core_code: Option<&'a str>,
    pub original_severity_number: Option<u8>,
    pub original_severity_text: Option<&'a str>,
    // WriteArtifact fields
    pub relative_path: Option<&'a str>,
    pub duration_ms: Option<u64>,
    // Event type for discriminated union. This is a duplicate
    // from the owning `ArrowTelemetryRecord` but is usefull
    // as it allows working with attributes outside of the record context.
    pub event_type: TelemetryAttributesType,
}

#[inline]
fn nanos_to_system_time(nanos: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(nanos)
}

fn arrow_to_location(arrow: &ArrowAttributes) -> RecordCodeLocation {
    RecordCodeLocation {
        file: arrow.file.map(str::to_string),
        line: arrow.line,
        module_path: arrow.module_path.map(str::to_string),
        target: arrow.location_target.map(str::to_string),
    }
}

impl<'a> From<&'a TelemetryRecord> for ArrowTelemetryRecord<'a> {
    fn from(record: &'a TelemetryRecord) -> Self {
        match record {
            TelemetryRecord::SpanStart(span) => {
                let attributes = ArrowAttributes::from(&span.attributes);
                ArrowTelemetryRecord {
                    record_type: record.into(),
                    trace_id: format!("{:032x}", span.trace_id),
                    span_id: Some(span.span_id),
                    span_name: Some(&span.span_name),
                    parent_span_id: span.parent_span_id,
                    start_time_unix_nano: Some(to_nanos(&span.start_time_unix_nano)),
                    end_time_unix_nano: None,
                    time_unix_nano: None,
                    severity_number: span.severity_number as u8,
                    severity_text: span.severity_text.as_ref(),
                    body: None,
                    status_code: None,
                    status_message: None,
                    event_type: TelemetryAttributesType::from(&span.attributes),
                    attributes,
                }
            }
            TelemetryRecord::SpanEnd(span) => {
                let attributes = ArrowAttributes::from(&span.attributes);
                ArrowTelemetryRecord {
                    record_type: record.into(),
                    trace_id: format!("{:032x}", span.trace_id),
                    span_id: Some(span.span_id),
                    span_name: Some(&span.span_name),
                    parent_span_id: span.parent_span_id,
                    start_time_unix_nano: Some(to_nanos(&span.start_time_unix_nano)),
                    end_time_unix_nano: Some(to_nanos(&span.end_time_unix_nano)),
                    time_unix_nano: None,
                    severity_number: span.severity_number as u8,
                    severity_text: span.severity_text.as_ref(),
                    body: None,
                    status_code: span.status.as_ref().map(|s| s.code as u32),
                    status_message: span.status.as_ref().and_then(|s| s.message.as_deref()),
                    event_type: TelemetryAttributesType::from(&span.attributes),
                    attributes,
                }
            }
            TelemetryRecord::LogRecord(log) => {
                let attributes = ArrowAttributes::from(&log.attributes);
                ArrowTelemetryRecord {
                    record_type: record.into(),
                    trace_id: format!("{:032x}", log.trace_id),
                    span_id: log.span_id,
                    span_name: log.span_name.as_deref(),
                    parent_span_id: None,
                    start_time_unix_nano: None,
                    end_time_unix_nano: None,
                    time_unix_nano: Some(to_nanos(&log.time_unix_nano)),
                    severity_number: log.severity_number as u8,
                    severity_text: log.severity_text.as_ref(),
                    body: Some(log.body.as_ref()),
                    status_code: None,
                    status_message: None,
                    event_type: TelemetryAttributesType::from(&log.attributes),
                    attributes,
                }
            }
        }
    }
}

impl<'a> From<&'a TelemetryAttributes> for ArrowAttributes<'a> {
    fn from(attr: &'a TelemetryAttributes) -> Self {
        match attr {
            TelemetryAttributes::Process(ProcessInfo {
                schema_url,
                schema_version,
                package,
                version,
                host_os,
                host_arch,
            }) => ArrowAttributes {
                schema_url: Some(schema_url.as_str()),
                schema_version: Some(*schema_version),
                package: Some(package.as_str()),
                version: Some(version),
                host_os: Some(host_os),
                host_arch: Some(host_arch),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Invocation(boxed_info) => {
                let InvocationInfo {
                    invocation_id,
                    raw_command,
                    eval_args,
                    process_info,
                    cloud_args,
                    metrics,
                } = boxed_info.as_ref();

                ArrowAttributes {
                    invocation_id: Some(invocation_id),
                    raw_command: Some(raw_command),
                    // Eval args
                    command: Some(eval_args.command.as_str()),
                    profiles_dir: eval_args.profiles_dir.as_deref(),
                    packages_install_path: eval_args.packages_install_path.as_deref(),
                    target: eval_args.target.as_deref(),
                    profile: eval_args.profile.as_deref(),
                    vars: Some(
                        serde_json::to_string(&eval_args.vars)
                            .expect("Failed to serialize vars to JSON"),
                    ),
                    limit: eval_args.limit,
                    num_threads: eval_args.num_threads,
                    selector: eval_args.selector.as_deref(),
                    select: Some(Cow::from(&eval_args.select)),
                    exclude: Some(Cow::from(&eval_args.exclude)),
                    indirect_selection: eval_args.indirect_selection.as_deref(),
                    output_keys: Some(Cow::from(&eval_args.output_keys)),
                    resource_types: Some(Cow::from(&eval_args.resource_types)),
                    exclude_resource_types: Some(Cow::from(&eval_args.exclude_resource_types)),
                    debug: Some(eval_args.debug),
                    log_format: Some(eval_args.log_format.as_str()),
                    log_level: eval_args.log_level.as_deref(),
                    log_path: eval_args.log_path.as_deref(),
                    target_path: eval_args.target_path.as_deref(),
                    project_dir: eval_args.project_dir.as_deref(),
                    quiet: Some(eval_args.quiet),
                    write_json: Some(eval_args.write_json),
                    write_catalog: Some(eval_args.write_catalog),
                    update_deps: Some(eval_args.update_deps),
                    replay_mode: eval_args.replay_mode.as_deref(),
                    replay_path: eval_args.replay_path.as_deref(),
                    static_analysis: Some(eval_args.static_analysis.as_str()),
                    interactive: Some(eval_args.interactive),
                    task_cache_url: Some(eval_args.task_cache_url.as_str()),
                    run_cache_mode: Some(eval_args.run_cache_mode.as_str()),
                    show_scans: Some(eval_args.show_scans),
                    max_depth: Some(eval_args.max_depth),
                    use_fqtn: Some(eval_args.use_fqtn),
                    skip_unreferenced_table_check: Some(eval_args.skip_unreferenced_table_check),
                    state: eval_args.state.as_deref(),
                    defer_state: eval_args.defer_state.as_deref(),
                    connection: Some(eval_args.connection),
                    warn_error: Some(eval_args.warn_error),
                    warn_error_options: Some(
                        serde_json::to_string(&eval_args.warn_error_options)
                            .expect("Failed to serialize warn_error_options to JSON"),
                    ),
                    version_check: Some(eval_args.version_check),
                    defer: eval_args.defer,
                    fail_fast: Some(eval_args.fail_fast),
                    empty: Some(eval_args.empty),
                    sample: eval_args.sample.as_deref(),
                    full_refresh: Some(eval_args.full_refresh),
                    favor_state: Some(eval_args.favor_state),
                    refresh_sources: Some(eval_args.refresh_sources),
                    send_anonymous_usage_stats: Some(eval_args.send_anonymous_usage_stats),
                    check_all: Some(eval_args.check_all),
                    // Process attributes
                    schema_url: Some(process_info.schema_url.as_str()),
                    schema_version: Some(process_info.schema_version),
                    package: Some(process_info.package.as_str()),
                    version: Some(process_info.version.as_str()),
                    host_os: Some(process_info.host_os.as_str()),
                    host_arch: Some(process_info.host_arch.as_str()),
                    // Cloud attributes
                    account_id: cloud_args.account_id.as_deref(),
                    environment_id: cloud_args.environment_id.as_deref(),
                    job_id: cloud_args.job_id.as_deref(),
                    run_id: cloud_args.run_id.as_deref(),
                    run_reason: cloud_args.run_reason.as_deref(),
                    run_reason_category: cloud_args.run_reason_category.as_deref(),
                    run_trigger_category: cloud_args.run_trigger_category.as_deref(),
                    project_id: cloud_args.project_id.as_deref(),
                    // Metrics
                    total_errors: metrics.total_errors,
                    total_warnings: metrics.total_warnings,
                    autofix_suggestions: metrics.autofix_suggestions,
                    event_type: TelemetryAttributesType::from(attr),
                    ..Default::default()
                }
            }
            TelemetryAttributes::Update(UpdateInfo {
                version,
                package,
                exe_path,
            }) => ArrowAttributes {
                update_version: version.as_deref(),
                update_package: package.as_deref(),
                exe_path: exe_path.as_deref(),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Phase(phase_info) => match phase_info {
                BuildPhaseInfo::Loading { shared }
                | BuildPhaseInfo::DependencyLoading { shared }
                | BuildPhaseInfo::Parsing { shared }
                | BuildPhaseInfo::Scheduling { shared }
                | BuildPhaseInfo::FreshnessAnalysis { shared }
                | BuildPhaseInfo::Lineage { shared } => ArrowAttributes {
                    phase: Some(phase_info.into()),
                    invocation_id: Some(&shared.invocation_id),
                    event_type: TelemetryAttributesType::from(attr),
                    ..Default::default()
                },
                BuildPhaseInfo::Analyzing { shared, node_count }
                | BuildPhaseInfo::Hydrating { shared, node_count }
                | BuildPhaseInfo::Compiling { shared, node_count }
                | BuildPhaseInfo::Executing { shared, node_count } => ArrowAttributes {
                    phase: Some(phase_info.into()),
                    invocation_id: Some(&shared.invocation_id),
                    node_count: Some(*node_count),
                    event_type: TelemetryAttributesType::from(attr),
                    ..Default::default()
                },
            },
            TelemetryAttributes::Node(NodeInfo {
                node_id,
                phase,
                status,
                num_rows,
            }) => ArrowAttributes {
                unique_id: Some(&node_id.unique_id),
                fqn: Some(&node_id.fqn),
                phase: Some(*phase),
                status: *status,
                num_rows: *num_rows,
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::DevInternal(DevInternalInfo {
                name,
                location,
                extra: _, // never serialized
            })
            | TelemetryAttributes::Unknown(UnknownInfo { name, location }) => ArrowAttributes {
                dev_name: Some(name),
                file: location.file.as_deref(),
                line: location.line,
                module_path: location.module_path.as_deref(),
                location_target: location.target.as_deref(),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Log(LogEventInfo {
                code,
                dbt_core_code,
                original_severity_number,
                original_severity_text,
                location,
            }) => ArrowAttributes {
                file: location.file.as_deref(),
                line: location.line,
                module_path: location.module_path.as_deref(),
                location_target: location.target.as_deref(),
                code: *code,
                dbt_core_code: dbt_core_code.as_deref(),
                original_severity_number: Some(*original_severity_number as u8),
                original_severity_text: Some(original_severity_text.as_ref()),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::LegacyLog(LegacyLogEventInfo {
                original_severity_number,
                original_severity_text,
                location,
            }) => ArrowAttributes {
                file: location.file.as_deref(),
                line: location.line,
                module_path: location.module_path.as_deref(),
                location_target: location.target.as_deref(),
                original_severity_number: Some(*original_severity_number as u8),
                original_severity_text: Some(original_severity_text.as_ref()),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::WriteArtifact(WriteArtifactInfo {
                relative_path,
                duration_ms,
            }) => ArrowAttributes {
                relative_path: relative_path.as_deref(),
                duration_ms: *duration_ms,
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
            TelemetryAttributes::Onboarding(info) => ArrowAttributes {
                onboarding_step: Some(&info.step),
                onboarding_invocation_id: Some(&info.invocation_id),
                event_type: TelemetryAttributesType::from(attr),
                ..Default::default()
            },
        }
    }
}

impl TryFrom<ArrowTelemetryRecord<'_>> for TelemetryRecord {
    type Error = String;

    fn try_from(arrow: ArrowTelemetryRecord) -> Result<Self, Self::Error> {
        match arrow.record_type {
            TelemetryRecordType::SpanStart => {
                let span_id = arrow
                    .span_id
                    .ok_or("Missing span_id for SpanStart record")?;
                let span_name = arrow
                    .span_name
                    .ok_or("Missing span_name for SpanStart record")?;
                let start_time_unix_nano = arrow
                    .start_time_unix_nano
                    .ok_or("Missing start_time_unix_nano for SpanStart record")?;

                Ok(TelemetryRecord::SpanStart(SpanStartInfo {
                    trace_id: u128::from_str_radix(&arrow.trace_id, 16)
                        .map_err(|e| format!("Invalid trace_id: {e}"))?,
                    span_id,
                    parent_span_id: arrow.parent_span_id,
                    span_name: span_name.to_string(),
                    start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                    attributes: TelemetryAttributes::try_from(arrow.attributes)?,
                    severity_number: SeverityNumber::from_repr(arrow.severity_number)
                        .ok_or("Invalid severity_number")?,
                    severity_text: arrow.severity_text.to_string(),
                }))
            }
            TelemetryRecordType::SpanEnd => {
                let span_id = arrow.span_id.ok_or("Missing span_id for SpanEnd record")?;
                let span_name = arrow
                    .span_name
                    .ok_or("Missing span_name for SpanEnd record")?;
                let start_time_unix_nano = arrow
                    .start_time_unix_nano
                    .ok_or("Missing start_time_unix_nano for SpanEnd record")?;
                let end_time_unix_nano = arrow
                    .end_time_unix_nano
                    .ok_or("Missing end_time_unix_nano for SpanEnd record")?;

                let status = if arrow.status_code.is_some() || arrow.status_message.is_some() {
                    Some(SpanStatus {
                        code: StatusCode::from_repr(arrow.status_code.unwrap_or(0) as u8)
                            .unwrap_or(StatusCode::Unset),
                        message: arrow.status_message.map(str::to_string),
                    })
                } else {
                    None
                };

                Ok(TelemetryRecord::SpanEnd(SpanEndInfo {
                    trace_id: u128::from_str_radix(&arrow.trace_id, 16)
                        .map_err(|e| format!("Invalid trace_id: {e}"))?,
                    span_id,
                    parent_span_id: arrow.parent_span_id,
                    span_name: span_name.to_string(),
                    start_time_unix_nano: nanos_to_system_time(start_time_unix_nano),
                    end_time_unix_nano: nanos_to_system_time(end_time_unix_nano),
                    attributes: TelemetryAttributes::try_from(arrow.attributes)?,
                    status,
                    severity_number: SeverityNumber::from_repr(arrow.severity_number)
                        .ok_or("Invalid severity_number")?,
                    severity_text: arrow.severity_text.to_string(),
                }))
            }
            TelemetryRecordType::LogRecord => {
                let time_unix_nano = arrow
                    .time_unix_nano
                    .ok_or("Missing time_unix_nano for LogRecord")?;
                let body = arrow.body.ok_or("Missing body for LogRecord")?;

                Ok(TelemetryRecord::LogRecord(LogRecordInfo {
                    time_unix_nano: nanos_to_system_time(time_unix_nano),
                    trace_id: u128::from_str_radix(&arrow.trace_id, 16)
                        .map_err(|e| format!("Invalid trace_id: {e}"))?,
                    span_id: arrow.span_id,
                    span_name: arrow.span_name.map(str::to_string),
                    severity_number: SeverityNumber::from_repr(arrow.severity_number)
                        .ok_or("Invalid severity_number")?,
                    severity_text: arrow.severity_text.to_string(),
                    body: body.to_string(),
                    attributes: TelemetryAttributes::try_from(arrow.attributes)?,
                }))
            }
        }
    }
}

impl TryFrom<&ArrowAttributes<'_>> for ProcessInfo {
    type Error = String;

    fn try_from(arrow: &ArrowAttributes) -> Result<Self, Self::Error> {
        Ok(ProcessInfo {
            schema_url: arrow
                .schema_url
                .map(str::to_string)
                .ok_or("Missing schema_url for Process attributes")?,
            schema_version: arrow
                .schema_version
                .ok_or("Missing schema_version for Process attributes")?,
            package: arrow
                .package
                .map(str::to_string)
                .ok_or("Missing package for Process attributes")?,
            version: arrow
                .version
                .map(str::to_string)
                .ok_or("Missing version for Process attributes")?,
            host_os: arrow
                .host_os
                .map(str::to_string)
                .ok_or("Missing host_os for Process attributes")?,
            host_arch: arrow
                .host_arch
                .map(str::to_string)
                .ok_or("Missing host_arch for Process attributes")?,
        })
    }
}

impl TryFrom<&ArrowAttributes<'_>> for InvocationEvalArgs {
    type Error = String;

    fn try_from(arrow: &ArrowAttributes) -> Result<Self, Self::Error> {
        let vars = serde_json::from_str(
            arrow
                .vars
                .as_ref()
                .ok_or("Missing vars for Invocation attributes")?,
        )
        .map_err(|e| format!("Failed to parse vars JSON: {e}"))?;

        let warn_error_options = serde_json::from_str(
            arrow
                .warn_error_options
                .as_ref()
                .ok_or("Missing warn_error_options for Invocation attributes")?,
        )
        .map_err(|e| format!("Failed to parse warn_error_options JSON: {e}"))?;

        Ok(InvocationEvalArgs {
            command: arrow
                .command
                .map(str::to_string)
                .ok_or("Missing command for Invocation attributes")?,
            profiles_dir: arrow.profiles_dir.map(str::to_string),
            packages_install_path: arrow.packages_install_path.map(str::to_string),
            target: arrow.target.map(str::to_string),
            profile: arrow.profile.map(str::to_string),
            vars,
            limit: arrow.limit,
            num_threads: arrow.num_threads,
            selector: arrow.selector.map(str::to_string),
            select: arrow
                .select
                .as_deref()
                .map(|s| s.into())
                .ok_or("Missing select for Invocation attributes")?,
            exclude: arrow
                .exclude
                .as_deref()
                .map(|s| s.into())
                .ok_or("Missing exclude for Invocation attributes")?,
            indirect_selection: arrow.indirect_selection.map(str::to_string),
            output_keys: arrow
                .output_keys
                .as_deref()
                .map(|s| s.into())
                .ok_or("Missing output_keys for Invocation attributes")?,
            resource_types: arrow
                .resource_types
                .as_deref()
                .map(|s| s.into())
                .ok_or("Missing resource_types for Invocation attributes")?,
            exclude_resource_types: arrow
                .exclude_resource_types
                .as_deref()
                .map(|s| s.into())
                .ok_or("Missing exclude_resource_types for Invocation attributes")?,
            debug: arrow
                .debug
                .ok_or("Missing debug for Invocation attributes")?,
            log_format: arrow
                .log_format
                .map(str::to_string)
                .ok_or("Missing log_format for Invocation attributes")?,
            log_level: arrow.log_level.map(str::to_string),
            log_path: arrow.log_path.map(str::to_string),
            target_path: arrow.target_path.map(str::to_string),
            project_dir: arrow.project_dir.map(str::to_string),
            quiet: arrow
                .quiet
                .ok_or("Missing quiet for Invocation attributes")?,
            write_json: arrow
                .write_json
                .ok_or("Missing write_json for Invocation attributes")?,
            write_catalog: arrow
                .write_catalog
                .ok_or("Missing write_catalog for Invocation attributes")?,
            update_deps: arrow
                .update_deps
                .ok_or("Missing update_deps for Invocation attributes")?,
            replay_mode: arrow.replay_mode.map(str::to_string),
            replay_path: arrow.replay_path.map(str::to_string),
            static_analysis: arrow
                .static_analysis
                .map(str::to_string)
                .ok_or("Missing static_analysis for Invocation attributes")?,
            interactive: arrow
                .interactive
                .ok_or("Missing interactive for Invocation attributes")?,
            task_cache_url: arrow
                .task_cache_url
                .map(str::to_string)
                .ok_or("Missing task_cache_url for Invocation attributes")?,
            run_cache_mode: arrow
                .run_cache_mode
                .map(str::to_string)
                .ok_or("Missing run_cache_mode for Invocation attributes")?,
            show_scans: arrow
                .show_scans
                .ok_or("Missing run_cache_mode for Invocation attributes")?,
            max_depth: arrow
                .max_depth
                .ok_or("Missing run_cache_mode for Invocation attributes")?,
            use_fqtn: arrow
                .use_fqtn
                .ok_or("Missing run_cache_mode for Invocation attributes")?,
            skip_unreferenced_table_check: arrow
                .skip_unreferenced_table_check
                .ok_or("Missing skip_unreferenced_table_check for Invocation attributes")?,
            state: arrow.state.map(str::to_string),
            defer_state: arrow.defer_state.map(str::to_string),
            connection: arrow
                .connection
                .ok_or("Missing connection for Invocation attributes")?,
            warn_error: arrow
                .warn_error
                .ok_or("Missing warn_error for Invocation attributes")?,
            warn_error_options,
            version_check: arrow
                .version_check
                .ok_or("Missing version_check for Invocation attributes")?,
            defer: arrow.defer,
            fail_fast: arrow
                .fail_fast
                .ok_or("Missing fail_fast for Invocation attributes")?,
            empty: arrow
                .empty
                .ok_or("Missing empty for Invocation attributes")?,
            sample: arrow.sample.map(str::to_string),
            full_refresh: arrow
                .full_refresh
                .ok_or("Missing full_refresh for Invocation attributes")?,
            favor_state: arrow
                .favor_state
                .ok_or("Missing favor_state for Invocation attributes")?,
            refresh_sources: arrow
                .refresh_sources
                .ok_or("Missing refresh_sources for Invocation attributes")?,
            send_anonymous_usage_stats: arrow
                .send_anonymous_usage_stats
                .ok_or("Missing send_anonymous_usage_stats for Invocation attributes")?,
            check_all: arrow
                .check_all
                .ok_or("Missing check_all for Invocation attributes")?,
        })
    }
}

impl From<&ArrowAttributes<'_>> for InvocationCloudAttributes {
    fn from(arrow: &ArrowAttributes) -> Self {
        InvocationCloudAttributes {
            account_id: arrow.account_id.map(str::to_string),
            environment_id: arrow.environment_id.map(str::to_string),
            job_id: arrow.job_id.map(str::to_string),
            run_id: arrow.run_id.map(str::to_string),
            run_reason: arrow.run_reason.map(str::to_string),
            run_reason_category: arrow.run_reason_category.map(str::to_string),
            run_trigger_category: arrow.run_trigger_category.map(str::to_string),
            project_id: arrow.project_id.map(str::to_string),
        }
    }
}

impl From<&ArrowAttributes<'_>> for InvocationMetrics {
    fn from(arrow: &ArrowAttributes) -> Self {
        InvocationMetrics {
            total_errors: arrow.total_errors,
            total_warnings: arrow.total_warnings,
            autofix_suggestions: arrow.autofix_suggestions,
        }
    }
}

impl TryFrom<ArrowAttributes<'_>> for TelemetryAttributes {
    type Error = String;

    fn try_from(arrow: ArrowAttributes) -> Result<Self, Self::Error> {
        match arrow.event_type {
            TelemetryAttributesType::Process => {
                Ok(TelemetryAttributes::Process(ProcessInfo::try_from(&arrow)?))
            }
            TelemetryAttributesType::Invocation => {
                Ok(TelemetryAttributes::Invocation(Box::new(InvocationInfo {
                    invocation_id: arrow
                        .invocation_id
                        .ok_or("Missing invocation_id for Invocation attributes")?
                        .to_string(),
                    raw_command: arrow
                        .raw_command
                        .ok_or("Missing raw_command for Invocation attributes")?
                        .to_string(),
                    eval_args: InvocationEvalArgs::try_from(&arrow)?,
                    process_info: ProcessInfo::try_from(&arrow)?,
                    cloud_args: InvocationCloudAttributes::from(&arrow),
                    metrics: InvocationMetrics::from(&arrow),
                })))
            }
            TelemetryAttributesType::Update => Ok(TelemetryAttributes::Update(UpdateInfo {
                version: arrow.update_version.map(str::to_string),
                package: arrow.update_package.map(str::to_string),
                exe_path: arrow.exe_path.map(str::to_string),
            })),
            TelemetryAttributesType::Phase => {
                let shared = SharedPhaseInfo {
                    invocation_id: arrow
                        .invocation_id
                        .ok_or("Missing invocation_id for Phase attributes")?
                        .to_string(),
                };
                let phase = arrow.phase.ok_or("Missing phase for Phase attributes")?;
                let phase_info = match phase {
                    BuildPhase::Loading => BuildPhaseInfo::Loading { shared },
                    BuildPhase::DependencyLoading => BuildPhaseInfo::DependencyLoading { shared },
                    BuildPhase::Parsing => BuildPhaseInfo::Parsing { shared },
                    BuildPhase::Scheduling => BuildPhaseInfo::Scheduling { shared },
                    BuildPhase::FreshnessAnalysis => BuildPhaseInfo::FreshnessAnalysis { shared },
                    BuildPhase::Lineage => BuildPhaseInfo::Lineage { shared },
                    BuildPhase::Analyzing => BuildPhaseInfo::Analyzing {
                        shared,
                        node_count: arrow.node_count.unwrap_or(0),
                    },
                    BuildPhase::Hydrating => BuildPhaseInfo::Hydrating {
                        shared,
                        node_count: arrow.node_count.unwrap_or(0),
                    },
                    BuildPhase::Compiling => BuildPhaseInfo::Compiling {
                        shared,
                        node_count: arrow.node_count.unwrap_or(0),
                    },
                    BuildPhase::Executing => BuildPhaseInfo::Executing {
                        shared,
                        node_count: arrow.node_count.unwrap_or(0),
                    },
                };
                Ok(TelemetryAttributes::Phase(phase_info))
            }
            TelemetryAttributesType::Node => {
                let node_id = NodeIdentifier {
                    unique_id: arrow
                        .unique_id
                        .ok_or("Missing unique_id for Node attributes")?
                        .to_string(),
                    fqn: arrow
                        .fqn
                        .ok_or("Missing fqn for Node attributes")?
                        .to_string(),
                };
                let phase = arrow.phase.ok_or("Missing phase for Node attributes")?;
                Ok(TelemetryAttributes::Node(NodeInfo {
                    node_id,
                    phase,
                    status: arrow.status,
                    num_rows: arrow.num_rows,
                }))
            }
            TelemetryAttributesType::DevInternal => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::DevInternal(DevInternalInfo {
                    name: arrow
                        .dev_name
                        .ok_or("Missing dev_name for DevInternal attributes")?
                        .to_string(),
                    location,
                    extra: None, // Arrow format doesn't store extra debug info
                }))
            }
            TelemetryAttributesType::Unknown => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::Unknown(UnknownInfo {
                    name: arrow
                        .dev_name
                        .ok_or("Missing dev_name for Unknown attributes")?
                        .to_string(),
                    location,
                }))
            }
            TelemetryAttributesType::Onboarding => {
                Ok(TelemetryAttributes::Onboarding(crate::OnboardingInfo {
                    step: arrow
                        .onboarding_step
                        .ok_or("Missing onboarding_step for Onboarding attributes")?
                        .to_string(),
                    invocation_id: arrow
                        .onboarding_invocation_id
                        .ok_or("Missing onboarding_invocation_id for Onboarding attributes")?
                        .to_string(),
                }))
            }
            TelemetryAttributesType::Log => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::Log(LogEventInfo {
                    code: arrow.code,
                    dbt_core_code: arrow.dbt_core_code.map(str::to_string),
                    original_severity_number: SeverityNumber::from_repr(
                        arrow.original_severity_number.unwrap_or(1),
                    )
                    .unwrap_or_default(),
                    original_severity_text: arrow
                        .original_severity_text
                        .unwrap_or("INFO")
                        .to_string(),
                    location,
                }))
            }
            TelemetryAttributesType::LegacyLog => {
                let location = arrow_to_location(&arrow);
                Ok(TelemetryAttributes::LegacyLog(LegacyLogEventInfo {
                    original_severity_number: SeverityNumber::from_repr(
                        arrow.original_severity_number.unwrap_or(1),
                    )
                    .unwrap_or_default(),
                    original_severity_text: arrow
                        .original_severity_text
                        .unwrap_or("INFO")
                        .to_string(),
                    location,
                }))
            }
            TelemetryAttributesType::WriteArtifact => {
                Ok(TelemetryAttributes::WriteArtifact(WriteArtifactInfo {
                    relative_path: arrow.relative_path.map(str::to_string),
                    duration_ms: arrow.duration_ms,
                }))
            }
        }
    }
}

/// Creates an Arrow schema for telemetry records.
///
/// This generates the Arrow schema definition that can be used to serialize
/// telemetry records to Parquet or other Arrow-compatible formats.
///
/// Timestamp fields are not using Timestamp(NANOSECOND) type due to serde_arrow
/// limitations. However, serialization function casts them to Timestamp(NANOSECOND)
/// after initial serialization to u64.
///
/// # Returns
///
/// Returns a vector of Arrow field references that define the schema structure,
/// or an error if schema generation fails.
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::create_arrow_schema;
///
/// let schema = create_arrow_schema().expect("Failed to create schema");
/// // Use schema for serialization...
/// ```
pub fn create_arrow_schema() -> Result<(Vec<FieldRef>, Vec<FieldRef>), serde_arrow::Error> {
    let tracing_options = TracingOptions::default().enums_without_data_as_strings(true);

    let serialisable_schema = Vec::<FieldRef>::from_type::<ArrowTelemetryRecord>(tracing_options)?;

    // Convert timestamp columns from u64 to Timestamp
    let schema_with_timestamps = serialisable_schema
        .iter()
        .map(|f| {
            if f.name() == "start_time_unix_nano"
                || f.name() == "end_time_unix_nano"
                || f.name() == "time_unix_nano"
            {
                Arc::new(Field::new(
                    f.name(),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ))
            } else {
                f.clone()
            }
        })
        .collect();

    Ok((serialisable_schema, schema_with_timestamps))
}

/// Serializes telemetry records to an Arrow RecordBatch.
///
/// Converts a slice of telemetry records into an Arrow RecordBatch that can be
/// written to Parquet files or other Arrow-compatible storage formats.
///
/// # Arguments
///
/// * `records` - Slice of telemetry records to serialize
/// * `serialisable_schema` - Arrow schema definition without timestamps (created with [`create_arrow_schema`])
/// * `schema_with_timestamps` - Arrow schema definition with timestamps converted to Timestamp(NANOSECOND)
///
/// # Returns
///
/// Returns an Arrow RecordBatch containing the serialized records, or an error
/// if serialization fails.
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::{create_arrow_schema, serialize_to_arrow};
/// use dbt_telemetry::TelemetryRecord;
///
/// let records: Vec<TelemetryRecord> = vec![/* ... */];
/// let schema = create_arrow_schema().expect("Failed to create schema");
/// let batch = serialize_to_arrow(&records, &schema).expect("Failed to serialize");
/// ```
pub fn serialize_to_arrow(
    records: &[TelemetryRecord],
    serialisable_schema: &[FieldRef],
    schema_with_timestamps: &[FieldRef],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let arrow_records: Vec<ArrowTelemetryRecord> =
        records.iter().map(ArrowTelemetryRecord::from).collect();

    // Serialize with the temporary schema
    let batch = serde_arrow::to_record_batch(serialisable_schema, &arrow_records)?;

    let mut columns = batch.columns().to_vec();

    for (i, field) in schema_with_timestamps.iter().enumerate() {
        if let DataType::Timestamp(TimeUnit::Nanosecond, None) = field.data_type() {
            if let Some(column) = columns.get(i) {
                columns[i] = cast_with_options(
                    column,
                    &DataType::Timestamp(TimeUnit::Nanosecond, None),
                    &CastOptions {
                        safe: false,
                        format_options: FormatOptions::new().with_display_error(false),
                    },
                )?
            }
        }
    }

    Ok(RecordBatch::try_new(
        Schema::new(schema_with_timestamps).into(),
        columns,
    )?)
}

/// Deserializes telemetry records from an Arrow RecordBatch.
///
/// Converts an Arrow RecordBatch (typically read from a Parquet file) back into
/// telemetry records. This function validates the data during deserialization
/// and will return errors for malformed or missing required fields.
///
/// # Arguments
///
/// * `batch` - Arrow RecordBatch to deserialize from
/// * `serialisable_schema` - Arrow schema definition without timestamps (created with [`create_arrow_schema`])
///
/// # Returns
///
/// Returns a vector of telemetry records, or an error if deserialization fails
/// due to invalid data format or missing required fields.
///
/// # Errors
///
/// This function will return an error if:
/// - The RecordBatch format is incompatible
/// - Required fields are missing (e.g., span_id for span records)
/// - Field values are invalid (e.g., malformed trace_id hex strings)
/// - Enum values are out of range (e.g., invalid severity numbers)
///
/// # Examples
///
/// ```rust
/// use dbt_telemetry::serialize::arrow::deserialize_from_arrow;
/// use arrow::record_batch::RecordBatch;
///
/// let batch: RecordBatch = /* read from file */;
/// let records = deserialize_from_arrow(&batch).expect("Failed to deserialize");
/// ```
pub fn deserialize_from_arrow(
    batch: &RecordBatch,
    serialisable_schema: &[FieldRef],
) -> Result<Vec<TelemetryRecord>, Box<dyn std::error::Error>> {
    // Convert timestamp columns back to u64 for serde_arrow deserialization
    let mut columns = batch.columns().to_vec();

    for col in columns.iter_mut() {
        if let DataType::Timestamp(TimeUnit::Nanosecond, None) = col.data_type() {
            *col = cast_with_options(
                col,
                &DataType::UInt64,
                &CastOptions {
                    safe: false,
                    format_options: FormatOptions::new().with_display_error(false),
                },
            )?;
        }
    }

    // Create temporary batch with u64 timestamp fields
    let temp_batch = RecordBatch::try_new(Schema::new(serialisable_schema).into(), columns)?;

    let arrow_records: Vec<ArrowTelemetryRecord> = serde_arrow::from_record_batch(&temp_batch)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    arrow_records
        .into_iter()
        .map(|record| {
            TelemetryRecord::try_from(record).map_err(|e| {
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                    as Box<dyn std::error::Error>
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use fake::rand::SeedableRng;
    use fake::rand::rngs::StdRng;
    use fake::{Fake, Faker};
    use strum::IntoEnumIterator;

    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Generate pseudo-random but deterministic values for testing
    fn hash_seed(seed: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        hasher.finish()
    }

    fn create_fake_attributes(
        seed: &str,
        event_type: TelemetryAttributesType,
        phase: Option<BuildPhase>,
    ) -> TelemetryAttributes {
        let hashed_seed = hash_seed(seed);
        let mut rng = StdRng::seed_from_u64(hashed_seed);
        match event_type {
            TelemetryAttributesType::Process => {
                TelemetryAttributes::Process(Faker.fake_with_rng(&mut rng))
            }
            TelemetryAttributesType::Invocation => {
                TelemetryAttributes::Invocation(Box::new(Faker.fake_with_rng(&mut rng)))
            }
            TelemetryAttributesType::Update => {
                TelemetryAttributes::Update(Faker.fake_with_rng(&mut rng))
            }
            TelemetryAttributesType::Onboarding => {
                TelemetryAttributes::Onboarding(Faker.fake_with_rng(&mut rng))
            }
            TelemetryAttributesType::Phase => {
                let Some(phase) = phase else {
                    return TelemetryAttributes::Phase(Faker.fake_with_rng(&mut rng));
                };

                let shared = Faker.fake_with_rng(&mut rng);
                let phase_info = match phase {
                    BuildPhase::Loading => BuildPhaseInfo::Loading { shared },
                    BuildPhase::DependencyLoading => BuildPhaseInfo::DependencyLoading { shared },
                    BuildPhase::Parsing => BuildPhaseInfo::Parsing { shared },
                    BuildPhase::Scheduling => BuildPhaseInfo::Scheduling { shared },
                    BuildPhase::FreshnessAnalysis => BuildPhaseInfo::FreshnessAnalysis { shared },
                    BuildPhase::Lineage => BuildPhaseInfo::Lineage { shared },
                    BuildPhase::Analyzing => BuildPhaseInfo::Analyzing {
                        shared,
                        node_count: Faker.fake_with_rng(&mut rng),
                    },
                    BuildPhase::Hydrating => BuildPhaseInfo::Hydrating {
                        shared,
                        node_count: Faker.fake_with_rng(&mut rng),
                    },
                    BuildPhase::Compiling => BuildPhaseInfo::Compiling {
                        shared,
                        node_count: Faker.fake_with_rng(&mut rng),
                    },
                    BuildPhase::Executing => BuildPhaseInfo::Executing {
                        shared,
                        node_count: Faker.fake_with_rng(&mut rng),
                    },
                };
                TelemetryAttributes::Phase(phase_info)
            }
            TelemetryAttributesType::Node => {
                TelemetryAttributes::Node(Faker.fake_with_rng(&mut rng))
            }
            TelemetryAttributesType::DevInternal => {
                TelemetryAttributes::DevInternal(Faker.fake_with_rng(&mut rng))
            }
            TelemetryAttributesType::Unknown => {
                TelemetryAttributes::Unknown(Faker.fake_with_rng(&mut rng))
            }
            TelemetryAttributesType::Log => TelemetryAttributes::Log(Faker.fake_with_rng(&mut rng)),
            TelemetryAttributesType::LegacyLog => {
                TelemetryAttributes::LegacyLog(Faker.fake_with_rng(&mut rng))
            }
            TelemetryAttributesType::WriteArtifact => {
                TelemetryAttributes::WriteArtifact(Faker.fake_with_rng(&mut rng))
            }
        }
    }

    fn create_all_fake_attributes(seed: &str) -> Vec<TelemetryAttributes> {
        let mut attributes = Vec::new();
        for event_type in TelemetryAttributesType::iter() {
            if event_type == TelemetryAttributesType::Phase {
                for phase in BuildPhase::iter() {
                    attributes.push(create_fake_attributes(seed, event_type, Some(phase)));
                }
            } else {
                attributes.push(create_fake_attributes(seed, event_type, None));
            }
        }
        attributes
    }

    fn create_test_span_start(seed: &str, attributes: TelemetryAttributes) -> TelemetryRecord {
        let hashed_seed = hash_seed(seed);
        let mut rng = StdRng::seed_from_u64(hashed_seed);
        let trace_id = Faker.fake_with_rng(&mut rng);
        let span_id = Faker.fake_with_rng(&mut rng);
        let parent_span_id = Faker.fake_with_rng(&mut rng);
        let start_time = Faker.fake_with_rng(&mut rng);

        TelemetryRecord::SpanStart(SpanStartInfo {
            trace_id,
            span_id,
            parent_span_id: Some(parent_span_id),
            span_name: attributes.to_string(),
            start_time_unix_nano: SystemTime::UNIX_EPOCH
                + std::time::Duration::from_nanos(start_time),
            attributes,
            severity_number: Faker.fake_with_rng(&mut rng),
            severity_text: ["TRACE", "DEBUG", "INFO", "WARN"][(hashed_seed % 4) as usize]
                .to_string(),
        })
    }

    fn create_test_span_end(seed: &str, span_start: &TelemetryRecord) -> TelemetryRecord {
        let TelemetryRecord::SpanStart(span_start_info) = span_start else {
            panic!("Expected SpanStart record");
        };

        let hashed_seed = hash_seed(seed);
        let mut rng = StdRng::seed_from_u64(hashed_seed);
        let elapsed = Faker.fake_with_rng(&mut rng);

        TelemetryRecord::SpanEnd(SpanEndInfo {
            trace_id: span_start_info.trace_id,
            span_id: span_start_info.span_id,
            parent_span_id: span_start_info.parent_span_id,
            span_name: span_start_info.span_name.clone(),
            start_time_unix_nano: span_start_info.start_time_unix_nano,
            end_time_unix_nano: span_start_info.start_time_unix_nano
                + std::time::Duration::from_nanos(elapsed),
            attributes: span_start_info.attributes.clone(),
            status: Some(SpanStatus {
                code: [StatusCode::Unset, StatusCode::Ok, StatusCode::Error]
                    [(hashed_seed % 3) as usize],
                message: Some(format!("status_{}", hashed_seed % 100)),
            }),
            severity_number: Faker.fake_with_rng(&mut rng),
            severity_text: ["TRACE", "DEBUG", "INFO", "WARN"][(hashed_seed % 4) as usize]
                .to_string(),
        })
    }

    fn create_test_log_record(seed: &str, attributes: TelemetryAttributes) -> TelemetryRecord {
        let hashed_seed = hash_seed(seed);
        let mut rng = StdRng::seed_from_u64(hashed_seed);
        let trace_id = Faker.fake_with_rng(&mut rng);
        let span_id = Faker.fake_with_rng(&mut rng);
        let log_time = Faker.fake_with_rng(&mut rng);

        TelemetryRecord::LogRecord(LogRecordInfo {
            time_unix_nano: SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(log_time),
            trace_id,
            span_id: Some(span_id),
            span_name: Some(attributes.to_string()),
            severity_number: Faker.fake_with_rng(&mut rng),
            severity_text: ["ERROR", "WARN", "INFO", "DEBUG"][(hashed_seed % 4) as usize]
                .to_string(),
            body: format!("Log message {}", hashed_seed % 10000),
            attributes,
        })
    }

    #[test]
    fn test_arrow_roundtrip_all_record_types() {
        // Create records of each record & event (aka attribute) type with a pseudo-random seed
        let mut original_records = vec![];
        create_all_fake_attributes("test_seed")
            .iter()
            .for_each(|attributes| {
                match attributes.record_type() {
                    // Span types
                    TelemetryRecordType::SpanEnd => {
                        let span_start = create_test_span_start("test_seed", attributes.clone());
                        // Create a matching span end for the start
                        let span_end = create_test_span_end("test_seed", &span_start);
                        original_records.push(span_start);
                        original_records.push(span_end);
                    }
                    TelemetryRecordType::SpanStart => {
                        panic!("SpanStart should not be returned here")
                    }
                    TelemetryRecordType::LogRecord => {
                        // Create a log record
                        let log_record = create_test_log_record("test_seed", attributes.clone());
                        original_records.push(log_record);
                    }
                }
            });

        let (serialisable_schema, schema_with_timestamps) = create_arrow_schema().unwrap();
        let batch = serialize_to_arrow(
            &original_records,
            &serialisable_schema,
            &schema_with_timestamps,
        )
        .unwrap();
        let deserialized = deserialize_from_arrow(&batch, &serialisable_schema).unwrap();

        // Use PartialEq to compare entire records
        for (original, deserialized) in original_records.iter().zip(deserialized.iter()) {
            assert_eq!(
                original, deserialized,
                "Record roundtrip failed for: {original:?}"
            );
        }
    }

    #[test]
    fn test_schema_creation() {
        let (serialisable_schema, schema_with_timestamps) = create_arrow_schema().unwrap();
        assert!(!serialisable_schema.is_empty());
        assert!(!schema_with_timestamps.is_empty());

        // Assert all expected top-level keys present (they are stable)
        [
            "record_type",
            "trace_id",
            "span_id",
            "span_name",
            "parent_span_id",
            "start_time_unix_nano",
            "end_time_unix_nano",
            "time_unix_nano",
            "severity_number",
            "severity_text",
            "body",
            "status_code",
            "status_message",
            "event_type",
            "attributes",
        ]
        .iter()
        .for_each(|&field| {
            let serializable_schema_field = serialisable_schema
                .iter()
                .find(|f| f.name() == field)
                .expect("Missing field in `serialisable_schema`");
            let schema_with_timestamps_field = schema_with_timestamps
                .iter()
                .find(|f| f.name() == field)
                .expect("Missing field in `schema_with_timestamps`");

            if field == "start_time_unix_nano"
                || field == "end_time_unix_nano"
                || field == "time_unix_nano"
            {
                assert_eq!(
                    *schema_with_timestamps_field.data_type(),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    "Field {field} should be Timestamp(NANOSECOND)"
                );
                assert_eq!(
                    *serializable_schema_field.data_type(),
                    DataType::UInt64,
                    "Field {field} should be UInt64 in `serialisable_schema`"
                );
            } else {
                assert_eq!(
                    serializable_schema_field.data_type(),
                    schema_with_timestamps_field.data_type(),
                    "Field {field} should have the same type in both schemas"
                );
            }
        });
    }
}
