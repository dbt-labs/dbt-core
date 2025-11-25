use std::{collections::HashMap, time::SystemTime};

use chrono::Utc;
use dbt_error::ErrorCode;
use dbt_telemetry::{
    ArtifactType, ArtifactWritten, ExecutionPhase, Invocation, ListItemOutput, LogMessage,
    LogRecordInfo, NodeEvaluated, NodeEvent, NodeOutcome, NodeProcessed, NodeSkipReason, NodeType,
    QueryExecuted, SeverityNumber, ShowDataOutput, SpanEndInfo, SpanStartInfo,
    TelemetryOutputFlags, TestOutcome, UserLogMessage, get_test_outcome,
};
use serde_json::json;
use tracing::level_filters::LevelFilter;

use super::super::{
    background_writer::BackgroundWriter,
    data_provider::DataProvider,
    formatters::{
        duration::format_timestamp_utc_zulu,
        invocation::format_invocation_summary,
        log_message::format_log_message,
        node::{
            format_node_evaluated_start, format_node_outcome_as_status, format_node_processed_end,
            format_node_processed_start, get_num_failures,
        },
        query_log::format_query_log,
    },
    layer::{ConsumerLayer, TelemetryConsumer},
    metrics::{InvocationMetricKey, MetricKey},
    shared_writer::SharedWriter,
    shutdown::TelemetryShutdownItem,
};

use proto_rust::v1::public::fields::core_types::CoreEventInfo;

/// Build a JSON compatibility layer. This will write directly to the writer.
/// If you want to write to slow IO sink, prefer `build_json_compat_layer_with_background_writer`
pub fn build_json_compat_layer<W: SharedWriter + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
    invocation_id: uuid::Uuid,
) -> ConsumerLayer {
    Box::new(JsonCompatLayer::new(writer, invocation_id).with_filter(max_log_verbosity))
}

/// Build a JSON compatibility layer with a background writer. This is preferred for writing to
/// slow IO sinks like files.
pub fn build_json_compat_layer_with_background_writer<W: std::io::Write + Send + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
    invocation_id: uuid::Uuid,
) -> (ConsumerLayer, TelemetryShutdownItem) {
    let (writer, handle) = BackgroundWriter::new(writer);

    (
        build_json_compat_layer(writer, max_log_verbosity, invocation_id),
        Box::new(handle),
    )
}

/// Build a NodeInfo JSON object from common node fields
fn build_node_info_json(
    node: NodeEvent,
    node_status: &str,
    start_time_unix_nano: SystemTime,
    end_time_unix_nano: Option<SystemTime>,
) -> serde_json::Value {
    let (
        unique_id,
        name,
        relative_path,
        node_type,
        materialization,
        node_checksum,
        database,
        schema,
        identifier,
    ) = match node {
        NodeEvent::Evaluated(n) => (
            &n.unique_id,
            &n.name,
            &n.relative_path,
            n.node_type(),
            if n.materialization.is_some() {
                let mat = n.materialization();
                if mat == dbt_telemetry::NodeMaterialization::Custom {
                    n.custom_materialization.as_deref()
                } else {
                    Some(mat.as_static_ref())
                }
            } else {
                None
            },
            &n.node_checksum,
            n.database.as_deref(),
            n.schema.as_deref(),
            n.identifier.as_deref(),
        ),
        NodeEvent::Processed(n) => (
            &n.unique_id,
            &n.name,
            &n.relative_path,
            n.node_type(),
            if n.materialization.is_some() {
                let mat = n.materialization();
                if mat == dbt_telemetry::NodeMaterialization::Custom {
                    n.custom_materialization.as_deref()
                } else {
                    Some(mat.as_static_ref())
                }
            } else {
                None
            },
            &n.node_checksum,
            n.database.as_deref(),
            n.schema.as_deref(),
            n.identifier.as_deref(),
        ),
    };

    // Build relation_name from database, schema, and identifier (if available)
    let relation_name = match (database, schema, identifier) {
        (Some(db), Some(sch), Some(ident)) => format!("{}.{}.{}", db, sch, ident),
        _ => String::new(),
    };

    // Format timestamps as ISO 8601
    let started_at_str = format_timestamp_utc_zulu(start_time_unix_nano);
    let finished_at_str = end_time_unix_nano
        .map(format_timestamp_utc_zulu)
        .unwrap_or_default();

    json!({
        "node_path": relative_path,
        "node_name": name,
        "unique_id": unique_id,
        "resource_type": node_type.as_static_ref(),
        "materialized": materialization.unwrap_or_default(),
        "node_status": node_status,
        "node_started_at": started_at_str,
        "node_finished_at": finished_at_str,
        "node_relation": {
            "database": database.unwrap_or_default(),
            "schema": schema.unwrap_or_default(),
            "alias": identifier.unwrap_or_default(),
            "relation_name": relation_name
        },
        "node_checksum": node_checksum
    })
}

/// A telemetry consumer that writes out events in a format compatible with dbt-core structured json logs.
///
/// It is only writing a small subset of event types and not 100% matching dbt-core schema and left only
/// for backward compatibility with what has been implemented in fusion by May 2025 launch.
struct JsonCompatLayer {
    writer: Box<dyn SharedWriter>,
    invocation_id: uuid::Uuid,
    filter_flag: TelemetryOutputFlags,
}

impl JsonCompatLayer {
    pub fn new<W: SharedWriter + 'static>(writer: W, invocation_id: uuid::Uuid) -> Self {
        let is_tty = writer.is_terminal();

        Self {
            writer: Box::new(writer),
            invocation_id,
            filter_flag: if is_tty {
                TelemetryOutputFlags::OUTPUT_CONSOLE
            } else {
                TelemetryOutputFlags::OUTPUT_LOG_FILE
            },
        }
    }

    fn build_core_event_info(
        &self,
        code: Option<&str>,
        name: Option<&str>,
        level: &str,
        msg: String,
    ) -> CoreEventInfo {
        CoreEventInfo {
            category: "".to_string(),
            code: code.unwrap_or("").to_string(),
            invocation_id: self.invocation_id.to_string(),
            name: name.unwrap_or("Generic").to_string(),
            pid: std::process::id() as i32,
            thread: std::thread::current().name().unwrap_or("main").to_string(),
            // drop the timezone offset and format as microseconds to conform to python logging timestamp parsing
            ts: Some(Utc::now().into()),
            msg,
            level: level.to_lowercase(),
            extra: HashMap::new(),
        }
    }

    fn emit_main_report_version(&self) {
        let version = env!("CARGO_PKG_VERSION");
        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("A001"),
            Some("MainReportVersion"),
            "info",
            format!("Running with dbt-fusion={}", version),
        ))
        .expect("Failed to serialize core event info to JSON");

        let value = json!({
            "info": info_json,
            "data": {
                "log_version": 3,
                "version": format!("={}", version),
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    fn emit_main_report_args(&self, invocation: &Invocation) {
        let mut args = serde_json::Map::new();

        // Add invocation_command from raw_command
        args.insert(
            "invocation_command".to_string(),
            json!(invocation.raw_command),
        );

        // Add fields from eval_args if present
        let msg = if let Some(eval_args) = &invocation.eval_args {
            // Add debug
            if let Some(debug) = eval_args.debug {
                args.insert("debug".to_string(), json!(debug.to_string()));
            }

            // Add indirect_selection
            if let Some(indirect_selection) = &eval_args.indirect_selection {
                args.insert("indirect_selection".to_string(), json!(indirect_selection));
            }

            // Add log_format
            if let Some(log_format) = &eval_args.log_format {
                args.insert("log_format".to_string(), json!(log_format));
            }

            // Add log_path
            if let Some(log_path) = &eval_args.log_path {
                args.insert("log_path".to_string(), json!(log_path));
            }

            // Add profiles_dir
            if let Some(profiles_dir) = &eval_args.profiles_dir {
                args.insert("profiles_dir".to_string(), json!(profiles_dir));
            }

            // Add quiet
            if let Some(quiet) = eval_args.quiet {
                args.insert("quiet".to_string(), json!(quiet.to_string()));
            }

            // Add target_path
            if let Some(target_path) = &eval_args.target_path {
                args.insert("target_path".to_string(), json!(target_path));
            }

            // Add write_json
            if let Some(write_json) = eval_args.write_json {
                args.insert("write_json".to_string(), json!(write_json.to_string()));
            }

            format!(
                "running dbt-fusion with argumets {}",
                serde_json::to_string(eval_args).unwrap_or_default()
            )
        } else {
            "running dbt-fusion".to_string()
        };

        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("A002"),
            Some("MainReportArgs"),
            "debug",
            msg,
        ))
        .expect("Failed to serialize core event info to JSON");

        let value = json!({
            "info": info_json,
            "data": {
                "args": args,
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    fn emit_query_executed(&self, query_data: &QueryExecuted, span: &SpanEndInfo) {
        // Format the query log using the shared formatter
        let formatted_query = format_query_log(
            query_data,
            span.start_time_unix_nano,
            span.end_time_unix_nano,
        );

        let info_json = serde_json::to_value(self.build_core_event_info(
            query_data.dbt_core_event_code.as_str().into(),
            Some("SQLQuery"),
            &span.severity_text,
            formatted_query,
        ))
        .expect("Failed to serialize core event info to JSON");

        let value = if let Some(unique_id) = query_data.unique_id.as_deref() {
            json!({
                "info": info_json,
                "data": {
                    "sql": query_data.sql,
                    "node_info": {
                        "unique_id": unique_id
                    }
                }
            })
        } else {
            json!({
                "info": info_json,
                "data": {
                    "sql": query_data.sql,
                }
            })
        };

        self.writer.writeln(value.to_string().as_str());
    }

    fn emit_invocation_completed(
        &self,
        invocation: &Invocation,
        span: &SpanEndInfo,
        data_provider: &mut DataProvider<'_>,
    ) {
        let formatted = format_invocation_summary(span, invocation, data_provider, false, None);

        if let Some(line) = formatted.autofix_line() {
            let info_json = serde_json::to_value(self.build_core_event_info(
                None,
                None,
                &span.severity_text,
                line.to_string(),
            ))
            .expect("Failed to serialize core event info to JSON");

            let value = json!({
                "info": info_json,
                "data": {}
            })
            .to_string();

            self.writer.writeln(value.as_str());
        }

        let message = formatted
            .summary_lines()
            .map(|summary_lines| summary_lines.join("\n"))
            .unwrap_or_default();

        let elapsed_secs = span
            .end_time_unix_nano
            .duration_since(span.start_time_unix_nano)
            .unwrap_or_default()
            .as_secs_f32();

        let completed_at_str = format_timestamp_utc_zulu(span.end_time_unix_nano);
        let success = data_provider.get_metric(MetricKey::InvocationMetric(
            InvocationMetricKey::TotalErrors,
        )) == 0;

        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("Q039"),
            Some("CommandCompleted"),
            &span.severity_text,
            message,
        ))
        .expect("Failed to serialize core event info to JSON");

        // We only use structured type for info, because proto defined data
        // types are not matching what fusion emitted for compatibility by the
        // original launch date.
        let value = json!({
            "info": info_json,
            "data": {
                "completed_at": completed_at_str,
                "elapsed": elapsed_secs,
                "success": success
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    fn emit_log_message(&self, log_msg: &LogMessage, log_record: &LogRecordInfo) {
        // Format the message
        let formatted_message = format_log_message(
            log_msg
                .code
                .and_then(|c| u16::try_from(c).ok())
                .and_then(|c| ErrorCode::try_from(c).ok()),
            // Unfortunately, we do not currently enforce log body to not contain ANSI codes,
            // so we need to make sure to strip them
            console::strip_ansi_codes(log_record.body.as_str()),
            log_record.severity_number,
            false,
            true,
        );

        let info_json = serde_json::to_value(self.build_core_event_info(
            None,
            None,
            &log_record.severity_text,
            formatted_message,
        ))
        .expect("Failed to serialize core event info to JSON");

        let mut data_obj = json!({});

        if let Some(unique_id) = log_msg.unique_id.as_deref() {
            data_obj.as_object_mut().unwrap().insert(
                "node_info".to_string(),
                json!({
                    "unique_id": unique_id
                }),
            );
        }

        let value = json!({
            "info": info_json,
            "data": data_obj
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }
    /// Handle UserLogMessage events (from Jinja print() and log() functions)
    /// These map to dbt-core's JinjaLogInfo (I062), JinjaDebugInfo (I063) and PrintEvent ("Z052") events
    fn emit_user_log_message(&self, user_log_msg: &UserLogMessage, log_record: &LogRecordInfo) {
        let (event_name, event_code) = match (user_log_msg.is_print, log_record.severity_number) {
            (true, _) => {
                // print() maps to PrintEvent (Z052)
                ("PrintEvent", "Z052")
            }
            (false, SeverityNumber::Info) => {
                // log(..., info=true) maps to JinjaLogInfo (I062)
                ("JinjaLogInfo", "I062")
            }
            (false, SeverityNumber::Debug) => {
                // log(..., info=false) maps to JinjaDebugInfo (I063)
                ("JinjaDebugInfo", "I063")
            }
            _ => {
                // Unknown combination, skip
                return;
            }
        };

        let info_json = serde_json::to_value(self.build_core_event_info(
            Some(event_code),
            Some(event_name),
            &log_record.severity_text,
            log_record.body.clone(),
        ))
        .expect("Failed to serialize core event info to JSON");

        let mut data_obj = json!({
            "msg": log_record.body, // Yes, duplicate the message here as well
        });

        if let Some(unique_id) = user_log_msg.unique_id.as_deref() {
            data_obj.as_object_mut().unwrap().insert(
                "node_info".to_string(),
                json!({
                    "unique_id": unique_id
                }),
            );
        }

        let value = json!({
            "info": info_json,
            "data": data_obj
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    /// Handle ListItemOutput events (from dbt list command)
    /// These map to PrintEvent (Z052) for backward compatibility
    fn emit_list_item_output(&self, list_item: &ListItemOutput, log_record: &LogRecordInfo) {
        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("Z052"),
            Some("PrintEvent"),
            &log_record.severity_text,
            list_item.content.clone(),
        ))
        .expect("Failed to serialize core event info to JSON");

        let value = json!({
            "info": info_json,
            "data": {
                "msg": &list_item.content, // Use content from event for backward compatibility
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    /// Handle ShowDataOutput events (from dbt show command)
    /// These map to ShowNode event (Q041) for backward compatibility
    fn emit_show_data_output(&self, inline_data: &ShowDataOutput, log_record: &LogRecordInfo) {
        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("Q041"),
            Some("ShowNode"),
            &log_record.severity_text,
            inline_data.content.clone(),
        ))
        .expect("Failed to serialize core event info to JSON");

        let data_obj = json!({
            "node_name": &inline_data.node_name,
            "preview": &inline_data.content,
            "is_inline": inline_data.is_inline,
            "output_format": inline_data.output_format().as_str(),
            "unique_id": inline_data.unique_id.as_deref().unwrap_or("sql_operation.inline_query"),
            "columns": &inline_data.columns,
        });

        let value = json!({
            "info": info_json,
            "data": data_obj,
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    /// Handle ArtifactWritten events
    /// Maps to ArtifactWritten (P001) for backward compatibility with dbt-core
    fn emit_artifact_written(&self, artifact_data: &ArtifactWritten, span: &SpanEndInfo) {
        // Convert artifact_type i32 to enum and then to string
        let artifact_type_str = ArtifactType::try_from(artifact_data.artifact_type)
            .map(|at| match at {
                ArtifactType::Manifest => "WritableManifest".to_string(),
                _ => at.as_str_name().to_string(),
            })
            .unwrap_or_else(|_| "UNKNOWN".to_string());

        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("P001"),
            Some("ArtifactWritten"),
            &span.severity_text,
            format!("Artifact written to {}", artifact_data.relative_path),
        ))
        .expect("Failed to serialize core event info to JSON");

        let value = json!({
            "info": info_json,
            "data": {
                "artifact_path": &artifact_data.relative_path,
                "artifact_type": artifact_type_str,
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    /// Handle NodeEvaluated span start events
    /// Generates NodeCompiling (Q030) for Render phase or NodeExecuting (Q031) for Run phase
    fn emit_node_evaluated_start(&self, node: &NodeEvaluated, span: &SpanStartInfo) {
        // Only emit for Render (NodeCompiling) or Run (NodeExecuting) phases
        let (event_name, event_code, node_status) = match node.phase() {
            ExecutionPhase::Render => ("NodeCompiling", "Q030", "compiling"),
            ExecutionPhase::Run => ("NodeExecuting", "Q031", "executing"),
            _ => return, // Skip Analyze and other phases
        };

        let msg = format_node_evaluated_start(node, false);

        let node_info =
            build_node_info_json(node.into(), node_status, span.start_time_unix_nano, None);

        let info_json = serde_json::to_value(self.build_core_event_info(
            Some(event_code),
            Some(event_name),
            "debug",
            msg,
        ))
        .expect("Failed to serialize core event info to JSON");

        let value = json!({
            "info": info_json,
            "data": {
                "node_info": node_info
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    /// Handle NodeProcessed span start events
    /// Generates NodeStart (Q024) and LogStartLine (Q011)
    fn emit_node_processed_start(&self, node: &NodeProcessed, span: &SpanStartInfo) {
        // Do not emit for non-selected nodes
        if !node.in_selection {
            return;
        }

        let node_info =
            build_node_info_json(node.into(), "started", span.start_time_unix_nano, None);

        // Emit NodeStart (Q024)
        let info_json_start = serde_json::to_value(self.build_core_event_info(
            Some("Q024"),
            Some("NodeStart"),
            "debug",
            format!("Began running node {}", node.unique_id),
        ))
        .expect("Failed to serialize core event info to JSON");

        let value_start = json!({
            "info": info_json_start,
            "data": {
                "node_info": node_info
            }
        })
        .to_string();

        self.writer.writeln(value_start.as_str());

        // Emit LogStartLine (Q011)
        let msg = format_node_processed_start(node, false);

        let info_json_log = serde_json::to_value(self.build_core_event_info(
            Some("Q011"),
            Some("LogStartLine"),
            "info",
            msg,
        ))
        .expect("Failed to serialize core event info to JSON");

        // TODO: we can theoretically add index & total either by exteding NodeProcessed or
        // tracking on TUI only TuiAllProcessingNodesGroup
        let value_log = json!({
            "info": info_json_log,
            "data": {
                "node_info": node_info
            }
        })
        .to_string();

        self.writer.writeln(value_log.as_str());
    }

    /// Handle MarkSkippedChildren event
    /// Emitted when a node fails or is skipped, indicating children will be skipped
    fn emit_mark_skipped_children(
        &self,
        node: &NodeProcessed,
        status: &str,
        run_result: &serde_json::Value,
    ) {
        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("Z033"),
            Some("MarkSkippedChildren"),
            "debug",
            format!("Marking children of {} as skipped", node.unique_id),
        ))
        .expect("Failed to serialize core event info to JSON");

        let value = json!({
            "info": info_json,
            "data": {
                "unique_id": node.unique_id,
                "status": status,
                "run_result": run_result
            }
        })
        .to_string();

        self.writer.writeln(value.as_str());
    }

    /// Handle NodeProcessed span end events
    /// Generates Log[NODE TYPE]Result based on node type & NodeFinished (Q025)
    fn emit_node_processed_end(&self, node: &NodeProcessed, span: &SpanEndInfo) {
        // Do not emit for non-selected nodes
        if !node.in_selection {
            return;
        }

        let node_outcome = node.node_outcome();

        let node_type = node.node_type();
        let node_skip_reason = node.node_skip_reason.map(|_| node.node_skip_reason());

        // Determine `Log[NODE TYPE]Result` event name and code based on node type and skip reason
        let (event_name, event_code) = if node_outcome == NodeOutcome::Skipped
            && matches!(node_skip_reason, Some(NodeSkipReason::NoOp))
        {
            // Special case for no-op result
            ("LogNodeNoOpResult", "Q019")
        } else {
            match node_type {
                NodeType::Test | NodeType::UnitTest => ("LogTestResult", "Q007"),
                NodeType::Model => ("LogModelResult", "Q012"),
                NodeType::Snapshot => ("LogSnapshotResult", "Q015"),
                NodeType::Seed => ("LogSeedResult", "Q016"),
                NodeType::Function => ("LogFunctionResult", "Q047"),
                _ => ("LogNodeResult", "Q008"),
            }
        };

        // Build status string
        let status = format_node_outcome_as_status(
            node_outcome,
            node.node_skip_reason.map(|_| node.node_skip_reason()),
            get_test_outcome(node.into()),
            false,
        );

        let duration = span
            .end_time_unix_nano
            .duration_since(span.start_time_unix_nano)
            .unwrap_or_default();

        let msg = format_node_processed_end(node, duration, false);

        let node_info = build_node_info_json(
            node.into(),
            &status,
            span.start_time_unix_nano,
            Some(span.end_time_unix_nano),
        );

        let info_json = serde_json::to_value(self.build_core_event_info(
            Some(event_code),
            Some(event_name),
            "info",
            msg,
        ))
        .expect("Failed to serialize core event info to JSON");

        // Extract num_failures for tests
        let num_failures = get_num_failures(node.into());

        // Build data object, including num_failures for tests if available
        let mut data = json!({
            "node_info": node_info,
            "status": status,
            "execution_time": duration.as_secs_f32()
        });

        if let Some(num_failures_val) = num_failures {
            data.as_object_mut()
                .unwrap()
                .insert("num_failures".to_string(), json!(num_failures_val));
        }

        let value = json!({
            "info": info_json,
            "data": data
        })
        .to_string();

        self.writer.writeln(value.as_str());

        // NodeFinished (Q025) with a stub for run results & maybe MarkSkippedChildren
        let info_json = serde_json::to_value(self.build_core_event_info(
            Some("Q025"),
            Some("NodeFinished"),
            "debug",
            format!("Finished running node {}", node.unique_id),
        ))
        .expect("Failed to serialize core event info to JSON");

        // Build run_result with num_failures if available
        let mut run_result = json!({
            "status": status,
            "execution_time": duration.as_secs_f32(),
        });

        if let Some(num_failures_val) = num_failures {
            run_result
                .as_object_mut()
                .unwrap()
                .insert("num_failures".to_string(), json!(num_failures_val));
        }

        // Emit MarkSkippedChildren if node outcome would cause children to be skipped
        // This happens when node fails, is skipped, canceled, or for tests with failures
        let should_mark_skipped = match (node_outcome, get_test_outcome(node.into())) {
            (NodeOutcome::Success, None) => false,
            // Tests report as success, we need to check test outcome for failures
            (NodeOutcome::Success, Some(to)) => to == TestOutcome::Failed,
            // Any error outcome causes children to be skipped
            (NodeOutcome::Error, _) => true,
            // Canceled does't mean skipped, just operation was aborted
            (NodeOutcome::Canceled, _) => false,
            // Skipped because of error causes children to be skipped as well
            (NodeOutcome::Skipped, _) => matches!(node_skip_reason, Some(NodeSkipReason::Upstream)),
            // should not happen, treat as not skipping children
            (NodeOutcome::Unspecified, _) => false,
        };

        if should_mark_skipped {
            self.emit_mark_skipped_children(node, &status, &run_result);
        }

        let value = json!({
            "info": info_json,
            "data": {
                "node_info": node_info,
                "run_result": run_result
            }
        })
        .to_string();

        // Now write NodeFinished event
        self.writer.writeln(value.as_str());
    }
}

impl TelemetryConsumer for JsonCompatLayer {
    fn is_span_enabled(&self, span: &SpanStartInfo) -> bool {
        span.attributes.output_flags().contains(self.filter_flag)
    }

    fn is_log_enabled(&self, log_record: &LogRecordInfo) -> bool {
        log_record
            .attributes
            .output_flags()
            .contains(self.filter_flag)
    }

    fn on_span_start(&self, span: &SpanStartInfo, _data_provider: &mut DataProvider<'_>) {
        // Emit MainReportVersion and MainReportArgs events once when invocation span is matched
        if let Some(invocation) = span.attributes.downcast_ref::<Invocation>() {
            self.emit_main_report_version();
            self.emit_main_report_args(invocation);
            return;
        }

        // Dispatch to NodeEvaluated handler
        if let Some(node_evaluated) = span.attributes.downcast_ref::<NodeEvaluated>() {
            self.emit_node_evaluated_start(node_evaluated, span);
            return;
        }

        // Dispatch to NodeProcessed handler
        if let Some(node_processed) = span.attributes.downcast_ref::<NodeProcessed>() {
            self.emit_node_processed_start(node_processed, span);
        }
    }

    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &mut DataProvider<'_>) {
        // Dispatch to QueryExecuted handler
        if let Some(query_data) = span.attributes.downcast_ref::<QueryExecuted>() {
            self.emit_query_executed(query_data, span);
            return;
        }

        // Dispatch to ArtifactWritten handler
        if let Some(artifact_data) = span.attributes.downcast_ref::<ArtifactWritten>() {
            self.emit_artifact_written(artifact_data, span);
            return;
        }

        // Dispatch to NodeProcessed handler
        if let Some(node_processed) = span.attributes.downcast_ref::<NodeProcessed>() {
            self.emit_node_processed_end(node_processed, span);
            return;
        }

        // Dispatch to Invocation handler
        if let Some(invocation) = span.attributes.downcast_ref::<Invocation>() {
            self.emit_invocation_completed(invocation, span, data_provider);
        }
    }

    fn on_log_record(&self, log_record: &LogRecordInfo, _data_provider: &mut DataProvider<'_>) {
        // Dispatch to LogMessage handler
        if let Some(log_msg) = log_record.attributes.downcast_ref::<LogMessage>() {
            self.emit_log_message(log_msg, log_record);
            return;
        }

        // Dispatch to UserLogMessage handler
        if let Some(user_log_msg) = log_record.attributes.downcast_ref::<UserLogMessage>() {
            self.emit_user_log_message(user_log_msg, log_record);
            return;
        }

        // Dispatch to ListItemOutput handler
        if let Some(list_item) = log_record.attributes.downcast_ref::<ListItemOutput>() {
            self.emit_list_item_output(list_item, log_record);
            return;
        }

        // Dispatch to ShowDataOutput handler
        if let Some(inline_data) = log_record.attributes.downcast_ref::<ShowDataOutput>() {
            self.emit_show_data_output(inline_data, log_record);
        }
    }
}
