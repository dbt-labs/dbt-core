use super::mocks::TestWriter;
use crate::tracing::{
    emit::{
        create_debug_span, create_info_span, create_root_info_span, emit_debug_event,
        emit_error_event, emit_info_event,
    },
    init::create_tracing_subcriber_with_layer,
    layers::{data_layer::TelemetryDataLayer, json_compat_layer::build_json_compat_layer},
    span_info::record_span_status_from_attrs,
    tests::mocks::MockDynSpanEvent,
};
use dbt_telemetry::{
    ArtifactType, ArtifactWritten, ExecutionPhase, Invocation, InvocationEvalArgs, ListItemOutput,
    ListOutputFormat, LogMessage, NodeEvaluated, NodeMaterialization, NodeOutcome,
    NodeOutcomeDetail, NodeProcessed, NodeSkipReason, NodeSkipUpstreamDetail, NodeType,
    QueryExecuted, QueryOutcome, SeverityNumber, ShowDataOutput, ShowDataOutputFormat,
    TelemetryOutputFlags, UserLogMessage, node_processed,
};
use serde_json::{Value, json};
use tracing::level_filters::LevelFilter;
use uuid::Uuid;

const TEST_DATABASE: &str = "SOME_DB";
const TEST_SCHEMA: &str = "SOME_SCHEMA";
const TEST_ALIAS: &str = "my_model";
const TEST_RELATION_NAME: &str = "SOME_DB.SOME_SCHEMA.my_model";
const TEST_REL_PATH: &str = "models/my_model.sql";
const TEST_NODE_CHECKSUM: &str = "abcdef1234567890";

/// Helper function to setup tracing with json compat layer and execute a closure
/// that produces events. Returns the collected JSON output lines.
/// Creates a dummy root span and executes test_fn within its scope.
fn with_json_compat_layer<F>(invocation_id: Uuid, test_fn: F) -> Vec<Value>
where
    F: FnOnce(),
{
    let trace_id = rand::random::<u128>();
    let writer = TestWriter::non_terminal();

    let json_layer = build_json_compat_layer(writer.clone(), LevelFilter::TRACE, invocation_id);

    let subscriber = create_tracing_subcriber_with_layer(
        LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            None,
            false,
            std::iter::empty(),
            std::iter::once(json_layer),
        ),
    );

    tracing::subscriber::with_default(subscriber, || {
        // Create root Invocation span and enter its scope
        let root_span = create_root_info_span(MockDynSpanEvent {
            name: "not used".to_string(),
            // this will ensure some root is present
            flags: TelemetryOutputFlags::OUTPUT_ALL,
            ..Default::default()
        });
        let _guard = root_span.enter();

        // Execute test function within the root span scope
        test_fn();
    });

    // Parse JSON lines
    writer
        .get_lines()
        .into_iter()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return None;
            }
            serde_json::from_str(trimmed).ok()
        })
        .collect()
}

/// Scrubs unstable fields (pid, thread, ts) from JSON output for comparison
fn scrub_json(mut value: Value, scrub_data_keys: &[&str]) -> Value {
    if let Some(info) = value.get_mut("info") {
        if let Some(obj) = info.as_object_mut() {
            obj.remove("pid");
            obj.remove("thread");
            obj.remove("ts");
        }
    }

    // Also scrub keys in data if present, supporting nested paths like "key.subkey.subsubkey"
    if let Some(data) = value.get_mut("data") {
        for &key in scrub_data_keys {
            let parts: Vec<&str> = key.split('.').collect();

            // Navigate to the nested value
            let mut current = &mut *data;

            for (i, &part) in parts.iter().enumerate() {
                if i == parts.len() - 1 {
                    // Last part - this is what we want to scrub
                    if let Some(v) = current.get_mut(part) {
                        *v = Value::String(format!("<redacted::{}>", part));
                    }
                } else {
                    // Intermediate part - navigate deeper
                    if let Some(next) = current.get_mut(part) {
                        current = next;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    value
}

/// Scrubs a list of JSON values
fn scrub_json_list(values: Vec<Value>, scrub_data_keys: &[&str]) -> Vec<Value> {
    values
        .into_iter()
        .map(|v| scrub_json(v, scrub_data_keys))
        .collect()
}

/// Helper to test events and compare against expected JSON outputs
fn test_events<F>(
    test_name: &str,
    invocation_id: Uuid,
    event_fn: F,
    scrub_data_keys: &[&str],
    expected: Vec<Value>,
) where
    F: FnOnce(),
{
    test_events_with_msg_fields_to_ignore(
        test_name,
        invocation_id,
        event_fn,
        scrub_data_keys,
        expected,
        &[],
    )
}

/// Helper to test events and compare against expected JSON outputs, scrubbing all msg fields
fn test_events_scrub_all_msg<F>(
    test_name: &str,
    invocation_id: Uuid,
    event_fn: F,
    scrub_data_keys: &[&str],
    expected: Vec<Value>,
) where
    F: FnOnce(),
{
    let events_len = expected.len();

    test_events_with_msg_fields_to_ignore(
        test_name,
        invocation_id,
        event_fn,
        scrub_data_keys,
        expected,
        &(0..events_len).collect::<Vec<usize>>(), // Ignore all msg fields
    )
}

/// Helper to test events and compare against expected JSON, optionally ignoring msg fields at specific indices
fn test_events_with_msg_fields_to_ignore<F>(
    test_name: &str,
    invocation_id: Uuid,
    event_fn: F,
    scrub_data_keys: &[&str],
    mut expected: Vec<Value>,
    ignore_msg_at_indices: &[usize],
) where
    F: FnOnce(),
{
    let mut actual_outputs = with_json_compat_layer(invocation_id, event_fn);

    assert_eq!(
        actual_outputs.len(),
        expected.len(),
        "[{}] Expected {} outputs, got {}",
        test_name,
        expected.len(),
        actual_outputs.len()
    );

    // Sort both by name to ensure consistent order
    let sort_by_name = |a: &Value, b: &Value| {
        let a_name = a
            .get("info")
            .and_then(|i| i.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let b_name = b
            .get("info")
            .and_then(|i| i.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        a_name.cmp(b_name)
    };
    actual_outputs.sort_by(sort_by_name);
    expected.sort_by(sort_by_name);

    let mut actual_scrubbed = scrub_json_list(actual_outputs, scrub_data_keys);
    let mut expected_scrubbed = scrub_json_list(expected, scrub_data_keys);

    // Remove msg fields at specified indices
    for &idx in ignore_msg_at_indices {
        if idx < actual_scrubbed.len() {
            if let Some(info) = actual_scrubbed[idx].get_mut("info") {
                if let Some(obj) = info.as_object_mut() {
                    obj.remove("msg");
                }
            }
        }
        if idx < expected_scrubbed.len() {
            if let Some(info) = expected_scrubbed[idx].get_mut("info") {
                if let Some(obj) = info.as_object_mut() {
                    obj.remove("msg");
                }
            }
        }
    }

    assert_eq!(
        actual_scrubbed, expected_scrubbed,
        "[{}] JSON outputs mismatch",
        test_name
    );
}

#[test]
fn test_query_executed() {
    let invocation_id = Uuid::new_v4();

    test_events_scrub_all_msg(
        "QueryExecuted",
        invocation_id,
        || {
            // With unique_id
            let _span = create_info_span(QueryExecuted {
                sql: "SELECT 1".to_string(),
                query_outcome: QueryOutcome::Success as i32,
                unique_id: Some("model.my_project.my_model".to_string()),
                dbt_core_event_code: "Q001".to_string(),
                ..Default::default()
            });
            // Without unique_id
            let _span = create_info_span(QueryExecuted {
                sql: "SELECT 2".to_string(),
                query_outcome: QueryOutcome::Success as i32,
                unique_id: None,
                dbt_core_event_code: "Q002".to_string(),
                ..Default::default()
            });
        },
        &[],
        vec![
            // Reverse order due to span closing order
            json!({
                "info": {
                    "category": "",
                    "code": "Q002",
                    "invocation_id": invocation_id.to_string(),
                    "name": "SQLQuery",
                    "level": "info",
                    "extra": {}
                },
                "data": {
                    "sql": "SELECT 2"
                }
            }),
            json!({
                "info": {
                    "category": "",
                    "code": "Q001",
                    "invocation_id": invocation_id.to_string(),
                    "name": "SQLQuery",
                    "level": "info",
                    "extra": {}
                },
                "data": {
                    "sql": "SELECT 1",
                    "node_info": {
                        "unique_id": "model.my_project.my_model"
                    }
                }
            }),
        ],
    );
}

#[test]
fn test_invocation_span() {
    let invocation_id = Uuid::new_v4();

    test_events_with_msg_fields_to_ignore(
        "Ivocation (MainReportVersion, MainReportArgs, CommandCompleted)",
        invocation_id,
        || {
            let _span = create_info_span(Invocation {
                invocation_id: invocation_id.to_string(),
                raw_command: "dbt build --log-format json".to_string(),
                eval_args: Some(InvocationEvalArgs {
                    command: "build".to_string(),
                    debug: Some(false),
                    indirect_selection: Some("eager".to_string()),
                    log_format: Some("json".to_string()),
                    log_path: Some("/path/to/logs".to_string()),
                    profiles_dir: Some("/path/to/profiles".to_string()),
                    quiet: Some(false),
                    target_path: Some("target".to_string()),
                    write_json: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            });
        },
        &["elapsed", "completed_at"],
        vec![
            // CommandCompleted
            json!({
                "info": {
                    "category": "",
                    "code": "Q039",
                    "invocation_id": invocation_id.to_string(),
                    "name": "CommandCompleted",
                    "level": "info",
                    "extra": {}
                },
                "data": {
                    "elapsed": "<redacted::elapsed>",
                    "success": true,
                    "completed_at": "<redacted::completed_at>"
                }
            }),
            // MainReportArgs
            json!({
                "info": {
                    "category": "",
                    "code": "A002",
                    "invocation_id": invocation_id.to_string(),
                    "name": "MainReportArgs",
                    "level": "debug",
                    "msg": "",
                    "extra": {}
                },
                "data": {
                    "args": {
                        "debug": "false",
                        "indirect_selection": "eager",
                        "invocation_command": "dbt build --log-format json",
                        "log_format": "json",
                        "log_path": "/path/to/logs",
                        "profiles_dir": "/path/to/profiles",
                        "quiet": "false",
                        "target_path": "target",
                        "write_json": "true"
                    }
                }
            }),
            // MainReportVersion
            json!({
                "info": {
                    "category": "",
                    "code": "A001",
                    "invocation_id": invocation_id.to_string(),
                    "name": "MainReportVersion",
                    "level": "info",
                    "msg": format!("Running with dbt-fusion={}", env!("CARGO_PKG_VERSION")),
                    "extra": {}
                },
                "data": {
                    "log_version": 3,
                    "version": format!("={}", env!("CARGO_PKG_VERSION"))
                }
            }),
        ],
        // do not check msg field for CommandCompleted, has duration
        // and MainReportArgs which is log and not critical here
        &[0, 1],
    );
}

#[test]
fn test_artifact_written() {
    let invocation_id = Uuid::new_v4();

    test_events(
        "ArtifactWritten",
        invocation_id,
        || {
            // Test Manifest type (should map to "WritableManifest")
            let _span = create_info_span(ArtifactWritten {
                relative_path: "target/manifest.json".to_string(),
                artifact_type: ArtifactType::Manifest as i32,
            });

            // Test other artifact types (should use as_str_name())
            let _span = create_info_span(ArtifactWritten {
                relative_path: "target/catalog.json".to_string(),
                artifact_type: ArtifactType::Catalog as i32,
            });
        },
        &[],
        vec![
            json!({
                "info": {
                    "category": "",
                    "code": "P001",
                    "invocation_id": invocation_id.to_string(),
                    "name": "ArtifactWritten",
                    "level": "info",
                    "msg": "Artifact written to target/catalog.json",
                    "extra": {}
                },
                "data": {
                    "artifact_path": "target/catalog.json",
                    "artifact_type": "ARTIFACT_TYPE_CATALOG"
                }
            }),
            json!({
                "info": {
                    "category": "",
                    "code": "P001",
                    "invocation_id": invocation_id.to_string(),
                    "name": "ArtifactWritten",
                    "level": "info",
                    "msg": "Artifact written to target/manifest.json",
                    "extra": {}
                },
                "data": {
                    "artifact_path": "target/manifest.json",
                    "artifact_type": "WritableManifest"
                }
            }),
        ],
    );
}

#[test]
fn test_log_message() {
    let invocation_id = Uuid::new_v4();

    test_events_with_msg_fields_to_ignore(
        "LogMessage",
        invocation_id,
        || {
            emit_error_event(
                LogMessage {
                    code: Some(1234),
                    original_severity_number: SeverityNumber::Error as i32,
                    original_severity_text: "ERROR".to_string(),
                    ..Default::default()
                },
                Some("Test error message"),
            );
        },
        &[],
        vec![json!({
            "info": {
                "category": "",
                "code": "",
                "invocation_id": invocation_id.to_string(),
                "name": "Generic",
                "level": "error",
                "extra": {},
                "msg": "error: Test error message"
            },
            "data": {}
        })],
        &[],
    );
}

#[test]
fn test_user_log_message_print() {
    let invocation_id = Uuid::new_v4();

    test_events(
        "UserLogMessage (print)",
        invocation_id,
        || {
            emit_info_event(
                UserLogMessage {
                    is_print: true,
                    ..Default::default()
                },
                Some("Hello from print()"),
            );
        },
        &[],
        vec![json!({
            "info": {
                "category": "",
                "code": "Z052",
                "invocation_id": invocation_id.to_string(),
                "name": "PrintEvent",
                "msg": "Hello from print()",
                "level": "info",
                "extra": {}
            },
            "data": {
                "msg": "Hello from print()"
            }
        })],
    );
}

#[test]
fn test_user_log_message_jinja_log_info() {
    let invocation_id = Uuid::new_v4();

    test_events(
        "UserLogMessage (log with info=true)",
        invocation_id,
        || {
            emit_info_event(
                UserLogMessage {
                    is_print: false,
                    ..Default::default()
                },
                Some("Log info message"),
            );
        },
        &[],
        vec![json!({
            "info": {
                "category": "",
                "code": "I062",
                "invocation_id": invocation_id.to_string(),
                "name": "JinjaLogInfo",
                "msg": "Log info message",
                "level": "info",
                "extra": {}
            },
            "data": {
                "msg": "Log info message"
            }
        })],
    );
}

#[test]
fn test_user_log_message_jinja_debug_info() {
    let invocation_id = Uuid::new_v4();

    test_events(
        "UserLogMessage (log with info=false)",
        invocation_id,
        || {
            emit_debug_event(
                UserLogMessage {
                    is_print: false,
                    ..Default::default()
                },
                Some("Log debug message"),
            );
        },
        &[],
        vec![json!({
            "info": {
                "category": "",
                "code": "I063",
                "invocation_id": invocation_id.to_string(),
                "name": "JinjaDebugInfo",
                "msg": "Log debug message",
                "level": "debug",
                "extra": {}
            },
            "data": {
                "msg": "Log debug message"
            }
        })],
    );
}

#[test]
fn test_list_item_output() {
    let invocation_id = Uuid::new_v4();

    test_events(
        "ListItemOutput",
        invocation_id,
        || {
            emit_info_event(
                ListItemOutput {
                    content: "model.my_project.my_model".to_string(),
                    output_format: ListOutputFormat::Name as i32,
                    unique_id: Some("model.my_project.my_model".to_string()),
                },
                Some("model.my_project.my_model"),
            );
        },
        &[],
        vec![json!({
            "info": {
                "category": "",
                "code": "Z052",
                "invocation_id": invocation_id.to_string(),
                "name": "PrintEvent",
                "msg": "model.my_project.my_model",
                "level": "info",
                "extra": {}
            },
            "data": {
                "msg": "model.my_project.my_model"
            }
        })],
    );
}

#[test]
fn test_show_data_output() {
    let invocation_id = Uuid::new_v4();

    test_events(
        "ShowDataOutput",
        invocation_id,
        || {
            emit_info_event(
                ShowDataOutput {
                    node_name: TEST_ALIAS.to_string(),
                    content: "col1\tcol2\nval1\tval2".to_string(),
                    is_inline: false,
                    output_format: ShowDataOutputFormat::Tsv as i32,
                    unique_id: Some("model.my_project.my_model".to_string()),
                    columns: vec!["col1".to_string(), "col2".to_string()],
                    dbt_core_event_code: "Q041".to_string(),
                },
                Some("col1\tcol2\nval1\tval2"),
            );
        },
        &[],
        vec![json!({
            "info": {
                "category": "",
                "code": "Q041",
                "invocation_id": invocation_id.to_string(),
                "name": "ShowNode",
                "msg": "col1\tcol2\nval1\tval2",
                "level": "info",
                "extra": {}
            },
            "data": {
                "node_name": TEST_ALIAS,
                "preview": "col1\tcol2\nval1\tval2",
                "is_inline": false,
                "output_format": "tsv",
                "unique_id": "model.my_project.my_model",
                "columns": ["col1", "col2"]
            }
        })],
    );
}

#[test]
fn test_show_data_output_inline_without_unique_id() {
    let invocation_id = Uuid::new_v4();

    test_events(
        "ShowDataOutput (inline without unique_id)",
        invocation_id,
        || {
            emit_info_event(
                ShowDataOutput {
                    node_name: "inline_query".to_string(),
                    content: "result".to_string(),
                    is_inline: true,
                    output_format: ShowDataOutputFormat::Text as i32,
                    unique_id: None,
                    columns: vec![],
                    dbt_core_event_code: "Q041".to_string(),
                },
                Some("result"),
            );
        },
        &[],
        vec![json!({
            "info": {
                "category": "",
                "code": "Q041",
                "invocation_id": invocation_id.to_string(),
                "name": "ShowNode",
                "msg": "result",
                "level": "info",
                "extra": {}
            },
            "data": {
                "node_name": "inline_query",
                "preview": "result",
                "is_inline": true,
                "output_format": "text",
                "unique_id": "sql_operation.inline_query",
                "columns": []
            }
        })],
    );
}

#[test]
fn test_node_events() {
    let invocation_id = Uuid::new_v4();
    let test_node_type = NodeType::Test;
    let test_unique_id = format!(
        "{}.my_project.{}",
        test_node_type.as_static_ref(),
        TEST_ALIAS
    );
    let test_upstream_id = format!(
        "{}.my_project.upstream_model",
        NodeType::Model.as_static_ref()
    );
    let test_custom_mat = "test_mat".to_string();

    let create_test_node_evaluated_span = |phase: ExecutionPhase| -> tracing::Span {
        create_debug_span(NodeEvaluated::start(
            test_unique_id.clone(),
            TEST_ALIAS.to_string(),
            Some(TEST_DATABASE.to_string()),
            Some(TEST_SCHEMA.to_string()),
            Some(TEST_ALIAS.to_string()),
            Some(NodeMaterialization::Custom),
            Some(test_custom_mat.clone()),
            test_node_type,
            phase,
            TEST_REL_PATH.to_string(),
            Some(1),
            Some(0),
            TEST_NODE_CHECKSUM.to_string(),
        ))
    };

    let setup = || {
        let np_span = create_info_span(NodeProcessed::start(
            test_unique_id.clone(),
            TEST_ALIAS.to_string(),
            Some(TEST_DATABASE.to_string()),
            Some(TEST_SCHEMA.to_string()),
            Some(TEST_ALIAS.to_string()),
            Some(NodeMaterialization::Custom),
            Some(test_custom_mat.clone()),
            test_node_type,
            Some(ExecutionPhase::Run),
            TEST_REL_PATH.to_string(),
            Some(1),
            Some(0),
            TEST_NODE_CHECKSUM.to_string(),
            true,
        ));

        // create NodeEvaluated spans for different phases that follow from NodeProcessed
        let render_ne_span = create_test_node_evaluated_span(ExecutionPhase::Render);
        render_ne_span.follows_from(np_span.clone());

        // Record success on render
        record_span_status_from_attrs(&render_ne_span, |attrs| {
            if let Some(ev) = attrs.downcast_mut::<NodeEvaluated>() {
                ev.set_node_outcome(NodeOutcome::Success);
            }
        });

        drop(render_ne_span);

        // Now analyze phase
        let analyze_ne_span = create_test_node_evaluated_span(ExecutionPhase::Analyze);
        analyze_ne_span.follows_from(np_span.clone());

        // Record success on analyze
        record_span_status_from_attrs(&analyze_ne_span, |attrs| {
            if let Some(ev) = attrs.downcast_mut::<NodeEvaluated>() {
                ev.set_node_outcome(NodeOutcome::Success);
            }
        });
        drop(analyze_ne_span);

        // Finally run phase with skip result
        let run_ne_span = create_test_node_evaluated_span(ExecutionPhase::Run);
        run_ne_span.follows_from(np_span.clone());

        // Record skip on run
        record_span_status_from_attrs(&run_ne_span, |attrs| {
            if let Some(ev) = attrs.downcast_mut::<NodeEvaluated>() {
                ev.set_node_outcome(NodeOutcome::Skipped);
                ev.set_node_skip_reason(NodeSkipReason::Upstream);
                ev.node_outcome_detail = Some(NodeOutcomeDetail::NodeSkipUpstreamDetail(
                    NodeSkipUpstreamDetail::new(test_upstream_id.clone()),
                ));
            }
        });
        drop(run_ne_span);

        // Record the same skip outcome on NodeProcessed
        record_span_status_from_attrs(&np_span, |attrs| {
            if let Some(ev) = attrs.downcast_mut::<NodeProcessed>() {
                ev.set_node_outcome(NodeOutcome::Skipped);
                ev.set_node_skip_reason(NodeSkipReason::Upstream);
                ev.node_outcome_detail =
                    Some(node_processed::NodeOutcomeDetail::NodeSkipUpstreamDetail(
                        NodeSkipUpstreamDetail::new(test_upstream_id.clone()),
                    ));
            }
        });
    };

    let expected_node_info = |node_status: &str, include_finished: bool| {
        json!({
            "node_path": TEST_REL_PATH,
            "node_name": TEST_ALIAS,
            "unique_id": test_unique_id.clone(),
            "resource_type": test_node_type.as_static_ref(),
            "materialized": test_custom_mat.clone(),
            "node_status": node_status,
            "node_started_at": "<redacted::node_started_at>",
            "node_finished_at": if include_finished { "<redacted::node_finished_at>" } else { "" },
            "node_relation": {
                "database": TEST_DATABASE,
                "schema": TEST_SCHEMA,
                "alias": TEST_ALIAS,
                "relation_name": TEST_RELATION_NAME
            },
            "node_checksum": TEST_NODE_CHECKSUM
        })
    };

    let expected_node_compiling = json!({
        "info": {
            "category": "",
            "code": "Q030",
            "invocation_id": invocation_id.to_string(),
            "name": "NodeCompiling",
            "level": "debug",
            "extra": {},
            "msg": format!("Started rendering test {}.{}", TEST_SCHEMA, TEST_ALIAS),
        },
        "data": {
            "node_info": expected_node_info("compiling", false)
        }
    });

    let expected_node_executing = json!({
        "info": {
            "category": "",
            "code": "Q031",
            "invocation_id": invocation_id.to_string(),
            "name": "NodeExecuting",
            "level": "debug",
            "extra": {},
            "msg": format!("Started running test {}.{}", TEST_SCHEMA, TEST_ALIAS),
        },
        "data": {
            "node_info": expected_node_info("executing", false)
        }
    });

    let expected_log_start_line = json!({
        "info": {
            "category": "",
            "code": "Q011",
            "invocation_id": invocation_id.to_string(),
            "name": "LogStartLine",
            "level": "info",
            "extra": {},
            "msg": format!("Started test {}.{}", TEST_SCHEMA, TEST_ALIAS),
        },
        "data": {
            "node_info": expected_node_info("started", false)
        }
    });

    let expected_node_start = json!({
        "info": {
            "category": "",
            "code": "Q024",
            "invocation_id": invocation_id.to_string(),
            "name": "NodeStart",
            "level": "debug",
            "msg": format!("Began running node {}", test_unique_id.clone()),
            "extra": {}
        },
        "data": {
            "node_info": expected_node_info("started", false)
        }
    });

    let expected_log_result = json!({
        "info": {
            "category": "",
            "code": "Q007",
            "invocation_id": invocation_id.to_string(),
            "name": "LogTestResult",
            "level": "info",
            "extra": {},
            "msg": format!("   Skipped [-------] test  {}.{} ({} - {}:1:0)", TEST_SCHEMA, TEST_ALIAS, test_custom_mat.clone(), TEST_REL_PATH)
        },
        "data": {
            "node_info": expected_node_info("skipped", true),
            "status": "skipped",
            "execution_time": "<redacted::execution_time>",
        }
    });

    let expected_mark_skipped = json!({
        "info": {
            "category": "",
            "code": "Z033",
            "invocation_id": invocation_id.to_string(),
            "name": "MarkSkippedChildren",
            "level": "debug",
            "extra": {},
            "msg": format!("Marking children of {} as skipped", test_unique_id.clone())
        },
        "data": {
            "unique_id": test_unique_id.clone(),
            "status": "skipped",
            "run_result": {
                "status": "skipped",
                "execution_time": "<redacted::execution_time>",
            }
        }
    });

    let expected_node_finished = json!({
        "info": {
            "category": "",
            "code": "Q025",
            "invocation_id": invocation_id.to_string(),
            "name": "NodeFinished",
            "level": "debug",
            "extra": {},
            "msg": format!("Finished running node {}", test_unique_id.clone()),
        },
        "data": {
            "node_info": expected_node_info("skipped", true),
            "run_result": {
                "status": "skipped",
                "execution_time": "<redacted::execution_time>",
            }
        }
    });

    test_events(
        "Node events (skipped test)",
        invocation_id,
        setup,
        &[
            "node_info.node_started_at",
            "node_info.node_finished_at",
            "execution_time",
            "run_result.execution_time",
        ],
        vec![
            expected_node_compiling,
            expected_node_executing,
            expected_log_start_line,
            expected_node_start,
            expected_log_result,
            expected_mark_skipped,
            expected_node_finished,
        ],
    );
}
