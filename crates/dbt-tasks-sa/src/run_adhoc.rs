use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion_expr::LogicalPlan;
use dbt_adapter_core::AdapterType;
use dbt_adbc::Connection;
use dbt_common::hashing::code_hash;
use dbt_common::tracing::span_info::record_current_span_status_from_attrs;
use dbt_common::{ErrorCode, FsError, FsResult, create_debug_span, err, fs_err};
use dbt_df_providers::delayed_table::is_schema_compat;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_scheduler::instructions::Instruction;
use dbt_schemas::schemas::telemetry::{QueryExecuted, QueryOutcome};
use dbt_tasks_core::AdhocRunner;

/// Runs queries remotely against the warehouse via an adapter connection.
pub struct RemoteAdhocRunner {
    pub env: Arc<JinjaEnv>,
    pub adapter_type: AdapterType,
}

impl AdhocRunner for RemoteAdhocRunner {
    fn run_adhoc<'a>(
        self: Arc<Self>,
        instruction: &'a Instruction,
        rendered_sql: &'a str,
        _unique_id: Option<&'a str>,
        connection: &'a mut Option<Box<dyn Connection>>,
    ) -> Pin<Box<dyn Future<Output = FsResult<(Vec<RecordBatch>, SchemaRef)>> + Send + 'a>> {
        Box::pin(async move {
            run_remote_adhoc_with_connection(
                instruction,
                rendered_sql,
                &self.env,
                self.adapter_type,
                connection,
            )
            .await
        })
    }
}

async fn run_remote_adhoc_with_connection(
    instruction: &Instruction,
    rendered_sql: &str,
    env: &JinjaEnv,
    adapter_type: AdapterType,
    conn_box: &mut Option<Box<dyn Connection>>,
) -> FsResult<(Vec<RecordBatch>, SchemaRef)> {
    if let Some(result) = replay_run_remote_adhoc_result() {
        return result;
    }

    let conn = {
        if let Some(conn) = conn_box {
            conn.as_mut()
        } else {
            let adapter_engine = env.get_base_adapter().map(|a| Arc::clone(a.engine()));
            let Some(engine) = adapter_engine else {
                return err!(
                    ErrorCode::RemoteError,
                    "No adapter engine configured in workspace"
                );
            };
            let conn = engine.new_connection(None, None)?;
            conn_box.replace(conn);
            conn_box.as_mut().unwrap().as_mut()
        }
    };
    let expected_schema = match instruction {
        Instruction::Sql(_) => None,
        Instruction::Lp(lp_instruction) => match &lp_instruction.plan {
            LogicalPlan::Ddl(..)
            | LogicalPlan::Dml(..)
            | LogicalPlan::Copy(..)
            | LogicalPlan::Repartition(..)
            | LogicalPlan::Statement(..)
            | LogicalPlan::Explain(..)
            | LogicalPlan::Analyze(..)
            | LogicalPlan::DescribeTable(..) => None,
            _ => Some(lp_instruction.plan.schema().clone()),
        },
    };
    let mut stmt = conn.new_statement().map_err(from_adbc_error)?;
    stmt.set_sql_query(rendered_sql).map_err(from_adbc_error)?;

    let (schema, mut reader) = {
        let sql_hash = code_hash(rendered_sql);
        let _query_span_guard = create_debug_span(QueryExecuted::start(
            rendered_sql.to_string(),
            sql_hash,
            adapter_type.as_ref().to_owned(),
            None,
            Some("dbt run query".to_string()),
        ))
        .entered();

        let reader = match stmt.execute() {
            Ok(r) => r,
            Err(e) => {
                record_current_span_status_from_attrs(|attrs| {
                    if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                        attrs.dbt_core_event_code = "E017".to_string();
                        attrs.set_query_outcome(QueryOutcome::Error);
                        attrs.query_error_adapter_message = Some(format!("{:?}", e));
                    }
                });

                if let Some(recorder) = dbt_adapter::time_machine::global_recorder() {
                    recorder.record_run_remote_adhoc(
                        rendered_sql,
                        &[],
                        &Arc::new(Schema::empty()),
                        false,
                        Some(format!("{:?}", e)),
                    );
                }

                return Err(from_adbc_error(e));
            }
        };

        let schema = reader.schema();

        record_current_span_status_from_attrs(|attrs| {
            if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                attrs.dbt_core_event_code = "E017".to_string();
                attrs.set_query_outcome(QueryOutcome::Success);
            }
        });

        (schema, reader)
    };

    if let Some(expected_schema) = expected_schema
        && !is_schema_compat(schema.as_ref(), expected_schema.as_arrow())
    {
        return err!(
            ErrorCode::RemoteError,
            "Detected schema mismatch for {}: \
                this is likely because your local workspace has changes that are not \
                yet reflected in the remote database, \
                you may need to (re)build the workspace",
            instruction.fqn().join(".")
        );
    }

    let records: Vec<RecordBatch> = reader.by_ref().collect::<Result<_, _>>()?;

    if let Some(recorder) = dbt_adapter::time_machine::global_recorder() {
        recorder.record_run_remote_adhoc(rendered_sql, &records, &schema, true, None);
    }

    Ok((records, schema))
}

fn replay_run_remote_adhoc_result() -> Option<FsResult<(Vec<RecordBatch>, SchemaRef)>> {
    let replayer = dbt_adapter::time_machine::global_replayer()?;

    Some(match replayer.get_run_remote_adhoc_result() {
        Some(result) => result.map_err(|e| fs_err!(ErrorCode::RemoteError, "{}", e)),
        None => err!(
            ErrorCode::RemoteError,
            "Missing recorded run_remote_adhoc event during replay; refusing to execute a live adhoc query"
        ),
    })
}

fn from_adbc_error(err: adbc_core::error::Error) -> Box<FsError> {
    fs_err!(ErrorCode::Generic, "{}", err)
}
