use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use dbt_adapter_core::AdapterType;
use dbt_auth::AdapterConfig;
use dbt_common::behavior_flags::Behavior;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::{Backend, Connection, QueryCtx};
use minijinja::State;
use xdbc_record_replay::{RecordConnection, RecordingContext, ReplayConnection};

use crate::cache::RelationCache;
use crate::engine::query_comment::QueryCommentConfig;
use crate::query_cache::QueryCache;
use crate::sql_types::TypeOps;
use crate::stmt_splitter::StmtSplitter;

use super::adapter_engine::{AdapterEngine, Options};

static GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_generation() -> u64 {
    GENERATION.fetch_add(1, Ordering::Relaxed)
}

enum Mode {
    Record,
    Replay,
}

pub struct RecordReplayEngine {
    inner: Arc<dyn AdapterEngine>,
    recordings_path: PathBuf,
    config: xdbc_record_replay::SharedConfig,
    query_comment: Option<QueryCommentConfig>,
    mode: Mode,
    generation: u64,
}

impl RecordReplayEngine {
    pub fn record(inner: Arc<dyn AdapterEngine>, recordings_path: PathBuf) -> Self {
        let generation = next_generation();
        if xdbc_record_replay::reset_counters(&recordings_path) {
            crate::connection::drain_recycling_pool();
        }
        Self {
            inner,
            recordings_path,
            config: Arc::new(xdbc_record_replay::Config {
                sql_normalizer: Box::new(DbtSqlNormalizer),
            }),
            query_comment: None,
            mode: Mode::Record,
            generation,
        }
    }

    pub fn replay(
        inner: Arc<dyn AdapterEngine>,
        recordings_path: PathBuf,
        query_comment: Option<QueryCommentConfig>,
    ) -> Self {
        let generation = next_generation();
        if xdbc_record_replay::reset_counters(&recordings_path) {
            crate::connection::drain_recycling_pool();
        }
        Self {
            inner,
            recordings_path,
            config: Arc::new(xdbc_record_replay::Config {
                sql_normalizer: Box::new(DbtSqlNormalizer),
            }),
            query_comment,
            mode: Mode::Replay,
            generation,
        }
    }
}

impl AdapterEngine for RecordReplayEngine {
    fn adapter_type(&self) -> AdapterType {
        self.inner.adapter_type()
    }

    fn backend(&self) -> Backend {
        self.inner.backend()
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.inner.quoting()
    }

    fn splitter(&self) -> &dyn StmtSplitter {
        self.inner.splitter()
    }

    fn type_ops(&self) -> &dyn TypeOps {
        self.inner.type_ops()
    }

    fn query_comment(&self) -> &QueryCommentConfig {
        self.query_comment
            .as_ref()
            .unwrap_or_else(|| self.inner.query_comment())
    }

    fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        self.inner.config(key)
    }

    fn get_config(&self) -> &AdapterConfig {
        self.inner.get_config()
    }

    fn query_cache(&self) -> Option<&Arc<dyn QueryCache>> {
        self.inner.query_cache()
    }

    fn relation_cache(&self) -> &Arc<RelationCache> {
        self.inner.relation_cache()
    }

    fn behavior(&self) -> &Arc<Behavior> {
        self.inner.behavior()
    }

    fn behavior_flag_overrides(&self) -> &BTreeMap<String, bool> {
        self.inner.behavior_flag_overrides()
    }

    fn is_replay(&self) -> bool {
        matches!(self.mode, Mode::Replay)
    }

    fn generation(&self) -> u64 {
        self.generation
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> dbt_common::AdapterResult<Box<dyn Connection>> {
        match self.mode {
            Mode::Replay => {
                let mut conn = ReplayConnection::new(
                    self.recordings_path.clone(),
                    self.config.clone(),
                    self.generation,
                );
                conn.set_recording_context(RecordingContext {
                    node_id,
                    metadata: false,
                });
                Ok(Box::new(conn))
            }
            Mode::Record => {
                let inner = self.inner.new_connection(state, node_id.clone())?;
                let mut conn =
                    RecordConnection::new(self.recordings_path.clone(), inner, self.generation);
                conn.set_recording_context(RecordingContext {
                    node_id,
                    metadata: false,
                });
                Ok(Box::new(conn))
            }
        }
    }

    fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> dbt_common::AdapterResult<Box<dyn Connection>> {
        match self.mode {
            Mode::Replay => {
                let conn = ReplayConnection::new(
                    self.recordings_path.clone(),
                    self.config.clone(),
                    self.generation,
                );
                Ok(Box::new(conn))
            }
            Mode::Record => self.inner.new_connection_with_config(config),
        }
    }

    fn execute_with_options(
        &self,
        state: Option<&State>,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        options: Options,
        fetch: bool,
        token: CancellationToken,
    ) -> dbt_common::AdapterResult<RecordBatch> {
        super::adapter_engine::adbc_execute_with_options(
            self, state, ctx, conn, sql, options, fetch, token,
        )
    }
}

struct DbtSqlNormalizer;

impl xdbc_record_replay::SqlNormalizer for DbtSqlNormalizer {
    fn normalize(&self, sql: &str) -> String {
        use crate::sql::normalize::normalize_dbt_tmp_name;
        let normalized = normalize_dbt_tmp_name(sql);
        let collapsed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
        collapsed
            .replace("DBT_TESTING_ALT", "[MASKED_ALT_WH]")
            .replace("DBT_TESTING", "[MASKED_WH]")
            .replace("FUSION_ADAPTER_TESTING", "[MASKED_WH]")
            .replace("FUSION_SLT_WAREHOUSE", "[MASKED_WH]")
    }
}
