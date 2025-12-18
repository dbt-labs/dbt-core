use crate::relation::databricks::base::*;
use crate::relation::databricks::incremental::IncrementalTableConfig;
use crate::relation::databricks::materialized_view::MaterializedViewConfig;
use crate::relation::databricks::relation_api::get_from_relation_config;
use crate::relation::databricks::streaming_table::StreamingTableConfig;
use crate::relation::databricks::view::ViewConfig;

use crate::AdapterResponse;
use crate::AdapterTyping;
use crate::adapter_engine::AdapterEngine;
use crate::catalog_relation::CatalogRelation;
use crate::column::Column;
use crate::errors::AdapterResult;
use crate::load_catalogs;
use crate::metadata::*;
use crate::query_ctx::query_ctx_from_state;
use crate::record_batch_utils::get_column_values;
use crate::relation::BaseRelationConfig;
use crate::relation::databricks::DatabricksRelationConfig;
use crate::typed_adapter::TypedBaseAdapter;
use arrow::array::{Array, StringArray};

use dbt_agate::AgateTable;
use dbt_common::{AdapterError, AdapterErrorKind};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::schemas::{InternalDbtNodeAttributes, InternalDbtNodeWrapper};
use dbt_xdbc::{Connection, QueryCtx};
use indexmap::IndexMap;
use minijinja::{State, Value};
use once_cell::sync::Lazy;
use regex::Regex;
use uuid::Uuid;

use std::collections::BTreeMap;
use std::fmt;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::OnceLock;

use super::version::DbrVersion;

static CREDENTIAL_IN_COPY_INTO_REGEX: Lazy<Regex> = Lazy::new(|| {
    // This is NOT the same as the Python regex used in dbt-databricks. Rust lacks lookaround.
    // This achieves the same result for the proper structure.  See original at time of port:
    // https://github.com/databricks/dbt-databricks/blob/66f513b960c62ee21c4c399264a41a56853f3d82/dbt/adapters/databricks/utils.py#L19
    Regex::new(r"credential\s*(\(\s*'[\w\-]+'\s*=\s*'.*?'\s*(?:,\s*'[\w\-]+'\s*=\s*'.*?'\s*)*\))")
        .expect("CREDENTIALS_IN_COPY_INTO_REGEX invalid")
});

/// An adapter for interacting with Databricks.
#[derive(Clone)]
pub struct DatabricksAdapter {
    engine: Arc<AdapterEngine>,
}

impl fmt::Debug for DatabricksAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl DatabricksAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }

    /// Get the Databricks Runtime version, caching the result for subsequent calls.
    ///
    /// To bypass the cache, use [`get_dbr_version()`](Self::get_dbr_version) directly.
    pub(crate) fn dbr_version(&self) -> AdapterResult<DbrVersion> {
        static CACHED_DBR_VERSION: OnceLock<AdapterResult<DbrVersion>> = OnceLock::new();

        CACHED_DBR_VERSION
            .get_or_init(|| {
                let query_ctx = QueryCtx::default().with_desc("get_dbr_version adapter call");
                let mut conn = self.engine.new_connection(None, None)?;
                self.get_dbr_version(&query_ctx, conn.deref_mut())
            })
            .clone()
    }

    /// Get the Databricks Runtime version without caching.
    fn get_dbr_version(
        &self,
        ctx: &QueryCtx,
        conn: &mut dyn Connection,
    ) -> AdapterResult<DbrVersion> {
        // https://docs.databricks.com/aws/en/sql/language-manual/functions/current_version
        // dbr_version is a null string if this query runs in non-cluster mode

        // It appears that this is a divergence from the dbt-databricks implementation,
        // which uses `SET spark.databricks.clusterUsageTags.sparkVersion` to read out the version instead.
        // They only do this if `is_cluster` is True, otherwise it would error.
        let batch = self.engine.execute(
            None,
            conn,
            ctx,
            "select current_version().dbr_version as dbr_version",
        )?;

        let dbr_version = get_column_values::<StringArray>(&batch, "dbr_version")?;
        debug_assert_eq!(dbr_version.len(), 1);

        // if dbr_version is null, then we are not on a cluster and we can assume the version is greater than the requested version
        // dbt is applying a similar logic here: https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/handle.py#L143-L144
        //
        // https://docs.databricks.com/aws/en/sql/language-manual/functions/version#examples
        // in format "[dbr_version] [git_hash]"
        //
        // TODO(cwalden): it looks like this might be wrong?
        //  `current_version().dbr_version` doesn't contain the git hash, so I don't think need this first split.
        dbr_version.value(0).parse::<DbrVersion>()
    }

    /// Given the relation, fetch its config from the remote data warehouse
    /// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L871
    pub fn get_from_relation<T: fmt::Debug + DatabricksRelationConfig>(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        base_relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<T> {
        let relation_type = base_relation.relation_type().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("relation_type is required for the input relation of adapter.get_relation_config. Input relation: {}", base_relation.render_self_as_str()),
            )
        })?;

        let database = base_relation.database_as_str()?;
        let schema = base_relation.schema_as_str()?;
        let identifier = base_relation.identifier_as_str()?;
        let rendered_relation = base_relation.render_self_as_str();

        // Start with common metadata
        let mut results_builder = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(self.describe_extended(
                &database,
                &schema,
                &identifier,
                state,
                &mut *conn,
            )?)
            .with_show_tblproperties(self.show_tblproperties(
                &rendered_relation,
                state,
                &mut *conn,
            )?);

        // Add materialization-specific metadata
        // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/adapters/databricks/impl.py#L914-L1021
        results_builder = match relation_type {
            RelationType::MaterializedView => results_builder.with_info_schema_views(
                self.get_view_description(&database, &schema, &identifier, state, &mut *conn)?,
            ),
            RelationType::View => results_builder
                .with_info_schema_views(self.get_view_description(
                    &database,
                    &schema,
                    &identifier,
                    state,
                    &mut *conn,
                )?)
                .with_info_schema_tags(self.fetch_tags(
                    &database,
                    &schema,
                    &identifier,
                    state,
                    &mut *conn,
                )?)
                .with_info_schema_column_tags(self.fetch_column_tags(
                    &database,
                    &schema,
                    &identifier,
                    state,
                    &mut *conn,
                )?),
            RelationType::StreamingTable => results_builder,
            RelationType::Table => {
                let is_hive_metastore =
                    base_relation.is_hive_metastore().try_into().map_err(|_| {
                        AdapterError::new(
                            AdapterErrorKind::Configuration,
                            format!(
                                "Unable to decode is_hive_metastore config for {}",
                                base_relation.render_self_as_str()
                            ),
                        )
                    })?;
                if is_hive_metastore {
                    return Err(AdapterError::new(
                        AdapterErrorKind::NotSupported,
                        format!(
                            "Incremental application of constraints and column masks is not supported for Hive Metastore! Relation: `{database}`.`{schema}`.`{identifier}`"
                        ),
                    ));
                }
                results_builder
                    .with_info_schema_tags(self.fetch_tags(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?)
                    .with_info_schema_column_tags(self.fetch_column_tags(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?)
                    .with_non_null_constraints(self.fetch_non_null_constraint_columns(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?)
                    .with_primary_key_constraints(self.fetch_primary_key_constraints(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?)
                    .with_foreign_key_constraints(self.fetch_foreign_key_constraints(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?)
                    .with_column_masks(self.fetch_column_masks(
                        &database,
                        &schema,
                        &identifier,
                        state,
                        &mut *conn,
                    )?)
            }
            _ => unreachable!(),
        };
        let result = from_results::<T>(results_builder.build())?;
        let tblproperties = result.get_config("tblproperties");
        if let Some(DatabricksComponentConfig::TblProperties(_tblproperties)) = tblproperties {
            // https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L908
            // todo: Implement polling for DLT pipeline status
            // we don't have the dbx client here
            // we might need to query internal delta system tables or expose something via ADBC
        }
        Ok(result)
    }

    // convenience for executing SQL
    fn execute_sql_with_context(
        &self,
        sql: &str,
        state: &State,
        desc: &str,
        conn: &mut dyn Connection,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let ctx = query_ctx_from_state(state)?.with_desc(desc);
        self.execute(
            Some(state),
            conn,
            &ctx,
            sql,
            false, // auto_begin
            true,  // fetch
            None,  // limit
            None,  // options
        )
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/6f2aae13e39c5df1c93e5d514678914142d71768/dbt-spark/src/dbt/include/spark/macros/adapters.sql#L314
    fn describe_extended(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!("DESCRIBE EXTENDED `{database}`.`{schema}`.`{identifier}`;");
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Describe table extended", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/adapters/metadata.sql#L78
    fn get_view_description(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT * 
            FROM `SYSTEM`.`INFORMATION_SCHEMA`.`VIEWS`
            WHERE TABLE_CATALOG = '{}'
                AND TABLE_SCHEMA = '{}'
                AND TABLE_NAME = '{}';",
            database.to_lowercase(),
            schema.to_lowercase(),
            identifier.to_lowercase()
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Query for view", conn)?;
        Ok(result)
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/6f2aae13e39c5df1c93e5d514678914142d71768/dbt-spark/src/dbt/include/spark/macros/adapters.sql#L127
    fn show_tblproperties(
        &self,
        relation_str: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!("SHOW TBLPROPERTIES {relation_str}");
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Show table properties", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/constraints.sql#L1
    fn fetch_non_null_constraint_columns(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT column_name
            FROM `{database}`.`information_schema`.`columns`
            WHERE table_catalog = '{database}' 
              AND table_schema = '{schema}'
              AND table_name = '{identifier}'
              AND is_nullable = 'NO';"
        );
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Fetch non null constraint columns", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/constraints.sql#L20
    fn fetch_primary_key_constraints(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT kcu.constraint_name, kcu.column_name
            FROM `{database}`.information_schema.key_column_usage kcu
            WHERE kcu.table_catalog = '{database}' 
                AND kcu.table_schema = '{schema}'
                AND kcu.table_name = '{identifier}' 
                AND kcu.constraint_name = (
                SELECT constraint_name
                FROM `{database}`.information_schema.table_constraints
                WHERE table_catalog = '{database}'
                    AND table_schema = '{schema}'
                    AND table_name = '{identifier}' 
                    AND constraint_type = 'PRIMARY KEY'
                )
            ORDER BY kcu.ordinal_position;"
        );
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Fetch PK constraints", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/column_mask.sql#L11
    fn fetch_column_masks(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT 
                column_name,
                mask_name,
                using_columns
            FROM `system`.`information_schema`.`column_masks`
            WHERE table_catalog = '{database}'
                AND table_schema = '{schema}'
                AND table_name = '{identifier}';"
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Fetch column masks", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/components/constraints.sql#L47
    fn fetch_foreign_key_constraints(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT
                kcu.constraint_name,
                kcu.column_name AS from_column,
                ukcu.table_catalog AS to_catalog,
                ukcu.table_schema AS to_schema,
                ukcu.table_name AS to_table,
                ukcu.column_name AS to_column
            FROM `{database}`.information_schema.key_column_usage kcu
            JOIN `{database}`.information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
            JOIN `{database}`.information_schema.key_column_usage ukcu
                ON rc.unique_constraint_name = ukcu.constraint_name
                AND kcu.ordinal_position = ukcu.ordinal_position
            WHERE kcu.table_catalog = '{database}'
                AND kcu.table_schema = '{schema}'
                AND kcu.table_name = '{identifier}'
                AND kcu.constraint_name IN (
                SELECT constraint_name
                FROM `{database}`.information_schema.table_constraints
                WHERE table_catalog = '{database}'
                    AND table_schema = '{schema}'
                    AND table_name = '{identifier}'
                    AND constraint_type = 'FOREIGN KEY'
                )
            ORDER BY kcu.ordinal_position;"
        );
        let (_, result) =
            self.execute_sql_with_context(&sql, state, "Fetch FK constraints", conn)?;
        Ok(result)
    }

    // https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/include/databricks/macros/relations/tags.sql#L11
    fn fetch_tags(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT tag_name, tag_value
            FROM `system`.`information_schema`.`table_tags`
            WHERE catalog_name = '{database}' 
                AND schema_name = '{schema}'
                AND table_name = '{identifier}'"
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Fetch tags", conn)?;
        Ok(result)
    }

    fn fetch_column_tags(
        &self,
        database: &str,
        schema: &str,
        identifier: &str,
        state: &State,
        conn: &mut dyn Connection,
    ) -> AdapterResult<AgateTable> {
        let sql = format!(
            "SELECT column_name, tag_name, tag_value
            FROM `system`.`information_schema`.`column_tags`
            WHERE catalog_name = '{database}' 
                AND schema_name = '{schema}'
                AND table_name = '{identifier}'"
        );
        let (_, result) = self.execute_sql_with_context(&sql, state, "Fetch column tags", conn)?;
        Ok(result)
    }
}

impl AdapterTyping for DatabricksAdapter {
    fn metadata_adapter(&self) -> Option<Box<dyn MetadataAdapter>> {
        Some(Box::new(self.clone()))
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }
}

impl TypedBaseAdapter for DatabricksAdapter {
    /// Get the full macro name for check_schema_exists
    ///
    /// # Returns
    ///
    /// Returns (package_name, macro_name)
    fn check_schema_exists_macro(
        &self,
        _state: &State,
        _args: &[Value],
    ) -> AdapterResult<(String, String)> {
        Ok((
            "dbt_spark".to_string(),
            "spark__check_schema_exists".to_string(),
        ))
    }

    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/utils.py#L94
    fn is_cluster(&self) -> AdapterResult<bool> {
        let http_path = self
            .engine()
            .get_config()
            .get_string("http_path")
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "http_path is required to determine Databricks compute type",
                )
            })?;

        let normalized = http_path.trim().to_ascii_lowercase();
        if normalized.contains("/warehouses/") {
            return Ok(false);
        }
        if normalized.contains("/protocolv1/") {
            return Ok(true);
        }
        Ok(false)
    }
    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/connections.py#L226-L227
    fn compare_dbr_version(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        major: i64,
        minor: i64,
    ) -> AdapterResult<Value> {
        let query_ctx = query_ctx_from_state(state)?.with_desc("compare_dbr_version adapter call");

        let current_version = self.get_dbr_version(&query_ctx, conn)?;
        let expected_version = DbrVersion::Full(major, minor);

        let result = match current_version.cmp(&expected_version) {
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Less => -1,
        };

        Ok(Value::from(result))
    }

    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L208-L209
    fn compute_external_path(
        &self,
        config: ModelConfig,
        node: &dyn InternalDbtNodeAttributes,
        is_incremental: bool,
    ) -> AdapterResult<String> {
        // TODO: dbt seems to allow optional database and schema
        // https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L212-L213
        let location_root = config
            .__warehouse_specific_config__
            .location_root
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "location_root is required for external tables.",
                )
            })?;

        let include_full_name_in_path = config
            .__warehouse_specific_config__
            .include_full_name_in_path
            .unwrap_or_default();

        // Build path using the same logic as posixpath.join
        let path = if include_full_name_in_path {
            format!(
                "{}/{}/{}/{}",
                location_root.trim_end_matches('/'),
                node.database().trim_end_matches('/'),
                node.schema().trim_end_matches('/'),
                node.name()
            )
        } else {
            format!(
                "{}/{}/{}",
                location_root.trim_end_matches('/'),
                node.database().trim_end_matches('/'),
                node.name()
            )
        };

        let path = if is_incremental {
            format!("{path}_tmp")
        } else {
            path
        };
        Ok(path)
    }

    /// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/impl.py#L187-L188
    ///
    /// squashes featureset of DatabricksAdapter iceberg_table_properties
    /// https://github.com/databricks/dbt-databricks/blob/53cd1a2c1fcb245ef25ecf2e41249335fd4c8e4b/dbt/adapters/databricks/impl.py#L229C9-L229C41
    fn update_tblproperties_for_uniform_iceberg(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
        tblproperties: &mut BTreeMap<String, Value>,
    ) -> AdapterResult<()> {
        // TODO(anna): Ideally from_model_config_and_catalogs would just take in an InternalDbtNodeWrapper instead of a Value. This is blocked by a Snowflake hack in `snowflake__drop_table`.
        let node_yml = node.as_internal_node().serialize();
        let catalog_relation = CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type(),
            &Value::from_object(dbt_common::serde_utils::convert_yml_to_value_map(node_yml)),
            load_catalogs::fetch_catalogs(),
        )?;
        // We only have to update tblproperties if using a UniForm Iceberg table
        if catalog_relation.table_format == "iceberg" {
            if self
                .compare_dbr_version(state, conn, 14, 3)?
                .as_i64()
                .expect("dbr_version is a number")
                < 0
            {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "Iceberg support requires Databricks Runtime 14.3 or later.",
                ));
            }

            if catalog_relation.file_format != Some("delta".to_string()) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "When table_format is 'iceberg', file_format must be 'delta'.",
                ));
            }

            let materialized = config.materialized.ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "materialized is required for iceberg tables.",
                )
            })?;

            // TODO(versusfacit): support snapshot
            if materialized != DbtMaterialization::Incremental
                && materialized != DbtMaterialization::Table
                && materialized != DbtMaterialization::Seed
            {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "When table_format is 'iceberg', materialized must be 'incremental', 'table', or 'seed'.",
                ));
            }

            tblproperties
                .entry("delta.enableIcebergCompatV2".to_string())
                .or_insert_with(|| Value::from(true));

            tblproperties
                .entry("delta.universalFormat.enabledFormats".to_string())
                .or_insert_with(|| Value::from("iceberg"));
        }
        Ok(())
    }

    /// https://github.com/databricks/dbt-databricks/blob/8cda62ee19d01e0670e3156e652841e3ffd3ed41/dbt/adapters/databricks/impl.py#L253
    fn is_uniform(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        config: ModelConfig,
        node: &InternalDbtNodeWrapper,
    ) -> AdapterResult<bool> {
        // TODO(anna): Ideally from_model_config_and_catalogs would just take in an InternalDbtNodeWrapper instead of a Value. This is blocked by a Snowflake hack in `snowflake__drop_table`.
        let node_yml = node.as_internal_node().serialize();
        let catalog_relation = CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type(),
            &Value::from_object(dbt_common::serde_utils::convert_yml_to_value_map(node_yml)),
            load_catalogs::fetch_catalogs(),
        )?;

        if catalog_relation.table_format != "iceberg" {
            return Ok(false);
        }

        if self
            .compare_dbr_version(state, conn, 14, 3)?
            .as_i64()
            .expect("dbr_version is a number")
            < 0
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Iceberg support requires Databricks Runtime 14.3 or later.",
            ));
        }

        let materialized = config.materialized.ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "materialized is required for iceberg tables.",
            )
        })?;

        // TODO(versusfacit): support snapshot
        if materialized != DbtMaterialization::Incremental
            && materialized != DbtMaterialization::Table
            && materialized != DbtMaterialization::Seed
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "When table_format is 'iceberg', materialized must be 'incremental', 'table', or 'seed'.",
            ));
        }

        let use_uniform = if let Some(val) = catalog_relation.adapter_properties.get("use_uniform")
        {
            val.eq_ignore_ascii_case("true")
        } else {
            false
        };

        if use_uniform && catalog_relation.catalog_type != "unity" {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Managed Iceberg tables are only supported in Unity Catalog. Set 'use_uniform' adapter property to true for Hive Metastore.",
            ));
        }

        Ok(use_uniform)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/4dc395b42dae78e895adf9c66ad6811534e879a6/dbt-athena/src/dbt/adapters/athena/impl.py#L445
    fn generate_unique_temporary_table_suffix(
        &self,
        suffix_initial: Option<String>,
    ) -> AdapterResult<String> {
        let suffix_initial = suffix_initial.as_deref().unwrap_or("__dbt_tmp");
        let uuid_str = Uuid::new_v4().to_string().replace('-', "_");
        Ok(format!("{suffix_initial}_{uuid_str}"))
    }

    /// Given a relation, fetch its configurations from the remote data warehouse
    /// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L797
    fn get_relation_config(
        &self,
        state: &State,
        conn: &mut dyn Connection,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Arc<dyn BaseRelationConfig>> {
        let relation_type = relation.relation_type().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "relation_type is required for the input relation of adapter.get_relation_config",
            )
        })?;
        let result: Arc<dyn BaseRelationConfig> = match relation_type {
            RelationType::Table => Arc::new(self.get_from_relation::<IncrementalTableConfig>(
                state,
                conn,
                relation.clone(),
            )?) as Arc<dyn BaseRelationConfig>,
            RelationType::View => {
                Arc::new(self.get_from_relation::<ViewConfig>(state, conn, relation.clone())?)
                    as Arc<dyn BaseRelationConfig>
            }
            RelationType::MaterializedView => Arc::new(
                self.get_from_relation::<MaterializedViewConfig>(state, conn, relation.clone())?,
            ) as Arc<dyn BaseRelationConfig>,
            RelationType::StreamingTable => Arc::new(
                self.get_from_relation::<StreamingTableConfig>(state, conn, relation.clone())?,
            ) as Arc<dyn BaseRelationConfig>,
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!("Unsupported relation type: {relation_type:?}"),
                ));
            }
        };
        Ok(result)
    }

    fn build_catalog_relation(&self, model: &Value) -> AdapterResult<CatalogRelation> {
        CatalogRelation::from_model_config_and_catalogs(
            &self.adapter_type(),
            model,
            load_catalogs::fetch_catalogs(),
        )
    }

    /// Given a model, parse and build its configurations
    /// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L810
    fn get_config_from_model(&self, model: &dyn InternalDbtNodeAttributes) -> AdapterResult<Value> {
        let result: Arc<dyn DatabricksRelationConfigBase> = match model.materialized() {
            DbtMaterialization::Incremental => {
                Arc::new(get_from_relation_config::<IncrementalTableConfig>(model)?)
                    as Arc<dyn DatabricksRelationConfigBase>
            }
            DbtMaterialization::MaterializedView => {
                Arc::new(get_from_relation_config::<MaterializedViewConfig>(model)?)
                    as Arc<dyn DatabricksRelationConfigBase>
            }
            DbtMaterialization::StreamingTable => {
                Arc::new(get_from_relation_config::<StreamingTableConfig>(model)?)
                    as Arc<dyn DatabricksRelationConfigBase>
            }
            DbtMaterialization::View => Arc::new(get_from_relation_config::<ViewConfig>(model)?)
                as Arc<dyn DatabricksRelationConfigBase>,
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!(
                        "Unsupported materialization type: {:?}",
                        model.materialized()
                    ),
                ));
            }
        };
        let result = DatabricksRelationConfigBaseObject::new(result);
        Ok(Value::from_object(result))
    }

    fn get_column_tags_from_model(
        &self,
        model: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Value> {
        use crate::relation::databricks::base::DatabricksComponentProcessor;
        use crate::relation::databricks::column_tags::{ColumnTagsConfig, ColumnTagsProcessor};
        use std::collections::BTreeMap;

        let processor = ColumnTagsProcessor;
        if let Some(DatabricksComponentConfig::ColumnTags(column_tags)) =
            processor.from_relation_config(model)?
        {
            let value = Value::from_serialize(&column_tags);
            return Ok(value);
        }

        Ok(Value::from_serialize(
            ColumnTagsConfig::new(BTreeMap::new()),
        ))
    }

    /// Given existing columns and columns from our model
    /// we determine which columns to update and persist docs for
    fn get_persist_doc_columns(
        &self,
        existing_columns: Vec<Column>,
        model_columns: IndexMap<String, dbt_schemas::schemas::dbt_column::DbtColumnRef>,
    ) -> AdapterResult<IndexMap<String, dbt_schemas::schemas::dbt_column::DbtColumnRef>> {
        // TODO(jasonlin45): grab comment info as well - we should avoid persisting for comments that are the same for performance reasons
        let mut result = IndexMap::new();
        // Intersection of existing columns and model columns that have descriptions
        for existing_col in existing_columns {
            if let Some(model_col) = model_columns.get(existing_col.name())
                && model_col.description.is_some()
            {
                result.insert(existing_col.name().to_string(), model_col.clone());
            }
        }
        Ok(result)
    }

    /// https://github.com/databricks/dbt-databricks/blob/4d82bd225df81296165b540d34ad5be43b45e44a/dbt/adapters/databricks/impl.py#L831
    /// TODO: implement if necessary, currently its noop
    fn clean_sql(&self, sql: &str) -> AdapterResult<String> {
        Ok(sql.to_string())
    }

    /// https://github.com/databricks/dbt-databricks/blob/66f513b960c62ee21c4c399264a41a56853f3d82/dbt/adapters/databricks/impl.py#L717
    fn redact_credentials(&self, sql: &str) -> AdapterResult<String> {
        let Some(caps) = CREDENTIAL_IN_COPY_INTO_REGEX.captures(sql) else {
            // WARN: Malformed input by user means credentials may leak.
            // However, this _is_ the fallback strategy implemented in Python.
            return Ok(sql.to_string());
        };

        // Capture the full matched credential(...) string, including the surrounding parentheses.
        // Then extract only the inner key-value content
        let full_parens = caps.get(1).unwrap().as_str();
        let inner = &full_parens[1..full_parens.len() - 1];

        let redacted_pairs = inner
            .split(',')
            .map(|pair| {
                let key = pair.split('=').next().unwrap_or("").trim();
                format!("{key} = '[REDACTED]'")
            })
            .collect::<Vec<_>>()
            .join(", ");

        let redacted_sql = sql.replacen(full_parens, &format!("({redacted_pairs})"), 1);

        Ok(redacted_sql)
    }
}
