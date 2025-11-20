// TODO: remove this once the TODO: implement this items are completed in this mod
#![allow(dead_code)]

use std::sync::Arc;

use crate::databricks::adapter::DatabricksAdapter;
use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksRelationResultsBuilder, from_results,
};
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::query_ctx::query_ctx_from_state;
use crate::{AdapterResponse, AdapterResult, TypedBaseAdapter};
use dbt_agate::AgateTable;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::Connection;
use minijinja::State;

pub mod base;
pub mod column_comments;
pub mod column_tags;
pub mod comment;
pub mod constraints;
pub mod liquid_clustering;
pub mod partitioning;
pub mod query;
pub mod refresh;
pub mod tags;
pub mod tblproperties;

mod configs;
pub use configs::*;

pub mod relation_api;

/// Databricks specific relation config fetching logic
impl DatabricksAdapter {
    // todo: serialize agate table results into SA structs for compile time safety and better error messages
    /// Given the relation, fetch its config from the remote data warehouse
    /// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L871
    pub fn get_from_relation<T: std::fmt::Debug + DatabricksRelationConfig>(
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
