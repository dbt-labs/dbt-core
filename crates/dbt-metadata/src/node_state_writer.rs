use std::collections::HashMap;
use std::sync::Arc;

use dbt_common::constants::*;
use dbt_common::io_args::{EvalArgs, IoArgs};
use dbt_common::{ErrorCode, FsError, FsResult, fs_err, stdfs};
use dbt_metadata_parquet::utils::{write_parquet_file, write_parquet_with_batch};
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::schemas::legacy_catalog::DbtCatalog;
use dbt_schemas::state::{NodeStatus, ResolverState};

use crate::file_registry::CompleteStateWithKind;
use crate::parquet_cas::generate_cas_record_batch;
use crate::parquet_column::{
    ParquetColumnRef, generate_columns_arrow_schema, serialize_to_pcolumns,
};
use crate::parquet_node::{
    WriteContext, collect_projects, generate_parquet_nodes, generate_pnode_arrow_schema,
};
use crate::parquet_project::{ParquetProjectRef, generate_projects_arrow_schema};

pub struct NodeStateWriter<'a> {
    pub io: &'a IoArgs,
    pub ctx: &'a WriteContext,
    pub eval_args: &'a EvalArgs,
    pub resolver_state: &'a ResolverState,
    pub registry: &'a CompleteStateWithKind,
    pub unchanged_node_statuses: &'a HashMap<String, NodeStatus>,
    pub catalog: Option<&'a DbtCatalog>,
    pub cloud_config: &'a Option<ResolvedCloudConfig>,
    pub dir_creation_attempted: bool,
}

impl NodeStateWriter<'_> {
    pub fn ensure_dir_exists(&mut self) -> FsResult<()> {
        if self.dir_creation_attempted {
            return Ok(());
        }
        self.dir_creation_attempted = true;
        stdfs::create_dir_all(self.io.out_dir.join(DBT_METADATA_DIR_NAME)).map_err(|e| {
            FsError::new(
                ErrorCode::IoError,
                format!("Failed to create metadata directory: {e}"),
            )
        })?;
        Ok(())
    }

    fn serialize_parquet_columns(&self) -> FsResult<Vec<ParquetColumnRef>> {
        let enabled_nodes = serialize_to_pcolumns(&self.resolver_state.nodes, self.catalog)?;
        let disabled_nodes =
            serialize_to_pcolumns(&self.resolver_state.disabled_nodes, self.catalog)?;

        let mut result = Vec::new();
        result.extend(enabled_nodes);
        result.extend(disabled_nodes);
        Ok(result)
    }

    pub fn write_columns(&mut self) -> FsResult<()> {
        self.ensure_dir_exists()?;

        let parquet_columns = self.serialize_parquet_columns()?;
        write_parquet_file(
            self.io,
            "columns.parquet",
            COLUMNS_WR,
            generate_columns_arrow_schema(),
            parquet_columns,
        )
        .map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to write metadata columns to parquet: {e}",
            )
        })?;
        Ok(())
    }

    pub fn write_projects(&mut self) -> FsResult<()> {
        self.ensure_dir_exists()?;

        let project_refs: Vec<ParquetProjectRef> =
            collect_projects(self.resolver_state, self.cloud_config)
                .into_iter()
                .map(Arc::new)
                .collect();
        write_parquet_file(
            self.io,
            "projects.parquet",
            NODES_WR,
            generate_projects_arrow_schema(),
            project_refs,
        )
        .map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to write metadata projects to parquet: {e}",
            )
        })?;
        Ok(())
    }

    pub fn write_nodes_and_cas(&mut self) -> FsResult<()> {
        self.ensure_dir_exists()?;

        // Generate parquet_nodes and CAS
        let (parquet_nodes, cas) = generate_parquet_nodes(
            self.io,
            self.ctx,
            self.eval_args,
            self.resolver_state,
            self.registry,
            self.unchanged_node_statuses,
            self.cloud_config,
        )?;

        // Write nodes.parquet
        write_parquet_file(
            self.io,
            "nodes.parquet",
            NODES_WR,
            generate_pnode_arrow_schema(),
            parquet_nodes,
        )
        .map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to write metadata nodes to parquet: {e}",
            )
        })?;

        // Write CAS metadata
        write_parquet_with_batch(self.io, "cas.parquet", CAS_WR, || {
            Ok(generate_cas_record_batch(&cas))
        })
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to write CAS metadata: {e}"))?;

        Ok(())
    }
}
