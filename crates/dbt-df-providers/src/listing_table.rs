//! Utilities for building DataFusion listing table providers with dbt semantics.

use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl},
    execution::options::ReadOptions,
    prelude::{NdJsonReadOptions, ParquetReadOptions},
};
use datafusion_catalog::Session;
use datafusion_common::DataFusionError;
use dbt_adapter_core::AdapterType;
use std::fs::File;
use std::io::BufReader;
use std::{path::Path, sync::Arc};
/// Supported on-disk table formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableFormat {
    Parquet,
    Csv,
    Json,
}

/// Strategies for normalizing inferred column names.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum InferColumnNameStrategy {
    Verbatim,
    Uppercase,
    Lowercase,
}

/// Creates a [`ListingTable`] by inferring the schema from the files on disk.
///
/// Column names are normalized using the provided [`InferColumnNameStrategy`],
/// mirroring dbt's seed-loading semantics.
pub async fn make_listing_table_provider(
    ctx: &dyn Session,
    table_path: &Path,
    table_format: TableFormat,
) -> Result<Arc<ListingTable>, DataFusionError> {
    let listing_options = match table_format {
        TableFormat::Parquet => {
            ParquetReadOptions::new().to_listing_options(ctx.config(), ctx.table_options().clone())
        }
        TableFormat::Csv => {
            return Err(DataFusionError::Internal(
                "TableFormat::Csv is not supported in make_listing_table_provider".to_string(),
            ));
        }
        TableFormat::Json => NdJsonReadOptions::default()
            .to_listing_options(ctx.config(), ctx.table_options().clone()),
    };
    let (table_path, schema) =
        infer_schema_for_listing_options(ctx, table_path, &listing_options).await?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);
    let provider = Arc::new(ListingTable::try_new(config).unwrap());
    Ok(provider)
}

/// Read a seed file's Arrow schema **without** constructing a DataFusion
/// `SessionContext` — parquet from the file footer, JSON via `arrow_json`'s
/// schema inference. Useful for seed registration (LSP / static-analysis
/// builds) where we need the schema for the binder but don't want to pull
/// the `datafusion` umbrella into the caller's dep graph.
///
/// The returned schema matches what [`make_listing_table_provider`] would
/// produce for the same file: for parquet we apply the same Utf8/Binary →
/// view-type transform that DataFusion's default `ParquetFormat` applies
/// (schema_force_view_types=true), so the schema we register upfront agrees
/// with the batches that `scan_seed_file_arrow` later streams into the data
/// store.
pub fn read_listing_schema(
    path: &Path,
    table_format: TableFormat,
) -> Result<Arc<Schema>, DataFusionError> {
    match table_format {
        TableFormat::Parquet => {
            let file = File::open(path).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Arc::new(transform_schema_to_view(builder.schema())))
        }
        TableFormat::Json => {
            let file = File::open(path).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut reader = BufReader::new(file);
            let (schema, _) = arrow::json::reader::infer_json_schema(&mut reader, None)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Arc::new(schema))
        }
        TableFormat::Csv => Err(DataFusionError::Internal(
            "TableFormat::Csv is not supported in read_listing_schema".to_string(),
        )),
    }
}

/// Port of `datafusion::datasource::file_format::parquet::transform_schema_to_view`.
/// Rewrites Utf8/LargeUtf8 as Utf8View and Binary/LargeBinary as BinaryView so
/// our compute-free schema inference agrees with what DataFusion's
/// `ParquetFormat::infer_schema` produces (given `schema_force_view_types=true`
/// in the default session config).
fn transform_schema_to_view(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => field_with_new_type(field, DataType::Utf8View),
            DataType::Binary | DataType::LargeBinary => {
                field_with_new_type(field, DataType::BinaryView)
            }
            _ => Arc::clone(field),
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn field_with_new_type(field: &Arc<Field>, new_type: DataType) -> Arc<Field> {
    Arc::new(field.as_ref().clone().with_data_type(new_type))
}

async fn infer_schema_for_listing_options(
    ctx: &dyn Session,
    table_path: &Path,
    listing_options: &datafusion::datasource::listing::ListingOptions,
) -> Result<(ListingTableUrl, Arc<Schema>), DataFusionError> {
    let table_url = ListingTableUrl::parse(table_path.to_string_lossy().as_ref())?;
    let schema = listing_options.infer_schema(ctx, &table_url).await?;
    Ok((table_url, schema))
}

pub fn adapt_schema(
    schema: Arc<Schema>,
    infer_column_name_strategy: InferColumnNameStrategy,
) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                // dbt always trims the field name
                let field_name = field.name().trim();
                let new_name = match infer_column_name_strategy {
                    InferColumnNameStrategy::Verbatim => field_name.to_string(),
                    InferColumnNameStrategy::Uppercase => field_name.to_uppercase(),
                    InferColumnNameStrategy::Lowercase => field_name.to_lowercase(),
                };
                Arc::new((**field).clone().with_name(new_name))
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}
/// Computes the appropriate [`InferColumnNameStrategy`] for seeds given dbt
/// configuration flags and dialect-specific casing rules.
pub fn infer_seed_column_name_strategy(
    quote_columns: bool,
    adapter_type: AdapterType,
) -> InferColumnNameStrategy {
    match (quote_columns, adapter_type) {
        // In Trino, all names are lowercase, even quoted.
        (true, _) => InferColumnNameStrategy::Verbatim,
        (
            false,
            AdapterType::Postgres
            | AdapterType::Salesforce
            | AdapterType::Redshift
            | AdapterType::DuckDB,
        ) => InferColumnNameStrategy::Lowercase,
        (false, AdapterType::Snowflake) => InferColumnNameStrategy::Uppercase,
        (
            false,
            AdapterType::Bigquery
            | AdapterType::Databricks
            | AdapterType::Spark
            | AdapterType::Fabric,
        ) => InferColumnNameStrategy::Verbatim,
        (false, AdapterType::ClickHouse) => todo!("ClickHouse"),
        (false, AdapterType::Exasol) => InferColumnNameStrategy::Uppercase,
        (false, AdapterType::Starburst) => todo!("Starburst"),
        (false, AdapterType::Athena) => todo!("Athena"),
        (false, AdapterType::Trino) => todo!("Trino"),
        (false, AdapterType::Dremio) => todo!("Dremio"),
        (false, AdapterType::Oracle) => todo!("Oracle"),
        (false, AdapterType::Datafusion) => todo!("Datafusion"),
    }
}
