//! Utilities for building DataFusion listing table providers with dbt semantics.

use arrow_schema::Schema;
use datafusion::{
    datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl},
    execution::options::ReadOptions,
    prelude::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions},
};
use datafusion_catalog::Session;
use datafusion_common::DataFusionError;
use dbt_common::adapter::AdapterType;
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
    delimiter: Option<char>,
) -> Result<Arc<ListingTable>, DataFusionError> {
    let listing_options = match table_format {
        TableFormat::Parquet => {
            ParquetReadOptions::new().to_listing_options(ctx.config(), ctx.table_options().clone())
        }
        TableFormat::Csv => {
            let mut csv_read_options = CsvReadOptions::new();
            // Handles case-insensitivity of csv file extensions
            let path = Path::new(table_path);
            csv_read_options = if let Some(extension) = path.extension() {
                csv_read_options.file_extension(extension.to_str().unwrap_or_default())
            } else {
                csv_read_options
            };
            csv_read_options = csv_read_options.delimiter_option(delimiter.map(|x| x as u8));
            csv_read_options.to_listing_options(ctx.config(), ctx.table_options().clone())
        }
        TableFormat::Json => NdJsonReadOptions::default()
            .to_listing_options(ctx.config(), ctx.table_options().clone()),
    };
    let csv_fallback = match table_format {
        TableFormat::Csv => Some(CsvInferFallback {
            has_header: true,
            delimiter: delimiter.map(|d| d as u8).unwrap_or(b','),
            quote: b'"',
            escape: None,
            terminator: None,
            comment: None,
        }),
        _ => None,
    };
    let (table_path, schema) =
        infer_schema_for_listing_options(ctx, table_path, &listing_options, csv_fallback).await?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);
    let provider = Arc::new(ListingTable::try_new(config).unwrap());
    Ok(provider)
}

#[derive(Debug, Clone, Copy)]
struct CsvInferFallback {
    has_header: bool,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    terminator: Option<u8>,
    comment: Option<u8>,
}

fn is_csv_unequal_lengths_error(err: &DataFusionError) -> bool {
    let s = err.to_string();
    s.contains("Error when processing CSV file")
        && (s.contains("Encountered unequal lengths between records")
            || s.contains("unequal lengths between records"))
}

async fn infer_schema_for_listing_options(
    ctx: &dyn Session,
    table_path: &Path,
    listing_options: &datafusion::datasource::listing::ListingOptions,
    csv_fallback: Option<CsvInferFallback>,
) -> Result<(ListingTableUrl, Arc<Schema>), DataFusionError> {
    let table_url = ListingTableUrl::parse(table_path.to_string_lossy().as_ref())?;
    match listing_options.infer_schema(ctx, &table_url).await {
        Ok(schema) => Ok((table_url, schema)),
        Err(err) => {
            if let Some(fallback) = csv_fallback
                && is_csv_unequal_lengths_error(&err)
            {
                // Workaround: avoid DataFusion's chunked CSV schema inference and instead
                // infer schema from a single reader using Arrow's CSV reader.
                let f = std::fs::File::open(table_path)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let mut format = datafusion::arrow::csv::reader::Format::default()
                    .with_header(fallback.has_header)
                    .with_delimiter(fallback.delimiter)
                    .with_quote(fallback.quote);
                if let Some(escape) = fallback.escape {
                    format = format.with_escape(escape);
                }
                if let Some(comment) = fallback.comment {
                    format = format.with_comment(comment);
                }
                if let Some(terminator) = fallback.terminator {
                    format = format.with_terminator(terminator);
                }

                let (schema, _records_read) = format
                    .infer_schema(f, None)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok((table_url, Arc::new(schema)))
            } else {
                Err(err)
            }
        }
    }
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
            | AdapterType::Fabric
            | AdapterType::Sidecar,
        ) => InferColumnNameStrategy::Verbatim,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    /// Repro: DataFusion CSV schema inference can fail on multiline quoted CSVs
    /// with "unequal lengths between records".
    #[test]
    fn repro_infer_schema_multiline_csv_unequal_lengths() {
        let seed_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/repro_multiline_seed.csv");
        assert!(
            seed_path.exists(),
            "seed file does not exist at {:?}",
            seed_path
        );

        let ctx = SessionContext::new();

        // Copy the CSV branch of `make_listing_table_provider`, but stop after `infer_schema`.
        let mut csv_read_options = CsvReadOptions::new().has_header(true);
        let path = seed_path.as_path();
        csv_read_options = if let Some(extension) = path.extension() {
            csv_read_options.file_extension(extension.to_str().unwrap_or_default())
        } else {
            csv_read_options
        };
        csv_read_options = csv_read_options.delimiter_option(Some(b','));

        let listing_options = csv_read_options
            .to_listing_options(ctx.state().config(), ctx.state().table_options().clone());

        let (table_url, schema) = futures::executor::block_on(async {
            infer_schema_for_listing_options(
                &ctx.state(),
                seed_path.as_path(),
                &listing_options,
                Some(CsvInferFallback {
                    has_header: true,
                    delimiter: b',',
                    quote: b'"',
                    escape: None,
                    terminator: None,
                    comment: None,
                }),
            )
            .await
            .expect("expected infer_schema to succeed for this repro CSV")
        });

        assert_eq!(
            schema.fields().len(),
            17,
            "expected inferred schema to have 17 columns (url={table_url})"
        );
    }
}
