use std::sync::LazyLock;

use crate::AdapterResult;
use crate::columns::{BigqueryColumnMode, StdColumn};
use crate::metadata;
use crate::sql_types::{self, TypeOps};
use arrow_schema::{DataType, FieldRef};
use dbt_common::adapter::AdapterType;
use regex::Regex;

pub struct ColumnBuilder {
    adapter_type: AdapterType,
}

impl ColumnBuilder {
    pub fn new(adapter_type: AdapterType) -> Self {
        Self { adapter_type }
    }

    pub fn build(&self, field: &FieldRef, type_ops: &dyn TypeOps) -> AdapterResult<StdColumn> {
        use AdapterType::*;
        match self.adapter_type {
            Snowflake => Ok(Self::build_snowflake(field, type_ops)),
            Bigquery => Ok(Self::build_bigquery(field, type_ops)),
            Databricks => Ok(Self::build_databricks(field, type_ops)),
            Redshift => Ok(Self::build_redshift(field, type_ops)),
            Postgres | Salesforce => Ok(Self::build_postgres_like(field, type_ops)),
        }
    }

    pub fn build_from_parts(
        &self,
        name: String,
        dtype: String,
        char_size: Option<u32>,
        numeric_precision: Option<u64>,
        numeric_scale: Option<u64>,
        mode: Option<BigqueryColumnMode>,
    ) -> StdColumn {
        use AdapterType::*;
        match self.adapter_type {
            Postgres => StdColumn::new(
                Postgres,
                name,
                dtype,
                char_size,
                numeric_precision,
                numeric_scale,
            ),
            Snowflake => StdColumn::new(
                Snowflake,
                name,
                dtype,
                char_size,
                numeric_precision,
                numeric_scale,
            ),
            // TODO: BigQuery fields
            Bigquery => StdColumn::new_bigquery(name, dtype, &[], mode.unwrap()),
            Redshift => StdColumn::new(
                Redshift,
                name,
                dtype,
                char_size,
                numeric_precision,
                numeric_scale,
            ),
            Databricks => StdColumn::new(
                Databricks, name, dtype, char_size, None, // numeric_precision
                None, // numeric_scale
            ),
            Salesforce => todo!("Salesforce column creation not implemented yet"),
        }
    }

    fn build_snowflake(field: &FieldRef, type_ops: &dyn TypeOps) -> StdColumn {
        use AdapterType::Snowflake;
        use sql_types::snowflake::*;

        // XXX: the code here is messy because it's the result of porting logic bug by bug
        // from a previous implementation. It can be greatly simplified and it will be.
        let data_type = field.data_type();
        let mut char_size = sql_types::var_size(Snowflake, data_type);

        // XXX: errors are ignored
        let (mut numeric_precision, mut numeric_scale) = {
            let precision_scale = sql_types::numeric_precision_scale(Snowflake, data_type)
                .ok()
                .flatten();
            match precision_scale {
                Some((p, Some(s))) => (Some(p), Some(s)),
                Some((p, None)) => (Some(p), None),
                None => (None, None),
            }
        };

        let mut type_name_or_formatted = String::new();
        if type_ops
            .format_arrow_type_as_sql(data_type, &mut type_name_or_formatted)
            .is_err()
        {
            // TODO this is for sure wrong type. We should rather propagate error here
            type_name_or_formatted = data_type.to_string();
            char_size = None;
            numeric_precision = None;
            numeric_scale = None;
        }
        let mut dtype = type_name_or_formatted.clone();

        static PRECISION_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"\(.*?\)$").unwrap());
        match data_type {
            DataType::Decimal128(_, _) => {
                dtype.clear();
                dtype.push_str("NUMBER");
            }
            // Snowflake: For timestamp/date/time types, extract precision if available
            dt if is_time(dt).is_yes() => {
                dtype.clear();
                dtype.push_str("TIME");
            }
            dt if is_timestamp_ntz(dt).is_yes()
                || is_timestamp_ltz(dt).is_yes()
                || is_timestamp_tz(dt).is_yes()
                || matches!(dt, DataType::Timestamp(_, _)) =>
            {
                dtype.clear();
                dtype.push_str(
                    PRECISION_REGEX
                        .replace(&type_name_or_formatted, "")
                        .as_ref(),
                )
            }

            _ => {}
        }

        // HACK(jason): the frontend does not provide character size, parse it out of the type ourselves if available
        let mut resolved_char_size = char_size;
        match data_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                // Extract size from metadata if present
                if let Some(char_size) = field
                    .metadata()
                    .get(metadata::snowflake::ARROW_FIELD_SNOWFLAKE_FIELD_WIDTH_METADATA_KEY)
                {
                    resolved_char_size = char_size
                        .parse::<usize>()
                        .ok()
                        .or(sql_types::max_varchar_size(Snowflake));
                }
            }
            DataType::Binary => {
                if let Some(char_size) = field
                    .metadata()
                    .get(metadata::snowflake::ARROW_FIELD_SNOWFLAKE_FIELD_WIDTH_METADATA_KEY)
                {
                    resolved_char_size = char_size
                        .parse::<usize>()
                        .ok()
                        .or(sql_types::max_varbinary_size(Snowflake));
                }
            }
            _ => {}
        }

        StdColumn::new(
            Snowflake,
            field.name().to_string(),
            dtype,
            resolved_char_size.map(|p| p as u32),
            numeric_precision.map(|p| p as u64),
            numeric_scale.map(|s| s as u64),
        )
    }

    /// The logic from `get_column_schema_from_query` for BigQuery [1].
    ///
    /// [1] https://github.com/dbt-labs/dbt-adapters/blob/c16cc7047e8678f8bb88ae294f43da2c68e9f5cc/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L444
    fn build_bigquery(field: &FieldRef, type_ops: &dyn TypeOps) -> StdColumn {
        let mut data_type = String::new();
        if type_ops
            .format_arrow_type_as_sql(field.data_type(), &mut data_type)
            .is_err()
        {
            // desperate fallback
            data_type = field.data_type().to_string();
        }
        let mode = match field.is_nullable() {
            true => BigqueryColumnMode::Nullable,
            false => {
                if matches!(
                    field.data_type(),
                    DataType::List(..)
                        | DataType::ListView(..)
                        | DataType::FixedSizeList(..)
                        | DataType::LargeList(..)
                        | DataType::LargeListView(..)
                ) {
                    BigqueryColumnMode::Repeated
                } else {
                    BigqueryColumnMode::Required
                }
            }
        };

        let inner_columns = match field.data_type() {
            DataType::List(inner)
            | DataType::ListView(inner)
            | DataType::FixedSizeList(inner, _)
            | DataType::LargeList(inner)
            | DataType::LargeListView(inner) => {
                match inner.data_type() {
                    // only structs can have named fields
                    DataType::Struct(inner) => inner
                        .into_iter()
                        .map(|f| Self::build_bigquery(f, type_ops))
                        .collect(),
                    // we don't need to worry about nested lists because that is not supported by
                    // BigQuery
                    _ => Vec::new(),
                }
            }
            DataType::Struct(fields) => fields
                .into_iter()
                .map(|f| Self::build_bigquery(f, type_ops))
                .collect(),
            _ => Vec::new(),
        };

        StdColumn::new_bigquery(field.name().to_string(), data_type, inner_columns, mode)
    }

    fn build_databricks(field: &FieldRef, type_ops: &dyn TypeOps) -> StdColumn {
        let name = field.name().to_string();
        let type_text = {
            // TODO(jason): This needs to be updated to match the driver convention once available
            let type_text = field
                .metadata()
                .get(metadata::ARROW_FIELD_ORIGINAL_TYPE_METADATA_KEY);
            if let Some(type_text) = type_text {
                type_text.to_owned()
            } else {
                let mut type_text = String::new();
                type_ops
                    .format_arrow_type_as_sql(field.data_type(), &mut type_text)
                    .unwrap();
                if !field.is_nullable() {
                    type_text.push_str(" not null");
                }
                type_text
            }
        };
        StdColumn::new(
            AdapterType::Databricks,
            name,
            type_text,
            None, // char_size
            None, // numeric_precision
            None, // numeric_scale
        )
    }

    fn build_postgres_like(field: &FieldRef, type_ops: &dyn TypeOps) -> StdColumn {
        let mut data_type = String::new();
        match field.data_type() {
            // Mimic broken conversion that was here before just in case
            // something depends on it.
            // TODO: remove this broken formatting behavior
            DataType::Timestamp(_, _) | DataType::Time64(_) => data_type.push_str("datetime"),
            _ => {
                type_ops
                    .format_arrow_type_as_sql(field.data_type(), &mut data_type)
                    .unwrap();
            }
        }
        if !field.is_nullable() {
            data_type.push_str(" not null");
        }
        StdColumn::new(
            AdapterType::Postgres,
            field.name().to_string(),
            data_type,
            None, // char_size
            None, // numeric_precision
            None, // numeric_scale
        )
    }

    fn build_redshift(field: &FieldRef, type_ops: &dyn TypeOps) -> StdColumn {
        use AdapterType::Redshift;
        let data_type = field.data_type();
        let char_size = sql_types::var_size(Redshift, data_type);
        // XXX: errors are ignored
        let (numeric_precision, numeric_scale) = {
            let precision_scale = sql_types::numeric_precision_scale(Redshift, data_type)
                .ok()
                .flatten();
            match precision_scale {
                Some((p, Some(s))) => (Some(p), Some(s)),
                Some((p, None)) => (Some(p), None),
                None => (None, None),
            }
        };

        let mut type_name_or_formatted = String::new();
        if type_ops
            .format_arrow_type_as_sql(data_type, &mut type_name_or_formatted)
            .is_err()
        {
            // TODO: this is for sure wrong type. We should rather propagate error here
            type_name_or_formatted = data_type.to_string();
        }

        let base_type_name = if matches!(
            data_type,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        ) {
            "NUMERIC".to_string()
        } else {
            type_name_or_formatted
        };

        StdColumn::new(
            Redshift,
            field.name().to_string(),
            base_type_name, // dtype
            char_size.map(|p| p as u32),
            numeric_precision.map(|p| p as u64),
            numeric_scale.map(|s| s as u64),
        )
    }
}
