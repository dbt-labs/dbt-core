use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use crate::AdapterResult;
use crate::base_adapter::backend_of;
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::metadata::snowflake::ARROW_FIELD_SNOWFLAKE_FIELD_WIDTH_METADATA_KEY;
use crate::metadata::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use dbt_common::adapter::AdapterType;
use dbt_xdbc::sql::types::SqlType;

// TODO: Add keys here as necessary
pub const REDSHIFT_METADATA_SQL_TYPE_KEY: &str = "Type";
pub const BIGQUERY_METADATA_SQL_TYPE_KEY: &str = "Type";
// XXX: Snowflake does DATA_TYPE for GetTableSchema and SNOWFLAKE_TYPE for other queries...
pub const SNOWFLAKE_METADATA_SQL_TYPE_KEY: &str = "DATA_TYPE";

/// An Arrow schema containing SDF types
#[derive(Clone)]
pub struct SdfSchema {
    original: Option<Arc<Schema>>,
    schema: Arc<Schema>,
}

impl SdfSchema {
    /// Creates a new SdfSchema from a transformed Arrow schema.
    ///
    /// PRE-CONDITION: the schema must have been transformed to use SDF types.
    /// All types have been converted to types that static analysis expects
    /// and all the canonicalization steps have been applied (e.g. the
    /// `FixedSizeList` hack for Snowflake timestamps)
    pub fn from_sdf_arrow_schema(original: Option<Arc<Schema>>, schema: Arc<Schema>) -> Self {
        SdfSchema { original, schema }
    }

    pub fn inner(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn into_inner(self) -> Arc<Schema> {
        self.schema
    }

    pub fn original(&self) -> Option<&Arc<Schema>> {
        self.original.as_ref()
    }
}

pub trait TypeOps: Send + Sync {
    /// Returns the adapter type this [TypeOps] instance is for.
    fn adapter_type(&self) -> AdapterType;

    /// Picks a SQL type for a given Arrow DataType and renders it as SQL.
    ///
    /// The implementation is dialect-specific.
    fn format_arrow_type_as_sql(&self, data_type: &DataType, out: &mut String)
    -> AdapterResult<()>;

    /// Renders a given SqlType as SQL.
    ///
    /// The implementation is dialect-specific.
    fn format_sql_type(&self, sql_type: SqlType, out: &mut String) -> AdapterResult<()>;

    fn parse_into_nullable_arrow_type(&self, s: &str) -> AdapterResult<(DataType, bool)>;

    fn parse_into_arrow_type(&self, s: &str) -> AdapterResult<DataType> {
        self.parse_into_nullable_arrow_type(s).map(|(dt, _)| dt)
    }
}

pub fn parse_nullable_sql_type(
    s: &str,
    adapter_type: AdapterType,
) -> AdapterResult<(SqlType, bool)> {
    let backend = backend_of(adapter_type);
    SqlType::parse(backend, s).map_err(|e| AdapterError::new(AdapterErrorKind::UnexpectedResult, e))
}

pub struct NaiveTypeOpsImpl(AdapterType, dbt_xdbc::Backend);

impl NaiveTypeOpsImpl {
    pub fn new(adapter_type: AdapterType) -> Self {
        let backend = backend_of(adapter_type);
        Self(adapter_type, backend)
    }
}

impl TypeOps for NaiveTypeOpsImpl {
    fn adapter_type(&self) -> AdapterType {
        self.0
    }

    fn format_arrow_type_as_sql(
        &self,
        data_type: &DataType,
        out: &mut String,
    ) -> AdapterResult<()> {
        let adapter_type = self.0;
        match adapter_type {
            AdapterType::Postgres | AdapterType::Salesforce => {
                postgres::try_format_type(data_type, true, out)
            }
            _ => {
                let hint: SqlTypeHint = data_type.try_into()?;
                // TODO: handle has_decimal_places correctly
                let has_decimal_places = false;
                let res = sql_type_hint_to_str(hint, has_decimal_places, adapter_type);
                out.push_str(res.as_ref());
                Ok(())
            }
        }
    }

    fn format_sql_type(&self, sql_type: SqlType, out: &mut String) -> AdapterResult<()> {
        let backend = self.1;
        sql_type.write(backend, out).map_err(|e| {
            AdapterError::new(
                AdapterErrorKind::NotSupported,
                format!("Failed to convert SQL type {sql_type:?}. Error: {e}"),
            )
        })
    }

    fn parse_into_nullable_arrow_type(&self, _s: &str) -> AdapterResult<(DataType, bool)> {
        todo!("NaiveTypeOpsImpl::parse_into_nullable_arrow_type")
    }
}

/// Replacement for `new_arrow_field_with_metadata` that fuses parsing and `Field` creation.
#[inline]
pub fn make_arrow_field(
    type_ops: &dyn TypeOps,
    col_name: String,
    sql_type_str: &str,
    nullable_override: Option<bool>,
    comment: Option<String>,
) -> Result<Field, AdapterError> {
    make_arrow_field_v1(type_ops, col_name, sql_type_str, nullable_override, comment)
}

/// Implementation of [make_arrow_field] that is currently in use.
pub fn make_arrow_field_v1(
    type_ops: &dyn TypeOps,
    col_name: String,
    sql_type_str: &str,
    nullable_override: Option<bool>,
    comment: Option<String>,
) -> Result<Field, AdapterError> {
    use SqlType::*;

    let (data_type, nullable) = type_ops.parse_into_nullable_arrow_type(sql_type_str)?;
    let field = Field::new(col_name, data_type, nullable_override.unwrap_or(nullable));

    let adapter_type = type_ops.adapter_type();
    let mut metadata = HashMap::new();
    metadata.insert(
        ARROW_FIELD_ORIGINAL_TYPE_METADATA_KEY.to_string(),
        sql_type_str.to_string(),
    );
    if let Some(comment) = comment {
        metadata.insert(ARROW_FIELD_COMMENT_METADATA_KEY.to_string(), comment);
    }

    // HACK: Insert the width of the field as its own value
    // Special handling for Snowflake char width fields
    // because these are given to the user as separate types
    if adapter_type == AdapterType::Snowflake {
        let sql_type_res = SqlType::parse(backend_of(adapter_type), sql_type_str).map(|(ty, _)| ty);
        match sql_type_res {
            Ok(Binary(Some(max_len))) | Ok(Varchar(Some(max_len), _)) => {
                metadata.insert(
                    ARROW_FIELD_SNOWFLAKE_FIELD_WIDTH_METADATA_KEY.to_string(),
                    max_len.to_string(),
                );
            }
            _ => (),
        }
    }

    let field = field.with_metadata(metadata);

    Ok(field)
}

/// The version we want to standardize on as we move away from using Arrow types
/// with SDF-isms encoded in them (e.g. FixedSizeList for timestamps).
pub fn make_arrow_field_v2(
    type_ops: &dyn TypeOps,
    col_name: String,
    sql_type_str: &str,
    nullable_override: Option<bool>,
    comment: Option<String>,
) -> Result<Field, AdapterError> {
    use AdapterType::*;
    use SqlType::*;
    let adapter_type = type_ops.adapter_type();
    let backend = backend_of(adapter_type);
    let (sql_type, nullable) = parse_nullable_sql_type(sql_type_str, adapter_type)?;
    let data_type = sql_type.pick_best_arrow_type(backend);

    let field = Field::new(col_name, data_type, nullable_override.unwrap_or(nullable));

    let mut metadata = HashMap::new();
    metadata.insert(
        ARROW_FIELD_ORIGINAL_TYPE_METADATA_KEY.to_string(),
        sql_type_str.to_string(),
    );
    if let Some(comment) = comment {
        metadata.insert(ARROW_FIELD_COMMENT_METADATA_KEY.to_string(), comment);
    }

    match (adapter_type, &sql_type) {
        (Snowflake, Varchar(Some(max_len), _)) | (Snowflake, Binary(Some(max_len))) => {
            metadata.insert(
                ARROW_FIELD_SNOWFLAKE_FIELD_WIDTH_METADATA_KEY.to_string(),
                max_len.to_string(),
            );
        }
        _ => {}
    }

    let field = field.with_metadata(metadata);

    Ok(field)
}

pub const fn get_field_sql_type_metadata_key(adapter_type: AdapterType) -> &'static str {
    match adapter_type {
        AdapterType::Bigquery => BIGQUERY_METADATA_SQL_TYPE_KEY,
        AdapterType::Redshift => REDSHIFT_METADATA_SQL_TYPE_KEY,
        AdapterType::Snowflake => SNOWFLAKE_METADATA_SQL_TYPE_KEY,
        AdapterType::Databricks => todo!(),
        AdapterType::Postgres => todo!(),
        AdapterType::Salesforce => todo!(),
    }
}

struct SdfSchemaBuilder {
    adapter_type: AdapterType,
    original: Arc<Schema>,
}

impl SdfSchemaBuilder {
    pub fn new(adapter_type: AdapterType, original: Arc<Schema>) -> Self {
        Self {
            adapter_type,
            original,
        }
    }

    fn convert_field(&self, type_ops: &dyn TypeOps, field: &Field) -> AdapterResult<Arc<Field>> {
        use AdapterType::*;
        match self.adapter_type {
            Bigquery => {
                let metadata = field.metadata();
                let current_type = field.data_type();
                let nullable = field.is_nullable();

                let maybe_original_type_text = bigquery::field_to_string(field);

                // XXX: We should probably error here rather than approximate
                let resolved_type = if let Some(ref original_type_text) = maybe_original_type_text {
                    type_ops
                        .parse_into_arrow_type(original_type_text)
                        .unwrap_or_else(|_| current_type.clone())
                } else {
                    current_type.clone()
                };

                // TODO: Comment handling for other adapters
                let comment = metadata.get("Description").map(|s| s.to_string());

                let field = new_arrow_field_with_metadata(
                    field.name(),
                    resolved_type,
                    nullable,
                    maybe_original_type_text,
                    comment,
                );
                Ok(Arc::new(field))
            }
            Redshift | Databricks => {
                let metadata = field.metadata();
                let current_type = field.data_type();
                let nullable = field.is_nullable();
                let original_type_text = metadata.get(ARROW_FIELD_ORIGINAL_TYPE_METADATA_KEY);
                let comment = metadata
                    .get(ARROW_FIELD_COMMENT_METADATA_KEY)
                    .map(|s| s.to_string());
                let resolved_type = if let Some(original_type_text) = original_type_text {
                    type_ops
                        .parse_into_arrow_type(original_type_text)
                        .unwrap_or_else(|_| current_type.clone())
                } else {
                    current_type.clone()
                };
                let field = new_arrow_field_with_metadata(
                    field.name(),
                    resolved_type,
                    nullable,
                    original_type_text.cloned(),
                    comment,
                );
                Ok(Arc::new(field))
            }
            // More adapters will be handled here when build_sdf_schema()
            // delegates to this function for more adapters.
            _ => unreachable!(),
        }
    }

    pub fn build_sdf_schema(self, type_ops: &dyn TypeOps) -> AdapterResult<SdfSchema> {
        use AdapterType::*;
        match self.adapter_type {
            Bigquery | Redshift | Databricks => {
                let original_fields = self.original.fields();
                let mut sdf_fields = Vec::with_capacity(original_fields.len());
                for field in original_fields {
                    let sdf_field = self.convert_field(type_ops, field)?;
                    sdf_fields.push(sdf_field);
                }
                // preserve original metadata
                let schema_metadata = self.original.metadata().clone();
                let sdf_arrow_schema =
                    Arc::new(Schema::new_with_metadata(sdf_fields, schema_metadata));
                // build the SdfSchema
                let sdf_schema =
                    SdfSchema::from_sdf_arrow_schema(Some(self.original), sdf_arrow_schema);
                Ok(sdf_schema)
            }
            Postgres | Snowflake | Salesforce => {
                // NOTE(felipecrv): this is not correct, but it's a temporary fallback
                // that allows us to call [to_sdf_arrow_schema] from anywhere.
                //
                // TODO: move conversion logic for other adapters here
                let sdf_arrow_schema = Arc::clone(&self.original);
                // build the SdfSchema
                let sdf_schema =
                    SdfSchema::from_sdf_arrow_schema(Some(self.original), sdf_arrow_schema);
                Ok(sdf_schema)
            }
        }
    }
}

/// Converts a regular Arrow Schema into an SDF Arrow Schema.
///
/// A regular Arrow Schema is one that may come from drivers or internal adapter
/// logic. It's free of any SDF-specific type encoding rules (e.g. `FixedSizeList`
/// hack for timestamps) which we can't expect to be present in these contexts.
///
/// Applies SDF-specific type encoding rules (e.g. `FixedSizeList` hack for timestamps).
pub fn arrow_schema_to_sdf_schema(
    src_schema: Arc<Schema>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<SdfSchema> {
    let builder = SdfSchemaBuilder::new(type_ops.adapter_type(), src_schema);
    builder.build_sdf_schema(type_ops)
}

pub enum SqlTypeHint {
    Integer,
    Floating,
    Decimal,
    Boolean,
    Datetime,
    Date,
    Time,
    Text,
}

impl TryFrom<&DataType> for SqlTypeHint {
    type Error = AdapterError;

    fn try_from(data_type: &DataType) -> Result<SqlTypeHint, Self::Error> {
        use SqlTypeHint::*;
        let hint = if data_type.is_null() {
            Text
        } else if data_type.is_integer() {
            Integer
        } else if data_type.is_floating() {
            Floating
        } else if data_type.is_numeric() {
            Decimal
        } else if *data_type == DataType::Boolean {
            Boolean
        } else if matches!(data_type, DataType::Timestamp(_, _)) {
            Datetime
        } else if matches!(data_type, DataType::Date32 | DataType::Date64) {
            Date
        } else if matches!(data_type, DataType::Duration(_) | DataType::Interval(_)) {
            Time
        } else if matches!(data_type, DataType::Utf8) {
            Text
        } else {
            return Err(AdapterError::new(
                AdapterErrorKind::NotSupported,
                format!("Unsupported data type {data_type:?}"),
            ));
        };
        Ok(hint)
    }
}

/// A function that replaces all `convert_{type}_type` functions [1].
///
/// The functions are:
/// - `convert_integer_type`
/// - `convert_number_type` (floating and decimal types)
/// - `convert_boolean_type`
/// - `convert_datetime_type`
/// - `convert_date_type`
/// - `convert_time_type`
/// - `convert_text_type`
///
/// Databricks uses the conversion rules from Spark [3].
///
/// [1] https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-adapters/src/dbt/adapters/sql/impl.py
/// [2] https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-bigquery/src/dbt/adapters/bigquery/impl.py
/// [3] https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-spark/src/dbt/adapters/spark/impl.py
pub fn sql_type_hint_to_str<'a>(
    hint: SqlTypeHint,
    _has_decimal_places: bool,
    adapter_type: AdapterType,
) -> Cow<'a, str> {
    use SqlTypeHint::*;
    use dbt_common::adapter::AdapterType::*;
    let str = match (adapter_type, hint) {
        // ## convert_integer_type()
        (Bigquery, Integer) => "int64",
        (Databricks, Integer) => "bigint",
        (_, Integer) => "integer",

        // ## convert_number_type()
        (Bigquery, Floating) => "int64", // TODO: fix to "float64" if has_decimal_places is true
        (Bigquery, Decimal) => "float64", // TODO: fix to "int64" if has_decimal_places is false
        (Databricks, Floating) => "bigint", // TODO: fix to "double" if has_decimal_places is true
        (Databricks, Decimal) => "double", // TODO: fix to "bigint" if has_decimal_places is false
        (_, Floating) => "integer",      // TODO: fix to "float8" if has_decimal_places is true
        (_, Decimal) => "float8",        // TODO: fix to "integer" if has_decimal_places is false

        // ## convert_boolean_type()
        (Bigquery, Boolean) => "bool",
        (_, Boolean) => "boolean",

        // ## convert_datetime_type()
        (Bigquery, Datetime) => "datetime",
        (Databricks, Datetime) => "timestamp",
        (_, Datetime) => "timestamp without time zone",

        // ## convert_date_type()
        (_, Date) => "date",

        // ## convert_time_type()
        (_, Time) => "time",

        // ## convert_text_type()
        (Bigquery | Databricks, Text) => "string",
        (_, Text) => "text",
    };
    Cow::Borrowed(str)
}

pub mod bigquery {
    // XXX: make private once all tests are moved to here
    use arrow_schema::{DataType, Field};
    use dbt_common::adapter::AdapterType;

    use crate::sql_types::get_field_sql_type_metadata_key;

    pub fn field_to_string(field: &Field) -> Option<String> {
        let type_key = get_field_sql_type_metadata_key(AdapterType::Bigquery);

        if let Some(original_type) = field.metadata().get(type_key) {
            let base_type = match original_type.as_str() {
                "RECORD" => {
                    // STRUCT/RECORD type, recurse and build original type
                    match field.data_type() {
                        DataType::Struct(fields) => {
                            let field_strings: Vec<String> = fields
                                .iter()
                                .map(|nested_field| {
                                    let field_name = format!("`{}`", nested_field.name());
                                    let field_type = field_to_string(nested_field)?;
                                    Some(format!("{field_name} {field_type}"))
                                })
                                .collect::<Option<Vec<_>>>()?;
                            Some(format!("STRUCT<{}>", field_strings.join(", ")))
                        }
                        _ => Some(original_type.to_string()),
                    }
                }
                _ => Some(map_bigquery_metadata_type(original_type).to_string()),
            };

            // REPEATED - this is an Array type
            if let Some(repeated) = field.metadata().get("Repeated")
                && repeated == "true"
            {
                return base_type.map(|t| format!("ARRAY<{t}>"));
            }

            base_type
        } else {
            None
        }
    }

    /// Maps bigquery aliases to their expected form for DDL statements
    fn map_bigquery_metadata_type(metadata_type: &str) -> &str {
        match metadata_type {
            "INTEGER" => "INT64",
            "FLOAT" => "FLOAT64",
            "BOOLEAN" => "BOOL",
            // XXX: This one has explicit special handling elsewhere. Added for completeness
            "RECORD" => "STRUCT",
            // Pass through other types as-is
            other => other,
        }
    }
}

pub mod postgres {
    use arrow_schema::{DataType, TimeUnit};

    use crate::AdapterResult;
    use crate::errors::{AdapterError, AdapterErrorKind};

    pub fn try_format_type(
        datatype: &DataType,
        nullable: bool,
        out: &mut String,
    ) -> AdapterResult<()> {
        use std::fmt::Write as _;
        match datatype {
            DataType::Null => out.push_str("null"),
            DataType::Boolean => out.push_str("boolean"),
            DataType::Int8 => out.push_str("tinyint"),
            DataType::Int16 => out.push_str("smallint"),
            DataType::Int32 => out.push_str("integer"),
            DataType::Int64 => out.push_str("bigint"),
            DataType::UInt8 => out.push_str("tinyint"),
            DataType::UInt16 => out.push_str("smallint"),
            DataType::UInt32 => out.push_str("integer"),
            DataType::UInt64 => out.push_str("bigint"),
            DataType::Float32 => out.push_str("real"),
            DataType::Float64 => out.push_str("double"),
            DataType::Timestamp(TimeUnit::Second, _) => out.push_str("timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                out.push_str("timestamp without time zone")
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                out.push_str("timestamp without time zone")
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                out.push_str("timestamp without time zone")
            }
            DataType::Date32 => out.push_str("date"),
            DataType::Time32(TimeUnit::Second) => out.push_str("time without time zone"),
            DataType::Time32(TimeUnit::Millisecond) => out.push_str("time without time zone"),
            DataType::Time64(TimeUnit::Microsecond) => out.push_str("time without time zone"),
            DataType::Time64(TimeUnit::Nanosecond) => out.push_str("time without time zone"),
            DataType::Interval(_) => out.push_str("interval"),
            DataType::Binary => out.push_str("binary"),
            DataType::Utf8 | DataType::Utf8View => out.push_str("text"),
            DataType::List(_) => out.push_str("array"),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::UInt16 && value.as_ref() == &DataType::Utf8 =>
            {
                out.push_str("text")
            }
            DataType::Decimal128(precision, scale) => {
                write!(out, "decimal({precision}, {scale})").unwrap()
            }
            _ => {
                return Err(AdapterError::new(
                    AdapterErrorKind::UnsupportedType,
                    format!("{datatype} is not convertible to postgres type"),
                ));
            }
        };
        if !nullable {
            out.push_str(" not null");
        }
        Ok(())
    }
}

pub const fn max_varchar_size(adapter_type: AdapterType) -> Option<usize> {
    use AdapterType::*;
    match adapter_type {
        // FIXME: Actual MAX is 134_217_728 - 16_777_216 is the default value
        Snowflake => Some(16_777_216),
        Redshift => Some(256),
        Postgres | Bigquery | Databricks | Salesforce => None,
    }
}

pub const fn max_varbinary_size(adapter_type: AdapterType) -> Option<usize> {
    use AdapterType::*;
    match adapter_type {
        Snowflake => Some(16_777_216),
        Redshift => Some(65_535),
        // TODO: define limits for more systems
        Postgres | Bigquery | Databricks | Salesforce => None,
    }
}

pub mod snowflake {
    use arrow_schema::DataType;

    // TODO: move away from this when we move away from the FixedSizeList hack
    // Additionally, it's a completely wrong assumption that drivers return types
    // like this. Drivers can't return these types. We should be using proper
    // SQL types and parsing them with [dbt_xdbc::sql::types] instead.

    #[derive(Clone, Copy)]
    pub struct TimePrecision(u8);

    impl From<TimePrecision> for u8 {
        fn from(val: TimePrecision) -> Self {
            val.0
        }
    }

    impl TimePrecision {
        /// PRE-CONDITION: valid_precision <= 9
        pub const fn new(valid_precision: u8) -> Self {
            TimePrecision(valid_precision)
        }
    }

    #[derive(Clone, Copy)]
    pub enum IsTimestamp {
        No,
        Yes(TimePrecision),
    }

    impl IsTimestamp {
        pub const fn is_yes(&self) -> bool {
            matches!(self, IsTimestamp::Yes(_))
        }

        pub const fn precision(&self) -> Option<TimePrecision> {
            match self {
                IsTimestamp::No => None,
                IsTimestamp::Yes(precision) => Some(*precision),
            }
        }

        pub fn unwrap(self) -> TimePrecision {
            match self {
                IsTimestamp::No => panic!("Cannot unwrap IsTimestamp::No"),
                IsTimestamp::Yes(precision) => precision,
            }
        }
    }

    pub fn is_time(data_type: &DataType) -> IsTimestamp {
        match data_type {
            DataType::FixedSizeList(field, 1) if field.name().starts_with("time:") => {
                IsTimestamp::Yes(TimePrecision::new(
                    field
                        .name()
                        .strip_prefix("time:")
                        .expect("string prefix checked")
                        .parse::<u8>()
                        .expect("invalid serialized time precision"),
                ))
            }
            _ => IsTimestamp::No,
        }
    }

    pub fn is_timestamp_ntz(data_type: &DataType) -> IsTimestamp {
        match data_type {
            DataType::FixedSizeList(field, 1) if field.name().starts_with("timestamp_ntz:") => {
                IsTimestamp::Yes(TimePrecision::new(
                    field
                        .name()
                        .strip_prefix("timestamp_ntz:")
                        .expect("string prefix checked")
                        .parse::<u8>()
                        .expect("invalid serialized timestamp precision"),
                ))
            }
            _ => IsTimestamp::No,
        }
    }

    pub fn is_timestamp_ltz(data_type: &DataType) -> IsTimestamp {
        match data_type {
            DataType::FixedSizeList(field, 1) if field.name().starts_with("timestamp_ltz:") => {
                IsTimestamp::Yes(TimePrecision::new(
                    field
                        .name()
                        .strip_prefix("timestamp_ltz:")
                        .expect("string prefix checked")
                        .parse::<u8>()
                        .expect("invalid serialized timestamp precision"),
                ))
            }
            _ => IsTimestamp::No,
        }
    }

    pub fn is_timestamp_tz(data_type: &DataType) -> IsTimestamp {
        match data_type {
            DataType::FixedSizeList(field, 1) if field.name().starts_with("timestamp_tz:") => {
                IsTimestamp::Yes(TimePrecision::new(
                    field
                        .name()
                        .strip_prefix("timestamp_tz:")
                        .expect("string prefix checked")
                        .parse::<u8>()
                        .expect("invalid serialized timestamp precision"),
                ))
            }
            _ => IsTimestamp::No,
        }
    }
}

/// Returns the number of fractional digits for a given Arrow time unit.
fn time_precision(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// The size constraint for variable-size types (e.g. VARCHAR, VARBINARY).
pub fn var_size(adapter_type: AdapterType, data_type: &DataType) -> Option<usize> {
    use AdapterType::*;
    match (adapter_type, data_type) {
        // Strings: Redshift wants a length; persist it in char_size
        // TODO(jason): We need to report the correct size and not just a default
        (Redshift, DataType::Utf8 | DataType::Utf8View) => max_varchar_size(Redshift),
        // For VARCHAR types, no explicit size in Snowflake unless specified
        (Snowflake, DataType::Utf8 | DataType::Utf8View) => None,
        // XXX: need to think about the defaults for these adapters
        (Postgres | Bigquery | Databricks | Salesforce, DataType::Utf8 | DataType::Utf8View) => {
            None
        }

        // Bytes
        // TODO(jason): We need to report the correct size and not just a default
        (Redshift, DataType::Binary) => max_varbinary_size(Redshift),
        // XXX: need to think about the defaults for these adapters
        (Snowflake | Postgres | Bigquery | Databricks | Salesforce, DataType::Binary) => None,

        // Snowflake: For timestamp/date/time types, extract precision if available
        (Snowflake, dt) if snowflake::is_time(dt).is_yes() => {
            let char_size: u8 = snowflake::is_time(dt).unwrap().into();
            Some(char_size as usize)
        }
        (Snowflake, dt)
            if snowflake::is_timestamp_ntz(dt).is_yes()
                || snowflake::is_timestamp_ltz(dt).is_yes()
                || snowflake::is_timestamp_tz(dt).is_yes() =>
        {
            // For timestamp types, the precision is the fractional seconds precision
            // For compatibility with dbt core column type rendering code, precision is stored as char_size
            let time_precision = if snowflake::is_timestamp_ntz(dt).is_yes() {
                snowflake::is_timestamp_ntz(dt).unwrap()
            } else if snowflake::is_timestamp_ltz(dt).is_yes() {
                snowflake::is_timestamp_ltz(dt).unwrap()
            } else if snowflake::is_timestamp_tz(dt).is_yes() {
                snowflake::is_timestamp_tz(dt).unwrap()
            } else {
                return None;
            };
            let char_size: u8 = time_precision.into();
            Some(char_size as usize)
        }

        // Recurse for dictionary-encoded types
        // XXX: the key type is irrelevant and should probably be removed from the match pattern
        (_, DataType::Dictionary(key_ty, value_ty))
            if key_ty.as_ref() == &DataType::UInt16 && value_ty.as_ref() == &DataType::Utf8 =>
        {
            var_size(adapter_type, value_ty)
        }

        _ => None,
    }
}

pub fn numeric_precision_scale(
    adapter_type: AdapterType,
    data_type: &DataType,
) -> AdapterResult<Option<(u8, Option<i8>)>> {
    use AdapterType::*;
    let precision_scale = match (adapter_type, data_type) {
        (_, DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale)) => {
            // cap precision at 38 for Redshift
            if adapter_type == Redshift && *precision > 38 {
                return Err(AdapterError::new(
                    AdapterErrorKind::NotSupported,
                    format!("Decimal precision '{}' exceed 38 place limit", *precision),
                ));
            }
            Some((*precision, Some(*scale)))
        }

        // For integer types (i.e. non-scaled numbers)
        (_, DataType::Int8) => Some((3, None)),
        (_, DataType::Int16) => Some((5, None)),
        (_, DataType::Int32) => Some((10, None)),
        (_, DataType::Int64) => Some((19, None)),
        (_, DataType::UInt8) => Some((3, None)),
        (_, DataType::UInt16) => Some((5, None)),
        (_, DataType::UInt32) => Some((10, None)),
        (_, DataType::UInt64) => Some((20, None)),

        // For floating point types (i.e. arbitrarily scaled numbers)
        (_, DataType::Float32) => Some((24, None)),
        (_, DataType::Float64) => Some((53, None)),

        // For timestamp/date/time types, extract precision if available
        (Snowflake, dt) if snowflake::is_time(dt).is_yes() => {
            let precision = snowflake::is_time(dt).unwrap();
            Some((precision.into(), None))
        }
        // XXX: maybe numeric_precision must be extract in this case too?
        // (Snowflake, dt) if snowflake::is_timestamp_ntz(dt).is_yes()
        //     || snowflake::is_timestamp_ltz(dt).is_yes()
        //     || snowflake::is_timestamp_tz(dt).is_yes() =>
        // {
        //     // For timestamp types, the precision is the fractional seconds precision
        //     // For compatibility with dbt core column type rendering code, precision is stored as char_size
        //     let time_precision = if snowflake::is_timestamp_ntz(dt).is_yes() {
        //         snowflake::is_timestamp_ntz(dt).unwrap()
        //     } else if snowflake::is_timestamp_ltz(dt).is_yes() {
        //         snowflake::is_timestamp_ltz(dt).unwrap()
        //     } else if snowflake::is_timestamp_tz(dt).is_yes() {
        //         snowflake::is_timestamp_tz(dt).unwrap()
        //     } else {
        //         return None;
        //     };
        //     let char_size: u8 = time_precision.into();
        //     Some(char_size as usize)
        // }

        // Handle general timestamp types
        (Snowflake, DataType::Timestamp(unit, _)) => {
            let precision = time_precision(*unit);
            Some((precision, None))
        }

        (_, DataType::Time64(_) | DataType::Time32(_)) => {
            // Redshift stores microseconds (6 fractional digits)
            Some((6, None))
        }
        // Timestamps (with or without tz) – clamp to microseconds
        // TODO: handle more complex timestamp/date/time types not in sdk front end
        (_, DataType::Timestamp(_, _)) => Some((6, None)),

        // Other types don't have specific precision/scale
        _ => None,
    };

    Ok(precision_scale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use SqlTypeHint::*;
    use dbt_common::adapter::AdapterType::*;

    #[test]
    fn test_convert_integer_type() {
        let convert_integer_type =
            |adapter_type| sql_type_hint_to_str(Integer, false, adapter_type);
        assert_eq!(convert_integer_type(Bigquery), "int64");
        assert_eq!(convert_integer_type(Databricks), "bigint");
        assert_eq!(convert_integer_type(Postgres), "integer");
        assert_eq!(convert_integer_type(Snowflake), "integer");
        assert_eq!(convert_integer_type(Redshift), "integer");
    }

    #[test]
    fn test_convert_number_type() {
        let convert_floating_type =
            |adapter_type| sql_type_hint_to_str(Floating, false, adapter_type);
        assert_eq!(convert_floating_type(Bigquery), "int64");
        assert_eq!(convert_floating_type(Databricks), "bigint");
        assert_eq!(convert_floating_type(Postgres), "integer");
        assert_eq!(convert_floating_type(Snowflake), "integer");
        assert_eq!(convert_floating_type(Redshift), "integer");
        let convert_decimal_type =
            |adapter_type| sql_type_hint_to_str(Decimal, false, adapter_type);
        assert_eq!(convert_decimal_type(Bigquery), "float64");
        assert_eq!(convert_decimal_type(Databricks), "double");
        assert_eq!(convert_decimal_type(Postgres), "float8");
        assert_eq!(convert_decimal_type(Snowflake), "float8");
        assert_eq!(convert_decimal_type(Redshift), "float8");
    }

    #[test]
    fn test_convert_boolean_type() {
        let convert_boolean_type =
            |adapter_type| sql_type_hint_to_str(Boolean, false, adapter_type);
        assert_eq!(convert_boolean_type(Bigquery), "bool");
        assert_eq!(convert_boolean_type(Databricks), "boolean");
        assert_eq!(convert_boolean_type(Postgres), "boolean");
        assert_eq!(convert_boolean_type(Snowflake), "boolean");
        assert_eq!(convert_boolean_type(Redshift), "boolean");
    }

    #[test]
    fn test_convert_datetime_type() {
        let convert_datetime_type =
            |adapter_type| sql_type_hint_to_str(Datetime, false, adapter_type);
        assert_eq!(convert_datetime_type(Bigquery), "datetime");
        assert_eq!(convert_datetime_type(Databricks), "timestamp");
        assert_eq!(
            convert_datetime_type(Postgres),
            "timestamp without time zone"
        );
        assert_eq!(
            convert_datetime_type(Snowflake),
            "timestamp without time zone"
        );
        assert_eq!(
            convert_datetime_type(Redshift),
            "timestamp without time zone"
        );
    }
    const ALL_ADAPTERS: [AdapterType; 5] = [Bigquery, Databricks, Postgres, Snowflake, Redshift];

    #[test]
    fn test_convert_date_type() {
        let convert_date_type = |adapter_type| sql_type_hint_to_str(Date, false, adapter_type);
        // Test all adapters return "date"
        for adapter_type in ALL_ADAPTERS {
            assert_eq!(convert_date_type(adapter_type), "date");
        }
    }

    #[test]
    fn test_convert_time_type() {
        let convert_time_type = |adapter_type| sql_type_hint_to_str(Time, false, adapter_type);
        // Test all adapters return "time"
        for adapter_type in ALL_ADAPTERS {
            assert_eq!(convert_time_type(adapter_type), "time");
        }
    }

    #[test]
    fn test_convert_text_type() {
        let convert_text_type = |adapter_type| sql_type_hint_to_str(Text, false, adapter_type);
        assert_eq!(convert_text_type(Bigquery), "string");
        assert_eq!(convert_text_type(Databricks), "string");
        assert_eq!(convert_text_type(Postgres), "text");
        assert_eq!(convert_text_type(Snowflake), "text");
        assert_eq!(convert_text_type(Redshift), "text");
    }
}
