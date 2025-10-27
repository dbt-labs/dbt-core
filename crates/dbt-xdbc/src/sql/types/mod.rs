use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::num::ParseIntError;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit};

use crate::Backend;

use super::ident::Ident;
use super::tokenizer::{Token, Tokenizer};

#[cfg(test)]
mod tests;

#[derive(Debug, Copy, Clone)]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl fmt::Display for DateTimeField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DateTimeField::*;
        match self {
            Year => write!(f, "YEAR"),
            Month => write!(f, "MONTH"),
            Day => write!(f, "DAY"),
            Hour => write!(f, "HOUR"),
            Minute => write!(f, "MINUTE"),
            Second => write!(f, "SECOND"),
            Millisecond => write!(f, "MILLISECOND"),
            Microsecond => write!(f, "MICROSECOND"),
            Nanosecond => write!(f, "NANOSECOND"),
        }
    }
}

impl DateTimeField {
    fn write(&self, backend: Backend, out: &mut String) -> fmt::Result {
        use Backend::*;
        use DateTimeField::*;
        use fmt::Write as _;
        // In PostgreSQL, the sub-second fields are expressed as
        // `SECOND` followed by a precision, e.g. `SECOND(3)`.
        if matches!(backend, Postgres | Redshift | RedshiftODBC) {
            match self {
                Millisecond => {
                    out.push_str("SECOND(3)");
                    Ok(())
                }
                Microsecond => {
                    out.push_str("SECOND(6)");
                    Ok(())
                }
                Nanosecond => {
                    out.push_str("SECOND(9)");
                    Ok(())
                }
                _ => write!(out, "{self}"),
            }
        } else {
            write!(out, "{self}")
        }
    }

    fn from_precision(p: u8) -> Self {
        use DateTimeField::*;
        match p {
            0..=2 => Second,
            3..=5 => Millisecond,
            6..=8 => Microsecond,
            9 => Nanosecond,
            _ => Nanosecond,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum TimeZoneSpec {
    /// WITH LOCAL TIME ZONE, TIMESTAMP_LTZ
    Local,
    // WITH TIME ZONE, TIMESTAMPTZ, TIMESTAMP_TZ
    With,
    // WITHOUT TIME ZONE, TIMESTAMP_NTZ
    Without,
    // no specification (e.g. TIMESTAMP)
    Unspecified,
}

impl TimeZoneSpec {
    fn write_with_leading_space(&self, backend: Backend, out: &mut String) -> fmt::Result {
        use Backend::*;
        use TimeZoneSpec::*;
        use fmt::Write as _;
        match (backend, self) {
            // BigQuery TIMESTAMP is always stored without a time zone so the type name never says
            // anything about time zones.
            //
            // NOTE: literals can contain time zone information, so *there are more types of
            // literals than types that end up stored in the database.* So in case a type were to
            // be instantiated with a time zone spec and we have to render it, we will produce the
            // "WITH TIME ZONE" form which can be useful for debugging.
            (BigQuery, Without | Unspecified) => Ok(()),

            // PostgreSQL TIMESTAMP WITHOUT TIME ZONE can be rendered as TIMESTAMP
            (Postgres | Redshift | RedshiftODBC, Without) => Ok(()),

            (_, Local) => write!(out, " WITH LOCAL TIME ZONE"),
            (_, With) => write!(out, " WITH TIME ZONE"),
            (_, Without) => write!(out, " WITHOUT TIME ZONE"),

            (_, Unspecified) => Ok(()),
        }
    }

    fn write_single_token_suffix(&self, backend: Backend, out: &mut String) -> fmt::Result {
        use Backend::*;
        use TimeZoneSpec::*;
        use fmt::Write as _;
        match (backend, self) {
            // See [TimeZoneSpec::write_with_leading_space] for explanation about BigQuery.
            (BigQuery, _) => {
                debug_assert!(
                    matches!(self, Without | Unspecified),
                    "BigQuery does not support time zone suffixes in its type names"
                );
                Ok(())
            }

            // TIMETZ and TIMESTAMPTZ in PostgreSQL which doesn't have
            // a type that is specifically for local time zone.
            (Postgres | Redshift | RedshiftODBC | Salesforce, Local | With) => {
                debug_assert!(
                    !matches!(self, Local),
                    "PostgreSQL does not have a TIMESTAMP WITH LOCAL TIME ZONE type"
                );
                write!(out, "TZ")
            }
            // In PostgreSQL, TIMESTAMP WITHOUT TIME ZONE is just TIMESTAMP
            (Postgres | Redshift | RedshiftODBC | Salesforce, Without | Unspecified) => Ok(()),

            // Databricks doesn't have a TIMESTAMP WITH TIME ZONE type, only WITH LOCAL TIME ZONE
            // (TIMESTAMP or TIMESTAMP_LTZ) and WITHOUT TIME ZONE (TIMESTAMP_NTZ).
            (Databricks | DatabricksODBC, Unspecified) => Ok(()),
            (Databricks | DatabricksODBC, Without) => write!(out, "_NTZ"),
            (Databricks | DatabricksODBC, With) => Ok(()),

            (_, Local) => write!(out, "_LTZ"),
            (_, With) => write!(out, "_TZ"),
            (_, Without) => write!(out, "_NTZ"),

            // No suffix for unspecified time zone spec.
            //
            // In Snowflake, TIMESTAMP without a time zone spec is ambiguous,
            // we forward the ambiguity to the rendered SQL instead of picking
            // a default.
            //
            // In Databricks, TIMESTAMP has TIMESTAMP_LTZ semantics by default,
            // but we don't render it as TIMESTAMP_LTZ unless explicitly specified
            // even though TIMESTAMP_LTZ is an alias to TIMESTAMP in Databricks.
            (_, Unspecified) => Ok(()),
        }
    }

    pub fn is_with_time_zone(self, backend: Backend) -> bool {
        use Backend::*;
        use TimeZoneSpec::*;
        match (backend, self) {
            // Databricks TIMESTAMP has WITH LOCAL TIME ZONE semantics by default
            (Databricks, Unspecified | With | Local) => true,

            (Snowflake, Unspecified) => {
                // Users can run `ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = TIMESTAMP_TZ;`
                // so the meaning of `TIMESTAMP` is dependent on the session state.
                debug_assert!(
                    false,
                    "Snowflake TIMESTAMP without a time zone spec is ambiguous. \
Avoid constructing Snowflake TIME/TIMESTAMP types without an explicit time zone specification."
                );
                false
            }

            (_, With | Local) => true,
            (_, Without | Unspecified) => false,
        }
    }
}

pub fn default_time_unit(backend: Backend) -> TimeUnit {
    use Backend::*;
    use TimeUnit::*;
    match backend {
        Snowflake | Databricks | DatabricksODBC => Nanosecond,
        BigQuery | Redshift | RedshiftODBC => Microsecond,
        Postgres | Salesforce => Microsecond,
        Generic { .. } => Microsecond, // a reasonable default
    }
}

pub const fn time_unit_to_precision(time_unit: TimeUnit) -> u8 {
    use TimeUnit::*;
    match time_unit {
        Second => 0,
        Millisecond => 3,
        Microsecond => 6,
        Nanosecond => 9,
    }
}

/// Returns the [TimeUnit] that can represent the given precision.
///
/// The unit should allow the representation of `n**(-precision)` seconds
/// for any `n <= 9` without being unnecessarily more precise than that.
pub fn suitable_time_unit(precision: u8) -> TimeUnit {
    use TimeUnit::*;
    match precision {
        0 => Second,
        1..=3 => Millisecond,
        4..=6 => Microsecond,
        7..=9 => Nanosecond,
        _ => {
            debug_assert!(false, "invalid time precision: {precision}");
            Nanosecond
        }
    }
}

/// Additional attributes for string types.
#[derive(Debug, Clone, Default)]
pub struct StringAttrs {
    pub collate_spec: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StructField {
    pub name: Ident,
    pub sql_type: SqlType,
    pub nullable: bool,
    /// The raw token found after the COMMENT keyword.
    pub comment_tok: Option<String>,
}

impl StructField {
    pub fn new(name: Ident, sql_type: SqlType, nullable: bool) -> Self {
        Self {
            name,
            sql_type,
            nullable,
            comment_tok: None,
        }
    }

    pub fn with_comment(mut self, comment: String) -> Self {
        self.comment_tok = Some(comment);
        self
    }
}

/// Syntactic representation of SQL types.
///
/// The string representation and semantics of each SQL type can only be
/// realized in the context of a specific [SQL backend](`crate::Backend`).
/// But this enum aims to be a common representation that can be used
/// across different backends with slight tweaks in the behavior.
#[derive(Debug, Clone)] // DO NOT derive PartialEq or Eq, use `to_string(backend)` for comparisons!
pub enum SqlType {
    /// BOOLEAN
    Boolean,
    /// TINYINT
    TinyInt,
    /// SMALLINT
    SmallInt,
    /// INTEGER / INT
    Integer,
    /// BIGINT
    BigInt,
    /// REAL
    Real,
    /// FLOAT [ '(' precision ')' ]
    Float(Option<u8>),
    /// DOUBLE PRECISION
    Double,
    /// (DECIMAL | NUMERIC) [ '(' precision [ ',' scale ] ')' ]
    Numeric(Option<(u8, Option<i8>)>),
    /// (BIGDECIMAL | BIGNUMERIC) [ '(' precision [ ',' scale ] ')' ]
    BigNumeric(Option<(u8, Option<i8>)>),
    /// (CHAR | CHARACTER | NCHAR | NATIONAL CHAR) [ '(' length ')' ]
    Char(Option<usize>),
    /// ((VARCHAR | CHARACTER VARYING) [ '(' length ')' ] |
    ///  (NVARCHAR | NATIONAL CHAR VARYING) [ '(' length ')' ])
    /// [ COLLATE collation ]
    Varchar(Option<usize>, StringAttrs),
    /// TEXT
    Text,
    /// CLOB / CHARACTER LARGE OBJECT
    Clob,
    /// BLOB / BINARY LARGE OBJECT
    Blob,
    /// BINARY / VARBINARY [ '(' length ')' ]
    Binary(Option<usize>),
    /// DATE
    Date,
    /// TIME [ '(' precision ')' ] [ WITH TIME ZONE | WITH LOCAL | WITHOUT TIME ZONE ]
    Time {
        precision: Option<u8>,
        time_zone_spec: TimeZoneSpec,
    },
    /// TIMESTAMP
    Timestamp {
        precision: Option<u8>,
        time_zone_spec: TimeZoneSpec,
    },
    /// DATETIME is different from timestamps in BigQuery.
    DateTime,
    /// INTERVAL [
    ///        <start field> TO <end field>
    ///      | <single datetime field>
    /// ]
    Interval(Option<(DateTimeField, Option<DateTimeField>)>),
    /// JSON
    Json,
    /// JSONB
    Jsonb,
    /// GEOMETRY
    Geometry,
    /// GEOGRAPHY
    Geography,
    /// ARRAY
    Array(Option<Box<SqlType>>),
    /// STRUCT, STRUCT<>, STRUCT<...>
    Struct(Option<Vec<StructField>>),
    /// MAP <key type, value type>
    Map(Option<(Box<SqlType>, Box<SqlType>)>),
    /// VARIANT
    Variant,
    /// VOID
    Void,
    /// Other SQL types that are not explicitly defined.
    ///
    /// This is useful in situations where we can treat the SQL type as an
    /// opaque name without needing to deal with it in a specific way. If
    /// we need more awareness about a specific type, we should expand
    /// the enum with a new variant.
    Other(String),
}

impl SqlType {
    pub fn varchar(max_len: Option<usize>) -> Self {
        SqlType::Varchar(max_len, Default::default())
    }

    /// Extract the SQL type and nullability from an Arrow `Field`.
    ///
    /// This is a lossless conversion if the SQL type is stored in the
    /// Arrow field metadata. If the SQL type is not present, it will try
    /// to come up with a best-effort conversion from the Arrow DataType.
    pub fn from_field(backend: Backend, field: &Field) -> Result<(Self, bool), String> {
        let type_string = original_type_string(backend, field);
        match type_string {
            Some(type_str) => {
                let (sql_type, nullable) = Self::parse(backend, type_str)?;
                let nullable = nullable || field.is_nullable();
                Ok((sql_type, nullable))
            }
            None => {
                let sql_type = Self::_from_arrow_type(backend, field.data_type());
                Ok((sql_type, field.is_nullable()))
            }
        }
    }

    /// Convert the SQL type to an Arrow `Field`.
    ///
    /// It encodes the SQL type as metadata in the Arrow field and picks the best
    /// Arrow `DataType` that matches for the SQL type.
    pub fn to_field(&self, backend: Backend, name: String, nullable: bool) -> Field {
        let data_type = self.pick_best_arrow_type(backend);
        let mut metadata = HashMap::new();
        metadata.insert(
            metadata_sql_type_key(backend).to_string(),
            self.to_string(backend),
        );
        Field::new(name, data_type, nullable).with_metadata(metadata)
    }

    /// Parse the SQL type and return it along with a boolean indicating if its nullable.
    pub fn parse(backend: Backend, input: &str) -> Result<(SqlType, bool), String> {
        let mut parser = Parser::new(input);
        parser
            .parse(backend)
            .map_err(|err| format!("Failed to parse SQL type '{input}': {err}"))
    }

    pub fn to_string(&self, backend: Backend) -> String {
        let mut out = String::new();
        self.write(backend, &mut out).unwrap();
        out
    }

    /// Render a SQL type string in the preferred syntax for a given backend.
    pub fn write(&self, backend: Backend, out: &mut String) -> fmt::Result {
        use Backend::*;
        use SqlType::*;
        use fmt::Write as _;
        match (backend, self) {
            // BigQuery {{{
            (BigQuery, Boolean) => write!(out, "BOOL"),
            (BigQuery, TinyInt | SmallInt | Integer | BigInt) => write!(out, "INT64"),
            (BigQuery, Real | Float(_) | Double) => {
                write!(out, "FLOAT64")
            }
            (BigQuery, Char(_) | Varchar(..) | Text | Clob) => {
                write!(out, "STRING")
            }
            (BigQuery, Blob | Binary(_)) => write!(out, "BYTES"),
            (BigQuery, Time { time_zone_spec, .. }) => {
                write!(out, "TIME")?;
                // BigQuery does not use precision for time and timestamp types
                time_zone_spec.write_with_leading_space(backend, out)
            }
            (BigQuery, Timestamp { time_zone_spec, .. }) => {
                write!(out, "TIMESTAMP",)?;
                // BigQuery does not use precision for timestamps
                time_zone_spec.write_with_leading_space(backend, out)
            }
            // }}}

            // Snowflake {{{
            (Snowflake, Float(_)) => write!(out, "FLOAT"),
            (Snowflake, Numeric(None) | BigNumeric(None)) => {
                write!(out, "NUMBER")
            }
            (Snowflake, Numeric(Some((p, None))) | BigNumeric(Some((p, None)))) => {
                write!(out, "NUMBER({p})")
            }
            (Snowflake, Numeric(Some((p, Some(s)))) | BigNumeric(Some((p, Some(s))))) => {
                write!(out, "NUMBER({p}, {s})")
            }
            (Snowflake, Clob) => write!(out, "TEXT"),
            (Snowflake, Blob) => write!(out, "BINARY"),
            (
                Snowflake,
                Time {
                    precision,
                    time_zone_spec,
                },
            ) => {
                write!(out, "TIME")?;
                if let Some(p) = precision {
                    write!(out, "({p})")?;
                }
                // Snowflake does not have a TIME WITH TIME ZONE type
                match time_zone_spec {
                    TimeZoneSpec::Unspecified | TimeZoneSpec::Without => Ok(()),
                    TimeZoneSpec::Local | TimeZoneSpec::With => {
                        // for debugging purposes, we still render these invalid specs
                        time_zone_spec.write_with_leading_space(backend, out)
                    }
                }
            }
            (
                Snowflake,
                Timestamp {
                    precision,
                    time_zone_spec,
                },
            ) => {
                write!(out, "TIMESTAMP")?;
                time_zone_spec.write_single_token_suffix(backend, out)?;
                match precision {
                    Some(p) => write!(out, "({p})"),
                    None => Ok(()),
                }
            }
            (Snowflake, DateTime) => write!(out, "TIMESTAMP_NTZ"),
            // }}}

            // PostgreSQL {{{
            (Postgres | Redshift | RedshiftODBC, TinyInt) => write!(out, "SMALLINT"),
            (Postgres | Redshift | RedshiftODBC, Binary(_) | Blob) => write!(out, "BYTEA"),
            (Postgres | Redshift | RedshiftODBC, DateTime) => write!(out, "TIMESTAMP"),
            (
                Postgres | Redshift | RedshiftODBC,
                Timestamp {
                    precision,
                    time_zone_spec,
                },
            ) => match precision {
                Some(p) => {
                    // if there is a precision, we use the (..) WITH TIME ZONE form
                    write!(out, "TIMESTAMP({p})")?;
                    time_zone_spec.write_with_leading_space(backend, out)
                }
                None => {
                    // if there is no precision, we use the TIMESTAMPTZ / TIMESTAMP form
                    write!(out, "TIMESTAMP")?;
                    time_zone_spec.write_single_token_suffix(backend, out)
                }
            },
            (Postgres | Redshift | RedshiftODBC, Float(_)) => write!(out, "REAL"),
            (Postgres | Redshift | RedshiftODBC, Clob) => write!(out, "TEXT"),
            (Postgres | Redshift | RedshiftODBC | Salesforce, Array(Some(inner))) => {
                inner.write(backend, out)?;
                write!(out, "[]")
            }
            // }}}

            // Databricks {{{
            (Databricks | DatabricksODBC, Binary(_) | Blob) => {
                // max_len for BINARY is ignored because Databricks doesn't support it
                write!(out, "BINARY")
            }
            (Databricks | DatabricksODBC, Clob | Text | Varchar(..)) => {
                write!(out, "STRING")?;
                if let Varchar(_, attrs) = self {
                    if let Some(collate_spec) = &attrs.collate_spec {
                        write!(out, " COLLATE {collate_spec}")?;
                    }
                }
                Ok(())
            }
            (Databricks | DatabricksODBC, Numeric(None) | BigNumeric(None)) => {
                write!(out, "DECIMAL")
            }
            (
                Databricks | DatabricksODBC,
                Numeric(Some((p, None))) | BigNumeric(Some((p, None))),
            ) => {
                write!(out, "DECIMAL({p})")
            }
            (
                Databricks | DatabricksODBC,
                Numeric(Some((p, Some(s)))) | BigNumeric(Some((p, Some(s)))),
            ) => {
                write!(out, "DECIMAL({p}, {s})")
            }
            (Databricks | DatabricksODBC, Real | Float(_)) => write!(out, "FLOAT"),
            (Databricks | DatabricksODBC, Double) => write!(out, "DOUBLE"),
            (Databricks | DatabricksODBC, DateTime) => write!(out, "TIMESTAMP_NTZ"),
            (Databricks | DatabricksODBC, Timestamp { time_zone_spec, .. }) => {
                write!(out, "TIMESTAMP")?;
                time_zone_spec.write_single_token_suffix(backend, out)
            }
            // }}}

            // Generic SQL / Fallback logic {{{
            (_, Boolean) => write!(out, "BOOLEAN"),
            (_, TinyInt) => write!(out, "TINYINT"),
            (_, SmallInt) => write!(out, "SMALLINT"),
            (_, Integer) => write!(out, "INT"),
            (_, BigInt) => write!(out, "BIGINT"),

            (_, Real) => write!(out, "REAL"),
            (_, Float(Some(p))) => write!(out, "FLOAT({p})"),
            (_, Float(None)) => write!(out, "FLOAT"),
            (_, Double) => write!(out, "DOUBLE PRECISION"),

            (_, Numeric(None)) => write!(out, "NUMERIC"),
            (_, Numeric(Some((p, None)))) => write!(out, "NUMERIC({p})"),
            (_, Numeric(Some((p, Some(s))))) => write!(out, "NUMERIC({p}, {s})"),
            (_, BigNumeric(None)) => write!(out, "BIGNUMERIC"),
            (_, BigNumeric(Some((p, None)))) => write!(out, "BIGNUMERIC({p})"),
            (_, BigNumeric(Some((p, Some(s))))) => write!(out, "BIGNUMERIC({p}, {s})"),

            (_, Char(None)) => write!(out, "CHAR"),
            (_, Char(Some(len))) => {
                write!(out, "CHAR")?;
                if *len > 0 {
                    write!(out, "({len})")?;
                }
                Ok(())
            }
            (_, Varchar(max_len, attrs)) => {
                write!(out, "VARCHAR")?;
                let max_len = max_len.unwrap_or(0);
                if max_len > 0 {
                    write!(out, "({max_len})")?;
                }
                if let Some(collate_spec) = &attrs.collate_spec {
                    write!(out, " COLLATE {collate_spec}")?;
                }
                Ok(())
            }
            (_, Text) => write!(out, "TEXT"),
            (_, Clob) => write!(out, "CLOB"),
            (_, Blob) => write!(out, "BLOB"),
            (_, Binary(None)) => write!(out, "BINARY"),
            (_, Binary(Some(len))) => {
                write!(out, "BINARY")?;
                if *len > 0 {
                    write!(out, "({len})")?;
                }
                Ok(())
            }

            (_, Date) => write!(out, "DATE"),
            (
                _,
                Time {
                    precision,
                    time_zone_spec,
                },
            ) => {
                match precision {
                    Some(p) => write!(out, "TIME({p})"),
                    None => write!(out, "TIME"),
                }?;
                time_zone_spec.write_with_leading_space(backend, out)
            }
            (_, DateTime) => write!(out, "DATETIME"),
            (
                _,
                Timestamp {
                    precision,
                    time_zone_spec,
                },
            ) => {
                match precision {
                    Some(p) => write!(out, "TIMESTAMP({p})"),
                    None => write!(out, "TIMESTAMP"),
                }?;
                time_zone_spec.write_with_leading_space(backend, out)
            }

            (_, Interval(qualifier)) => match qualifier {
                None => write!(out, "INTERVAL"),
                Some((start, end)) => {
                    write!(out, "INTERVAL ")?;
                    match end {
                        Some(end) => {
                            write!(out, "{start} TO ")?;
                            end.write(backend, out)
                        }
                        None => start.write(backend, out),
                    }
                }
            },

            (_, Json) => write!(out, "JSON"),
            (_, Jsonb) => write!(out, "JSONB"),
            (_, Geometry) => write!(out, "GEOMETRY"),
            (_, Geography) => write!(out, "GEOGRAPHY"),
            (_, Array(None)) => write!(out, "ARRAY"),
            (backend, Array(Some(inner))) => {
                match backend {
                    Snowflake => write!(out, "ARRAY(")?,
                    _ => write!(out, "ARRAY<")?,
                }
                inner.write(backend, out)?;
                match backend {
                    Snowflake => write!(out, ")"),
                    _ => write!(out, ">"),
                }
            }
            (_, Struct(None)) => write!(out, "STRUCT"),
            (_, Struct(Some(fields))) => {
                match backend {
                    Snowflake => write!(out, "OBJECT(")?,
                    BigQuery | Databricks | DatabricksODBC => write!(out, "STRUCT<")?,
                    Postgres | Salesforce => write!(out, "(")?,
                    // Redshift doesn't support object/struct types
                    Redshift | RedshiftODBC => write!(out, "(")?,
                    Generic { .. } => write!(out, "STRUCT<")?,
                }
                for (i, field) in fields.iter().enumerate() {
                    let StructField {
                        name,
                        sql_type,
                        nullable,
                        comment_tok,
                    } = field;

                    if i > 0 {
                        write!(out, ", ")?;
                    }
                    write!(
                        out,
                        "{}{}",
                        name.display(backend),
                        // Databricks allows a `:` between the name and type
                        if matches!(backend, Databricks | DatabricksODBC) {
                            ": "
                        } else {
                            " "
                        }
                    )?;
                    sql_type.write(backend, out)?;
                    if !nullable {
                        write!(out, " NOT NULL")?;
                    }
                    if let Some(tok) = comment_tok {
                        write!(out, " COMMENT {tok}")?;
                    }
                }
                match backend {
                    Snowflake => write!(out, ")"),
                    BigQuery | Databricks | DatabricksODBC => write!(out, ">"),
                    Postgres | Salesforce => write!(out, ")"),
                    Redshift | RedshiftODBC => write!(out, ")"),
                    Generic { .. } => write!(out, ">"),
                }
            }
            (_, Map(None)) => write!(out, "MAP"),
            (_, Map(Some((key, value)))) => {
                write!(out, "MAP<")?;
                key.write(backend, out)?;
                write!(out, ", ")?;
                value.write(backend, out)?;
                write!(out, ">")
            }
            (_, Variant) => write!(out, "VARIANT"),
            (_, Void) => write!(out, "VOID"),
            (_, Other(s)) => write!(out, "{s}"),
            // }}}
        }
    }

    /// Best-effort conversion from an Arrow `DataType` to a `SqlType`.
    ///
    /// Arrow types are less expressive than SQL types, so this function
    /// will return a `SqlType` that is the closest match. This is only
    /// used in situations where the field metadata in an Arrow schema
    /// doesn't contain the SQL type string.
    fn _from_arrow_type(backend: Backend, data_type: &DataType) -> SqlType {
        match data_type {
            DataType::Null => SqlType::Varchar(None, Default::default()),
            DataType::Boolean => SqlType::Boolean,
            DataType::Int8 | DataType::UInt8 | DataType::Int16 => SqlType::SmallInt,
            DataType::UInt16 | DataType::Int32 => SqlType::Integer,
            DataType::UInt32 | DataType::Int64 | DataType::UInt64 => SqlType::BigInt,
            DataType::Float16 | DataType::Float32 => SqlType::Real,
            DataType::Float64 => SqlType::Double,
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                // XXX: make these more succinct by looking up the defaults
                // for each different backend.
                SqlType::Numeric(Some((*p, Some(*s))))
            }
            DataType::Utf8View | DataType::Utf8 => SqlType::Varchar(None, Default::default()),
            DataType::LargeUtf8 => SqlType::Text,
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => SqlType::Binary(None),
            DataType::Date32 | DataType::Date64 => SqlType::Date,
            DataType::Time32(TimeUnit::Second) => SqlType::Time {
                precision: None,
                time_zone_spec: TimeZoneSpec::Without,
            },
            DataType::Time32(TimeUnit::Millisecond) => SqlType::Time {
                precision: Some(3),
                time_zone_spec: TimeZoneSpec::Without,
            },
            DataType::Time64(TimeUnit::Microsecond) => SqlType::Time {
                precision: Some(6),
                time_zone_spec: TimeZoneSpec::Without,
            },
            DataType::Time64(TimeUnit::Nanosecond) => SqlType::Time {
                precision: Some(9),
                time_zone_spec: TimeZoneSpec::Without,
            },
            DataType::Time32(_) | DataType::Time64(_) => {
                unreachable!("unexpected time unit in Arrow data type: {data_type:?}")
            }
            DataType::Timestamp(TimeUnit::Second, tz) => SqlType::Timestamp {
                precision: None,
                time_zone_spec: if tz.is_some() {
                    TimeZoneSpec::With
                } else {
                    TimeZoneSpec::Without
                },
            },
            DataType::Timestamp(TimeUnit::Millisecond, tz) => SqlType::Timestamp {
                precision: Some(3),
                time_zone_spec: if tz.is_some() {
                    TimeZoneSpec::With
                } else {
                    TimeZoneSpec::Without
                },
            },
            DataType::Timestamp(TimeUnit::Microsecond, tz) => SqlType::Timestamp {
                precision: Some(6),
                time_zone_spec: if tz.is_some() {
                    TimeZoneSpec::With
                } else {
                    TimeZoneSpec::Without
                },
            },
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => SqlType::Timestamp {
                precision: Some(9),
                time_zone_spec: if tz.is_some() {
                    TimeZoneSpec::With
                } else {
                    TimeZoneSpec::Without
                },
            },
            DataType::Duration(..) => todo!(),
            // Proposal for extending Arrow to support more SQL interval types:
            // https://docs.google.com/document/d/12ghQxWxyAhSQeZyy0IWiwJ02gTqFOgfYm8x851HZFLk/edit
            DataType::Interval(interval_unit) => match interval_unit {
                IntervalUnit::YearMonth => {
                    SqlType::Interval(Some((DateTimeField::Year, Some(DateTimeField::Month))))
                }
                IntervalUnit::DayTime => {
                    SqlType::Interval(Some((DateTimeField::Day, Some(DateTimeField::Millisecond))))
                }
                // MonthDayNano was added to Arrow because it is closest to how Postgress
                // and BigQuery model intervals.  Each field is independent (e.g. there is
                // no constraint that nanoseconds have the same sign as days or that the
                // quantity of nanoseconds represents less than a day's worth of time).
                // One limitation that might be problematic is that the number of
                // nanoseconds alone can't represent a full +/- 10K years range as
                // required by the SQL spec. But a literal representing
                //
                //     "9999 years, 10 months, 8 days, and 100 milliseconds"
                //
                // can be represented by turning the number of year into a number of
                // months so the nanoseconds part does not overflow the 64 bits
                //
                //     MonthDayNano {
                //        months: 9999 * 12 + 10,
                //        days: 8,
                //        nanos: 100 * 10^-6 * 10^9,
                //     }
                //
                //
                // Internal PostgreSQL representation of intervals
                // ===============================================
                //
                // ```c
                // typedef struct {
                //     int64 time;   // microseconds
                //     int32 day;    // days
                //     int32 month;  // months
                // } Interval;
                //
                // The 64-bit field together with the two 32-bit fields create a struct
                // that needs no padding. So these can be stored contiguously in memory
                // without wasting space.
                // ```
                IntervalUnit::MonthDayNano => SqlType::Interval(Some((
                    DateTimeField::Month,
                    Some(DateTimeField::Nanosecond),
                ))),
            },

            // XXX: things get tricky here and conversions don't really work well yet
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => SqlType::Array(None), // XXX
            DataType::FixedSizeList(_, _) => SqlType::Other("ARRAY".to_string()),
            DataType::Struct(fields) => {
                let mut sql_fields = Vec::with_capacity(fields.len());
                for field in fields {
                    let sql_type = Self::_from_arrow_type(backend, field.data_type());
                    let nullable = field.is_nullable();
                    // XXX: this is not necessarily correct, field names might contain
                    // quote characters that need to be escaped (meaning they should exist
                    // in a Ident::Unquoted). But we don't have that information here.
                    let name = Ident::Plain(field.name().clone());
                    sql_fields.push(StructField::new(name, sql_type, nullable));
                }
                SqlType::Struct(Some(sql_fields))
            }
            DataType::Union(..) => SqlType::Other("UNION".to_string()),
            DataType::Map(..) => SqlType::Map(None), // TODO: handle key/value types
            DataType::Dictionary(_, value_type) => Self::_from_arrow_type(backend, value_type),
            DataType::RunEndEncoded(_, values) => {
                Self::_from_arrow_type(backend, values.as_ref().data_type())
            }
        }
    }

    /// DON'T USE THIS. Try to approximate the best Arrow [DataType] for this SQL type.
    ///
    /// Going from SQL types to Arrow types is lossy because SQL types are more expressive
    /// than Arrow types. This function tries to pick the best matching Arrow type for a
    /// given SQL type. But there are many SQL types that don't have an isomorphic mapping
    /// in Arrow.
    ///
    /// # SQL Timestamps and Apache Arrow types
    ///
    /// ## TIMESTAMP WITH LOCAL TIME ZONE
    ///
    /// *Meaning:* a point in time that is *displayed* and *processed* in the local time zone of
    /// the database client's session.
    ///
    /// *Example:* the scheduled time of a meeting in a calendar app. Each user sees the time
    /// of the meeting in their own time zone, but the meeting happens at a single point in
    /// time.
    ///
    /// *Also known as:* `TIMESTAMP_LTZ`.
    ///
    /// ### Storage
    ///
    /// `Timestamp(Second | Millisecond | Microsecond | Nanosecond, "UTC")` is sufficient to
    /// store a local-tz timestamp because the actual point in time is stored in UTC. But the
    /// type does not capture the fact that the timestamp is supposed to be displayed in the
    /// local time zone of the user. This information has to be conveyed separately, e.g. in
    /// the metadata of an Arrow `Field`.
    ///
    /// ## TIMESTAMP WITH TIME ZONE
    ///
    /// *Meaning:* a point in time that is stored along with a time zone offset. The time zone
    /// offset is used to convert the timestamp to UTC for storage, and to convert it back
    /// to the original time zone when displaying it.
    ///
    /// *Example:* a flight departure and arrival time in an airline reservation system. Both
    /// timestamps are stored along with the local time zone of the airport, so they can be
    /// displayed correctly to users.
    ///
    /// *Also known as:* `TIMESTAMP_TZ`.
    ///
    /// ### Storage
    ///
    /// Most systems store the timestamp in UTC and the time zone offset (in minutes) as
    /// a signed integer. To render the timestamp in the local time zone, the offset is
    /// added to the UTC timestamp. If the system chooses to store the timestamp pre-added
    /// to the offset, it needs to subtract the offset to get the UTC timestamp back.
    ///
    /// When inserting data into the database, we can push a `Timestamp(_, UTC)` along
    /// and the database will store the offset=0 for us. If we push a `Timestamp(_, Some(tz))`
    /// with a non-UTC time zone, the database should convert the timestamp to UTC and resolve
    /// the offset for us. Storing the same offset for all the values we are inserting.
    ///
    /// The situation is more complicated when reading data from the database [1]. Since each
    /// value of the column can have a different offset, we can't represent the data as a simple
    /// `Timestamp(_, Some(tz))` because that assumes a single time zone for the whole column.
    ///
    /// For these situations, we are going to propose and Arrow Extension type [2] that dbt
    /// Fusion and ADBC drivers can adopt:
    ///
    ///     name: "arrow.timestamp_with_offsets"
    ///     storage_type:
    ///         struct<
    ///             timestamp: timestamp[s | ms | us | ns, tz=UTC],
    ///             offset_minutes: int16
    ///         >
    ///
    /// The `timestamp` field stores the point in time in UTC. We recommend always setting the
    /// timezone explicitly to UTC to avoid ambiguity. If not provided, UTC is assumed. Anything
    /// else should be considered an error.
    ///
    /// The `offset_minutes` field stores the time zone offset in minutes that was used to
    /// convert the original timestamp to UTC. This allows reconstructing the original
    /// timestamp in the local time zone by adding the offset to the UTC timestamp.
    ///
    /// ## TIMESTAMP WITHOUT TIME ZONE
    ///
    /// *Meaning:* a data and time literal that is not associated with any time zone.
    /// It represents a calendar date and time that is the same for all users, regardless
    /// of their time zone.
    ///
    /// *Example:* a birth date and time in a user profile. The timestamp is stored
    /// as-is without any conversion or time zone information.
    ///
    /// *Also known as:* `TIMESTAMP_NTZ`, `DATETIME`.
    ///
    /// ### Storage
    ///
    /// `Timestamp(Second | Millisecond | Microsecond | Nanosecond, None)` is sufficient.
    /// The lack of a time zone indicates that the timestamp is to be interpreted as a literal.
    /// So to render it in YYYY-MM-DD HH:MM:SS format, the system just needs to format the
    /// timestamp assuming it is in the UTC time zone, but the resulting string does not
    /// mean the point in time represented by that UTC timestamp.
    ///
    /// [1] Trying to insert a column with different offsets for each value would also not work
    ///     with a `Timestamp(_, Some(tz))` because that assumes a single time zone for the
    ///     whole column.
    /// [2] https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types
    pub fn pick_best_arrow_type(&self, backend: Backend) -> DataType {
        use Backend::*;
        use SqlType::*;
        use TimeZoneSpec::*;

        let arrow_timestamp = |precision: Option<u8>, arrow_tz: Option<Arc<str>>| {
            let time_unit = precision
                .map(suitable_time_unit)
                .unwrap_or_else(|| default_time_unit(backend));
            DataType::Timestamp(time_unit, arrow_tz)
        };
        let arrow_timestamp_with_local_tz =
            |precision: Option<u8>| arrow_timestamp(precision, Some("UTC".into()));
        // TODO: use an extension type for `TIMESTAMP WITH TIME ZONE` becaues SQL
        // timestamps with time zone can have one offset per row, while Arrow's
        // `Timestamp(_, Some(tz))` assumes a single time zone for the whole column.
        let arrow_timestamp_tz = |precision: Option<u8>| {
            let time_unit = precision
                .map(suitable_time_unit)
                .unwrap_or_else(|| default_time_unit(backend));
            let fields = Fields::from(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(time_unit, Some("UTC".into())),
                    false,
                ),
                Field::new("offset_minutes", DataType::Int16, false),
            ]);
            DataType::Struct(fields)
        };

        let data_type: DataType = match (backend, self) {
            (_, Boolean) => DataType::Boolean,

            // Snowflake {{{
            (Snowflake, TinyInt | SmallInt | Integer | BigInt) => DataType::Decimal128(38, 0),
            (Snowflake, Real | Float(_) | Double) => DataType::Float64,
            // "NUMBER" in Snowflake is an alias for "DECIMAL(38,0)"
            (Snowflake, Numeric(None) | BigNumeric(None)) => DataType::Decimal128(38, 0),

            (Snowflake, DateTime) => arrow_timestamp(Some(9), None),
            // }}}

            // BigQuery {{{
            (BigQuery, TinyInt | SmallInt | Integer | BigInt) => DataType::Int64,
            (BigQuery, Numeric(None)) => DataType::Decimal128(38, 9),
            (BigQuery, BigNumeric(None)) => DataType::Decimal256(76, 38),

            // BigQuery's DATETIME has microsecond precision
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type
            (BigQuery, DateTime) => arrow_timestamp(Some(6), None),

            // BigQuery's TIME always has microsecond precision and no time zone
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#time_type
            (
                BigQuery,
                Time {
                    precision: _,
                    time_zone_spec: _,
                },
            ) => DataType::Time64(TimeUnit::Microsecond),

            // BigQuery floats are 64-bit
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#floating_point_types
            (BigQuery, Real | Float(_) | Double) => DataType::Float64,
            // }}}

            // Databricks {{{
            // https://docs.databricks.com/aws/en/sql/language-manual/data-types/decimal-type
            (Databricks, Numeric(None) | BigNumeric(None)) => DataType::Decimal128(10, 0),
            // }}}

            // Redshift {{{
            (Redshift, Numeric(None) | BigNumeric(None)) => {
                // The default precision, if not specified, is 18. The maximum precision is 38.
                // The default scale, if not specified, is 0. The maximum scale is 37.
                // https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html#r_Numeric_types201-decimal-or-numeric-type
                DataType::Decimal128(18, 0)
            }
            // }}}

            // PostgreSQL {{{
            (Postgres | Salesforce, Numeric(None) | BigNumeric(None)) => {
                // PostgreSQL's NUMERIC type is truly arbitrary (precision can go to a 1000!). When
                // precision and scale are not specified, it's truly unconstrained. We pick the max
                // precision that fits in Decimal128 with a scale of 9 as a reasonable default,
                // but it doesn't reflect the full range supported by PostgreSQL.
                //
                // For more portability, PostgreSQL docs recommend users to always specify
                // precision and scale explicitly.
                //
                // https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
                DataType::Decimal128(38, 0)
            }
            // }}}
            (_, TinyInt) => DataType::Int8,
            (_, SmallInt) => DataType::Int16,
            (_, Integer) => DataType::Int32,
            (_, BigInt) => DataType::Int64,

            (_, Real) => DataType::Float32,
            (_, Float(_)) => DataType::Float32,
            (_, Double) => DataType::Float64,

            (_, Numeric(Some((p, s))) | BigNumeric(Some((p, s)))) => {
                let (p, s) = (*p, s.unwrap_or(0));
                if p <= 38 {
                    DataType::Decimal128(p, s)
                } else {
                    DataType::Decimal256(p, s)
                }
            }

            (_, Numeric(None) | BigNumeric(None)) => {
                unimplemented!("defaults for DECIMAL on {backend:?}")
            }

            (_, Char(_)) => DataType::Utf8,
            (_, Varchar(..)) => DataType::Utf8,
            (_, Text) => DataType::Utf8,
            (_, Clob) => DataType::Utf8,
            (_, Blob) => DataType::Binary,
            (_, Binary(_)) => DataType::Binary,
            (_, Date) => DataType::Date32,
            (
                backend,
                Time {
                    precision,
                    time_zone_spec: _, // TODO: handle timezones in TIME type
                },
            ) => {
                let time_unit = match (backend, precision) {
                    (_, Some(precision)) => suitable_time_unit(*precision),
                    // TIME's default precision on Snowflake is 9 (nanoseconds)
                    // https://docs.snowflake.com/en/sql-reference/data-types-datetime#time
                    (Snowflake, None) => TimeUnit::Nanosecond,
                    // TIME's default precision on BigQuery is 6 (microseconds)
                    // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#time_type
                    (BigQuery, None) => TimeUnit::Microsecond,
                    // Databricks doesn't have the TIME type, so we use the precision of its
                    // TIMESTAMP type as the default here, which is 6 (microseconds).
                    // https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-type
                    (Databricks | DatabricksODBC, None) => TimeUnit::Microsecond,
                    // TIME's default precision on Redshift is assumed to matche PostgreSQL's but
                    // nothing is mentioned in the docs page
                    // https://docs.aws.amazon.com/redshift/latest/dg/r_Date_and_time_literals.html#r_Date_and_time_literals-times
                    (Redshift | RedshiftODBC, None) => TimeUnit::Microsecond,
                    // TIME's default precision on PostgreSQL is 6 (microseconds)
                    // https://www.postgresql.org/docs/current/datatype-datetime.html
                    (Postgres | Salesforce, None) => TimeUnit::Microsecond,
                    (Generic { .. }, None) => {
                        // we pick microseconds as a reasonable default
                        TimeUnit::Microsecond
                    }
                };
                match time_unit {
                    TimeUnit::Second | TimeUnit::Millisecond => DataType::Time32(time_unit),
                    TimeUnit::Microsecond | TimeUnit::Nanosecond => DataType::Time64(time_unit),
                }
            }
            (
                backend,
                Timestamp {
                    precision,
                    time_zone_spec,
                },
            ) => {
                match (backend, time_zone_spec) {
                    (_, Local) => arrow_timestamp_with_local_tz(*precision),
                    (_, With) => arrow_timestamp_tz(*precision),

                    // Databricks TIMESTAMP and TIMESTAMP_LTZ are both local-tz timestamps
                    (Databricks, Without | Unspecified) => {
                        arrow_timestamp_with_local_tz(*precision)
                    }
                    // This is configuration on Snowflake (!), but the default for TIMESTAMP
                    // is to be an alias for TIMESTAMP_NTZ which is what we assume here.
                    (Snowflake, Unspecified) => arrow_timestamp(*precision, None),

                    (_, Without | Unspecified) => arrow_timestamp(*precision, None),
                }
            }
            // A DATETIME is a timestamp without time zone information
            (backend, DateTime) => DataType::Timestamp(default_time_unit(backend), None),

            (backend, Interval(fields)) => {
                use DateTimeField::*;
                use IntervalUnit::*;
                let interval_unit = match backend {
                    Snowflake => MonthDayNano, // XXX: intervals types are not supported on Snowflake, only value literals
                    Databricks | DatabricksODBC | Redshift | RedshiftODBC => {
                        // ## Databricks
                        //
                        //     INTERVAL { yearMonthIntervalQualifier | dayTimeIntervalQualifier }
                        //
                        // ## Redshift
                        //
                        //       INTERVAL year_to_month_qualifier
                        //     | INTERVAL day_to_second_qualifier [ (fractional_precision) ]
                        //
                        //  The maximum value of `fractional_precision` is 6 and it only appears
                        //  after SECOND in day_to_second_qualifier. This parser turns `SECOND(3)`
                        //  into `DateTimeField::Millisecond` simplifying the processing below.
                        //
                        // https://docs.databricks.com/aws/en/sql/language-manual/data-types/interval-type
                        // https://docs.aws.amazon.com/redshift/latest/dg/r_interval_data_types.html
                        fields
                            .map(|range| match range {
                                // yearMonthIntervalQualifier: { YEAR [TO MONTH] | MONTH }
                                //
                                // YEAR
                                // MONTH
                                // YEAR TO MONTH
                                (Year, None) | (Year, Some(Month)) | (Month, None) => YearMonth,

                                // ## Databricks
                                //
                                //     dayTimeIntervalQualifier:
                                //       { DAY    [TO { HOUR | MINUTE | SECOND } ] |
                                //         HOUR   [TO {        MINUTE | SECOND } ] |
                                //         MINUTE [TO                   SECOND] |
                                //         SECOND }
                                //
                                // ## Redshift (`INTERVAL day_to_second_qualifier [ (fractional_precision) ]`)
                                //
                                //       DAY
                                //     | HOUR
                                //     | MINUTE
                                //     | SECOND [ (fractional_precision) ]
                                //     | DAY TO HOUR
                                //     | DAY TO MINUTE
                                //     | DAY TO SECOND [ (fractional_precision) ]
                                //     | HOUR TO MINUTE
                                //     | HOUR TO SECOND [ (fractional_precision) ]
                                //     | MINUTE TO SECOND [ (fractional_precision) ]
                                (Day, None | Some(Hour | Minute | Second))
                                | (Hour, None | Some(Minute | Second))
                                | (Minute, None | Some(Second))
                                | (Second, None) => DayTime,
                                // -- Redshift-specific
                                (Day | Hour | Minute | Second, Some(Millisecond)) => DayTime,
                                (Day | Hour | Minute | Second, Some(Microsecond)) => MonthDayNano,

                                // not supported directly, but we map it in some reasonable way
                                (Year, Some(Year)) | (Month, Some(Month)) => {
                                    YearMonth
                                }
                                (Day, Some(Day))
                                | (Hour, Some(Hour))
                                | (Minute, Some(Minute))
                                | (Second, Some(Second))
                                | (Millisecond, None | Some(Millisecond)) => DayTime,
                                // -- anything more precise than millis, requires MonthDayNano
                                (Microsecond, _)
                                | (Nanosecond, _)
                                | (_, Some(Microsecond))
                                | (_, Some(Nanosecond))
                                // -- anything outside of Year-Month or Day-Time ranges
                                | (
                                    Year | Month,
                                    Some(Day | Hour | Minute | Second | Millisecond),
                                )
                                // -- inverted patterns (here so we can rely on exhaustiveness checking)
                                | (
                                    Month | Day | Hour | Minute | Second | Millisecond,
                                    Some(Year),
                                )
                                | (Day | Hour | Minute | Second | Millisecond, Some(Month))
                                | (Hour | Minute | Second | Millisecond, Some(Day))
                                | (Minute | Second | Millisecond, Some(Hour))
                                | (Second | Millisecond, Some(Minute))
                                | (Millisecond, Some(Second)) => MonthDayNano,
                            })
                            .unwrap_or(
                                // INTERVAL in Databricks must have fields specified, but if not,
                                // we pick `MonthDayNano` as a reasonable default that can represent
                                // all possible values.
                                MonthDayNano,
                            )
                    }
                    BigQuery | Postgres => MonthDayNano, // MonthDayNano is exactly what BQ and PG use internally
                    Salesforce => MonthDayNano,          // Salesforce seems to follow PostgreSQL
                    Generic { .. } => MonthDayNano,      // Reasonable default
                };
                DataType::Interval(interval_unit)
            }

            (_, Json) => DataType::Utf8,
            (_, Jsonb) => unimplemented!("{}", self.to_string(backend)),
            (_, Geometry) => unimplemented!("{}", self.to_string(backend)),
            (_, Geography) => DataType::Utf8,
            (_, Array(Some(inner_sql_type))) => {
                let inner_sql_type_string = inner_sql_type.to_string(backend);
                let inner_ty = inner_sql_type.pick_best_arrow_type(backend);
                let inner_metadata = {
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        metadata_sql_type_key(backend).to_string(),
                        inner_sql_type_string,
                    );
                    metadata
                };
                let inner_field = Field::new("item", inner_ty, true).with_metadata(inner_metadata);
                DataType::List(Arc::new(inner_field))
            }
            (_, Array(None)) => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            (_, Struct(fields)) => {
                let arrow_fields = match fields {
                    Some(struct_fields) => {
                        let arrow_fields_vec = struct_fields
                            .iter()
                            .map(
                                |StructField {
                                     name,
                                     sql_type,
                                     nullable,
                                     comment_tok,
                                 }| {
                                    let inner_ty = sql_type.pick_best_arrow_type(backend);

                                    // XXX: can't preserve the original quotes here because an
                                    // Arrow `Field` is meant to keep a name w/o quotes.
                                    //
                                    // TODO(felipecrv): figure out how to preserve quoting status of
                                    // identifiers through Arrow types (e.g. using metadata)
                                    //
                                    // `name.display(backend)` can't be used because it might
                                    // render quotes which are not desired in Arrow field names.
                                    let name = name.to_string_lossy();
                                    // render the SQL type according to the backend-specific syntax
                                    let sql_type_string = sql_type.to_string(backend);

                                    let metadata = {
                                        let mut metadata = HashMap::new();
                                        metadata.insert(
                                            metadata_sql_type_key(backend).to_string(),
                                            sql_type_string,
                                        );
                                        if let Some(tok) = comment_tok {
                                            metadata.insert("comment".to_string(), tok.clone());
                                        }
                                        metadata
                                    };
                                    Field::new(name, inner_ty, *nullable).with_metadata(metadata)
                                },
                            )
                            .collect::<Vec<_>>();
                        Fields::from(arrow_fields_vec)
                    }
                    None => Fields::empty(),
                };
                DataType::Struct(arrow_fields)
            }
            (backend, Map(inner)) => {
                let fallback = Varchar(None, Default::default());
                let (key_type, value_type) = inner
                    .as_ref()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .unwrap_or_else(|| (&fallback, &fallback));
                let entries_struct = Struct(Some(vec![
                    StructField::new(
                        Ident::Plain("key".to_string()),
                        key_type.clone(),
                        false, // keys must be non-nullable in Arrow
                    ),
                    StructField::new(
                        Ident::Plain("value".to_string()),
                        value_type.clone(),
                        true, // values are nullable
                    ),
                ]));
                let entries = Field::new(
                    "entries",
                    entries_struct.pick_best_arrow_type(backend),
                    false,
                );
                DataType::Map(Arc::new(entries), false)
            }
            (_, Variant) => unimplemented!("{}", self.to_string(backend)),
            (_, Void) => unimplemented!("{}", self.to_string(backend)),
            (_, Other(_)) => unimplemented!("{}", self.to_string(backend)),
        };
        data_type
    }
}

// We want drivers, CSV parsers and anything producing Arrow schemas
// that interact with SQL data warehouses to inform us about the SQL type
// they intend to use for each field. These are the metadata keys we
// are supporting for each backend.
//
// The first one is the one we use when writing the Arrow schema, but when
// reading we check all of them in order to be compatible with existing
// schemas that might have been written using different metadata keys.
const POSTGRES_KEYS: [&str; 2] = ["POSTGRES:type", "type_text"];
const SNOWFLAKE_KEYS: [&str; 2] = ["SNOWFLAKE:type", "type_text"];
const BIGQUERY_KEYS: [&str; 4] = ["BIGQUERY:type", "type_text", "Type", "type"];
const DATABRICKS_KEYS: [&str; 2] = ["DBX:type", "type_text"];
const REDSHIFT_KEYS: [&str; 2] = ["REDSHIFT:type", "type_text"];
const GENERIC_KEYS: [&str; 2] = ["SQL:type", "type_text"];

fn metadata_type_candidate_keys(backend: Backend) -> &'static [&'static str] {
    match backend {
        Backend::Postgres | Backend::Salesforce => &POSTGRES_KEYS,
        Backend::Snowflake => &SNOWFLAKE_KEYS,
        Backend::BigQuery => &BIGQUERY_KEYS,
        Backend::Databricks => &DATABRICKS_KEYS,
        Backend::Redshift | Backend::RedshiftODBC => &REDSHIFT_KEYS,
        Backend::DatabricksODBC => &DATABRICKS_KEYS,
        Backend::Generic { .. } => &GENERIC_KEYS,
    }
}

pub fn metadata_sql_type_key(backend: Backend) -> &'static str {
    metadata_type_candidate_keys(backend)[0]
}

/// Get the type string metadata from an Arrow `Field` for a given backend.
pub fn original_type_string(backend: Backend, field: &Field) -> Option<&String> {
    metadata_type_candidate_keys(backend)
        .iter()
        .find_map(|&k| field.metadata().get(k))
}

fn eqi(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

#[derive(Debug)]
enum ParseError<'source> {
    UnexpectedEndOfInput,
    Unexpected(Token<'source>),
    ParseIntError(ParseIntError),
    UnclosedQuote(char),
    ExpectedDateTimeField,
}

impl Error for ParseError<'_> {}

impl fmt::Display for ParseError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEndOfInput => write!(f, "unexpected end of input"),
            ParseError::Unexpected(token) => write!(f, "unexpected token: {token:}"),
            ParseError::ParseIntError(err) => write!(f, "{err}"),
            ParseError::UnclosedQuote(quote) => write!(f, "'{}' is not closed", *quote),
            ParseError::ExpectedDateTimeField => {
                write!(
                    f,
                    "expected a date/time field (e.g. YEAR, DAY, SECOND, etc.)"
                )
            }
        }
    }
}

impl From<ParseIntError> for ParseError<'_> {
    fn from(err: ParseIntError) -> Self {
        ParseError::ParseIntError(err)
    }
}

/// Converts a [Token::Word] to an identifier by removing quotes and resolving escape sequences.
///
/// NOTE: uppercasing IS NOT performed, callers should use [eqi] if case-insensitive comparison is
/// needed. PostgreSQL docs, for instance, say "Quoting an identifier also makes it case-sensitive" [1].
///
/// [1] https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
fn word2ident<'source>(word: String, backend: Backend) -> Result<Ident, ParseError<'source>> {
    let mut bytes = word.bytes();
    let first_byte = bytes.next().ok_or(ParseError::UnexpectedEndOfInput)?;
    let is_quoted = [b'\'', b'"', b'`'].contains(&first_byte);

    // TODO: handle the U&"..." quoted identifiers in PostgreSQL

    if is_quoted {
        // If the first byte is a quote, the last byte must be the same quote.
        // This is not enough to validate the quoted identifier, but it's a necessary
        // condition. More thorough validation is done in _unescape_quoted_ident.
        //
        // For instance, "abc"" passes this check, but is not a valid quoted identifier
        // because the last "" are a escaped "" and the closing quote is missing.
        let open_ended = match bytes.next_back() {
            Some(b) => b != first_byte,
            None => true,
        };
        if open_ended {
            let err = ParseError::UnclosedQuote(first_byte as char);
            return Err(err);
        }
        let s = _unescape_quoted_ident(word.as_ref(), first_byte, backend)?;
        Ok(Ident::Unquoted(first_byte.try_into().unwrap(), s))
    } else {
        // Plain identifier, return as is
        Ok(Ident::Plain(word))
    }
}

/// Unescape a quoted identifier based on the backend rules.
///
/// PRE-CONDITIONS:
/// - `quote` is one of: `"`, `'`, or `` ` ``
/// - `word` is quoted with the same quote character at the start and end.
fn _unescape_quoted_ident<'source>(
    word: &str,
    quote: u8,
    backend: Backend,
) -> Result<String, ParseError<'source>> {
    use Backend::*;

    debug_assert!(word.len() >= 2);
    debug_assert!(word.as_bytes()[0] == quote);
    debug_assert!(word.as_bytes()[word.len() - 1] == quote);

    let inner = &word[1..word.len() - 1];
    // TODO: review all the ident escaping rules for different backends here
    let unescaped_string = match (backend, quote) {
        (Postgres | Redshift | RedshiftODBC, b'"') => {
            // In PostgreSQL, double quotes are escaped by doubling them
            inner.replace("\"\"", "\"")
        }
        (_, b'\'') => {
            // In SQL, single quotes are escaped by doubling them
            inner.replace("''", "'")
        }
        _ => inner.to_string(),
    };
    Ok(unescaped_string)
}

struct Parser<'source> {
    tokenizer: Tokenizer<'source>,
}

impl<'source> Parser<'source> {
    pub fn new(input: &'source str) -> Self {
        Parser {
            tokenizer: Tokenizer::new(input),
        }
    }

    // Basic token operations

    fn next(&mut self) -> Result<Token<'source>, ParseError<'source>> {
        self.tokenizer
            .next()
            .ok_or(ParseError::UnexpectedEndOfInput)
    }

    fn expect(&mut self, expected: Token<'source>) -> Result<(), ParseError<'source>> {
        let tok = self.next()?;
        if tok == expected {
            Ok(())
        } else {
            Err(ParseError::Unexpected(tok))
        }
    }

    fn match_(&mut self, pat: Token<'source>) -> bool {
        self.tokenizer.match_(move |tok| tok == pat)
    }

    fn match_word(&mut self, word: &'source str) -> bool {
        self.match_(Token::Word(word))
    }

    fn next_int<T>(&mut self) -> Result<T, ParseError<'source>>
    where
        T: std::str::FromStr<Err = ParseIntError>,
    {
        let tok = self.next()?;
        if let Token::Word(w) = tok {
            let value = w.parse::<T>()?;
            Ok(value)
        } else {
            Err(ParseError::Unexpected(tok))
        }
    }

    // Grammar productions

    /// Parse optional parenthesized integer value (e.g. `(3)`).
    fn precision<T>(&mut self) -> Result<Option<T>, ParseError<'source>>
    where
        T: std::str::FromStr<Err = ParseIntError>,
    {
        if self.match_(Token::LParen) {
            let value = self.next_int::<T>()?;
            self.expect(Token::RParen)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn precision_and_scale(&mut self) -> Result<Option<(u8, Option<i8>)>, ParseError<'source>> {
        if self.match_(Token::LParen) {
            let precision = self.next_int::<u8>()?;
            let scale = if self.match_(Token::Comma) {
                Some(self.next_int::<i8>()?)
            } else {
                None
            };
            self.expect(Token::RParen)?;
            Ok(Some((precision, scale)))
        } else {
            Ok(None)
        }
    }

    fn time_zone_spec(&mut self) -> Result<TimeZoneSpec, ParseError<'source>> {
        if self.match_word("WITH") {
            let local = self.match_word("LOCAL");
            self.expect(Token::Word("TIME"))?;
            self.expect(Token::Word("ZONE"))?;
            Ok(if local {
                TimeZoneSpec::Local
            } else {
                TimeZoneSpec::With
            })
        } else if self.match_word("WITHOUT") {
            self.expect(Token::Word("TIME"))?;
            self.expect(Token::Word("ZONE"))?;
            Ok(TimeZoneSpec::Without)
        } else {
            Ok(TimeZoneSpec::Unspecified)
        }
    }

    fn datetime_field(&mut self) -> Option<DateTimeField> {
        self.tokenizer.peek_and_then(|tok| {
            if let Token::Word(w) = tok {
                let field = if eqi(w, "YEAR") {
                    DateTimeField::Year
                } else if eqi(w, "MONTH") {
                    DateTimeField::Month
                } else if eqi(w, "DAY") {
                    DateTimeField::Day
                } else if eqi(w, "HOUR") {
                    DateTimeField::Hour
                } else if eqi(w, "MINUTE") {
                    DateTimeField::Minute
                } else if eqi(w, "SECOND") {
                    DateTimeField::Second
                } else if eqi(w, "MILLISECOND") {
                    DateTimeField::Millisecond
                } else if eqi(w, "MICROSECOND") {
                    DateTimeField::Microsecond
                } else if eqi(w, "NANOSECOND") {
                    DateTimeField::Nanosecond
                } else {
                    return None;
                };
                Some(field)
            } else {
                None
            }
        })
    }

    fn interval_qualifier(
        &mut self,
    ) -> Result<Option<(DateTimeField, Option<DateTimeField>)>, ParseError<'source>> {
        if let Some(start) = self.datetime_field() {
            if self.match_word("TO") {
                let end = self.datetime_field();
                if end.is_some() {
                    // XXX: validate end is higher resolution than start unit?
                    return Ok(Some((start, end)));
                } else {
                    return Err(ParseError::ExpectedDateTimeField);
                }
            }
            Ok(Some((start, None)))
        } else {
            Ok(None)
        }
    }

    fn nullable(&mut self) -> Result<Option<bool>, ParseError<'source>> {
        if self.match_word("NOT") {
            self.expect(Token::Word("NULL"))?;
            Ok(Some(false))
        } else if self.match_word("NULLABLE") {
            Ok(Some(true))
        } else {
            Ok(None)
        }
    }

    /// Parse an identifier, which can be a quoted or unquoted word.
    #[allow(dead_code)]
    fn identifier(&mut self, backend: Backend) -> Result<Ident, ParseError<'source>> {
        let tok = self.next()?;
        match tok {
            Token::Word(w) => word2ident(w.to_string(), backend),
            _ => Err(ParseError::Unexpected(tok)),
        }
    }

    /// Parse a string literal.
    ///
    /// Currently used only on Databricks struct field comments (after COMMENT).
    fn string_literal(&mut self) -> Result<Token<'source>, ParseError<'source>> {
        let tok = self.next()?;
        // TODO: check that token starts with a quote for the given backend
        Ok(tok)
    }

    /// Parse the token that comes after COLLATE in a type definition.
    ///
    /// The token is returned *AS IT IS* in the input string, without removing
    /// the quotes or unescaping anything.
    ///
    /// On Snowflake it seems to be a string literal since it uses single quotes
    /// and the characterr for quoting identifiers in Snowflake is the double quote.
    ///
    /// On Databricks it seems to be an identifier. I haven't found examples where this
    /// identifier is quoted because no collation name contains special characters.
    fn collation_spec(&mut self, _backend: Backend) -> Result<String, ParseError<'source>> {
        let tok = self.next()?;
        Ok(tok.to_string())
    }

    fn string_attrs(&mut self, backend: Backend) -> Result<StringAttrs, ParseError<'source>> {
        let mut collate_spec = None;
        loop {
            if collate_spec.is_none() && self.match_word("COLLATE") {
                collate_spec = Some(self.collation_spec(backend)?);
                continue;
            }
            break;
        }
        Ok(StringAttrs { collate_spec })
    }

    /// Parse the inner fields of a struct type after `(` or after `STRUCT<`.
    ///
    /// `terminator` is either `Token::RParen` or `Token::RAndle`.
    fn struct_fields(
        &mut self,
        backend: Backend,
        terminator: Token<'source>,
    ) -> Result<Vec<StructField>, ParseError<'source>> {
        let mut fields = Vec::new();
        loop {
            let tok = self.next()?;
            let name = match tok {
                tok if tok == terminator => break,
                Token::Word(w) => word2ident(w.to_string(), backend)?,
                _ => {
                    let e = ParseError::Unexpected(tok);
                    return Err(e);
                }
            };

            // Databricks supports an optional `:` after the field name,
            // so we consume it if present.
            let _ = self.match_(Token::Colon);

            let (ty, nullable) = self.parse_constrained_type(backend)?;
            // Assume nullable if NOT NULL or NULLABLE is not specified for the field
            let nullable = nullable.unwrap_or(true);

            // Databricks struct fields can have additional attributes:
            //
            //     [COLLATE <ident>] [COMMENT <string>]
            //
            // We also allow COLLATE to happen after COMMENT, but if we find a COLLATE
            // like that, we have to place it in the attributes of the string type itself.
            let mut collate: Option<String> = None;
            let mut comment: Option<Token> = None;
            loop {
                if collate.is_none() && self.match_word("COLLATE") {
                    collate = Some(self.collation_spec(backend)?);
                    continue;
                }
                if comment.is_none() && self.match_word("COMMENT") {
                    comment = Some(self.string_literal()?);
                    continue;
                }
                break;
            }
            let sql_type = match (ty, collate) {
                (SqlType::Varchar(max_len, attrs), Some(collate_spec)) => {
                    let mut attrs = attrs;
                    attrs.collate_spec = Some(collate_spec);
                    SqlType::Varchar(max_len, attrs)
                }
                (ty, Some(_)) => {
                    debug_assert!(false, "COLLATE specified for a non-string type");
                    ty
                }
                (ty, _) => ty,
            };

            let mut field = StructField::new(name, sql_type, nullable);
            if let Some(tok) = comment {
                field.comment_tok = Some(tok.to_string());
            }
            fields.push(field);

            let tok = self.next()?;
            match tok {
                Token::Comma => continue,
                tok if tok == terminator => break,
                _ => {
                    let e = ParseError::Unexpected(tok);
                    return Err(e);
                }
            }
        }
        Ok(fields)
    }

    /// Parse a SQL type that might have a NOT NULL constraint.
    fn parse_constrained_type(
        &mut self,
        backend: Backend,
    ) -> Result<(SqlType, Option<bool>), ParseError<'source>> {
        let sql_type = self.parse_unconstrained_type(backend)?;
        let nullable = self.nullable()?;
        Ok((sql_type, nullable))
    }

    // External API

    /// Parse the SQL type and return it along with a boolean indicating if its nullable.
    fn parse(&mut self, backend: Backend) -> Result<(SqlType, bool), ParseError<'source>> {
        let (ty, nullable) = self.parse_constrained_type(backend)?;
        Ok((ty, nullable.unwrap_or(true)))
    }

    fn parse_unconstrained_type(
        &mut self,
        backend: Backend,
    ) -> Result<SqlType, ParseError<'source>> {
        use Backend::*;
        let mut sql_type = self.parse_inner(backend)?;
        // postfix-[] syntax for arrays in Postgres and Generic SQL
        if matches!(backend, Postgres | Redshift | RedshiftODBC | Generic { .. }) {
            while self.match_(Token::LBracket) {
                self.expect(Token::RBracket)?;
                sql_type = SqlType::Array(Some(Box::new(sql_type)));
            }
        }
        Ok(sql_type)
    }

    /// Parse the SQL type string without consuming the entire string.
    ///
    /// The goal of this function is to create the `SqlType` instance that better represents
    /// the syntax of the SQL type string. For instance, the fact that Snowflake's FLOAT is
    /// actually a DOUBLE PRECISION under the hood, only becomes relevant when picking storage
    /// data structures for the values of this type.
    ///
    /// This parser, parses, it does not validate. If BigQuery accepts BOOL as a synonym for
    /// BOOLEAN, then this parser will accept it too no matter the backend passed to it.
    /// Weirder types like FLOAT4 and FLOAT8 are guarded by a backend check, just in case
    /// some other system in the future decided they mean 4 or 8 bits instead of 4 or 8
    /// bytes like Snowflake.
    ///
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    /// https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
    fn parse_inner(&mut self, backend: Backend) -> Result<SqlType, ParseError<'source>> {
        use Backend::*;
        let tok = self.next()?;
        let sql_type = match tok {
            Token::LParen => {
                // Structs in Postgres are defined by enclosing the fields in parentheses:
                //
                //     CREATE TYPE item_info AS (
                //         product_id INT,
                //         qty        INT
                //     );
                if matches!(backend, Postgres | Redshift | RedshiftODBC | Generic { .. }) {
                    let fields = self.struct_fields(backend, Token::RParen)?;
                    SqlType::Struct(Some(fields))
                } else {
                    return Err(ParseError::Unexpected(tok));
                }
            }
            Token::RParen
            | Token::LBracket
            | Token::RBracket
            | Token::LAngle
            | Token::RAngle
            | Token::Comma
            | Token::Colon => {
                return Err(ParseError::Unexpected(tok));
            }
            Token::Word(w) => {
                if eqi(w, "BOOLEAN") || eqi(w, "BOOL") {
                    SqlType::Boolean
                } else if eqi(w, "TINYINT") || eqi(w, "BYTEINT") {
                    SqlType::TinyInt
                } else if eqi(w, "SMALLINT")
                    || (eqi(w, "INT2") || eqi(w, "SMALLSERIAL") || eqi(w, "SERIAL2"))
                {
                    SqlType::SmallInt
                } else if eqi(w, "INTEGER")
                    || eqi(w, "INT")
                    || eqi(w, "INT4")
                    || eqi(w, "SERIAL")
                    || eqi(w, "SERIAL4")
                {
                    SqlType::Integer
                } else if eqi(w, "BIGINT")
                    || eqi(w, "INT64")
                    || eqi(w, "INT8")
                    || eqi(w, "BIGSERIAL")
                    || eqi(w, "SERIAL8")
                {
                    SqlType::BigInt
                } else if eqi(w, "REAL") {
                    SqlType::Real
                } else if eqi(w, "FLOAT") {
                    let precision = self.precision()?;
                    SqlType::Float(precision)
                } else if eqi(w, "FLOAT4") {
                    // Snowflake also has FLOAT4, and FLOAT8. The names FLOAT, FLOAT4, and FLOAT8
                    // are for compatibility with other systems. Snowflake treats all three as
                    // 64-bit floating-point numbers.
                    //
                    // Postgres has FLOAT4 as an alias for REAL.
                    if matches!(backend, Postgres | Redshift | RedshiftODBC) {
                        SqlType::Real
                    } else {
                        SqlType::Float(None)
                    }
                } else if eqi(w, "FLOAT8") || eqi(w, "FLOAT64") {
                    // Postgres has FLOAT8 as an alias for DOUBLE PRECISION.
                    // BigQuery uses FLOAT64 as an alias for DOUBLE PRECISION.
                    SqlType::Double
                } else if eqi(w, "DOUBLE") {
                    let _ = self.match_word("PRECISION");
                    SqlType::Double
                } else if eqi(w, "DECIMAL")
                    || eqi(w, "NUMERIC")
                    // Snowflake uses NUMBER as an alias for DECIMAL and NUMERIC
                    || eqi(w, "NUMBER")
                    // Snowflake and Databricks support DEC
                    || eqi(w, "DEC")
                {
                    let precision_and_scale = self.precision_and_scale()?;
                    SqlType::Numeric(precision_and_scale)
                } else if eqi(w, "BIGDECIMAL") || eqi(w, "BIGNUMERIC") {
                    // BigQuery has BIGNUMERIC and BIGDECIMAL
                    let precision_and_scale = self.precision_and_scale()?;
                    SqlType::BigNumeric(precision_and_scale)
                } else if eqi(w, "CHAR") || eqi(w, "CHARACTER") || eqi(w, "NCHAR") {
                    if self.match_word("LARGE") {
                        self.expect(Token::Word("OBJECT"))?;
                        SqlType::Clob // CHARACTER LARGE OBJECT
                    } else {
                        let varying = self.match_word("VARYING");
                        let len = self.precision()?;
                        if varying {
                            let attrs = self.string_attrs(backend)?;
                            SqlType::Varchar(len, attrs)
                        } else {
                            SqlType::Char(len)
                        }
                    }
                } else if eqi(w, "VARCHAR") || eqi(w, "NVARCHAR") {
                    let len = self.precision()?;
                    let attrs = self.string_attrs(backend)?;
                    SqlType::Varchar(len, attrs)
                } else if eqi(w, "NATIONAL") {
                    self.expect(Token::Word("CHAR"))?;
                    let varying = self.match_word("VARYING");
                    let len = self.precision()?;
                    if varying {
                        let attrs = self.string_attrs(backend)?;
                        SqlType::Varchar(len, attrs)
                    } else {
                        SqlType::Char(len)
                    }
                } else if eqi(w, "STRING") {
                    // BigQuery uses STRING as an alias for VARCHAR
                    let attrs = self.string_attrs(backend)?;
                    SqlType::Varchar(None, attrs)
                } else if eqi(w, "TEXT") {
                    SqlType::Text
                } else if eqi(w, "CLOB") {
                    SqlType::Clob
                } else if eqi(w, "BLOB") {
                    SqlType::Blob
                } else if eqi(w, "BINARY") {
                    if self.match_word("LARGE") {
                        self.expect(Token::Word("OBJECT"))?;
                        SqlType::Blob // BINARY LARGE OBJECT
                    } else if self.match_word("VARYING") {
                        // BINARY VARYING (Redshift)
                        let len = self.precision()?;
                        SqlType::Binary(len)
                    } else {
                        let len = self.precision()?;
                        SqlType::Binary(len)
                    }
                } else if eqi(w, "VARBINARY")
                    // BigQuery uses BYTES
                    || eqi(w, "BYTES")
                    // PostgreSQL uses BYTEA
                    || eqi(w, "BYTEA")
                    // Redshift also uses VARBYTE and VARBINARY
                    || eqi(w, "VARBYTE")
                {
                    let len = self.precision()?;
                    SqlType::Binary(len)
                } else if eqi(w, "DATE") {
                    SqlType::Date
                } else if eqi(w, "TIME") {
                    let precision = self.precision()?;
                    let time_zone_spec = self.time_zone_spec()?;
                    SqlType::Time {
                        precision,
                        time_zone_spec:
                            // For the TIME type, it's fair to assume that if the time zone
                            // is not specified, then it is WITHOUT time zone. TIMESTAMP is
                            // more complicated because of the different defaults in different
                            // SQL dialects.
                            if let TimeZoneSpec::Unspecified = time_zone_spec {
                                TimeZoneSpec::Without
                            } else {
                                time_zone_spec
                            }
                    }
                } else if eqi(w, "TIMETZ") {
                    SqlType::Time {
                        precision: None,
                        time_zone_spec: TimeZoneSpec::With,
                    }
                } else if eqi(w, "TIMESTAMP") {
                    let precision = self.precision()?;
                    let time_zone_spec = self.time_zone_spec()?;
                    SqlType::Timestamp {
                        precision,
                        time_zone_spec,
                    }
                } else if eqi(w, "TIMESTAMPTZ") {
                    SqlType::Timestamp {
                        precision: None,
                        time_zone_spec: TimeZoneSpec::With,
                    }
                } else if eqi(w, "TIMESTAMP_LTZ") {
                    let precision = self.precision()?;
                    SqlType::Timestamp {
                        precision,
                        time_zone_spec: TimeZoneSpec::Local,
                    }
                } else if eqi(w, "TIMESTAMP_NTZ") {
                    let precision = self.precision()?;
                    SqlType::Timestamp {
                        precision,
                        time_zone_spec: TimeZoneSpec::Without,
                    }
                } else if eqi(w, "DATETIME") {
                    // In Snowflake DATETIME is an alias for TIMESTAMP_NTZ,
                    // but in BigQuery it's not the same as the TIMESTAMP type.
                    let precision = self.precision()?;
                    if backend == Snowflake {
                        SqlType::Timestamp {
                            precision,
                            time_zone_spec: TimeZoneSpec::Without,
                        }
                    } else {
                        SqlType::DateTime
                    }
                } else if eqi(w, "TIMESTAMP_TZ") {
                    let precision = self.precision()?;
                    SqlType::Timestamp {
                        precision,
                        time_zone_spec: TimeZoneSpec::With,
                    }
                } else if eqi(w, "INTERVAL") {
                    // Some backends (like PostgreSQL) support a precision for the sub-second part
                    // instead of having the time unit spelled out. Examples of equivalents:
                    //
                    //     INTERVAL / INTERVAL SECOND
                    //     INTERVAL (3) / INTERVAL MILLISECOND
                    //     INTERVAL MINUTE
                    //     INTERVAL SECOND(3) / INTERVAL MILLISECOND
                    //     INTERVAL DAY TO SECOND(6) / INTERVAL DAY TO MICROSECOND
                    //
                    // PostgreSQL treats the YEAR, MONTH TO DAY, etc., in an interval type
                    // declaration as decorative metadata, not an actual constraint. But this
                    // parser will preserve that metadata so that it can be used when rendering
                    // the type as a string.
                    let qualifier = match self.interval_qualifier()? {
                        Some((start, None)) => {
                            if matches!(start, DateTimeField::Second) {
                                self.precision()?
                                    .map(DateTimeField::from_precision)
                                    .map(|unit| (unit, None))
                                    .or(Some((start, None)))
                            } else {
                                Some((start, None))
                            }
                        }
                        Some((start, Some(end))) => {
                            if matches!(end, DateTimeField::Second) {
                                self.precision()?
                                    .map(DateTimeField::from_precision)
                                    .map(|unit| (start, Some(unit)))
                                    .or(Some((start, Some(end))))
                            } else {
                                Some((start, Some(end)))
                            }
                        }
                        None => self
                            .precision()?
                            .map(DateTimeField::from_precision)
                            .map(|unit| (unit, None)),
                    };
                    SqlType::Interval(qualifier)
                } else if eqi(w, "JSON") {
                    SqlType::Json
                } else if eqi(w, "JSONB") {
                    SqlType::Jsonb
                } else if eqi(w, "GEOMETRY") {
                    SqlType::Geometry
                } else if eqi(w, "GEOGRAPHY") {
                    SqlType::Geography
                } else if eqi(w, "ARRAY") {
                    let (left, right) = match backend {
                        Snowflake => (Token::LParen, Token::RParen),
                        _ => (Token::LAngle, Token::RAngle),
                    };
                    if self.match_(left) {
                        let inner_type = self.parse_unconstrained_type(backend)?;
                        self.expect(right)?;
                        SqlType::Array(Some(Box::new(inner_type)))
                    } else {
                        SqlType::Array(None)
                    }
                } else if eqi(w, "RECORD") {
                    // In some scenarios, we get "RECORD" as a type from BigQuery.
                    // That just means a generic struct.
                    SqlType::Struct(None)
                } else if eqi(w, "OBJECT") || eqi(w, "STRUCT") {
                    let (left, right) = match backend {
                        Snowflake => (Token::LParen, Token::RParen),
                        _ => (Token::LAngle, Token::RAngle),
                    };
                    let inner_fields = if self.match_(left) {
                        let fields = self.struct_fields(backend, right)?;
                        Some(fields)
                    } else {
                        None
                    };
                    SqlType::Struct(inner_fields)
                } else if eqi(w, "MAP") {
                    let kv = if self.match_(Token::LAngle) {
                        let key_type = self.parse_unconstrained_type(backend)?;
                        self.expect(Token::Comma)?;
                        let value_type = self.parse_unconstrained_type(backend)?;
                        self.expect(Token::RAngle)?;
                        Some((Box::new(key_type), Box::new(value_type)))
                    } else {
                        None
                    };
                    SqlType::Map(kv)
                } else if eqi(w, "VARIANT") {
                    SqlType::Variant
                } else if eqi(w, "VOID") {
                    SqlType::Void
                } else {
                    // gather all tokens before "[NOT] NULL" and return Other(..)
                    let mut other = w.to_string();
                    while let Some(tok) = {
                        self.tokenizer.peek_and_then(|t| {
                            if t == Token::Word("NOT") || t == Token::Word("NULL") {
                                None
                            } else {
                                Some(t)
                            }
                        })
                    } {
                        use fmt::Write as _;
                        write!(&mut other, " {tok}").unwrap();
                    }
                    SqlType::Other(other)
                }
            }
        };
        // Other PostgreSQL types that we don't explicitly support yet:
        //
        //     money       currency amount
        //
        //     bit [ (n) ]                             fixed-length bit string
        //     bit varying [ (n) ] / varbit [ (n) ]    variable-length bit string
        //
        //     cidr        IPv4 or IPv6 network address
        //     inet        IPv4 or IPv6 host address
        //     macaddr     MAC (Media Access Control) address
        //     macaddr8    MAC (Media Access Control) address (EUI-64 format)
        //
        //     point       geometric point on a plane
        //     polygon     closed geometric path on a plane
        //     box         rectangular box on a plane
        //     circle      circle on a plane
        //     line        infinite line on a plane
        //     lseg        line segment on a plane
        //     path        geometric path on a plane
        //
        //     tsquery     text search query
        //     tsvector    text search document
        //     uuid        universally unique identifier
        //     xml
        //
        //     pg_lsn         PostgreSQL Log Sequence Number
        //     pg_snapshot    user-level transaction ID snapshot
        //     txid_snapshot  user-level transaction ID snapshot (deprecated; see pg_snapshot)
        Ok(sql_type)
    }
}
