use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Results of `DESCRIBE TABLE EXTENDED {database}.{schema}.{identifier} AS JSON;`
/// https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct DatabricksDescribeTableExtended {
    pub table_name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub namespace: Vec<String>,
    #[serde(rename = "type")]
    pub type_: String,
    pub provider: Option<String>,
    pub columns: Vec<DatabricksColumnInfo>,
    pub partition_values: BTreeMap<String, String>,
    pub partition_columns: Vec<String>,
    pub location: String,
    pub view_text: Option<String>,
    pub view_original_text: Option<String>,
    pub view_schema_mode: Option<String>,
    pub view_catalog_and_namespace: Option<String>,
    pub view_query_output_columns: Option<Vec<String>>,
    pub comment: String,
    pub table_properties: BTreeMap<String, String>,
    pub statistics: Option<DatabricksTableStatistics>,
    pub storage_properties: BTreeMap<String, String>,
    pub serde_library: String,
    pub input_format: String,
    pub output_format: String,
    pub num_buckets: Option<i32>,
    pub bucket_columns: Vec<String>,
    pub sort_columns: Vec<String>,
    pub created_time: String,
    pub created_by: String,
    pub last_access: String,
    pub partition_provider: Option<String>,
    pub collation: Option<String>,
    pub language: Option<String>,
    pub row_filter: Option<DatabricksRowFilter>,
    pub column_masks: Vec<DatabricksColumnMask>,
    pub owner: String, // this field isn't documented but is returned
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabricksColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: DatabricksColumnTypeInfo,
    pub comment: Option<String>,
    pub nullable: bool,
    pub default: Option<String>,
    pub is_measure: Option<bool>, // for measure columns
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct DatabricksTableStatistics {
    pub num_rows: Option<i64>,
    pub size_in_bytes: Option<i64>,
    pub table_change_stats: Option<DatabricksTableChangeStats>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct DatabricksTableChangeStats {
    pub inserted: Option<i64>,
    pub deleted: Option<i64>,
    pub updated: Option<i64>,
    pub change_percent: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct DatabricksRowFilter {
    pub filter_function: DatabricksFunctionReference,
    pub arguments: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct DatabricksColumnMask {
    pub column_name: String,
    pub mask_function: DatabricksFunctionReference,
    pub arguments: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct DatabricksFunctionReference {
    pub catalog_name: String,
    pub schema_name: String,
    pub function_name: String,
    pub specific_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
#[serde(rename_all = "lowercase")]
pub enum DatabricksColumnTypeInfo {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal {
        precision: u8,
        scale: u8,
    },
    String {
        collation: Option<String>,
    },
    VarChar {
        length: u32,
    },
    Char {
        length: u32,
    },
    Binary,
    Boolean,
    Date,
    #[serde(rename = "timestamp_ltz")]
    Timestamp,
    #[serde(rename = "timestamp_ntz")]
    TimestampNtz,
    Interval {
        start_unit: String,
        end_unit: Option<String>,
    },
    Array {
        element_type: Box<DatabricksColumnTypeInfo>,
        element_nullable: bool,
    },
    Map {
        key_type: Box<DatabricksColumnTypeInfo>,
        value_type: Box<DatabricksColumnTypeInfo>,
        element_nullable: bool,
    },
    Struct {
        fields: Vec<DatabricksStructFieldInfo>,
    },
    Variant,
}

impl DatabricksColumnTypeInfo {
    pub fn name(&self) -> &str {
        match self {
            DatabricksColumnTypeInfo::TinyInt => "tinyint",
            DatabricksColumnTypeInfo::SmallInt => "smallint",
            DatabricksColumnTypeInfo::Int => "int",
            DatabricksColumnTypeInfo::BigInt => "bigint",
            DatabricksColumnTypeInfo::Float => "float",
            DatabricksColumnTypeInfo::Double => "double",
            DatabricksColumnTypeInfo::Decimal { .. } => "decimal",
            DatabricksColumnTypeInfo::String { .. } => "string",
            DatabricksColumnTypeInfo::VarChar { .. } => "varchar",
            DatabricksColumnTypeInfo::Char { .. } => "char",
            DatabricksColumnTypeInfo::Binary => "binary",
            DatabricksColumnTypeInfo::Boolean => "boolean",
            DatabricksColumnTypeInfo::Date => "date",
            DatabricksColumnTypeInfo::Timestamp => "timestamp_ltz",
            DatabricksColumnTypeInfo::TimestampNtz => "timestamp_ntz",
            DatabricksColumnTypeInfo::Interval { .. } => "interval",
            DatabricksColumnTypeInfo::Array { .. } => "array",
            DatabricksColumnTypeInfo::Map { .. } => "map",
            DatabricksColumnTypeInfo::Struct { .. } => "struct",
            DatabricksColumnTypeInfo::Variant => "variant",
        }
    }

    /// Converts [DatabricksColumnTypeInfo] into Raw Databricks SQL Type
    /// https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes
    pub fn raw_type(&self) -> String {
        match &self {
            DatabricksColumnTypeInfo::TinyInt
            | DatabricksColumnTypeInfo::SmallInt
            | DatabricksColumnTypeInfo::Int
            | DatabricksColumnTypeInfo::BigInt
            | DatabricksColumnTypeInfo::Float
            | DatabricksColumnTypeInfo::Double
            | DatabricksColumnTypeInfo::Binary
            | DatabricksColumnTypeInfo::Boolean
            | DatabricksColumnTypeInfo::Date
            | DatabricksColumnTypeInfo::Timestamp
            | DatabricksColumnTypeInfo::TimestampNtz
            | DatabricksColumnTypeInfo::Variant => self.name().to_string(),
            DatabricksColumnTypeInfo::Decimal { precision, scale } => {
                format!("DECIMAL({precision},{scale})")
            }
            DatabricksColumnTypeInfo::String { collation: _ } => {
                // todo: collation syntax is undocumented, figure out how to apply it
                "STRING".to_string()
            }
            DatabricksColumnTypeInfo::VarChar { length } => {
                // not documented - under the hood it's a String with length constraint
                format!("VARCHAR({length})")
            }
            DatabricksColumnTypeInfo::Char { length } => {
                format!("CHAR({length})")
            }
            DatabricksColumnTypeInfo::Interval {
                start_unit,
                end_unit,
            } => {
                if let Some(end_unit) = end_unit {
                    format!("INTERVAL {start_unit} TO {end_unit}")
                } else {
                    format!("INTERVAL {start_unit}")
                }
            }
            DatabricksColumnTypeInfo::Array {
                element_type,
                element_nullable: _,
            } => {
                // todo: element_nullable isn't possible to set here, find workaround
                format!("ARRAY<{}>", element_type.raw_type())
            }
            DatabricksColumnTypeInfo::Map {
                key_type,
                value_type,
                element_nullable: _,
            } => {
                // todo: element_nullable isn't possible to set here
                format!("MAP<{},{}>", key_type.raw_type(), value_type.raw_type())
            }
            DatabricksColumnTypeInfo::Struct { fields } => {
                format!(
                    "STRUCT<{}>",
                    fields
                        .iter()
                        .map(|field| {
                            format!(
                                "{}:{}{}{}",
                                field.name,
                                field.type_.raw_type(),
                                if field.nullable { "" } else { " NOT NULL" },
                                if let Some(comment) = &field.comment {
                                    // escape the comment string literal
                                    format!(" COMMENT \"{}\"", comment.replace("\"", "\\\""))
                                } else {
                                    "".to_string()
                                }
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabricksStructFieldInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: DatabricksColumnTypeInfo,
    pub nullable: bool,
    pub comment: Option<String>,
    pub default: Option<String>,
}

// based on samples from the docs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_type_raw_type() {
        // Test basic primitive types
        assert_eq!(DatabricksColumnTypeInfo::TinyInt.raw_type(), "tinyint");
        assert_eq!(DatabricksColumnTypeInfo::SmallInt.raw_type(), "smallint");
        assert_eq!(DatabricksColumnTypeInfo::Int.raw_type(), "int");
        assert_eq!(DatabricksColumnTypeInfo::BigInt.raw_type(), "bigint");
        assert_eq!(DatabricksColumnTypeInfo::Float.raw_type(), "float");
        assert_eq!(DatabricksColumnTypeInfo::Double.raw_type(), "double");
        assert_eq!(DatabricksColumnTypeInfo::Binary.raw_type(), "binary");
        assert_eq!(DatabricksColumnTypeInfo::Boolean.raw_type(), "boolean");
        assert_eq!(DatabricksColumnTypeInfo::Date.raw_type(), "date");
        assert_eq!(
            DatabricksColumnTypeInfo::Timestamp.raw_type(),
            "timestamp_ltz"
        );
        assert_eq!(
            DatabricksColumnTypeInfo::TimestampNtz.raw_type(),
            "timestamp_ntz"
        );
        assert_eq!(DatabricksColumnTypeInfo::Variant.raw_type(), "variant");
    }

    #[test]
    fn test_decimal_raw_type() {
        let decimal = DatabricksColumnTypeInfo::Decimal {
            precision: 10,
            scale: 2,
        };
        assert_eq!(decimal.raw_type(), "DECIMAL(10,2)");
    }

    #[test]
    fn test_string_types_raw_type() {
        let string = DatabricksColumnTypeInfo::String {
            collation: Some("UTF8_BINARY".to_string()),
        };
        assert_eq!(string.raw_type(), "STRING");

        let varchar = DatabricksColumnTypeInfo::VarChar { length: 255 };
        assert_eq!(varchar.raw_type(), "VARCHAR(255)");

        let char = DatabricksColumnTypeInfo::Char { length: 10 };
        assert_eq!(char.raw_type(), "CHAR(10)");
    }

    #[test]
    fn test_interval_raw_type() {
        // Single unit intervals
        let year_interval = DatabricksColumnTypeInfo::Interval {
            start_unit: "year".to_string(),
            end_unit: None,
        };
        assert_eq!(year_interval.raw_type(), "INTERVAL year");

        let month_interval = DatabricksColumnTypeInfo::Interval {
            start_unit: "month".to_string(),
            end_unit: None,
        };
        assert_eq!(month_interval.raw_type(), "INTERVAL month");

        // Range intervals
        let year_to_month = DatabricksColumnTypeInfo::Interval {
            start_unit: "year".to_string(),
            end_unit: Some("month".to_string()),
        };
        assert_eq!(year_to_month.raw_type(), "INTERVAL year TO month");

        let day_to_hour = DatabricksColumnTypeInfo::Interval {
            start_unit: "day".to_string(),
            end_unit: Some("hour".to_string()),
        };
        assert_eq!(day_to_hour.raw_type(), "INTERVAL day TO hour");
    }

    #[test]
    fn test_simple_array_raw_type() {
        // Array of primitive types
        let int_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::Int),
            element_nullable: true,
        };
        assert_eq!(int_array.raw_type(), "ARRAY<int>");

        let string_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
            element_nullable: false,
        };
        assert_eq!(string_array.raw_type(), "ARRAY<STRING>");

        let decimal_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::Decimal {
                precision: 15,
                scale: 3,
            }),
            element_nullable: true,
        };
        assert_eq!(decimal_array.raw_type(), "ARRAY<DECIMAL(15,3)>");
    }

    #[test]
    fn test_nested_array_raw_type() {
        let nested_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::Array {
                element_type: Box::new(DatabricksColumnTypeInfo::Int),
                element_nullable: true,
            }),
            element_nullable: false,
        };
        assert_eq!(nested_array.raw_type(), "ARRAY<ARRAY<int>>");

        let triple_nested = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::Array {
                element_type: Box::new(DatabricksColumnTypeInfo::Array {
                    element_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
                    element_nullable: true,
                }),
                element_nullable: false,
            }),
            element_nullable: true,
        };
        assert_eq!(triple_nested.raw_type(), "ARRAY<ARRAY<ARRAY<STRING>>>");
    }

    #[test]
    fn test_array_with_complex_elements_raw_type() {
        // Array of structs
        let struct_field = DatabricksStructFieldInfo {
            name: "id".to_string(),
            type_: DatabricksColumnTypeInfo::Int,
            nullable: false,
            comment: None,
            default: None,
        };

        let struct_type = DatabricksColumnTypeInfo::Struct {
            fields: vec![struct_field],
        };

        let struct_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(struct_type),
            element_nullable: true,
        };
        assert_eq!(struct_array.raw_type(), "ARRAY<STRUCT<id:int NOT NULL>>");

        // Array of maps
        let map_type = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
            value_type: Box::new(DatabricksColumnTypeInfo::Int),
            element_nullable: true,
        };

        let map_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(map_type),
            element_nullable: false,
        };
        assert_eq!(map_array.raw_type(), "ARRAY<MAP<STRING,int>>");
    }

    #[test]
    fn test_simple_map_raw_type() {
        // Map with primitive types
        let string_int_map = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
            value_type: Box::new(DatabricksColumnTypeInfo::Int),
            element_nullable: true,
        };
        assert_eq!(string_int_map.raw_type(), "MAP<STRING,int>");

        let int_double_map = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::Int),
            value_type: Box::new(DatabricksColumnTypeInfo::Double),
            element_nullable: false,
        };
        assert_eq!(int_double_map.raw_type(), "MAP<int,double>");
    }

    #[test]
    fn test_complex_map_raw_type() {
        // Map with complex key/value types
        let map_with_decimal_key = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::Decimal {
                precision: 10,
                scale: 2,
            }),
            value_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
            element_nullable: true,
        };
        assert_eq!(map_with_decimal_key.raw_type(), "MAP<DECIMAL(10,2),STRING>");

        let map_with_array_values = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::Int),
            value_type: Box::new(DatabricksColumnTypeInfo::Array {
                element_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
                element_nullable: true,
            }),
            element_nullable: false,
        };
        assert_eq!(map_with_array_values.raw_type(), "MAP<int,ARRAY<STRING>>");
    }

    #[test]
    fn test_nested_map_raw_type() {
        let nested_map = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
            value_type: Box::new(DatabricksColumnTypeInfo::Map {
                key_type: Box::new(DatabricksColumnTypeInfo::Int),
                value_type: Box::new(DatabricksColumnTypeInfo::Double),
                element_nullable: true,
            }),
            element_nullable: false,
        };
        assert_eq!(nested_map.raw_type(), "MAP<STRING,MAP<int,double>>");

        // Map with array of maps as values
        let map_with_array_of_maps = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::Int),
            value_type: Box::new(DatabricksColumnTypeInfo::Array {
                element_type: Box::new(DatabricksColumnTypeInfo::Map {
                    key_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
                    value_type: Box::new(DatabricksColumnTypeInfo::Boolean),
                    element_nullable: true,
                }),
                element_nullable: false,
            }),
            element_nullable: true,
        };
        assert_eq!(
            map_with_array_of_maps.raw_type(),
            "MAP<int,ARRAY<MAP<STRING,boolean>>>"
        );
    }

    #[test]
    fn test_simple_struct_raw_type() {
        // Struct with basic fields
        let simple_struct = DatabricksColumnTypeInfo::Struct {
            fields: vec![
                DatabricksStructFieldInfo {
                    name: "id".to_string(),
                    type_: DatabricksColumnTypeInfo::Int,
                    nullable: false,
                    comment: None,
                    default: None,
                },
                DatabricksStructFieldInfo {
                    name: "name".to_string(),
                    type_: DatabricksColumnTypeInfo::String { collation: None },
                    nullable: true,
                    comment: None,
                    default: None,
                },
            ],
        };
        assert_eq!(
            simple_struct.raw_type(),
            "STRUCT<id:int NOT NULL,name:STRING>"
        );
    }

    #[test]
    fn test_struct_with_comments_raw_type() {
        // Struct with field comments
        let struct_with_comments = DatabricksColumnTypeInfo::Struct {
            fields: vec![
                DatabricksStructFieldInfo {
                    name: "user_id".to_string(),
                    type_: DatabricksColumnTypeInfo::BigInt,
                    nullable: false,
                    comment: Some("Unique user identifier".to_string()),
                    default: None,
                },
                DatabricksStructFieldInfo {
                    name: "email".to_string(),
                    type_: DatabricksColumnTypeInfo::String { collation: None },
                    nullable: true,
                    comment: Some("User email address".to_string()),
                    default: None,
                },
            ],
        };
        assert_eq!(
            struct_with_comments.raw_type(),
            "STRUCT<user_id:bigint NOT NULL COMMENT \"Unique user identifier\",email:STRING COMMENT \"User email address\">"
        );
    }

    #[test]
    fn test_struct_with_escaped_comments_raw_type() {
        // Struct with comments containing quotes that need escaping
        let struct_with_escaped_comments = DatabricksColumnTypeInfo::Struct {
            fields: vec![DatabricksStructFieldInfo {
                name: "description".to_string(),
                type_: DatabricksColumnTypeInfo::String { collation: None },
                nullable: true,
                comment: Some("Contains \"quoted\" text".to_string()),
                default: None,
            }],
        };
        assert_eq!(
            struct_with_escaped_comments.raw_type(),
            "STRUCT<description:STRING COMMENT \"Contains \\\"quoted\\\" text\">"
        );
    }

    #[test]
    fn test_struct_with_complex_fields_raw_type() {
        // Struct with array and map fields
        let complex_struct = DatabricksColumnTypeInfo::Struct {
            fields: vec![
                DatabricksStructFieldInfo {
                    name: "tags".to_string(),
                    type_: DatabricksColumnTypeInfo::Array {
                        element_type: Box::new(DatabricksColumnTypeInfo::String {
                            collation: None,
                        }),
                        element_nullable: true,
                    },
                    nullable: true,
                    comment: None,
                    default: None,
                },
                DatabricksStructFieldInfo {
                    name: "metadata".to_string(),
                    type_: DatabricksColumnTypeInfo::Map {
                        key_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
                        value_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
                        element_nullable: true,
                    },
                    nullable: false,
                    comment: None,
                    default: None,
                },
            ],
        };
        assert_eq!(
            complex_struct.raw_type(),
            "STRUCT<tags:ARRAY<STRING>,metadata:MAP<STRING,STRING> NOT NULL>"
        );
    }

    #[test]
    fn test_nested_struct_raw_type() {
        // Struct containing nested structs
        let address_struct = DatabricksColumnTypeInfo::Struct {
            fields: vec![
                DatabricksStructFieldInfo {
                    name: "street".to_string(),
                    type_: DatabricksColumnTypeInfo::String { collation: None },
                    nullable: true,
                    comment: None,
                    default: None,
                },
                DatabricksStructFieldInfo {
                    name: "city".to_string(),
                    type_: DatabricksColumnTypeInfo::String { collation: None },
                    nullable: true,
                    comment: None,
                    default: None,
                },
            ],
        };

        let person_struct = DatabricksColumnTypeInfo::Struct {
            fields: vec![
                DatabricksStructFieldInfo {
                    name: "name".to_string(),
                    type_: DatabricksColumnTypeInfo::String { collation: None },
                    nullable: false,
                    comment: None,
                    default: None,
                },
                DatabricksStructFieldInfo {
                    name: "address".to_string(),
                    type_: address_struct,
                    nullable: true,
                    comment: None,
                    default: None,
                },
            ],
        };
        assert_eq!(
            person_struct.raw_type(),
            "STRUCT<name:STRING NOT NULL,address:STRUCT<street:STRING,city:STRING>>"
        );
    }

    #[test]
    fn test_deeply_nested_complex_types_raw_type() {
        // Test cominations of arrays, maps, and structs
        let inner_struct = DatabricksColumnTypeInfo::Struct {
            fields: vec![
                DatabricksStructFieldInfo {
                    name: "key".to_string(),
                    type_: DatabricksColumnTypeInfo::String { collation: None },
                    nullable: false,
                    comment: None,
                    default: None,
                },
                DatabricksStructFieldInfo {
                    name: "value".to_string(),
                    type_: DatabricksColumnTypeInfo::Int,
                    nullable: true,
                    comment: None,
                    default: None,
                },
            ],
        };

        let struct_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(inner_struct),
            element_nullable: true,
        };

        let map_of_struct_arrays = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
            value_type: Box::new(struct_array),
            element_nullable: false,
        };

        let array_of_maps = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(map_of_struct_arrays),
            element_nullable: true,
        };

        let final_struct = DatabricksColumnTypeInfo::Struct {
            fields: vec![DatabricksStructFieldInfo {
                name: "complex_data".to_string(),
                type_: array_of_maps,
                nullable: true,
                comment: Some("Complex nested data structure".to_string()),
                default: None,
            }],
        };

        assert_eq!(
            final_struct.raw_type(),
            "STRUCT<complex_data:ARRAY<MAP<STRING,ARRAY<STRUCT<key:STRING NOT NULL,value:int>>>> COMMENT \"Complex nested data structure\">"
        );
    }

    #[test]
    fn test_interval_in_complex_types_raw_type() {
        // Test intervals within complex types
        let interval_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::Interval {
                start_unit: "day".to_string(),
                end_unit: Some("hour".to_string()),
            }),
            element_nullable: true,
        };
        assert_eq!(interval_array.raw_type(), "ARRAY<INTERVAL day TO hour>");

        let interval_map = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::String { collation: None }),
            value_type: Box::new(DatabricksColumnTypeInfo::Interval {
                start_unit: "year".to_string(),
                end_unit: None,
            }),
            element_nullable: false,
        };
        assert_eq!(interval_map.raw_type(), "MAP<STRING,INTERVAL year>");

        let struct_with_interval = DatabricksColumnTypeInfo::Struct {
            fields: vec![DatabricksStructFieldInfo {
                name: "duration".to_string(),
                type_: DatabricksColumnTypeInfo::Interval {
                    start_unit: "month".to_string(),
                    end_unit: None,
                },
                nullable: true,
                comment: None,
                default: None,
            }],
        };
        assert_eq!(
            struct_with_interval.raw_type(),
            "STRUCT<duration:INTERVAL month>"
        );
    }

    #[test]
    fn test_decimal_in_complex_types_raw_type() {
        let decimal_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::Decimal {
                precision: 38,
                scale: 18,
            }),
            element_nullable: true,
        };
        assert_eq!(decimal_array.raw_type(), "ARRAY<DECIMAL(38,18)>");

        let map_with_decimal = DatabricksColumnTypeInfo::Map {
            key_type: Box::new(DatabricksColumnTypeInfo::Decimal {
                precision: 10,
                scale: 2,
            }),
            value_type: Box::new(DatabricksColumnTypeInfo::Decimal {
                precision: 20,
                scale: 10,
            }),
            element_nullable: true,
        };
        assert_eq!(
            map_with_decimal.raw_type(),
            "MAP<DECIMAL(10,2),DECIMAL(20,10)>"
        );
    }

    #[test]
    fn test_timestamp_types_in_complex_types_raw_type() {
        let timestamp_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::Timestamp),
            element_nullable: true,
        };
        assert_eq!(timestamp_array.raw_type(), "ARRAY<timestamp_ltz>");

        let timestamp_ntz_array = DatabricksColumnTypeInfo::Array {
            element_type: Box::new(DatabricksColumnTypeInfo::TimestampNtz),
            element_nullable: false,
        };
        assert_eq!(timestamp_ntz_array.raw_type(), "ARRAY<timestamp_ntz>");

        let struct_with_timestamps = DatabricksColumnTypeInfo::Struct {
            fields: vec![
                DatabricksStructFieldInfo {
                    name: "created_at".to_string(),
                    type_: DatabricksColumnTypeInfo::Timestamp,
                    nullable: false,
                    comment: None,
                    default: None,
                },
                DatabricksStructFieldInfo {
                    name: "updated_at".to_string(),
                    type_: DatabricksColumnTypeInfo::TimestampNtz,
                    nullable: true,
                    comment: None,
                    default: None,
                },
            ],
        };
        assert_eq!(
            struct_with_timestamps.raw_type(),
            "STRUCT<created_at:timestamp_ltz NOT NULL,updated_at:timestamp_ntz>"
        );
    }
}
