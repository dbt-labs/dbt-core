use crate::Backend;
use crate::sql::ident::*;
use crate::sql::types::*;

use Backend::*;
use DateTimeField::*;
use SqlType::*;

fn assert_parses_to(line: u32, input: &str, expected: &SqlType, backend: Backend) {
    let (parsed, _nullable) = SqlType::parse(backend, input).unwrap();
    let rendered = parsed.to_string(backend);
    let expected_rendered = expected.to_string(backend);
    assert_eq!(
        rendered,
        expected_rendered,
        "input: {input}, expected: {expected:?} ({backend}) from {}:{line}",
        file!()
    );
}

/// Test that parsing leads to the expected SqlType on every backend.
///
/// Uses [assert_parses_to] for every pair.
#[test]
fn test_parser() {
    let data_for_backend = |backend: Backend| {
        vec![
            (line!(), "   boOL ", Boolean),
            (line!(), "boOLEan ", Boolean),
            (line!(), " tinyint", TinyInt),
            (line!(), "smallint", SmallInt),
            (line!(), "int2    ", SmallInt),
            (line!(), "smallserial", SmallInt),
            (line!(), "serial2 ", SmallInt),
            (line!(), "iNTEger ", Integer),
            (line!(), " iNT    ", Integer),
            (line!(), " Int4   ", Integer),
            (line!(), "serial  ", Integer),
            (line!(), " Serial4", Integer),
            (line!(), " bigint ", BigInt),
            (line!(), "bigserial", BigInt),
            (line!(), "serial8 ", BigInt),
            (line!(), " int8   ", BigInt),
            // (line!(), "  real  ", Float(None)), // Real/Float/Double depending on backend
            // (line!(), " float4 ", Double), // Float/Double depending on backend
            (line!(), " float8 ", Double),
            (line!(), "Float64 ", Double),
            (line!(), "  douBLE", Double),
            (line!(), "double PRECIsion", Double),
            (line!(), "DECimal         ", Numeric(None)),
            (line!(), "decimal(20)     ", Numeric(Some((20, None)))),
            (line!(), "deCImal( 60,  2)", Numeric(Some((60, Some(2))))),
            (line!(), "NUMeric         ", Numeric(None)),
            (line!(), "numeric(20)     ", Numeric(Some((20, None)))),
            (line!(), "nuMERic( 60,  2)", Numeric(Some((60, Some(2))))),
            (line!(), "bigDECimal      ", BigNumeric(None)),
            (line!(), "bignumeric(20)  ", BigNumeric(Some((20, None)))),
            (line!(), "bigdeCIMal(60,2)", BigNumeric(Some((60, Some(2))))),
            (line!(), "bigNUMeric      ", BigNumeric(None)),
            (line!(), "bignumeric(20)  ", BigNumeric(Some((20, None)))),
            (line!(), "bignuMERic(60,2)", BigNumeric(Some((60, Some(2))))),
            (line!(), "CHar         ", Char(None)),
            (line!(), "CHar(20)     ", Char(Some(20))),
            (line!(), "chARACter    ", Char(None)),
            (line!(), "chARACter(20)", Char(Some(20))),
            (
                line!(),
                "charaCTER VARying      ",
                Varchar(None, Default::default()),
            ),
            (
                line!(),
                "charaCTER VARying (20 )",
                Varchar(Some(20), Default::default()),
            ),
            (line!(), "natioNAL CHar          ", Char(None)),
            (
                line!(),
                "natioNAL CHar vaRying  ",
                Varchar(None, Default::default()),
            ),
            (
                line!(),
                "VARCHAR COLLATE 'utf8'",
                Varchar(
                    None,
                    StringAttrs {
                        collate_spec: Some("'utf8'".to_string()),
                    },
                ),
            ),
            (
                line!(),
                "STRING COLLATE UNICODE_CI",
                Varchar(
                    None,
                    StringAttrs {
                        collate_spec: Some("UNICODE_CI".to_string()),
                    },
                ),
            ),
            (line!(), "charactER LARge  object", Clob),
            (line!(), "  binaRY   LARge object", Blob),
            (line!(), "binary     ", Binary(None)),
            (line!(), "binary(16) ", Binary(Some(16))),
            (line!(), "binary(255)", Binary(Some(255))),
            (line!(), "varbinary  ", Binary(None)),
            (line!(), "varbinary(32)", Binary(Some(32))),
            (line!(), "bytea      ", Binary(None)),
            (line!(), "bytea(64)  ", Binary(Some(64))),
            (line!(), "date       ", Date),
            (
                line!(),
                "tiME ( 0)  ",
                Time {
                    precision: Some(0),
                    time_zone_spec: TimeZoneSpec::Without,
                },
            ),
            (
                line!(),
                "TIMe(   5) ",
                Time {
                    precision: Some(5),
                    time_zone_spec: TimeZoneSpec::Without,
                },
            ),
            (
                line!(),
                "TIMe(5) without time ZONE",
                Time {
                    precision: Some(5),
                    time_zone_spec: TimeZoneSpec::Without,
                },
            ),
            (
                line!(),
                "TIME(5)   WITH   TIME ZONE",
                Time {
                    precision: Some(5),
                    time_zone_spec: TimeZoneSpec::With,
                },
            ),
            (
                line!(),
                "timESTamp ( 0) ",
                Timestamp {
                    precision: Some(0),
                    time_zone_spec: TimeZoneSpec::Unspecified,
                },
            ),
            (
                line!(),
                "TIMestamp(   5)",
                Timestamp {
                    precision: Some(5),
                    time_zone_spec: TimeZoneSpec::Unspecified,
                },
            ),
            (
                line!(),
                "TIMestamp(9) without TIME ZONE",
                Timestamp {
                    precision: Some(9),
                    time_zone_spec: TimeZoneSpec::Without,
                },
            ),
            (
                line!(),
                "TIMestamp(9) with TIME ZONE",
                Timestamp {
                    precision: Some(9),
                    time_zone_spec: TimeZoneSpec::With,
                },
            ),
            (line!(), "INTERVal", Interval(None)),
            (line!(), "interval (0 )", Interval(Some((Second, None)))),
            (
                line!(),
                "interval ( 3)",
                Interval(Some((Millisecond, None))),
            ),
            (
                line!(),
                "interval second(3)",
                Interval(Some((Millisecond, None))),
            ),
            (
                line!(),
                "interval year to second(6)",
                Interval(Some((Year, Some(Microsecond)))),
            ),
            (
                line!(),
                "interval year to microsecond",
                Interval(Some((Year, Some(Microsecond)))),
            ),
            (line!(), "interval minute", Interval(Some((Minute, None)))),
            (line!(), "jSON", Json),
            (line!(), "jSONb", Jsonb),
            (line!(), "geoMETRY", Geometry(None)),
            (line!(), "geoGRAPHy", Geography(None)),
            (line!(), "geoMETRY(ANy)", Geometry(Some("ANY".to_string()))),
            (
                line!(),
                "geoGRAPHy(ANy)",
                Geography(Some("ANY".to_string())),
            ),
            (line!(), "geoMETRY(123)", Geometry(Some("123".to_string()))),
            (
                line!(),
                "geoGRAPHy(123)",
                Geography(Some("123".to_string())),
            ),
            (line!(), "arrAY", Array(None)),
            (
                line!(),
                if backend == Snowflake {
                    "arrAY(INTeger)"
                } else {
                    "arrAY<INTeger>"
                },
                Array(Some(Box::new(Integer))),
            ),
            (
                line!(),
                if backend == Snowflake {
                    "arrAY(Array(CHARACTER VARYING))"
                } else {
                    "arrAY<Array<CHARACTER VARYING>>"
                },
                Array(Some(Box::new(Array(Some(Box::new(SqlType::varchar(
                    None,
                ))))))),
            ),
            (line!(), "struct", Struct(None)),
            (
                line!(),
                if backend == Snowflake {
                    "object()"
                } else {
                    "struct<>"
                },
                Struct(Some(vec![])),
            ),
            (
                line!(),
                if backend == Snowflake {
                    "OBJECT(name varchar, age int NOT NULL)"
                } else {
                    "STRUCT<name VARchar, age int NOT NULL>"
                },
                Struct(Some(vec![
                    StructField::new(Ident::new("name", backend), SqlType::varchar(None), true),
                    StructField::new(Ident::new("age", backend), Integer, false),
                ])),
            ),
            (
                line!(),
                if backend == Snowflake {
                    r#"objECT(name VARchar, age int NULLABLE)"#
                } else {
                    r#"strUCT<name VARchar, age int NULLABLE>"#
                },
                Struct(Some(vec![
                    StructField::new(Ident::new("name", backend), SqlType::varchar(None), true),
                    StructField::new(Ident::new("age", backend), Integer, true),
                ])),
            ),
            (
                line!(),
                if backend == Snowflake {
                    r#"OBJECT(name VARCHAR COLLATE UNICODE_CI NOT NULL COMMENT 'the comment', age int)"#
                } else {
                    "STRUCT<name STRING COLLATE UNICODE_CI NOT NULL COMMENT 'the comment', age int>"
                },
                Struct(Some(vec![
                    StructField::new(
                        Ident::new("name", backend),
                        Varchar(
                            None,
                            StringAttrs {
                                collate_spec: Some("UNICODE_CI".to_string()),
                            },
                        ),
                        false,
                    )
                    .with_comment("'the comment'".to_string()),
                    StructField::new(Ident::new("age", backend), Integer, true),
                ])),
            ),
            (
                line!(),
                if backend == Snowflake {
                    "object(name varchar, info object(id int, value varchar))"
                } else {
                    "struct<name varchar, info struct<id int, value varchar>>"
                },
                Struct(Some(vec![
                    StructField::new(Ident::new("name", backend), SqlType::varchar(None), true),
                    StructField::new(
                        Ident::new("info", backend),
                        Struct(Some(vec![
                            StructField::new(Ident::new("id", backend), Integer, true),
                            StructField::new(
                                Ident::new("value", backend),
                                SqlType::varchar(None),
                                true,
                            ),
                        ])),
                        true,
                    ),
                ])),
            ),
            (
                line!(),
                "MAP<VARchar, int>",
                Map(Some((Box::new(SqlType::varchar(None)), Box::new(Integer)))),
            ),
            (line!(), "Variant", Variant),
            (line!(), "SUPER", Variant),
            (line!(), " void  ", Void),
            (line!(), "other", Other("other".to_string())),
            (
                line!(),
                "another type that is \"not\" known NOT NULL",
                Other("another type that is \"not\" known".to_string()),
            ),
        ]
    };
    for backend in backends() {
        let data = data_for_backend(backend);
        for (line, input, expected) in data.iter() {
            assert_parses_to(*line, input, expected, backend);
        }
    }
}

/// Test parsing of strings that might only be recognized by BigQuery.
#[test]
fn test_bigquery_types() {
    let table = vec![
        (line!(), "BOOL", Boolean),
        (line!(), "BYTES", Binary(None)),
        (line!(), "INT64", BigInt),
        (line!(), "FLOAT64", Double),
        (line!(), "DATETIME", DateTime),
        (line!(), "ARRAY<INT64>", Array(Some(Box::new(BigInt)))),
        (
            line!(),
            "ARRAY<BIGNUMERIC>",
            Array(Some(Box::new(BigNumeric(None)))),
        ),
        (
            line!(),
            "ARRAY<FLOAT64>",
            Array(Some(Box::new(Float(None)))),
        ),
    ];
    for (line, input, expected) in table {
        assert_parses_to(line, input, &expected, BigQuery);
    }
}

fn backends() -> Vec<Backend> {
    vec![
        Postgres,
        Snowflake,
        BigQuery,
        Databricks,
        DatabricksODBC,
        RedshiftODBC,
        Generic {
            library_name: "generic",
            entrypoint: None,
        },
    ]
}

/// Assert that `ty` renders to `s` on the given backend, and that parsing `s` back
/// to a [SqlType] results in the same type.
fn assert_roundtrip(line: u32, ty: &SqlType, s: &str, backend: Backend) {
    let rendered = format!("{} ({backend})", ty.to_string(backend));
    let expected = format!("{s} ({backend})");
    assert_eq!(
        rendered,
        expected,
        "rendered != expected while rendering: {ty:?} ({backend:?}) from {}:{line}",
        file!()
    );

    let (parsed, _nullable) = SqlType::parse(backend, s).unwrap();
    let rendered = format!("{} ({backend})", parsed.to_string(backend));
    assert_eq!(rendered, expected, "parsing: {parsed:?}, expected: {ty:?}");
}

/// Returns a vector of triplets with a line number, SQL type, and its rendering for a given backend.
fn expected_type_rendering_for(backend: Backend) -> Vec<(u32, SqlType, &'static str)> {
    // | # | SQLType | - | BigQuery | Snowflake | Postgres | Redshift | Databricks | generic |
    let sqltype_bg_generic_snow_table = vec![
        (
            line!(),
            Boolean,
            "BOOL",
            "BOOLEAN",
            "BOOLEAN",
            "BOOLEAN",
            "BOOLEAN",
            "BOOLEAN",
        ),
        (
            line!(),
            TinyInt,
            "INT64",
            "TINYINT",
            "SMALLINT",
            "SMALLINT",
            "TINYINT",
            "TINYINT",
        ),
        (
            line!(),
            SmallInt,
            "INT64",
            "SMALLINT",
            "SMALLINT",
            "SMALLINT",
            "SMALLINT",
            "SMALLINT",
        ),
        (line!(), Integer, "INT64", "INT", "INT", "INT", "INT", "INT"),
        (
            line!(),
            BigInt,
            "INT64",
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "BIGINT",
            "BIGINT",
        ),
        (
            line!(),
            Real,
            "FLOAT64",
            "REAL",
            "REAL",
            "REAL",
            "FLOAT",
            "REAL",
        ),
        (
            line!(),
            Float(None),
            "FLOAT64",
            "FLOAT",
            "REAL",
            "REAL",
            "FLOAT",
            "FLOAT",
        ),
        (
            line!(),
            Float(Some(3)),
            "FLOAT64",
            "FLOAT",
            "REAL",
            "REAL",
            "FLOAT",
            "FLOAT(3)",
        ),
        (
            line!(),
            Double,
            "FLOAT64",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE",
            "DOUBLE PRECISION",
        ),
        (
            line!(),
            Numeric(None),
            "NUMERIC",
            "NUMBER",
            "NUMERIC",
            "NUMERIC",
            "DECIMAL",
            "NUMERIC",
        ),
        (
            line!(),
            Numeric(Some((20, None))),
            "NUMERIC(20)",
            "NUMBER(20)",
            "NUMERIC(20)",
            "NUMERIC(20)",
            "DECIMAL(20)",
            "NUMERIC(20)",
        ),
        (
            line!(),
            Numeric(Some((60, Some(2)))),
            "NUMERIC(60, 2)",
            "NUMBER(60, 2)",
            "NUMERIC(60, 2)",
            "NUMERIC(60, 2)",
            "DECIMAL(60, 2)",
            "NUMERIC(60, 2)",
        ),
        (
            line!(),
            SqlType::varchar(None),
            "STRING",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "STRING",
            "VARCHAR",
        ),
        (
            line!(),
            SqlType::varchar(Some(255)),
            "STRING",
            "VARCHAR(255)",
            "VARCHAR(255)",
            "VARCHAR(255)",
            "STRING",
            "VARCHAR(255)",
        ),
        (
            line!(),
            Text,
            "STRING",
            "TEXT",
            "TEXT",
            "TEXT",
            "STRING",
            "TEXT",
        ),
        (
            line!(),
            Clob,
            "STRING",
            "TEXT",
            "TEXT",
            "TEXT",
            "STRING",
            "CLOB",
        ),
        (
            line!(),
            Blob,
            "BYTES",
            "BINARY",
            "BYTEA",
            "BYTEA",
            "BINARY",
            "BLOB",
        ),
        (
            line!(),
            Binary(None),
            "BYTES",
            "BINARY",
            "BYTEA",
            "BYTEA",
            "BINARY",
            "BINARY",
        ),
        (
            line!(),
            Binary(Some(16)),
            "BYTES",
            "BINARY(16)",
            "BYTEA",
            "BYTEA",
            "BINARY",
            "BINARY(16)",
        ),
        (
            line!(),
            Binary(Some(255)),
            "BYTES",
            "BINARY(255)",
            "BYTEA",
            "BYTEA",
            "BINARY",
            "BINARY(255)",
        ),
        (
            line!(),
            Date,
            "DATE",
            "DATE",
            "DATE",
            "DATE",
            "DATE",
            "DATE",
        ),
        (
            line!(),
            Time {
                precision: None,
                time_zone_spec: TimeZoneSpec::Without,
            },
            "TIME",
            "TIME",
            "TIME",
            "TIME",
            "TIME WITHOUT TIME ZONE", // Databricks doesn't actually have a TIME type
            "TIME WITHOUT TIME ZONE",
        ),
        (
            line!(),
            Time {
                precision: Some(0),
                time_zone_spec: TimeZoneSpec::Without,
            },
            "TIME",
            "TIME(0)",
            "TIME(0)",
            "TIME(0)",
            "TIME(0) WITHOUT TIME ZONE", // Databricks doesn't actually have a TIME type
            "TIME(0) WITHOUT TIME ZONE",
        ),
        (
            line!(),
            Time {
                precision: Some(5),
                time_zone_spec: TimeZoneSpec::Without,
            },
            "TIME",
            "TIME(5)",
            "TIME(5)",
            "TIME(5)",
            "TIME(5) WITHOUT TIME ZONE", // Databricks doesn't actually have a TIME type
            "TIME(5) WITHOUT TIME ZONE",
        ),
        (
            line!(),
            Time {
                precision: Some(9),
                time_zone_spec: TimeZoneSpec::Without,
            },
            "TIME",
            "TIME(9)",
            "TIME(9)",
            "TIME(9)",
            "TIME(9) WITHOUT TIME ZONE", // Databricks doesn't actually have a TIME type
            "TIME(9) WITHOUT TIME ZONE",
        ),
        (
            line!(),
            Time {
                precision: Some(9),
                time_zone_spec: TimeZoneSpec::With,
            },
            "TIME WITH TIME ZONE",
            "TIME(9) WITH TIME ZONE",
            "TIME(9) WITH TIME ZONE",
            "TIME(9) WITH TIME ZONE",
            "TIME(9) WITH TIME ZONE", // Databricks doesn't actually have a TIME type
            "TIME(9) WITH TIME ZONE",
        ),
        (
            line!(),
            DateTime,
            "DATETIME",
            "TIMESTAMP_NTZ",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "DATETIME",
        ),
        (
            line!(),
            Timestamp {
                precision: None,
                time_zone_spec: TimeZoneSpec::Without,
            },
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "TIMESTAMP WITHOUT TIME ZONE",
        ),
        (
            line!(),
            Timestamp {
                precision: None,
                time_zone_spec: TimeZoneSpec::With,
            },
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP_TZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE",
        ),
        (
            line!(),
            Timestamp {
                precision: Some(3),
                time_zone_spec: TimeZoneSpec::Without,
            },
            "TIMESTAMP",
            "TIMESTAMP_NTZ(3)",
            "TIMESTAMP(3)",
            "TIMESTAMP(3)",
            "TIMESTAMP_NTZ",
            "TIMESTAMP(3) WITHOUT TIME ZONE",
        ),
        (
            line!(),
            Timestamp {
                precision: Some(3),
                time_zone_spec: TimeZoneSpec::With,
            },
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP_TZ(3)",
            "TIMESTAMP(3) WITH TIME ZONE",
            "TIMESTAMP(3) WITH TIME ZONE",
            "TIMESTAMP",
            "TIMESTAMP(3) WITH TIME ZONE",
        ),
        (
            line!(),
            Interval(None),
            "INTERVAL",
            "INTERVAL",
            "INTERVAL",
            "INTERVAL",
            "INTERVAL",
            "INTERVAL",
        ),
        (
            line!(),
            Interval(Some((Second, None))),
            "INTERVAL SECOND",
            "INTERVAL SECOND",
            "INTERVAL SECOND",
            "INTERVAL SECOND",
            "INTERVAL SECOND",
            "INTERVAL SECOND",
        ),
        (
            line!(),
            Interval(Some((Millisecond, None))),
            "INTERVAL MILLISECOND",
            "INTERVAL MILLISECOND",
            "INTERVAL SECOND(3)",
            "INTERVAL SECOND(3)",
            "INTERVAL MILLISECOND",
            "INTERVAL MILLISECOND",
        ),
        (
            line!(),
            Interval(Some((Day, Some(Microsecond)))),
            "INTERVAL DAY TO MICROSECOND",
            "INTERVAL DAY TO MICROSECOND",
            "INTERVAL DAY TO SECOND(6)",
            "INTERVAL DAY TO SECOND(6)",
            "INTERVAL DAY TO MICROSECOND",
            "INTERVAL DAY TO MICROSECOND",
        ),
        (
            line!(),
            Interval(Some((Year, None))),
            "INTERVAL YEAR",
            "INTERVAL YEAR",
            "INTERVAL YEAR",
            "INTERVAL YEAR",
            "INTERVAL YEAR",
            "INTERVAL YEAR",
        ),
        (
            line!(),
            Interval(Some((Day, Some(Hour)))),
            "INTERVAL DAY TO HOUR",
            "INTERVAL DAY TO HOUR",
            "INTERVAL DAY TO HOUR",
            "INTERVAL DAY TO HOUR",
            "INTERVAL DAY TO HOUR",
            "INTERVAL DAY TO HOUR",
        ),
        (
            line!(),
            Interval(Some((Day, Some(Minute)))),
            "INTERVAL DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE",
            "INTERVAL DAY TO MINUTE",
        ),
        (
            line!(),
            Interval(Some((Day, Some(Second)))),
            "INTERVAL DAY TO SECOND",
            "INTERVAL DAY TO SECOND",
            "INTERVAL DAY TO SECOND",
            "INTERVAL DAY TO SECOND",
            "INTERVAL DAY TO SECOND",
            "INTERVAL DAY TO SECOND",
        ),
        (
            line!(),
            Interval(Some((Hour, Some(Minute)))),
            "INTERVAL HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE",
            "INTERVAL HOUR TO MINUTE",
        ),
        (
            line!(),
            Interval(Some((Hour, Some(Second)))),
            "INTERVAL HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND",
            "INTERVAL HOUR TO SECOND",
        ),
        (
            line!(),
            Interval(Some((Minute, Some(Second)))),
            "INTERVAL MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND",
            "INTERVAL MINUTE TO SECOND",
        ),
        (
            line!(),
            Interval(Some((Month, Some(Day)))),
            "INTERVAL MONTH TO DAY",
            "INTERVAL MONTH TO DAY",
            "INTERVAL MONTH TO DAY",
            "INTERVAL MONTH TO DAY",
            "INTERVAL MONTH TO DAY",
            "INTERVAL MONTH TO DAY",
        ),
        (
            line!(),
            Interval(Some((Month, Some(Hour)))),
            "INTERVAL MONTH TO HOUR",
            "INTERVAL MONTH TO HOUR",
            "INTERVAL MONTH TO HOUR",
            "INTERVAL MONTH TO HOUR",
            "INTERVAL MONTH TO HOUR",
            "INTERVAL MONTH TO HOUR",
        ),
        (
            line!(),
            Interval(Some((Month, Some(Minute)))),
            "INTERVAL MONTH TO MINUTE",
            "INTERVAL MONTH TO MINUTE",
            "INTERVAL MONTH TO MINUTE",
            "INTERVAL MONTH TO MINUTE",
            "INTERVAL MONTH TO MINUTE",
            "INTERVAL MONTH TO MINUTE",
        ),
        (
            line!(),
            Interval(Some((Month, Some(Second)))),
            "INTERVAL MONTH TO SECOND",
            "INTERVAL MONTH TO SECOND",
            "INTERVAL MONTH TO SECOND",
            "INTERVAL MONTH TO SECOND",
            "INTERVAL MONTH TO SECOND",
            "INTERVAL MONTH TO SECOND",
        ),
        (
            line!(),
            Interval(Some((Year, Some(Day)))),
            "INTERVAL YEAR TO DAY",
            "INTERVAL YEAR TO DAY",
            "INTERVAL YEAR TO DAY",
            "INTERVAL YEAR TO DAY",
            "INTERVAL YEAR TO DAY",
            "INTERVAL YEAR TO DAY",
        ),
        (
            line!(),
            Interval(Some((Year, Some(Hour)))),
            "INTERVAL YEAR TO HOUR",
            "INTERVAL YEAR TO HOUR",
            "INTERVAL YEAR TO HOUR",
            "INTERVAL YEAR TO HOUR",
            "INTERVAL YEAR TO HOUR",
            "INTERVAL YEAR TO HOUR",
        ),
        (
            line!(),
            Interval(Some((Year, Some(Minute)))),
            "INTERVAL YEAR TO MINUTE",
            "INTERVAL YEAR TO MINUTE",
            "INTERVAL YEAR TO MINUTE",
            "INTERVAL YEAR TO MINUTE",
            "INTERVAL YEAR TO MINUTE",
            "INTERVAL YEAR TO MINUTE",
        ),
        (
            line!(),
            Interval(Some((Year, Some(Month)))),
            "INTERVAL YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH",
            "INTERVAL YEAR TO MONTH",
        ),
        (
            line!(),
            Interval(Some((Year, Some(Second)))),
            "INTERVAL YEAR TO SECOND",
            "INTERVAL YEAR TO SECOND",
            "INTERVAL YEAR TO SECOND",
            "INTERVAL YEAR TO SECOND",
            "INTERVAL YEAR TO SECOND",
            "INTERVAL YEAR TO SECOND",
        ),
        (
            line!(),
            Array(Some(Box::new(Json))),
            "ARRAY<JSON>",
            "ARRAY(JSON)",
            "JSON[]",
            "JSON[]",
            "ARRAY<JSON>",
            "ARRAY<JSON>",
        ),
        (
            line!(),
            Struct(Some(vec![StructField::new(
                Ident::plain("a"),
                Float(None),
                true,
            )])),
            "STRUCT<a FLOAT64>",
            "OBJECT(a FLOAT)",
            "(a REAL)",
            "(a REAL)",
            "STRUCT<a: FLOAT>",
            "STRUCT<a FLOAT>",
        ),
        (
            line!(),
            Struct(Some(vec![
                StructField::new(Ident::plain("name"), SqlType::varchar(None), true),
                StructField::new(Ident::plain("age"), Integer, false),
            ])),
            "STRUCT<name STRING, age INT64 NOT NULL>",
            "OBJECT(name VARCHAR, age INT NOT NULL)",
            "(name VARCHAR, age INT NOT NULL)",
            "(name VARCHAR, age INT NOT NULL)",
            "STRUCT<name: STRING, age: INT NOT NULL>",
            "STRUCT<name VARCHAR, age INT NOT NULL>",
        ),
        (
            line!(),
            Struct(Some(vec![
                StructField::new(
                    Ident::plain("last_completion_time"),
                    Timestamp {
                        precision: None,
                        time_zone_spec: TimeZoneSpec::Without,
                    },
                    true,
                ),
                StructField::new(
                    Ident::plain("error_time"),
                    Timestamp {
                        precision: None,
                        time_zone_spec: TimeZoneSpec::Without,
                    },
                    true,
                ),
                StructField::new(
                    Ident::plain("error"),
                    Struct(Some(vec![
                        StructField::new(Ident::plain("reason"), SqlType::varchar(None), true),
                        StructField::new(Ident::plain("location"), SqlType::varchar(None), true),
                        StructField::new(Ident::plain("message"), SqlType::varchar(None), true),
                    ])),
                    true,
                ),
            ])),
            "STRUCT<last_completion_time TIMESTAMP, error_time TIMESTAMP, error STRUCT<reason STRING, location STRING, message STRING>>",
            "OBJECT(last_completion_time TIMESTAMP_NTZ, error_time TIMESTAMP_NTZ, error OBJECT(reason VARCHAR, location VARCHAR, message VARCHAR))",
            "(last_completion_time TIMESTAMP, error_time TIMESTAMP, error (reason VARCHAR, location VARCHAR, message VARCHAR))",
            "(last_completion_time TIMESTAMP, error_time TIMESTAMP, error (reason VARCHAR, location VARCHAR, message VARCHAR))",
            "STRUCT<last_completion_time: TIMESTAMP_NTZ, error_time: TIMESTAMP_NTZ, error: STRUCT<reason: STRING, location: STRING, message: STRING>>",
            "STRUCT<last_completion_time TIMESTAMP WITHOUT TIME ZONE, error_time TIMESTAMP WITHOUT TIME ZONE, error STRUCT<reason VARCHAR, location VARCHAR, message VARCHAR>>",
        ),
        (
            line!(),
            Array(Some(Box::new(Struct(Some(vec![
                StructField::new(Ident::plain("date"), Date, true),
                StructField::new(Ident::plain("value"), SqlType::varchar(None), true),
            ]))))),
            "ARRAY<STRUCT<date DATE, value STRING>>",
            "ARRAY(OBJECT(date DATE, value VARCHAR))",
            "(date DATE, value VARCHAR)[]",
            "(date DATE, value VARCHAR)[]",
            "ARRAY<STRUCT<date: DATE, value: STRING>>",
            "ARRAY<STRUCT<date DATE, value VARCHAR>>",
        ),
        (
            line!(),
            Struct(Some(vec![StructField::new(
                Ident::plain("elements"),
                Array(Some(Box::new(Struct(Some(vec![
                    StructField::new(Ident::plain("date"), Date, true),
                    StructField::new(Ident::plain("value"), SqlType::varchar(None), true),
                ]))))),
                true,
            )])),
            "STRUCT<elements ARRAY<STRUCT<date DATE, value STRING>>>",
            "OBJECT(elements ARRAY(OBJECT(date DATE, value VARCHAR)))",
            "(elements (date DATE, value VARCHAR)[])",
            "(elements (date DATE, value VARCHAR)[])",
            "STRUCT<elements: ARRAY<STRUCT<date: DATE, value: STRING>>>",
            "STRUCT<elements ARRAY<STRUCT<date DATE, value VARCHAR>>>",
        ),
        (
            line!(),
            Map(Some((Box::new(SqlType::varchar(None)), Box::new(Integer)))),
            "MAP<STRING, INT64>",
            "MAP<VARCHAR, INT>",
            "MAP<VARCHAR, INT>",
            "MAP<VARCHAR, INT>",
            "MAP<STRING, INT>",
            "MAP<VARCHAR, INT>",
        ),
        (
            line!(),
            Variant,
            "VARIANT",
            "VARIANT",
            "VARIANT",
            "SUPER", // https://docs.aws.amazon.com/redshift/latest/dg/r_SUPER_type.html
            "VARIANT",
            "VARIANT",
        ),
        (
            line!(),
            Void,
            "VOID",
            "VOID",
            "VOID",
            "VOID",
            "VOID",
            "VOID",
        ),
        (
            line!(),
            Other("ANY OTHER TYPE".to_string()),
            "ANY OTHER TYPE",
            "ANY OTHER TYPE",
            "ANY OTHER TYPE",
            "ANY OTHER TYPE",
            "ANY OTHER TYPE",
            "ANY OTHER TYPE",
        ),
    ];
    let zipped = sqltype_bg_generic_snow_table
        .into_iter()
        .map(|(line, t, bq, snow, pq, rs, dbx, generic)| {
            let s = match backend {
                BigQuery => bq,
                Snowflake => snow,
                Postgres | Salesforce => pq,
                Redshift | RedshiftODBC => rs,
                Databricks | DatabricksODBC => dbx,
                Generic { .. } => generic,
            };
            (line, t, s)
        })
        .collect::<Vec<_>>();
    zipped
}

#[test]
fn test_string_roundtrip_for_all_types_on_all_backends() {
    for backend in backends() {
        for (line, t, s) in expected_type_rendering_for(backend) {
            assert_roundtrip(line, &t, s, backend);
        }
    }
}

#[test]
fn test_roundtrip_struct_with_quoted_field() {
    // the quote style carried on the SqlType depends on the backend
    let expected_ty = |backend| {
        Struct(Some(vec![
            StructField::new(Ident::plain("name"), SqlType::varchar(None), true),
            StructField::new(
                Ident::unquoted(canonical_quote(backend), "age"),
                Integer,
                true,
            ),
        ]))
    };
    let table = vec![
        (line!(), BigQuery, r#"STRUCT<name VARCHAR, `age` INT>"#),
        (line!(), Snowflake, r#"OBJECT(name VARCHAR, "age" INT)"#),
        (line!(), Postgres, r#"STRUCT<name VARCHAR, "age" INT>"#),
        (line!(), Databricks, r#"STRUCT<name VARCHAR, `age` INT>"#),
    ];
    for (line, backend, input) in table {
        let ty = expected_ty(backend);
        assert_parses_to(line, input, &ty, backend);
    }
}

/// This test makes it easier to attach a debugger and step through
/// a specific function call compared to `test_string_roundtrip_for_all_types_on_all_backends`.
#[test]
fn test_timestamp_on_databricks() {
    let s = "TIMESTAMP";
    let t = Timestamp {
        precision: None,
        time_zone_spec: TimeZoneSpec::With,
    };
    assert_roundtrip(line!(), &t, s, Databricks);
}

/// This test makes it easier to attach a debugger and step through
/// a specific function call compared to `test_string_roundtrip_for_all_types_on_all_backends`.
#[test]
fn test_struct_on_snowflake() {
    let s = r#"OBJECT(name VARCHAR, "age" INT)"#;
    let t = Struct(Some(vec![
        StructField::new(Ident::new("name", Snowflake), SqlType::varchar(None), true),
        StructField::new(
            Ident::unquoted(canonical_quote(Snowflake), "age"),
            Integer,
            true,
        ),
    ]));
    assert_roundtrip(line!(), &t, s, Snowflake);
}

#[test]
fn test_struct_on_databricks() {
    let s = "STRUCT<`name`: STRING, `age`: INT, `active`: BOOLEAN>";
    let t = Struct(Some(vec![
        StructField::new(
            Ident::unquoted(canonical_quote(Databricks), "name"),
            SqlType::varchar(None),
            true,
        ),
        StructField::new(
            Ident::unquoted(canonical_quote(Databricks), "age"),
            Integer,
            true,
        ),
        StructField::new(
            Ident::unquoted(canonical_quote(Databricks), "active"),
            Boolean,
            true,
        ),
    ]));
    assert_roundtrip(line!(), &t, s, Databricks);

    let dt = t.pick_best_arrow_type(Databricks);
    assert!(matches!(dt, DataType::Struct(_)));
    if let DataType::Struct(fields) = dt {
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name(), "name");
        assert_eq!(fields[1].name(), "age");
        assert_eq!(fields[2].name(), "active");
    }
}
