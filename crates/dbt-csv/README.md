# dbt-csv

A CSV reader designed to be **100% conformant with Mantle's CSV parsing behavior**.

## Background

Mantle's CSV parsing leverages Python's `agate` library via `dbt-common/dbt_common/clients/agate_helper.py`. The parsing pipeline consists of three steps:

1. **Parse CSV** — Use Python's built-in `csv` module to read data as string rows
2. **Infer types** — Use agate's type testers to determine column types from string values
3. **Cast values** — Convert string rows to Python-typed values based on inferred types

This produces a CSV table of Python-typed values (integers, decimals, dates, booleans, etc.).

## How This Crate Works

This crate replicates Mantle's behavior in Rust, producing Arrow record batches instead of Python objects:

1. **Parse CSV** — Use Rust's `csv` crate to read data as string rows
2. **Infer types** — Use `type_tester.rs` to infer agate-compatible types (`AgateType`)
3. **Cast values** — Convert string rows to Arrow arrays based on inferred `AgateType`

The output is a stream of Arrow `RecordBatch` objects with types that match what Mantle would produce.

## Type Inference Rules

Following agate's type priority (highest to lowest):

| Priority | AgateType | Arrow Type | Example Values |
|----------|-----------|------------|----------------|
| 1 | Integer | Int64 | `42`, ` 123 ` (whitespace trimmed) |
| 2 | Number | Float64 | `3.14`, `50%`, `1.5e10` |
| 3 | Date | Date32 | `2024-01-15` |
| 4 | DateTime | Timestamp(ns) | `2024-01-15 10:30:00` |
| 5 | ISODateTime | Timestamp(ns) | `2024-01-15T10:30:00Z`, `20240115T103000Z` |
| 6 | Boolean | Boolean | `true`, `false` (case-insensitive) |
| 7 | Text | Utf8 | Everything else |

**Null values:** Empty strings (`""`) and `"null"` (case-insensitive) are treated as NULL.

## Usage

Enable with the environment variable:

```bash
USE_DBT_CSV=1 fs seed
```

## Crate Structure

```
src/
├── lib.rs              # Public API exports
├── type_tester.rs      # Agate-compatible type inference (AgateType, AgateSchema)
└── reader/
    ├── mod.rs          # CSV reader, decoder, and string-to-Arrow parsing
    └── records.rs      # Low-level record decoder (forked from arrow-csv)
```

### Key Components

- **`AgateType`** — Enum representing agate's type system
- **`AgateSchema`** — Schema using `AgateType` instead of Arrow `DataType`
- **`Format`** — Configures CSV parsing options and performs type inference
- **`ReaderBuilder`** / **`Reader`** — Builds and iterates over `RecordBatch` results
- **`RecordDecoder`** — Low-level CSV field parsing (forked from arrow-csv because the original is not public)

## Origin

Forked from [arrow-csv](https://github.com/apache/arrow-rs/tree/main/arrow-csv) with modifications for agate conformance.
