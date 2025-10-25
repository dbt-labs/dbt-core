
All files under this directory are licensed under the Apache 2.0 License (see
LICENSE)

# dbt SQL Grammars

This directory contains the ANTLR grammars for various SQL dialects supported by
dbt Fusion, including:
- BigQuery (dbt-parser-bigquery/src/BigQuery.g4)
- Databricks (dbt-parser-databricks/src/Databricks.g4)
- Redshift (dbt-parser-redshift/src/Redshift.g4)
- Snowflake (dbt-parser-snowflake/src/Snowflake.g4)
- Trino (dbt-parser-trino/src/Trino.g4)

All grammars are implemented using ANTLR4, and can be processed by Antlr4 4.8.4.
The generated Rust parser code from each grammar is also checked in under the
`src/generated/` directory of each parser crate.

*NOTE*: This directory is currently released for public preview *only*. Please
do NOT submit PRs against the contents of this directory as we are currently
unable to accept them.
