//! DuckDB v2-catalog `ATTACH` statement composition.
//!
//! Extracted from the generic `AdbcEngine` (`engine/adbc.rs`) so this
//! DuckDB-specific logic is not hardcoded in the cross-adapter engine.
//!
//! FIXME: as the number of catalog types and attach options grows, audit whether
//! the attach composition logic here is clean and scalable — e.g. per-catalog-type
//! attach builders rather than branching on V2CatalogType inline.

use std::collections::HashMap;

use dbt_adapter_core::AdapterType;
use dbt_common::AdapterResult;
use dbt_schemas::schemas::dbt_catalogs_v2::{CatalogSpecV2View, DbtCatalogsV2View, V2CatalogType};

use dbt_adapter_sql::ident::escape_string_literal;

use crate::errors::{AdapterError, AdapterErrorKind};
use crate::metadata::duckdb::{CatalogSpecDuckDbExt, attaches_via_iceberg_rest};

/// Pure: compose the DuckDB v2-catalog ATTACH statements for a parsed
/// `DbtCatalogsV2View`. Returns the statements in emission order, with a
/// leading `INSTALL ducklake` prelude when any DuckLake catalog is present.
///
/// Local filesystem catalogs intentionally do not emit ATTACH SQL; they provide
/// file roots and defaults consumed by source rendering and external writes.
///
/// Errors when alias sanitization produces an empty alias or a duplicate
/// alias across catalogs.
pub fn compose_v2_catalog_attach_stmts(
    view: &DbtCatalogsV2View<'_>,
    platform: &str,
) -> AdapterResult<Vec<String>> {
    // INSTALL ducklake must lead all ATTACHes but we can't know it's needed until we've seen the catalogs
    let mut needs_ducklake = false;
    let mut stmts: Vec<String> = Vec::new();
    let mut seen_aliases: HashMap<String, String> = HashMap::new();

    for (catalog, duckdb) in view
        .catalogs
        .iter()
        .filter(|catalog| {
            matches!(catalog.catalog_type, V2CatalogType::DuckLake)
                || attaches_via_iceberg_rest(catalog.catalog_type)
        })
        .filter_map(|catalog| {
            // Prefer the caller's platform block (e.g. `alt` for the compute
            // engine), falling back to the `duckdb` block.
            catalog
                .config_block(platform)
                .or_else(|| catalog.config_block("duckdb"))
                .map(|duckdb| (catalog, duckdb))
        })
    {
        let (alias, stmt) = match catalog.catalog_type {
            V2CatalogType::DuckLake => {
                needs_ducklake = true;
                build_duckdb_ducklake_attach_stmt(catalog, duckdb)?
            }
            _ => build_duckdb_catalog_attach_stmt(catalog, duckdb)?,
        };
        if let Some(prior) = seen_aliases.get(&alias) {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!(
                    "Catalog '{}' duckdb attach alias '{alias}' collides with catalog '{prior}'",
                    catalog.name
                ),
            ));
        }
        seen_aliases.insert(alias, catalog.name.to_string());
        stmts.push(stmt);
    }

    if needs_ducklake {
        stmts.insert(0, "INSTALL ducklake".to_string());
    }

    Ok(stmts)
}

/// The catalog's sanitized attach alias (via [`CatalogSpecDuckDbExt`], the same
/// resolution metadata routing uses), or a Configuration error when nothing
/// identifier-safe is left after sanitization.
fn resolve_required_attach_alias(catalog: &CatalogSpecV2View<'_>) -> AdapterResult<String> {
    let alias = catalog.resolved_attach_alias().unwrap_or_default();
    if alias.is_empty() {
        return Err(AdapterError::new(
            AdapterErrorKind::Configuration,
            format!(
                "Catalog '{}' duckdb attach alias is empty after sanitization",
                catalog.name
            ),
        ));
    }
    Ok(alias)
}

fn build_duckdb_ducklake_attach_stmt(
    catalog: &CatalogSpecV2View<'_>,
    duckdb: &dbt_yaml::Mapping,
) -> AdapterResult<(String, String)> {
    let alias = resolve_required_attach_alias(catalog)?;

    let metadata_path = duckdb_get_str(duckdb, "metadata_path").unwrap_or_default();
    let mut opts = String::new();
    let mut push_opt = |opt: String| {
        if !opts.is_empty() {
            opts.push_str(", ");
        }
        opts.push_str(&opt);
    };
    if let Some(data_path) = duckdb_get_str(duckdb, "data_path") {
        push_opt(format!(
            "DATA_PATH '{}'",
            escape_string_literal(data_path, AdapterType::DuckDB)
        ));
    }
    if let Some(metadata_schema) = duckdb_get_str(duckdb, "metadata_schema") {
        push_opt(format!(
            "METADATA_SCHEMA '{}'",
            escape_string_literal(metadata_schema, AdapterType::DuckDB)
        ));
    }
    // The catalog/database name inside the metadata store (e.g. a named DuckDB
    // catalog, or the database for a postgres/mysql metadata backend).
    if let Some(metadata_catalog) = duckdb_get_str(duckdb, "metadata_catalog") {
        push_opt(format!(
            "METADATA_CATALOG '{}'",
            escape_string_literal(metadata_catalog, AdapterType::DuckDB)
        ));
    }
    // Row count below which DuckLake inlines inserts into the metadata DB
    // rather than writing a Parquet data file.
    if let Some(row_limit) = duckdb_get_i64(duckdb, "data_inlining_row_limit") {
        push_opt(format!("DATA_INLINING_ROW_LIMIT {row_limit}"));
    }
    for (key, sql_key) in [
        ("create_if_not_exists", "CREATE_IF_NOT_EXISTS"),
        ("read_only", "READ_ONLY"),
        ("encrypted", "ENCRYPTED"),
        // Auto-migrate the catalog's DuckLake format version on attach (needed
        // when a newer DuckLake wrote a catalog an older reader must open).
        ("automatic_migration", "AUTOMATIC_MIGRATION"),
        // Allow attaching with a DATA_PATH that differs from the one recorded
        // in an existing catalog (otherwise the mismatch is a hard error).
        ("override_data_path", "OVERRIDE_DATA_PATH"),
    ] {
        if let Some(val) = duckdb_get_bool(duckdb, key)? {
            push_opt(format!("{sql_key} {val}"));
        }
    }

    let source = format!(
        "'ducklake:{}'",
        escape_string_literal(metadata_path, AdapterType::DuckDB)
    );
    let mut stmt = format!("ATTACH IF NOT EXISTS {source} AS {alias}");
    if !opts.is_empty() {
        stmt.push_str(" (");
        stmt.push_str(&opts);
        stmt.push(')');
    }

    Ok((alias, stmt))
}

fn build_duckdb_catalog_attach_stmt(
    catalog: &CatalogSpecV2View<'_>,
    duckdb: &dbt_yaml::Mapping,
) -> AdapterResult<(String, String)> {
    let alias = resolve_required_attach_alias(catalog)?;
    let endpoint_type = duckdb_get_str(duckdb, "endpoint_type");

    // Emission order is part of the snapshot contract: connection options, then
    // user-supplied passthrough options, then dbt's defaults.
    let mut opts = vec!["TYPE ICEBERG".to_string()];
    push_connection_opts(&mut opts, duckdb, endpoint_type);
    push_passthrough_opts(&mut opts, duckdb)?;
    push_default_opts(&mut opts, duckdb, catalog.catalog_type, endpoint_type)?;

    let source = format!(
        "'{}'",
        escape_string_literal(
            resolve_attach_source(catalog, duckdb, endpoint_type),
            AdapterType::DuckDB
        )
    );

    Ok((
        alias.clone(),
        format!(
            "ATTACH IF NOT EXISTS {source} AS {alias} ({})",
            opts.join(", ")
        ),
    ))
}

/// How DuckDB reaches the catalog: which secret to authenticate with, and where
/// the REST endpoint lives.
fn push_connection_opts(
    opts: &mut Vec<String>,
    duckdb: &dbt_yaml::Mapping,
    endpoint_type: Option<&str>,
) {
    if let Some(secret) = duckdb_get_str(duckdb, "secret") {
        let secret = dbt_adapter_sql::ident::sanitize_identifier(secret, AdapterType::DuckDB);
        if !secret.is_empty() {
            opts.push(format!("SECRET {secret}"));
        }
    }
    // The managed-endpoint shortcut: DuckDB derives ENDPOINT and SIGV4
    // authorization itself from the endpoint type plus the secret's region
    // (GLUE) or the warehouse ARN's region (S3_TABLES). Mutually exclusive with
    // `endpoint` and `authorization_type` — both in our schema and in DuckDB,
    // which hard-errors on the combination.
    if let Some(et) = endpoint_type {
        opts.push(format!(
            "ENDPOINT_TYPE '{}'",
            escape_string_literal(et, AdapterType::DuckDB)
        ));
    }
    if let Some(ep) = duckdb_get_str(duckdb, "endpoint") {
        opts.push(format!(
            "ENDPOINT '{}'",
            escape_string_literal(ep, AdapterType::DuckDB)
        ));
    }
}

/// ATTACH options passed straight through from `config.duckdb`, emitted only
/// when the user set them.
fn push_passthrough_opts(opts: &mut Vec<String>, duckdb: &dbt_yaml::Mapping) -> AdapterResult<()> {
    for (key, sql_key) in [
        ("default_region", "DEFAULT_REGION"),
        ("default_schema", "DEFAULT_SCHEMA"),
        ("max_table_staleness", "MAX_TABLE_STALENESS"),
        ("authorization_type", "AUTHORIZATION_TYPE"),
        ("access_delegation_mode", "ACCESS_DELEGATION_MODE"),
    ] {
        if let Some(val) = duckdb_get_str(duckdb, key) {
            opts.push(format!(
                "{sql_key} '{}'",
                escape_string_literal(val, AdapterType::DuckDB)
            ));
        }
    }
    // The write-compat options (STAGE_CREATE_TABLES, DISABLE_MULTI_TABLE_COMMIT,
    // SKIP_CREATE_TABLE_METADATA_UPDATES, REMOVE_FILES_ON_DELETE) require
    // duckdb 1.5.4 / duckdb-iceberg#1017.
    for (key, sql_key) in [
        ("support_nested_namespaces", "SUPPORT_NESTED_NAMESPACES"),
        ("stage_create_tables", "STAGE_CREATE_TABLES"),
        ("disable_multi_table_commit", "DISABLE_MULTI_TABLE_COMMIT"),
        (
            "skip_create_table_metadata_updates",
            "SKIP_CREATE_TABLE_METADATA_UPDATES",
        ),
        ("remove_files_on_delete", "REMOVE_FILES_ON_DELETE"),
        ("purge_requested", "PURGE_REQUESTED"),
    ] {
        if let Some(val) = duckdb_get_bool(duckdb, key)? {
            opts.push(format!("{sql_key} {val}"));
        }
    }
    Ok(())
}

/// Options dbt supplies on the user's behalf: the read-write default and the
/// per-catalog-type write-compat presets, each yielding to an explicit user value.
fn push_default_opts(
    opts: &mut Vec<String>,
    duckdb: &dbt_yaml::Mapping,
    catalog_type: V2CatalogType,
    endpoint_type: Option<&str>,
) -> AdapterResult<()> {
    // duckdb's AUTOMATIC access mode resolves to read-only for a remote Iceberg
    // REST catalog, which blocks CREATE/INSERT. dbt writes to its catalogs, so
    // attach read-write by default; a user-supplied `read_only` config overrides.
    let read_only = duckdb_get_bool(duckdb, "read_only")?.unwrap_or(false);
    opts.push(format!("READ_ONLY {read_only}"));
    for (config_key, default_clause) in catalog_attach_defaults(catalog_type) {
        // `endpoint_type` already implies an authorization type inside DuckDB,
        // which rejects the pair outright ("'authorization_type' can not be
        // combined with 'endpoint_type'"). Presetting one here would turn a
        // valid config into a failed ATTACH.
        if *config_key == "authorization_type" && endpoint_type.is_some() {
            continue;
        }
        if !duckdb_has_key(duckdb, config_key) {
            opts.push((*default_clause).to_string());
        }
    }
    if duckdb_get_bool(duckdb, "encode_entire_prefix")?.unwrap_or(false) {
        opts.push("ENCODE_ENTIRE_PREFIX true".to_string());
    }
    Ok(())
}

/// The `ATTACH '<source>'` operand. For Iceberg REST catalogs this is the
/// warehouse name, not the endpoint URL.
fn resolve_attach_source<'a>(
    catalog: &CatalogSpecV2View<'a>,
    duckdb: &'a dbt_yaml::Mapping,
    endpoint_type: Option<&str>,
) -> &'a str {
    duckdb_get_str(duckdb, "warehouse").unwrap_or_else(|| {
        // Glue reads the source as a catalog path, not a free-form warehouse
        // name, and validates it against its own grammar (':', a 12-digit
        // account id, 'cat1/cat2', ...) — a dbt catalog name fails that check.
        // ':' is Glue's default catalog in the caller's own account.
        if is_glue_attach(catalog.catalog_type, endpoint_type) {
            ":"
        } else {
            catalog.name
        }
    })
}

/// Whether this catalog attaches to AWS Glue, by either route: a `glue` catalog
/// type pinning its own `endpoint`, or `endpoint_type: GLUE` on any Iceberg REST
/// catalog. Both land on DuckDB's Glue code path, which reads the ATTACH source
/// as a Glue catalog path.
fn is_glue_attach(catalog_type: V2CatalogType, endpoint_type: Option<&str>) -> bool {
    matches!(catalog_type, V2CatalogType::Glue)
        || endpoint_type.is_some_and(|et| et.trim().eq_ignore_ascii_case("GLUE"))
}

fn duckdb_get_str<'a>(duckdb: &'a dbt_yaml::Mapping, key: &str) -> Option<&'a str> {
    duckdb
        .get(dbt_yaml::Value::from(key))
        .and_then(|v| v.as_str())
}

fn duckdb_get_i64(duckdb: &dbt_yaml::Mapping, key: &str) -> Option<i64> {
    duckdb
        .get(dbt_yaml::Value::from(key))
        .and_then(|v| v.as_i64())
}

/// Boolean ATTACH options accept the same lenient YAML the schema validator
/// accepts (bool literals or parseable strings like `"true"`, via
/// `try_get_bool`). Reading raw `as_bool()` here would silently drop a value
/// validation approved — e.g. `read_only: "true"` attaching read-write.
fn duckdb_get_bool(duckdb: &dbt_yaml::Mapping, key: &str) -> AdapterResult<Option<bool>> {
    dbt_common::serde_utils::try_get_bool(duckdb, key)
        .map_err(|e| AdapterError::new(AdapterErrorKind::Configuration, format!("{e}")))
}

fn duckdb_has_key(duckdb: &dbt_yaml::Mapping, key: &str) -> bool {
    duckdb.get(dbt_yaml::Value::from(key)).is_some()
}

/// DuckDB Iceberg `ATTACH` write-compat defaults per catalog type, encoding
/// dbt's maintainer knowledge of each managed-storage backend. Each entry is
/// `(config.duckdb key, full SQL option clause)`; the clause is emitted only
/// when the user has not set that key, so explicit user values always win.
/// These options require duckdb 1.5.4 / duckdb-iceberg#1017.
fn catalog_attach_defaults(catalog_type: V2CatalogType) -> &'static [(&'static str, &'static str)] {
    match catalog_type {
        // Snowflake Horizon (Polaris) Iceberg REST: OAuth2 + vended credentials,
        // and a write path that supports neither staged creates nor multi-table
        // commits.
        V2CatalogType::Horizon => &[
            ("authorization_type", "AUTHORIZATION_TYPE 'OAUTH2'"),
            (
                "access_delegation_mode",
                "ACCESS_DELEGATION_MODE 'VENDED_CREDENTIALS'",
            ),
            ("stage_create_tables", "STAGE_CREATE_TABLES false"),
            (
                "disable_multi_table_commit",
                "DISABLE_MULTI_TABLE_COMMIT true",
            ),
            (
                "skip_create_table_metadata_updates",
                "SKIP_CREATE_TABLE_METADATA_UPDATES true",
            ),
            ("remove_files_on_delete", "REMOVE_FILES_ON_DELETE false"),
        ],
        // AWS Glue's Iceberg REST endpoint authenticates with SigV4, never
        // OAuth2 (DuckDB's fallback). `endpoint_type: GLUE` gets SigV4 from
        // DuckDB itself, but a Glue catalog pinning an explicit `endpoint` —
        // the only route for regions off the plain amazonaws.com suffix — has
        // nothing else to supply it.
        V2CatalogType::Glue => &[("authorization_type", "AUTHORIZATION_TYPE 'SIGV4'")],
        // Databricks Unity Catalog's Iceberg REST endpoint does not support the
        // REST multi-table commit, so default it off; single-table commits work.
        // (Other write-path options are left to DuckDB's defaults pending
        // confirmation against a live Unity catalog.)
        V2CatalogType::Unity => &[(
            "disable_multi_table_commit",
            "DISABLE_MULTI_TABLE_COMMIT true",
        )],
        _ => &[],
    }
}
