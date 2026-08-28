//! Schema-level tests. These need no fixtures: the schema and the projection's
//! output types are pure functions of the spec.

use std::collections::HashSet;
use std::path::Path;

use serde_json::{Value, json};

use crate::parquet::{schema_for, write_table};

use super::project::out_schema;
use super::schema::INFO_SCHEMA;
use super::spec::{Ns, Src};

/// Every column that claims a source must actually have one.
///
/// A mistyped source name would otherwise yield a silently all-null column
/// that no other test would notice, so this is the guard that makes the
/// declarative schema safe to edit.
#[test]
fn every_source_column_resolves() {
    let mut missing = Vec::new();
    for table in INFO_SCHEMA {
        for col in table.cols {
            if col.ty.is_some() {
                continue; // declared type, no source by design
            }
            let found = match table.src {
                Src::Table(t) => schema_for(t).field_with_name(col.src).is_ok(),
                Src::Join { left, right, .. } => {
                    schema_for(left).field_with_name(col.src).is_ok()
                        || schema_for(right).field_with_name(col.src).is_ok()
                }
                Src::Own => false,
            };
            if !found {
                missing.push(format!(
                    "{}.{} <- {} ({:?})",
                    table.qualified_name(),
                    col.out,
                    col.src,
                    table.src
                ));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "columns with no resolvable source:\n  {}",
        missing.join("\n  ")
    );
}

/// Assembled tables declare their own types, since there is no source field to
/// derive them from.
#[test]
fn assembled_tables_declare_every_type() {
    for table in INFO_SCHEMA {
        if !matches!(table.src, Src::Own) {
            continue;
        }
        for col in table.cols {
            assert!(
                col.ty.is_some(),
                "{}.{} is assembled, so it must declare a type",
                table.qualified_name(),
                col.out
            );
        }
    }
}

#[test]
fn table_names_are_unique() {
    let mut seen = HashSet::new();
    for table in INFO_SCHEMA {
        assert!(
            seen.insert(table.qualified_name()),
            "duplicate table {}",
            table.qualified_name()
        );
    }
}

#[test]
fn column_names_are_unique_within_a_table() {
    for table in INFO_SCHEMA {
        let mut seen = HashSet::new();
        for col in table.cols {
            assert!(
                seen.insert(col.out),
                "{} declares {} twice",
                table.qualified_name(),
                col.out
            );
        }
    }
}

/// The output schema must be derivable for every table.
#[test]
fn output_schema_derives_for_every_table() {
    for table in INFO_SCHEMA {
        let schema =
            out_schema(table).unwrap_or_else(|e| panic!("{}: {e}", table.qualified_name()));
        assert_eq!(
            schema.fields().len(),
            table.cols.len(),
            "{} field count",
            table.qualified_name()
        );
        for (field, col) in schema.fields().iter().zip(table.cols) {
            assert_eq!(
                field.name(),
                col.out,
                "{} column order",
                table.qualified_name()
            );
        }
    }
}

/// Source-specific columns belong to `dbt.sources` and nowhere else.
#[test]
fn source_columns_live_only_on_sources() {
    const SOURCE_ONLY: &[&str] = &[
        "source_name",
        "source_description",
        "loader",
        "loaded_at_field",
        "loaded_at_query",
        "freshness",
        "external",
        "source_meta",
        "quoting",
    ];
    for table in INFO_SCHEMA {
        // The project table legitimately carries its own `quoting`.
        if table.name == "sources" || table.name == "project" {
            continue;
        }
        for col in table.cols {
            assert!(
                !SOURCE_ONLY.contains(&col.out),
                "{} must not carry the source-only column {}",
                table.qualified_name(),
                col.out
            );
        }
    }
}

/// Columns the schema retires must not survive anywhere under their old name.
#[test]
fn retired_columns_are_gone() {
    const RETIRED: &[&str] = &[
        "access_level",
        "group_name",
        "node_constraints",
        "patch_path",
        "table_role",
        "checksum",
        "compiled_at",
        "raw_code_hash",
        "extra_ctes",
        "file_path",
        "edge_type",
        "declared_type",
        "inferred_type",
        "catalog_type",
        "column_constraints",
        "catalog_comment",
        "lineage_kind",
        "test_namespace",
        "attached_node",
        "test_where",
        "test_limit",
        "kwargs",
        "block_contents",
        "git_is_dirty",
        "used_in",
        "depends_on_nodes",
        "depends_on_macros",
        "supported_languages",
        "project_id",
    ];
    for table in INFO_SCHEMA {
        for col in table.cols {
            // `dbt_internal` mirrors its source verbatim, and macros keep a
            // genuine macro-to-macro dependency list.
            if table.ns == Ns::DbtInternal
                || (table.name == "macros" && col.out == "depends_on_macros")
            {
                continue;
            }
            assert!(
                !RETIRED.contains(&col.out),
                "{} still exposes the retired column {}",
                table.qualified_name(),
                col.out
            );
        }
    }
}

/// `from_*`/`to_*` are retired on lineage but are the *new* names on semantic
/// relationships, so this pair has to be checked per table rather than globally.
#[test]
fn lineage_uses_parent_child_naming() {
    let lineage = INFO_SCHEMA
        .iter()
        .find(|t| t.name == "column_lineage")
        .expect("column_lineage");
    let names: Vec<&str> = lineage.cols.iter().map(|c| c.out).collect();
    for retired in [
        "from_node_unique_id",
        "from_column_name",
        "to_node_unique_id",
        "to_column_name",
    ] {
        assert!(
            !names.contains(&retired),
            "column_lineage still has {retired}"
        );
    }
    for expected in [
        "parent_node_unique_id",
        "parent_column_name",
        "child_node_unique_id",
        "child_column_name",
        "evolution",
    ] {
        assert!(
            names.contains(&expected),
            "column_lineage missing {expected}"
        );
    }

    let rel = INFO_SCHEMA
        .iter()
        .find(|t| t.name == "semantic_relationships")
        .expect("semantic_relationships");
    let names: Vec<&str> = rel.cols.iter().map(|c| c.out).collect();
    for expected in [
        "from_node_unique_id",
        "to_node_unique_id",
        "from_column_names",
        "to_column_names",
    ] {
        assert!(
            names.contains(&expected),
            "semantic_relationships missing {expected}"
        );
    }
}

/// `views.sql` must cover every table, in every namespace, exactly once.
#[test]
fn views_sql_covers_every_table() {
    let dir = tempfile::tempdir().unwrap();
    super::views::write_views_sql(dir.path()).unwrap();
    let sql = std::fs::read_to_string(dir.path().join("views.sql")).unwrap();

    for ns in Ns::ALL {
        assert!(
            sql.contains(&format!("CREATE SCHEMA IF NOT EXISTS {};", ns.prefix())),
            "missing schema {}",
            ns.prefix()
        );
    }
    for table in INFO_SCHEMA {
        let stmt = format!(
            "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}');",
            table.qualified_name(),
            table.file_name()
        );
        assert_eq!(
            sql.matches(&stmt).count(),
            1,
            "expected exactly one view for {}",
            table.qualified_name()
        );
    }
    assert!(sql.contains("dbt_rt.run_results_latest"));
}

/// Tables removed by the overhaul must not reappear.
#[test]
fn removed_tables_are_absent() {
    const REMOVED: &[&str] = &[
        "nodes",
        "generation",
        "catalog_tables",
        "catalog_stats",
        "column_stats",
        "sample_data",
        "context",
        "context_terms",
        "context_links",
        "nodes_enriched",
        "tests_enriched",
        "invocation_nodes",
        "test_failures",
        "dag_validity",
        "node_status",
        "test_metadata",
        "docs",
        "graph_nodes",
    ];
    for table in INFO_SCHEMA {
        assert!(
            !REMOVED.contains(&table.name),
            "{} was removed by the overhaul",
            table.qualified_name()
        );
    }
}

// ── projection tests ────────────────────────────────────────────────────────
// These write source-shaped parquet directly, so they exercise the projection
// without needing a dbt run or a metadata directory.

/// A source-shaped node row with every non-nullable field populated.
fn node_row(unique_id: &str, name: &str, resource_type: &str) -> Value {
    json!({
        "unique_id": unique_id,
        "name": name,
        "resource_type": resource_type,
        "package_name": "pkg",
        "file_path": format!("models/{name}.sql"),
        "original_file_path": format!("models/{name}.sql"),
        "fqn": ["pkg", name],
        "database_name": "db",
        "schema_name": "sch",
        "enabled": true,
        "contract_enforced": false,
        "primary_key": [],
        "docs_show": true,
        "tags": [],
        "classifiers": [],
        "grain": [],
        "grain_declared": [],
        "grain_tested": [],
        "grain_inferred": [],
        "ingested_at": "2026-08-21T00:00:00Z",
    })
}

fn write_source(dir: &Path, table: &str, rows: &[Value]) {
    let prefix =
        if crate::db::DBT_RT_TABLES.contains(&table) && !crate::db::DBT_TABLES.contains(&table) {
            "dbt_rt"
        } else {
            "dbt"
        };
    write_table(
        &dir.join(format!("{prefix}.{table}.parquet")),
        schema_for(table),
        rows,
    )
    .unwrap();
}

/// Column names of a written output table.
fn out_columns(dir: &Path, file: &str) -> Vec<String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(dir.join(file)).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Rows of a written output table, as JSON objects.
fn out_rows(dir: &Path, file: &str) -> Vec<Value> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let path = dir.join(file);
    let f = std::fs::File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = Vec::new();
    for batch in reader.flatten() {
        let mut decoded: Vec<Value> = serde_arrow::from_record_batch(&batch).unwrap();
        rows.append(&mut decoded);
    }
    rows
}

/// Each resource type lands in its own table, and the source-only columns
/// exist only on `dbt.sources`.
#[test]
fn resource_types_split_into_their_own_tables() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    write_source(
        staging.path(),
        "nodes",
        &[
            node_row("model.pkg.a", "a", "model"),
            node_row("model.pkg.b", "b", "model"),
            node_row("seed.pkg.s", "s", "seed"),
            node_row("source.pkg.src.t", "t", "source"),
            node_row("snapshot.pkg.snap", "snap", "snapshot"),
        ],
    );
    super::project_all(staging.path(), out.path()).unwrap();

    assert_eq!(out_rows(out.path(), "dbt.models.parquet").len(), 2);
    assert_eq!(out_rows(out.path(), "dbt.seeds.parquet").len(), 1);
    assert_eq!(out_rows(out.path(), "dbt.sources.parquet").len(), 1);
    assert_eq!(out_rows(out.path(), "dbt.snapshots.parquet").len(), 1);
    // Nothing leaks into a table it does not belong to.
    assert_eq!(out_rows(out.path(), "dbt.analyses.parquet").len(), 0);

    let models = out_columns(out.path(), "dbt.models.parquet");
    assert!(!models.contains(&"source_name".to_string()));
    assert!(!models.contains(&"loader".to_string()));
    let sources = out_columns(out.path(), "dbt.sources.parquet");
    assert!(sources.contains(&"source_name".to_string()));
    assert!(sources.contains(&"external".to_string()));
    assert!(!sources.contains(&"external_config".to_string()));
}

/// Node-level columns are renamed, and the retired names are gone.
#[test]
fn node_columns_are_renamed_and_carry_their_data() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut row = node_row("model.pkg.a", "a", "model");
    row["access_level"] = json!("public");
    row["group_name"] = json!("finance");
    row["patch_path"] = json!("models/schema.yml");
    row["table_role"] = json!("dimension");
    write_source(staging.path(), "nodes", &[row]);
    super::project_all(staging.path(), out.path()).unwrap();

    let cols = out_columns(out.path(), "dbt.models.parquet");
    for new in [
        "access",
        "group",
        "properties_yml_file_path",
        "layer_inferred",
    ] {
        assert!(cols.contains(&new.to_string()), "missing {new}");
    }
    for old in [
        "access_level",
        "group_name",
        "patch_path",
        "table_role",
        "checksum",
    ] {
        assert!(!cols.contains(&old.to_string()), "still has {old}");
    }

    // The data moved with the name, not just the header.
    let rows = out_rows(out.path(), "dbt.models.parquet");
    assert_eq!(rows[0]["access"], json!("public"));
    assert_eq!(rows[0]["group"], json!("finance"));
    assert_eq!(
        rows[0]["properties_yml_file_path"],
        json!("models/schema.yml")
    );
    assert_eq!(rows[0]["layer_inferred"], json!("dimension"));
}

/// A test with no generic-test detail must survive the join. An inner join
/// would silently drop every singular test.
#[test]
fn data_tests_keeps_tests_without_detail() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    write_source(
        staging.path(),
        "nodes",
        &[
            node_row("test.pkg.generic", "generic", "test"),
            node_row("test.pkg.singular", "singular", "test"),
        ],
    );
    write_source(
        staging.path(),
        "test_metadata",
        &[json!({
            "unique_id": "test.pkg.generic",
            "test_name": "not_null",
            "test_namespace": "dbt_utils",
            "attached_node": "model.pkg.a",
            "test_where": "id > 0",
            "test_limit": 10,
            "kwargs": "{\"column_name\":\"id\"}",
            "ingested_at": "2026-08-21T00:00:00Z",
        })],
    );
    super::project_all(staging.path(), out.path()).unwrap();

    let rows = out_rows(out.path(), "dbt.data_tests.parquet");
    assert_eq!(rows.len(), 2, "the singular test must not be dropped");

    let generic = rows.iter().find(|r| r["name"] == json!("generic")).unwrap();
    // Node-level and detail columns on the same row, under their new names.
    assert_eq!(generic["package_name"], json!("pkg"));
    assert_eq!(generic["test_name"], json!("not_null"));
    assert_eq!(generic["test_definition_package"], json!("dbt_utils"));
    assert_eq!(generic["node_unique_id"], json!("model.pkg.a"));
    assert_eq!(generic["where"], json!("id > 0"));
    assert_eq!(generic["limit"], json!(10));
    assert_eq!(generic["arguments"], json!("{\"column_name\":\"id\"}"));

    let singular = rows
        .iter()
        .find(|r| r["name"] == json!("singular"))
        .unwrap();
    assert_eq!(singular["package_name"], json!("pkg"));
    assert!(singular["test_name"].is_null());
    assert!(singular["node_unique_id"].is_null());
}

/// `dag_nodes` covers resource types that never reach the node set, skips
/// disabled resources, and skips types that are not part of the DAG.
#[test]
fn dag_nodes_spans_the_whole_graph() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut disabled = node_row("model.pkg.off", "off", "model");
    disabled["enabled"] = json!(false);
    write_source(
        staging.path(),
        "nodes",
        &[
            node_row("model.pkg.a", "a", "model"),
            node_row("seed.pkg.s", "s", "seed"),
            // Not a DAG participant.
            node_row("macro.pkg.m", "m", "macro"),
            disabled,
        ],
    );
    // Exposures live only in their own table, never in the node set.
    write_source(
        staging.path(),
        "exposures",
        &[json!({
            "unique_id": "exposure.pkg.dash",
            "name": "dash",
            "exposure_type": "dashboard",
            "fqn": ["pkg", "dash"],
            "depends_on_nodes": [],
            "depends_on_macros": [],
            "tags": [],
            "ingested_at": "2026-08-21T00:00:00Z",
        })],
    );
    super::project_all(staging.path(), out.path()).unwrap();

    let rows = out_rows(out.path(), "dbt.dag_nodes.parquet");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r["unique_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"model.pkg.a"));
    assert!(ids.contains(&"seed.pkg.s"));
    assert!(
        ids.contains(&"exposure.pkg.dash"),
        "side tables must be included"
    );
    assert!(!ids.contains(&"macro.pkg.m"), "macros are not DAG nodes");
    assert!(
        !ids.contains(&"model.pkg.off"),
        "disabled nodes are not DAG nodes"
    );
}

/// `project_vars` is one row per (project, variable), and the adapter's
/// built-in packages are left out.
#[test]
fn project_vars_is_one_row_per_project_and_variable() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    write_source(
        staging.path(),
        "project",
        &[json!({
            "project_name": "pkg",
            "adapter_type": "duckdb",
            "ingested_at": "2026-08-21T00:00:00Z",
        })],
    );
    // The source keys each package's whole variable map by package name.
    let vars = "{\"region\":\"emea\",\"retention_days\":90}";
    write_source(
        staging.path(),
        "project_vars",
        &[
            json!({"var_name": "pkg", "var_value": vars, "ingested_at": "2026-08-21T00:00:00Z"}),
            json!({"var_name": "dbt", "var_value": vars, "ingested_at": "2026-08-21T00:00:00Z"}),
            json!({"var_name": "dbt_duckdb", "var_value": vars, "ingested_at": "2026-08-21T00:00:00Z"}),
        ],
    );
    super::project_all(staging.path(), out.path()).unwrap();

    let rows = out_rows(out.path(), "dbt.project_vars.parquet");
    assert_eq!(
        rows.len(),
        2,
        "one row per variable, built-in packages skipped"
    );
    for row in &rows {
        assert_eq!(row["project_name"], json!("pkg"));
    }
    let region = rows
        .iter()
        .find(|r| r["var_name"] == json!("region"))
        .unwrap();
    // A plain string is rendered bare rather than as JSON.
    assert_eq!(region["var_value"], json!("emea"));
    let retention = rows
        .iter()
        .find(|r| r["var_name"] == json!("retention_days"))
        .unwrap();
    assert_eq!(retention["var_value"], json!("90"));
}

/// Column lineage is renamed to parent/child.
#[test]
fn column_lineage_is_renamed() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    write_source(
        staging.path(),
        "column_lineage",
        &[json!({
            "from_node_unique_id": "model.pkg.a",
            "from_column_name": "id",
            "to_node_unique_id": "model.pkg.b",
            "to_column_name": "id",
            "lineage_kind": "copy",
            "ingested_at": "2026-08-21T00:00:00Z",
        })],
    );
    super::project_all(staging.path(), out.path()).unwrap();

    let rows = out_rows(out.path(), "dbt.column_lineage.parquet");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["parent_node_unique_id"], json!("model.pkg.a"));
    assert_eq!(rows[0]["child_node_unique_id"], json!("model.pkg.b"));
    assert_eq!(rows[0]["evolution"], json!("copy"));
}

/// Every table gets a file even with nothing to put in it, because `views.sql`
/// creates a view per table and a view over a missing file fails every query.
#[test]
fn every_table_is_written_even_when_empty() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    super::project_all(staging.path(), out.path()).unwrap();
    super::views::write_views_sql(out.path()).unwrap();

    for table in INFO_SCHEMA {
        let path = out.path().join(table.file_name());
        assert!(path.exists(), "{} was not written", table.qualified_name());
        // Readable, and carrying the declared shape rather than an empty schema.
        let cols = out_columns(out.path(), &table.file_name());
        assert_eq!(
            cols.len(),
            table.cols.len(),
            "{} column count",
            table.qualified_name()
        );
    }
    assert!(out.path().join("views.sql").exists());
}

// ── versioning ──────────────────────────────────────────────────────────────

/// Key-value metadata of a written output table.
fn out_kv(dir: &Path, file: &str) -> Vec<(String, Option<String>)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let f = std::fs::File::open(dir.join(file)).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Every file is self-describing on its own, even copied out of its directory.
#[test]
fn every_file_carries_the_version_in_its_metadata() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    super::project_all(staging.path(), out.path()).unwrap();

    let want = super::INFO_SCHEMA_VERSION.to_string();
    for table in INFO_SCHEMA {
        let kv = out_kv(out.path(), &table.file_name());
        assert!(
            kv.iter()
                .any(|(k, v)| k == super::INFO_SCHEMA_VERSION_KEY && v.as_deref() == Some(&want)),
            "{} is missing {}",
            table.qualified_name(),
            super::INFO_SCHEMA_VERSION_KEY
        );
    }
}

/// `dbt.project.schema_version` is filled by the writer, not projected, so it
/// has to survive a projection that has no source column for it.
#[test]
fn project_carries_the_schema_version() {
    let staging = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    write_source(
        staging.path(),
        "project",
        &[json!({
            "project_name": "pkg",
            "dbt_version": "2.0.0",
            "adapter_type": "duckdb",
            "ingested_at": "2026-08-21T00:00:00Z",
        })],
    );
    super::project_all(staging.path(), out.path()).unwrap();

    let rows = out_rows(out.path(), "dbt.project.parquet");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["schema_version"], json!(super::INFO_SCHEMA_VERSION));
}

/// Tables land under the version, and `views.sql` lands beside them — its
/// `read_parquet` paths are relative, so the two must share a directory.
#[test]
fn output_lands_under_the_version_directory() {
    let metadata = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let staging = tempfile::tempdir().unwrap();
    super::write_info_schema(metadata.path(), root.path(), staging.path()).unwrap();

    let versioned = super::versioned_dir(root.path());
    assert_eq!(
        versioned.file_name().unwrap().to_string_lossy(),
        format!("v{}", super::INFO_SCHEMA_VERSION)
    );
    assert!(versioned.join("views.sql").exists());
    for table in INFO_SCHEMA {
        assert!(
            versioned.join(table.file_name()).exists(),
            "{} was not written under the version directory",
            table.qualified_name()
        );
        assert!(
            !root.path().join(table.file_name()).exists(),
            "{} must not also be written at the unversioned root",
            table.qualified_name()
        );
    }
}

/// The benchmark harness runs, separates the cold run from the steady state, and
/// reports every stage. The numbers themselves are not asserted — this pins the
/// harness so a real corpus run cannot fail on the harness rather than the
/// conversion.
#[test]
fn bench_harness_reports_every_stage() {
    let metadata = tempfile::tempdir().unwrap();
    let workdir = tempfile::tempdir().unwrap();
    let report = super::bench::run("empty", metadata.path(), workdir.path(), 3).unwrap();

    assert_eq!(report.iterations.len(), 3);
    assert!(report.iterations[0].cold, "the first run must be cold");
    assert!(
        report.iterations[1..].iter().all(|i| !i.cold),
        "only the first run may be cold"
    );
    assert_eq!(report.steady().len(), 2, "the cold run is not steady state");
    assert_eq!(report.iterations[0].tables, INFO_SCHEMA.len());

    // An empty metadata directory records no invocation, so there is no
    // denominator — the harness must still report rather than divide by zero.
    assert!(report.invocation_secs.is_none());
    assert!(report.fraction_of_invocation().is_none());

    let table = report.table();
    for stage in crate::ingest::timings::Stage::ALL {
        assert!(
            table.contains(stage.label()),
            "report omits {}:\n{table}",
            stage.label()
        );
    }
    assert!(table.contains("not recorded"), "{table}");
}

/// Benchmark a metadata corpus produced outside the test suite, named by
/// `FS_INFO_SCHEMA_BENCH_METADATA`. This is how the large corpora (`scale_1k`,
/// `scale_6k`) are measured without carrying thousands of models in the repo, and
/// it needs no CLI — only a `target/metadata` directory:
///
/// ```text
/// FS_INFO_SCHEMA_BENCH_METADATA=/path/to/project/target/metadata \
///   cargo nextest run -p dbt-index-core bench_external_corpus \
///     --run-ignored all --no-capture
/// ```
///
/// `#[ignore]`d and printing rather than asserting: it is a measurement, and the
/// corpus it needs is not in the repo. Skips loudly if the variable is unset so a
/// deliberate run cannot be mistaken for a pass.
#[test]
#[ignore = "benchmark: needs FS_INFO_SCHEMA_BENCH_METADATA"]
fn bench_external_corpus_stage_attribution() {
    const CORPUS_ENV: &str = "FS_INFO_SCHEMA_BENCH_METADATA";
    let Ok(corpus) = std::env::var(CORPUS_ENV) else {
        eprintln!("SKIP: {CORPUS_ENV} is not set");
        return;
    };
    let metadata = std::path::PathBuf::from(&corpus);
    assert!(
        metadata.is_dir(),
        "{CORPUS_ENV} does not name a directory: {corpus}"
    );

    let workdir = tempfile::tempdir().unwrap();
    // The project directory two levels up from `<project>/target/metadata` is the
    // most useful label; fall back to the path itself.
    let label = metadata
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| corpus.clone());
    let report = super::bench::run(label, &metadata, workdir.path(), 5).unwrap();

    eprintln!("\n{}", report.table());
    assert!(
        report.calls(crate::ingest::timings::Stage::EpochRead) > 0,
        "no epoch file was read — {corpus} holds no epoch metadata, so these \
         numbers measure nothing:\n{}",
        report.table()
    );
}
