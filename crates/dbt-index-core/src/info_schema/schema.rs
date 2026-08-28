//! The information schema: one `TableSpec` per output table.
//!
//! This file is the reviewable statement of the schema contract. Reading it
//! top to bottom should answer "what columns does `dbt.models` have, and where
//! does each one come from".

use super::spec::{ColSpec, ColTy, Filter, Ns, Src, TableSpec, c, n, r};

/// Columns shared by every table derived from the node set.
///
/// `$extra` is spliced in before `ingested_at` so that column stays last, as
/// it is in every source table.
///
/// Textual expansion rather than a shared `const` slice because const slice
/// concatenation is not available here.
macro_rules! node_cols {
    ($($extra:expr,)*) => {
        &[
            c("unique_id"),
            c("name"),
            c("resource_type"),
            c("package_name"),
            // `file_path` is dropped: of the two path columns only
            // `original_file_path` is populated.
            c("original_file_path"),
            c("fqn"),
            c("alias"),
            c("description"),
            c("node_language"),
            c("raw_code"),
            c("database_name"),
            c("schema_name"),
            c("relation_name"),
            c("identifier"),
            c("enabled"),
            c("materialized"),
            c("incremental_strategy"),
            c("on_schema_change"),
            c("unique_key"),
            c("full_refresh"),
            c("persist_docs"),
            c("pre_hook"),
            c("post_hook"),
            c("grants"),
            c("config"),
            r("access", "access_level"),
            r("group", "group_name"),
            c("contract_enforced"),
            c("version"),
            c("latest_version"),
            c("deprecation_date"),
            r("constraints", "node_constraints"),
            c("primary_key"),
            c("docs_show"),
            r("properties_yml_file_path", "patch_path"),
            c("time_spine"),
            c("tags"),
            c("classifiers"),
            c("meta"),
            c("ai_context"),
            c("compiled_code"),
            c("compiled_path"),
            c("search_text"),
            c("grain"),
            c("grain_declared"),
            c("grain_tested"),
            c("grain_inferred"),
            r("layer_inferred", "table_role"),
            $($extra,)*
            c("ingested_at"),
        ]
    };
}

/// Every table in the information schema.
pub const INFO_SCHEMA: &[TableSpec] = &[
    // ── project metadata ────────────────────────────────────────────────
    TableSpec {
        ns: Ns::Dbt,
        name: "project",
        src: Src::Table("project"),
        filter: Filter::All,
        // `project_id` and `description` are dropped: nothing sets either.
        // `last_full_parse_at` absorbs the former single-row `generation`
        // table; it is filled in by the writer, not projected. So is
        // `schema_version`, which makes a set of files self-describing without
        // the consumer having to read parquet metadata or parse the path.
        cols: &[
            n("schema_version", ColTy::I64),
            c("project_name"),
            c("dbt_version"),
            c("adapter_type"),
            c("quoting"),
            c("ai_context"),
            c("git_sha"),
            c("git_branch"),
            r("git_uncommitted_changes", "git_is_dirty"),
            n("last_full_parse_at", ColTy::TsUtc),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "packages",
        src: Src::Table("packages"),
        filter: Filter::All,
        cols: &[
            c("package_name"),
            c("package_source"),
            c("version"),
            c("git_url"),
            c("git_revision"),
            c("local_path"),
            c("ingested_at"),
        ],
    },
    // One row per (project, variable). Assembled by the writer, which
    // un-nests the two-level `{package: {var: value}}` map that the source
    // table collapses into one row per package.
    TableSpec {
        ns: Ns::Dbt,
        name: "project_vars",
        src: Src::Own,
        filter: Filter::All,
        cols: &[
            n("project_name", ColTy::Utf8),
            n("var_name", ColTy::Utf8),
            n("var_value", ColTy::Utf8),
            n("ingested_at", ColTy::TsUtc),
        ],
    },
    // `used_in` is dropped: it is never populated.
    TableSpec {
        ns: Ns::Dbt,
        name: "project_env_vars",
        src: Src::Table("project_env_vars"),
        filter: Filter::All,
        cols: &[c("env_var_name"), c("ingested_at")],
    },
    // ── one table per resource type ─────────────────────────────────────
    TableSpec {
        ns: Ns::Dbt,
        name: "models",
        src: Src::Table("nodes"),
        filter: Filter::ResourceTypeIn(&["model"]),
        cols: node_cols![],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "seeds",
        src: Src::Table("nodes"),
        filter: Filter::ResourceTypeIn(&["seed"]),
        cols: node_cols![],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "snapshots",
        src: Src::Table("nodes"),
        filter: Filter::ResourceTypeIn(&["snapshot"]),
        cols: node_cols![],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "functions",
        src: Src::Table("nodes"),
        filter: Filter::ResourceTypeIn(&["function"]),
        cols: node_cols![],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "analyses",
        src: Src::Table("nodes"),
        filter: Filter::ResourceTypeIn(&["analysis"]),
        cols: node_cols![],
    },
    // Empty for now: operations are not part of the node set, so nothing
    // reaches this table. The shape is published so it can be filled later
    // without a schema change.
    TableSpec {
        ns: Ns::Dbt,
        name: "hooks",
        src: Src::Table("nodes"),
        filter: Filter::ResourceTypeIn(&["operation", "sql_operation"]),
        cols: node_cols![],
    },
    // The source-specific columns live here and nowhere else.
    TableSpec {
        ns: Ns::Dbt,
        name: "sources",
        src: Src::Table("nodes"),
        filter: Filter::ResourceTypeIn(&["source"]),
        cols: node_cols![
            c("source_name"),
            c("source_description"),
            c("loader"),
            c("loaded_at_field"),
            c("loaded_at_query"),
            c("freshness"),
            r("external", "external_config"),
            c("source_meta"),
            c("quoting"),
        ],
    },
    // Node-level columns joined with the generic-test detail. The join is a
    // LEFT join: a singular test has a node row and no detail row, and must
    // still appear.
    TableSpec {
        ns: Ns::Dbt,
        name: "data_tests",
        src: Src::Join {
            left: "nodes",
            right: "test_metadata",
            left_on: "unique_id",
            right_on: "unique_id",
        },
        filter: Filter::ResourceTypeIn(&["test"]),
        cols: &[
            c("unique_id"),
            c("name"),
            c("package_name"),
            c("original_file_path"),
            c("fqn"),
            c("description"),
            c("database_name"),
            c("schema_name"),
            c("relation_name"),
            c("enabled"),
            c("materialized"),
            c("config"),
            c("tags"),
            c("meta"),
            r("group", "group_name"),
            r("properties_yml_file_path", "patch_path"),
            c("compiled_code"),
            c("compiled_path"),
            // generic-test detail
            c("test_name"),
            r("test_definition_package", "test_namespace"),
            r("arguments", "kwargs"),
            c("column_name"),
            r("node_unique_id", "attached_node"),
            c("severity"),
            c("warn_if"),
            c("error_if"),
            c("fail_calc"),
            c("store_failures"),
            c("store_failures_as"),
            r("where", "test_where"),
            r("limit", "test_limit"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "unit_tests",
        src: Src::Table("unit_tests"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("model"),
            c("description"),
            c("package_name"),
            c("original_file_path"),
            c("fqn"),
            c("given"),
            c("expect"),
            c("overrides"),
            c("versions"),
            c("config"),
            c("created_at"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "macros",
        src: Src::Table("macros"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("package_name"),
            c("original_file_path"),
            c("macro_sql"),
            c("description"),
            c("depends_on_macros"),
            c("arguments"),
            c("docs_show"),
            r("properties_yml_file_path", "patch_path"),
            c("meta"),
            c("created_at"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "groups",
        src: Src::Table("groups"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("description"),
            c("package_name"),
            c("original_file_path"),
            c("owner_name"),
            c("owner_email"),
            c("config"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "exposures",
        src: Src::Table("exposures"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("exposure_type"),
            c("label"),
            c("owner_name"),
            c("owner_email"),
            c("url"),
            c("maturity"),
            c("description"),
            c("package_name"),
            c("original_file_path"),
            c("fqn"),
            r("depends_on", "depends_on_nodes"),
            c("tags"),
            c("meta"),
            c("config"),
            c("created_at"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "metrics",
        src: Src::Table("metrics"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("label"),
            c("metric_type"),
            c("description"),
            c("package_name"),
            c("original_file_path"),
            c("fqn"),
            c("type_params"),
            c("metric_filter"),
            c("time_granularity"),
            c("semantic_model_name"),
            c("input_metric_names"),
            r("group", "group_name"),
            c("tags"),
            c("meta"),
            c("ai_context"),
            c("config"),
            c("created_at"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "docs_blocks",
        src: Src::Table("docs"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("package_name"),
            c("original_file_path"),
            r("content", "block_contents"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "saved_queries",
        src: Src::Table("saved_queries"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("label"),
            c("description"),
            c("package_name"),
            c("original_file_path"),
            c("fqn"),
            c("query_params"),
            c("exports"),
            r("depends_on", "depends_on_nodes"),
            r("group", "group_name"),
            c("tags"),
            c("config"),
            c("created_at"),
            c("ingested_at"),
        ],
    },
    // ── semantic layer ──────────────────────────────────────────────────
    TableSpec {
        ns: Ns::Dbt,
        name: "semantic_models",
        src: Src::Table("semantic_models"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("model"),
            c("label"),
            c("description"),
            c("package_name"),
            c("original_file_path"),
            c("fqn"),
            c("node_relation"),
            c("primary_entity"),
            c("defaults"),
            r("group", "group_name"),
            c("config"),
            c("created_at"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "semantic_entities",
        src: Src::Table("semantic_entities"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("entity_type"),
            c("description"),
            c("label"),
            c("entity_role"),
            c("expr"),
            c("config"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "semantic_measures",
        src: Src::Table("semantic_measures"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("agg"),
            c("description"),
            c("label"),
            c("expr"),
            c("create_metric"),
            c("agg_time_dimension"),
            c("agg_params"),
            c("non_additive_dimension"),
            c("config"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "semantic_dimensions",
        src: Src::Table("semantic_dimensions"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("name"),
            c("dimension_type"),
            c("description"),
            c("label"),
            c("expr"),
            c("is_partition"),
            c("time_granularity"),
            c("validity_params"),
            c("config"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "semantic_relationships",
        src: Src::Table("semantic_relationships"),
        filter: Filter::All,
        cols: &[
            c("name"),
            r("from_node_unique_id", "from_unique_id"),
            r("to_node_unique_id", "to_unique_id"),
            r("from_column_names", "from_columns"),
            r("to_column_names", "to_columns"),
            c("cardinality"),
            c("relationship_type"),
            c("ai_context"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "time_spines",
        src: Src::Table("time_spines"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("primary_column"),
            c("primary_granularity"),
            c("custom_granularities"),
            c("node_relation"),
            c("ingested_at"),
        ],
    },
    // ── graph ───────────────────────────────────────────────────────────
    // Every enabled resource that participates in the DAG, with its type.
    // Assembled by the writer from the node set.
    TableSpec {
        ns: Ns::Dbt,
        name: "dag_nodes",
        src: Src::Own,
        filter: Filter::All,
        cols: &[
            n("unique_id", ColTy::Utf8),
            n("resource_type", ColTy::Utf8),
            n("ingested_at", ColTy::TsUtc),
        ],
    },
    // `edge_type` is dropped.
    TableSpec {
        ns: Ns::Dbt,
        name: "edges",
        src: Src::Table("edges"),
        filter: Filter::All,
        cols: &[
            c("parent_unique_id"),
            c("child_unique_id"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "node_columns",
        src: Src::Table("node_columns"),
        filter: Filter::All,
        cols: &[
            r("node_unique_id", "unique_id"),
            c("column_name"),
            c("column_index"),
            r("data_type_declared", "declared_type"),
            r("data_type_inferred", "inferred_type"),
            r("data_type_actual", "catalog_type"),
            c("data_type"),
            c("description"),
            c("label"),
            c("expression"),
            c("quote"),
            c("granularity"),
            c("tags"),
            c("classifiers"),
            c("meta"),
            r("constraints", "column_constraints"),
            c("tests"),
            r("comment", "catalog_comment"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::Dbt,
        name: "column_lineage",
        src: Src::Table("column_lineage"),
        filter: Filter::All,
        cols: &[
            r("parent_node_unique_id", "from_node_unique_id"),
            r("parent_column_name", "from_column_name"),
            r("child_node_unique_id", "to_node_unique_id"),
            r("child_column_name", "to_column_name"),
            r("evolution", "lineage_kind"),
            c("ingested_at"),
        ],
    },
    // The label taxonomy. Assembled by the writer; the source table is a
    // schemaless placeholder today.
    TableSpec {
        ns: Ns::Dbt,
        name: "classifiers",
        src: Src::Own,
        filter: Filter::All,
        cols: &[
            n("name", ColTy::Utf8),
            n("propagate", ColTy::Bool),
            n("labels", ColTy::ListUtf8),
            n("description", ColTy::Utf8),
            n("ingested_at", ColTy::TsUtc),
        ],
    },
    // ── runtime ─────────────────────────────────────────────────────────
    TableSpec {
        ns: Ns::DbtRt,
        name: "invocations",
        src: Src::Table("invocations"),
        filter: Filter::All,
        cols: &[
            c("invocation_id"),
            c("command"),
            c("selector"),
            c("dbt_version"),
            c("generated_at"),
            c("elapsed_time"),
            c("args"),
            c("node_count"),
            c("target_name"),
            c("target_type"),
            c("target_database"),
            c("target_schema"),
            c("target_threads"),
            c("vars_override"),
            c("git_sha"),
            c("git_branch"),
            r("git_uncommitted_changes", "git_is_dirty"),
        ],
    },
    TableSpec {
        ns: Ns::DbtRt,
        name: "run_results",
        src: Src::Table("run_results"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("invocation_id"),
            c("status"),
            c("execution_time"),
            c("thread_id"),
            c("message"),
            c("failures"),
            c("compiled"),
            c("compiled_code_hash"),
            c("relation_name"),
            c("adapter_response"),
            c("timing"),
            c("batch_results"),
            c("rows_affected"),
            c("created_at"),
        ],
    },
    // Moved out of the project namespace: this is a runtime result.
    TableSpec {
        ns: Ns::DbtRt,
        name: "source_freshness",
        src: Src::Table("source_freshness"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("invocation_id"),
            c("status"),
            c("max_loaded_at"),
            c("snapshotted_at"),
            c("max_loaded_at_time_ago"),
            c("execution_time"),
            c("thread_id"),
            c("error"),
            c("warn_after_count"),
            c("warn_after_period"),
            c("error_after_count"),
            c("error_after_period"),
            c("freshness_filter"),
            c("adapter_response"),
            c("timing"),
            c("created_at"),
            c("ingested_at"),
        ],
    },
    TableSpec {
        ns: Ns::DbtRt,
        name: "diagnostics",
        src: Src::Table("diagnostics"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("invocation_id"),
            c("severity"),
            c("code"),
            c("message"),
            c("detail"),
            c("source_phase"),
            c("created_at"),
        ],
    },
    TableSpec {
        ns: Ns::DbtRt,
        name: "adapter_queries",
        src: Src::Table("adapter_queries"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("invocation_id"),
            c("query_index"),
            c("query_sql"),
            c("query_id"),
            c("rows_affected"),
            c("bytes_scanned"),
            c("query_cost"),
            c("error_message"),
            c("started_at"),
            c("completed_at"),
        ],
    },
    // ── internal ────────────────────────────────────────────────────────
    // Not part of the public contract.
    TableSpec {
        ns: Ns::DbtInternal,
        name: "node_input_files",
        src: Src::Table("node_input_files"),
        filter: Filter::All,
        cols: &[
            c("unique_id"),
            c("file_path"),
            c("file_hash"),
            c("input_kind"),
            c("ingested_at"),
        ],
    },
];

/// Resource types that participate in the DAG, for `dbt.dag_nodes`.
pub const DAG_RESOURCE_TYPES: &[&str] = &[
    "model",
    "seed",
    "source",
    "snapshot",
    "analysis",
    "function",
    "test",
    "unit_test",
    "metric",
    "exposure",
];

/// Look up a spec by namespace and name.
pub fn spec_for(ns: Ns, name: &str) -> Option<&'static TableSpec> {
    INFO_SCHEMA.iter().find(|t| t.ns == ns && t.name == name)
}

/// Every source table the schema reads from.
pub fn source_tables() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = Vec::new();
    for t in INFO_SCHEMA {
        match t.src {
            Src::Table(name) => v.push(name),
            Src::Join { left, right, .. } => {
                v.push(left);
                v.push(right);
            }
            Src::Own => {}
        }
    }
    v.sort_unstable();
    v.dedup();
    v
}

// Silence the unused-import warning when the macro above is the only user.
const _: fn(&'static str) -> ColSpec = c;
