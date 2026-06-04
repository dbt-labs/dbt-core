//! `GET /api/v1/search` — cross-resource project search (ADR-8).
//!
//! Supports two modes:
//! - **Browse mode** (`?q=` absent or empty): returns all resources sorted by
//!   `(name, unique_id)`, `matched_field: null`, `highlight: null`.
//! - **Search mode** (`?q=<term>`): whitespace-split AND ILIKE across
//!   `name`, `column`, `tag`, `fqn`, `description` (priority order).
//!
//! Cursor pagination: default 50, max 200. `?type=`, `?package=`, `?tag=`,
//! `?modeling_layer=` filters apply in both modes.

use std::collections::BTreeSet;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, bad_request_coded, internal_error};
use crate::handlers::node_base::str_col;
use crate::handlers::pagination::{Cursor, PageInfo, cursor_where_fragment};
use crate::handlers::sql::{escape_ilike, escape_str};
use crate::state::SharedState;

// Search uses its own page-size constants — not the module-level MAX_PAGE_SIZE = 1000.
const SEARCH_DEFAULT_PAGE_SIZE: u32 = 50;
const SEARCH_MAX_PAGE_SIZE: u32 = 200;

/// All resource types that can appear in search results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ResourceType {
    Model,
    Source,
    Seed,
    Snapshot,
    Test,
    UnitTest,
    Exposure,
    Metric,
    SemanticModel,
    SavedQuery,
    Macro,
    Group,
}

impl ResourceType {
    fn all() -> BTreeSet<ResourceType> {
        use ResourceType::*;
        [
            Model,
            Source,
            Seed,
            Snapshot,
            Test,
            UnitTest,
            Exposure,
            Metric,
            SemanticModel,
            SavedQuery,
            Macro,
            Group,
        ]
        .into_iter()
        .collect()
    }

    fn from_str(s: &str) -> Option<ResourceType> {
        use ResourceType::*;
        match s {
            "model" => Some(Model),
            "source" => Some(Source),
            "seed" => Some(Seed),
            "snapshot" => Some(Snapshot),
            "test" => Some(Test),
            "unit_test" => Some(UnitTest),
            "exposure" => Some(Exposure),
            "metric" => Some(Metric),
            "semantic_model" => Some(SemanticModel),
            "saved_query" => Some(SavedQuery),
            "macro" => Some(Macro),
            "group" => Some(Group),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        use ResourceType::*;
        match self {
            Model => "model",
            Source => "source",
            Seed => "seed",
            Snapshot => "snapshot",
            Test => "test",
            UnitTest => "unit_test",
            Exposure => "exposure",
            Metric => "metric",
            SemanticModel => "semantic_model",
            SavedQuery => "saved_query",
            Macro => "macro",
            Group => "group",
        }
    }

    /// Whether this resource type lives in `dbt.nodes` parquet.
    fn lives_in_dbt_nodes(self) -> bool {
        use ResourceType::*;
        matches!(self, Model | Source | Seed | Snapshot | Test)
    }
}

/// Parse a comma-separated list of resource types, returning 400-able error on unknown values.
fn parse_csv_resource_types(raw: &str) -> Result<BTreeSet<ResourceType>, String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| ResourceType::from_str(s).ok_or_else(|| format!("invalid type: {s}")))
        .collect()
}

/// Allowed modeling-layer values.
const VALID_MODELING_LAYERS: &[&str] = &["Staging", "Intermediate", "Marts"];

/// Layer -> LIKE conditions (mirrors models.rs LAYER_CONDITIONS).
const LAYER_CONDITIONS: &[(&str, &str)] = &[
    (
        "Staging",
        "lower(b.original_file_path) LIKE '%/staging/%' \
         OR lower(b.original_file_path) LIKE '%/stg_%' \
         OR lower(b.original_file_path) LIKE 'staging/%'",
    ),
    (
        "Intermediate",
        "lower(b.original_file_path) LIKE '%/intermediate/%' \
         OR lower(b.original_file_path) LIKE '%/int_%' \
         OR lower(b.original_file_path) LIKE 'intermediate/%'",
    ),
    (
        "Marts",
        "lower(b.original_file_path) LIKE '%/marts/%' \
         OR lower(b.original_file_path) LIKE '%/dim_%' \
         OR lower(b.original_file_path) LIKE '%/fct_%' \
         OR lower(b.original_file_path) LIKE 'marts/%'",
    ),
];

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// A single edge in the search response — one matched resource.
#[derive(Serialize)]
pub struct SearchEdge {
    pub matched_field: Option<String>,
    pub highlight: Option<String>,
    pub hit: SearchHit,
}

/// A flat hit struct covering all resource types. Type-specific fields are
/// `#[serde(skip_serializing_if = "Option::is_none")]` so they are absent
/// (not null) for types that don't have them.
#[derive(Serialize)]
pub struct SearchHit {
    pub unique_id: String,
    pub resource_type: String,
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fqn: Option<Vec<String>>,
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    pub package_name: Option<String>,
    /// model only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized: Option<String>,
    /// model only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_level: Option<String>,
    /// source only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    /// source only — null when has_source_freshness is false
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness_checked: Option<bool>,
    /// test / unit_test only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_type: Option<String>,
    /// exposure only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exposure_type: Option<String>,
    /// nodes only — last run completed_at (ISO string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executed_at: Option<String>,
}

/// Response envelope for `GET /api/v1/search`.
#[derive(Serialize)]
pub struct SearchResponse {
    pub data: Vec<SearchEdge>,
    pub page_info: PageInfo,
}

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
pub struct SearchQueryParams {
    /// Search query; absent or empty triggers browse mode.
    pub q: Option<String>,
    /// Comma-separated resource_type filter.
    #[serde(rename = "type")]
    pub type_filter: Option<String>,
    /// Comma-separated package_name filter.
    pub package: Option<String>,
    /// Comma-separated tag filter.
    pub tag: Option<String>,
    /// Comma-separated modeling_layer filter.
    pub modeling_layer: Option<String>,
    /// Comma-separated materialization filter (model-scoped). Any string accepted;
    /// unknown values return empty results, never 400.
    pub materialization: Option<String>,
    /// Page size — clamped to [1, SEARCH_MAX_PAGE_SIZE].
    pub first: Option<u32>,
    /// Opaque cursor from a prior page's `page_info.end_cursor`.
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL branch builders
// ---------------------------------------------------------------------------

/// Build the nodes branch (model/source/seed/snapshot/test) with optional
/// freshness JOIN. Projects a uniform column set.
fn nodes_branch_sql(types: &[&str], with_freshness: bool, with_run_results: bool) -> String {
    let type_list = types
        .iter()
        .map(|t| format!("'{}'", escape_str(t)))
        .collect::<Vec<_>>()
        .join(", ");

    let freshness_join = if with_freshness {
        "LEFT JOIN dbt.source_freshness sf ON sf.unique_id = n.unique_id"
    } else {
        ""
    };
    let freshness_col = if with_freshness {
        "sf.unique_id IS NOT NULL"
    } else {
        "NULL::BOOLEAN"
    };

    let (rr_join, executed_at_col) = if with_run_results {
        (
            "LEFT JOIN ( \
           SELECT unique_id, CAST(MAX(created_at) AS VARCHAR) AS executed_at \
           FROM dbt_rt.run_results \
           GROUP BY unique_id \
         ) rr ON rr.unique_id = n.unique_id",
            "rr.executed_at",
        )
    } else {
        ("", "NULL::VARCHAR")
    };

    format!(
        "SELECT n.unique_id, n.name, n.resource_type, n.package_name, \
         n.fqn, n.tags, n.description, \
         n.materialized, n.access_level, n.source_name, \
         {freshness_col} AS freshness_checked, \
         CASE WHEN n.resource_type = 'test' THEN 'test' \
              WHEN n.resource_type = 'unit_test' THEN 'unit_test' \
              ELSE NULL END AS test_type, \
         NULL::VARCHAR AS exposure_type, \
         {executed_at_col} AS executed_at, \
         n.original_file_path \
         FROM dbt.nodes n \
         {freshness_join} \
         {rr_join} \
         WHERE n.resource_type IN ({type_list})"
    )
}

fn exposures_branch_sql() -> String {
    "SELECT e.unique_id, e.name, 'exposure'::VARCHAR AS resource_type, e.package_name, \
     e.fqn, e.tags, e.description, \
     NULL::VARCHAR AS materialized, NULL::VARCHAR AS access_level, NULL::VARCHAR AS source_name, \
     NULL::BOOLEAN AS freshness_checked, \
     NULL::VARCHAR AS test_type, \
     e.exposure_type, \
     NULL::VARCHAR AS executed_at, \
     NULL::VARCHAR AS original_file_path \
     FROM dbt.exposures e"
        .to_owned()
}

fn macros_branch_sql() -> String {
    "SELECT m.unique_id, m.name, 'macro'::VARCHAR AS resource_type, m.package_name, \
     NULL::VARCHAR[] AS fqn, NULL::VARCHAR[] AS tags, m.description, \
     NULL::VARCHAR AS materialized, NULL::VARCHAR AS access_level, NULL::VARCHAR AS source_name, \
     NULL::BOOLEAN AS freshness_checked, \
     NULL::VARCHAR AS test_type, \
     NULL::VARCHAR AS exposure_type, \
     NULL::VARCHAR AS executed_at, \
     NULL::VARCHAR AS original_file_path \
     FROM dbt.macros m"
        .to_owned()
}

fn metrics_branch_sql() -> String {
    "SELECT m.unique_id, m.name, 'metric'::VARCHAR AS resource_type, m.package_name, \
     m.fqn, m.tags, m.description, \
     NULL::VARCHAR AS materialized, NULL::VARCHAR AS access_level, NULL::VARCHAR AS source_name, \
     NULL::BOOLEAN AS freshness_checked, \
     NULL::VARCHAR AS test_type, \
     NULL::VARCHAR AS exposure_type, \
     NULL::VARCHAR AS executed_at, \
     NULL::VARCHAR AS original_file_path \
     FROM dbt.metrics m"
        .to_owned()
}

fn saved_queries_branch_sql() -> String {
    "SELECT sq.unique_id, sq.name, 'saved_query'::VARCHAR AS resource_type, sq.package_name, \
     sq.fqn, sq.tags, sq.description, \
     NULL::VARCHAR AS materialized, NULL::VARCHAR AS access_level, NULL::VARCHAR AS source_name, \
     NULL::BOOLEAN AS freshness_checked, \
     NULL::VARCHAR AS test_type, \
     NULL::VARCHAR AS exposure_type, \
     NULL::VARCHAR AS executed_at, \
     NULL::VARCHAR AS original_file_path \
     FROM dbt.saved_queries sq"
        .to_owned()
}

fn semantic_models_branch_sql() -> String {
    "SELECT sm.unique_id, sm.name, 'semantic_model'::VARCHAR AS resource_type, sm.package_name, \
     sm.fqn, \
     COALESCE((json_extract(sm.config, '$.tags'))::VARCHAR[], []::VARCHAR[]) AS tags, \
     sm.description, \
     NULL::VARCHAR AS materialized, NULL::VARCHAR AS access_level, NULL::VARCHAR AS source_name, \
     NULL::BOOLEAN AS freshness_checked, \
     NULL::VARCHAR AS test_type, \
     NULL::VARCHAR AS exposure_type, \
     NULL::VARCHAR AS executed_at, \
     NULL::VARCHAR AS original_file_path \
     FROM dbt.semantic_models sm"
        .to_owned()
}

fn groups_branch_sql() -> String {
    "SELECT g.unique_id, g.name, 'group'::VARCHAR AS resource_type, g.package_name, \
     NULL::VARCHAR[] AS fqn, NULL::VARCHAR[] AS tags, g.description, \
     NULL::VARCHAR AS materialized, NULL::VARCHAR AS access_level, NULL::VARCHAR AS source_name, \
     NULL::BOOLEAN AS freshness_checked, \
     NULL::VARCHAR AS test_type, \
     NULL::VARCHAR AS exposure_type, \
     NULL::VARCHAR AS executed_at, \
     NULL::VARCHAR AS original_file_path \
     FROM dbt.groups g"
        .to_owned()
}

fn unit_tests_branch_sql() -> String {
    "SELECT ut.unique_id, ut.name, 'unit_test'::VARCHAR AS resource_type, ut.package_name, \
     ut.fqn, NULL::VARCHAR[] AS tags, ut.description, \
     NULL::VARCHAR AS materialized, NULL::VARCHAR AS access_level, NULL::VARCHAR AS source_name, \
     NULL::BOOLEAN AS freshness_checked, \
     'unit_test'::VARCHAR AS test_type, \
     NULL::VARCHAR AS exposure_type, \
     NULL::VARCHAR AS executed_at, \
     NULL::VARCHAR AS original_file_path \
     FROM dbt.unit_tests ut"
        .to_owned()
}

// ---------------------------------------------------------------------------
// SQL builder
// ---------------------------------------------------------------------------

/// Build the UNION base SQL from the requested resource types.
fn build_base_union(
    requested_types: &BTreeSet<ResourceType>,
    with_freshness: bool,
    with_run_results: bool,
) -> Option<String> {
    let mut branches: Vec<String> = Vec::new();

    let nodes_types: Vec<&str> = requested_types
        .iter()
        .filter(|t| t.lives_in_dbt_nodes())
        .map(|t| t.as_str())
        .collect();
    if !nodes_types.is_empty() {
        branches.push(nodes_branch_sql(
            &nodes_types,
            with_freshness,
            with_run_results,
        ));
    }
    if requested_types.contains(&ResourceType::Exposure) {
        branches.push(exposures_branch_sql());
    }
    if requested_types.contains(&ResourceType::Macro) {
        branches.push(macros_branch_sql());
    }
    if requested_types.contains(&ResourceType::Metric) {
        branches.push(metrics_branch_sql());
    }
    if requested_types.contains(&ResourceType::SavedQuery) {
        branches.push(saved_queries_branch_sql());
    }
    if requested_types.contains(&ResourceType::SemanticModel) {
        branches.push(semantic_models_branch_sql());
    }
    if requested_types.contains(&ResourceType::Group) {
        branches.push(groups_branch_sql());
    }
    if requested_types.contains(&ResourceType::UnitTest) {
        branches.push(unit_tests_branch_sql());
    }

    if branches.is_empty() {
        return None;
    }

    Some(branches.join("\nUNION ALL\n"))
}

/// Build the `WHERE` fragment for `?package=` filter.
fn package_where(raw: &str) -> String {
    let list = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| format!("'{}'", escape_str(s)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("b.package_name IN ({list})")
}

/// Build the `WHERE` fragment for `?tag=` filter.
/// Resource types without a tags column project NULL — `list_filter` on NULL
/// returns NULL, and `len(NULL) > 0` is false, so they're silently excluded.
fn tag_where(raw: &str) -> String {
    let conditions: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            let escaped = escape_str(s);
            format!("len(list_filter(b.tags, x -> lower(x) = lower('{escaped}'))) > 0")
        })
        .collect();
    if conditions.len() == 1 {
        conditions.into_iter().next().unwrap()
    } else {
        conditions
            .into_iter()
            .map(|c| format!("({c})"))
            .collect::<Vec<_>>()
            .join(" OR ")
    }
}

/// Build the `WHERE` fragment for `?modeling_layer=` filter.
fn modeling_layer_where(raw: &str) -> String {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|layer| {
            let cond = LAYER_CONDITIONS
                .iter()
                .find(|(name, _)| *name == layer)
                .map(|(_, cond)| *cond)
                .expect("layer already validated");
            format!("({cond})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Build the `field_matches` CTE for multi-token search.
/// Each token must match at least one field. The priority is:
/// name(1) > column(2) > tag(3) > fqn(4) > description(5).
fn build_field_matches_cte(tokens: &[String]) -> String {
    let mut per_token_matches = Vec::new();
    for token in tokens {
        let q = escape_ilike(token);
        let block = format!(
            "    SELECT unique_id, 'name' AS matched_field, 1 AS priority FROM base \
             WHERE name ILIKE '%' || '{q}' || '%' ESCAPE '\\'\n\
             UNION ALL\n\
             SELECT b.unique_id, 'column', 2 FROM base b \
             JOIN dbt.node_columns c USING (unique_id) \
             WHERE c.column_name ILIKE '%' || '{q}' || '%' ESCAPE '\\'\n\
             UNION ALL\n\
             SELECT unique_id, 'tag', 3 FROM base \
             WHERE tags IS NOT NULL \
             AND len(list_filter(tags, x -> x ILIKE '%' || '{q}' || '%' ESCAPE '\\')) > 0\n\
             UNION ALL\n\
             SELECT unique_id, 'fqn', 4 FROM base \
             WHERE fqn IS NOT NULL \
             AND array_to_string(fqn, '.') ILIKE '%' || '{q}' || '%' ESCAPE '\\'\n\
             UNION ALL\n\
             SELECT unique_id, 'description', 5 FROM base \
             WHERE description ILIKE '%' || '{q}' || '%' ESCAPE '\\'"
        );
        per_token_matches.push(block);
    }

    // For multi-token, we need all tokens to match. We do this by:
    // 1. Collect field_matches per token
    // 2. INTERSECT on unique_id across tokens
    // For simplicity with multi-token, use a single field_matches that includes
    // all tokens ANDed via INTERSECT.
    if tokens.len() == 1 {
        format!(
            "field_matches AS (\n{}\n)",
            per_token_matches.into_iter().next().unwrap()
        )
    } else {
        // Multi-token: need winner to have matched ALL tokens.
        // Build per-token CTEs then intersect on unique_id.
        let mut token_cte_names = Vec::new();
        let mut ctes = Vec::new();
        for (i, token) in tokens.iter().enumerate() {
            let q = escape_ilike(token);
            let cte_name = format!("token_matches_{i}");
            let cte = format!(
                "{cte_name} AS (\n\
                 SELECT unique_id, 'name' AS matched_field, 1 AS priority FROM base \
                 WHERE name ILIKE '%' || '{q}' || '%' ESCAPE '\\'\n\
                 UNION ALL\n\
                 SELECT b.unique_id, 'column', 2 FROM base b \
                 JOIN dbt.node_columns c USING (unique_id) \
                 WHERE c.column_name ILIKE '%' || '{q}' || '%' ESCAPE '\\'\n\
                 UNION ALL\n\
                 SELECT unique_id, 'tag', 3 FROM base \
                 WHERE tags IS NOT NULL \
                 AND len(list_filter(tags, x -> x ILIKE '%' || '{q}' || '%' ESCAPE '\\')) > 0\n\
                 UNION ALL\n\
                 SELECT unique_id, 'fqn', 4 FROM base \
                 WHERE fqn IS NOT NULL \
                 AND array_to_string(fqn, '.') ILIKE '%' || '{q}' || '%' ESCAPE '\\'\n\
                 UNION ALL\n\
                 SELECT unique_id, 'description', 5 FROM base \
                 WHERE description ILIKE '%' || '{q}' || '%' ESCAPE '\\'\n\
                 )"
            );
            ctes.push(cte);
            token_cte_names.push(cte_name);
        }

        // field_matches = union of all token matches, but only for rows that appear in ALL tokens
        let first_name = &token_cte_names[0];
        let intersect_parts: Vec<String> = token_cte_names
            .iter()
            .map(|n| format!("SELECT unique_id FROM {n}"))
            .collect();
        let valid_ids = intersect_parts.join("\nINTERSECT\n");

        let union_all: Vec<String> = token_cte_names
            .iter()
            .map(|n| format!("SELECT * FROM {n}"))
            .collect();
        let union_body = union_all.join("\nUNION ALL\n");

        let _ = first_name; // suppress warning
        let fm_cte = format!(
            "field_matches AS (\n\
             {union_body}\n\
             )"
        );

        // Build a "valid_unique_ids" CTE that is the intersection
        let valid_cte = format!(
            "valid_unique_ids AS (\n\
             {valid_ids}\n\
             )"
        );

        // Return as part of the larger CTE chain — caller will prepend the per-token CTEs
        // We return a combined string that the caller slots into the WITH clause
        let all_ctes: Vec<String> = ctes
            .into_iter()
            .chain(std::iter::once(valid_cte))
            .chain(std::iter::once(fm_cte))
            .collect();
        all_ctes.join(",\n")
    }
}

/// Concatenate ranking tiers into one fixed-width, lexicographically-sortable key.
///
/// Each tier is `(width, expr)`: the expr is left-padded with `'0'` to `width` chars
/// so byte-wise string ordering matches numeric ordering (`'01' < '02' < … < '10'`).
/// Widening a digit field that can exceed `9` is a one-place change to its `width`.
/// A `width` of `0` appends the expr unpadded — the variable-width tail (raw name).
fn join_fixed_width_sort_key(tiers: &[(usize, &str)]) -> String {
    tiers
        .iter()
        .map(|(width, expr)| match *width {
            // width 0 → variable-width tail (raw name), appended unpadded.
            0 => format!("({expr})"),
            // Left-pad to `width` so e.g. '01' < '02' < … sorts numerically.
            w => format!("LPAD(CAST(({expr}) AS VARCHAR), {w}, '0')"),
        })
        .collect::<Vec<_>>()
        .join("\n          || ")
}

/// Build the composite `cursor_key` SQL expression for search-mode ranking.
///
/// Encodes multiple ranking tiers into a single lexicographically-sortable VARCHAR so
/// the existing single-key cursor machinery (`cursor_where_fragment`) needs no structural
/// change — only the sort expression changes.
///
/// Tier order (strongest → weakest); each tier is left-padded to a declared
/// fixed width by `join_fixed_width_sort_key` so byte-wise string ordering matches
/// numeric ordering. All ranking tiers are single-digit (width 1) today — widening
/// one (e.g. if a digit field can exceed `9`) is a one-place change to its width:
///   0. NULL name sentinel — sorts last (`'9~9999'`, no name suffix).
///   1. Exact name match — `lower(b.name) = lower(q)` → `'0'`/`'1'`.
///   2. Prefix name match — `lower(b.name) LIKE lower(q) || '%'` → `'0'`/`'1'`.
///   3. Field-match priority — `w.match_priority` (1–5: name < column < tag < fqn < desc).
///   4. Resource type — model first, test/other last.
///   5. Modeling layer — marts first, none last.
///   6. Raw name — case-sensitive byte-for-byte tiebreak (width 0, unpadded tail),
///      preserving today's alphabetical order within any tier.
///
/// Note: `unique_id` remains the final tiebreak via the existing cursor uid slot.
///
/// # Divergence from dbt Catalog (codex-api)
/// dbt Catalog (codex-api `catalog.search`) weights resource type (`×1000`) ABOVE
/// match quality (`×100`), so an exact name match is not guaranteed first — a known
/// limitation tracked as a TODO to better rank exact matches (`search.ts:455`). Here we
/// invert that priority: **exact/prefix name match outranks resource type** because
/// when viewing docs for a stateless run, the user-typed name matters more than the
/// type hierarchy. Resource type is a tiebreak among otherwise-equal matches, not a
/// dominant signal.
///
/// # TODO(META-7529 follow-up): parity signals not yet portable
/// - **Full-text / stemming** (dbt Catalog uses `tsvector` / OpenSearch BM25): DuckDB
///   has an FTS extension but it is out of scope; matching stays `ILIKE` substring.
/// - **Popularity** (dbt Catalog: `NTILE(5)` over 30-day query counts): requires
///   query-usage telemetry not present in the parquet index.
/// - **Fuzzy / prefix on non-name fields**: dbt Catalog OpenSearch-only; stays
///   substring-only here.
///
/// A multi-column typed cursor (sort each tier as its own `ORDER BY` column rather
/// than packing them into one padded string) is the structurally cleaner long-term
/// design, but it changes the `(sort_value, unique_id)` cursor contract shared by
/// every paginated endpoint — out of scope here.
fn ranking_cursor_key_expr(q: &str) -> String {
    let q_lit = escape_str(q); // safe for `= 'q'`
    let q_ilike = escape_ilike(q); // safe for `LIKE 'q%'`

    // Staging / Intermediate / Marts conditions come from LAYER_CONDITIONS, which are
    // already `b.`-qualified and path-based (`lower(b.original_file_path) LIKE …`).
    // Order: marts → intermediate → staging → none (best-to-worst, digit 0/1/2/3).
    let (_, marts_cond) = LAYER_CONDITIONS
        .iter()
        .find(|(n, _)| *n == "Marts")
        .unwrap();
    let (_, inter_cond) = LAYER_CONDITIONS
        .iter()
        .find(|(n, _)| *n == "Intermediate")
        .unwrap();
    let (_, staging_cond) = LAYER_CONDITIONS
        .iter()
        .find(|(n, _)| *n == "Staging")
        .unwrap();

    let exact_match = format!("CASE WHEN lower(b.name) = lower('{q_lit}') THEN '0' ELSE '1' END");
    let prefix_match = format!(
        "CASE WHEN lower(b.name) LIKE lower('{q_ilike}') || '%' ESCAPE '\\' THEN '0' ELSE '1' END"
    );
    let resource_rank = "CASE b.resource_type \
           WHEN 'model'          THEN '0' \
           WHEN 'source'         THEN '1' \
           WHEN 'metric'         THEN '2' \
           WHEN 'exposure'       THEN '3' \
           WHEN 'snapshot'       THEN '4' \
           WHEN 'semantic_model' THEN '5' \
           WHEN 'seed'           THEN '6' \
           WHEN 'saved_query'    THEN '7' \
           WHEN 'test'           THEN '8' \
           ELSE '9' END";
    let layer_rank = format!(
        "CASE WHEN {marts_cond} THEN '0' \
              WHEN {inter_cond} THEN '1' \
              WHEN {staging_cond} THEN '2' \
              ELSE '3' END"
    );

    let key = join_fixed_width_sort_key(&[
        (1, &exact_match),
        (1, &prefix_match),
        (1, "w.match_priority"),
        (1, resource_rank),
        (1, &layer_rank),
        (0, "b.name"),
    ]);

    format!("CASE WHEN b.name IS NULL THEN '9~9999' ELSE {key} END")
}

/// Build the full SQL for a search query.
/// Returns `(page_sql, count_sql)`.
struct SearchSql {
    page_sql: String,
    count_sql: String,
}

fn build_search_sql(
    params: &SearchQueryParams,
    requested_types: &BTreeSet<ResourceType>,
    with_freshness: bool,
    with_run_results: bool,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<SearchSql, String> {
    let base_union = build_base_union(requested_types, with_freshness, with_run_results)
        .ok_or_else(|| "no resource types to search".to_string())?;

    // Build base WHERE fragment (package, tag, modeling_layer filters).
    let mut base_where_parts: Vec<String> = Vec::new();

    if let Some(pkg) = params.package.as_deref().filter(|s| !s.is_empty()) {
        base_where_parts.push(package_where(pkg));
    }

    if let Some(tag_raw) = params.tag.as_deref().filter(|s| !s.is_empty()) {
        base_where_parts.push(tag_where(tag_raw));
    }

    if let Some(ml_raw) = params.modeling_layer.as_deref().filter(|s| !s.is_empty()) {
        // Validate modeling_layer values.
        for v in ml_raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if !VALID_MODELING_LAYERS.contains(&v) {
                return Err(format!("invalid modeling_layer value: {v}"));
            }
        }
        let ml_cond = modeling_layer_where(ml_raw);
        base_where_parts.push(format!("({ml_cond})"));
    }

    if let Some(mat) = params.materialization.as_deref().filter(|s| !s.is_empty()) {
        let list = mat
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| format!("'{}'", escape_str(s)))
            .collect::<Vec<_>>()
            .join(", ");
        if !list.is_empty() {
            base_where_parts.push(format!("b.materialized IN ({list})"));
        }
    }

    let base_where = if base_where_parts.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", base_where_parts.join(" AND "))
    };

    let is_search = params
        .q
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);

    let peek = first + 1;

    if is_search {
        let q_str = params.q.as_deref().unwrap().trim();
        let tokens: Vec<String> = q_str.split_whitespace().map(|s| s.to_owned()).collect();

        // Multi-token: build one field_matches block; for multi-token also
        // include a valid_unique_ids CTE to AND-filter.
        let is_multi = tokens.len() > 1;

        let (middle_ctes, field_matches_name, valid_ids_name) = if is_multi {
            // build_field_matches_cte returns multiple CTEs joined by commas
            let ctes_str = build_field_matches_cte(&tokens);
            (
                ctes_str,
                "field_matches".to_owned(),
                Some("valid_unique_ids".to_owned()),
            )
        } else {
            let ctes_str = build_field_matches_cte(&tokens);
            (ctes_str, "field_matches".to_owned(), None)
        };

        // Build the ranking cursor-key expression once; reused in SELECT and WHERE.
        // Using the expression directly in WHERE avoids DuckDB's restriction that
        // SELECT aliases cannot appear in WHERE clauses.
        let cursor_key_expr = ranking_cursor_key_expr(q_str);

        let cursor_cond = if let Some(c) = cursor {
            let frag = cursor_where_fragment(
                &cursor_key_expr,
                "b.unique_id",
                crate::handlers::pagination::SortDir::Asc,
                c.sort_value.as_deref(),
                &c.unique_id,
            );
            format!(" AND {frag}")
        } else {
            String::new()
        };

        // The valid-ids JOIN AND-filters multi-token results. It must reference the
        // left-side alias of the query it lands in: `base b` in page_sql, `winners w`
        // in count_sql (which has no `base`).
        let valid_join = if let Some(ref vname) = valid_ids_name {
            format!("JOIN {vname} vuid ON vuid.unique_id = b.unique_id")
        } else {
            String::new()
        };
        let valid_join_count = if let Some(ref vname) = valid_ids_name {
            format!("JOIN {vname} vuid ON vuid.unique_id = w.unique_id")
        } else {
            String::new()
        };

        let page_sql = format!(
            "WITH base AS (\n\
             SELECT * FROM (\n{base_union}\n) _b\n\
             {base_where}\n\
             ),\n\
             {middle_ctes},\n\
             winners AS (\n\
               SELECT unique_id, arg_min(matched_field, priority) AS matched_field,\n\
                      min(priority) AS match_priority\n\
               FROM {field_matches_name}\n\
               GROUP BY unique_id\n\
             )\n\
             SELECT b.*, w.matched_field, ({cursor_key_expr}) AS cursor_key\n\
             FROM base b\n\
             JOIN winners w ON w.unique_id = b.unique_id\n\
             {valid_join}\n\
             WHERE 1=1{cursor_cond}\n\
             ORDER BY cursor_key ASC, b.unique_id ASC\n\
             LIMIT {peek}"
        );

        let count_sql = format!(
            "WITH base AS (\n\
             SELECT * FROM (\n{base_union}\n) _b\n\
             {base_where}\n\
             ),\n\
             {middle_ctes},\n\
             winners AS (\n\
               SELECT unique_id, arg_min(matched_field, priority) AS matched_field\n\
               FROM {field_matches_name}\n\
               GROUP BY unique_id\n\
             )\n\
             SELECT COUNT(*)\n\
             FROM winners w\n\
             {valid_join_count}"
        );

        Ok(SearchSql {
            page_sql,
            count_sql,
        })
    } else {
        // Browse mode: no field_matches, no winners CTE.
        let cursor_cond = if let Some(c) = cursor {
            let frag = cursor_where_fragment(
                "b.name",
                "b.unique_id",
                crate::handlers::pagination::SortDir::Asc,
                c.sort_value.as_deref(),
                &c.unique_id,
            );
            if base_where.is_empty() {
                format!("WHERE {frag}")
            } else {
                format!("{base_where} AND {frag}")
            }
        } else {
            base_where.clone()
        };

        let count_where = base_where;

        let page_sql = format!(
            "WITH base AS (\n\
             SELECT * FROM (\n{base_union}\n) _b\n\
             )\n\
             SELECT b.*, NULL::VARCHAR AS matched_field\n\
             FROM base b\n\
             {cursor_cond}\n\
             ORDER BY b.name ASC NULLS LAST, b.unique_id ASC\n\
             LIMIT {peek}"
        );

        let count_sql = format!(
            "WITH base AS (\n\
             SELECT * FROM (\n{base_union}\n) _b\n\
             )\n\
             SELECT COUNT(*) FROM base b\n\
             {count_where}"
        );

        Ok(SearchSql {
            page_sql,
            count_sql,
        })
    }
}

// ---------------------------------------------------------------------------
// Row extraction
// ---------------------------------------------------------------------------

/// Extract a `varchar[]` column as a `Vec<String>` at row `i`.
fn extract_list_at(batch: &RecordBatch, col_name: &str, row: usize) -> Option<Vec<String>> {
    let col = batch.column_by_name(col_name)?;
    if col.is_null(row) {
        return None;
    }
    let list = col.as_list_opt::<i32>()?;
    let values = list.value(row);
    let strings = values.as_any().downcast_ref::<StringArray>()?;
    let out: Vec<String> = (0..strings.len())
        .filter(|&i| !strings.is_null(i))
        .map(|i| strings.value(i).to_owned())
        .collect();
    Some(out)
}

/// Extract a nullable bool column at row `i`.
fn extract_bool_opt(batch: &RecordBatch, col_name: &str, row: usize) -> Option<bool> {
    use arrow_array::BooleanArray;
    let col = batch.column_by_name(col_name)?;
    let bool_arr = col.as_any().downcast_ref::<BooleanArray>()?;
    if bool_arr.is_null(row) {
        None
    } else {
        Some(bool_arr.value(row))
    }
}

/// Convert Arrow batches into `SearchEdge` rows (without highlight — that is added later).
/// Returns `(SearchEdge, Option<cursor_key>)` pairs.
///
/// The `cursor_key` column is only present in search-mode SQL (where the composite
/// ranking expression is projected). Browse-mode SQL and mock-backend tests never emit
/// it, so its absence is treated as `None` — the handler falls back to `e.hit.name`.
fn batches_to_search_rows(batches: &[RecordBatch]) -> Vec<(SearchEdge, Option<String>)> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let resource_type = str_col(batch, "resource_type");
        let package_name = str_col(batch, "package_name");
        let description = str_col(batch, "description");
        let materialized = str_col(batch, "materialized");
        let access_level = str_col(batch, "access_level");
        let source_name = str_col(batch, "source_name");
        let test_type = str_col(batch, "test_type");
        let exposure_type = str_col(batch, "exposure_type");
        let executed_at = str_col(batch, "executed_at");
        let matched_field = str_col(batch, "matched_field");
        // Present in search mode, absent in browse mode and mock tests.
        let cursor_key_col = batch.column_by_name("cursor_key");

        let opt = |col: &StringArray, i: usize| -> Option<String> {
            if col.is_null(i) {
                None
            } else {
                Some(col.value(i).to_owned())
            }
        };

        for i in 0..batch.num_rows() {
            let uid = unique_id.value(i).to_owned();
            let rt = resource_type.value(i).to_owned();
            let nm = opt(name, i);
            let pkg = opt(package_name, i);
            let desc = opt(description, i);
            let fqn_val = extract_list_at(batch, "fqn", i);
            let tags_val = extract_list_at(batch, "tags", i);
            let freshness_checked = extract_bool_opt(batch, "freshness_checked", i);

            let hit = SearchHit {
                unique_id: uid.clone(),
                resource_type: rt.clone(),
                name: nm,
                fqn: fqn_val,
                description: desc,
                tags: tags_val,
                package_name: pkg,
                materialized: opt(materialized, i),
                access_level: opt(access_level, i),
                source_name: opt(source_name, i),
                freshness_checked,
                test_type: opt(test_type, i),
                exposure_type: opt(exposure_type, i),
                executed_at: opt(executed_at, i),
            };

            let mf = opt(matched_field, i);
            let edge = SearchEdge {
                matched_field: mf,
                highlight: None, // filled in after
                hit,
            };

            // Read the composite cursor key when present (search mode only).
            let ck: Option<String> = cursor_key_col.and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    col.as_string_opt::<i32>().map(|a| a.value(i).to_owned())
                }
            });

            rows.push((edge, ck));
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Highlight generation
// ---------------------------------------------------------------------------

/// Wrap every case-insensitive occurrence of each token in `<b>...</b>`.
/// Preserves original casing from `field_value`.
pub(crate) fn build_highlight(field_value: &str, tokens: &[&str]) -> String {
    let mut result = field_value.to_owned();
    for token in tokens {
        if token.is_empty() {
            continue;
        }
        let lower_result = result.to_lowercase();
        let lower_token = token.to_lowercase();
        let mut out = String::with_capacity(result.len() + 32);
        let mut pos = 0;
        while let Some(found) = lower_result[pos..].find(lower_token.as_str()) {
            let abs_start = pos + found;
            let abs_end = abs_start + token.len();
            out.push_str(&result[pos..abs_start]);
            out.push_str("<b>");
            out.push_str(&result[abs_start..abs_end]);
            out.push_str("</b>");
            pos = abs_end;
        }
        out.push_str(&result[pos..]);
        result = out;
    }
    result
}

/// Build description highlight with 80-char window around first match.
fn build_description_highlight(desc: &str, tokens: &[&str]) -> String {
    // Find the first match position
    let lower = desc.to_lowercase();
    let first_pos = tokens
        .iter()
        .filter_map(|t| {
            if t.is_empty() {
                return None;
            }
            lower.find(&t.to_lowercase())
        })
        .min();

    let windowed = if let Some(pos) = first_pos {
        // Character-based window of 80 chars centered around match
        let chars: Vec<char> = desc.chars().collect();
        // Find char position of byte position `pos`
        let char_pos = desc[..pos].chars().count();
        let window_half = 40;
        let start_char = char_pos.saturating_sub(window_half);
        let end_char = (char_pos + window_half + 10).min(chars.len());

        let prefix = if start_char > 0 { "..." } else { "" };
        let suffix = if end_char < chars.len() { "..." } else { "" };
        let slice: String = chars[start_char..end_char].iter().collect();
        format!("{prefix}{slice}{suffix}")
    } else {
        desc.to_owned()
    };

    build_highlight(&windowed, tokens)
}

/// Build tag highlight — the alphabetically-first matching tag, with matched
/// substring wrapped in `<b>`.
fn build_tag_highlight(tags: &[String], tokens: &[&str]) -> Option<String> {
    let mut matching_tags: Vec<&String> = tags
        .iter()
        .filter(|tag| {
            let lower_tag = tag.to_lowercase();
            tokens.iter().all(|t| lower_tag.contains(&t.to_lowercase()))
        })
        .collect();
    matching_tags.sort();
    matching_tags
        .first()
        .map(|tag| build_highlight(tag, tokens))
}

/// Build column highlight — comma-joined matching column names with `<b>` wrapping.
fn build_column_highlight(
    backend: &dyn crate::providers::Backend,
    unique_id: &str,
    tokens: &[&str],
) -> Option<String> {
    if tokens.is_empty() {
        return None;
    }
    let conditions: Vec<String> = tokens
        .iter()
        .map(|t| {
            let escaped = escape_ilike(t);
            format!("column_name ILIKE '%' || '{escaped}' || '%' ESCAPE '\\'")
        })
        .collect();
    let condition = conditions.join(" AND ");
    let uid_escaped = escape_str(unique_id);
    let sql = format!(
        "SELECT DISTINCT column_name FROM dbt.node_columns \
         WHERE unique_id = '{uid_escaped}' AND ({condition}) \
         ORDER BY column_name"
    );
    let batches = backend.query_arrow(&sql).ok()?;
    let mut col_names: Vec<String> = Vec::new();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = str_col(batch, "column_name");
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                col_names.push(col.value(i).to_owned());
            }
        }
    }
    if col_names.is_empty() {
        return None;
    }
    let highlighted: Vec<String> = col_names
        .iter()
        .map(|c| build_highlight(c, tokens))
        .collect();
    Some(highlighted.join(", "))
}

/// Generate highlight for a search edge based on its `matched_field`.
fn generate_highlight(
    edge: &SearchEdge,
    tokens: &[&str],
    backend: &dyn crate::providers::Backend,
) -> Option<String> {
    match edge.matched_field.as_deref() {
        Some("name") => None, // always null for name matches
        Some("description") => edge
            .hit
            .description
            .as_deref()
            .map(|desc| build_description_highlight(desc, tokens)),
        Some("tag") => edge
            .hit
            .tags
            .as_ref()
            .and_then(|tags| build_tag_highlight(tags, tokens)),
        Some("fqn") => edge.hit.fqn.as_ref().map(|fqn| {
            let dotted = fqn.join(".");
            build_highlight(&dotted, tokens)
        }),
        Some("column") => build_column_highlight(backend, &edge.hit.unique_id, tokens),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/search` — cross-resource project search (ADR-8).
pub async fn search(
    State(state): State<SharedState>,
    Query(params): Query<SearchQueryParams>,
) -> Response {
    // Validate query length (chars, not bytes).
    if let Some(q) = params.q.as_deref() {
        if q.chars().count() > 1024 {
            return bad_request_coded("query_too_long", "query exceeds 1024 character limit");
        }
    }

    // Clamp page size — search has its own constants, NOT clamp_first.
    let first = params
        .first
        .unwrap_or(SEARCH_DEFAULT_PAGE_SIZE)
        .clamp(1, SEARCH_MAX_PAGE_SIZE);

    // Decode cursor.
    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(_) => return bad_request_coded("invalid_cursor", "cursor is invalid or expired"),
        },
        None => None,
    };

    // Parse type filter.
    let mut requested_types: BTreeSet<ResourceType> =
        match params.type_filter.as_deref().filter(|s| !s.is_empty()) {
            Some(raw) => match parse_csv_resource_types(raw) {
                Ok(types) => types,
                Err(msg) => return bad_request_coded("invalid_type", &msg),
            },
            None => ResourceType::all(),
        };

    // modeling_layer is model-only — intersect.
    if params
        .modeling_layer
        .as_deref()
        .filter(|s| !s.is_empty())
        .is_some()
    {
        // Validate first.
        if let Some(ml_raw) = params.modeling_layer.as_deref() {
            for v in ml_raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if !VALID_MODELING_LAYERS.contains(&v) {
                    let msg = format!(
                        "unknown modeling_layer value: {v}; allowed: Staging, Intermediate, Marts"
                    );
                    return bad_request_coded("invalid_modeling_layer", &msg);
                }
            }
        }
        requested_types.retain(|t| *t == ResourceType::Model);
    }

    // materialization is model-only — intersect requested types to {Model}.
    if params
        .materialization
        .as_deref()
        .filter(|s| !s.is_empty())
        .is_some()
    {
        requested_types.retain(|t| *t == ResourceType::Model);
    }

    // First call validates params; subsequent calls use .expect() per models.rs pattern.
    let sql_full = match build_search_sql(
        &params,
        &requested_types,
        true,
        true,
        first,
        cursor.as_ref(),
    ) {
        Ok(s) => s,
        Err(msg) => return bad_request(&msg),
    };
    let sql_no_fresh = build_search_sql(
        &params,
        &requested_types,
        false,
        true,
        first,
        cursor.as_ref(),
    )
    .expect("params already validated");
    let sql_no_rr = build_search_sql(
        &params,
        &requested_types,
        false,
        false,
        first,
        cursor.as_ref(),
    )
    .expect("params already validated");

    let is_search = params
        .q
        .as_deref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);

    let tokens: Vec<String> = if is_search {
        params
            .q
            .as_deref()
            .unwrap()
            .split_whitespace()
            .map(|s| s.to_owned())
            .collect()
    } else {
        vec![]
    };

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let (total, batches) = if let Some(count_str) = backend.query_scalar(&sql_full.count_sql) {
            let total = count_str
                .parse::<u64>()
                .map_err(|e| format!("could not parse search count: {e}"))?;
            let batches = backend
                .query_arrow(&sql_full.page_sql)
                .map_err(|e| e.to_string())?;
            (total, batches)
        } else if let Some(count_str) = backend.query_scalar(&sql_no_fresh.count_sql) {
            // freshness view absent but run_results present — keep executed_at.
            let total = count_str
                .parse::<u64>()
                .map_err(|e| format!("could not parse search count: {e}"))?;
            let batches = backend
                .query_arrow(&sql_no_fresh.page_sql)
                .map_err(|e| e.to_string())?;
            (total, batches)
        } else {
            // run_results absent — retry without both optional joins.
            let total = backend
                .query_scalar(&sql_no_rr.count_sql)
                .ok_or_else(|| "count query returned no rows".to_string())?
                .parse::<u64>()
                .map_err(|e| format!("could not parse search count: {e}"))?;
            let batches = backend
                .query_arrow(&sql_no_rr.page_sql)
                .map_err(|e| e.to_string())?;
            (total, batches)
        };
        // Generate highlights inline (column highlight needs backend).
        let mut raw_rows = batches_to_search_rows(&batches);
        if is_search {
            let token_refs: Vec<&str> = tokens.iter().map(|s| s.as_str()).collect();
            for (edge, _uid) in &mut raw_rows {
                edge.highlight = generate_highlight(edge, &token_refs, backend.as_ref());
            }
        }
        Ok((total, raw_rows))
    })
    .await;

    let (total_count, raw_rows) = match result {
        Ok(Ok((total, rows))) => (total, rows),
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut pairs: Vec<(SearchEdge, Option<String>)> = raw_rows;

    let has_next_page = pairs.len() as u32 > first;
    if has_next_page {
        pairs.truncate(first as usize);
    }

    // Build cursors from the (edge, cursor_key) pairs BEFORE converting to plain edges.
    // The cursor's sort_value is the composite cursor_key when present (search mode),
    // or the raw name when absent (browse mode / mock-backed tests).
    let cursor_from_pair = |(e, ck): &(SearchEdge, Option<String>)| {
        let sort_value = ck.clone().or_else(|| e.hit.name.clone());
        Cursor {
            sort_value,
            unique_id: e.hit.unique_id.clone(),
        }
        .encode()
    };

    let start_cursor = pairs.first().map(&cursor_from_pair);
    let end_cursor = if has_next_page {
        pairs.last().map(&cursor_from_pair)
    } else {
        None
    };

    let edges: Vec<SearchEdge> = pairs.into_iter().map(|(e, _)| e).collect();

    Json(SearchResponse {
        data: edges,
        page_info: PageInfo {
            total_count,
            start_cursor,
            end_cursor,
            has_next_page,
        },
    })
    .into_response()
}

// ---------------------------------------------------------------------------
// GET /api/v1/search/facets
// ---------------------------------------------------------------------------

/// A single facet value with a project-wide count of matching resources.
#[derive(Serialize)]
pub struct SearchFacetValue {
    pub value: String,
    pub count: u64,
}

impl SearchFacetValue {
    fn new(value: impl Into<String>, count: u64) -> Self {
        Self {
            value: value.into(),
            count,
        }
    }
}

#[derive(Serialize)]
pub struct SearchFacetsResponse {
    /// Distinct model access levels with resource counts.
    pub accesses: Vec<SearchFacetValue>,
    /// Distinct modeling layers (Staging/Intermediate/Marts) with model counts.
    pub modeling_layers: Vec<SearchFacetValue>,
    /// Distinct model materialization strategies with model counts.
    pub materialization_types: Vec<SearchFacetValue>,
    /// Distinct tag values across all taggable resource types with resource counts.
    pub tags: Vec<SearchFacetValue>,
    /// Distinct package names across all resource types with resource counts.
    pub packages: Vec<SearchFacetValue>,
}

// SQL comment prefix `/* facets:* */` lets the mock backend in tests route
// each query to the correct fixture batch without executing real SQL.
// All queries return (value VARCHAR, cnt BIGINT).

const SEARCH_FACETS_ACCESSES_SQL: &str = "\
/* facets:access */ \
SELECT access_level AS value, COUNT(*) AS cnt \
FROM dbt.nodes \
WHERE resource_type = 'model' AND access_level IS NOT NULL \
GROUP BY access_level \
ORDER BY value";

/// Builds the modeling-layers facets SQL dynamically from LAYER_CONDITIONS so
/// the query stays in sync with the filter validation logic above.
fn search_facets_modeling_layers_sql() -> String {
    let case_branches: Vec<String> = LAYER_CONDITIONS
        .iter()
        .map(|(name, cond)| {
            // LAYER_CONDITIONS use `b.` alias (valid in search UNION CTEs).
            // Strip the alias for this standalone query against dbt.nodes directly.
            let cond_unaliased = cond.replace("b.", "");
            format!("WHEN {cond_unaliased} THEN '{name}'")
        })
        .collect();
    let case_expr = format!("CASE {} ELSE NULL END", case_branches.join(" "));
    format!(
        "/* facets:layers */ \
         SELECT {case_expr} AS value, COUNT(*) AS cnt \
         FROM dbt.nodes \
         WHERE resource_type = 'model' \
         GROUP BY value \
         HAVING value IS NOT NULL \
         ORDER BY value"
    )
}

const SEARCH_FACETS_MATERIALIZATIONS_SQL: &str = "\
/* facets:mat */ \
SELECT materialized AS value, COUNT(*) AS cnt \
FROM dbt.nodes \
WHERE resource_type = 'model' AND materialized IS NOT NULL \
GROUP BY materialized \
ORDER BY value";

const SEARCH_FACETS_TAGS_SQL: &str = "\
/* facets:tags */ \
SELECT t.value, COUNT(*) AS cnt FROM (\
  SELECT unnest(tags) AS value FROM dbt.nodes WHERE tags IS NOT NULL \
  UNION ALL \
  SELECT unnest(tags) AS value FROM dbt.exposures WHERE tags IS NOT NULL \
  UNION ALL \
  SELECT unnest(tags) AS value FROM dbt.metrics WHERE tags IS NOT NULL \
  UNION ALL \
  SELECT unnest(tags) AS value FROM dbt.saved_queries WHERE tags IS NOT NULL\
) t \
GROUP BY t.value \
ORDER BY t.value";

const SEARCH_FACETS_PACKAGES_SQL: &str = "\
/* facets:pkg */ \
SELECT pkg AS value, COUNT(*) AS cnt FROM (\
  SELECT package_name AS pkg FROM dbt.nodes WHERE package_name IS NOT NULL \
  UNION ALL \
  SELECT package_name AS pkg FROM dbt.macros WHERE package_name IS NOT NULL \
  UNION ALL \
  SELECT package_name AS pkg FROM dbt.exposures WHERE package_name IS NOT NULL\
) t \
GROUP BY pkg \
ORDER BY pkg";

/// Access level values in display order (matches dbt's `access` config).
const FACET_ACCESS_LEVELS: &[&str] = &["private", "protected", "public"];

/// Standard dbt materialization strategies in alphabetical order (dbt 1.13).
/// Ref: https://docs.getdbt.com/docs/build/materializations
/// Custom strategies (e.g. `iceberg`, `python`) appear after these in results
/// but are not guaranteed to be present with `count: 0`.
const FACET_STANDARD_MATERIALIZATIONS: &[&str] = &[
    "table",
    "view",
    "incremental",
    "ephemeral",
    "materialized_view",
];

/// Ensure every value in `defaults` appears in the output, with `count: 0`
/// if absent from `sql_results`. Defaults are emitted in `defaults` order;
/// any extra values from SQL (custom types) are appended alphabetically.
fn with_defaults(sql_results: Vec<SearchFacetValue>, defaults: &[&str]) -> Vec<SearchFacetValue> {
    use std::collections::HashMap;
    let mut map: HashMap<String, u64> = sql_results
        .into_iter()
        .map(|f| (f.value, f.count))
        .collect();

    // Emit defaults in specified order, count 0 if absent.
    let mut out: Vec<SearchFacetValue> = defaults
        .iter()
        .map(|d| SearchFacetValue::new(*d, map.remove(*d).unwrap_or(0)))
        .collect();

    // Append remaining (custom) values alphabetically.
    let mut extras: Vec<SearchFacetValue> = map
        .into_iter()
        .map(|(v, c)| SearchFacetValue::new(v, c))
        .collect();
    extras.sort_by(|a, b| a.value.cmp(&b.value));
    out.extend(extras);
    out
}

/// Extract `(value, cnt)` pairs from a two-column Arrow result.
///
/// All search facets SQL queries return `value VARCHAR` and `cnt BIGINT`.
fn batches_to_search_facet_values(batches: &[RecordBatch]) -> Vec<SearchFacetValue> {
    use arrow_array::Int64Array;
    let mut out = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let val_col = str_col(batch, "value");
        let cnt_col = batch
            .column_by_name("cnt")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        for i in 0..batch.num_rows() {
            if !val_col.is_null(i) {
                let count = cnt_col
                    .map(|c| if c.is_null(i) { 0i64 } else { c.value(i) })
                    .unwrap_or(0) as u64;
                out.push(SearchFacetValue::new(val_col.value(i), count));
            }
        }
    }
    out
}

/// `GET /api/v1/search/facets` — project-wide filter dimensions with counts.
///
/// Returns distinct values and resource counts for all search filter dimensions.
/// Counts are project-wide (not query-dependent). No query params; no cursor.
pub async fn search_facets(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let layers_sql = search_facets_modeling_layers_sql();
    let result = tokio::task::spawn_blocking(move || -> Result<SearchFacetsResponse, String> {
        let access_batches = backend
            .query_arrow(SEARCH_FACETS_ACCESSES_SQL)
            .map_err(|e| e.to_string())?;
        let layers_batches = backend
            .query_arrow(&layers_sql)
            .map_err(|e| e.to_string())?;
        let mat_batches = backend
            .query_arrow(SEARCH_FACETS_MATERIALIZATIONS_SQL)
            .map_err(|e| e.to_string())?;
        let tags_batches = backend
            .query_arrow(SEARCH_FACETS_TAGS_SQL)
            .map_err(|e| e.to_string())?;
        let pkg_batches = backend
            .query_arrow(SEARCH_FACETS_PACKAGES_SQL)
            .map_err(|e| e.to_string())?;
        // Finite-enum dimensions use `with_defaults` so every known value
        // appears with count 0 even if no project resources use it.
        // materialization_types also ensures the four standard strategies
        // appear; custom strategies are appended when present.
        Ok(SearchFacetsResponse {
            accesses: with_defaults(
                batches_to_search_facet_values(&access_batches),
                FACET_ACCESS_LEVELS,
            ),
            modeling_layers: with_defaults(
                batches_to_search_facet_values(&layers_batches),
                &LAYER_CONDITIONS.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            ),
            materialization_types: with_defaults(
                batches_to_search_facet_values(&mat_batches),
                FACET_STANDARD_MATERIALIZATIONS,
            ),
            tags: batches_to_search_facet_values(&tags_batches),
            packages: batches_to_search_facet_values(&pkg_batches),
        })
    })
    .await;

    match result {
        Ok(Ok(resp)) => Json(resp).into_response(),
        Ok(Err(err)) => internal_error(err),
        Err(err) => internal_error(err.to_string()),
    }
}

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;
