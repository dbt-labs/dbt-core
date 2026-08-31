//! Project quality checks — runs user-authored SQL checks against the dbt index.
//!
//! Checks are `*.sql` files under `<project_root>/checks/`. Each file is a
//! query against parse-safe `dbt.*` views (via `info_schema()`). A query
//! returning zero rows passes; any returned rows are violations.
//!
//! Discovery and evaluation live here; `check_index_adapter` executes the SQL
//! against a published parse index.

use std::collections::BTreeSet;
use std::path::Path;

use dbt_index_core::ingest::metadata_to_parquet::index_is_current;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Severity of a check. `Error` fails the run (non-zero exit); `Warn` reports
/// the violation but does not fail the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Severity {
    #[default]
    Error,
    Warn,
}

/// Which output column(s) a node `--select` scopes a check by.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SelectionFilter {
    /// Default: filter on `unique_id` if the check outputs that column,
    /// otherwise do not scope (whole project).
    #[default]
    Auto,
    /// Never scope — always the whole project (e.g. aggregate checks).
    None,
    /// Scope on these output columns: keep a row if the node id in ANY of them
    /// is in the selection. Each named column must exist in the check's output
    /// (else the check errors).
    Columns(Vec<String>),
}

/// The node set a check's violation rows should be scoped to, or `None` for no scoping.
///
/// One definition, used by the single parse-time runner. Earlier designs ran checks from two
/// places and diverged over scoping once, and the failure mode is a silent vacuous pass rather
/// than an error, so the rule lives in one place deliberately.
///
/// Returns `None` unless a selector was actually given. Scoping against the *default* selection is
/// wrong on both counts: it isn't a statement of intent, and it doesn't cover every node kind a check
/// may legitimately report on — `groups` and `macros` are absent from `Nodes::materializable_keys`,
/// so a "every group has an owner" check would have every violation filtered away.
///
/// Check ids are kept in the set: a check is a node in `dbt.nodes`/`dbt.checks` like any other, so
/// `--select some_check_name` must narrow a check-about-checks (reading `dbt.checks`) to that
/// check's own row, exactly like selecting a model narrows a check reading `dbt.models`. This
/// governs scoping only — which checks *run* is decided separately by `check_names`/`named` at the
/// call site.
pub fn scope_for_selection<'a>(
    selection_active: bool,
    selected_nodes: impl IntoIterator<Item = &'a String>,
) -> Option<BTreeSet<String>> {
    if !selection_active {
        return None;
    }
    Some(selected_nodes.into_iter().cloned().collect())
}

/// Whether a zero-row result means "nothing was examined" rather than "nothing is wrong".
///
/// A node-scoped check evaluated against an empty scope returns zero rows because there was nothing
/// in scope. Reporting that as a pass would claim a validation that never ran, which is the one
/// outcome a check must never produce.
///
/// Imprecise for [`SelectionFilter::Auto`]: `Auto` only scopes when the output actually carries a
/// node column, and that isn't known until the rows come back, so an aggregate check under `Auto` is
/// treated as node-scoped here. Erring toward "skipped" is the safer direction — it under-claims
/// rather than over-claims.
pub fn zero_rows_is_vacuous(scope: Option<&BTreeSet<String>>, filter: &SelectionFilter) -> bool {
    matches!(scope, Some(s) if s.is_empty()) && !matches!(filter, SelectionFilter::None)
}

/// Map a check node's resolved `selection_filter_on` config onto a [`SelectionFilter`].
///
/// One definition for the single execution path; two paths previously disagreed
/// about what a check's rows are scoped by.
pub fn selection_filter_for(
    selection_filter_on: Option<&dbt_schemas::schemas::project::SelectionFilterOn>,
) -> SelectionFilter {
    use dbt_schemas::schemas::project::SelectionFilterOn;
    match selection_filter_on {
        None => SelectionFilter::Auto,
        Some(SelectionFilterOn::One(s)) if s.eq_ignore_ascii_case("none") => SelectionFilter::None,
        Some(SelectionFilterOn::One(s)) => SelectionFilter::Columns(vec![s.clone()]),
        Some(SelectionFilterOn::Many(v)) => SelectionFilter::Columns(v.clone()),
    }
}

// ---------------------------------------------------------------------------
// Result-batch evaluation
// ---------------------------------------------------------------------------

/// Why a check could not read the index, in user-facing terms. `None` means it can.
///
/// Checks are **pure readers**: they never build or refresh the index. An earlier version had
/// each check catch the index up itself, which went wrong three ways: concurrent checks rewrote
/// the same parquet files while siblings had them open (a non-deterministic `IO error: No such
/// file or directory`), `target/index/` appeared even when the invocation was never asked to
/// write one, and what it left there lacked the `.fusion_state.json` / `.artifact_meta.json`
/// bookkeeping of a published index — so anything reading `target/index` afterwards would treat
/// a half-built directory as real.
pub fn index_unavailable_reason(metadata_dir: &Path, index_dir: &Path) -> Option<String> {
    if index_is_current(metadata_dir, index_dir) {
        return None;
    }
    Some(format!(
        "no current metadata index at {} — checks read the index but never build it; \
         run with `--write-index`, or use `dbt check`",
        index_dir.display()
    ))
}

/// Applied by the parse-time runner before any check executes. Guarding only one of two paths was
/// how a single invocation ended up skipping a parse-tier check for "no current metadata index"
/// while reporting a confident verdict from a compile-tier check against that same index.
/// Count violation rows in a check's result batches and build a `col=value, …`
/// preview (up to `max_rows`).
///
/// When `selected` is `Some`, rows are scoped to that node set according to `filter`, so a
/// project-wide check evaluates only the selected nodes. Shared by the standalone `dbt check`
/// parse-time runner; kept in one place so "what counts as a violation" has a single answer.
pub fn evaluate_batches(
    batches: &[arrow::record_batch::RecordBatch],
    filter: &SelectionFilter,
    selected: Option<&BTreeSet<String>>,
    max_rows: usize,
) -> Result<(u64, Vec<String>), String> {
    use arrow::util::display::ArrayFormatter;
    use dbt_index_core::format::FMT_OPTS;
    let mut count: u64 = 0;
    let mut preview: Vec<String> = Vec::new();
    for batch in batches {
        let Ok(formatters) = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &FMT_OPTS))
            .collect::<Result<Vec<_>, _>>()
        else {
            continue;
        };
        let schema = batch.schema();
        // Drop the redundant `name` column from previews when `unique_id` is
        // present — the id already identifies the node. Drop `message` always:
        // check SQL often aliases a prose column there, but the log preview is
        // the row's identity, not a second copy of that comment.
        let drop_name = schema.index_of("unique_id").is_ok();
        // Output column indices to match against the selection. Empty => no
        // filtering for this batch (count every row).
        let filter_idxs: Vec<usize> = match (selected, filter) {
            (None, _) | (Some(_), SelectionFilter::None) => Vec::new(),
            (Some(_), SelectionFilter::Auto) => {
                schema.index_of("unique_id").ok().into_iter().collect()
            }
            (Some(_), SelectionFilter::Columns(cols)) => {
                let mut idxs = Vec::with_capacity(cols.len());
                for c in cols {
                    match schema.index_of(c) {
                        Ok(i) => idxs.push(i),
                        Err(_) => {
                            return Err(format!(
                                "selection_filter_on: column '{c}' is not in the check's output columns"
                            ));
                        }
                    }
                }
                idxs
            }
        };
        for row in 0..batch.num_rows() {
            if let Some(sel) = selected
                && !filter_idxs.is_empty()
                && !filter_idxs
                    .iter()
                    .any(|&i| sel.contains(&formatters[i].value(row).to_string()))
            {
                continue;
            }
            count += 1;
            if preview.len() < max_rows {
                let cells: Vec<String> = formatters
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| {
                        let n = schema.field(*i).name();
                        n != "message" && !(drop_name && n == "name")
                    })
                    .map(|(i, f)| format!("{}={}", schema.field(i).name(), f.value(row)))
                    .collect();
                if !cells.is_empty() {
                    preview.push(cells.join(", "));
                }
            }
        }
    }
    Ok((count, preview))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── scoping ──────────────────────────────────────────────────────────────────

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// No selector means no scoping at all — *not* scoping against the default selection.
    ///
    /// The default selection is not a statement of intent, and `Nodes::materializable_keys` omits
    /// `groups` and `macros`, so scoping against it would filter away every violation from a check
    /// keyed on those (e.g. "every group has an owner") and report a vacuous pass.
    #[test]
    fn scope_is_none_without_a_selector() {
        let selected = ids(&["model.p.a", "group.p.analytics"]);
        assert_eq!(scope_for_selection(false, selected.iter()), None);
    }

    /// With a selector, scope to the selected nodes, checks included — a check is a node in
    /// `dbt.nodes`/`dbt.checks` like any other, so `--select some_check_name` must narrow a
    /// check-about-checks (reading `dbt.checks`) to that check's own row, the same way selecting a
    /// model narrows a check reading `dbt.models`. Which checks *run* is governed separately by
    /// `check_names`/`named` at the call site — this only ever scopes rows.
    #[test]
    fn scope_with_a_selector_includes_check_ids() {
        let selected = ids(&["model.p.a", "check.p.some_check", "seed.p.s"]);
        let scope = scope_for_selection(true, selected.iter()).expect("selector active");
        assert_eq!(
            scope,
            BTreeSet::from([
                "model.p.a".to_string(),
                "check.p.some_check".to_string(),
                "seed.p.s".to_string()
            ])
        );
    }

    /// A selector matching only checks scopes to exactly those checks — useful on its own now that
    /// a check can report on other checks, and still not a pass if nothing else matches: a per-node
    /// check reading `dbt.models` finds nothing in an all-checks scope and reports skipped, not pass.
    #[test]
    fn scope_can_contain_only_check_ids() {
        let selected = ids(&["check.p.one", "check.p.two"]);
        let scope = scope_for_selection(true, selected.iter()).expect("selector active");
        assert_eq!(
            scope,
            BTreeSet::from(["check.p.one".to_string(), "check.p.two".to_string()])
        );
    }

    /// Zero rows against an empty scope means "nothing was examined", not "nothing is wrong".
    #[test]
    fn zero_rows_is_vacuous_only_when_scoped_and_empty() {
        let empty = BTreeSet::new();
        let non_empty = BTreeSet::from(["model.p.a".to_string()]);

        // Node-scoped check, empty scope -> vacuous.
        assert!(zero_rows_is_vacuous(Some(&empty), &SelectionFilter::Auto));
        assert!(zero_rows_is_vacuous(
            Some(&empty),
            &SelectionFilter::Columns(vec!["unique_id".to_string()])
        ));

        // Something was in scope -> a genuine pass.
        assert!(!zero_rows_is_vacuous(
            Some(&non_empty),
            &SelectionFilter::Auto
        ));

        // No selector at all -> whole project was examined, so a genuine pass.
        assert!(!zero_rows_is_vacuous(None, &SelectionFilter::Auto));

        // `None` filter opts out of scoping entirely, so an empty scope is irrelevant to it.
        assert!(!zero_rows_is_vacuous(Some(&empty), &SelectionFilter::None));
    }

    fn preview_batch(cols: &[(&str, &[&str])]) -> arrow::record_batch::RecordBatch {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
            .collect();
        let arrays: Vec<Arc<dyn arrow::array::Array>> = cols
            .iter()
            .map(|(_, vals)| {
                Arc::new(StringArray::from(
                    vals.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )) as _
            })
            .collect();
        arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn preview_omits_name_and_message_when_unique_id_is_present() {
        let batch = preview_batch(&[
            ("unique_id", &["model.p.a"]),
            ("name", &["a"]),
            ("column_name", &["id"]),
            ("message", &["column has no description"]),
        ]);
        let (count, preview) = evaluate_batches(&[batch], &SelectionFilter::None, None, 5).unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            preview,
            vec!["unique_id=model.p.a, column_name=id".to_string()]
        );
    }
}

// ---------------------------------------------------------------------------
// Parse-time execution
// ---------------------------------------------------------------------------

/// What running the project's parse-time checks established.
#[derive(Debug, Default)]
pub struct CheckRunOutcome {
    /// One row per check, for `run_results.json` — which is what `dbt retry` reads.
    pub results: Vec<CheckResult>,
    /// Checks that failed at `error` severity. Non-empty means the invocation must not proceed.
    pub failed: usize,
}

/// A single check's verdict, in the vocabulary `run_results` uses for assertions.
#[derive(Debug)]
pub struct CheckResult {
    pub unique_id: String,
    pub name: String,
    /// `pass` | `fail` | `warn` | `error` | `skipped`.
    pub status: &'static str,
    pub message: Option<String>,
    pub violations: Option<u64>,
}

impl CheckResult {
    /// A check that could not be evaluated at all — the index was missing/stale, or the check has
    /// no rendered SQL to run. Never `pass`/`fail`/`warn`: we do not know what the check would have
    /// found, so reporting anything but `error` would be a guess.
    fn error(unique_id: String, name: String, message: String) -> Self {
        Self {
            unique_id,
            name,
            status: "error",
            message: Some(message),
            violations: None,
        }
    }
}

/// Run every check against the index, before anything downstream is built.
///
/// This is the *only* place checks execute. `dbt check` and `dbt build` both call it, so the two
/// cannot drift — earlier designs ran them from two places and silently disagreed about scoping and
/// about what counted as a vacuous pass.
///
/// Placement is the whole design: parse has finished, the parse layer has been published to the
/// index, and the task graph has not been built. So a failing check needs no graph edges to stop
/// anything — the caller simply does not proceed. That removes the gating apparatus entirely, along
/// with its sharpest edge: a node configured `static_analysis: off` has no analyze task, so gating
/// only the analyze heads used to let it materialize straight through a failing check.
///
/// `selection` scopes each check's *rows* (by `unique_id`, or the columns named in `selection_filter_on`); it
/// never decides whether a check runs. A selector that matches nothing therefore yields `skipped`
/// rather than `pass`, because a green result must not stand in for a check that examined nothing.
pub fn run_parse_time_checks(
    checks: &[std::sync::Arc<dbt_schemas::schemas::DbtCheck>],
    index_dir: &Path,
    metadata_dir: &Path,
    selection: Option<&BTreeSet<String>>,
    max_preview_rows: usize,
) -> CheckRunOutcome {
    let mut outcome = CheckRunOutcome::default();
    if checks.is_empty() {
        return outcome;
    }

    // Checks read the index and never build it. If it does not reflect the metadata this invocation
    // just wrote, there is nothing trustworthy to query — and every check must say so rather than
    // return zero rows, which would read as a pass.
    if let Some(reason) = index_unavailable_reason(metadata_dir, index_dir) {
        for c in checks {
            outcome.failed += 1;
            outcome.results.push(CheckResult::error(
                c.__common_attr__.unique_id.clone(),
                c.__common_attr__.name.clone(),
                reason.clone(),
            ));
        }
        return outcome;
    }

    let adapter = match crate::check_index_adapter::open_index_adapter(
        index_dir,
        dbt_common::cancellation::never_cancels(),
    ) {
        Ok(a) => a,
        Err(e) => {
            for c in checks {
                outcome.failed += 1;
                outcome.results.push(CheckResult::error(
                    c.__common_attr__.unique_id.clone(),
                    c.__common_attr__.name.clone(),
                    e.clone(),
                ));
            }
            return outcome;
        }
    };

    for c in checks {
        let name = c.__common_attr__.name.clone();
        let unique_id = c.__common_attr__.unique_id.clone();
        let severity = c
            .deprecated_config
            .severity
            .clone()
            .unwrap_or(dbt_schemas::schemas::common::Severity::Error);
        let filter = selection_filter_for(c.deprecated_config.selection_filter_on.as_ref());

        let Some(sql) = c.__check_attr__.compiled_sql.as_deref() else {
            outcome.failed += 1;
            outcome.results.push(CheckResult::error(
                unique_id,
                name,
                "check has no rendered SQL".to_string(),
            ));
            continue;
        };

        match crate::check_index_adapter::query_index(&adapter, sql)
            .and_then(|batches| evaluate_batches(&batches, &filter, selection, max_preview_rows))
        {
            // An execution error is hard whatever the severity: we do not know whether the check
            // would have passed, so reporting a pass would be a lie.
            Err(message) => {
                outcome.failed += 1;
                outcome
                    .results
                    .push(CheckResult::error(unique_id, name, message));
            }
            Ok((0, _)) if zero_rows_is_vacuous(selection, &filter) => {
                outcome.results.push(CheckResult {
                    unique_id,
                    name,
                    status: "skipped",
                    message: Some(
                        "selection matched no nodes this check can report on".to_string(),
                    ),
                    violations: None,
                });
            }
            Ok((0, _)) => outcome.results.push(CheckResult {
                unique_id,
                name,
                status: "pass",
                message: None,
                violations: Some(0),
            }),
            Ok((violations, preview)) => {
                let detail = if preview.is_empty() {
                    None
                } else {
                    Some(preview.join("\n  "))
                };
                let hard = severity == dbt_schemas::schemas::common::Severity::Error;
                if hard {
                    outcome.failed += 1;
                }
                outcome.results.push(CheckResult {
                    unique_id,
                    name,
                    status: if hard { "fail" } else { "warn" },
                    message: detail,
                    violations: Some(violations),
                });
            }
        }
    }
    outcome
}
