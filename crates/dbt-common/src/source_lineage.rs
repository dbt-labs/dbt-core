use dbt_base::HashMap;
use dbt_base::HashSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use console::Term;

use crate::tracing::formatters::color::{CYAN, DIM, GREEN, RED, WHITE, YELLOW, maybe_apply_color};
use crate::tracing::formatters::layout::format_delimiter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnKind {
    Attributed,
    Ambiguous(Vec<String>),
    Unsatisfiable,
    Derived,
}

pub struct ModelLineage {
    pub relation: String,
    pub columns: Vec<(String, ColumnKind, String)>,
}

fn report() -> &'static Mutex<Vec<ModelLineage>> {
    static REPORT: OnceLock<Mutex<Vec<ModelLineage>>> = OnceLock::new();
    REPORT.get_or_init(|| Mutex::new(Vec::new()))
}

fn show_sources_flag() -> &'static AtomicBool {
    static FLAG: OnceLock<AtomicBool> = OnceLock::new();
    FLAG.get_or_init(|| AtomicBool::new(false))
}

pub fn set_show_sources(value: bool) {
    if value {
        show_sources_flag().store(true, Ordering::Relaxed);
    }
}

fn resolve_ambiguous_cols_flag() -> &'static AtomicBool {
    static FLAG: OnceLock<AtomicBool> = OnceLock::new();
    FLAG.get_or_init(|| AtomicBool::new(false))
}

pub fn set_resolve_ambiguous_cols(value: bool) {
    if value {
        resolve_ambiguous_cols_flag().store(true, Ordering::Relaxed);
    }
}

const RELAUNCHED_ENV_VAR: &str = "DBT_AMBIGUOUS_COLS_RELAUNCHED";
const OVERRIDES_FILE_NAME: &str = ".dbt_resolved_ambiguous_cols.json";

type OverrideMap = HashMap<String, HashMap<String, String>>;

fn project_dir_slot() -> &'static Mutex<Option<PathBuf>> {
    static DIR: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
    DIR.get_or_init(|| Mutex::new(None))
}

pub fn set_project_dir(dir: &Path) {
    *project_dir_slot()
        .lock()
        .expect("project dir lock poisoned") = Some(dir.to_path_buf());
}

fn project_dir() -> PathBuf {
    project_dir_slot()
        .lock()
        .expect("project dir lock poisoned")
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
}

fn load_overrides_from(path: &Path) -> OverrideMap {
    let Ok(text) = std::fs::read_to_string(path) else {
        return HashMap::default();
    };
    let Ok(raw) = serde_json::from_str::<Vec<(String, String, String)>>(&text) else {
        return HashMap::default();
    };
    let mut map = OverrideMap::default();
    for (relation, column, source) in raw {
        map.entry(relation).or_default().insert(column, source);
    }
    map
}

fn save_overrides_to(path: &Path, map: &OverrideMap) {
    let raw: Vec<(&String, &String, &String)> = map
        .iter()
        .flat_map(|(relation, columns)| {
            columns
                .iter()
                .map(move |(column, source)| (relation, column, source))
        })
        .collect();
    if let Ok(text) = serde_json::to_string_pretty(&raw) {
        let _ = std::fs::write(path, text);
    }
}

fn prune_stale(map: &mut OverrideMap, models: &[ModelLineage]) {
    let mut known_columns: HashMap<&str, HashSet<&str>> = HashMap::default();
    for model in models {
        known_columns
            .entry(model.relation.as_str())
            .or_default()
            .extend(model.columns.iter().map(|(column, _, _)| column.as_str()));
    }
    for (relation, columns) in map.iter_mut() {
        if let Some(known) = known_columns.get(relation.as_str()) {
            columns.retain(|column, _| known.contains(column.as_str()));
        }
    }
    map.retain(|_, columns| !columns.is_empty());
}

struct OverridesState {
    dir: PathBuf,
    map: OverrideMap,
}

/// Reloads `state` from disk whenever `dir` differs from what it currently
/// holds, so the cache can never keep serving one project's overrides after
/// a process (e.g. dbt-lsp/dbt-repl) switches to a different project dir.
fn reload_if_dir_changed(state: &mut Option<OverridesState>, dir: &Path) {
    let up_to_date = matches!(state, Some(s) if s.dir == dir);
    if !up_to_date {
        let map = load_overrides_from(&dir.join(OVERRIDES_FILE_NAME));
        *state = Some(OverridesState {
            dir: dir.to_path_buf(),
            map,
        });
    }
}

fn overrides_state() -> &'static Mutex<Option<OverridesState>> {
    static STATE: OnceLock<Mutex<Option<OverridesState>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

fn with_overrides<T>(f: impl FnOnce(&mut OverrideMap) -> T) -> T {
    let dir = project_dir();
    let mut guard = overrides_state()
        .lock()
        .expect("ambiguous column overrides lock poisoned");
    reload_if_dir_changed(&mut guard, &dir);
    f(&mut guard
        .as_mut()
        .expect("reload_if_dir_changed always initializes")
        .map)
}

pub fn resolved_overrides_for(relation: &str) -> Vec<(String, String)> {
    with_overrides(|map| {
        map.get(relation)
            .map(|columns| {
                columns
                    .iter()
                    .map(|(column, source)| (column.clone(), source.clone()))
                    .collect()
            })
            .unwrap_or_default()
    })
}

fn lookup_override(relation: &str, column: &str) -> Option<String> {
    with_overrides(|map| {
        map.get(relation)
            .and_then(|columns| columns.get(column))
            .cloned()
    })
}

fn record_override(relation: &str, column: &str, source: String) {
    with_overrides(|map| {
        map.entry(relation.to_string())
            .or_default()
            .insert(column.to_string(), source);
    })
}

fn relaunch_with_resolutions(models: &[ModelLineage]) {
    let dir = project_dir();
    with_overrides(|map| {
        prune_stale(map, models);
        save_overrides_to(&dir.join(OVERRIDES_FILE_NAME), map);
    });
    let mut args = std::env::args();
    let Some(program) = args.next() else {
        return;
    };
    let rest: Vec<String> = args.collect();
    let display = format_relaunch_command(&program, &rest);
    let colorize = Term::stdout().is_term();
    println!();
    println!(
        "{}",
        maybe_apply_color(
            &CYAN,
            &format!("Rerunning `{display}` with your selections..."),
            colorize
        )
    );
    println!();
    let status = std::process::Command::new(&program)
        .args(&rest)
        .env(RELAUNCHED_ENV_VAR, "1")
        .status();
    let code = match status {
        Ok(status) => status.code().unwrap_or(1),
        Err(_) => 1,
    };
    std::process::exit(code);
}

fn format_relaunch_command(program: &str, rest: &[String]) -> String {
    std::iter::once(program)
        .chain(rest.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

fn should_prompt_interactively(
    resolve_flag: bool,
    is_terminal: bool,
    already_relaunched: bool,
) -> bool {
    resolve_flag && is_terminal && !already_relaunched
}

pub fn record(relation: String, columns: Vec<(String, ColumnKind, String)>) {
    report()
        .lock()
        .expect("source lineage report lock poisoned")
        .push(ModelLineage { relation, columns });
}

pub fn print_report() {
    let mut models = report()
        .lock()
        .expect("source lineage report lock poisoned")
        .split_off(0);
    if models.is_empty() {
        return;
    }
    models.sort_by(|a, b| a.relation.cmp(&b.relation));

    let (mut attributed, mut ambiguous, mut unsatisfiable, mut derived) = (0, 0, 0, 0);
    for model in &models {
        for (_, kind, _) in &model.columns {
            match kind {
                ColumnKind::Attributed => attributed += 1,
                ColumnKind::Ambiguous(_) => ambiguous += 1,
                ColumnKind::Unsatisfiable => unsatisfiable += 1,
                ColumnKind::Derived => derived += 1,
            }
        }
    }
    let total = attributed + ambiguous + unsatisfiable + derived;

    let colorize = Term::stdout().is_term();
    let width = Term::stdout().size_checked().map(|(_, cols)| cols as usize);

    println!();
    println!(
        "{}",
        format_delimiter(" Inferred Schema Summary ", width, colorize)
    );
    println!(
        "{} | {} | {} | {} ({} across {} models)",
        colored_count(attributed, "attributed", colorize, &GREEN),
        colored_count(ambiguous, "ambiguous", colorize, &YELLOW),
        colored_count(unsatisfiable, "unsatisfiable", colorize, &RED),
        colored_count(derived, "derived", colorize, &DIM),
        colored_count(total, "total", colorize, &WHITE),
        models.len()
    );

    if ambiguous > 0 {
        println!();
        let already_relaunched = std::env::var_os(RELAUNCHED_ENV_VAR).is_some();
        let interactive = should_prompt_interactively(
            resolve_ambiguous_cols_flag().load(Ordering::Relaxed),
            std::io::stdin().is_terminal(),
            already_relaunched,
        );
        let mut resolved_any = false;
        for model in &models {
            for (column, kind, _) in &model.columns {
                let ColumnKind::Ambiguous(candidates) = kind else {
                    continue;
                };
                let choice = match lookup_override(&model.relation, column) {
                    Some(previous) => Some(previous),
                    None if interactive => {
                        let picked = prompt_pick(column, &model.relation, candidates);
                        if let Some(picked) = &picked {
                            record_override(&model.relation, column, picked.clone());
                            resolved_any = true;
                        }
                        picked
                    }
                    None => None,
                };
                match choice {
                    Some(picked) => {
                        println!("  {column} in {} resolved to {picked}", model.relation);
                    }
                    None => {
                        let message = format!(
                            "'{column}' in {} could not be attributed — candidates: {}",
                            model.relation,
                            candidates.join(", ")
                        );
                        println!("{}", maybe_apply_color(&YELLOW, &message, colorize));
                    }
                }
            }
        }
        if resolved_any && !already_relaunched {
            relaunch_with_resolutions(&models);
        }
    }

    if show_sources_flag().load(Ordering::Relaxed) {
        println!();
        println!(
            "{}",
            format_delimiter(" Inferred Source Lineage ", width, colorize)
        );
        for model in &models {
            println!("{}", maybe_apply_color(&WHITE, &model.relation, colorize));
            for (column, kind, source) in &model.columns {
                println!("{}", format_source_line(column, kind, source));
            }
        }
    }
}

fn format_source_line(column: &str, kind: &ColumnKind, source: &str) -> String {
    match kind {
        ColumnKind::Ambiguous(candidates) => {
            format!("  {column} <- ? {}", candidates.join(" | "))
        }
        _ => format!("  {column} <- {source}"),
    }
}

fn colored_count(value: usize, label: &str, colorize: bool, style: &console::Style) -> String {
    let text = format!("{value} {label}");
    if value == 0 {
        maybe_apply_color(&DIM, &text, colorize)
    } else {
        maybe_apply_color(style, &text, colorize)
    }
}

const SKIP_OPTION: &str = "I don't know / skip";

fn build_options(candidates: &[String]) -> Vec<String> {
    let mut options = candidates.to_vec();
    options.push(SKIP_OPTION.to_string());
    options
}

fn prompt_pick(column: &str, relation: &str, candidates: &[String]) -> Option<String> {
    let options = build_options(candidates);
    let selection = dialoguer::Select::with_theme(&dialoguer::theme::ColorfulTheme::default())
        .with_prompt(format!("'{column}' in {relation} could be attributed to"))
        .items(&options)
        .default(0)
        .interact()
        .ok()?;
    options
        .get(selection)
        .filter(|s| *s != SKIP_OPTION)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_source_line_single_attributed_unchanged() {
        let line = format_source_line("id", &ColumnKind::Attributed, "DB.SCH.T.ID");
        assert_eq!(line, "  id <- DB.SCH.T.ID");
    }

    #[test]
    fn format_source_line_multi_source_attributed() {
        let line = format_source_line(
            "opens",
            &ColumnKind::Attributed,
            "DB.SCH.T.EVENT_TYPE + DB.SCH.T.SENT_BY_EVENT_ID",
        );
        assert_eq!(
            line,
            "  opens <- DB.SCH.T.EVENT_TYPE + DB.SCH.T.SENT_BY_EVENT_ID"
        );
    }

    #[test]
    fn format_source_line_ambiguous() {
        let line = format_source_line(
            "status",
            &ColumnKind::Ambiguous(vec![
                "DB.SCH.USERS".to_string(),
                "DB.SCH.ORDERS".to_string(),
            ]),
            "",
        );
        assert_eq!(line, "  status <- ? DB.SCH.USERS | DB.SCH.ORDERS");
    }

    #[test]
    fn format_source_line_derived_unchanged() {
        let line = format_source_line("total", &ColumnKind::Derived, "derived");
        assert_eq!(line, "  total <- derived");
    }

    #[test]
    fn format_source_line_unsatisfiable_unchanged() {
        let line = format_source_line("total", &ColumnKind::Unsatisfiable, "unsatisfiable");
        assert_eq!(line, "  total <- unsatisfiable");
    }

    #[test]
    fn build_options_appends_skip_option_to_two_candidates() {
        let options = build_options(&["DB.SCH.USERS".to_string(), "DB.SCH.ORDERS".to_string()]);
        assert_eq!(options, vec!["DB.SCH.USERS", "DB.SCH.ORDERS", SKIP_OPTION]);
    }

    #[test]
    fn build_options_appends_skip_option_to_three_candidates() {
        let options = build_options(&[
            "DB.SCH.USERS".to_string(),
            "DB.SCH.ORDERS".to_string(),
            "DB.SCH.ACCOUNTS".to_string(),
        ]);
        assert_eq!(
            options,
            vec![
                "DB.SCH.USERS",
                "DB.SCH.ORDERS",
                "DB.SCH.ACCOUNTS",
                SKIP_OPTION
            ]
        );
    }

    #[test]
    fn overrides_round_trip_through_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("overrides.json");
        let mut map = OverrideMap::default();
        map.entry("db.dev.stg_two_way".to_string())
            .or_default()
            .insert("status".to_string(), "db.raw.orders".to_string());
        map.entry("db.dev.stg_three_way".to_string())
            .or_default()
            .insert("status".to_string(), "db.raw.contacts".to_string());
        save_overrides_to(&path, &map);
        assert_eq!(load_overrides_from(&path), map);
    }

    #[test]
    fn load_overrides_missing_file_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("missing.json");
        assert!(load_overrides_from(&path).is_empty());
    }

    #[test]
    fn reload_if_dir_changed_does_not_leak_overrides_across_project_dirs() {
        let dir_a = tempfile::tempdir().expect("tempdir");
        let dir_b = tempfile::tempdir().expect("tempdir");
        let mut map_a = OverrideMap::default();
        map_a
            .entry("db.a".to_string())
            .or_default()
            .insert("col".to_string(), "db.raw.x".to_string());
        save_overrides_to(&dir_a.path().join(OVERRIDES_FILE_NAME), &map_a);

        let mut state: Option<OverridesState> = None;
        reload_if_dir_changed(&mut state, dir_a.path());
        assert_eq!(state.as_ref().expect("loaded").map, map_a);

        reload_if_dir_changed(&mut state, dir_b.path());
        assert!(
            state.as_ref().expect("loaded").map.is_empty(),
            "switching to a different project dir must not keep the previous dir's overrides"
        );

        reload_if_dir_changed(&mut state, dir_a.path());
        assert_eq!(
            state.as_ref().expect("loaded").map,
            map_a,
            "switching back to a known dir must reload its overrides"
        );
    }

    #[test]
    fn reload_if_dir_changed_keeps_in_memory_edits_when_dir_unchanged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut state: Option<OverridesState> = None;
        reload_if_dir_changed(&mut state, dir.path());
        state
            .as_mut()
            .expect("loaded")
            .map
            .entry("db.a".to_string())
            .or_default()
            .insert("col".to_string(), "db.raw.x".to_string());

        reload_if_dir_changed(&mut state, dir.path());
        assert!(
            state.as_ref().expect("loaded").map.contains_key("db.a"),
            "re-checking the same dir must not discard in-memory edits made since the last load"
        );
    }

    #[test]
    fn should_prompt_interactively_forces_false_when_already_relaunched() {
        assert!(!should_prompt_interactively(true, true, true));
        assert!(should_prompt_interactively(true, true, false));
        assert!(!should_prompt_interactively(false, true, false));
        assert!(!should_prompt_interactively(true, false, false));
    }

    #[test]
    fn format_relaunch_command_joins_program_and_args() {
        let command = format_relaunch_command(
            "dbt",
            &[
                "compile".to_string(),
                "--resolve-ambiguous-cols".to_string(),
            ],
        );
        assert_eq!(command, "dbt compile --resolve-ambiguous-cols");
    }

    #[test]
    fn prune_stale_drops_columns_no_longer_present_in_the_model() {
        let mut map = OverrideMap::default();
        map.entry("db.dev.stg".to_string())
            .or_default()
            .insert("status".to_string(), "db.raw.orders".to_string());
        map.entry("db.dev.stg".to_string())
            .or_default()
            .insert("renamed_col".to_string(), "db.raw.orders".to_string());
        let models = vec![ModelLineage {
            relation: "db.dev.stg".to_string(),
            columns: vec![("status".to_string(), ColumnKind::Derived, String::new())],
        }];
        prune_stale(&mut map, &models);
        let columns = map.get("db.dev.stg").expect("relation retained");
        assert!(columns.contains_key("status"));
        assert!(!columns.contains_key("renamed_col"));
    }

    #[test]
    fn prune_stale_keeps_relations_absent_from_this_run() {
        let mut map = OverrideMap::default();
        map.entry("db.dev.not_selected_this_run".to_string())
            .or_default()
            .insert("status".to_string(), "db.raw.orders".to_string());
        prune_stale(&mut map, &[]);
        assert!(map.contains_key("db.dev.not_selected_this_run"));
    }
}
