use crate::schemas::macros::DbtMacro;
use dbt_common::{ErrorCode, FsResult, adapter::AdapterType, fs_err};
use minijinja::{
    compiler::parser::materialization_macro_name,
    dispatch_object::{get_adapter_prefixes, get_internal_packages},
};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

// Built-in materializations
const BUILTIN_MATERIALIZATIONS: &[&str] = &[
    "view",
    "table",
    "incremental",
    "materialized_view",
    "test",
    "unit",
    "snapshot",
    "seed",
    "clone",
];

/// Indicates where a macro originates from for resolution priority  
/// Values match Python exactly: Core=1, Imported=2, Root=3
/// After sorting, the LAST element is taken (highest precedence)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MacroLocality {
    /// dbt core or adapter-internal package (value 0, matches Python Core=1)
    Core,
    /// Any installed third-party package (value 1, matches Python Imported=2)
    Imported,
    /// The current root project (value 2, matches Python Root=3 - highest precedence)
    Root,
}

/// A candidate materialization macro with associated metadata used for tie-breaking
#[derive(Debug, Clone)]
pub struct MaterializationCandidate {
    /// Fully qualified macro function name (e.g., `materialization_table_postgres`)
    pub macro_name: String,
    /// The package where the macro is defined
    pub package_name: String,
    /// Macro origin used to order candidates
    pub locality: MacroLocality,
    /// Adapter specificity index (0 = exact adapter, 1 = parent, ...)
    pub specificity: usize,
}

impl MaterializationCandidate {
    /// Construct a new materialization candidate
    pub fn new(
        macro_name: String,
        package_name: String,
        locality: MacroLocality,
        specificity: usize,
    ) -> Self {
        Self {
            macro_name,
            package_name,
            locality,
            specificity,
        }
    }
}

/// A collection of materialization macro candidates with helpers to select the best
#[derive(Clone, Default)]
pub struct MaterializationCandidateList {
    /// Inner candidate list
    pub candidates: Vec<MaterializationCandidate>,
}

impl MaterializationCandidateList {
    /// Create an empty list
    pub fn new() -> Self {
        Self {
            candidates: Vec::new(),
        }
    }

    /// Add a candidate to the list
    pub fn add(&mut self, candidate: MaterializationCandidate) {
        self.candidates.push(candidate);
    }

    /// Sort candidates to match dbt Core's MaterializationCandidate.__lt__ method
    ///
    /// This sorting logic appears "backwards" but is intentional:
    ///
    /// **Priority Rules (what we want):**
    /// - Lower specificity is better (0=exact adapter, 1=parent, 2=default)
    /// - Higher locality is better (Core=0 < Imported=1 < Root=2)
    /// - Take the LAST element after sorting (highest precedence)
    ///
    /// **Why the "backwards" comparison:**
    /// Python's __lt__ method makes "worse" candidates "less than" better ones,
    /// so they sort first. Since we take the last element, the best candidate ends up last.
    ///
    /// **Examples:**
    /// - Candidate A: specificity=1, locality=Root → "less than" B: specificity=0, locality=Core
    /// - Result: [A, B] → take B (last) = correct winner (lower specificity + higher locality)
    ///
    /// See: https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/contracts/graph/manifest.py
    fn sort(&mut self) {
        self.candidates
            .sort_by(|a, b| match b.specificity.cmp(&a.specificity) {
                // Reversed: higher specificity values (worse) come first in sort
                std::cmp::Ordering::Equal => a.locality.cmp(&b.locality), // Normal: lower locality values (worse) come first
                other => other,
            });
    }

    /// Return the best candidate after sorting
    ///
    /// The sorting puts "worse" candidates first and "better" candidates last.
    /// We take the last element because it has the highest precedence:
    /// - Lowest specificity (most adapter-specific match)
    /// - Highest locality (Root > Imported > Core)
    pub fn best_candidate(&mut self) -> Option<&MaterializationCandidate> {
        self.sort();
        self.candidates.last()
    }

    /// Filter candidates by locality
    pub fn candidates_with_locality(
        &self,
        locality: MacroLocality,
    ) -> Vec<&MaterializationCandidate> {
        self.candidates
            .iter()
            .filter(|c| c.locality == locality)
            .collect()
    }
}

/// Resolver that applies dbt's multiple-dispatch rules to find a materialization macro
#[derive(Debug)]
pub struct MaterializationResolver {
    /// Pre-filtered materialization macros only, keyed by unique_id
    pub materialization_macros: BTreeMap<String, DbtMacro>,
    /// Active adapter type (e.g., `postgres`, `redshift`)
    pub adapter_type: String,
    /// Root project name for locality determination
    pub root_project_name: String,
    /// Cache mapping materialization name -> fully qualified macro name
    pub cache: Mutex<HashMap<String, String>>,
}

impl MaterializationResolver {
    /// Create a new resolver instance with pre-filtered materialization macros
    ///
    /// * `macros` - All macros in the project
    /// * `adapter_type` - Current adapter type (e.g., "snowflake", "postgres")
    /// * `root_project_name` - Name of the root project
    pub fn new(
        macros: &BTreeMap<String, DbtMacro>,
        adapter_type: AdapterType,
        root_project_name: &str,
    ) -> Self {
        // Pre-filter to only materialization macros
        let materialization_macros: BTreeMap<String, DbtMacro> = macros
            .iter()
            .filter(|(_, macro_obj)| macro_obj.name.starts_with("materialization_"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Self {
            materialization_macros,
            adapter_type: adapter_type.to_string(),
            root_project_name: root_project_name.to_string(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn classify_macro_locality(&self, package_name: &str) -> MacroLocality {
        let internal_packages = get_internal_packages(&self.adapter_type);
        if internal_packages.contains(&package_name.to_string()) {
            MacroLocality::Core
        } else if package_name == self.root_project_name {
            MacroLocality::Root
        } else {
            MacroLocality::Imported
        }
    }

    fn find_materialization_candidates(
        &self,
        materialization_name: &str,
    ) -> MaterializationCandidateList {
        let mut candidates = MaterializationCandidateList::new();
        let adapter_prefixes = get_adapter_prefixes(&self.adapter_type);
        let expected_names: Vec<String> = adapter_prefixes
            .iter()
            .map(|suffix| materialization_macro_name(materialization_name, suffix))
            .collect();
        // Only iterate through pre-filtered materialization macros
        for macro_obj in self.materialization_macros.values() {
            if let Some(position) = expected_names
                .iter()
                .position(|name| &macro_obj.name == name)
            {
                let locality = self.classify_macro_locality(&macro_obj.package_name);
                let candidate = MaterializationCandidate::new(
                    macro_obj.name.clone(),
                    macro_obj.package_name.clone(),
                    locality,
                    position,
                );
                candidates.add(candidate);
            }
        }
        candidates
    }

    /// Resolve the fully qualified materialization macro function name according to dbt rules
    ///
    /// Rules:
    /// - When require_explicit_package_overrides_for_builtin_materializations is true:
    ///   - For built-in materializations (view, table, etc.), Core and Root take precedence over Imported
    ///   - For custom materializations, standard adapter specificity and locality rules apply
    /// - When require_explicit_package_overrides_for_builtin_materializations is false:
    ///   - For all materializations, standard adapter specificity and locality rules apply
    ///   - This means Imported packages can override Core materializations
    pub fn find_materialization_macro_by_name(
        &self,
        materialization_name: &str,
    ) -> FsResult<String> {
        if let Ok(cache) = self.cache.lock() {
            if let Some(cached) = cache.get(materialization_name) {
                return Ok(cached.clone());
            }
        }

        // Standard resolution path for non-dot notation names
        let mut candidates = self.find_materialization_candidates(materialization_name);
        let is_builtin = BUILTIN_MATERIALIZATIONS.contains(&materialization_name);
        let has_core_candidates = !candidates
            .candidates_with_locality(MacroLocality::Core)
            .is_empty();

        // When there are core candidates available, we need to filter out imported materializations for built-ins
        // otherwise it's handled by the non-builtin path below.
        if is_builtin && has_core_candidates {
            let mut temp = candidates.clone();
            if let Some(best) = temp.best_candidate() {
                if best.locality == MacroLocality::Imported {
                    let mut filtered = MaterializationCandidateList::new();
                    for c in &candidates.candidates {
                        if matches!(c.locality, MacroLocality::Root | MacroLocality::Core) {
                            filtered.add(c.clone());
                        }
                    }
                    if let Some(best_filtered) = filtered.best_candidate() {
                        let result = format!(
                            "{}.{}",
                            best_filtered.package_name, best_filtered.macro_name
                        );
                        if let Ok(mut cache) = self.cache.lock() {
                            cache.insert(materialization_name.to_string(), result.clone());
                        }
                        return Ok(result);
                    }
                }
            }
        }

        // For non-builtin materializations, we should apply standard adapter specificity and locality rules
        // and return the best candidate
        if let Some(best) = candidates.best_candidate() {
            let result = format!("{}.{}", best.package_name, best.macro_name);
            if let Ok(mut cache) = self.cache.lock() {
                cache.insert(materialization_name.to_string(), result.clone());
            }
            return Ok(result);
        }

        Err(fs_err!(
            ErrorCode::Unexpected,
            "Materialization macro not found for materialization: {}, adapter: {}",
            materialization_name,
            &self.adapter_type
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn build_macro(name: &str, package: &str) -> DbtMacro {
        let mut m = DbtMacro::default();
        m.name = name.to_string();
        m.package_name = package.to_string();
        m.unique_id = format!("macro.{package}.{name}");
        m.original_file_path = PathBuf::from(format!("macros/{package}/{name}.sql"));
        m.path = m.original_file_path.clone();
        m.description = String::new();
        m
    }

    fn resolver_with(
        all_macros: Vec<DbtMacro>,
        adapter_type: AdapterType,
        root_project_name: &str,
    ) -> MaterializationResolver {
        let mut map = BTreeMap::<String, DbtMacro>::new();
        for m in all_macros {
            map.insert(m.unique_id.clone(), m);
        }
        MaterializationResolver::new(&map, adapter_type, root_project_name)
    }

    #[test]
    fn local_plugin_specific_beats_local_default() {
        // Root project defines both plugin-specific and default for "view"
        // Expect plugin-specific to win
        let root = "my_root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            build_macro("materialization_view_postgres", root),
            build_macro("materialization_view_default", root),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        assert_eq!(got, format!("{}.{}", root, "materialization_view_postgres"));
    }

    #[test]
    fn imported_plugin_specific_beats_local_default() {
        // Imported plugin-specific vs root default → plugin-specific should win
        let root = "my_root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            build_macro("materialization_view_default", root),
            build_macro("materialization_view_postgres", "pkg_a"),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        assert_eq!(got, "pkg_a.materialization_view_postgres".to_string());
    }

    #[test]
    fn imported_plugin_specific_beats_imported_default() {
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            build_macro("materialization_view_postgres", "pkg_x"),
            build_macro("materialization_view_default", "pkg_x"),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        assert_eq!(got, "pkg_x.materialization_view_postgres".to_string());
    }

    #[test]
    fn core_plugin_specific_beats_imported_default() {
        // Core (dbt_postgres) plugin-specific vs imported default → plugin-specific should win
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            build_macro("materialization_view_postgres", "dbt_postgres"),
            build_macro("materialization_view_default", "pkg_y"),
            build_macro("materialization_view_default", "dbt"),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        assert_eq!(
            got,
            "dbt_postgres.materialization_view_postgres".to_string()
        );
    }

    #[test]
    fn root_beats_core_when_same_specificity() {
        // Root plugin-specific should beat Core plugin-specific when same adapter specificity
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            build_macro("materialization_view_postgres", root),
            build_macro("materialization_view_postgres", "dbt_postgres"),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        assert_eq!(got, format!("{}.{}", root, "materialization_view_postgres"));
    }

    #[test]
    fn parent_adapter_fallback_is_used() {
        // Adapter redshift falls back to postgres if redshift-specific macro doesn't exist
        let root = "root";
        let adapter = AdapterType::Redshift; // prefixes: redshift, postgres, default
        let macros = vec![
            // No redshift-specific
            build_macro("materialization_view_postgres", root), // parent adapter-specific
            build_macro("materialization_view_default", "dbt"), // default core fallback
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve via parent adapter fallback");
        assert_eq!(got, format!("{}.{}", root, "materialization_view_postgres"));
    }

    #[test]
    fn default_fallback_is_used_when_no_specific_or_parent() {
        // When neither adapter-specific nor parent exists, default should be used
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            // Only default exists (core)
            build_macro("materialization_view_default", "dbt"),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve to default core");
        assert_eq!(got, "dbt.materialization_view_default".to_string());
    }

    #[test]
    fn error_on_undefined_materialization() {
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![build_macro("materialization_view_default", "dbt")];
        let resolver = resolver_with(macros, adapter, root);
        let err = resolver
            .find_materialization_macro_by_name("does_not_exist")
            .unwrap_err();
        let msg = format!(
            "Materialization macro not found for materialization: {}, adapter: {}",
            "does_not_exist", adapter
        );
        assert!(format!("{err}").contains(&msg));
    }

    #[test]
    fn imported_cannot_override_builtin() {
        // For built-in names, when flag is enabled and core exists, imported should be ignored.
        // Craft scenario where imported is more specific than core (plugin-specific vs core default)
        // so that the flag meaningfully changes the outcome.
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            // Core only has DEFAULT for built-in "view"
            build_macro("materialization_view_default", "dbt"),
            // Imported provides plugin-specific for current adapter
            build_macro("materialization_view_postgres", "pkg_override"),
        ];
        // Flag enabled → imported excluded → core default selected
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        assert_eq!(got, "dbt.materialization_view_default".to_string());
    }

    #[test]
    fn imported_can_define_non_builtin_materializations() {
        // For non-builtin materializations like 'test_table', imported packages can define them
        // regardless of flag setting
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            // External package provides test_table materialization
            build_macro("materialization_test_table_postgres", "external_package"),
        ];

        // Flag enabled - imported still used for non-builtin 'test_table'
        let resolver = resolver_with(macros.clone(), adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("test_table")
            .expect("should resolve");
        assert_eq!(
            got,
            "external_package.materialization_test_table_postgres".to_string()
        );

        // Flag disabled - same behavior
        let resolver2 = resolver_with(macros, adapter, root);
        let got2 = resolver2
            .find_materialization_macro_by_name("test_table")
            .expect("should resolve");
        assert_eq!(
            got2,
            "external_package.materialization_test_table_postgres".to_string()
        );
    }

    #[test]
    fn root_can_still_override_builtin() {
        // a root reimplementation should be able to override the default implementation
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            // Core
            build_macro("materialization_view_postgres", "dbt_postgres"),
            build_macro("materialization_view_default", "dbt"),
            // Root reimplementation (wrapper pattern)
            build_macro("materialization_view_postgres", root),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let got = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        assert_eq!(got, format!("{}.{}", root, "materialization_view_postgres"));
    }

    #[test]
    fn caching_returns_consistent_results() {
        let root = "root";
        let adapter = AdapterType::Postgres;
        let macros = vec![
            build_macro("materialization_view_postgres", root),
            build_macro("materialization_view_default", "dbt"),
        ];
        let resolver = resolver_with(macros, adapter, root);
        let first = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve");
        let second = resolver
            .find_materialization_macro_by_name("view")
            .expect("should resolve from cache");
        assert_eq!(first, second);
    }
}
