use dbt_common::node_selector::{IndirectSelection, SelectExpression};
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_common::{ErrorCode, FsResult, err, fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::parse::build_resolve_context;
use dbt_jinja_utils::serde::value_from_file;
use dbt_schemas::schemas::{
    manifest::DbtSelector,
    selectors::{SelectorEntry, SelectorFile},
};
use dbt_selector_parser::{ResolvedSelector, SelectorParser};
use dbt_serde_yaml::Value as YmlValue;
use std::collections::{BTreeMap, HashMap};

use crate::args::ResolveArgs;

/// Loads and resolves selector definitions from a selectors.yml file.
pub fn resolve_selectors_from_yaml(
    arg: &ResolveArgs,
    root_package_name: &str,
    jinja_env: &JinjaEnv,
) -> FsResult<HashMap<String, SelectorEntry>> {
    match load_and_parse_selectors_file(arg, root_package_name, jinja_env)? {
        Some(yaml) => resolve_selector_definitions(yaml, arg),
        None => Ok(HashMap::new()), // No selectors.yml file found
    }
}

/// Converts resolved selectors to manifest format.
/// Takes the already resolved selectors and converts them to DbtSelector format for the manifest.
pub fn resolve_manifest_selectors(
    resolved_selectors: HashMap<String, SelectorEntry>,
) -> FsResult<BTreeMap<String, DbtSelector>> {
    validate_default_selectors(&resolved_selectors)?;

    // Convert to manifest format
    let manifest_selectors = resolved_selectors
        .into_iter()
        .map(|(name, entry)| {
            let definition_value = select_expression_to_yaml(&entry.include);

            let selector = DbtSelector {
                name: name.clone(),
                description: entry.description.unwrap_or_default(),
                __definition__: definition_value,
                __other__: BTreeMap::new(),
            };
            (name, selector)
        })
        .collect();

    Ok(manifest_selectors)
}

/// Computes the final include/exclude expressions from resolved selectors.
/// This function takes already resolved selectors and computes the final selection
/// that should be used by the scheduler.
///
/// The function:
/// 1. Validates that only one selector is marked as default
/// 2. Computes the final include/exclude expressions based on:
///    - CLI selector flag or default selector
///    - Selector's include/exclude expressions
///    - CLI include/exclude flags
///    - CLI indirect selection mode (fallback if not specified in YAML)
///
/// Returns the final include and exclude expressions to be used by the scheduler.
pub fn resolve_final_selectors(
    resolved_selectors: HashMap<String, SelectorEntry>,
    arg: &ResolveArgs,
) -> FsResult<ResolvedSelector> {
    validate_default_selectors(&resolved_selectors)?;

    // Find default selector name if no explicit selector provided
    let default_sel_name = resolved_selectors.iter().find_map(|(name, entry)| {
        // Command line arguments (if provided) take precedence over the default
        if entry.is_default && !(arg.select.is_some() || arg.exclude.is_some()) {
            Some(name.clone())
        } else {
            None
        }
    });

    // Use explicit selector, default selector, or fall back to CLI flags
    if let Some(sel_name) = arg.selector.as_ref().or(default_sel_name.as_ref()) {
        // Look up selector and error if missing
        let entry = resolved_selectors.get(sel_name.as_str()).ok_or_else(|| {
            fs_err!(
                ErrorCode::SelectorError,
                "Unknown selector `{}` (see selectors.yml)",
                sel_name
            )
        })?;

        // Use selector's include and apply CLI indirect selection as fallback
        let mut include = entry.include.clone();
        if let Some(cli_mode) = arg.indirect_selection {
            include.set_indirect_selection(cli_mode);
        }

        // Set exclude to CLI exclude and apply CLI indirect selection as fallback
        let mut exclude = arg.exclude.clone();
        if let (Some(cli_mode), Some(exc)) = (arg.indirect_selection, exclude.as_mut()) {
            exc.set_indirect_selection(cli_mode);
        }

        Ok(ResolvedSelector {
            include: Some(include),
            exclude,
        })
    } else {
        // No selector chosen → use CLI flags and apply CLI indirect selection
        let mut resolved = ResolvedSelector {
            include: arg.select.clone(),
            exclude: arg.exclude.clone(),
        };

        let default_mode = if arg.indirect_selection.is_some() {
            arg.indirect_selection.unwrap()
        } else {
            // eager is the default
            IndirectSelection::default()
        };

        if let Some(ref mut include) = resolved.include {
            include.apply_default_indirect_selection(default_mode);
        }
        if let Some(ref mut exclude) = resolved.exclude {
            exclude.apply_default_indirect_selection(default_mode);
        }

        Ok(resolved)
    }
}

/// Loads and parses the selectors.yml file from the project root.
/// Returns the parsed selectors.yml file if it exists, otherwise returns None.
fn load_and_parse_selectors_file(
    arg: &ResolveArgs,
    root_package_name: &str,
    jinja_env: &JinjaEnv,
) -> FsResult<Option<SelectorFile>> {
    let path = arg.io.in_dir.join("selectors.yml");
    if !path.exists() {
        return Ok(None);
    }

    let raw_selectors = value_from_file(&arg.io, &path, true, None)?;
    let context = build_resolve_context(
        root_package_name,
        root_package_name,
        &BTreeMap::new(),
        DISPATCH_CONFIG.get().unwrap().read().unwrap().clone(),
    );

    let yaml: SelectorFile = match dbt_jinja_utils::serde::into_typed_with_jinja(
        &arg.io,
        raw_selectors,
        false,
        jinja_env,
        &context,
        &[],
        None,
        true,
    ) {
        Ok(yaml) => yaml,
        Err(e) => {
            return err!(
                ErrorCode::SelectorError,
                "Error parsing selectors.yml: {}",
                e
            );
        }
    };

    Ok(Some(yaml))
}

/// Parses and resolves selector definitions from a YAML file.
/// Returns a map of selector names to their resolved entries.
fn resolve_selector_definitions(
    yaml: SelectorFile,
    arg: &ResolveArgs,
) -> FsResult<HashMap<String, SelectorEntry>> {
    let defs = yaml
        .selectors
        .iter()
        .map(|d| (d.name.clone(), d.clone()))
        .collect::<BTreeMap<_, _>>();
    let parser = SelectorParser::new(defs, &arg.io);
    let mut resolved_selectors = HashMap::new();

    for def in yaml.selectors {
        let resolved = parser.parse_definition(&def.definition)?;
        resolved_selectors.insert(
            def.name.clone(),
            SelectorEntry {
                include: resolved,
                is_default: def.default.unwrap_or(false),
                description: def.description,
            },
        );
    }

    Ok(resolved_selectors)
}

/// Converts a SelectExpression to the normalized YAML format expected by the manifest.
fn select_expression_to_yaml(expr: &SelectExpression) -> YmlValue {
    match expr {
        SelectExpression::Atom(criteria) => {
            let mut map = dbt_serde_yaml::Mapping::new();
            map.insert(
                YmlValue::String("method".to_string(), Default::default()),
                YmlValue::String(criteria.method.to_string(), Default::default()),
            );
            map.insert(
                YmlValue::String("value".to_string(), Default::default()),
                YmlValue::String(criteria.value.clone(), Default::default()),
            );

            if criteria.parents_depth.is_some() {
                map.insert(
                    YmlValue::String("parents".to_string(), Default::default()),
                    YmlValue::Bool(true, Default::default()),
                );
                // include the depth value if it's not unlimited
                if let Some(depth) = criteria.parents_depth
                    && depth != u32::MAX
                {
                    map.insert(
                        YmlValue::String("parents_depth".to_string(), Default::default()),
                        YmlValue::String(depth.to_string(), Default::default()),
                    );
                }
            }
            if criteria.children_depth.is_some() {
                map.insert(
                    YmlValue::String("children".to_string(), Default::default()),
                    YmlValue::Bool(true, Default::default()),
                );
                // include the depth value if it's not unlimited
                if let Some(depth) = criteria.children_depth
                    && depth != u32::MAX
                {
                    map.insert(
                        YmlValue::String("children_depth".to_string(), Default::default()),
                        YmlValue::String(depth.to_string(), Default::default()),
                    );
                }
            }
            if criteria.childrens_parents {
                map.insert(
                    YmlValue::String("childrens_parents".to_string(), Default::default()),
                    YmlValue::Bool(true, Default::default()),
                );
            }

            YmlValue::Mapping(map, Default::default())
        }
        SelectExpression::Or(expressions) => {
            let values: Vec<YmlValue> = expressions.iter().map(select_expression_to_yaml).collect();

            let mut union_map = dbt_serde_yaml::Mapping::new();
            union_map.insert(
                YmlValue::String("union".to_string(), Default::default()),
                YmlValue::Sequence(values, Default::default()),
            );
            YmlValue::Mapping(union_map, Default::default())
        }
        SelectExpression::And(expressions) => {
            let values: Vec<YmlValue> = expressions.iter().map(select_expression_to_yaml).collect();

            let mut intersection_map = dbt_serde_yaml::Mapping::new();
            intersection_map.insert(
                YmlValue::String("intersection".to_string(), Default::default()),
                YmlValue::Sequence(values, Default::default()),
            );
            YmlValue::Mapping(intersection_map, Default::default())
        }
        SelectExpression::Exclude(expr) => {
            let mut exclude_map = dbt_serde_yaml::Mapping::new();
            exclude_map.insert(
                YmlValue::String("exclude".to_string(), Default::default()),
                select_expression_to_yaml(expr),
            );
            YmlValue::Mapping(exclude_map, Default::default())
        }
    }
}

fn validate_default_selectors(resolved_selectors: &HashMap<String, SelectorEntry>) -> FsResult<()> {
    if resolved_selectors.values().filter(|e| e.is_default).count() > 1 {
        return err!(
            ErrorCode::SelectorError,
            "Multiple selectors have `default: true`"
        );
    }
    Ok(())
}
