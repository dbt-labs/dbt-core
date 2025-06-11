use dbt_common::node_selector::{IndirectSelection, SelectExpression};
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_common::{err, fs_err, ErrorCode, FsResult};
use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_jinja_utils::phases::parse::build_resolve_context;
use dbt_jinja_utils::serde::value_from_file;
use dbt_schemas::schemas::selectors::{SelectorEntry, SelectorFile};
use dbt_selector_parser::{ResolvedSelector, SelectorParser};
use std::collections::{BTreeMap, HashMap};

use crate::args::ResolveArgs;

/// Resolves selectors from YAML and computes the final include/exclude expressions.
/// This combines both parsing the selectors.yml file and computing the final expressions
/// that should be used by the scheduler.
///
/// The function:
/// 1. Parses and resolves selectors from YAML with Jinja templating
/// 2. Validates that only one selector is marked as default
/// 3. Computes the final include/exclude expressions based on:
///    - CLI selector flag or default selector
///    - Selector's include/exclude expressions
///    - CLI include/exclude flags
///    - CLI indirect selection mode (fallback if not specified in YAML)
///
/// Returns the final include and exclude expressions to be used by the scheduler.
pub fn resolve_final_selectors(
    root_package_name: &str,
    jinja_env: &JinjaEnvironment<'static>,
    arg: &ResolveArgs,
) -> FsResult<ResolvedSelector> {
    let path = arg.io.in_dir.join("selectors.yml");
    if !path.exists() {
        // No YAML selectors - apply CLI indirect selection to any CLI select/exclude
        let mut resolved = ResolvedSelector {
            include: arg.select.clone(),
            exclude: arg.exclude.clone(),
        };

        // Apply CLI indirect selection to both include and exclude expressions
        if let Some(cli_mode) = arg.indirect_selection {
            if let Some(ref mut include) = resolved.include {
                include.apply_default_indirect_selection(cli_mode);
            }
            if let Some(ref mut exclude) = resolved.exclude {
                exclude.apply_default_indirect_selection(cli_mode);
            }
        }

        return Ok(resolved);
    }

    let raw_selectors = value_from_file(Some(&arg.io), &path)?;

    let context = build_resolve_context(
        root_package_name,
        root_package_name,
        &BTreeMap::new(),
        DISPATCH_CONFIG.get().unwrap().read().unwrap().clone(),
    );

    // Parse and resolve selectors from YAML
    let yaml: SelectorFile = match dbt_jinja_utils::serde::into_typed_with_jinja(
        Some(&arg.io),
        raw_selectors,
        false,
        jinja_env,
        &context,
        None,
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

    // Build selector definitions map and resolve each selector
    let defs = yaml
        .selectors
        .iter()
        .map(|d| (d.name.clone(), d.clone()))
        .collect::<BTreeMap<_, _>>();
    let parser = SelectorParser::new(defs);
    let mut resolved_selectors = HashMap::new();
    for def in yaml.selectors {
        let resolved = parser.parse_definition(&def.definition)?;
        resolved_selectors.insert(
            def.name.clone(),
            SelectorEntry {
                resolved,
                is_default: def.default.unwrap_or(false),
                description: def.description,
            },
        );
    }

    // Validate only one default selector
    if resolved_selectors.values().filter(|e| e.is_default).count() > 1 {
        return err!(
            ErrorCode::SelectorError,
            "Multiple selectors have `default: true`"
        );
    }

    // Find default selector name if no explicit selector provided
    let default_sel_name = resolved_selectors.iter().find_map(|(name, entry)| {
        if entry.is_default {
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
        let mut include = entry.resolved.include.clone();
        if let (Some(cli_mode), Some(inc)) = (arg.indirect_selection, include.as_mut()) {
            inc.set_indirect_selection(cli_mode);
        }

        // Combine selector's exclude with CLI exclude and apply CLI indirect selection as fallback
        let mut exclude = match (entry.resolved.exclude.clone(), arg.exclude.clone()) {
            (Some(e1), Some(e2)) => Some(SelectExpression::Or(vec![e1, e2])),
            (Some(e1), None) => Some(e1),
            (None, Some(e2)) => Some(e2),
            (None, None) => None,
        };
        if let (Some(cli_mode), Some(exc)) = (arg.indirect_selection, exclude.as_mut()) {
            exc.set_indirect_selection(cli_mode);
        }

        Ok(ResolvedSelector { include, exclude })
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
