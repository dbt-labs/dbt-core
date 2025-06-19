use dbt_serde_yaml::Verbatim;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::schemas::common::merge_meta;
use crate::schemas::common::merge_tags;
use crate::schemas::common::DbtQuoting;
use crate::schemas::common::Hooks;
use crate::schemas::serde::StringOrArrayOfStrings;

/// Helper function to handle default_to logic for hooks (pre_hook/post_hook)
/// Hooks should be extended, not replaced when merging configs
pub fn default_hooks(
    child_hooks: &mut Verbatim<Option<Hooks>>,
    parent_hooks: &Verbatim<Option<Hooks>>,
) {
    if let Some(parent_hooks) = &**parent_hooks {
        if let Some(child_hooks) = &mut **child_hooks {
            child_hooks.extend(parent_hooks);
        } else {
            *child_hooks = Verbatim(Some(parent_hooks.clone()));
        }
    }
}

/// Helper function to handle default_to logic for quoting configs
/// Quoting has its own default_to method that should be called
pub fn default_quoting(
    child_quoting: &mut Option<DbtQuoting>,
    parent_quoting: &Option<DbtQuoting>,
) {
    if let Some(quoting) = child_quoting {
        if let Some(parent_quoting) = parent_quoting {
            quoting.default_to(parent_quoting);
        }
    } else {
        *child_quoting = *parent_quoting;
    }
}

/// Helper function to handle default_to logic for meta and tags
/// Uses the existing merge functions for proper merging behavior
pub fn default_meta_and_tags(
    child_meta: &mut Option<BTreeMap<String, Value>>,
    parent_meta: &Option<BTreeMap<String, Value>>,
    child_tags: &mut Option<StringOrArrayOfStrings>,
    parent_tags: &Option<StringOrArrayOfStrings>,
) {
    // Handle meta using existing merge function
    *child_meta = merge_meta(child_meta.take(), parent_meta.clone());

    // Handle tags using existing merge function
    let child_tags_vec = child_tags.take().map(|tags| tags.into());
    let parent_tags_vec = parent_tags.clone().map(|tags| tags.into());
    *child_tags =
        merge_tags(child_tags_vec, parent_tags_vec).map(StringOrArrayOfStrings::ArrayOfStrings);
}

/// Helper function to handle default_to logic for column_types
/// Column types should be merged, with parent values filling in missing keys
pub fn default_column_types(
    child_column_types: &mut Option<BTreeMap<String, String>>,
    parent_column_types: &Option<BTreeMap<String, String>>,
) {
    match (child_column_types, parent_column_types) {
        (Some(inner_column_types), Some(parent_column_types)) => {
            for (key, value) in parent_column_types {
                inner_column_types
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        (column_types, Some(parent_column_types)) => {
            *column_types = Some(parent_column_types.clone())
        }
        (_, None) => {}
    }
}
