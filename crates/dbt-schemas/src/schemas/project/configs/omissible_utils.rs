//! Utilities for handling hierarchical configuration overrides with Omissible fields
//! This module provides generic functionality for managing configuration inheritance
//! where explicit null values can override parent configurations.

use dbt_common::serde_utils::Omissible;

/// Generic helper to handle hierarchical override logic for Omissible fields
/// This encapsulates the complex logic needed for config overrides
/// and makes it easy to extend to other configuration fields
pub fn handle_omissible_override<T: Clone>(
    self_field: &mut Omissible<Option<T>>,
    parent_field: &Omissible<Option<T>>,
) {
    match (self_field.clone(), parent_field.clone()) {
        (Omissible::Omitted, _) => {
            // Self doesn't specify field, inherit parent's setting
            *self_field = parent_field.clone();
        }
        (Omissible::Present(None), Omissible::Present(Some(_))) => {
            // Self explicitly sets field to null, this overrides parent's value
            // (keep self's None value - do nothing)
        }
        (Omissible::Present(Some(_)), Omissible::Present(None)) => {
            // Parent explicitly sets field to null, this should override self's value
            // This handles the case where parent config has +field: null
            *self_field = parent_field.clone();
        }
        (Omissible::Present(Some(_)), Omissible::Omitted) => {
            // Self explicitly sets field to a value, parent doesn't override
            // (keep self's value - do nothing)
        }
        (Omissible::Present(Some(_)), _) => {
            // Self explicitly sets field to a value, this overrides parent
            // (keep self's value - do nothing)
        }
        _ => {
            // Other cases: keep self's value
            // (do nothing)
        }
    }
}
