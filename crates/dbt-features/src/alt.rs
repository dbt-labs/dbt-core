use std::sync::Arc;

use dbt_tasks_core::alt_catalog_attach::AltCatalogAttachChecker;
use dbt_tasks_core::alt_propagation::AltPropagationChecker;

pub struct AltFeature {
    /// A checker for verifying propagation from an alt/remote compute target
    /// to the profile's native connection.
    ///
    /// Used during `dbt debug`. Returning `None` (the default) means this
    /// build has no such check available.
    pub propagation_checker: Option<Arc<dyn AltPropagationChecker>>,
    /// A checker for verifying that the catalogs a project declares
    /// are reachable from an alt/remote compute target.
    ///
    /// For use during `dbt debug`. Returning `None` (the default) means
    /// this build has no such check available.
    pub catalog_attach_checker: Option<Arc<dyn AltCatalogAttachChecker>>,
}
