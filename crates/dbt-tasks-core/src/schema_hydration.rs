use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dbt_common::FsError;

#[derive(Clone)]
pub struct SchemaHydrationDownloadWarning {
    pub err: Arc<FsError>,
    pub unique_id: String,
}

#[derive(Clone, Default)]
pub struct SchemaHydrationState {
    pub fetched_schema_fqns: HashSet<String>,
    pub download_warnings_by_fqn: HashMap<String, SchemaHydrationDownloadWarning>,
}
