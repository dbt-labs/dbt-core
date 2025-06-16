use serde::Deserialize;

/// SnapshotStrategy
#[derive(Clone, Debug, Deserialize)]
pub struct SnapshotStrategy {
    pub unique_key: Option<String>,
    pub updated_at: Option<String>,
    pub row_changed: Option<String>,
    pub scd_id: Option<String>,
    pub hard_deletes: Option<String>,
}
