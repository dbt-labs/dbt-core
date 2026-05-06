use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ParquetProject {
    pub project_name: String,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
}

pub type ParquetProjectRef = Arc<ParquetProject>;

pub(crate) fn generate_projects_arrow_schema() -> Vec<Field> {
    vec![
        Field::new("project_name", DataType::Utf8, false),
        Field::new("project_id", DataType::Utf8, true),
        Field::new("environment_id", DataType::Utf8, true),
    ]
}
