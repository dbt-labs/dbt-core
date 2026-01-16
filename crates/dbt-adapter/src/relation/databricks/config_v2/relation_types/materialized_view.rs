//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/materialized_view.py

use crate::relation::config_v2::ComponentConfigChange;
use crate::relation::config_v2::{ComponentConfigLoader, RelationConfigLoader};
use crate::relation::databricks::config_v2::{DatabricksRelationMetadata, components};
use indexmap::IndexMap;

fn requires_full_refresh(components: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
    super::requires_full_refresh(super::MaterializationType::MaterializedView, components)
}

/// Create a `RelationConfigLoader` for Databricks materialized views
pub(crate) fn new_loader() -> RelationConfigLoader<DatabricksRelationMetadata> {
    // TODO: missing from Python dbt-databricks:
    // - liquid clustering
    // - relation tags
    // - query
    let loaders: [Box<dyn ComponentConfigLoader<DatabricksRelationMetadata>>; 4] = [
        // Box::new(components::LiquidClusteringLoader),
        Box::new(components::RelationCommentLoader),
        Box::new(components::PartitionByLoader),
        // Box::new(components::QueryLoader),
        Box::new(components::RefreshLoader),
        // Box::new(components::RelationTagsLoader),
        Box::new(components::TblPropertiesLoader),
    ];

    RelationConfigLoader::new(loaders, requires_full_refresh)
}

#[cfg(test)]
mod tests {
    use super::{new_loader, requires_full_refresh};
    use crate::relation::config_v2::{ComponentConfigChange, RelationComponentConfigChangeSet};
    use crate::relation::databricks::config_v2::{
        DatabricksRelationMetadata, components,
        test_helpers::{TestModelConfig, run_test_cases},
    };
    use crate::relation::test_helpers::TestCase;
    use indexmap::IndexMap;

    fn create_test_cases() -> Vec<TestCase<DatabricksRelationMetadata, TestModelConfig>> {
        vec![
            TestCase {
                description: "changing any of materialized view's components except refresh or tags should trigger a full refresh",
                relation_loader: new_loader(),
                current_state: TestModelConfig {
                    persist_relation_comments: true,
                    query: Some("SELECT 1".to_string()),
                    tbl_properties: IndexMap::from_iter([
                        ("delta.enableRowTracking".to_string(), "false".to_string()),
                        (
                            "pipelines.pipelineId".to_string(),
                            "my_old_pipeline".to_string(),
                        ),
                        ("custom.key".to_string(), "old".to_string()),
                    ]),
                    partition_by: vec!["partition_column_old".to_string()],
                    ..Default::default()
                },
                desired_state: TestModelConfig {
                    persist_relation_comments: true,
                    query: Some("SELECT 1000".to_string()),
                    tbl_properties: IndexMap::from_iter([
                        ("delta.enableRowTracking".to_string(), "true".to_string()),
                        (
                            "pipelines.pipelineId".to_string(),
                            "my_old_pipeline".to_string(),
                        ),
                        ("custom.key".to_string(), "new".to_string()),
                    ]),
                    partition_by: vec!["partition_column_new".to_string()],
                    ..Default::default()
                },
                expected_changeset: RelationComponentConfigChangeSet::new(
                    [
                        (
                            components::TblPropertiesLoader::type_name(),
                            ComponentConfigChange::Some(components::TblPropertiesLoader::new(
                                IndexMap::from_iter([(
                                    "custom.key".to_string(),
                                    "new".to_string(),
                                )]),
                            )),
                        ),
                        (
                            components::PartitionByLoader::type_name(),
                            ComponentConfigChange::Some(components::PartitionByLoader::new(vec![
                                "partition_column_new".to_string(),
                            ])),
                        ),
                    ],
                    requires_full_refresh,
                ),
                requires_full_refresh: true,
            },
            TestCase {
                description: "changing a materialized view's refresh cron or tags should not trigger a full refresh",
                relation_loader: new_loader(),
                current_state: TestModelConfig {
                    cron: Some("* * * * *".to_string()),
                    time_zone: Some("UTC".to_string()),
                    tags: IndexMap::from_iter([
                        ("a_tag".to_string(), "old".to_string()),
                        ("b_tag".to_string(), "old".to_string()),
                    ]),
                    ..Default::default()
                },
                desired_state: TestModelConfig {
                    cron: Some("*/60 * * * *".to_string()),
                    time_zone: Some("UTC".to_string()),
                    tags: IndexMap::from_iter([("a_tag".to_string(), "new".to_string())]),
                    ..Default::default()
                },
                expected_changeset: RelationComponentConfigChangeSet::new(
                    [
                        (
                            components::RefreshLoader::type_name(),
                            ComponentConfigChange::Some(components::RefreshLoader::new(
                                Some("*/60 * * * *".to_string()),
                                Some("UTC".to_string()),
                            )),
                        ),
                        // TODO: re-add tags
                        // (
                        //     components::RelationTagsLoader::type_name(),
                        //     ComponentConfigChange::Some(components::RelationTagsLoader::new(
                        //         IndexMap::from_iter([("a_tag".to_string(), "new".to_string())]),
                        //     )),
                        // ),
                    ],
                    requires_full_refresh,
                ),
                requires_full_refresh: false,
            },
        ]
    }

    #[test]
    fn test_cases() {
        run_test_cases(create_test_cases());
    }
}
