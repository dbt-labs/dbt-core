//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/streaming_table.py

use crate::relation::config_v2::ComponentConfigChange;
use crate::relation::config_v2::{ComponentConfigLoader, RelationConfigLoader};
use crate::relation::databricks::config_v2::{DatabricksRelationMetadata, components};
use indexmap::IndexMap;

fn requires_full_refresh(components: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
    super::requires_full_refresh(super::MaterializationType::StreamingTable, components)
}

/// Create a `RelationConfigLoader` for Databricks streaming tables
pub(crate) fn new_loader() -> RelationConfigLoader<DatabricksRelationMetadata> {
    // TODO: missing from Python dbt-databricks:
    // - liquid clustering
    // - relation tags
    let loaders: [Box<dyn ComponentConfigLoader<DatabricksRelationMetadata>>; 4] = [
        // Box::new(components::LiquidClusteringLoader),
        Box::new(components::PartitionByLoader),
        Box::new(components::RelationCommentLoader),
        Box::new(components::TblPropertiesLoader),
        Box::new(components::RefreshLoader),
        // Box::new(components::RelationTagsLoader),
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
                description: "changing any streaming table components except partition by should not trigger a full refresh",
                relation_loader: new_loader(),
                current_state: TestModelConfig {
                    persist_relation_comments: true,
                    relation_comment: Some("old comment".to_string()),
                    cluster_by: vec!["cluster_by_old".to_string()],
                    cron: Some("* * * * *".to_string()),
                    time_zone: Some("UTC".to_string()),
                    tags: IndexMap::from_iter([
                        ("a_tag".to_string(), "old".to_string()),
                        ("b_tag".to_string(), "old".to_string()),
                    ]),
                    tbl_properties: IndexMap::from_iter([
                        ("delta.enableRowTracking".to_string(), "false".to_string()),
                        (
                            "pipelines.pipelineId".to_string(),
                            "my_old_pipeline".to_string(),
                        ),
                        ("customKey".to_string(), "old".to_string()),
                    ]),
                    ..Default::default()
                },
                desired_state: TestModelConfig {
                    persist_relation_comments: true,
                    relation_comment: Some("new comment".to_string()),
                    cluster_by: vec!["cluster_by_new".to_string()],
                    cron: Some("*/60 * * * *".to_string()),
                    time_zone: Some("UTC".to_string()),
                    tags: IndexMap::from_iter([
                        ("a_tag".to_string(), "new".to_string()),
                        ("b_tag".to_string(), "old".to_string()),
                    ]),
                    tbl_properties: IndexMap::from_iter([
                        // changing these key should not result in anything as these should be ignored
                        ("delta.enableRowTracking".to_string(), "true".to_string()),
                        (
                            "pipelines.pipelineId".to_string(),
                            "my_new_pipeline".to_string(),
                        ),
                        // changing a key not in the ignore list should cause a changeset entry
                        ("customKey".to_string(), "new".to_string()),
                        // introducing a new key should also add it to the changeset
                        ("customKey2".to_string(), "value".to_string()),
                    ]),
                    ..Default::default()
                },
                expected_changeset: RelationComponentConfigChangeSet::new(
                    [
                        // TODO: add liquid clustering to changeset here once that gets implemented
                        (
                            components::RefreshLoader::type_name(),
                            ComponentConfigChange::Some(components::RefreshLoader::new(
                                Some("*/60 * * * *".to_string()),
                                Some("UTC".to_string()),
                            )),
                        ),
                        (
                            components::RelationCommentLoader::type_name(),
                            ComponentConfigChange::Some(components::RelationCommentLoader::new(
                                Some("new comment".to_string()),
                            )),
                        ),
                        // TODO: re-add tags
                        // (
                        //     components::RelationTagsLoader::type_name(),
                        //     ComponentConfigChange::Some(components::RelationTagsLoader::new(
                        //         IndexMap::from_iter([
                        //             ("a_tag".to_string(), "new".to_string()),
                        //             ("b_tag".to_string(), "old".to_string()),
                        //         ]),
                        //     )),
                        // ),
                        (
                            components::TblPropertiesLoader::type_name(),
                            ComponentConfigChange::Some(components::TblPropertiesLoader::new(
                                IndexMap::from_iter([
                                    ("customKey".to_string(), "new".to_string()),
                                    ("customKey2".to_string(), "value".to_string()),
                                ]),
                            )),
                        ),
                    ],
                    requires_full_refresh,
                ),
                requires_full_refresh: false,
            },
            TestCase {
                description: "changing streaming table partition by should trigger a full refresh",
                relation_loader: new_loader(),
                current_state: TestModelConfig {
                    partition_by: vec!["partition_by_old".to_string()],
                    ..Default::default()
                },
                desired_state: TestModelConfig {
                    partition_by: vec!["partition_by_new".to_string()],
                    ..Default::default()
                },
                expected_changeset: RelationComponentConfigChangeSet::new(
                    [(
                        components::PartitionByLoader::type_name(),
                        ComponentConfigChange::Some(components::PartitionByLoader::new(vec![
                            "partition_by_new".to_string(),
                        ])),
                    )],
                    requires_full_refresh,
                ),
                requires_full_refresh: true,
            },
        ]
    }

    #[test]
    fn test_cases() {
        run_test_cases(create_test_cases());
    }
}
