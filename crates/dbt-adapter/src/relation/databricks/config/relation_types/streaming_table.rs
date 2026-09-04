//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/streaming_table.py

use crate::AdapterType;
use crate::relation::config_v2::ComponentConfigChange;
use crate::relation::config_v2::{ComponentConfigLoader, RelationConfigLoader};
use crate::relation::databricks::config::{DatabricksRelationMetadata, components};
use indexmap::IndexMap;

fn requires_full_refresh(components: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
    super::requires_full_refresh(super::MaterializationType::StreamingTable, components)
}

/// Create a `RelationConfigLoader` for Databricks streaming tables
pub(crate) fn new_loader() -> RelationConfigLoader<'static, DatabricksRelationMetadata> {
    // TODO: query (#16138)
    let loaders: [Box<dyn ComponentConfigLoader<DatabricksRelationMetadata>>; 8] = [
        Box::new(components::LiquidClusteringLoader),
        Box::new(components::PartitionByLoader),
        Box::new(components::RelationCommentLoader),
        Box::new(components::TblPropertiesLoader),
        Box::new(components::RefreshLoader),
        Box::new(components::RelationTagsLoader),
        Box::new(components::RowFilterLoader),
        Box::new(components::ColumnMasksLoader),
    ];

    RelationConfigLoader::new(AdapterType::Databricks, loaders, requires_full_refresh)
}

#[cfg(test)]
mod tests {
    use super::{new_loader, requires_full_refresh};
    use crate::AdapterType;
    use crate::relation::config_v2::{
        ComponentConfigChange, ComponentConfigLoader, RelationComponentConfigChangeSet,
    };
    use crate::relation::databricks::config::{
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
                    row_filter_function: Some("row_filter_fn".to_string()),
                    row_filter_columns: vec!["col1".to_string()],
                    tags: IndexMap::from_iter([
                        ("a_tag".to_string(), "old".to_string()),
                        ("b_tag".to_string(), "old".to_string()),
                    ]),
                    tbl_properties: IndexMap::from_iter([
                        (
                            "pipelines.pipelineId".to_string(),
                            "dlt-pipeline-1".to_string(),
                        ),
                        ("data.quality".to_string(), "bronze".to_string()),
                        ("source.system".to_string(), "events-v1".to_string()),
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
                    row_filter_function: None,
                    row_filter_columns: vec![],
                    tbl_properties: IndexMap::from_iter([
                        (
                            "pipelines.pipelineId".to_string(),
                            "dlt-pipeline-1".to_string(),
                        ),
                        ("data.quality".to_string(), "silver".to_string()),
                        ("source.system".to_string(), "events-v2".to_string()),
                    ]),
                    ..Default::default()
                },
                expected_changeset: RelationComponentConfigChangeSet::new(
                    AdapterType::Databricks,
                    [
                        (
                            components::LiquidClusteringLoader.type_name(),
                            ComponentConfigChange::Some(
                                components::LiquidClusteringLoader::new_component_type_erased(
                                    false,
                                    vec!["cluster_by_new".to_string()],
                                ),
                            ),
                        ),
                        (
                            components::RefreshLoader.type_name(),
                            ComponentConfigChange::Some(
                                components::RefreshLoader::new_component_type_erased(
                                    Some("*/60 * * * *".to_string()),
                                    Some("UTC".to_string()),
                                ),
                            ),
                        ),
                        (
                            components::RelationCommentLoader.type_name(),
                            ComponentConfigChange::Some(
                                components::RelationCommentLoader::new_component_type_erased(Some(
                                    "new comment".to_string(),
                                )),
                            ),
                        ),
                        (
                            components::RelationTagsLoader.type_name(),
                            ComponentConfigChange::Some(
                                components::RelationTagsLoader::new_component_type_erased(
                                    IndexMap::from_iter([
                                        ("a_tag".to_string(), "new".to_string()),
                                        ("b_tag".to_string(), "old".to_string()),
                                    ]),
                                ),
                            ),
                        ),
                        (
                            components::RowFilterLoader.type_name(),
                            ComponentConfigChange::Some(
                                components::RowFilterLoader::new_component_type_erased(
                                    None,
                                    vec![],
                                ),
                            ),
                        ),
                        (
                            components::TblPropertiesLoader.type_name(),
                            ComponentConfigChange::Some(
                                components::TblPropertiesLoader::new_component_type_erased(
                                    IndexMap::from_iter([
                                        (
                                            "pipelines.pipelineId".to_string(),
                                            "dlt-pipeline-1".to_string(),
                                        ),
                                        ("data.quality".to_string(), "silver".to_string()),
                                        ("source.system".to_string(), "events-v2".to_string()),
                                    ]),
                                ),
                            ),
                        ),
                    ],
                    requires_full_refresh,
                ),
                changeset_jinja: "
<liquid_clustering>
    <auto_cluster>
        False
    </auto_cluster>
    <cluster_by>
        cluster_by_new
    </cluster_by>
</liquid_clustering>
<comment>
    <comment>
        new comment
    </comment>
    <persist>
        True
    </persist>
</comment>
<tblproperties>
    <tblproperties>
        <data.quality>
            silver
        </data.quality>
        <source.system>
            events-v2
        </source.system>
    </tblproperties>
    <pipeline_id>
        dlt-pipeline-1
    </pipeline_id>
</tblproperties>
<refresh>
    <cron>
        */60 * * * *
    </cron>
    <time_zone_value>
        UTC
    </time_zone_value>
    <is_altered>
        True
    </is_altered>
</refresh>
<tags>
    <set_tags>
        <a_tag>
            new
        </a_tag>
        <b_tag>
            old
        </b_tag>
    </set_tags>
</tags>
<row_filter>
    <function>
        None
    </function>
    <columns>
    </columns>
    <should_unset>
        True
    </should_unset>
    <is_change>
        True
    </is_change>
</row_filter>
                    ",
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
                    AdapterType::Databricks,
                    [(
                        components::PartitionByLoader.type_name(),
                        ComponentConfigChange::Some(
                            components::PartitionByLoader::new_component_type_erased(vec![
                                "partition_by_new".to_string(),
                            ]),
                        ),
                    )],
                    requires_full_refresh,
                ),
                changeset_jinja: "
<partitioned_by>
    <partition_by>
        partition_by_new
    </partition_by>
</partitioned_by>
                    ",
                requires_full_refresh: true,
            },
        ]
    }

    #[test]
    fn test_cases() {
        run_test_cases(create_test_cases());
    }
}
