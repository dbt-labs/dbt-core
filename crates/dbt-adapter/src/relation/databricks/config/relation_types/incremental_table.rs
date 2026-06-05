//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/incremental.py

use crate::AdapterType;
use crate::relation::config_v2::ComponentConfigChange;
use crate::relation::config_v2::{ComponentConfigLoader, RelationConfigLoader};
use crate::relation::databricks::config::{DatabricksRelationMetadata, components};
use indexmap::IndexMap;

fn requires_full_refresh(components: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
    super::requires_full_refresh(super::MaterializationType::IncrementalTable, components)
}

/// Create a `RelationConfigLoader` for Databricks incremental tables
pub(crate) fn new_loader() -> RelationConfigLoader<'static, DatabricksRelationMetadata> {
    // TODO: missing from Python dbt-databricks:
    // - liquid clustering
    let loaders: [Box<dyn ComponentConfigLoader<DatabricksRelationMetadata>>; 7] = [
        // TODO: column mask
        Box::new(components::ColumnCommentsLoader),
        Box::new(components::ColumnTagsLoader),
        Box::new(components::RelationCommentLoader),
        Box::new(components::ConstraintsLoader),
        // Box::new(components::LiquidClusteringLoader),
        Box::new(components::RelationTagsLoader),
        Box::new(components::TblPropertiesLoader),
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
        test_helpers::{TestModelColumn, TestModelConfig, run_test_cases},
    };
    use crate::relation::test_helpers::TestCase;
    use dbt_schemas::schemas::common::{Constraint, ConstraintType};
    use dbt_schemas::schemas::dbt_column::ColumnMask;
    use indexmap::{IndexMap, IndexSet};

    fn create_test_cases() -> Vec<TestCase<DatabricksRelationMetadata, TestModelConfig>> {
        vec![TestCase {
            description: "changing any incremental table components should not trigger a full refresh",
            relation_loader: new_loader(),
            current_state: TestModelConfig {
                persist_relation_comments: true,
                persist_column_comments: true,
                relation_comment: Some("old comment".to_string()),
                cluster_by: vec!["cluster_by_old".to_string()],
                columns: vec![
                    TestModelColumn {
                        name: "a_column".to_string(),
                        comment: Some("old comment".to_string()),
                        ..Default::default()
                    },
                    TestModelColumn {
                        name: "b_column".to_string(),
                        comment: Some("old comment".to_string()),
                        tags: IndexMap::from_iter([("col_tag".to_string(), "old".to_string())]),
                        constraints: vec![Constraint {
                            type_: ConstraintType::NotNull,
                            ..Default::default()
                        }],
                        column_mask: Some(ColumnMask {
                            function: "function".to_string(),
                            using_columns: None,
                        }),
                    },
                ],
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
                persist_column_comments: true,
                relation_comment: Some("new comment".to_string()),
                cluster_by: vec!["cluster_by_new".to_string()],
                columns: vec![
                    TestModelColumn {
                        name: "a_column".to_string(),
                        comment: Some("new comment".to_string()),
                        constraints: vec![Constraint {
                            type_: ConstraintType::NotNull,
                            ..Default::default()
                        }],
                        column_mask: Some(ColumnMask {
                            function: "other function".to_string(),
                            using_columns: None,
                        }),
                        ..Default::default()
                    },
                    TestModelColumn {
                        name: "b_column".to_string(),
                        comment: Some("old comment".to_string()),
                        tags: IndexMap::from_iter([("col_tag".to_string(), "new".to_string())]),
                        constraints: Vec::new(),
                        ..Default::default()
                    },
                ],
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
                AdapterType::Databricks,
                [
                    // TODO: add liquid clustering to changeset here once that gets implemented
                    (
                        components::ColumnCommentsLoader.type_name(),
                        ComponentConfigChange::Some(
                            components::ColumnCommentsLoader::new_component_type_erased(
                                IndexMap::from_iter([(
                                    "`a_column`".to_string(),
                                    "new comment".to_string(),
                                )]),
                            ),
                        ),
                    ),
                    (
                        components::ColumnTagsLoader.type_name(),
                        ComponentConfigChange::Some(
                            components::ColumnTagsLoader::new_component_type_erased(
                                IndexMap::from_iter([(
                                    "b_column".to_string(),
                                    IndexMap::from_iter([(
                                        "col_tag".to_string(),
                                        "new".to_string(),
                                    )]),
                                )]),
                            ),
                        ),
                    ),
                    (
                        components::ConstraintsLoader.type_name(),
                        ComponentConfigChange::Some(
                            components::ConstraintsLoader::new_component_type_erased(
                                // set non-nulls
                                IndexSet::from_iter(["a_column".to_string()]),
                                // unset non-nulls
                                IndexSet::from_iter(["b_column".to_string()]),
                                IndexSet::new(),
                                IndexSet::new(),
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
                        components::TblPropertiesLoader.type_name(),
                        ComponentConfigChange::Some(
                            components::TblPropertiesLoader::new_component_type_erased(
                                IndexMap::from_iter([
                                    ("customKey".to_string(), "new".to_string()),
                                    ("customKey2".to_string(), "value".to_string()),
                                ]),
                            ),
                        ),
                    ),
                    (
                        components::ColumnMasksLoader.type_name(),
                        ComponentConfigChange::Some(
                            components::ColumnMasksLoader::new_component_type_erased(
                                IndexMap::from_iter([(
                                    "a_column".to_string(),
                                    ColumnMask {
                                        function: "other function".to_string(),
                                        using_columns: None,
                                    },
                                )]),
                                vec!["b_column".to_string()],
                            ),
                        ),
                    ),
                ],
                requires_full_refresh,
            ),
            changeset_jinja: "
<column_comments>
    <comments>
        <a_column>
            new comment
        </a_column>
    </comments>
    <persist>
        True
    </persist>
</column_comments>
<column_tags>
    <tags>
        <b_column>
            <col_tag>
                new
            </col_tag>
        </b_column>
    </tags>
</column_tags>
<comment>
    <comment>
        new comment
    </comment>
    <persist>
        True
    </persist>
</comment>
<constraints>
    <set_non_nulls>
        a_column
    </set_non_nulls>
    <unset_non_nulls>
        b_column
    </unset_non_nulls>
    <set_constraints>
    </set_constraints>
    <unset_constraints>
    </unset_constraints>
</constraints>
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
<tblproperties>
    <tblproperties>
        <customKey>
            new
        </customKey>
        <customKey2>
            value
        </customKey2>
        <delta.enableRowTracking>
            true
        </delta.enableRowTracking>
    </tblproperties>
    <pipeline_id>
        my_new_pipeline
    </pipeline_id>
</tblproperties>
<column_masks>
    <set_column_masks>
        <a_column>
            <function>
                other function
            </function>
            <using_columns>
                None
            </using_columns>
        </a_column>
    </set_column_masks>
    <unset_column_masks>
        b_column
    </unset_column_masks>
</column_masks>
                ",
            requires_full_refresh: false,
        }]
    }

    #[test]
    fn test_cases() {
        run_test_cases(create_test_cases());
    }
}
