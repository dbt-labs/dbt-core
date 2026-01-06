// TODO(serramatutu): revisit this expect dead_code once all this code is being used for real
#[expect(dead_code)]
pub(crate) mod components;
pub(crate) mod relation_types;

#[cfg(test)]
pub(crate) mod test_helpers;

use dbt_agate::AgateTable;
use indexmap::IndexMap;

// TODO: delete this and make it use RecordBatches instead
#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) enum DatabricksRelationMetadataKey {
    InfoSchemaViews,
    InfoSchemaRelationTags,
    InfoSchemaColumnTags,
    DescribeExtended,
    ShowTblProperties,
    ColumnMasks,
    PrimaryKeyConstraints,
    ForeignKeyConstraints,
    NonNullConstraints,
}

// string conversions based on string keys from:
// https://github.com/databricks/dbt-databricks/blob/9e2566fdb56318cb7a59a4492f96c7aaa7af73b0/dbt/adapters/databricks/impl.py#L914-L1021
impl From<DatabricksRelationMetadataKey> for String {
    fn from(key: DatabricksRelationMetadataKey) -> Self {
        match key {
            DatabricksRelationMetadataKey::InfoSchemaViews => {
                "information_schema.views".to_string()
            }
            DatabricksRelationMetadataKey::InfoSchemaRelationTags => {
                "information_schema.tags".to_string()
            }
            DatabricksRelationMetadataKey::InfoSchemaColumnTags => {
                "information_schema.column_tags".to_string()
            }
            DatabricksRelationMetadataKey::DescribeExtended => "describe_extended".to_string(),
            DatabricksRelationMetadataKey::ShowTblProperties => "show_tblproperties".to_string(),
            DatabricksRelationMetadataKey::ColumnMasks => "column_masks".to_string(),
            DatabricksRelationMetadataKey::PrimaryKeyConstraints => {
                "primary_key_constraints".to_string()
            }
            DatabricksRelationMetadataKey::ForeignKeyConstraints => {
                "foreign_key_constraints".to_string()
            }
            DatabricksRelationMetadataKey::NonNullConstraints => {
                "non_null_constraint_columns".to_string()
            }
        }
    }
}

pub(crate) type DatabricksRelationMetadata = IndexMap<DatabricksRelationMetadataKey, AgateTable>;
