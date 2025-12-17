//! Testing utilities for databricks relation configs
use crate::AdapterType;
use crate::relation::config as config_v1;
use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigChange, RelationComponentConfigChangeSet, RelationConfig,
    RelationConfigLoader,
};
use crate::relation::databricks as rc_v1;
use crate::relation::databricks::config_v2::{DatabricksRelationMetadata, components};
use arrow::csv::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use dbt_agate::AgateTable;
use dbt_schemas::schemas::{
    common::*, dbt_column::DbtColumn, manifest::PartitionConfig, nodes::*, project::*,
};
use dbt_serde_yaml::{Spanned, Value as YmlValue};
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct TestModelColumn {
    pub name: String,
    pub comment: Option<String>,
    pub constraints: Vec<Constraint>,
    pub tags: HashMap<String, String>,
}

#[derive(Default)]
pub(crate) struct TestModelConfig {
    // This will be used once we actually implement liquid clustering
    #[expect(dead_code)]
    pub auto_cluster: bool,
    // This will be used once we actually implement liquid clustering
    #[expect(dead_code)]
    pub cluster_by: Vec<String>,
    pub columns: Vec<TestModelColumn>,
    pub cron: Option<String>,
    pub partition_by: Vec<String>,
    pub persist_column_comments: bool,
    pub persist_relation_comments: bool,
    // This will be used once we actually implement query
    #[expect(dead_code)]
    pub query: Option<String>,
    pub relation_comment: Option<String>,
    pub tags: HashMap<String, String>,
    pub tbl_properties: HashMap<String, String>,
    pub table_format: Option<String>,
    pub time_zone: Option<String>,
}

pub(crate) fn create_mock_dbt_model(cfg: TestModelConfig) -> DbtModel {
    let persist_docs = PersistDocsConfig {
        relation: Some(cfg.persist_relation_comments),
        columns: Some(cfg.persist_column_comments),
    };

    let base_attrs = NodeBaseAttributes {
        database: "test_db".to_string(),
        schema: "test_schema".to_string(),
        alias: "test_table".to_string(),
        relation_name: None,
        quoting: dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING,
        quoting_ignore_case: false,
        materialized: DbtMaterialization::Table,
        static_analysis: Spanned::new(dbt_common::io_args::StaticAnalysisKind::On),
        static_analysis_off_reason: None,
        enabled: true,
        extended_model: false,
        persist_docs: Some(persist_docs),
        columns: cfg
            .columns
            .into_iter()
            .map(|c| {
                Arc::new(DbtColumn {
                    name: c.name,
                    description: c.comment,
                    constraints: c.constraints,
                    databricks_tags: Some(
                        c.tags
                            .into_iter()
                            .map(|(k, v)| (k, YmlValue::from(v)))
                            .collect(),
                    ),
                    ..Default::default()
                })
            })
            .collect(),
        refs: vec![],
        sources: vec![],
        functions: vec![],
        metrics: vec![],
        depends_on: NodeDependsOn::default(),
    };

    let wh_config = WarehouseSpecificNodeConfig {
        tblproperties: Some(
            cfg.tbl_properties
                .into_iter()
                .map(|(k, v)| (k, dbt_serde_yaml::Value::from(v)))
                .collect(),
        ),
        partition_by: Some(PartitionConfig::List(cfg.partition_by)),
        schedule: Some(ScheduleConfig {
            cron: cfg.cron,
            time_zone_value: cfg.time_zone,
        }),
        databricks_tags: Some(
            cfg.tags
                .into_iter()
                .map(|(k, v)| (k, YmlValue::from(v)))
                .collect(),
        ),
        ..Default::default()
    };

    let adapter_attr = AdapterAttr::from_config_and_dialect(&wh_config, AdapterType::Databricks);

    DbtModel {
        deprecated_config: ModelConfig {
            table_format: cfg.table_format,
            __warehouse_specific_config__: wh_config,
            ..Default::default()
        },
        __common_attr__: CommonAttributes {
            name: "test_model".to_string(),
            fqn: vec!["test".to_string(), "test_model".to_string()],
            description: cfg.relation_comment,
            ..Default::default()
        },
        __adapter_attr__: adapter_attr,
        __base_attr__: base_attrs,
        ..Default::default()
    }
}

pub(crate) fn create_mock_describe_extended_table<'a>(
    partition_columns: impl IntoIterator<Item = &'a str>,
    rows: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> AgateTable {
    let mut csv_data = "key,value\n".to_string();

    // Add regular table info rows
    csv_data.push_str("Table,test_table\n");
    csv_data.push_str("Owner,test_user\n");

    for (label, value) in rows.into_iter() {
        csv_data.push_str(&format!("{label},{value}\n"))
    }

    // Add partition information section
    let partition_columns_vec = Vec::from_iter(partition_columns);
    if !partition_columns_vec.is_empty() {
        csv_data.push_str("# Partition Information,\n");
        csv_data.push_str("# col_name,data_type\n");
        for col in partition_columns_vec {
            csv_data.push_str(&format!("{col},string\n"));
        }
        csv_data.push_str(",\n");
    }

    // Add remaining info
    csv_data.push_str("# Detailed Table Information,\n");

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]));

    let file = io::Cursor::new(csv_data);
    let mut reader = ReaderBuilder::new(schema)
        .with_header(true)
        .build(file)
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    AgateTable::from_record_batch(Arc::new(batch))
}

// TODO(serramatutu): once we get rid of v1, we should use the `RelationConfig` here directly
// instead of using `vx_relation_loader`
/// A single test case to test whether a relation config
/// of a given type results in the correct changeset
pub(crate) struct TestCase<T: rc_v1::DatabricksRelationConfig> {
    pub description: &'static str,

    pub current_state: TestModelConfig,
    pub desired_state: TestModelConfig,

    pub v1_relation_loader: PhantomData<T>,
    pub v1_errors: Vec<&'static str>,
    pub v2_relation_loader: RelationConfigLoader<DatabricksRelationMetadata>,

    pub expected_changeset: RelationComponentConfigChangeSet,
    pub requires_full_refresh: bool,
}

fn get_v1_component_name(v2_name: &str) -> &str {
    match v2_name {
        "relation_comment" => "comment",
        "relation_tags" => "tags",
        "partition_by" => "partitioned_by",
        "tbl_properties" => "tblproperties",
        _ => v2_name,
    }
}

fn eq_or_err<T: Eq + std::fmt::Debug>(a: T, b: T) -> Option<String> {
    if a != b {
        Some(format!("v1 != v2, (v1={:?}, v2={:?})", a, b))
    } else {
        None
    }
}

/// Compare v1 and v2 ComponentConfig to ensure they are equal
fn components_eq_v1_v2(
    v1: &rc_v1::base::DatabricksComponentConfig,
    v2: &dyn ComponentConfig,
) -> Option<String> {
    match (v1, v2.type_name()) {
        (rc_v1::base::DatabricksComponentConfig::ColumnComments(v1), "column_comments") => {
            let v2 = v2
                .as_any()
                .downcast_ref::<components::ColumnComments>()
                .unwrap();

            let mut v1_comments: Vec<_> = v1.comments.iter().collect();
            v1_comments.sort_by_key(|e| e.0);

            let mut v2_comments: Vec<_> = v2.value.iter().collect();
            v2_comments.sort_by_key(|e| e.0);

            eq_or_err(v1_comments, v2_comments)
        }
        (rc_v1::base::DatabricksComponentConfig::ColumnTags(v1), "column_tags") => {
            let v2 = v2
                .as_any()
                .downcast_ref::<components::ColumnTags>()
                .unwrap();

            let mut v1_tags = Vec::new();
            for (col, tags) in &v1.tags {
                let mut tags_vec: Vec<_> = tags.iter().collect();
                tags_vec.sort_by_key(|e| e.0);
                v1_tags.push((col, tags_vec));
            }
            v1_tags.sort_by_key(|e| e.0);

            let mut v2_tags = Vec::new();
            for (col, tags) in &v2.value {
                let mut tags_vec: Vec<_> = tags.iter().collect();
                tags_vec.sort_by_key(|e| e.0);
                v2_tags.push((col, tags_vec));
            }
            v2_tags.sort_by_key(|e| e.0);

            eq_or_err(v1_tags, v2_tags)
        }
        (rc_v1::base::DatabricksComponentConfig::Comment(v1), "relation_comment") => {
            let v2 = v2
                .as_any()
                .downcast_ref::<components::RelationComment>()
                .unwrap();
            eq_or_err(&v1.comment, &v2.value)
        }
        (rc_v1::base::DatabricksComponentConfig::Constraints(v1), "constraints") => {
            let v2 = v2
                .as_any()
                .downcast_ref::<components::Constraints>()
                .unwrap();

            fn to_vec<'a, T: Ord>(set: impl IntoIterator<Item = &'a T>) -> Vec<&'a T> {
                let mut v = set.into_iter().collect::<Vec<_>>();
                v.sort();
                v
            }

            eq_or_err(to_vec(&v1.set_non_nulls), to_vec(&v2.set_non_nulls))
                .or_else(|| eq_or_err(to_vec(&v1.unset_non_nulls), to_vec(&v2.unset_non_nulls)))
                .or_else(|| eq_or_err(to_vec(&v1.set_constraints), to_vec(&v2.set_constraints)))
                .or_else(|| eq_or_err(to_vec(&v1.unset_constraints), to_vec(&v2.unset_constraints)))
        }
        (rc_v1::base::DatabricksComponentConfig::PartitionedBy(v1), "partition_by") => {
            let v2 = v2
                .as_any()
                .downcast_ref::<components::PartitionBy>()
                .unwrap();
            eq_or_err(&v1.partition_by, &v2.value)
        }
        (rc_v1::base::DatabricksComponentConfig::Query(v1), "query") => {
            let v2 = v2.as_any().downcast_ref::<components::Query>().unwrap();
            eq_or_err(&v1.query, &v2.value)
        }
        (rc_v1::base::DatabricksComponentConfig::Refresh(v1), "refresh") => {
            let v2 = v2.as_any().downcast_ref::<components::Refresh>().unwrap();
            eq_or_err(&v1.cron, &v2.value.cron)
                .or_else(|| eq_or_err(&v1.time_zone_value, &v2.value.time_zone_value))
        }
        (rc_v1::base::DatabricksComponentConfig::Tags(v1), "relation_tags") => {
            let v2 = v2
                .as_any()
                .downcast_ref::<components::RelationTags>()
                .unwrap();

            let mut v1_tags: Vec<_> = v1.set_tags.iter().collect();
            v1_tags.sort_by_key(|e| e.0);

            let mut v2_tags: Vec<_> = v2.value.iter().collect();
            v2_tags.sort_by_key(|e| e.0);

            eq_or_err(v1_tags, v2_tags)
        }
        (rc_v1::base::DatabricksComponentConfig::TblProperties(v1), "tbl_properties") => {
            let v2 = v2
                .as_any()
                .downcast_ref::<components::TblProperties>()
                .unwrap();

            let mut v1_properties: Vec<_> = v1.tblproperties.iter().collect();

            let key = "pipelines.pipelineId".to_string();
            if let Some(pipeline_id) = &v1.pipeline_id {
                v1_properties.push((&key, pipeline_id));
            }
            v1_properties.sort_by_key(|e| e.0);

            let mut v2_properties: Vec<_> = v2.value.iter().collect();
            v2_properties.sort_by_key(|e| e.0);

            eq_or_err(v1_properties, v2_properties)
        }
        (_, _) => Some(format!(
            "mismatched types (v1={:?}, v2={:?})",
            v1,
            v2.type_name()
        )),
    }
}

/// Compare v1 and v2 RelationChangeSet to ensure they are equal (i.e hold the same configs)
fn changesets_eq_v1_v2(
    v1: Option<Arc<dyn config_v1::RelationChangeSet>>,
    v2: &RelationComponentConfigChangeSet,
) -> Vec<String> {
    let mut errors = Vec::new();

    let Some(v1) = v1 else {
        if v2.len() > 0 {
            errors.push("v1 changeset is empty but v2 has changes".to_string());
        }

        return errors;
    };

    if v1.changes().len() != v2.len() {
        errors.push(format!(
            "changeset lengths differ (v1={}, v2={})",
            v1.changes().len(),
            v2.len()
        ));
    }

    if v1.requires_full_refresh() != v2.requires_full_refresh() {
        errors.push(format!(
            "requires_full_refresh mismatch (v1={}, v2={})",
            v1.requires_full_refresh(),
            v2.requires_full_refresh()
        ));
    }

    for (v2_name, v2_change) in v2.iter() {
        let v1_name = get_v1_component_name(v2_name);
        let v1_change = v1.get_change(v1_name);
        let change_error = match (v1_change, v2_change) {
            (None, ComponentConfigChange::None) => None,
            (None, ComponentConfigChange::Drop) => None,
            (
                Some(v1_change),
                ComponentConfigChange::Some(v2_change),
            ) => {
                v1_change
                    .as_any()
                    .downcast_ref::<rc_v1::base::DatabricksComponentConfig>()
                    .map(|v1_change| {
                        components_eq_v1_v2(v1_change, v2_change.as_ref())
                            .map(|err| format!("{v2_name}: {err}"))
                    })
                    .unwrap_or_else(|| Some("Could not downcast v1 change as DatabricksComponentConfig. Something went terribly wrong".to_string()))
            },
            (v1, v2) => Some(format!(
                "{v1_name}: v1 and v2 config change types mismatch (v1={:?}, v2={:?})",
                v1, v2
            )),
        };

        if let Some(change_error) = change_error {
            errors.push(change_error);
        }
    }

    errors
}

fn components_eq(a: &dyn ComponentConfig, b: &dyn ComponentConfig) -> bool {
    let a_diff = a.diff_from(Some(b));
    let b_diff = b.diff_from(Some(a));

    a_diff.is_none() && b_diff.is_none()
}

fn changesets_eq(
    a: &RelationComponentConfigChangeSet,
    b: &RelationComponentConfigChangeSet,
) -> Vec<String> {
    if a.len() != b.len() {
        return vec![format!(
            "changeset lengths differ (expected {}, got {})",
            a.len(),
            b.len()
        )];
    }

    let mut errors = Vec::new();

    for (a_type, a_change) in a.iter() {
        let b_change = b.get(a_type);

        let err = match (a_change, b_change) {
            (
                ComponentConfigChange::Some(a_component),
                ComponentConfigChange::Some(b_component),
            ) => {
                if !components_eq(a_component.as_ref(), b_component.as_ref()) {
                    Some(format!(
                        "diffs mismatch \n     expected: {a_component:?}\n     got     : {b_component:?}"
                    ))
                } else {
                    None
                }
            }
            (ComponentConfigChange::Drop, ComponentConfigChange::Drop) => None,
            (ComponentConfigChange::None, ComponentConfigChange::None) => None,
            (a, b) => Some(format!("config change types mismatch ({a:?}, {b:?})")),
        };

        if let Some(err) = err {
            errors.push(format!("{a_type} :: {err}"));
        }
    }

    debug_assert!(
        !errors.is_empty() || a.requires_full_refresh() == b.requires_full_refresh(),
        "Programming error, this is a bug"
    );

    errors
}

fn run_test_case<T: rc_v1::DatabricksRelationConfig>(tc: TestCase<T>) -> Option<String> {
    let desired_local_config = create_mock_dbt_model(tc.desired_state);
    let current_local_config = create_mock_dbt_model(tc.current_state);

    let v2_desired = tc
        .v2_relation_loader
        .from_local_config(&desired_local_config);
    let v2_current = tc
        .v2_relation_loader
        .from_local_config(&current_local_config);
    let changeset = RelationConfig::diff(&v2_desired, &v2_current);

    let mut errors = changesets_eq(&tc.expected_changeset, &changeset);

    if changeset.requires_full_refresh() != tc.requires_full_refresh {
        errors.push(format!(
            "expected requires_full_refresh={}\n",
            tc.requires_full_refresh
        ));
    }

    let v1_desired = T::new(rc_v1::base::from_relation_config::<T>(&desired_local_config).unwrap());
    let v1_current = T::new(rc_v1::base::from_relation_config::<T>(&current_local_config).unwrap());
    let v1_changeset = v1_desired.get_changeset(Some(&v1_current));

    let mut migration_errors = changesets_eq_v1_v2(v1_changeset, &changeset);
    migration_errors.sort();

    let mut expected_errors = tc.v1_errors.iter();
    let mut current_expected_err = expected_errors.next().copied();

    migration_errors.retain(|err| {
        if let Some(expected_err) = current_expected_err {
            if err.contains(expected_err) {
                current_expected_err = expected_errors.next().copied();
                false
            } else {
                true
            }
        } else {
            true
        }
    });

    for non_used_err in expected_errors {
        migration_errors.push(format!(
            "Expected migration error was not consumed: {non_used_err}"
        ))
    }

    let mut out = String::new();
    out.push_str(tc.description);

    let has_errors = !migration_errors.is_empty() || !errors.is_empty();

    if !migration_errors.is_empty() {
        out.push_str("\nMIGRATION ERRORS\n");
        for err in migration_errors {
            out.push_str("  > ");
            out.push_str(err.as_str());
            out.push('\n');
        }
    }

    if !errors.is_empty() {
        errors.sort();

        out.push_str("\nOVERALL ERRORS\n");
        for err in errors {
            out.push_str("  > ");
            out.push_str(err.as_str());
            out.push('\n');
        }

        out.push_str("EXPECTED CHANGESET\n");
        out.push_str(format!("{:#?}", tc.expected_changeset).as_str());

        out.push_str("\n\nGOT CHANGESET\n");
        out.push_str(format!("{:#?}", changeset).as_str());
    }

    if has_errors { Some(out) } else { None }
}

/// Run all test cases. Panic if any of them fails.
pub(crate) fn run_test_cases<T: rc_v1::DatabricksRelationConfig>(test_cases: Vec<TestCase<T>>) {
    let mut errors = Vec::new();
    for tc in test_cases {
        if let Some(err) = run_test_case(tc) {
            errors.push(err);
        }
    }

    if !errors.is_empty() {
        let err_str = errors.join("\n>>>>> ");
        panic!(">>>>> {err_str}");
    }
}
