use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME;
use dbt_common::string_utils::maybe_truncate_test_name;
use dbt_common::{CodeLocationWithFile, FsResult, stdfs};
use dbt_schemas::schemas::InternalDbtNode;
use dbt_schemas::schemas::Nodes;
use dbt_schemas::schemas::common::{DbtChecksum, Severity};
use dbt_schemas::schemas::nodes::{DbtTest, TestMetadata};
use dbt_schemas::schemas::profiles::Execute;

// NOTE: This module intentionally mirrors the legacy resolve-phase aggregation logic
// but runs at task time and only considers selected, enabled generic column tests.

#[derive(Debug, Clone)]
pub struct GenericTest {
    pub unique_id: String,
    pub schema: String,
    pub alias: String,
    pub column_name: String,
    pub severity: Option<Severity>,
    pub defined_at: Option<CodeLocationWithFile>,
}

#[derive(Debug, Clone)]
pub struct GenericTestGroup {
    pub unique_id: String,
    pub name: String,
    pub aggregated_test: Arc<DbtTest>,
    pub member_tests: Vec<Arc<DbtTest>>,
    pub tests: Vec<GenericTest>,
}

#[derive(Debug, Clone, Default)]
pub struct GenericTestRelationships {
    // Map from test unique ID to test group name
    pub group_names: HashMap<String, String>,
    // Map from test group name to list of test unique IDs
    pub unique_ids: HashMap<String, Vec<String>>,
    // Map from test group name and normalized column name to GenericTest
    pub tests: HashMap<String, HashMap<String, GenericTest>>,
}

#[derive(Debug, Default, Clone)]
pub struct GenericTestAggregation {
    pub groups: HashMap<String, Arc<GenericTestGroup>>,
    pub group_ids: HashMap<String, String>,
    pub relationships: GenericTestRelationships,
}

fn is_aggregatable_test(test: &DbtTest) -> bool {
    let Some(macro_name) = get_macro_name(test) else {
        return false;
    };

    let config = &test.deprecated_config;
    let enabled = config.enabled.is_none_or(|enabled| enabled);
    let eligible = matches!(macro_name.as_str(), "unique" | "not_null");
    let safe = config.fail_calc.is_none()
        && config.limit.is_none()
        && config.severity.is_none()
        && config.error_if.is_none()
        && config.warn_if.is_none()
        && config.store_failures.is_none()
        && config.store_failures_as.is_none()
        && config.where_.is_none();

    eligible && enabled && safe
}

fn get_test_group_key(test: &DbtTest) -> Option<(String, String)> {
    if !is_aggregatable_test(test) {
        return None;
    }

    let resource_name = test.__test_attr__.attached_node.clone()?;
    let macro_name = get_macro_name(test)?;
    Some((resource_name, macro_name))
}

/// Generates test group name using the same conventions as persist_generic_data_tests.
fn get_group_name(
    macro_name: &str,
    resource_name_display: &str,
    column_names: &[String],
) -> String {
    use regex::Regex;
    use std::sync::LazyLock;

    static CLEAN_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"[^0-9a-zA-Z_]+").expect("valid regex"));

    let test_identifier = format!("{macro_name}_{resource_name_display}");

    // Clean column names and join them
    let clean_columns: Vec<String> = column_names
        .iter()
        .map(|col| {
            CLEAN_REGEX
                .replace_all(col.trim_matches('"'), "_")
                .to_string()
        })
        .collect();

    let suffix = clean_columns.join("__");

    maybe_truncate_test_name(&test_identifier, &format!("{test_identifier}_{suffix}"))
}

fn get_macro_name(test: &DbtTest) -> Option<String> {
    let metadata = test.__test_attr__.test_metadata.as_ref()?;
    let macro_name = metadata.name.clone();
    Some(macro_name)
}

fn get_column_name(test: &DbtTest) -> Option<String> {
    let metadata = test.__test_attr__.test_metadata.as_ref()?;
    let column_name = metadata
        .kwargs
        .get("column_name")
        .and_then(|v| v.as_str())?
        .to_string();
    Some(column_name)
}

/// Normalize column name the same way resolve phase did.
pub fn normalize_column_name(column_name: &str) -> String {
    column_name.trim_matches('"').to_ascii_lowercase()
}

fn create_generic_test_relationships(
    test_groups: &HashMap<String, Arc<GenericTestGroup>>,
) -> GenericTestRelationships {
    let mut relationships = GenericTestRelationships::default();

    for test_group in test_groups.values() {
        let group_name = test_group.name.clone();

        relationships.unique_ids.insert(
            group_name.clone(),
            test_group
                .tests
                .iter()
                .map(|m| m.unique_id.clone())
                .collect(),
        );

        for test in &test_group.tests {
            relationships
                .group_names
                .insert(test.unique_id.clone(), group_name.clone());

            let column_name = normalize_column_name(&test.column_name);
            relationships
                .tests
                .entry(group_name.clone())
                .or_default()
                .insert(column_name, test.clone());
        }
    }

    relationships
}

/// Create test aggregation from resolved nodes and the current schedule.
///
/// Only selected, enabled generic column tests are considered.
pub fn create_generic_test_aggregation(
    io: &dbt_common::io_args::IoArgs,
    schedule: &dbt_dag::schedule::Schedule<String>,
    nodes: &Nodes,
    execute: Execute,
) -> FsResult<Option<GenericTestAggregation>> {
    if execute != Execute::Remote {
        return Ok(None);
    }

    // Collect eligible tests keyed by (attached_node, macro_name)
    let mut grouped_tests: HashMap<(String, String), Vec<Arc<DbtTest>>> = HashMap::new();

    for unique_id in &schedule.selected_nodes {
        let Some(test) = nodes.tests.get(unique_id) else {
            continue;
        };
        let Some((resource_name, macro_name)) = get_test_group_key(test) else {
            continue;
        };
        grouped_tests
            .entry((resource_name, macro_name))
            .or_default()
            .push(test.clone());
    }

    let mut groups: HashMap<String, Arc<GenericTestGroup>> = HashMap::new();
    let mut group_ids = HashMap::new();

    for ((resource_name, macro_name), member_tests) in grouped_tests {
        // Too few to aggregate
        if member_tests.len() < 2 {
            continue;
        }

        let mut tests = Vec::with_capacity(member_tests.len());
        let mut column_names = Vec::new();

        for test in &member_tests {
            let column_name = get_column_name(test.as_ref()).expect("checked");
            column_names.push(column_name.clone());

            let test = GenericTest {
                unique_id: test.common().unique_id.clone(),
                schema: test.base().schema.clone(),
                alias: test.base().alias.clone(),
                column_name: column_name.clone(),
                severity: test.deprecated_config.severity.clone(),
                defined_at: test.defined_at.clone(),
            };
            tests.push(test);
        }

        let group_name = get_group_name(
            &format!("aggregated_{macro_name}"),
            &resource_name.replace('.', "_"),
            &column_names,
        );
        let group_id = format!(
            "test.{}.{}",
            member_tests[0].common().package_name,
            group_name
        );

        // Synthesize (aggregated) group test node
        let aggregated_test =
            create_aggregated_test(&group_name, &group_id, &member_tests[0], &column_names, io)?;

        let group = GenericTestGroup {
            unique_id: group_id.clone(),
            name: group_name.clone(),
            aggregated_test: Arc::new(aggregated_test),
            tests: tests.clone(),
            member_tests: member_tests.clone(),
        };

        for test in &tests {
            group_ids.insert(test.unique_id.clone(), group_id.clone());
        }

        groups.insert(group_id, Arc::new(group));
    }

    if groups.is_empty() {
        return Ok(None);
    }

    let relationships = create_generic_test_relationships(&groups);

    Ok(Some(GenericTestAggregation {
        groups,
        group_ids,
        relationships,
    }))
}

fn create_aggregated_test(
    test_group_name: &str,
    test_group_id: &str,
    template: &DbtTest,
    columns: &[String],
    io_args: &dbt_common::io_args::IoArgs,
) -> FsResult<DbtTest> {
    let path = PathBuf::from(DBT_GENERIC_TESTS_DIR_NAME).join(format!("{test_group_name}.sql"));
    let absolute_path = io_args.out_dir.join(&path);

    let mut kwargs = BTreeMap::new();
    kwargs.insert(
        "column_names".to_string(),
        dbt_yaml::Value::Sequence(
            columns
                .iter()
                .map(|c| dbt_yaml::Value::string(c.clone()))
                .collect(),
            dbt_yaml::Span::default(),
        ),
    );

    let test_metadata = TestMetadata {
        name: format!(
            "aggregated_{}",
            template
                .__test_attr__
                .test_metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_else(|| "unknown".to_string())
        ),
        kwargs,
        namespace: None,
    };

    let raw_code = format!(
        "{{{{ test_{macro_name}(**_dbt_generic_test_kwargs) }}}}{{{{ config(alias=\"{alias}\") }}}}",
        macro_name = test_metadata.name,
        alias = test_group_name,
    );

    // write SQL to target so render_sql_instruction can read it
    if let Some(parent) = absolute_path.parent() {
        stdfs::create_dir_all(parent)?;
    }
    stdfs::write(&absolute_path, &raw_code)?;

    let mut test = template.clone();

    test.__common_attr__.name = test_group_name.to_string();
    test.__common_attr__.unique_id = test_group_id.to_string();
    test.__common_attr__.path = path;
    test.__common_attr__.original_file_path = absolute_path.clone();
    test.manifest_original_file_path = absolute_path;
    test.__common_attr__.raw_code = Some(raw_code.clone());
    test.__common_attr__.checksum = DbtChecksum::hash(raw_code.trim().as_bytes());
    test.__common_attr__.fqn = vec![
        test.__common_attr__.package_name.clone(),
        "test".to_string(),
        test_group_name.to_string(),
    ];

    // update base attributes
    test.__base_attr__.alias = test_group_name.to_string();
    test.__base_attr__.relation_name = None;
    test.__base_attr__.static_analysis_off_reason = None;

    // metadata
    test.__test_attr__.column_name = None;
    test.__test_attr__.attached_node = test.__test_attr__.attached_node.clone();
    test.__test_attr__.test_metadata = Some(test_metadata);

    Ok(test)
}
