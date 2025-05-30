use std::{collections::BTreeMap, path::PathBuf};

use dbt_schemas::schemas::{
    common::DbtChecksum,
    manifest::{CommonAttributes, DbtConfig, DbtOperation, NodeBaseAttributes},
    project::DbtProject,
};

pub fn resolve_operations(
    database: &str,
    schema: &str,
    dbt_project: &DbtProject,
) -> (Vec<DbtOperation>, Vec<DbtOperation>) {
    let mut on_run_start = Vec::new();
    let mut on_run_end = Vec::new();

    for start in dbt_project.on_run_start.iter() {
        let operations: Vec<String> = start.clone().into();
        on_run_start.extend(new_operation(
            "on_run_start",
            &operations,
            database,
            schema,
            dbt_project,
        ));
    }

    for end in dbt_project.on_run_end.iter() {
        let operations: Vec<String> = end.clone().into();
        on_run_end.extend(new_operation(
            "on_run_end",
            &operations,
            database,
            schema,
            dbt_project,
        ));
    }

    (on_run_start, on_run_end)
}

fn new_operation(
    operation_type: &str,
    operations: &[String],
    database: &str,
    schema: &str,
    dbt_project: &DbtProject,
) -> Vec<DbtOperation> {
    let project_name = &dbt_project.name;
    // Map with index
    operations
        .iter()
        .enumerate()
        .map(|(index, operation_sql)| {
            let name = format!("{}-{}-{}", project_name, operation_type, index);
            let unique_id = format!("operation.{}.{}", project_name, name);
            DbtOperation {
                common_attr: CommonAttributes {
                    database: database.to_string(),
                    schema: schema.to_string(),
                    name: name.clone(),
                    package_name: project_name.to_string(),
                    path: PathBuf::from("hooks").join(&name),
                    original_file_path: PathBuf::from("./dbt_project.yml"),
                    unique_id,
                    fqn: vec![project_name.to_string(), "hooks".to_string(), name.clone()],
                    ..Default::default()
                },
                base_attr: NodeBaseAttributes {
                    alias: name,
                    checksum: DbtChecksum::hash(operation_sql),
                    raw_code: Some(operation_sql.to_string()),
                    language: Some("sql".to_string()),
                    ..Default::default()
                },
                config: DbtConfig {
                    enabled: Some(true),
                    tags: vec![operation_type.to_string()].into(),
                    ..Default::default()
                },
                other: BTreeMap::new(),
            }
        })
        .collect()
}
