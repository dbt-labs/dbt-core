use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::io_args::IoArgs;
use dbt_common::{err, fs_err, stdfs};
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_schemas::schemas::macros::DbtDocsMacro;
use dbt_schemas::schemas::macros::DbtMacro;
use dbt_schemas::schemas::macros::MacroDependsOn;
use dbt_schemas::state::DbtAsset;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;

use crate::utils::parse_macro_statements;

/// Resolve docs macros from a list of docs macro files
pub fn resolve_docs_macros(
    docs_macro_files: &[DbtAsset],
) -> FsResult<BTreeMap<String, DbtDocsMacro>> {
    let mut docs_map: BTreeMap<String, DbtDocsMacro> = BTreeMap::new();

    for DbtAsset {
        path: docs_file,
        base_path,
        package_name,
    } in docs_macro_files
    {
        let docs_macro = fs::read_to_string(base_path.join(docs_file)).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to read docs file '{}': {}",
                docs_file.display(),
                e
            )
        })?;

        let resources = parse_macro_statements(&docs_macro, docs_file, &["docs"]);
        match resources {
            Ok(resources) => {
                if resources.is_empty() {
                    continue;
                }
                for resource in resources {
                    match resource {
                        SqlResource::Doc(name, span) => {
                            let unique_id = format!("doc.{package_name}.{name}");
                            let part =
                                &docs_macro[span.start_offset as usize..span.end_offset as usize];
                            if let Some(existing_doc) = docs_map.get(&unique_id) {
                                return err!(
                                    ErrorCode::Unexpected,
                                    "dbt found two docs with the same name: '{}' in files: '{}' and '{}'",
                                    name,
                                    docs_file.display(),
                                    existing_doc.path.display()
                                );
                            }
                            docs_map.insert(
                                unique_id.clone(),
                                DbtDocsMacro {
                                    name: name.clone(),
                                    package_name: package_name.clone(),
                                    path: docs_file.clone(),
                                    original_file_path: docs_file.clone(),
                                    unique_id,
                                    block_contents: part.to_string(),
                                },
                            );
                        }
                        _ => {
                            return err!(
                                ErrorCode::Unexpected,
                                "Encountered unexpected resource in docs file: {}",
                                docs_file.display()
                            );
                        }
                    }
                }
            }
            Err(err) => return Err(Box::new(err.with_location(docs_file.clone()))),
        }
    }

    Ok(docs_map)
}

/// Resolve macros from a list of macro files
pub fn resolve_macros(
    io: &IoArgs,
    macro_files: &[&DbtAsset],
) -> FsResult<HashMap<String, DbtMacro>> {
    let mut nodes = HashMap::new();

    for dbt_asset in macro_files {
        let DbtAsset {
            path: macro_file,
            base_path,
            package_name,
        } = dbt_asset;
        if macro_file.extension() == Some(OsStr::new("jinja"))
            || macro_file.extension() == Some(OsStr::new("sql"))
        {
            let macro_file_path = base_path.join(macro_file);
            let macro_sql = fs::read_to_string(&macro_file_path).map_err(|e| {
                fs_err!(
                    code => ErrorCode::IoError,
                    loc => macro_file_path.to_path_buf(),
                    "Failed to read macro file: {}", e
                )
            })?;
            let relative_macro_file_path = stdfs::diff_paths(&macro_file_path, &io.in_dir)?;
            let resources = parse_macro_statements(
                &macro_sql,
                &relative_macro_file_path,
                &["macro", "test", "materialization", "snapshot"],
            )?;

            if resources.is_empty() {
                continue;
            }

            for resource in resources {
                match resource {
                    SqlResource::Test(name, span) => {
                        let unique_id = format!("macro.{package_name}.{name}");
                        let split_macro_sql =
                            &macro_sql[span.start_offset as usize..span.end_offset as usize];

                        let dbt_macro = DbtMacro {
                            name: name.clone(),
                            package_name: package_name.clone(),
                            path: macro_file.clone(),
                            original_file_path: relative_macro_file_path.clone(),
                            span: Some(span),
                            unique_id: unique_id.clone(),
                            macro_sql: split_macro_sql.to_string(),
                            depends_on: MacroDependsOn { macros: vec![] }, // Populate as needed
                            description: String::new(),                    // Populate as needed
                            meta: BTreeMap::new(),                         // Populate as needed
                            patch_path: None,
                            funcsign: None,
                            args: vec![],
                            other: BTreeMap::new(),
                        };

                        nodes.insert(unique_id, dbt_macro);
                    }
                    SqlResource::Macro(name, span, func_sign, args) => {
                        let unique_id = format!("macro.{package_name}.{name}");
                        let split_macro_sql =
                            &macro_sql[span.start_offset as usize..span.end_offset as usize];

                        let dbt_macro = DbtMacro {
                            name: name.clone(),
                            package_name: package_name.clone(),
                            path: macro_file.clone(),
                            original_file_path: relative_macro_file_path.clone(),
                            span: Some(span),
                            unique_id: unique_id.clone(),
                            macro_sql: split_macro_sql.to_string(),
                            depends_on: MacroDependsOn { macros: vec![] }, // Populate as needed
                            description: String::new(),                    // Populate as needed
                            meta: BTreeMap::new(),                         // Populate as needed
                            patch_path: None,
                            funcsign: func_sign.clone(),
                            args: args.clone(),
                            other: BTreeMap::new(),
                        };

                        nodes.insert(unique_id, dbt_macro);
                    }
                    SqlResource::Materialization(name, _, span) => {
                        let split_macro_sql =
                            &macro_sql[span.start_offset as usize..span.end_offset as usize];
                        // TODO: Return the adapter type with the SqlResource (for now, default always)
                        let unique_id = format!("macro.{package_name}.{name}");
                        let dbt_macro = DbtMacro {
                            name: name.clone(),
                            package_name: package_name.clone(),
                            path: macro_file.clone(),
                            original_file_path: relative_macro_file_path.clone(),
                            span: Some(span),
                            unique_id: unique_id.clone(),
                            macro_sql: split_macro_sql.to_string(),
                            depends_on: MacroDependsOn { macros: vec![] },
                            description: String::new(),
                            meta: BTreeMap::new(),
                            patch_path: None,
                            funcsign: None,
                            args: vec![],
                            other: BTreeMap::new(),
                        };

                        nodes.insert(unique_id, dbt_macro);
                    }
                    SqlResource::Snapshot(name, span) => {
                        let unique_id = format!("snapshot.{package_name}.{name}");
                        let split_macro_sql =
                            &macro_sql[span.start_offset as usize..span.end_offset as usize];

                        let dbt_macro = DbtMacro {
                            name: name.clone(),
                            package_name: package_name.clone(),
                            path: macro_file.clone(),
                            original_file_path: relative_macro_file_path.clone(),
                            span: Some(span),
                            unique_id: unique_id.clone(),
                            macro_sql: split_macro_sql.to_string(),
                            depends_on: MacroDependsOn { macros: vec![] }, // Populate as needed
                            description: String::new(),                    // Populate as needed
                            meta: BTreeMap::new(),                         // Populate as needed
                            patch_path: None,
                            funcsign: None,
                            args: vec![],
                            other: BTreeMap::new(),
                        };

                        nodes.insert(unique_id, dbt_macro);
                    }
                    _ => {
                        return err!(
                            ErrorCode::MacroSyntaxError,
                            "Refs, sources, configs and other resources are not allowed in macros. Path: {}",
                            macro_file.display()
                        );
                    }
                }
            }
        }
    }

    Ok(nodes)
}
