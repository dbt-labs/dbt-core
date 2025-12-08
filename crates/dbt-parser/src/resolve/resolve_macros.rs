use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::io_args::IoArgs;
use dbt_common::stdfs::diff_paths;
use dbt_common::tracing::emit::emit_warn_log_from_fs_error;
use dbt_common::{err, fs_err};
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
    io: &IoArgs,
    docs_macro_files: &[DbtAsset],
) -> FsResult<BTreeMap<String, DbtDocsMacro>> {
    let mut docs_map: BTreeMap<String, DbtDocsMacro> = BTreeMap::new();

    for docs_asset in docs_macro_files {
        if let Err(err) = process_docs_macro_file(io, &mut docs_map, docs_asset) {
            let err = err.with_location(docs_asset.path.clone());
            emit_warn_log_from_fs_error(&err, io.status_reporter.as_ref());
        }
    }

    Ok(docs_map)
}

fn process_docs_macro_file(
    io: &IoArgs,
    docs_map: &mut BTreeMap<String, DbtDocsMacro>,
    docs_asset: &DbtAsset,
) -> FsResult<()> {
    let docs_file_path = docs_asset.base_path.join(&docs_asset.path);
    let docs_macro = fs::read_to_string(&docs_file_path).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to read docs file '{}': {}",
            docs_asset.path.display(),
            e
        )
    })?;

    let relative_docs_file_path = &diff_paths(&docs_file_path, &io.in_dir)?;
    let resources = parse_macro_statements(&docs_macro, relative_docs_file_path, &["docs"])?;
    if resources.is_empty() {
        return Ok(());
    }

    let package_name = &docs_asset.package_name;
    for resource in resources {
        match resource {
            SqlResource::Doc(name, span) => {
                let unique_id = format!("doc.{package_name}.{name}");
                let part = &docs_macro[span.start_offset as usize..span.end_offset as usize];
                if let Some(existing_doc) = docs_map.get(&unique_id) {
                    return err!(
                        ErrorCode::Unexpected,
                        "dbt found two docs with the same name: '{}' in files: '{}' and '{}'",
                        name,
                        docs_asset.path.display(),
                        existing_doc.path.display()
                    );
                }
                docs_map.insert(
                    unique_id.clone(),
                    DbtDocsMacro {
                        name: name.clone(),
                        package_name: package_name.to_string(),
                        path: docs_asset.path.clone(),
                        original_file_path: relative_docs_file_path.clone(),
                        unique_id,
                        block_contents: part.to_string(),
                    },
                );
            }
            _ => {
                return err!(
                    ErrorCode::Unexpected,
                    "Encountered unexpected resource in docs file: {}",
                    docs_asset.path.display()
                );
            }
        }
    }

    Ok(())
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
            let relative_macro_file_path = diff_paths(&macro_file_path, &io.in_dir)?;
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
                    SqlResource::Test(name, span, macro_name_span) => {
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
                            macro_name_span: Some(macro_name_span),
                            __other__: BTreeMap::new(),
                        };

                        nodes.insert(unique_id, dbt_macro);
                    }
                    SqlResource::Macro(name, span, func_sign, args, macro_name_span) => {
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
                            macro_name_span: Some(macro_name_span),
                            __other__: BTreeMap::new(),
                        };

                        nodes.insert(unique_id, dbt_macro);
                    }
                    SqlResource::Materialization(name, _, span, macro_name_span) => {
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
                            macro_name_span: Some(macro_name_span),
                            __other__: BTreeMap::new(),
                        };

                        nodes.insert(unique_id, dbt_macro);
                    }
                    SqlResource::Snapshot(name, span, macro_name_span) => {
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
                            macro_name_span: Some(macro_name_span),
                            __other__: BTreeMap::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::FsError;
    use dbt_common::io_args::{IoArgs, StaticAnalysisOffReason};
    use dbt_common::io_utils::StatusReporter;
    use dbt_serde_yaml::Span;
    use dbt_telemetry::{ExecutionPhase, NodeOutcome};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[derive(Default)]
    struct MockStatusReporter {
        warnings: Mutex<Vec<(ErrorCode, Option<dbt_common::CodeLocation>)>>,
    }

    impl MockStatusReporter {
        fn warnings(&self) -> Vec<(ErrorCode, Option<dbt_common::CodeLocation>)> {
            self.warnings.lock().unwrap().clone()
        }
    }

    impl StatusReporter for MockStatusReporter {
        fn collect_error(&self, _error: &FsError) {}

        fn collect_warning(&self, warning: &FsError) {
            self.warnings
                .lock()
                .unwrap()
                .push((warning.code, warning.location.clone()));
        }

        fn collect_node_evaluation(
            &self,
            _file_path: PathBuf,
            _execution_phase: ExecutionPhase,
            _node_outcome: NodeOutcome,
            _upstream_target: Option<(String, String, bool)>,
            _static_analysis_off_reason: (Option<StaticAnalysisOffReason>, Span),
        ) {
        }

        fn show_progress(&self, _action: &str, _target: &str, _description: Option<&str>) {}

        fn bulk_publish_empty(&self, _file_paths: Vec<PathBuf>) {}
    }

    #[test]
    fn invalid_markdown_doc_reports_warning_and_continues() -> FsResult<()> {
        let tmp_dir = tempdir().unwrap();
        let base_path = tmp_dir.path().to_path_buf();
        fs::create_dir_all(base_path.join("models")).unwrap();

        struct InvalidCase<'a> {
            name: &'a str,
            files: Vec<(&'a str, &'a str)>,
            expected_code: ErrorCode,
            expected_warning_paths: Vec<&'a str>,
        }

        let invalid_cases = vec![
            InvalidCase {
                name: "missing_endblock",
                files: vec![("missing_endblock.md", "{% docs broken %}missing endblock")],
                expected_code: ErrorCode::MacroSyntaxError,
                expected_warning_paths: vec!["models/missing_endblock.md"],
            },
            InvalidCase {
                name: "duplicate_name",
                files: vec![
                    (
                        "dup_first.md",
                        r#"
                    {% docs dup_doc %}
                    first
                    {% enddocs %}
                    "#,
                    ),
                    (
                        "dup_second.md",
                        r#"
                    {% docs dup_doc %}
                    second
                    {% enddocs %}
                    "#,
                    ),
                ],
                expected_code: ErrorCode::Unexpected,
                expected_warning_paths: vec!["models/dup_second.md"],
            },
        ];

        for case in invalid_cases {
            let reporter = Arc::new(MockStatusReporter::default());
            let io_args = IoArgs {
                in_dir: base_path.clone(),
                status_reporter: Some(reporter.clone()),
                ..Default::default()
            };

            let mut assets = Vec::new();
            for (file_name, content) in &case.files {
                let file_path = PathBuf::from(format!("models/{}", file_name));
                fs::write(base_path.join(&file_path), content).unwrap();
                assets.push(DbtAsset {
                    base_path: base_path.clone(),
                    path: file_path,
                    package_name: "pkg".to_string(),
                });
            }

            let _ = resolve_docs_macros(&io_args, &assets)?;
            let warnings = reporter.warnings();
            assert_eq!(
                warnings.len(),
                case.expected_warning_paths.len(),
                "expected one warning for case {}",
                case.name
            );
            for (warning, expected_path) in warnings.iter().zip(case.expected_warning_paths.iter())
            {
                assert_eq!(warning.0, case.expected_code);
                assert_eq!(
                    warning.1.as_ref().map(|loc| loc.file.clone()),
                    Some(PathBuf::from(expected_path)),
                    "expected warning location for case {}",
                    case.name
                );
            }
        }

        // Positive case
        let valid_path = PathBuf::from("models/valid_doc.md");
        fs::write(
            base_path.join(&valid_path),
            "{% docs ok_doc %}all good{% enddocs %}",
        )
        .unwrap();

        let reporter = Arc::new(MockStatusReporter::default());
        let io_args = IoArgs {
            in_dir: base_path.clone(),
            status_reporter: Some(reporter.clone()),
            ..Default::default()
        };

        let docs_asset = DbtAsset {
            base_path,
            path: valid_path,
            package_name: "pkg".to_string(),
        };

        let docs = resolve_docs_macros(&io_args, &[docs_asset])?;
        assert!(
            docs.contains_key("doc.pkg.ok_doc"),
            "expected valid doc to be collected"
        );
        assert!(
            reporter.warnings().is_empty(),
            "did not expect warnings for valid doc"
        );

        Ok(())
    }
}
