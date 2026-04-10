use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::io_args::IoArgs;
use dbt_common::path::DbtPath;
use dbt_common::stdfs::diff_paths;
use dbt_common::tracing::emit::{emit_warn_log_from_fs_error, emit_warn_log_message};
use dbt_common::{err, fs_err};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::macros::DbtDocsMacro;
use dbt_schemas::schemas::macros::DbtMacro;
use dbt_schemas::schemas::macros::MacroArgument;
use dbt_schemas::schemas::macros::MacroDependsOn;
use dbt_schemas::schemas::properties::MacrosProperties;
use dbt_schemas::state::DbtAsset;
use minijinja::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use crate::resolve::resolve_properties::MinimalPropertiesEntry;

use crate::utils::parse_macro_statements;

/// Resolve docs macros from a list of docs macro files
pub fn resolve_docs_macros(
    io: &IoArgs,
    docs_macro_files: &[DbtAsset],
    embedded_contents: Option<&HashMap<DbtPath, String>>,
) -> FsResult<BTreeMap<String, DbtDocsMacro>> {
    let mut docs_map: BTreeMap<String, DbtDocsMacro> = BTreeMap::new();

    for docs_asset in docs_macro_files {
        if let Err(err) = process_docs_macro_file(io, &mut docs_map, docs_asset, embedded_contents)
        {
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
    embedded_contents: Option<&HashMap<DbtPath, String>>,
) -> FsResult<()> {
    let docs_file_path = docs_asset.base_path.join(&docs_asset.path);
    let docs_macro = read_file_content(&docs_asset.path, &docs_file_path, embedded_contents)?;

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
                        block_contents: part.trim().to_string(),
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
    embedded_contents: Option<&HashMap<DbtPath, String>>,
) -> FsResult<HashMap<String, DbtMacro>> {
    let mut nodes = HashMap::new();

    for dbt_asset in macro_files {
        let DbtAsset {
            path: macro_file,
            original_path: _original_path,
            base_path,
            package_name,
        } = dbt_asset;
        if macro_file.extension() == Some(OsStr::new("jinja"))
            || macro_file.extension() == Some(OsStr::new("sql"))
        {
            let macro_file_path = base_path.join(macro_file);
            let macro_sql = read_file_content(macro_file, &macro_file_path, embedded_contents)?;
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
                            depends_on: MacroDependsOn { macros: vec![] },
                            // Description is patched from YAML schema files via apply_macro_patches
                            description: String::new(),
                            meta: BTreeMap::new(),
                            docs: None,
                            patch_path: None,
                            funcsign: None,
                            args: vec![],
                            arguments: vec![],
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
                            depends_on: MacroDependsOn { macros: vec![] },
                            // Description is patched from YAML schema files via apply_macro_patches
                            description: String::new(),
                            meta: BTreeMap::new(),
                            docs: None,
                            patch_path: None,
                            funcsign: func_sign.clone(),
                            args: args.clone(),
                            arguments: vec![],
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
                            docs: None,
                            patch_path: None,
                            funcsign: None,
                            args: vec![],
                            arguments: vec![],
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
                            depends_on: MacroDependsOn { macros: vec![] },
                            // Description is patched from YAML schema files via apply_macro_patches
                            description: String::new(),
                            meta: BTreeMap::new(),
                            docs: None,
                            patch_path: None,
                            funcsign: None,
                            args: vec![],
                            arguments: vec![],
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

/// Read file content from the embedded cache or from disk.
fn read_file_content(
    relative_path: &Path,
    absolute_path: &Path,
    embedded_contents: Option<&HashMap<DbtPath, String>>,
) -> FsResult<String> {
    match embedded_contents.and_then(|m| m.get(&DbtPath::from_path(relative_path))) {
        Some(content) => Ok(content.clone()),
        None => fs::read_to_string(absolute_path).map_err(|e| {
            fs_err!(
                code => ErrorCode::IoError,
                loc => absolute_path.to_path_buf(),
                "Failed to read file: {}", e
            )
        }),
    }
}

/// Returns true if the given type string is a valid dbt macro argument type.
///
/// Grammar (dbt Core v1.10+):
/// ```text
/// type      = atom ("|" atom)*
/// atom      = primitive | "list" "[" type "]" | "optional" "[" type "]"
///           | "dict" "[" type "," type "]"
/// primitive = "string" | "str" | "boolean" | "bool" | "integer" | "int"
///           | "float" | "any" | "relation" | "column"
/// ```
/// Union (`|`) and container separators (`,`) are only recognised at bracket
/// depth 0, so `list[str|int]` and `dict[str, list[int|bool]]` are both valid.
pub fn is_valid_macro_arg_type(s: &str) -> bool {
    /// Split `s` on `sep` only at bracket depth 0.
    fn split_shallow(s: &str, sep: char) -> Vec<&str> {
        let mut parts = Vec::new();
        let mut depth: usize = 0;
        let mut start = 0;
        for (i, c) in s.char_indices() {
            match c {
                '[' => depth += 1,
                ']' => depth = depth.saturating_sub(1),
                _ if c == sep && depth == 0 => {
                    parts.push(&s[start..i]);
                    start = i + c.len_utf8();
                }
                _ => {}
            }
        }
        parts.push(&s[start..]);
        parts
    }

    /// A type is one or more atoms joined by `|` at depth 0.
    fn parse_type(s: &str) -> bool {
        let s = s.trim();
        split_shallow(s, '|')
            .iter()
            .all(|part| parse_atom(part.trim()))
    }

    /// An atom is a primitive or a bracketed container.
    fn parse_atom(s: &str) -> bool {
        let s = s.trim();
        if matches!(
            s,
            "string"
                | "str"
                | "boolean"
                | "bool"
                | "integer"
                | "int"
                | "float"
                | "any"
                | "relation"
                | "column"
                | "list"
                | "dict"
                | "optional"
        ) {
            return true;
        }
        if let Some(bracket) = s.find('[') {
            if !s.ends_with(']') {
                return false;
            }
            let prefix = s[..bracket].trim();
            let inner = &s[bracket + 1..s.len() - 1];
            match prefix {
                "list" | "optional" => parse_type(inner),
                "dict" => {
                    let parts = split_shallow(inner, ',');
                    parts.len() == 2 && parse_type(parts[0].trim()) && parse_type(parts[1].trim())
                }
                _ => false,
            }
        } else {
            false
        }
    }

    parse_type(&s.to_lowercase())
}

/// Apply macro patches from YAML schema files to the resolved macros.
/// This updates description and patch_path fields based on YAML macro definitions.
///
/// When `validate_macro_args` is true (from dbt_project.yml flags), this function also:
/// - Warns when YAML argument names don't match the actual Jinja macro parameters
/// - Warns when YAML argument `type` values use unsupported or malformed type syntax
/// - Infers undocumented parameters from the Jinja definition and adds them to `arguments`
pub fn apply_macro_patches(
    io: &IoArgs,
    macros: &mut BTreeMap<String, DbtMacro>,
    macro_properties: &BTreeMap<String, MinimalPropertiesEntry>,
    package_name: &str,
    jinja_env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
    validate_macro_args: bool,
) -> FsResult<()> {
    for (macro_name, props_entry) in macro_properties {
        // Build the unique_id to look up the macro
        let unique_id = format!("macro.{package_name}.{macro_name}");

        // Check if this macro exists in our resolved macros
        if let Some(dbt_macro) = macros.get_mut(&unique_id) {
            // Parse the macro properties with Jinja rendering (for doc blocks)
            let macro_props: MacrosProperties = into_typed_with_jinja(
                io,
                props_entry.schema_value.clone(),
                false,
                jinja_env,
                base_ctx,
                &[],
                dependency_package_name_from_ctx(jinja_env, base_ctx),
                true,
            )?;

            // Update description if provided
            if let Some(description) = macro_props.description {
                dbt_macro.description = description;
            }

            // Merge meta: top-level and config.meta are merged, config.meta wins on conflicts
            let top_meta = macro_props.meta.unwrap_or_default();
            let config_meta = macro_props
                .config
                .as_ref()
                .and_then(|c| c.meta.clone())
                .unwrap_or_default();
            if !top_meta.is_empty() || !config_meta.is_empty() {
                let mut merged = top_meta;
                merged.extend(config_meta);
                dbt_macro.meta = merged.into_iter().collect();
            }

            // Update docs if provided (config.docs takes precedence over top-level docs)
            let docs = macro_props
                .config
                .as_ref()
                .and_then(|c| c.docs.clone())
                .or(macro_props.docs);
            if docs.is_some() {
                dbt_macro.docs = docs;
            }

            // Update arguments if provided in YAML
            if let Some(yml_arguments) = macro_props.arguments {
                let mut arguments: Vec<MacroArgument> = yml_arguments
                    .into_iter()
                    .map(|arg| MacroArgument {
                        name: arg.name,
                        type_: arg.type_,
                        description: arg.description.unwrap_or_default(),
                    })
                    .collect();

                if validate_macro_args {
                    let jinja_arg_names: Vec<&str> =
                        dbt_macro.args.iter().map(|a| a.name.as_str()).collect();

                    // Warn about YAML argument names not present in Jinja definition
                    for yml_arg in &arguments {
                        if !jinja_arg_names.contains(&yml_arg.name.as_str()) {
                            emit_warn_log_message(
                                ErrorCode::ValidateMacroArgs,
                                format!(
                                    "Macro \"{macro_name}\": documented argument \"{}\" not found \
                                     in macro definition",
                                    yml_arg.name
                                ),
                                io.status_reporter.as_ref(),
                            );
                        }
                    }

                    // Warn about invalid type strings
                    for yml_arg in &arguments {
                        if let Some(type_str) = &yml_arg.type_ {
                            if !is_valid_macro_arg_type(type_str) {
                                emit_warn_log_message(
                                    ErrorCode::ValidateMacroArgs,
                                    format!(
                                        "Macro \"{macro_name}\": argument \"{}\" has unsupported \
                                         type \"{type_str}\". Supported types are: string, str, \
                                         boolean, bool, integer, int, float, any, relation, \
                                         column, list[T], dict[K,V], optional[T], T1|T2|...",
                                        yml_arg.name
                                    ),
                                    io.status_reporter.as_ref(),
                                );
                            }
                        }
                    }

                    // Infer undocumented Jinja args and append them to the arguments list
                    let documented_names: std::collections::HashSet<String> =
                        arguments.iter().map(|a| a.name.clone()).collect();
                    for jinja_arg in &dbt_macro.args {
                        if !documented_names.contains(&jinja_arg.name) {
                            arguments.push(MacroArgument {
                                name: jinja_arg.name.clone(),
                                type_: None,
                                description: String::new(),
                            });
                        }
                    }
                }

                dbt_macro.arguments = arguments;
            } else if validate_macro_args && !dbt_macro.args.is_empty() {
                // No YAML arguments at all — infer all from Jinja definition
                dbt_macro.arguments = dbt_macro
                    .args
                    .iter()
                    .map(|a| MacroArgument {
                        name: a.name.clone(),
                        type_: None,
                        description: String::new(),
                    })
                    .collect();
            }

            // Set patch_path to indicate this macro was patched from a YAML file
            // Format: package_name://path/to/schema.yml
            let patch_path = PathBuf::from(format!(
                "{}://{}",
                package_name,
                props_entry.relative_path.display()
            ));
            dbt_macro.patch_path = Some(patch_path);
        } else {
            // Emit a warning when YAML references a macro that doesn't exist
            emit_warn_log_message(
                ErrorCode::SchemaError,
                format!(
                    "Found patch for macro \"{}\" which was not found",
                    macro_name
                ),
                io.status_reporter.as_ref(),
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::FsError;
    use dbt_common::io_args::{IoArgs, StaticAnalysisKind, StaticAnalysisOffReason};
    use dbt_common::io_utils::StatusReporter;
    use dbt_common::path::DbtPath;
    use dbt_telemetry::{ExecutionPhase, NodeOutcome};
    use dbt_yaml::Span;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[derive(Default)]
    struct MockStatusReporter {
        warnings: Mutex<Vec<(ErrorCode, Option<dbt_common::CodeLocationWithFile>)>>,
    }

    impl MockStatusReporter {
        fn warnings(&self) -> Vec<(ErrorCode, Option<dbt_common::CodeLocationWithFile>)> {
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
            _unique_id: &str,
            _execution_phase: ExecutionPhase,
            _node_outcome: NodeOutcome,
            _upstream_target: Option<(String, String, bool)>,
            _static_analysis: StaticAnalysisKind,
            _static_analysis_off_reason: (Option<StaticAnalysisOffReason>, Span),
        ) {
        }

        fn show_progress(&self, _action: &str, _target: &str, _description: Option<&str>) {}

        fn bulk_publish_empty(&self, _file_paths: Vec<DbtPath>) {}
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
                    original_path: file_path.clone(),
                    path: file_path,
                    package_name: "pkg".to_string(),
                });
            }

            let _ = resolve_docs_macros(&io_args, &assets, None)?;
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
                    warning.1.as_ref().map(|loc| loc.file.as_ref().clone()),
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
            original_path: valid_path.clone(),
            path: valid_path,
            package_name: "pkg".to_string(),
        };

        let docs = resolve_docs_macros(&io_args, &[docs_asset], None)?;
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

    #[test]
    fn test_is_valid_macro_arg_type_primitives() {
        for ty in &[
            "string", "str", "boolean", "bool", "integer", "int", "float", "any", "relation",
            "column",
        ] {
            assert!(
                is_valid_macro_arg_type(ty),
                "{ty} should be a valid primitive type"
            );
        }
        assert!(!is_valid_macro_arg_type("text"));
        assert!(!is_valid_macro_arg_type("varchar"));
        assert!(!is_valid_macro_arg_type(""));
    }

    #[test]
    fn test_is_valid_macro_arg_type_containers() {
        assert!(is_valid_macro_arg_type("list[string]"));
        assert!(is_valid_macro_arg_type("list[int]"));
        assert!(is_valid_macro_arg_type("optional[boolean]"));
        assert!(is_valid_macro_arg_type("dict[str, int]"));
        assert!(is_valid_macro_arg_type("dict[str, list[int]]"));
        assert!(is_valid_macro_arg_type("list[dict[str, int]]"));
        assert!(is_valid_macro_arg_type("optional[list[string]]"));

        // Invalid containers
        assert!(!is_valid_macro_arg_type("list[]"));
        assert!(!is_valid_macro_arg_type("list[unknown]"));
        assert!(!is_valid_macro_arg_type("dict[str]")); // missing second type arg
        assert!(!is_valid_macro_arg_type("map[str, int]")); // unsupported container
        assert!(!is_valid_macro_arg_type("list[str")); // missing closing bracket
    }
}
