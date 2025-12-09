//! Python AST visitor for extracting dbt function calls
//!
//! This module implements an AST visitor that walks Python code to extract
//! calls to dbt.ref(), dbt.source(), dbt.config(), and dbt.config.get()

use crate::python_ast::{
    LiteralValue, PythonLiteralEvalError, compute_line_starts, get_full_attr_name, literal_eval,
    offset_to_line_col,
};
use crate::python_file_info::PythonFileInfo;
use dbt_common::{ErrorCode, FsResult, err, io_args::IoArgs};
use dbt_frontend_common::error::CodeLocation;
use dbt_jinja_utils::serde::into_typed_with_error;
use dbt_schemas::schemas::project::DefaultTo;
use ruff_python_ast::{
    self as ast, Expr, Stmt,
    visitor::{Visitor, walk_expr, walk_stmt},
};
use ruff_text_size::Ranged;
use std::path::PathBuf;

/// Type alias for function call arguments (positional, keyword)
type CallArgs = (Vec<LiteralValue>, Vec<(String, LiteralValue)>);

/// Convert a Python LiteralValue to a dbt_serde_yaml::Value
fn literal_value_to_yaml(
    value: LiteralValue,
    span: &dbt_serde_yaml::Span,
) -> Result<dbt_serde_yaml::Value, String> {
    match value {
        LiteralValue::String(s) => Ok(dbt_serde_yaml::Value::String(s, span.clone())),
        LiteralValue::Integer(i) => Ok(dbt_serde_yaml::Value::Number(
            dbt_serde_yaml::Number::from(i),
            span.clone(),
        )),
        LiteralValue::Float(f) => Ok(dbt_serde_yaml::Value::Number(
            dbt_serde_yaml::Number::from(f),
            span.clone(),
        )),
        LiteralValue::Bool(b) => Ok(dbt_serde_yaml::Value::Bool(b, span.clone())),
        LiteralValue::None => Ok(dbt_serde_yaml::Value::Null(span.clone())),
        LiteralValue::List(items) => {
            let mut sequence = Vec::with_capacity(items.len());
            for item in items {
                sequence.push(literal_value_to_yaml(item, span)?);
            }
            Ok(dbt_serde_yaml::Value::Sequence(sequence, span.clone()))
        }
        LiteralValue::Dict(pairs) => {
            let mut mapping = dbt_serde_yaml::Mapping::with_capacity(pairs.len());
            for (key, val) in pairs {
                let key_yaml = literal_value_to_yaml(key, span)?;
                let val_yaml = literal_value_to_yaml(val, span)?;
                mapping.insert(key_yaml, val_yaml);
            }
            Ok(dbt_serde_yaml::Value::Mapping(mapping, span.clone()))
        }
        LiteralValue::Tuple(items) => {
            // Treat tuples as sequences in YAML
            let mut sequence = Vec::with_capacity(items.len());
            for item in items {
                sequence.push(literal_value_to_yaml(item, span)?);
            }
            Ok(dbt_serde_yaml::Value::Sequence(sequence, span.clone()))
        }
    }
}

/// Visitor for extracting dbt function calls from Python AST
pub struct DbtPythonVisitor<'a, T: DefaultTo<T>> {
    /// The file being parsed
    pub file_path: &'a PathBuf,
    /// Collected information about the Python file
    pub file_info: PythonFileInfo<T>,
    /// Errors encountered during parsing
    pub errors: Vec<String>,
    /// IO args for emitting strict parse warnings/errors
    io_args: &'a IoArgs,
    /// Dependency package name if analyzing a package asset
    dependency_package_name: Option<&'a str>,
    /// Display path used when reporting errors
    error_path: Option<PathBuf>,
    /// Precomputed line start offsets for translating ranges to positions
    line_starts: Vec<usize>,
}

impl<'a, T: DefaultTo<T>> DbtPythonVisitor<'a, T> {
    /// Create a new visitor
    pub fn new(
        file_path: &'a PathBuf,
        file_info: PythonFileInfo<T>,
        io_args: &'a IoArgs,
        dependency_package_name: Option<&'a str>,
        error_path: Option<PathBuf>,
        source: &str,
    ) -> Self {
        let line_starts = compute_line_starts(source);
        Self {
            file_path,
            file_info,
            errors: Vec::new(),
            io_args,
            dependency_package_name,
            error_path,
            line_starts,
        }
    }

    /// Extract arguments from a function call as literal values
    fn extract_call_args(
        &mut self,
        args: &[Expr],
        keywords: &[ast::Keyword],
    ) -> Result<CallArgs, PythonLiteralEvalError> {
        let mut positional_args = Vec::new();
        for arg in args {
            positional_args.push(literal_eval(arg, &self.line_starts)?);
        }

        let mut keyword_args = Vec::new();
        for kw in keywords {
            if let Some(arg_name) = &kw.arg {
                let value = literal_eval(&kw.value, &self.line_starts)?;
                keyword_args.push((arg_name.to_string(), value));
            } else {
                // **kwargs unpacking not supported
                let range = kw.range();
                let (line, col) =
                    offset_to_line_col(range.start().to_u32() as usize, &self.line_starts);
                return Err(PythonLiteralEvalError {
                    message: "**kwargs unpacking is not supported".to_string(),
                    line,
                    col,
                });
            }
        }

        Ok((positional_args, keyword_args))
    }

    /// Handle a dbt.ref() call
    fn handle_ref(
        &mut self,
        args: Vec<LiteralValue>,
        kwargs: Vec<(String, LiteralValue)>,
        range_line: usize,
    ) {
        // Extract version from keyword arguments (v or version)
        let version_from_kwargs = kwargs
            .iter()
            .find(|(key, _)| key == "v" || key == "version")
            .and_then(|(_, val)| {
                // Version can be a string or integer
                val.as_string()
                    .or_else(|| val.as_int().map(|i| i.to_string()))
            });

        match args.len() {
            1 => {
                // dbt.ref("model") or dbt.ref("model", v=1)
                if let Some(model_name) = args[0].as_string() {
                    self.file_info.refs.push((
                        model_name,
                        None,
                        version_from_kwargs,
                        // TODO: add column and index
                        CodeLocation {
                            line: range_line as u32,
                            col: 0,
                            index: 0,
                        },
                    ));
                }
            }
            2 => {
                // dbt.ref("package", "model") or dbt.ref("package", "model", v=1)
                if let (Some(package), Some(model)) = (args[0].as_string(), args[1].as_string()) {
                    self.file_info.refs.push((
                        model,
                        Some(package),
                        version_from_kwargs,
                        // TODO: add column and index
                        CodeLocation {
                            line: range_line as u32,
                            col: 0,
                            index: 0,
                        },
                    ));
                }
            }
            3 => {
                // dbt.ref("package", "model", "version") - positional version takes precedence
                if let (Some(package), Some(model), Some(version)) = (
                    args[0].as_string(),
                    args[1].as_string(),
                    args[2].as_string(),
                ) {
                    self.file_info.refs.push((
                        model,
                        Some(package),
                        Some(version),
                        CodeLocation {
                            line: range_line as u32,
                            col: 0,
                            index: 0,
                        },
                    ));
                }
            }
            _ => {
                self.errors.push(format!(
                    "dbt.ref() expects 1-3 arguments, got {} at line {}",
                    args.len(),
                    range_line
                ));
            }
        }
    }

    /// Handle a dbt.source() call
    fn handle_source(&mut self, args: Vec<LiteralValue>, range_line: usize) {
        if args.len() == 2 {
            if let (Some(source_name), Some(table_name)) =
                (args[0].as_string(), args[1].as_string())
            {
                self.file_info.sources.push((
                    source_name,
                    table_name,
                    // TODO: add column and index
                    CodeLocation {
                        line: range_line as u32,
                        col: 0,
                        index: 0,
                    },
                ));
            }
        } else {
            self.errors.push(format!(
                "dbt.source() expects 2 arguments, got {} at line {}",
                args.len(),
                range_line
            ));
        }
    }

    /// Handle a dbt.config() call
    fn handle_config(&mut self, kwargs: Vec<(String, LiteralValue)>) {
        // Track which config keys were used
        for (key, _value) in &kwargs {
            self.file_info.config_keys_used.push(key.clone());
        }

        // Convert kwargs into YAML mapping and deserialize into config struct
        // This handles all config fields uniformly, similar to SQL models
        let span = dbt_serde_yaml::Span::default();
        let mut mapping = dbt_serde_yaml::Mapping::with_capacity(kwargs.len());

        for (key, value) in kwargs {
            // Convert LiteralValue to dbt_serde_yaml::Value
            let yaml_value = match literal_value_to_yaml(value, &span) {
                Ok(v) => v,
                Err(e) => {
                    self.errors.push(format!(
                        "Failed to convert config value for '{}': {}",
                        key, e
                    ));
                    continue;
                }
            };

            mapping.insert(dbt_serde_yaml::Value::String(key, span.clone()), yaml_value);
        }

        // Deserialize the entire mapping into the config struct, emitting strict warnings on unused keys
        let yaml_value = dbt_serde_yaml::Value::Mapping(mapping, span);
        match into_typed_with_error(
            self.io_args,
            yaml_value,
            true,
            self.dependency_package_name,
            self.error_path.clone(),
        ) {
            Ok(config_update) => {
                self.file_info.update_config(config_update);
            }
            Err(e) => {
                self.errors
                    .push(format!("Failed to parse dbt.config(): {}", e));
            }
        }
    }

    /// Handle a dbt.config.get() call
    fn handle_config_get(&mut self, args: Vec<LiteralValue>, range_line: usize) {
        match args.len() {
            1 => {
                if let Some(key) = args[0].as_string() {
                    self.file_info.config_keys_used.push(key);
                }
            }
            2 => {
                if let Some(key) = args[0].as_string() {
                    self.file_info.config_keys_used.push(key.clone());
                    // Store the default value as a string representation
                    let default_str = format!("{:?}", args[1]);
                    self.file_info.config_keys_defaults.push((key, default_str));
                }
            }
            _ => {
                self.errors.push(format!(
                    "dbt.config.get() expects 1-2 arguments, got {} at line {}",
                    args.len(),
                    range_line
                ));
            }
        }
    }

    /// Process a function call that might be a dbt function
    fn process_dbt_call(&mut self, expr: &Expr) {
        if let Expr::Call(ast::ExprCall {
            func,
            arguments: ast::Arguments { args, keywords, .. },
            ..
        }) = expr
            && let Some(full_name) = get_full_attr_name(func)
        {
            let range = expr.range();
            let (line, _col) =
                offset_to_line_col(range.start().to_u32() as usize, &self.line_starts);

            match full_name.as_str() {
                "dbt.ref" => match self.extract_call_args(args, keywords) {
                    Ok((positional_args, keyword_args)) => {
                        self.handle_ref(positional_args, keyword_args, line)
                    }
                    Err(e) => self.errors.push(e.to_string()),
                },
                "dbt.source" => match self.extract_call_args(args, keywords) {
                    Ok((positional_args, _)) => self.handle_source(positional_args, line),
                    Err(e) => self.errors.push(e.to_string()),
                },
                "dbt.config" => match self.extract_call_args(args, keywords) {
                    Ok((_positional_args, keyword_args)) => self.handle_config(keyword_args),
                    Err(e) => self.errors.push(e.to_string()),
                },
                "dbt.config.get" => match self.extract_call_args(args, keywords) {
                    Ok((positional_args, _)) => self.handle_config_get(positional_args, line),
                    Err(e) => self.errors.push(e.to_string()),
                },
                _ => {}
            }
        }
    }
}

impl<'a, T: DefaultTo<T>> Visitor<'_> for DbtPythonVisitor<'a, T> {
    fn visit_stmt(&mut self, stmt: &Stmt) {
        // Track import statements for package telemetry
        match stmt {
            Stmt::Import(ast::StmtImport { names, .. }) => {
                for alias in names {
                    self.file_info.packages.push(alias.name.to_string());
                }
            }
            Stmt::ImportFrom(ast::StmtImportFrom {
                module: Some(module_name),
                ..
            }) => {
                self.file_info.packages.push(module_name.to_string());
            }
            _ => {}
        }

        walk_stmt(self, stmt);
    }

    fn visit_expr(&mut self, expr: &Expr) {
        // Check if this expression is a dbt function call
        self.process_dbt_call(expr);

        // Continue walking the AST - walk_expr will recursively visit nested expressions
        // so we don't need to manually process them here
        walk_expr(self, expr);
    }
}

/// Analyze a Python model file and extract dbt function calls
pub fn analyze_python_file<T: DefaultTo<T>>(
    file_path: &PathBuf,
    source: &str,
    stmts: &[Stmt],
    checksum: dbt_schemas::schemas::common::DbtChecksum,
    io_args: &IoArgs,
    dependency_package_name: Option<&str>,
    error_path: Option<PathBuf>,
) -> FsResult<PythonFileInfo<T>> {
    let file_info = PythonFileInfo::new(checksum);
    let mut visitor = DbtPythonVisitor::new(
        file_path,
        file_info,
        io_args,
        dependency_package_name,
        error_path,
        source,
    );

    // Walk the AST
    for stmt in stmts {
        visitor.visit_stmt(stmt);
    }

    // Check for errors
    if !visitor.errors.is_empty() {
        return err!(
            code => ErrorCode::Generic,
            loc => file_path.clone(),
            "Errors parsing Python model:\n{}",
            visitor.errors.join("\n")
        );
    }

    Ok(visitor.file_info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::python_ast::parse_python;
    use dbt_common::io_args::IoArgs;
    use dbt_schemas::schemas::common::DbtChecksum;
    use std::path::PathBuf;

    #[test]
    fn test_extract_ref() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref('customers')
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "customers");
    }

    #[test]
    fn test_extract_source() {
        let source = r#"
def model(dbt, session):
    df = dbt.source('raw', 'customers')
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.sources[0].0, "raw");
        assert_eq!(result.sources[0].1, "customers");
    }

    #[test]
    fn test_extract_config() {
        let source = r#"
def model(dbt, session):
    dbt.config(materialized='table')
    return session.table('data')
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert!(
            result
                .config_keys_used
                .contains(&"materialized".to_string())
        );
    }

    #[test]
    fn test_extract_imports() {
        let source = r#"
import pandas as pd
from sklearn import datasets

def model(dbt, session):
    return pd.DataFrame()
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.packages.len(), 2);
        assert!(result.packages.contains(&"pandas".to_string()));
        assert!(result.packages.contains(&"sklearn".to_string()));
    }

    #[test]
    fn test_ref_in_list() {
        let source = r#"
def model(dbt, session):
    refs = [dbt.ref('a'), dbt.ref('b')]
    return refs
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 2);
    }

    #[test]
    fn test_extract_config_get() {
        let source = r#"
def model(dbt, session):
    mat = dbt.config.get('materialized', 'table')
    schema = dbt.config.get('schema')
    return session.table('data')
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.config_keys_used.len(), 2);
        assert!(
            result
                .config_keys_used
                .contains(&"materialized".to_string())
        );
        assert!(result.config_keys_used.contains(&"schema".to_string()));
        assert_eq!(result.config_keys_defaults.len(), 1);
        assert_eq!(result.config_keys_defaults[0].0, "materialized");
    }

    #[test]
    fn test_versioned_ref() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref('my_package', 'my_model', 'v1')
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].1, Some("my_package".to_string()));
        assert_eq!(result.refs[0].2, Some("v1".to_string()));
    }

    #[test]
    fn test_config_with_complex_values() {
        let source = r#"
def model(dbt, session):
    dbt.config(
        materialized='table',
        tags=['important', 'daily'],
        meta={'owner': 'data-team'}
    )
    return session.table('data')
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert!(
            result
                .config_keys_used
                .contains(&"materialized".to_string())
        );
        assert!(result.config_keys_used.contains(&"tags".to_string()));
        assert!(result.config_keys_used.contains(&"meta".to_string()));
    }

    #[test]
    fn test_multiple_config_calls() {
        let source = r#"
def model(dbt, session):
    dbt.config(materialized='table')
    dbt.config(tags=['test'])
    return session.table('data')
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        // Both config calls should be processed
        assert!(
            result
                .config_keys_used
                .contains(&"materialized".to_string())
        );
        assert!(result.config_keys_used.contains(&"tags".to_string()));
    }

    #[test]
    fn test_ref_with_invalid_argument_count() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref('a', 'b', 'c', 'd')  # Too many arguments
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("dbt.ref() expects 1-3 arguments")
        );
    }

    #[test]
    fn test_source_with_invalid_argument_count() {
        let source = r#"
def model(dbt, session):
    df = dbt.source('raw')  # Not enough arguments
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("dbt.source() expects 2 arguments")
        );
    }

    // =========================================================================
    // Comprehensive tests from dbt-core test suite
    // =========================================================================

    /// Test complex Python model parsing with multiple refs in various contexts
    /// Mirrors dbt-core's test_python_model_parse
    #[test]
    fn test_complex_python_model_parse() {
        let source = r#"
import textblob
import text as a
from torch import b
import textblob.text
import sklearn

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['sklearn==0.1.0']
    )
    df0 = dbt.ref("a_model").to_pandas()
    df1 = dbt.ref("my_sql_model").task.limit(2)
    df2 = dbt.ref("my_sql_model_1")
    df3 = dbt.ref("my_sql_model_2")
    df4 = dbt.source("test", 'table1').limit(max=[max(dbt.ref('something'))])
    df5 = [dbt.ref('test1')]

    a_dict = {'test2': dbt.ref('test2')}
    df5 = {'test2': dbt.ref('test3')}
    df6 = [dbt.ref("test4")]
    f"{dbt.ref('test5')}"

    df = df0.limit(2)
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        // Check all refs were extracted (10 total)
        let ref_names: Vec<&str> = result
            .refs
            .iter()
            .map(|(name, _, _, _)| name.as_str())
            .collect();
        assert_eq!(ref_names.len(), 10);
        assert!(ref_names.contains(&"a_model"));
        assert!(ref_names.contains(&"my_sql_model"));
        assert!(ref_names.contains(&"my_sql_model_1"));
        assert!(ref_names.contains(&"my_sql_model_2"));
        assert!(ref_names.contains(&"something"));
        assert!(ref_names.contains(&"test1"));
        assert!(ref_names.contains(&"test2"));
        assert!(ref_names.contains(&"test3"));
        assert!(ref_names.contains(&"test4"));
        assert!(ref_names.contains(&"test5"));

        // Check source was extracted
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.sources[0].0, "test");
        assert_eq!(result.sources[0].1, "table1");

        // Check packages were extracted from imports
        assert!(result.packages.contains(&"textblob".to_string()));
        assert!(result.packages.contains(&"text".to_string()));
        assert!(result.packages.contains(&"torch".to_string()));
        assert!(result.packages.contains(&"sklearn".to_string()));

        // Check config keys were used
        assert!(
            result
                .config_keys_used
                .contains(&"materialized".to_string())
        );
        assert!(result.config_keys_used.contains(&"packages".to_string()));
    }

    /// Test config.get() extracts config keys
    /// Mirrors dbt-core's test_python_model_config
    #[test]
    fn test_python_model_config_get() {
        let source = r#"
def model(dbt, session):
    dbt.config.get("param_1")
    dbt.config.get("param_2")
    return dbt.ref("some_model")
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.config_keys_used.len(), 2);
        assert!(result.config_keys_used.contains(&"param_1".to_string()));
        assert!(result.config_keys_used.contains(&"param_2".to_string()));
    }

    /// Test config.get() with defaults
    /// Mirrors dbt-core's test_python_model_config_with_defaults
    #[test]
    fn test_python_model_config_with_defaults() {
        let source = r#"
def model(dbt, session):
    dbt.config.get("param_None", None)
    dbt.config.get("param_Str", "default")
    dbt.config.get("param_List", [1, 2])
    return dbt.ref("some_model")
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.config_keys_used.len(), 3);
        assert_eq!(result.config_keys_defaults.len(), 3);

        // Check that all keys are present
        assert!(result.config_keys_used.contains(&"param_None".to_string()));
        assert!(result.config_keys_used.contains(&"param_Str".to_string()));
        assert!(result.config_keys_used.contains(&"param_List".to_string()));

        // Check defaults are captured
        let default_keys: Vec<&str> = result
            .config_keys_defaults
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert!(default_keys.contains(&"param_None"));
        assert!(default_keys.contains(&"param_Str"));
        assert!(default_keys.contains(&"param_List"));
    }

    /// Test config.get() in f-string
    /// Mirrors dbt-core's test_python_model_f_string_config
    #[test]
    fn test_python_model_f_string_config() {
        let source = r#"
import pandas as pd

def model(dbt, fal):
    dbt.config(materialized="table")
    print(f"my var: {dbt.config.get('my_var')}")
    df: pd.DataFrame = dbt.ref("some_model")
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert!(result.config_keys_used.contains(&"my_var".to_string()));
        assert!(
            result
                .config_keys_used
                .contains(&"materialized".to_string())
        );
    }

    /// Test that ref() in variable names (not as a call) doesn't get extracted
    /// Mirrors dbt-core's test_python_model_incorrect_ref
    #[test]
    fn test_python_model_non_literal_ref() {
        let source = r#"
def model(dbt, session):
    model_names = ["orders", "customers"]
    models = []

    for model_name in model_names:
        # This is invalid - ref must be called with literal strings
        models.extend(dbt.ref(model_name))

    return models[0]
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        );

        // Should fail because ref() is called with a variable, not a literal
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Non-literal expression")
        );
    }

    /// Test default materialization
    #[test]
    fn test_python_model_default_materialization() {
        let source = r#"
import pandas as pd

def model(dbt, session):
    return pd.dataframe([1, 2])
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        // No explicit materialization set, will use default (table)
        assert!(result.config.materialized.is_none());
    }

    /// Test custom materialization
    #[test]
    fn test_python_model_custom_materialization() {
        let source = r#"
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="incremental")
    return pd.dataframe([1, 2])
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(
            *result.config.materialized.as_ref().unwrap(),
            dbt_schemas::schemas::common::DbtMaterialization::Incremental
        );
    }

    /// Test that refs in dicts and lists at various nesting levels are captured
    #[test]
    fn test_refs_in_nested_structures() {
        let source = r#"
def model(dbt, session):
    # Refs in lists
    list1 = [dbt.ref('a'), dbt.ref('b')]

    # Refs in dicts
    dict1 = {'key1': dbt.ref('c'), 'key2': dbt.ref('d')}

    # Refs in nested structures
    nested = {
        'list': [dbt.ref('e')],
        'dict': {'inner': dbt.ref('f')}
    }

    return session.table('data')
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 6);
        let ref_names: Vec<&str> = result
            .refs
            .iter()
            .map(|(name, _, _, _)| name.as_str())
            .collect();
        assert!(ref_names.contains(&"a"));
        assert!(ref_names.contains(&"b"));
        assert!(ref_names.contains(&"c"));
        assert!(ref_names.contains(&"d"));
        assert!(ref_names.contains(&"e"));
        assert!(ref_names.contains(&"f"));
    }

    /// Test package version specification in config
    #[test]
    fn test_packages_config() {
        let source = r#"
def model(dbt, session):
    dbt.config(
        materialized='table',
        packages=['sklearn==0.1.0', 'pandas>=1.0.0']
    )
    return session.table('data')
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert!(result.config_keys_used.contains(&"packages".to_string()));
        // The packages list should be in the config
        assert!(result.config.packages.is_some());
    }

    /// Test two-argument ref (package, model)
    #[test]
    fn test_two_argument_ref() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref('my_package', 'my_model')
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].1, Some("my_package".to_string()));
        assert_eq!(result.refs[0].2, None);
    }

    /// Test tags in config
    #[test]
    fn test_config_with_tags() {
        let source = r#"
def model(dbt, session):
    dbt.config(tags=['important', 'daily'])
    return session.table('data')
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert!(result.config_keys_used.contains(&"tags".to_string()));
        assert!(result.config.tags.is_some());
    }

    // =========================================================================
    // Tests for versioned refs with keyword arguments
    // =========================================================================

    /// Test ref with v= keyword argument
    /// Mirrors dbt-core functional test behavior
    #[test]
    fn test_ref_with_v_keyword() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref("my_model", v=1)
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].1, None); // no package
        assert_eq!(result.refs[0].2, Some("1".to_string())); // version
    }

    /// Test ref with version= keyword argument
    #[test]
    fn test_ref_with_version_keyword() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref("my_model", version=1)
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].1, None);
        assert_eq!(result.refs[0].2, Some("1".to_string()));
    }

    /// Test ref with string version in keyword argument
    #[test]
    fn test_ref_with_string_version_keyword() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref("my_model", v="v1")
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].2, Some("v1".to_string()));
    }

    /// Test ref with package and v= keyword argument
    #[test]
    fn test_ref_with_package_and_v_keyword() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref("test_package", "my_model", v=1)
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].1, Some("test_package".to_string()));
        assert_eq!(result.refs[0].2, Some("1".to_string()));
    }

    /// Test ref with package and version= keyword argument
    #[test]
    fn test_ref_with_package_and_version_keyword() {
        let source = r#"
def model(dbt, session):
    df = dbt.ref("test_package", "my_model", version=1)
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        assert_eq!(result.refs.len(), 1);
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].1, Some("test_package".to_string()));
        assert_eq!(result.refs[0].2, Some("1".to_string()));
    }

    /// Test multiple refs with different versioning styles
    /// Mirrors the dbt-core functional test basic_python
    #[test]
    fn test_multiple_versioned_refs_mixed_styles() {
        let source = r#"
def model(dbt, _):
    dbt.config(materialized='table')
    df1 = dbt.ref("my_model")
    df2 = dbt.ref("my_versioned_model", v=1)
    df3 = dbt.ref("my_versioned_model", version=1)
    df4 = dbt.ref("test", "my_versioned_model", v=1)
    df5 = dbt.ref("test", "my_versioned_model", version=1)
    df6 = dbt.source("test_source", "test_table")
    return df1
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        )
        .unwrap();

        // Check all refs were extracted
        assert_eq!(result.refs.len(), 5);

        // df1: simple ref
        assert_eq!(result.refs[0].0, "my_model");
        assert_eq!(result.refs[0].1, None);
        assert_eq!(result.refs[0].2, None);

        // df2: ref with v=1
        assert_eq!(result.refs[1].0, "my_versioned_model");
        assert_eq!(result.refs[1].1, None);
        assert_eq!(result.refs[1].2, Some("1".to_string()));

        // df3: ref with version=1
        assert_eq!(result.refs[2].0, "my_versioned_model");
        assert_eq!(result.refs[2].1, None);
        assert_eq!(result.refs[2].2, Some("1".to_string()));

        // df4: ref with package and v=1
        assert_eq!(result.refs[3].0, "my_versioned_model");
        assert_eq!(result.refs[3].1, Some("test".to_string()));
        assert_eq!(result.refs[3].2, Some("1".to_string()));

        // df5: ref with package and version=1
        assert_eq!(result.refs[4].0, "my_versioned_model");
        assert_eq!(result.refs[4].1, Some("test".to_string()));
        assert_eq!(result.refs[4].2, Some("1".to_string()));

        // Check source was extracted
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.sources[0].0, "test_source");
        assert_eq!(result.sources[0].1, "test_table");
    }

    /// Test that too many positional arguments raises an error
    #[test]
    fn test_ref_with_too_many_args() {
        let source = r#"
def model(dbt, session):
    # This is invalid - too many arguments
    df = dbt.ref("package", "model", "version", "extra")
    return df
"#;
        let path = PathBuf::from("test.py");
        let suite = parse_python(source, &path).unwrap();
        let result = analyze_python_file::<dbt_schemas::schemas::project::ModelConfig>(
            &path,
            source,
            &suite,
            DbtChecksum::default(),
            &IoArgs::default(),
            None,
            Some(path.clone()),
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("dbt.ref() expects 1-3 arguments")
        );
    }
}
