use super::utils::{base_tests_inner, column_tests_inner};
use crate::args::ResolveArgs;
use dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME;
use dbt_common::FsError;
use dbt_common::FsResult;
use dbt_common::{err, ErrorCode};
use dbt_common::{fs_err, stdfs};
use dbt_frontend_common::Dialect;
use dbt_jinja_utils::serde::check_single_expression_without_whitepsace_control;
use dbt_schemas::schemas::common::normalize_quote;
use dbt_schemas::schemas::common::Versions;
use dbt_schemas::schemas::data_tests::{
    AcceptedValuesTestProperties, DataTests, NotNullTestProperties, RelationshipsTestProperties,
    UniqueTestProperties,
};

use dbt_schemas::schemas::dbt_column::ColumnProperties;
use dbt_schemas::schemas::project::DataTestConfig;
use dbt_schemas::schemas::properties::Tables;
use dbt_schemas::schemas::properties::{ModelProperties, SeedProperties, SnapshotProperties};
use dbt_schemas::state::DbtAsset;
use itertools::Itertools; // For .sorted_by() on iterators
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;

const CONFIG_ARGS: &[&str] = &[
    "severity",
    "tags",
    "enabled",
    "where",
    "limit",
    "warn_if",
    "error_if",
    "fail_calc",
    "store_failures",
    "store_failures_as",
    "meta",
    "database",
    "schema",
    "alias",
];

pub struct TestableNode<'a, T: TestableNodeTrait> {
    inner: &'a T,
}

impl<T: TestableNodeTrait> TestableNode<'_, T> {
    pub fn persist(
        &self,
        project_name: &str,
        out_dir: &Path,
        collected_tests: &mut Vec<DbtAsset>,
        adapter_type: &str,
    ) -> FsResult<()> {
        let test_configs: Vec<GenericTestConfig> = self.try_into()?;
        // Process tests for each version (or single resource)
        let dialect = Dialect::from_str(adapter_type)
            .map_err(|e| fs_err!(ErrorCode::Unexpected, "Failed to parse adapter type: {}", e))?;
        for test_config in test_configs {
            // Handle model-level tests
            if let Some(tests) = &test_config.model_tests {
                for test in tests {
                    let dbt_asset = persist_inner(project_name, out_dir, &test_config, None, test)?;
                    collected_tests.push(dbt_asset);
                }
            }

            // Handle column-level tests
            if let Some(column_tests) = &test_config.column_tests {
                for (column_name, (should_quote, tests)) in column_tests {
                    for test in tests {
                        // Need dialect to quote properly
                        let (column_name, should_quote) =
                            normalize_quote(*should_quote, adapter_type, column_name);
                        let quoted_column_name = if should_quote {
                            format!(
                                "{}{}{}",
                                dialect.quote_char(),
                                column_name,
                                dialect.quote_char()
                            )
                        } else {
                            column_name.to_string()
                        };
                        let dbt_asset = persist_inner(
                            project_name,
                            out_dir,
                            &test_config,
                            Some(&quoted_column_name),
                            test,
                        )?;
                        collected_tests.push(dbt_asset);
                    }
                }
            }
        }

        Ok(())
    }
}

fn persist_inner(
    project_name: &str,
    out_dir: &Path,
    test_config: &GenericTestConfig,
    column_name: Option<&str>,
    test: &DataTests,
) -> FsResult<DbtAsset> {
    let details = get_test_details(test, test_config, column_name)?;
    let TestDetails {
        test_macro_name,
        custom_test_name,
        kwargs,
        namespace,
        config,
        jinja_set_vars,
    } = details;

    let full_name = generate_test_name(
        test_macro_name.as_str(),
        custom_test_name.is_some(),
        project_name,
        test_config,
        &kwargs,
        namespace.as_ref(),
        &jinja_set_vars,
    );
    let path = PathBuf::from(DBT_GENERIC_TESTS_DIR_NAME).join(format!("{full_name}.sql"));
    let test_file = out_dir.join(&path);
    let generated_test_sql = generate_test_macro(
        test_macro_name.as_str(),
        &kwargs,
        namespace.as_deref(),
        &config,
        &jinja_set_vars,
    )?;
    stdfs::write(&test_file, generated_test_sql)?;
    Ok(DbtAsset {
        path,
        base_path: out_dir.to_path_buf(),
        package_name: project_name.to_string(),
    })
}

#[derive(Debug, Clone)]
struct TestDetails {
    test_macro_name: String,
    custom_test_name: Option<String>,
    kwargs: BTreeMap<String, Value>,
    namespace: Option<String>,
    config: BTreeMap<String, Value>,
    jinja_set_vars: BTreeMap<String, String>,
}

fn get_test_details(
    test: &DataTests,
    test_config: &GenericTestConfig,
    column_name: Option<&str>,
) -> FsResult<TestDetails> {
    let mut kwargs = BTreeMap::new();
    let mut config = BTreeMap::new();
    let mut jinja_set_vars = BTreeMap::new();

    // Common kwargs for all tests
    // Determine the model string based on the resource type
    let model_string = match test_config.resource_type.as_str() {
        "source" => {
            if let Some(source_name) = &test_config.source_name {
                format!(
                    "source('{}', '{}')",
                    source_name, &test_config.resource_name
                )
            } else {
                return err!(
                    ErrorCode::SchemaError,
                    "Source identifiers are missing for a source resource",
                );
            }
        }
        _ => {
            if let Some(ref version_num) = test_config.version_num {
                format!("ref('{}', v={})", &test_config.resource_name, version_num)
            } else {
                format!("ref('{}')", &test_config.resource_name)
            }
        }
    };

    kwargs.insert(
        "model".to_string(),
        Value::String(format!("get_where_subquery({model_string})")),
    );
    if let Some(col) = column_name {
        kwargs.insert("column_name".to_string(), Value::String(col.to_string()));
    }

    let (test_macro_name, custom_test_name, namespace) = match test {
        DataTests::String(test_name) => {
            let (test_macro_name, namespace) = parse_test_name_and_namespace(test_name);
            (test_macro_name, None, namespace)
        }

        DataTests::NotNullTest(test) => {
            let (custom_test_name, test_macro_name) = parse_builtin_macro_name_and_test_name(
                &mut kwargs,
                &mut config,
                &test.not_null,
                "not_null",
            )?;
            (test_macro_name, custom_test_name, None)
        }

        DataTests::UniqueTest(test) => {
            let (custom_test_name, test_macro_name) = parse_builtin_macro_name_and_test_name(
                &mut kwargs,
                &mut config,
                &test.unique,
                "unique",
            )?;
            (test_macro_name, custom_test_name, None)
        }

        DataTests::AcceptedValuesTest(test) => {
            let (custom_test_name, test_macro_name) = parse_builtin_macro_name_and_test_name(
                &mut kwargs,
                &mut config,
                &test.accepted_values,
                "accepted_values",
            )?;
            (test_macro_name, custom_test_name, None)
        }

        DataTests::RelationshipsTest(test) => {
            let (custom_test_name, test_macro_name) = parse_builtin_macro_name_and_test_name(
                &mut kwargs,
                &mut config,
                &test.relationships,
                "relationships",
            )?;
            (test_macro_name, custom_test_name, None)
        }

        DataTests::CustomTest(custom_test) => {
            if let Value::Object(map) = custom_test {
                if map.len() == 1 {
                    // Get the first key as the test name and the value are the test args
                    let (test_macro_name, args) =
                        map.iter().next().expect("CustomTest value is not empty");
                    let (custom_test_name, namespace) =
                        parse_test_name_and_namespace(test_macro_name);

                    let args = args.as_object().ok_or(fs_err!(
                        ErrorCode::SchemaError,
                        "CustomTest value is not an object"
                    ))?;

                    let set_vars = extract_kwargs_and_config(args, &mut kwargs, &mut config)?;
                    jinja_set_vars.extend(set_vars);

                    (custom_test_name, None, namespace)
                } else {
                    let mut args = map.clone();
                    let test_macro_name = args.remove("test_name").ok_or(fs_err!(
                        ErrorCode::SchemaError,
                        "CustomTest is missing test_name"
                    ))?;

                    let test_macro_name = test_macro_name.as_str().ok_or(fs_err!(
                        ErrorCode::SchemaError,
                        "CustomTest test_name is not a string"
                    ))?;
                    let (test_macro_name, namespace) =
                        parse_test_name_and_namespace(test_macro_name);

                    let custom_test_name: String = args
                        .remove("name")
                        .and_then(|v| v.as_str().map(String::from))
                        .unwrap_or(test_macro_name.clone());

                    let set_vars = extract_kwargs_and_config(&args, &mut kwargs, &mut config)?;
                    jinja_set_vars.extend(set_vars);

                    (test_macro_name, Some(custom_test_name), namespace)
                }
            } else {
                return Err(Box::new(FsError::new(
                    ErrorCode::SchemaError,
                    "Custom test is not an object",
                )));
            }
        }
    };

    Ok(TestDetails {
        test_macro_name: normalize_test_name(&test_macro_name)?,
        custom_test_name,
        kwargs,
        namespace,
        config,
        jinja_set_vars,
    })
}

fn parse_builtin_macro_name_and_test_name<T>(
    kwargs: &mut BTreeMap<String, Value>,
    config: &mut BTreeMap<String, Value>,
    test_inner: T,
    macro_name: &str,
) -> FsResult<(Option<String>, String)>
where
    T: serde::Serialize + std::fmt::Debug + Copy,
{
    let test_inner_value = serde_json::to_value(test_inner).map_err(|e| {
        fs_err!(
            ErrorCode::SchemaError,
            "Failed to serialize test properties: {}",
            e
        )
    })?;
    let mut args = test_inner_value
        .as_object()
        .ok_or(fs_err!(
            ErrorCode::SchemaError,
            "{} test is not an object",
            macro_name
        ))?
        .to_owned();
    let name = args
        .remove("name")
        .and_then(|v| v.as_str().map(String::from));
    let (macro_name, name) = if let Some(name) = name {
        (Some(name), macro_name.to_string())
    } else {
        (None, macro_name.to_string())
    };
    extract_kwargs_and_config(&args, kwargs, config)?;
    Ok((macro_name, name))
}

fn parse_test_name_and_namespace(test_name: &str) -> (String, Option<String>) {
    if let Some((package, test_name)) = test_name.split_once('.') {
        (test_name.to_owned(), Some(package.to_owned()))
    } else {
        (test_name.to_owned(), None)
    }
}

static CLEAN_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[^0-9a-zA-Z_]+").expect("valid regex"));

//https://github.com/dbt-labs/dbt-core/blob/31881d2a3bea030e700e9df126a3445298385698/core/dbt/parser/generic_test_builders.py#L26
/// Generates a test name and alias for a generic test.
///
/// * `test_name` - Name of the test (e.g. "unique", "not_null", etc)
/// * `is_custom_test_name` - Whether a custom name was provided for this test
fn generate_test_name(
    test_macro_name: &str,
    is_custom_test_name: bool,
    project_name: &str,
    test_config: &GenericTestConfig,
    kwargs: &BTreeMap<String, Value>,
    package_name: Option<&String>,
    jinja_set_vars: &BTreeMap<String, String>,
) -> String {
    // Flatten args (excluding 'model' and config args)
    let mut flat_args = Vec::new();
    for (arg_name, arg_val) in kwargs.iter().sorted_by(|a, b| a.0.cmp(b.0)) {
        // Skip 'model' and any config arguments
        if arg_name == "model" || CONFIG_ARGS.contains(&arg_name.as_str()) {
            continue;
        }

        // Check if this arg references a Jinja set variable
        let actual_value = if let Value::String(s) = arg_val {
            if let Some(original_value) = jinja_set_vars.get(s) {
                // Use the original value from the set variable instead of the variable name
                Value::String(original_value.clone())
            } else {
                arg_val.clone()
            }
        } else {
            arg_val.clone()
        };

        let parts = match actual_value {
            Value::Object(map) => map.values().map(|v| v.to_string()).collect::<Vec<_>>(),
            Value::Array(arr) => arr.iter().map(|v| v.to_string()).collect(),
            _ => vec![actual_value.to_string()],
        };

        flat_args.extend(parts);
    }

    // Clean args to only allow alphanumeric and underscore
    let mut clean_flat_args: Vec<String> = flat_args
        .iter()
        .map(|arg| {
            CLEAN_REGEX
                .replace_all(arg, "_")
                .trim_matches('_')
                .to_string()
        })
        .collect();

    // we don't need to any suffix since a name is explicitly defined using `name` field
    if is_custom_test_name {
        clean_flat_args = vec![];
    }

    // Join args with double underscores - empty string if no args
    let suffix = if !clean_flat_args.is_empty() {
        clean_flat_args.join("__")
    } else {
        String::new()
    };

    // Build the test name from here
    let (prefix, resource_name) = match &test_config.source_name {
        Some(source_name) => (
            // handles the test from a source model
            format!("source_{test_macro_name}"),
            format!("{}_{}", source_name, &test_config.resource_name),
        ),
        None => (
            test_macro_name.to_string(),
            test_config.resource_name.clone(),
        ),
    };

    let test_identifier = match &test_config.version_num {
        Some(version_num) => format!("{prefix}_{resource_name}_v{version_num}"),
        None => format!("{prefix}_{resource_name}"),
    };

    let result = match package_name {
        Some(pkg_name) if pkg_name != project_name => {
            format!("{pkg_name}_{test_identifier}_{suffix}")
        }
        _ => {
            format!("{test_identifier}_{suffix}")
        }
    };

    // linux has 255 bytes limit on filename (255- 16 = 239 for file name extension .sql or .macro_spans.json)
    if result.len() >= 239 {
        let mut hasher = DefaultHasher::new();
        result.as_str().hash(&mut hasher);
        let hash = hasher.finish();
        format!("t_{hash:X}") // Uppercase hex format is alphanumeric
    } else {
        result
    }
}

/// Represents column inheritance rules for a model version
#[derive(Debug, Clone)]
struct GenericTestColumnInheritanceRules {
    includes: Vec<String>, // Empty vec means include all
    excludes: Vec<String>,
}
impl GenericTestColumnInheritanceRules {
    // Given a column block in a versioned model, return the includes and excludes for that model
    fn from_version_columns(columns: &Value) -> Option<Self> {
        if let Value::Array(cols) = columns {
            for col in cols {
                if let Value::Object(map) = col {
                    // Only create inheritance rules if there's an include or exclude
                    if map.contains_key("include") || map.contains_key("exclude") {
                        let includes = map
                            .get("include")
                            .map(|v| match v {
                                Value::String(s) if s == "*" || s == "all" => Vec::new(), // Empty vec means include all
                                Value::Array(arr) => arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect(),
                                Value::String(s) => vec![s.clone()],
                                _ => Vec::new(),
                            })
                            .unwrap_or_default(); // Default to empty vec (include all)

                        let excludes = map
                            .get("exclude")
                            .map(|v| match v {
                                Value::Array(arr) => arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect(),
                                Value::String(s) => vec![s.clone()],
                                _ => Vec::new(),
                            })
                            .unwrap_or_default();

                        return Some(GenericTestColumnInheritanceRules { includes, excludes });
                    }
                }
            }
        }
        None // No inheritance rules specified means use default (inherit all)
    }

    /// given a column name, return true if it should be included in the tests based on the includes and excludes and inheritance rules
    fn should_include_column(&self, column_name: &str) -> bool {
        if self.includes.is_empty() {
            // Empty includes means include all except excluded
            !self.excludes.contains(&column_name.to_string())
        } else {
            // Specific includes: must be in includes and not in excludes
            self.includes.contains(&column_name.to_string())
                && !self.excludes.contains(&column_name.to_string())
        }
    }
}

/// Represents test configuration for a model version
#[derive(Debug, Clone)]
struct GenericTestConfig {
    resource_type: String,
    resource_name: String,
    version_num: Option<String>,
    model_tests: Option<Vec<DataTests>>,
    column_tests: Option<BTreeMap<String, (bool, Vec<DataTests>)>>,
    source_name: Option<String>,
}

/// Generates the Jinja macro call for a generic test
fn generate_test_macro(
    test_macro_name: &str,
    kwargs: &BTreeMap<String, Value>,
    namespace: Option<&str>,
    config: &BTreeMap<String, Value>,
    jinja_set_vars: &BTreeMap<String, String>,
) -> FsResult<String> {
    let mut sql = String::new();

    // Add Jinja set blocks at the beginning of the file
    for (var_name, var_value) in jinja_set_vars {
        let set_val = if check_single_expression_without_whitepsace_control(var_value) {
            format!(
                "{{% set {} = {} %}}\n\n",
                var_name,
                &var_value[2..var_value.len() - 2].trim()
            )
        } else {
            format!("{{% set {var_name} %}}\n{var_value}\n{{% endset %}}\n\n")
        };
        sql.push_str(&set_val);
    }

    // Add config block if present
    if !config.is_empty() {
        // Convert config to a DataTestConfig and use its to_string method
        let config_str = render_config_to_kwargs(config);
        sql.push_str(&format!("{{{{ config({config_str}) }}}}\n"));
    }

    // Build test macro call with namespace
    // dbt allows referencing a macro of test_<name> using just <name> in data_tests
    // via the qualified_name prefix using 'test_'
    let qualified_name = if let Some(ns) = namespace {
        format!("{ns}.test_{test_macro_name}")
    } else {
        format!("test_{test_macro_name}")
    };

    // Format all kwargs, handling ref calls specially
    let formatted_args: Vec<String> = kwargs
        .iter()
        .map(|(k, v)| {
            let value_str = if let Value::String(s) = v {
                // Check if this is a reference to one of our Jinja set variables
                if s.starts_with("get_where_subquery(")
                    || s.starts_with("ref(")
                    || s.starts_with("source(")
                    || (s.starts_with('"') && s.ends_with('"'))
                    || jinja_set_vars.iter().any(|(var_name, _)| var_name == s)
                // Check if this is a reference to one of our Jinja set variables
                {
                    s.to_string() // Don't add quotes if it's already a ref, source, or already quoted
                } else {
                    let escaped = s
                        .replace('\\', "\\\\") // Escape backslashes
                        .replace('"', "\\\"") // Escape double quotes
                        .replace('{', "\\{") // Escape curly braces
                        .replace('}', "\\}"); // Escape closing curly braces

                    format!("\"{escaped}\"") // Do NOT add extra quotes
                }
            } else {
                v.to_string()
            };
            format!("{k}={value_str}")
        })
        .collect();

    sql.push_str(&format!(
        "{{{{ {}({}) }}}}",
        qualified_name,
        formatted_args.join(", ")
    ));
    Ok(sql)
}

fn render_config_to_kwargs(config: &BTreeMap<String, Value>) -> String {
    let value = serde_json::to_value(config).unwrap();

    // Convert to a map and filter out None values
    if let Value::Object(map) = value {
        let filtered_map: serde_json::Map<String, Value> =
            map.into_iter().filter(|(_, v)| !v.is_null()).collect();

        let output_str = serde_json::to_string(&Value::Object(filtered_map)).unwrap();

        // Process Jinja expressions more robustly
        process_jinja_expressions(&output_str)
    } else {
        // Fallback in case the value is not an object (shouldn't happen)
        serde_json::to_string(&value).unwrap()
    }
}

/// Processes a JSON string to find quoted Jinja expressions and remove the quotes
/// while properly handling escaped content within the expressions
fn process_jinja_expressions(input: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Look for "{{
        if i + 2 < chars.len() && chars[i] == '"' && chars[i + 1] == '{' && chars[i + 2] == '{' {
            // Found start of a potential Jinja expression
            let start_pos = i;
            i += 1; // Move past the opening quote

            let mut brace_count = 0;
            let mut jinja_end = None;

            // Find the matching }} and closing quote
            while i < chars.len() {
                if chars[i] == '{' && i + 1 < chars.len() && chars[i + 1] == '{' {
                    brace_count += 1;
                    i += 2;
                } else if chars[i] == '}' && i + 1 < chars.len() && chars[i + 1] == '}' {
                    brace_count -= 1;
                    i += 2;

                    // Check if this completes the expression and has a closing quote
                    if brace_count == 0 && i < chars.len() && chars[i] == '"' {
                        jinja_end = Some(i);
                        break;
                    }
                } else {
                    i += 1;
                }
            }

            if let Some(end_pos) = jinja_end {
                // Extract the content between the quotes, excluding the outer {{ and }}
                let full_content: String = chars[start_pos + 1..end_pos].iter().collect();

                // Remove the outer {{ and }} braces
                let inner_content =
                    if full_content.starts_with("{{") && full_content.ends_with("}}") {
                        &full_content[2..full_content.len() - 2]
                    } else {
                        &full_content
                    };

                // Remove escape sequences within the Jinja expression
                let unescaped_content = inner_content
                    .replace("\\\"", "\"") // Unescape double quotes
                    .replace("\\\\", "\\") // Unescape backslashes
                    .replace("\\{", "{") // Unescape opening braces
                    .replace("\\}", "}"); // Unescape closing braces

                result.push_str(&unescaped_content);
                i = end_pos + 1; // Move past the closing quote
            } else {
                // Not a valid Jinja expression, add the original character and continue
                result.push(chars[start_pos]);
                i = start_pos + 1;
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    result
}

impl<T> TryFrom<&TestableNode<'_, T>> for Vec<GenericTestConfig>
where
    T: TestableNodeTrait,
{
    // TODO this is currently infallible, we could implement From instead
    type Error = Box<FsError>;

    fn try_from(value: &TestableNode<T>) -> Result<Self, Self::Error> {
        let base = GenericTestConfig {
            resource_type: value.inner.resource_type().to_owned(),
            resource_name: value.inner.resource_name().to_owned(),
            version_num: None,
            model_tests: value.inner.base_tests()?,
            column_tests: value.inner.column_tests()?,
            source_name: value.inner.source_name(),
        };
        if let Some(versions) = value.inner.versions() {
            Ok(collect_versioned_model_tests(&base, versions))
        } else {
            Ok(vec![base])
        }
    }
}

// Given a model def from a properties file, and a list of versions,
// collect all the tests for each version and return a map of versioned model names to test configs
fn collect_versioned_model_tests(
    base_test_config: &GenericTestConfig,
    versions: &[Versions],
) -> Vec<GenericTestConfig> {
    let mut version_tests = vec![];
    // For each version, merge base tests with version-specific tests
    for version in versions {
        let version_suffix = match &version.v {
            Value::String(s) => Some(s.to_string()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
        .unwrap_or_else(|| {
            panic!("Version '{}' does not meet the required format", version.v);
        });

        // Start with base tests but set the version number
        let mut version_config = base_test_config.clone();
        version_config.version_num = Some(version_suffix.to_string());

        // Override with version-specific tests if they exist
        // Base model level tests are exclusive or with versioned model level tests
        if let Some(tests) = version
            .__additional_properties__
            .get("tests")
            .or_else(|| version.__additional_properties__.get("data_tests"))
        {
            if let Ok(version_tests) = serde_json::from_value::<Vec<DataTests>>(tests.clone()) {
                version_config.model_tests = Some(version_tests);
            }
        }

        // Handle version-specific column tests and inheritance
        if let Some(columns) = version.__additional_properties__.get("columns") {
            let mut column_tests = if let Some(inheritance_rules) =
                GenericTestColumnInheritanceRules::from_version_columns(columns)
            {
                // Apply inheritance rules
                base_test_config
                    .column_tests
                    .as_ref()
                    .map(|base_column_tests| {
                        base_column_tests
                            .iter()
                            .filter_map(|(col_name, tests)| {
                                if inheritance_rules.should_include_column(col_name) {
                                    Some((col_name.clone(), tests.clone()))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            } else {
                // No inheritance rules specified - inherit all column tests
                base_test_config.column_tests.clone().unwrap_or_default()
            };

            // Then handle any explicit column test definitions
            if let Ok(column_map) = serde_json::from_value::<Vec<ColumnProperties>>(columns.clone())
            {
                for col in column_map {
                    if let Some(tests) = col.tests.as_ref() {
                        column_tests.insert(
                            col.name.clone(),
                            (col.quote.unwrap_or(false), tests.clone()),
                        );
                    }
                }
            }

            if !column_tests.is_empty() {
                version_config.column_tests = Some(column_tests);
            }
        } else {
            // No columns section at all - inherit all column tests
            version_config.column_tests = base_test_config.column_tests.clone();
        }

        // Use versioned name as key
        version_tests.push(version_config);
    }
    version_tests
}

/// The minimal info we need to generate generic tests for a single dbt resource.
pub trait TestableNodeTrait {
    /// "model", "seed", "snapshot", or "source".
    fn resource_type(&self) -> &str;

    fn resource_name(&self) -> &str;

    fn unique_id(&self, project_name: &str, version: Option<&str>) -> String {
        if let Some(version) = version {
            format!(
                "{}.{}.{}.v{}",
                self.resource_type(),
                project_name,
                self.resource_name(),
                version
            )
        } else if let Some(source) = &self.source_name() {
            format!(
                "{}.{}.{}.{}",
                self.resource_type(),
                project_name,
                source,
                self.resource_name()
            )
        } else {
            format!(
                "{}.{}.{}",
                self.resource_type(),
                project_name,
                self.resource_name()
            )
        }
    }

    /// For _Tables from _Sources, return its corresponding source name.
    /// For everything else, return None.
    fn source_name(&self) -> Option<String> {
        None
    }

    /// Top-level tests (equivalent to "tests" or "data_tests").
    fn base_tests(&self) -> FsResult<Option<Vec<DataTests>>>;

    /// Columns, each with optional tests.
    #[allow(clippy::type_complexity)]
    fn column_tests(&self) -> FsResult<Option<BTreeMap<String, (bool, Vec<DataTests>)>>>;

    /// Versions for models, or None for everything else.
    fn versions(&self) -> Option<&[Versions]> {
        None
    }

    fn as_testable(&self) -> TestableNode<Self>
    where
        Self: Sized,
    {
        TestableNode { inner: self }
    }
}

impl TestableNodeTrait for ModelProperties {
    fn resource_type(&self) -> &str {
        "model"
    }

    fn resource_name(&self) -> &str {
        &self.name
    }

    fn base_tests(&self) -> FsResult<Option<Vec<DataTests>>> {
        base_tests_inner(self.tests.as_deref(), self.data_tests.as_deref())
    }

    fn column_tests(&self) -> FsResult<Option<BTreeMap<String, (bool, Vec<DataTests>)>>> {
        column_tests_inner(&self.columns)
    }

    fn versions(&self) -> Option<&[Versions]> {
        self.versions.as_deref()
    }
}

impl TestableNodeTrait for SeedProperties {
    fn resource_type(&self) -> &str {
        "seed"
    }

    fn resource_name(&self) -> &str {
        &self.name
    }

    fn base_tests(&self) -> FsResult<Option<Vec<DataTests>>> {
        base_tests_inner(self.tests.as_deref(), self.data_tests.as_deref())
    }

    fn column_tests(&self) -> FsResult<Option<BTreeMap<String, (bool, Vec<DataTests>)>>> {
        column_tests_inner(&self.columns)
    }
}

impl TestableNodeTrait for SnapshotProperties {
    fn resource_type(&self) -> &str {
        "snapshot"
    }

    fn resource_name(&self) -> &str {
        &self.name
    }

    fn base_tests(&self) -> FsResult<Option<Vec<DataTests>>> {
        base_tests_inner(self.tests.as_deref(), self.data_tests.as_deref())
    }

    fn column_tests(&self) -> FsResult<Option<BTreeMap<String, (bool, Vec<DataTests>)>>> {
        column_tests_inner(&self.columns)
    }
}

/// _Tables doesn't know its source, so we wrap it in a struct that does.
pub struct TestableTable<'a> {
    pub source_name: String,
    pub table: &'a Tables,
}

impl TestableNodeTrait for TestableTable<'_> {
    fn resource_type(&self) -> &str {
        "source"
    }

    fn resource_name(&self) -> &str {
        &self.table.name
    }

    fn source_name(&self) -> Option<String> {
        Some(self.source_name.clone())
    }

    fn base_tests(&self) -> FsResult<Option<Vec<DataTests>>> {
        base_tests_inner(
            self.table.tests.as_deref(),
            self.table.data_tests.as_deref(),
        )
    }

    fn column_tests(&self) -> FsResult<Option<BTreeMap<String, (bool, Vec<DataTests>)>>> {
        column_tests_inner(&self.table.columns)
    }
}

/// Extracts the kwargs and config for a generic test
fn extract_kwargs_and_config(
    source: &serde_json::Map<String, Value>,
    kwargs: &mut BTreeMap<String, Value>,
    config: &mut BTreeMap<String, Value>,
) -> FsResult<BTreeMap<String, String>> {
    // Check for reserved "model" argument first
    if source.contains_key("model") {
        return err!(
            ErrorCode::SchemaError,
            "Test arguments include \"model\", which is a reserved argument",
        );
    }

    let config_block = source.get("config").and_then(|v| v.as_object());
    let mut jinja_set_vars = BTreeMap::new();

    // Process all map entries in a single pass
    for (key, value) in source {
        match key.as_str() {
            "config" => continue,      // Skip config block, we'll process it at the end
            "description" => continue, // Skip description as per TODO
            // TODO: handle descriptions across all nodes
            // generic tests are particularly tricky becauase the node isn't created till resolve_singular tests
            "model" => unreachable!(), // We checked this above
            key if CONFIG_ARGS.contains(&key) => {
                // Check for duplicate config keys
                if let Some(config_map) = &config_block {
                    if config_map.contains_key(key) {
                        return err!(
                            ErrorCode::SchemaError,
                            "Test cannot have the same key at the top-level and in config",
                        );
                    }
                }
                config.insert(key.to_string(), value.clone());

                // Process value for config args just like regular kwargs
                process_kwarg(key, value, &mut jinja_set_vars, kwargs);
            }
            _ => {
                // Process value for regular kwargs
                process_kwarg(key, value, &mut jinja_set_vars, kwargs);
            }
        }
    }

    // Add config block values last so they take precedence
    if let Some(config_map) = config_block {
        config.extend(config_map.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    Ok(jinja_set_vars)
}

/// Helper function to process a kwarg value and detect if it needs a Jinja set block
fn process_kwarg(
    key: &str,
    value: &Value,
    jinja_set_vars: &mut BTreeMap<String, String>,
    kwargs: &mut BTreeMap<String, Value>,
) {
    if let Value::String(s) = value {
        if needs_jinja_set_block(s) {
            // Generate a unique var name based on the key with a prefix to avoid collisions
            // Add a random suffix to ensure uniqueness even with the same key name
            let var_name = format!("dbt_custom_arg_{key}");
            jinja_set_vars.insert(var_name.clone(), s.clone());
            kwargs.insert(key.to_string(), Value::String(var_name));
        } else {
            // For simple values, just use the value directly
            kwargs.insert(key.to_string(), value.clone());
        }
    } else {
        // For non-string values, use as is
        kwargs.insert(key.to_string(), value.clone());
    }
}

/// Determines if a string value needs to be wrapped in a Jinja set block
fn needs_jinja_set_block(value: &str) -> bool {
    // Check for multi-line content
    if value.contains('\n') {
        return true;
    }

    // Check for Jinja expressions
    if value.contains("{{") && value.contains("}}") {
        return true;
    }

    false
}

/// Normalizes a test name following the existing dbt behavior
/// https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/parser/generic_test_builders.py#L121-L122
fn normalize_test_name(input: &str) -> FsResult<String> {
    let name_pattern = Regex::new(r"^([a-zA-Z_][0-9a-zA-Z_]*)+").expect("Valid test name pattern");
    name_pattern
        .captures(input)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| fs_err!(ErrorCode::InvalidConfig, "Invalid test name: {}", input))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;

    #[test]
    fn test_no_double_quoting() {
        // Test case 1: Already double-quoted string
        let mut kwargs1 = BTreeMap::new();
        kwargs1.insert(
            "model".to_string(),
            Value::String("ref('my_model')".to_string()),
        );
        kwargs1.insert(
            "arg1".to_string(),
            Value::String("\"already quoted\"".to_string()),
        );

        // Test case 2: Already single-quoted string
        let mut kwargs2 = BTreeMap::new();
        kwargs2.insert(
            "model".to_string(),
            Value::String("ref('my_model')".to_string()),
        );
        kwargs2.insert(
            "arg1".to_string(),
            Value::String("'already quoted'".to_string()),
        );

        // Test case 3: Unquoted string that should get quotes
        let mut kwargs3 = BTreeMap::new();
        kwargs3.insert(
            "model".to_string(),
            Value::String("ref('my_model')".to_string()),
        );
        kwargs3.insert(
            "arg1".to_string(),
            Value::String("needs quotes".to_string()),
        );

        // Test case 4: ref call that shouldn't get quotes
        let mut kwargs4 = BTreeMap::new();
        kwargs4.insert(
            "model".to_string(),
            Value::String("ref('my_model')".to_string()),
        );
        kwargs4.insert(
            "arg1".to_string(),
            Value::String("ref('other_model')".to_string()),
        );

        // Test case 5: source call that shouldn't get quotes
        let mut kwargs5 = BTreeMap::new();
        kwargs5.insert(
            "model".to_string(),
            Value::String("ref('my_model')".to_string()),
        );
        kwargs5.insert(
            "arg1".to_string(),
            Value::String("source('src', 'tbl')".to_string()),
        );

        let config = BTreeMap::new();
        let test_name = "unique";
        let namespace = None;
        let jinja_set_vars = BTreeMap::new();

        // Generate test macros
        let result1 =
            generate_test_macro(test_name, &kwargs1, namespace, &config, &jinja_set_vars).unwrap();
        let result2 =
            generate_test_macro(test_name, &kwargs2, namespace, &config, &jinja_set_vars).unwrap();
        let result3 =
            generate_test_macro(test_name, &kwargs3, namespace, &config, &jinja_set_vars).unwrap();
        let result4 =
            generate_test_macro(test_name, &kwargs4, namespace, &config, &jinja_set_vars).unwrap();
        let result5 =
            generate_test_macro(test_name, &kwargs5, namespace, &config, &jinja_set_vars).unwrap();

        // Verify results - note that BTreeMap sorts keys alphabetically, so arg1 comes before model
        assert_eq!(
            result1,
            "{{ test_unique(arg1=\"already quoted\", model=ref('my_model')) }}"
        );
        assert_eq!(
            result2,
            "{{ test_unique(arg1=\"'already quoted'\", model=ref('my_model')) }}"
        );
        assert_eq!(
            result3,
            "{{ test_unique(arg1=\"needs quotes\", model=ref('my_model')) }}"
        );
        assert_eq!(
            result4,
            "{{ test_unique(arg1=ref('other_model'), model=ref('my_model')) }}"
        );
        assert_eq!(
            result5,
            "{{ test_unique(arg1=source('src', 'tbl'), model=ref('my_model')) }}"
        );

        // Test for no triple or quadruple quotes
        assert!(!result1.contains("\"\"\""));
        assert!(!result1.contains("\"\"\"\""));
        assert!(!result2.contains("'''"));
        assert!(!result2.contains("''''"));
    }

    #[test]
    fn test_jinja_set_var_extraction() {
        // Create a test input with a complex SQL query containing Jinja
        let mut test_args = serde_json::Map::new();
        test_args.insert(
            "model_column".to_string(),
            Value::String("num_in_motion_network_disruption_events".to_string()),
        );
        test_args.insert(
            "model_agg_type".to_string(),
            Value::String("sum".to_string()),
        );
        test_args.insert(
            "model_filter".to_string(),
            Value::String("event_date_utc >= DATEADD(DAY, -7, DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)))".to_string()),
        );

        // This is the complex SQL query with Jinja that should be extracted
        let complex_sql = "SELECT MD5( CONCAT( DATE_TRUNC('day', event_timestamp_utc), COALESCE(asset_external_id, '-99'), COALESCE(device_serial, '-99'),
            COALESCE(deployment_id, -99), COALESCE(app_version, '-99'), COALESCE(app_name, '-99'), COALESCE(android_os_version, '-99'), COALESCE(tablet_brand,
            '-99'), COALESCE(tablet_model, '-99'), COALESCE(last_heartbeat_cvd_esn, '-99'), COALESCE(last_heartbeat_cvd_type, '-99') ) )   AS pseudo_dbt_id,
            COUNT(DISTINCT event_timestamp_utc) AS num_events FROM {{ ref('connection_events_staging') }} WHERE DATE_TRUNC('DAY', event_timestamp_utc) >=
            DATEADD(DAY, -7, DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP))) AND last_heartbeat_speed > 0 AND event_name = 'NetworkChange' and
            state = 'DISCONNECTED' GROUP BY pseudo_dbt_id";

        test_args.insert(
            "upstream_model_cte".to_string(),
            Value::String(complex_sql.to_string()),
        );

        test_args.insert(
            "upstream_column".to_string(),
            Value::String("num_events".to_string()),
        );
        test_args.insert(
            "upstream_agg_type".to_string(),
            Value::String("sum".to_string()),
        );
        test_args.insert("upstream_filter".to_string(), Value::Null);
        test_args.insert("severity".to_string(), Value::String("warn".to_string()));

        // Process the args
        let mut kwargs = BTreeMap::new();
        let mut config = BTreeMap::new();
        let jinja_set_vars =
            extract_kwargs_and_config(&test_args, &mut kwargs, &mut config).unwrap();

        // Verify that the complex SQL was extracted
        assert!(
            !jinja_set_vars.is_empty(),
            "No Jinja set vars were extracted"
        );

        // Find the upstream_model_cte variable
        let extracted_var_name = kwargs.get("upstream_model_cte").and_then(|v| v.as_str());
        assert!(
            extracted_var_name.is_some(),
            "upstream_model_cte value should be a string variable reference"
        );

        let var_name = extracted_var_name.unwrap();
        assert!(
            jinja_set_vars.contains_key(var_name),
            "upstream_model_cte variable {var_name} not found in set vars"
        );

        let extracted_sql = jinja_set_vars.get(var_name).unwrap();
        assert_eq!(
            extracted_sql, complex_sql,
            "Extracted SQL doesn't match original"
        );

        // Verify that simple args were not extracted
        assert!(
            !jinja_set_vars.values().any(|v| v == "sum"),
            "Simple value 'sum' should not be extracted to a set var"
        );
    }

    #[test]
    fn test_generate_test_name_with_set_vars() {
        // Create test inputs
        let test_macro_name = "upstream_column_comparison";
        let project_name = "my_project";
        let test_config = GenericTestConfig {
            resource_type: "model".to_string(),
            resource_name: "my_model".to_string(),
            version_num: None,
            model_tests: None,
            column_tests: None,
            source_name: None,
        };

        // Create kwargs with a reference to a set variable
        let mut kwargs = BTreeMap::new();
        kwargs.insert(
            "model".to_string(),
            Value::String("ref('my_model')".to_string()),
        );
        kwargs.insert(
            "column_name".to_string(),
            Value::String("my_column".to_string()),
        );

        // This is the variable reference that should be replaced with its actual value
        let set_var_name = "dbt_parser_upstream_model_cte_12345";
        kwargs.insert(
            "upstream_model_cte".to_string(),
            Value::String(set_var_name.to_string()),
        );

        // Create the set variables map with the original SQL
        let mut jinja_set_vars = BTreeMap::new();
        let original_sql = "SELECT * FROM staging WHERE complex_condition";
        jinja_set_vars.insert(set_var_name.to_string(), original_sql.to_string());

        // Generate the test name
        let test_name = generate_test_name(
            test_macro_name,
            false,
            project_name,
            &test_config,
            &kwargs,
            None,
            &jinja_set_vars,
        );

        // Verify that the test name contains parts of the original SQL, not the variable name
        assert!(
            test_name.contains("SELECT"),
            "Test name should contain part of the original SQL"
        );
        assert!(
            !test_name.contains(set_var_name),
            "Test name should not contain the set variable name"
        );

        // Also test with an empty set vars map to ensure it still works
        let empty_set_vars = BTreeMap::new();
        let test_name_no_vars = generate_test_name(
            test_macro_name,
            false,
            project_name,
            &test_config,
            &kwargs,
            None,
            &empty_set_vars,
        );

        // In this case, the variable name should be used since there's no replacement
        assert!(
            test_name_no_vars.contains(set_var_name),
            "Test name should contain the variable name when no set vars are provided"
        );
    }

    #[test]
    fn test_needs_jinja_set_block() {
        // Multiline content
        assert!(
            needs_jinja_set_block("line1\nline2"),
            "Multiline content should need a set block"
        );

        // Content with Jinja expression
        assert!(
            needs_jinja_set_block("SELECT * FROM {{ ref('model') }}"),
            "Content with Jinja expression should need a set block"
        );

        // Simple string without Jinja
        assert!(
            !needs_jinja_set_block("simple string"),
            "Simple string should not need a set block"
        );

        // Unbalanced Jinja brackets shouldn't trigger (single opening bracket)
        assert!(
            !needs_jinja_set_block("Text with { one bracket"),
            "Text with single bracket should not need a set block"
        );

        // Unbalanced Jinja brackets shouldn't trigger (single closing bracket)
        assert!(
            !needs_jinja_set_block("Text with } one bracket"),
            "Text with single bracket should not need a set block"
        );
    }

    #[test]
    fn test_normalize_test_name_valid_cases() {
        let input_expected_pairs = vec![
            ("test", "test"),
            ("_test", "_test"),
            ("test_name", "test_name"),
            ("test+extra", "test"),
            ("valid::invalid", "valid"),
            ("name=with=equals", "name"),
            ("test+++", "test"),
        ];

        for (input, expected) in input_expected_pairs {
            match normalize_test_name(input) {
                Ok(result) => assert_eq!(
                    result, expected,
                    "Input '{input}' should normalize to '{expected}', got '{result}'"
                ),
                Err(e) => panic!("Expected success for input '{input}', got error: {e:?}"),
            }
        }
    }

    #[test]
    fn test_normalize_test_name_invalid_cases() {
        let invalid_cases = vec![
            "", "+test", "123test", "=test", ":test", "+++", "::::", "====", " test", "\ntest",
        ];

        for input in invalid_cases {
            assert!(normalize_test_name(input).is_err());
        }
    }
}
