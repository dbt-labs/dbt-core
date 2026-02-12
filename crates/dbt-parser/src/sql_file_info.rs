//! This module contains the SqlFileInfo struct, which is used to collect details about processed sql files.

use dbt_frontend_common::error::CodeLocation;
use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
use dbt_schemas::schemas::{common::DbtChecksum, project::DefaultTo};
use minijinja::{ArgSpec, machinery::Span};

/// Collected details about processed sql files
#[derive(Debug, Clone)]
pub struct SqlFileInfo<T: DefaultTo<T>> {
    /// e.g. source('a', 'b')
    pub sources: Vec<(String, String, CodeLocation)>,
    /// e.g. ref('a', 'b', 'c')
    pub refs: Vec<(String, Option<String>, Option<String>, CodeLocation)>,
    /// true if `this` is referenced in this .sql file, otherwise false
    pub this: bool,
    /// e.g. metric('a', 'b')
    pub metrics: Vec<(String, Option<String>)>,
    /// e.g. config( a= 1, b = [1,2], c = 'string')
    pub config: Box<T>,
    /// Merged config values from explicit SQL `{{ config(...) }}` calls only.
    ///
    /// This intentionally excludes the initial "base" config pushed into the parse context.
    /// Used to detect which values were explicitly overridden inline in SQL.
    pub explicit_config: Option<Box<T>>,
    /// e.g. tests
    pub tests: Vec<(String, Span)>,
    /// e.g. macros
    pub macros: Vec<(String, Span, Option<String>, Vec<ArgSpec>)>,
    /// e.g. materializations
    pub materializations: Vec<(String, String, Span)>,
    /// e.g. docs
    pub docs: Vec<(String, Span)>,
    /// e.g. snapshots
    pub snapshots: Vec<(String, Span)>,
    /// e.g. functions
    pub functions: Vec<(String, Option<String>, CodeLocation)>,
    /// e.g. checksums
    pub checksum: DbtChecksum,
    /// true if `execute` flag exists in this .sql file, otherwise false
    pub execute: bool,
}

impl<T: DefaultTo<T>> Default for SqlFileInfo<T> {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            refs: Vec::new(),
            this: false,
            metrics: Vec::new(),
            config: Box::new(T::default()),
            explicit_config: None,
            tests: Vec::new(),
            macros: Vec::new(),
            materializations: Vec::new(),
            docs: Vec::new(),
            snapshots: Vec::new(),
            functions: Vec::new(),
            checksum: DbtChecksum::default(),
            execute: false,
        }
    }
}

impl<T: DefaultTo<T>> SqlFileInfo<T> {
    /// Create a new SqlFileInfo from a list of SqlResources
    pub fn from_sql_resources(
        resources: Vec<SqlResource<T>>,
        checksum: DbtChecksum,
        execute: bool,
    ) -> Self {
        let mut sources = Vec::new();
        let mut refs = Vec::new();
        let mut this = false;
        let mut metrics = Vec::new();
        let mut config = Box::new(T::default());
        let mut explicit_config: Option<Box<T>> = None;
        let mut tests = Vec::new();
        let mut macros = Vec::new();
        let mut materializations = Vec::new();
        let mut docs = Vec::new();
        let mut snapshots = Vec::new();
        let mut functions = Vec::new();

        for resource in resources {
            match resource {
                SqlResource::Source(source) => sources.push(source),
                SqlResource::Ref(reference) => refs.push(reference),
                SqlResource::This => this = true,
                SqlResource::Function(function) => functions.push(function),
                SqlResource::Metric(metric) => metrics.push(metric),
                SqlResource::BaseConfig(mut resource_config) => {
                    // The parse context always pushes exactly one initial "base" config before
                    // evaluating the file.
                    resource_config.default_to(&*config);
                    config = resource_config;
                }
                SqlResource::ConfigCall(mut resource_config) => {
                    // Merge explicit SQL config calls together, excluding the base config.
                    // This preserves dbt's precedence across multiple `config()` calls
                    // while avoiding falsely treating inherited/defaulted values as explicit.
                    let mut explicit_call = resource_config.clone();
                    if let Some(prev) = explicit_config.as_deref() {
                        explicit_call.default_to(prev);
                    }
                    explicit_config = Some(explicit_call);

                    resource_config.default_to(&*config);
                    config = resource_config;
                }
                SqlResource::Test(name, span, _) => tests.push((name, span)),
                SqlResource::Macro(name, span, func_sign, args, _) => {
                    macros.push((name, span, func_sign, args))
                }
                SqlResource::Materialization(name, adapter, span, _) => {
                    materializations.push((name, adapter, span))
                }
                SqlResource::Doc(name, span) => docs.push((name, span)),
                SqlResource::Snapshot(name, span, _) => snapshots.push((name, span)),
            }
        }

        SqlFileInfo {
            sources,
            refs,
            this,
            metrics,
            config,
            explicit_config,
            tests,
            macros,
            materializations,
            docs,
            snapshots,
            functions,
            checksum,
            execute,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SqlFileInfo;
    use dbt_jinja_utils::phases::parse::sql_resource::SqlResource;
    use dbt_schemas::schemas::common::DbtChecksum;
    use dbt_schemas::schemas::project::ModelConfig;

    #[test]
    fn explicit_config_is_only_from_config_calls_and_last_call_wins() {
        // Base config only => explicit_config must remain None.
        let base = Box::new(ModelConfig::default());
        let info = SqlFileInfo::from_sql_resources(
            vec![SqlResource::BaseConfig(base)],
            DbtChecksum::default(),
            false,
        );
        assert!(info.explicit_config.is_none());

        // Multiple config calls => explicit_config should merge, with later calls taking precedence.
        let call1 = ModelConfig {
            alias: Some("a1".to_string()),
            schema: dbt_common::serde_utils::Omissible::Present(Some("s1".to_string())),
            ..Default::default()
        };

        let call2 = ModelConfig {
            alias: Some("a2".to_string()), // overrides call1 alias
            ..Default::default()
        };
        // schema omitted here => should be inherited from call1 in explicit_config

        let info = SqlFileInfo::from_sql_resources(
            vec![
                SqlResource::BaseConfig(Box::new(ModelConfig::default())),
                SqlResource::ConfigCall(Box::new(call1)),
                SqlResource::ConfigCall(Box::new(call2)),
            ],
            DbtChecksum::default(),
            false,
        );

        let explicit = info
            .explicit_config
            .expect("expected explicit_config to be set");
        assert_eq!(explicit.alias.as_deref(), Some("a2"));
        assert_eq!(
            explicit
                .schema
                .clone()
                .into_inner()
                .unwrap_or(None)
                .as_deref(),
            Some("s1")
        );
    }
}
