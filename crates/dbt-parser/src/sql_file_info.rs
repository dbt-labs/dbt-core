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
    /// e.g. metric('a', 'b')
    pub metrics: Vec<(String, Option<String>)>,
    /// e.g. config( a= 1, b = [1,2], c = 'string')
    pub config: Box<T>,
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
            metrics: Vec::new(),
            config: Box::new(T::default()),
            tests: Vec::new(),
            macros: Vec::new(),
            materializations: Vec::new(),
            docs: Vec::new(),
            snapshots: Vec::new(),
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
        let mut metrics = Vec::new();
        let mut config = Box::new(T::default());
        let mut tests = Vec::new();
        let mut macros = Vec::new();
        let mut materializations = Vec::new();
        let mut docs = Vec::new();
        let mut snapshots = Vec::new();

        for resource in resources {
            match resource {
                SqlResource::Source(source) => sources.push(source),
                SqlResource::Ref(reference) => refs.push(reference),
                SqlResource::Metric(metric) => metrics.push(metric),
                SqlResource::Config(mut resource_config) => {
                    resource_config.default_to(&*config);
                    config = resource_config;
                }
                SqlResource::Test(name, span) => tests.push((name, span)),
                SqlResource::Macro(name, span, func_sign, args) => {
                    macros.push((name, span, func_sign, args))
                }
                SqlResource::Materialization(name, adapter, span) => {
                    materializations.push((name, adapter, span))
                }
                SqlResource::Doc(name, span) => docs.push((name, span)),
                SqlResource::Snapshot(name, span) => snapshots.push((name, span)),
            }
        }

        SqlFileInfo {
            sources,
            refs,
            metrics,
            config,
            tests,
            macros,
            materializations,
            docs,
            snapshots,
            checksum,
            execute,
        }
    }
}
