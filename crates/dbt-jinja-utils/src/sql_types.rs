//! Module contains the types for resources encountered while
//! rendering sql and macros
use std::fmt;

use dbt_schemas::schemas::dbt_manifest::DbtConfig;
use minijinja::machinery::Span;
use dbt_frontend_common::error::CodeLocation;

/// Resources that are encountered while rendering sql and macros
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlResource {
    /// A source call (e.g. `{{ source('a', 'b') }}`)
    Source((String, String, CodeLocation)),
    /// A ref call (e.g. `{{ ref('a', 'b') }}`)
    Ref((String, Option<String>, Option<String>, CodeLocation)), // Note (Ani + Michelle): could be a string or an int could mess up ordering
    /// A metric call (e.g. `{{ metric('a', 'b') }}`)
    Metric((String, Option<String>)),
    // If all can be made numeric it is ordered numerically, if not it is ordered lexicographically
    /// A config call (e.g. `{{ config(database='a', schema='b') }}`)
    Config(Box<DbtConfig>),
    /// A test definition (e.g. `{% test foo() %}`)
    Test(String, Span),
    /// A macro definition (e.g. `{% macro my_macro(a, b) %}`)
    Macro(String, Span),
    /// A docs definition (e.g. `{% docs my_docs %}`)
    Doc(String, Span),
    /// A materialization macro definition (e.g. `{% materialization my_materialization, adapter='snowflake' %}`)
    Materialization(String, String, Span),
}

impl fmt::Display for SqlResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlResource::Source((a, b, location)) => {
                write!(f, "Source({}, {}, {:?})", a, b, location)
            }
            SqlResource::Ref((a, b, c, location)) => {
                write!(f, "Ref({}, {:?}, {:?}, {:?})", a, b, c, location)
            }
            SqlResource::Metric((a, b)) => {
                write!(f, "Metric({}, {:?})", a, b)
            }
            SqlResource::Config(config) => write!(f, "Config({:?})", config),
            SqlResource::Test(name, span) => write!(f, "Test({} {:#?})", name, span),
            SqlResource::Macro(name, span) => write!(f, "Macro({} {:#?})", name, span),
            SqlResource::Doc(name, span) => write!(f, "Docs({} {:#?})", name, span),
            SqlResource::Materialization(name, adapter, span) => {
                write!(f, "Materialization({} {} {:#?})", name, adapter, span)
            }
        }
    }
}
