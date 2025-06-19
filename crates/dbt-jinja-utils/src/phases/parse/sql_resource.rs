//! This module contains the resources that are encountered while rendering sql and macros.

use std::fmt::Debug;

use dbt_schemas::schemas::project::DefaultTo;

use dbt_frontend_common::error::CodeLocation;
use minijinja::machinery::Span;

/// Resources that are encountered while rendering sql and macros
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlResource<T: DefaultTo<T>> {
    /// A source call (e.g. `{{ source('a', 'b') }}`)
    Source((String, String, CodeLocation)),
    /// A ref call (e.g. `{{ ref('a', 'b') }}`)
    Ref((String, Option<String>, Option<String>, CodeLocation)),
    /// A metric call (e.g. `{{ metric('a', 'b') }}`)
    Metric((String, Option<String>)),
    // If all can be made numeric it is ordered numerically, if not it is ordered lexicographically
    /// A config call (e.g. `{{ config(database='a', schema='b') }}`)
    Config(Box<T>),
    /// A test definition (e.g. `{% test foo() %}`)
    Test(String, Span),
    /// A macro definition (e.g. `{% macro my_macro(a, b) %}`)
    Macro(String, Span),
    /// A docs definition (e.g. `{% docs my_docs %}`)
    Doc(String, Span),
    /// A snapshot definition (e.g. `{% snapshot my_snapshot %}`)
    Snapshot(String, Span),
    /// A materialization macro definition (e.g. `{% materialization my_materialization, adapter='snowflake' %}`)
    Materialization(String, String, Span),
}

impl<T: DefaultTo<T>> std::fmt::Display for SqlResource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            SqlResource::Snapshot(name, span) => {
                write!(f, "Snapshot({} {:#?})", name, span)
            }
        }
    }
}
