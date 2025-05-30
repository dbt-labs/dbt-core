#![deny(missing_docs)]
//! This crate provides proc macros for the minijinja crate.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Derive macro for minijinja Object trait for structs that implement BaseRelation
#[proc_macro_derive(BaseRelationObject)]
pub fn derive_base_relation_object(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl minijinja::value::Object for #name {
            fn call_method(
                self: &std::sync::Arc<Self>,
                state: &minijinja::State,
                name: &str,
                args: &[minijinja::Value],
                _listener: std::rc::Rc<dyn minijinja::listener::RenderingEventListener>,
            ) -> Result<minijinja::Value, minijinja::Error> {
                match name {
                    "create_from" => self.create_from(state, args),
                    "replace_path" => self.replace_path(args),
                    "get" => self.get(args),
                    "render" => self.render_self(),
                    "without_identifier" => self.without_identifier(args),
                    "include" => self.include(args),
                    "incorporate" => self.incorporate(args),
                    "information_schema" => self.information_schema(args),
                    "relation_max_name_length" => self.relation_max_name_length(args),
                    // Below are available for Snowflake
                    "get_ddl_prefix_for_create" => self.get_ddl_prefix_for_create(args),
                    "get_ddl_prefix_for_alter" => self.get_ddl_prefix_for_alter(),
                    "needs_to_drop" => self.needs_to_drop(args),
                    "get_iceberg_ddl_options" => self.get_iceberg_ddl_options(args),
                    "dynamic_table_config_changeset" => self.dynamic_table_config_changeset(args),
                    "from_config" => self.from_config(args),
                    // Below are available for Databricks
                    "is_hive_metastore" => Ok(self.is_hive_metastore()),
                    _ => Err(minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("Unknown method on BaseRelationObject: '{}'", name),
                    )),
                }
            }

            fn get_value(self: &std::sync::Arc<Self>, key: &minijinja::Value) -> Option<minijinja::Value> {
                match key.as_str() {
                    Some("database") => Some(self.database()),
                    Some("schema") => Some(self.schema()),
                    Some("identifier") | Some("name") | Some("table") => Some(self.identifier()),
                    Some("is_table") => Some(Value::from(self.is_table())),
                    Some("is_view") => Some(Value::from(self.is_view())),
                    Some("is_materialized_view") => Some(Value::from(self.is_materialized_view())),
                    Some("is_cte") => Some(Value::from(self.is_cte())),
                    Some("is_pointer") => Some(Value::from(self.is_pointer())),
                    Some("type") => Some(self.relation_type_as_value()),
                    Some("can_be_renamed") => Some(Value::from(self.can_be_renamed())),
                    Some("can_be_replaced") => Some(Value::from(self.can_be_replaced())),
                    _ => None,
                }
            }
            fn enumerate(self: &std::sync::Arc<Self>) -> Enumerator {
                Enumerator::Str(&[
                    "database",
                    "schema",
                    "identifier",
                    "is_table",
                    "is_view",
                    "is_materialized_view",
                    "is_cte",
                    "is_pointer",
                    "can_be_renamed",
                    "can_be_replaced",
                    "name",
                ])
            }

            fn render(self: &std::sync::Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
            where
                Self: Sized + 'static,
            {
                let text = self.render_self().expect("could not render self");
                write!(
                    f,
                    "{}",
                    text
                )
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for minijinja Object trait for structs that implement StaticBaseRelation
#[proc_macro_derive(StaticBaseRelationObject)]
pub fn derive_static_base_relation_object(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl minijinja::value::Object for #name {
            fn call_method(
                self: &std::sync::Arc<Self>,
                _state: &minijinja::State,
                name: &str,
                args: &[minijinja::Value],
                _listener: std::rc::Rc<dyn minijinja::listener::RenderingEventListener>,
            ) -> Result<minijinja::Value, minijinja::Error> {
                match name {
                    "create" => Self::create(args),
                    "scd_args" => Ok(minijinja::Value::from(Self::scd_args(args))),
                    _ => Err(minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("Unknown method on StaticBaseRelationObject: '{}'", name),
                    )),
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for minijinja Object trait for structs that implement BaseColumn
#[proc_macro_derive(BaseColumnObject)]
pub fn derive_base_column_object(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {

        impl minijinja::value::Object for #name {
            fn call_method(
                self: &std::sync::Arc<Self>,
                state: &minijinja::State,
                name: &str,
                args: &[minijinja::Value],
                _listener: std::rc::Rc<dyn minijinja::listener::RenderingEventListener>,
            ) -> Result<minijinja::Value, minijinja::Error> {
                match name {
                    "is_string" => Ok(Value::from(self.is_string())),
                    "string_size" => Ok(Value::from(self.string_size()?)),
                    "is_number" => Ok(Value::from(self.is_number())),
                    "is_float" => Ok(Value::from(self.is_float())),
                    "is_integer" => Ok(Value::from(self.is_integer())),
                    "is_numeric" => Ok(Value::from(self.is_numeric())),
                    "can_expand_to" => {
                        let mut parser = ArgParser::new(args, None);
                        check_num_args(current_function_name!(), &parser, 1, 1)?;
                        let other = parser.get::<Value>("other_column")?;
                        let result = self.can_expand_to(other)?;
                        Ok(Value::from(result))
                    }
                    _ => Err(minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("Unknown method on BaseColumnObject: '{}'", name),
                    )),
                }
            }

            fn get_value(self: &std::sync::Arc<Self>, key: &minijinja::Value) -> Option<minijinja::Value> {
                match key.as_str() {
                    Some("name") | Some("column") => Some(self.name()),
                    Some("quoted") => Some(self.quoted()),
                    Some("data_type") => Some(self.data_type()),
                    Some("dtype") => Some(self.dtype()),
                    Some("char_size") => Some(self.char_size()),
                    Some("numeric_precision") => Some(self.numeric_precision()),
                    Some("numeric_scale") => Some(self.numeric_scale()),
                    _ => None,
                }
            }

            fn enumerate(self: &std::sync::Arc<Self>) -> Enumerator {
                Enumerator::Str(&[
                    "name",
                    "dtype",
                    "char_size",
                    "column",
                    "quoted",
                    "numeric_precision",
                    "numeric_scale"
                ])
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for minijinja Object trait for structs that implement StaticBaseColumn
#[proc_macro_derive(StaticBaseColumnObject)]
pub fn derive_static_base_column_object(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl minijinja::value::Object for #name {
            fn call_method(
                self: &std::sync::Arc<Self>,
                _state: &minijinja::State,
                name: &str,
                args: &[minijinja::Value],
                _listener: std::rc::Rc<dyn minijinja::listener::RenderingEventListener>,
            ) -> Result<minijinja::Value, minijinja::Error> {
                match name {
                    "create" => Self::create(args),
                    "translate_type" => Self::translate_type(args),
                    "numeric_type" => Self::numeric_type(args),
                    "string_type" => Self::string_type(args),
                    "from_description" => Self::from_description(args),
                    _ => Err(minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("Unknown method on StaticBaseColumnObject: '{}'", name),
                    )),
                }
            }

            fn call(
                self: &std::sync::Arc<Self>,
                _state: &minijinja::State,
                args: &[minijinja::Value],
                _listener: std::rc::Rc<dyn minijinja::listener::RenderingEventListener>,
            ) -> Result<minijinja::Value, minijinja::Error> {
                Self::create(args)
            }
        }
    };

    TokenStream::from(expanded)
}
