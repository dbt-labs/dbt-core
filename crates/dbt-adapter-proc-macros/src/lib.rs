#![deny(missing_docs)]
//! This crate provides proc macros for the minijinja crate.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

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
                    // Below are DatabricksColumn-only
                    "format_add_column_list" => Self::format_add_column_list(args),
                    "format_remove_column_list" => Self::format_remove_column_list(args),
                    "get_name" => Self::get_name(args),
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
