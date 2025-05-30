use syn::{Ident, Variant};

extern crate proc_macro;

const FRONTEND_ERROR_CODES: &str = include_str!("../../dbt-frontend-common/src/error/codes.rs");

/// This macro is used to include the error codes from the frontend crate into
/// the CLI crate. This way we don't need to manually sync the error codes.
#[proc_macro_attribute]
pub fn include_frontend_error_codes(
    _args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let ast = syn::parse_file(FRONTEND_ERROR_CODES).expect("Could not parse error codes file");
    let frontend_err_code_def = ast
        .items
        .into_iter()
        .find(|item| {
            if let syn::Item::Enum(err_def) = item {
                err_def.ident == "ErrorCode"
            } else {
                false
            }
        })
        .map(|item| {
            if let syn::Item::Enum(err_def) = item {
                err_def
            } else {
                unreachable!()
            }
        })
        .expect("Could not find ErrorCode enum definition");
    let mut err_code_def = syn::parse_macro_input!(item as syn::ItemEnum);
    err_code_def
        .variants
        .extend(
            frontend_err_code_def
                .variants
                .into_iter()
                .filter_map(|variant| {
                    if let Some((eq, syn::Expr::Lit(lit))) = &variant.discriminant {
                        if let syn::Lit::Int(int) = &lit.lit {
                            let code = int.base10_parse::<u16>().expect("Invalid error code");
                            if code < 900 {
                                // Regular errors just map to the same code
                                return Some(variant);
                            } else {
                                // Internal errors map to the 9k range
                                return Some(Variant {
                                    ident: Ident::new(
                                        &format!("Frontend{}", variant.ident),
                                        variant.ident.span(),
                                    ),
                                    discriminant: Some((*eq, syn::parse_quote!(#code + 9000))),
                                    ..variant
                                });
                            }
                        }
                    };
                    None
                }),
        );

    let output = quote::quote! {
        #err_code_def
    };
    output.into()
}
