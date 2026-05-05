use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    Expr, Field, Fields, GenericArgument, Ident, LitStr, PathArguments, Type, Variant,
    spanned::Spanned,
};

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
                    if let Some((eq, Expr::Lit(lit))) = &variant.discriminant
                        && let syn::Lit::Int(int) = &lit.lit
                    {
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
                    };
                    None
                }),
        );

    let output = quote::quote! {
        #err_code_def
    };
    output.into()
}

/// `#[derive(Resolvable)]` generates a flat `Resolved{StructName}` counterpart for a config struct.
///
/// Place on a config struct. Fields annotated with `#[resolved(promote)]` are promoted from
/// `Option<T>` to `T` in the generated struct. All other fields are copied verbatim, except that
/// `serde` and `schemars` attributes are stripped from verbatim and `or_else` fields in the
/// generated struct.
///
/// **Required:** the annotated struct must have an `enabled: bool` field, as the generated
/// `ResolvedConfig` impl unconditionally references it.
///
/// Generates alongside the annotated struct:
/// - `pub struct Resolved{Name} { ... }` with `#[derive(Clone, Debug)]`
/// - `impl From<Resolved{Name}> for {Name}` (back-conversion, promoted fields wrapped in `Some`)
/// - `impl crate::schemas::project::dbt_project::ResolvedConfig for Resolved{Name}`
///   - Always includes `enabled()` via the `enabled` field
///   - If the struct has a `pre_hook` field: generates `get_pre_hook()`
///   - If the struct has a `post_hook` field: generates `get_post_hook()`
///   - If the struct has a `static_analysis` field: generates `get_static_analysis()`
///   - These optional methods override the `ResolvedConfig` trait's default `None`
///     implementations, enabling generic code bounded by `ResolvedConfig` to access
///     these fields uniformly across all config types.
/// - `impl {Name} { pub fn finalize_resolved(self) -> Resolved{Name} { ... } }`
///
/// Field annotations control how each promoted field is initialized in `finalize_resolved`:
/// - `#[resolved(promote)]` → `self.field.unwrap_or_default()`
/// - `#[resolved(promote, method = name)]` → `self.name()`
/// - `#[resolved(promote, default = expr)]` → generates `pub fn default_field() -> T { expr }`
///   on the struct and uses `self.field.unwrap_or_else(Self::default_field)` in `finalize_resolved`
/// - `#[resolved(promote, expect = "msg")]` → `self.field.expect("msg")`
/// - `#[resolved(or_else = expr)]` → `self.field.or_else(|| expr)` (stays `Option<T>`)
///
/// **Static vs. dynamic defaults:** `default = expr` and `unwrap_or_default()` are evaluated
/// inside `finalize_resolved` and are therefore fully static — they cannot depend on runtime
/// values such as the CLI's `--static-analysis` flag. For fields that need a runtime-supplied
/// default (e.g. `static_analysis`), use `#[resolved(promote, expect = "…")]` together with
/// `ResolvableConfig::apply_resolve_defaults`, which is called just before `finalize()` and receives
/// the `ResolveDefaults` value from `ProjectConfigResolver::with_resolve_defaults`.
#[proc_macro_derive(Resolvable, attributes(resolved))]
pub fn resolvable_derive(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = syn::parse_macro_input!(item as syn::ItemStruct);

    let struct_name = input.ident.clone();
    let resolved_name = Ident::new(&format!("Resolved{}", struct_name), Span::call_site());

    let named_fields = match &mut input.fields {
        Fields::Named(f) => &mut f.named,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "#[derive(Resolvable)] requires a struct with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut collector = FieldCollector::default();

    for field in named_fields.iter_mut() {
        if let Err(e) = collector.process_field(field) {
            return e;
        }
    }

    let FieldCollector {
        resolved_field_defs,
        from_assignments,
        finalize_pre_lets,
        finalize_assignments,
        static_default_methods,
        has_pre_hook,
        has_post_hook,
        static_analysis_verbatim_ty,
        has_static_analysis_promoted,
    } = collector;

    let resolved_config_trait = quote! { crate::schemas::project::dbt_project::ResolvedConfig };

    let pre_hook_method = if has_pre_hook {
        quote! {
            fn get_pre_hook(&self) -> ::core::option::Option<&crate::schemas::common::Hooks> {
                (*self.pre_hook).as_ref()
            }
        }
    } else {
        quote! {}
    };
    let post_hook_method = if has_post_hook {
        quote! {
            fn get_post_hook(&self) -> ::core::option::Option<&crate::schemas::common::Hooks> {
                (*self.post_hook).as_ref()
            }
        }
    } else {
        quote! {}
    };

    let static_analysis_method = if has_static_analysis_promoted {
        quote! {
            fn get_static_analysis(&self) -> ::core::option::Option<dbt_yaml::Spanned<dbt_common::io_args::StaticAnalysisKind>> {
                ::core::option::Option::Some(self.static_analysis.clone())
            }
        }
    } else if let Some(ref sa_outer_ty) = static_analysis_verbatim_ty {
        // Generate accessor based on whether the inner Option type is Spanned<T> or T directly
        let inner_ty = extract_option_inner(sa_outer_ty);
        if inner_ty.is_some_and(is_spanned_type) {
            quote! {
                fn get_static_analysis(&self) -> ::core::option::Option<dbt_yaml::Spanned<dbt_common::io_args::StaticAnalysisKind>> {
                    self.static_analysis.clone()
                }
            }
        } else {
            quote! {
                fn get_static_analysis(&self) -> ::core::option::Option<dbt_yaml::Spanned<dbt_common::io_args::StaticAnalysisKind>> {
                    self.static_analysis.map(dbt_yaml::Spanned::new)
                }
            }
        }
    } else {
        quote! {}
    };

    let generated = quote! {
        #[derive(Clone, Debug)]
        pub struct #resolved_name {
            #(#resolved_field_defs,)*
        }

        impl ::core::convert::From<#resolved_name> for #struct_name {
            fn from(r: #resolved_name) -> Self {
                Self {
                    #(#from_assignments,)*
                }
            }
        }

        impl #resolved_config_trait for #resolved_name {
            fn enabled(&self) -> bool {
                self.enabled
            }
            #pre_hook_method
            #post_hook_method
            #static_analysis_method
        }
    };

    let finalize_impl = quote! {
        impl #struct_name {
            pub fn finalize_resolved(self) -> #resolved_name {
                #(#finalize_pre_lets)*
                #resolved_name {
                    #(#finalize_assignments,)*
                }
            }
            #(#static_default_methods)*
        }
    };

    let output = quote! {
        #generated
        #finalize_impl
    };

    output.into()
}

#[derive(Default)]
struct FieldCollector {
    resolved_field_defs: Vec<TokenStream2>,
    from_assignments: Vec<TokenStream2>,
    // Pre-bindings (let x = self.method()) computed before the struct literal to avoid
    // "borrow of partially moved value" when method borrows self before other fields are moved.
    finalize_pre_lets: Vec<TokenStream2>,
    finalize_assignments: Vec<TokenStream2>,
    // Generated default_<field>() methods for static_default fields
    static_default_methods: Vec<TokenStream2>,
    has_pre_hook: bool,
    has_post_hook: bool,
    // Holds the Option<T> field type for verbatim static_analysis (to inspect inner T)
    static_analysis_verbatim_ty: Option<Type>,
    has_static_analysis_promoted: bool,
}

impl FieldCollector {
    fn process_field(&mut self, field: &mut Field) -> Result<(), proc_macro::TokenStream> {
        let field_name = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;
        let vis = &field.vis;

        let resolved_pos = field
            .attrs
            .iter()
            .position(|a| a.path().is_ident("resolved"));

        if let Some(pos) = resolved_pos {
            let resolved_attr = field.attrs.remove(pos);
            let mut promote = false;
            let mut method_name: Option<Ident> = None;
            let mut default_expr: Option<Expr> = None;
            let mut expect_msg: Option<LitStr> = None;
            let mut or_else_expr: Option<Expr> = None;

            let _ = resolved_attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("promote") {
                    promote = true;
                } else if meta.path.is_ident("method") {
                    let value = meta.value()?;
                    method_name = Some(value.parse()?);
                } else if meta.path.is_ident("default") {
                    let value = meta.value()?;
                    default_expr = Some(value.parse()?);
                } else if meta.path.is_ident("expect") {
                    let value = meta.value()?;
                    expect_msg = Some(value.parse()?);
                } else if meta.path.is_ident("or_else") {
                    let value = meta.value()?;
                    or_else_expr = Some(value.parse()?);
                }
                Ok(())
            });

            if promote {
                let Some(inner_ty) = extract_option_inner(field_ty) else {
                    return Err(syn::Error::new_spanned(
                        field_ty,
                        "#[resolved(promote)] requires an Option<T> field type",
                    )
                    .to_compile_error()
                    .into());
                };
                if field_name == "static_analysis" {
                    self.has_static_analysis_promoted = true;
                }
                self.resolved_field_defs
                    .push(quote! { pub #field_name: #inner_ty });
                self.from_assignments
                    .push(quote! { #field_name: ::core::option::Option::Some(r.#field_name) });
                // Method calls borrow `self`, so extract as `let` bindings before the struct
                // literal to avoid "borrow of partially moved value".
                if let Some(method) = &method_name {
                    self.finalize_pre_lets
                        .push(quote! { let #field_name = self.#method(); });
                    self.finalize_assignments.push(quote! { #field_name });
                } else if let Some(expr) = &default_expr {
                    let default_method =
                        Ident::new(&format!("default_{}", field_name), resolved_attr.span());
                    self.static_default_methods.push(quote! {
                        pub fn #default_method() -> #inner_ty {
                            #expr
                        }
                    });
                    self.finalize_assignments.push(quote! {
                        #field_name: self.#field_name.unwrap_or_else(Self::#default_method)
                    });
                } else if let Some(msg) = &expect_msg {
                    self.finalize_assignments
                        .push(quote! { #field_name: self.#field_name.expect(#msg) });
                } else {
                    self.finalize_assignments
                        .push(quote! { #field_name: self.#field_name.unwrap_or_default() });
                }
                return Ok(());
            }

            // Handle or_else: field stays Option<T> in resolved struct but gets a
            // .or_else(|| #expr) transform in finalize_resolved.
            if let Some(or_else) = &or_else_expr {
                self.track_special_field(field_name, field_ty);
                let other_attrs: Vec<_> = field
                    .attrs
                    .iter()
                    .filter(|a| !a.path().is_ident("serde") && !a.path().is_ident("schemars"))
                    .collect();
                self.resolved_field_defs
                    .push(quote! { #(#other_attrs)* #vis #field_name: #field_ty });
                self.from_assignments
                    .push(quote! { #field_name: r.#field_name });
                self.finalize_assignments
                    .push(quote! { #field_name: self.#field_name.or_else(|| #or_else) });
                return Ok(());
            }
        }

        // Verbatim field: copy to resolved struct as-is, strip serde/schemars attrs.
        self.track_special_field(field_name, field_ty);
        let other_attrs: Vec<_> = field
            .attrs
            .iter()
            .filter(|a| !a.path().is_ident("serde") && !a.path().is_ident("schemars"))
            .collect();
        self.resolved_field_defs
            .push(quote! { #(#other_attrs)* #vis #field_name: #field_ty });
        self.from_assignments
            .push(quote! { #field_name: r.#field_name });
        self.finalize_assignments
            .push(quote! { #field_name: self.#field_name });
        Ok(())
    }

    fn track_special_field(&mut self, field_name: &Ident, field_ty: &Type) {
        if field_name == "pre_hook" {
            self.has_pre_hook = true;
        } else if field_name == "post_hook" {
            self.has_post_hook = true;
        } else if field_name == "static_analysis" {
            self.static_analysis_verbatim_ty = Some(field_ty.clone());
        }
    }
}

fn is_spanned_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if type_path.qself.is_none() {
            let segments = &type_path.path.segments;
            if let Some(last) = segments.last() {
                return last.ident == "Spanned";
            }
        }
    }
    false
}

fn extract_option_inner(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        if type_path.qself.is_none() {
            let segments = &type_path.path.segments;
            if segments.len() == 1 && segments[0].ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segments[0].arguments {
                    if args.args.len() == 1 {
                        if let GenericArgument::Type(inner) = &args.args[0] {
                            return Some(inner);
                        }
                    }
                }
            }
        }
    }
    None
}
