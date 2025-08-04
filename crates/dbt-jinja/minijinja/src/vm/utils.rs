use std::path::Path;
use std::sync::Arc;

use dashmap::DashMap;

use crate::compiler::ast;
use crate::types::funcsign_parser::parse;
use crate::types::function::{Argument, UndefinedFunctionType, UserDefinedFunctionType};
use crate::types::{DynObject, Type};

use crate::compiler::typecheck::FunctionRegistry;
use crate::machinery::Span;
use crate::types::utils::CodeLocation;

/// Find macro signatures in the template.
///
/// This function is used to find macro signatures in the template.
/// It is used to type check the template.
pub fn find_macro_signatures(
    root: &ast::Stmt,
    last_func_sign: &mut Option<(Span, String)>,
    funcsigns: &mut FunctionRegistry,
    path: &Path,
    registry: Arc<DashMap<String, Type>>,
) -> Result<(), crate::Error> {
    match root {
        ast::Stmt::Template(template) => {
            for child in template.children.iter() {
                find_macro_signatures(child, last_func_sign, funcsigns, path, registry.clone())?;
            }
        }
        ast::Stmt::EmitRaw(emit_raw) => {
            // find "-- funcsign:" in emit_raw.raw
            let raw = emit_raw.raw.trim();
            if raw.contains("-- funcsign: ") {
                *last_func_sign = Some((
                    emit_raw.span,
                    raw.split("-- funcsign: ")
                        .nth(1)
                        .unwrap()
                        .trim()
                        .to_string(),
                ));
            } else {
                *last_func_sign = None;
            }
        }
        ast::Stmt::Macro((macro_decl, _, _)) => {
            let macro_name = macro_decl.name;
            if let Some((span, func_sign)) = last_func_sign {
                if span.start_line >= macro_decl.span.start_line {
                    return Err(crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        "[BUG] funcsign is after macro declaration",
                    ));
                }
                let (arg_types, returns) = parse(func_sign, registry).map_err(|e| {
                    crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        format!("failed to parse funcsign in {path:?}: {e}"),
                    )
                })?;

                if arg_types.len() != macro_decl.args.len() {
                    return Err(crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        format!(
                            "{}: funcsign has {} args, but macro has {} args",
                            macro_name,
                            arg_types.len(),
                            macro_decl.args.len()
                        ),
                    ));
                }

                let non_optional_args_len = macro_decl.args.len() - macro_decl.defaults.len();

                let args = macro_decl
                    .args
                    .iter()
                    .zip(arg_types.iter())
                    .enumerate()
                    .map(|(i, (arg, arg_type))| match arg {
                        ast::Expr::Var(spanned) => Argument {
                            name: spanned.id.to_string(),
                            type_: arg_type.clone(),
                            is_optional: i >= non_optional_args_len,
                        },
                        _ => todo!(),
                    })
                    .collect::<Vec<_>>();

                funcsigns.insert(
                    macro_name.to_string(),
                    DynObject::new(Arc::new(UserDefinedFunctionType::new(
                        macro_name, args, returns,
                    ))),
                );
                *last_func_sign = None;
            } else if !funcsigns.contains_key(macro_name) {
                funcsigns.insert(
                    macro_name.to_string(),
                    DynObject::new(Arc::new(UndefinedFunctionType::new(
                        macro_name,
                        CodeLocation {
                            line: macro_decl.span.start_line,
                            col: macro_decl.span.start_col,
                            file: path.to_path_buf(),
                        },
                    ))),
                );
            }
        }
        _ => {}
    }
    Ok(())
}
