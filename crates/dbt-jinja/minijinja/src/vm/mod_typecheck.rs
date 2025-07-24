use std::collections::BTreeMap;

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use crate::compiler::ast;
use crate::compiler::cfg::build_cfg;
use crate::compiler::instructions::Instructions;
use crate::types::funcsign_parser::parse;
use crate::types::function::{
    Argument, DynFunctionType, UndefinedFunctionType, UserDefinedFunctionType,
};
use crate::vm::listeners::TypecheckingEventListener;
use crate::vm::typemeta::TypeChecker;

use crate::compiler::typecheck::FunctionRegistry;
use crate::machinery::Span;
use crate::types::utils::CodeLocation;
use crate::utils::AutoEscape;
use crate::value::{value_optimization, Value};
use crate::vm::context::Stack;
pub(crate) use crate::vm::context::{Context, Frame};
use crate::vm::prepare_blocks;
pub use crate::vm::state::State;
use crate::vm::Vm;

impl<'env> Vm<'env> {
    /// eval type check
    #[allow(clippy::too_many_arguments)]
    pub fn typecheck<'template>(
        &self,
        instructions: &'template Instructions<'env>,
        root: Value,
        blocks: &'template BTreeMap<&'env str, Instructions<'env>>,
        auto_escape: AutoEscape,
        funcsigns: Arc<FunctionRegistry>,
        warning_printer: Rc<dyn TypecheckingEventListener>,
    ) -> Result<(), crate::Error> {
        let _guard = value_optimization();

        let ctx_result = Context::new_with_frame_and_stack_depth(
            match Frame::new_checked(root.clone()) {
                Ok(frame) => frame,
                Err(_) => {
                    return Err(crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        "Failed to create frame",
                    ))
                }
            },
            self.env.recursion_limit(),
            root.get_attr_fast("file_stack")
                .map_or(vec![], |value| deserialize_file_stack(&value)),
            0,
        );

        let mut state = State::new(
            self.env,
            ctx_result,
            auto_escape,
            instructions,
            prepare_blocks(blocks),
        );
        self.typecheck_impl(&mut state, Stack::default(), 0, funcsigns, warning_printer)
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn typecheck_impl(
        &self,
        state: &mut State<'_, 'env>,
        _stack: Stack,
        _pc: usize,
        funcsigns: Arc<FunctionRegistry>,
        warning_printer: Rc<dyn TypecheckingEventListener>,
    ) -> Result<(), crate::Error> {
        // warning_printer.warn(&Span::default(), &format!("start a file"));
        // dbg!(state.instructions);
        // get instructions
        let instructions = &state.instructions.instructions;

        // build CFG
        let cfg = build_cfg(instructions);
        // create a typechecker
        let mut typechecker = TypeChecker::new(instructions, cfg, funcsigns);

        match typechecker.check(warning_printer) {
            Ok(()) => {}
            Err(err) => {
                return Err(crate::Error::new(
                    crate::error::ErrorKind::InvalidOperation,
                    format!("Type checking failed: {err}"),
                ));
            }
        }

        Ok(())
    }
}

fn deserialize_file_stack(value: &Value) -> Vec<(PathBuf, Span, u32)> {
    let mut result = vec![];
    for item in value.try_iter().unwrap() {
        let mut iter = item.try_iter().unwrap();
        let path = iter.next().unwrap();
        let path = deserialize_path(&path);
        let span = iter.next().unwrap();
        let span = deserialize_span(&span);
        let delta_line = iter.next().unwrap();
        let delta_line = delta_line.as_usize().unwrap() as u32;

        result.push((path, span, delta_line));
    }
    result
}

fn deserialize_path(value: &Value) -> PathBuf {
    PathBuf::from(value.as_str().unwrap())
}

fn deserialize_span(value: &Value) -> Span {
    Span {
        start_line: value
            .get_attr_fast("start_line")
            .unwrap()
            .as_usize()
            .unwrap() as u32,
        start_col: value
            .get_attr_fast("start_col")
            .unwrap()
            .as_usize()
            .unwrap() as u32,
        start_offset: value
            .get_attr_fast("start_offset")
            .unwrap()
            .as_usize()
            .unwrap() as u32,
        end_line: value.get_attr_fast("end_line").unwrap().as_usize().unwrap() as u32,
        end_col: value.get_attr_fast("end_col").unwrap().as_usize().unwrap() as u32,
        end_offset: value
            .get_attr_fast("end_offset")
            .unwrap()
            .as_usize()
            .unwrap() as u32,
    }
}

/// Find macro signatures in the template.
///
/// This function is used to find macro signatures in the template.
/// It is used to type check the template.
pub fn find_macro_signatures(
    root: &ast::Stmt,
    last_func_sign: &mut Option<(Span, String)>,
    funcsigns: &mut FunctionRegistry,
    path: &Path,
) -> Result<(), crate::Error> {
    match root {
        ast::Stmt::Template(template) => {
            for child in template.children.iter() {
                find_macro_signatures(child, last_func_sign, funcsigns, path)?;
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
                let (arg_types, returns) = parse(func_sign).map_err(|e| {
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
                    DynFunctionType::new(Arc::new(UserDefinedFunctionType::new(
                        macro_name,
                        args,
                        returns,
                        CodeLocation {
                            line: macro_decl.span.start_line,
                            col: macro_decl.span.start_col,
                            file: path.to_path_buf(),
                        },
                    ))),
                );
                *last_func_sign = None;
            } else if !funcsigns.contains_key(macro_name) {
                funcsigns.insert(
                    macro_name.to_string(),
                    DynFunctionType::new(Arc::new(UndefinedFunctionType::new(
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
