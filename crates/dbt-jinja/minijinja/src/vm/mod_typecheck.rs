use std::collections::BTreeMap;

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use crate::compiler::cfg::build_cfg;
use crate::compiler::instructions::{Instruction, Instructions};
use crate::types::function::{
    parse_macro_signature, DynFunctionType, UndefinedFunctionType, UserDefinedFunctionType,
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
        funcsigns: &FunctionRegistry,
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

    /// Get macro signatures from the instructions
    pub fn get_macro_signature(
        &self,
        instructions: &Instructions<'_>,
        path: &Path,
    ) -> FunctionRegistry {
        let mut funcsigns: FunctionRegistry = BTreeMap::new();
        let mut current_funcsign = String::new();
        for (i, instruction) in instructions.instructions.iter().enumerate() {
            match instruction {
                Instruction::EmitRaw(val) => {
                    // if val starts with "[\n]*-- funcsign:" in regex
                    let trimmed = val.trim_start();
                    if let Some(funcsign_str) = trimmed.strip_prefix("-- funcsign:") {
                        // parse the function signature
                        current_funcsign = funcsign_str.to_string();
                    }
                }
                Instruction::BuildMacro(name, _offset, _flags) => {
                    // Check if the previous instruction is MacroStart and the next is MacroStop (by variant)
                    let prev_is_macro_start = instructions
                        .instructions
                        .get(i.wrapping_sub(1))
                        .map(|instr| matches!(instr, Instruction::MacroStart(_, _, _, _, _, _)))
                        .unwrap_or(false);
                    let next_is_macro_stop = instructions
                        .instructions
                        .get(i + 1)
                        .map(|instr| matches!(instr, Instruction::MacroStop(_, _, _,)))
                        .unwrap_or(false);

                    if prev_is_macro_start && next_is_macro_stop {
                        // get the line and column numbers from the MacroStart instruction
                        let (line_num, col_num, _index) = match instructions.instructions[i - 1] {
                            Instruction::MacroStart(line, col, idx, _, _, _) => (line, col, idx),
                            _ => continue, // should not happen
                        };

                        // add the function signature if it exists
                        if !current_funcsign.is_empty() {
                            let (args, ret_type) = parse_macro_signature(current_funcsign.clone());
                            let funcsign = UserDefinedFunctionType::new(
                                name,
                                args,
                                ret_type,
                                CodeLocation {
                                    line: line_num,
                                    col: col_num,
                                    file: path.to_path_buf(),
                                },
                            );
                            funcsigns
                                .insert(name.to_string(), DynFunctionType::new(Arc::new(funcsign)));
                            current_funcsign.clear();
                        } else {
                            funcsigns.insert(
                                name.to_string(),
                                DynFunctionType::new(Arc::new(UndefinedFunctionType::new(
                                    name,
                                    CodeLocation {
                                        line: line_num,
                                        col: col_num,
                                        file: path.to_path_buf(),
                                    },
                                ))),
                            );
                        }
                    }
                }
                _ => {
                    // continue to the next instruction
                }
            }
        }
        funcsigns
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn typecheck_impl(
        &self,
        state: &mut State<'_, 'env>,
        _stack: Stack,
        _pc: usize,
        funcsigns: &FunctionRegistry,
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
