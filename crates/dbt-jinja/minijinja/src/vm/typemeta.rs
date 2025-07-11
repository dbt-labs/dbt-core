use crate::compiler::cfg::CFG;
use crate::compiler::instructions::Instruction;
use crate::compiler::typecheck::FunctionRegistry;
use crate::types::adapter::AdapterType;
use crate::types::api::ApiType;
use crate::types::builtin::Type;
use crate::types::class::DynClassType;
use crate::types::function::{
    DynFunctionType, FunctionType, LoadResultFunctionType, StoreRawResultFunctionType,
    StoreResultFunctionType,
};
use crate::types::internal_func::InternalCaller;
use crate::types::relation::RelationType;
use crate::types::utils::{infer_type_from_const_value, instr_name, CodeLocation};
use crate::vm::listeners::TypecheckingEventListener;
use crate::Value;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;

/// symbol table mapping local variable names to their types
type SymbolTable = BTreeMap<String, Type>;

/// The states of the type checker
#[derive(Clone)]
pub struct TypecheckState {
    pub stack: Vec<Type>,
    pub locals: SymbolTable,
    pub frame_base: usize,
    pub cur_loop_obj_type: Option<Type>,
}

impl TypecheckState {
    pub fn new() -> Self {
        TypecheckState {
            stack: Vec::new(),
            locals: SymbolTable::from([
                (
                    "this".to_string(),
                    Type::Class(DynClassType::new(Arc::new(RelationType::default()))),
                ),
                ("database".to_string(), Type::String),
                ("schema".to_string(), Type::String),
                ("identifier".to_string(), Type::String),
                ("config".to_string(), Type::Map(BTreeMap::default())),
                (
                    "model".to_string(),
                    Type::Seq {
                        field1: Box::new(Type::Any),
                    },
                ),
                (
                    "store_result".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        StoreResultFunctionType::default(),
                    ))),
                ),
                (
                    "load_result".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        LoadResultFunctionType::default(),
                    ))),
                ),
                (
                    "store_raw_result".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        StoreRawResultFunctionType::default(),
                    ))),
                ),
                ("TARGET_PACKAGE_NAME".to_string(), Type::String),
                ("TARGET_UNIQUE_ID".to_string(), Type::String),
                (
                    "api".to_string(),
                    Type::Class(DynClassType::new(Arc::new(ApiType::default()))),
                ),
                (
                    "adapter".to_string(),
                    Type::Class(DynClassType::new(Arc::new(AdapterType::default()))),
                ),
            ]),
            frame_base: 0,
            cur_loop_obj_type: None,
        }
    }

    pub fn drop_top(&mut self, n: usize) {
        self.stack.truncate(self.stack.len().saturating_sub(n));
    }

    #[track_caller]
    pub fn peek(&self) -> &Type {
        self.stack.last().unwrap()
    }

    pub fn push_frame(&mut self) {
        self.frame_base = self.stack.len();
    }

    pub fn get_call_args(&mut self, n: u16) -> Vec<Type> {
        // get n items from the stack
        self.stack
            .drain(self.stack.len().saturating_sub(n as usize)..)
            .collect()
    }
}

impl Default for TypecheckState {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a type error
/// We current only use the 'message', 'line_num' and 'col_num' are saved for future uses
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TypeError {
    pub message: String,
    pub location: CodeLocation,
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TypeError: {}", self.message)
    }
}

impl std::fmt::Debug for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TypeError: {}", self.message)
    }
}

/// CFG-based type checker
pub struct TypeChecker<'src> {
    pub instr: &'src [Instruction<'src>],
    pub cfg: CFG,
    pub in_states: Vec<TypecheckState>,
    pub function_registry: FunctionRegistry,
}

/// Typecheck logic implementation
impl<'src> TypeChecker<'src> {
    pub fn new(instr: &'src [Instruction<'src>], cfg: CFG, funcsigns: &FunctionRegistry) -> Self {
        let in_states = vec![TypecheckState::default(); cfg.blocks.len()];
        Self {
            instr,
            cfg,
            in_states,
            function_registry: funcsigns.clone(),
        }
    }

    pub fn check(
        &mut self,
        warning_printer: Rc<dyn TypecheckingEventListener>,
    ) -> Result<(), crate::Error> {
        let mut worklist = VecDeque::new();
        let mut visited = vec![false; self.cfg.blocks.len()];
        let mut first_merge = vec![true; self.cfg.blocks.len()];

        // Find all roots (blocks with no predecessors)
        for (i, block) in self.cfg.blocks.iter().enumerate() {
            if block.predecessor.is_empty() {
                self.in_states[i] = TypecheckState::default();
                worklist.push_back(i);
                visited[i] = true;
                first_merge[i] = false;
            }
        }

        while let Some(bb_id) = worklist.pop_front() {
            let out_state = self.transfer_block(bb_id, warning_printer.clone());

            match out_state {
                Ok(out_state) => {
                    for (succ, _) in self.cfg.successor(bb_id) {
                        let changed = if first_merge[*succ] {
                            self.in_states[*succ] = out_state.clone();
                            first_merge[*succ] = false;
                            true
                        } else {
                            Self::merge_into(&mut self.in_states[*succ], &out_state)
                        };
                        if !visited[*succ] || changed {
                            worklist.push_back(*succ);
                            visited[*succ] = true;
                        }
                    }
                }
                Err(e) => match e.try_abrupt_return() {
                    Some(rv) => {
                        let mut registry_ret_type = Type::Invalid;
                        if let Some(macro_block) = self.cfg.get_block(bb_id) {
                            if let Some(macro_name) = macro_block.current_macro.as_ref() {
                                if let Some(funcsign) = self.function_registry.get(macro_name) {
                                    registry_ret_type = funcsign.ret_type.clone();
                                }
                            }
                        }
                        // try match rv with registry_ret_type
                        let rv_type = rv
                            .downcast_object_ref::<Type>()
                            .cloned()
                            .unwrap_or(Type::Any);
                        let span = e.get_abrupt_return_span();
                        if rv_type.coerce(&registry_ret_type).is_none() {
                            warning_printer.warn(
                                &span,
                                &format!(
                                    "Type mismatch: expected return type {registry_ret_type}, got {rv_type}"
                                ),
                            );
                        }
                        continue;
                    }
                    None => {
                        return Err(e);
                    }
                },
            }
        }
        Ok(())
    }

    /// The internal function typechecking a single block.
    #[allow(clippy::too_many_arguments)]
    fn transfer_block(
        &mut self,
        bb_id: usize,
        warning_printer: Rc<dyn TypecheckingEventListener>,
    ) -> Result<TypecheckState, crate::Error> {
        let mut typestate = self.in_states[bb_id].clone();
        let slice = self.cfg.instructions(bb_id, self.instr);

        for (offset, inst) in slice.iter().enumerate() {
            let global_idx = self.cfg.blocks[bb_id].start + offset;

            match inst {
                Instruction::Swap => {
                    // TYPECHECK: NO
                    let a = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on swap",
                            ))
                        }
                    };
                    let b = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on swap",
                            ))
                        }
                    };
                    typestate.stack.push(b);
                    typestate.stack.push(a);
                }
                Instruction::EmitRaw(_) => {
                    // TYPECHECK: NO
                    // no need to update the type stack
                }
                Instruction::Emit => {
                    // TYPECHECK: NO
                    let _item1 = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on emit",
                            ));
                        }
                    };
                }
                Instruction::StoreLocal(name) => {
                    // TYPECHECK: NO
                    let value_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => Type::Any,
                    };
                    typestate.locals.insert(name.to_string(), value_type);
                }
                Instruction::Lookup(name, span) => {
                    // TYPECHECK: NO
                    let name_str: &str = name;
                    // first try to search in self.cfg.get_block(bb_id).type_narrow
                    if let Some(ty) = self.cfg.blocks[bb_id].type_constraints.get(name_str) {
                        typestate.stack.push(ty.clone());
                    } else if let Some(typeset) = typestate.locals.get(name_str) {
                        typestate.stack.push(typeset.clone());
                    } else if name_str == "adapter" {
                        typestate.stack.push(Type::Class(DynClassType::new(Arc::new(
                            AdapterType::default(),
                        ))));
                    }
                    // TODO: other internal states
                    else {
                        warning_printer.warn(
                            span,
                            &format!("Potential TypeError: Unknown local variable '{name_str}'"),
                        );
                        typestate.stack.push(Type::Any);
                    }
                }
                Instruction::GetAttr(name, span) => {
                    // TYPECHECK: YES
                    // pop a type from the stack
                    let value_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on get attr",
                            ))
                        }
                    };
                    match value_type.get_attribute(name) {
                        Ok(rv) => typestate.stack.push(rv),
                        Err(e) => {
                            warning_printer.warn(
                                span,
                                &format!("Unknown attribute '{value_type}.{name}': {e}"),
                            );
                            typestate.stack.push(Type::Any);
                        }
                    }
                }

                Instruction::SetAttr(_name) => {
                    // TYPECHECK: NO
                    let _item1 = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on set attr",
                            ))
                        }
                    };
                    let _item2 = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on set attr",
                            ))
                        }
                    };
                }
                Instruction::GetItem => {
                    // TYPECHECK: NO
                    let _item1 = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on get item",
                            ))
                        }
                    };
                    let _item2 = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on get item",
                            ))
                        }
                    };
                    typestate.stack.push(Type::Any);
                }
                Instruction::Slice(span) => {
                    // TYPECHECK: YES
                    // b, step, stop must be Integer, None, or Value (or a union containing any of these)
                    let step = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on slice",
                            ))
                        }
                    };
                    let stop = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on slice",
                            ))
                        }
                    };
                    let _ = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on slice",
                            ))
                        }
                    };
                    let b = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on slice",
                            ))
                        }
                    };

                    for (name, slice_type) in [("b", &b), ("stop", &stop), ("step", &step)] {
                        if slice_type.coerce(&Type::Integer).is_none() {
                            warning_printer.warn(
                                span,
                                &format!("Type mismatch for slice {name}: type = {slice_type:?}"),
                            );
                        }
                    }

                    typestate.stack.push(Type::Any);
                }
                Instruction::LoadConst(val) => {
                    // TYPECHECK: NO
                    typestate.stack.push(infer_type_from_const_value(val));
                }
                Instruction::BuildMap(pair_count) => {
                    // TYPECHECK: NO
                    let mut args_map = BTreeMap::new();
                    for _ in 0..*pair_count {
                        let k = match typestate.stack.pop() {
                            Some(val) => val,
                            None => {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Stack underflow on build map key",
                                ))
                            }
                        };
                        let v = match typestate.stack.pop() {
                            Some(val) => val,
                            None => {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Stack underflow on build map value",
                                ))
                            }
                        };

                        args_map.insert(k.to_string(), Box::new(v));
                    }
                    typestate.stack.push(Type::Map(args_map));
                }
                Instruction::BuildKwargs(pair_count) => {
                    // TYPECHECK: NO
                    let mut args_map = BTreeMap::new();
                    for _ in 0..*pair_count {
                        let key = match typestate.stack.pop() {
                            Some(val) => val,
                            None => Type::Any,
                        };
                        let value = match typestate.stack.pop() {
                            Some(val) => val,
                            None => Type::Any,
                        };

                        args_map.insert(key.to_string(), Box::new(value));
                    }
                    typestate.stack.push(Type::Kwargs(args_map));
                }
                Instruction::MergeKwargs(count) => {
                    // TYPECHECK: NO
                    let mut args_map = BTreeMap::new();
                    for _ in 0..*count {
                        let kwargs = match typestate.stack.pop() {
                            Some(val) => val,
                            None => {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Stack underflow on merge kwargs",
                                ))
                            }
                        };
                        // get the map from the kwargs type
                        if let Type::Kwargs(kwargs_map) = kwargs {
                            for (k, v) in kwargs_map {
                                args_map.insert(k, v);
                            }
                        }
                    }
                    typestate.stack.push(Type::Kwargs(args_map));
                }
                Instruction::BuildList(n) => {
                    // TODO
                    // We need to modify BuildList to make the arg mandatory
                    // Consider add the loopstart instruction at the start of a loop with a filter
                    // When calling the loopstart instruction we backup stack
                    // When calling the BuildList instruction we restore the stack

                    if let Some(count) = n {
                        if *count == 0 {
                            typestate.stack.push(Type::Seq {
                                field1: Box::new(Type::Any),
                            });
                        } else {
                            // Collect the types of the items to be popped
                            let mut item_type = Type::None;
                            for _ in 0..*count {
                                let other = match typestate.stack.pop() {
                                    Some(val) => val,
                                    None => {
                                        return Err(crate::Error::new(
                                            crate::error::ErrorKind::InvalidOperation,
                                            "Stack underflow on build list",
                                        ))
                                    }
                                };
                                item_type = item_type.union(&other);
                            }

                            typestate.stack.push(Type::Seq {
                                field1: Box::new(item_type),
                            });
                        }
                    } else {
                        // push a List with a single Value type
                        typestate.stack.push(Type::Seq {
                            field1: Box::new(Type::Any),
                        });
                    }
                }
                Instruction::BuildTuple(n) => {
                    // TODO
                    // I checked and there's no BuildTuple reference using the None
                    // Could easily modify it to a 'usize
                    let list_type = typestate.peek().clone();

                    if list_type.is_union() {
                        if let Some(count) = n {
                            // currently not support n == None
                            // pop the items from the stack
                            for _ in 0..*count {
                                let _item_type = match typestate.stack.pop() {
                                    Some(val) => val,
                                    None => {
                                        return Err(crate::Error::new(
                                            crate::error::ErrorKind::InvalidOperation,
                                            "Stack underflow on build tuple",
                                        ))
                                    }
                                };
                            }
                        }
                        typestate.stack.push(Type::Seq {
                            field1: Box::new(Type::Any),
                        });
                        continue;
                    }
                    if let Some(count) = n {
                        // currently not support n == None
                        // pop the items from the stack
                        for _ in 0..*count {
                            let _item_type = match typestate.stack.pop() {
                                Some(val) => val,
                                None => {
                                    return Err(crate::Error::new(
                                        crate::error::ErrorKind::InvalidOperation,
                                        "Stack underflow on build tuple",
                                    ))
                                }
                            };
                        }
                    }
                    typestate.stack.push(Type::Seq {
                        field1: Box::new(list_type),
                    });
                }
                Instruction::UnpackList(count, span) => {
                    let list_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on unpack list",
                            ))
                        }
                    };
                    // if list_type is not a Seq, we have a type error
                    let element_type = list_type.get_seq_element_type();
                    if !element_type.is_none() {
                        for _ in 0..*count {
                            typestate.stack.push(element_type.clone());
                        }
                    } else {
                        warning_printer.warn(
                            span,
                            &format!(
                                "Type mismatch for unpack list: expected Seq, got {list_type:?}"
                            ),
                        );
                    }
                }
                Instruction::UnpackLists(_count) => {
                    // TODO
                    // We need to modify the structure of the UnpackLists instruction, adding an expected total items count
                }
                Instruction::Add(span)
                | Instruction::Sub(span)
                | Instruction::Mul(span)
                | Instruction::Div(span)
                | Instruction::IntDiv(span)
                | Instruction::Pow(span) => {
                    // TYPECHECK: YES
                    // lhs and rhs must have the same type
                    let op = instr_name(&self.instr[global_idx]);
                    let rhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on binary operation",
                            ))
                        }
                    };
                    let lhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on binary operation",
                            ))
                        }
                    };

                    let result_type = lhs_type.coerce(&rhs_type);
                    if !result_type.is_none() {
                        typestate.stack.push(result_type);
                    } else {
                        warning_printer.warn(
                            span,
                            &format!("Type mismatch for {op}: lhs = {lhs_type}, rhs = {rhs_type}"),
                        );
                        typestate.stack.push(Type::Any);
                    }
                }

                Instruction::Rem(span) => {
                    // TYPECHECK: YES
                    // lhs and rhs must have the same type
                    // or, according to the runtime logic, Rem can be used with lhs = String, rhs = Seq
                    let op = instr_name(&self.instr[global_idx]);

                    let rhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on rem operation",
                            ))
                        }
                    };
                    let lhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on rem operation",
                            ))
                        }
                    };

                    // Check for string formatting case: lhs = String, rhs = Seq
                    if !lhs_type.coerce(&Type::String).is_none()
                        && !rhs_type
                            .coerce(&Type::Seq {
                                field1: Box::new(Type::None),
                            })
                            .is_none()
                    {
                        typestate.stack.push(Type::String);
                        continue;
                    }

                    let result_type = lhs_type.coerce(&rhs_type);
                    if !result_type.is_none() {
                        typestate.stack.push(result_type);
                    } else {
                        warning_printer.warn(
                            span,
                            &format!(
                                "Type mismatch for {op}: lhs = {lhs_type:?}, rhs = {rhs_type:?}"
                            ),
                        );
                        typestate.stack.push(Type::Any);
                    }
                }

                Instruction::Eq(span)
                | Instruction::Ne(span)
                | Instruction::Lt(span)
                | Instruction::Lte(span)
                | Instruction::Gt(span)
                | Instruction::Gte(span) => {
                    // TYPECHECK: YES
                    // lhs and rhs must have the same type
                    let op = instr_name(&self.instr[global_idx]);
                    let rhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on binary operation",
                            ))
                        }
                    };
                    let lhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on binary operation",
                            ))
                        }
                    };

                    let result_type = lhs_type.can_compare_with(&rhs_type, op);
                    if !result_type {
                        warning_printer.warn(
                            span,
                            &format!(
                                "Type mismatch for {op}: lhs = {lhs_type:?}, rhs = {rhs_type:?}"
                            ),
                        );
                    }
                    typestate.stack.push(Type::Bool);
                }

                Instruction::Not(_) => {
                    // TYPECHECK: NO
                    let _item_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on not operation",
                            ))
                        }
                    };
                    typestate.stack.push(Type::Bool);
                }
                Instruction::StringConcat(_) => {
                    // TYPECHECK: NO
                    // Stringconcat can actually concat any two types
                    let _rhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on string concat operation",
                            ))
                        }
                    };
                    let _lhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on string concat operation",
                            ))
                        }
                    };

                    typestate.stack.push(Type::String);
                }
                Instruction::In(_) => {
                    // TYPECHECK: NO
                    let _rhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on in operation",
                            ))
                        }
                    };
                    let _lhs_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on in operation",
                            ))
                        }
                    };

                    typestate.stack.push(Type::Bool);
                }
                Instruction::Neg(_) => {
                    // TYPECHECK: YES
                    // The operand must be a number
                    let a = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on negation",
                            ))
                        }
                    };

                    // TODO impl a.neg()
                    typestate.stack.push(a);
                }
                Instruction::PushWith => {
                    // TYPECHECK: NO
                    typestate.push_frame();
                }
                Instruction::PopFrame => {
                    // TYPECHECK: NO
                    typestate.stack.truncate(typestate.frame_base);

                    typestate.cur_loop_obj_type = None;

                    let maybe_capture = false;
                    if maybe_capture {
                        typestate.stack.push(Type::Any);
                    }

                    typestate.frame_base = typestate.stack.len();
                }
                #[cfg(feature = "macros")]
                Instruction::IsUndefined => {
                    // TYPECHECK: NO
                    typestate.stack.pop();

                    typestate.stack.push(Type::Bool);
                }
                Instruction::PushLoop(_flags, span) => {
                    // TYPECHECK: NO
                    if let Some(loop_object) = typestate.stack.pop() {
                        let element_type = loop_object.get_seq_element_type();
                        if !element_type.is_none() {
                            typestate.stack.push(element_type);
                        } else {
                            warning_printer.warn(
                                span,
                                &format!(
                                    "Type mismatch for loop object: expected a sequence type, found {loop_object:?}"
                                ),
                            );
                            typestate.stack.push(Type::Any);
                        }
                    }

                    typestate.push_frame();
                }
                Instruction::Iterate(_jump_target) => {
                    // TYPECHECK: NO
                    let elem_ty = typestate.cur_loop_obj_type.clone().unwrap_or(Type::Any); // fallback

                    typestate.stack.push(elem_ty);
                }
                Instruction::PushDidNotIterate => {
                    // TYPECHECK: NO
                    typestate.stack.push(Type::Any);
                }
                Instruction::Jump(_jump_target) => {
                    // TYPECHECK: NO
                    // have nothing to do with the stack
                }
                Instruction::JumpIfFalse(_else_label) => {
                    let _item_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on jump if false",
                            ))
                        }
                    };
                }
                Instruction::JumpIfFalseOrPop(_jump_target, span) => {
                    // TYPECHECK: YES
                    // the operand must be a boolean
                    let a = typestate.peek().clone();

                    if !a.is_condition() {
                        warning_printer.warn(
                            span,
                            &format!("Type mismatch for jump condition: type = {a:?}"),
                        );
                    }

                    let path_true = typestate.clone(); // cond == true
                    let mut path_false = typestate; // reuse cur => cond == false

                    Self::merge_into(&mut path_false, &path_true);
                    typestate = path_false;
                }
                Instruction::JumpIfTrueOrPop(_jump_target, span) => {
                    // TYPECHECK: YES
                    // the operand must be a boolean
                    let a = typestate.peek().clone();

                    if !a.is_condition() {
                        warning_printer.warn(
                            span,
                            &format!("Type mismatch for jump condition: type = {a:?}"),
                        );
                    }

                    let path_false = typestate.clone(); // cond == false
                    let mut path_true = typestate; // reuse cur => cond == true

                    Self::merge_into(&mut path_true, &path_false);
                    typestate = path_true;
                }
                #[cfg(feature = "multi_template")]
                Instruction::CallBlock(_name) => {
                    // TYPECHECK: NO
                    let saved_base = typestate.stack.len();
                    // truncate
                    typestate.stack.truncate(saved_base);
                }
                Instruction::PushAutoEscape(span) => {
                    // TYPECHECK: YES
                    // the operand must be a string
                    let a = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on push auto escape",
                            ))
                        }
                    };

                    if a.coerce(&Type::String).is_none() {
                        warning_printer
                            .warn(span, &format!("Type mismatch for auto escape: type = {a}"));
                    }
                }
                Instruction::PopAutoEscape => {
                    // TYPECHECK: NO
                    // nothing to do with the stack
                }
                Instruction::BeginCapture(_mode) => {
                    // TYPECHECK: NO
                    // nothing to do with the stack
                }
                Instruction::EndCapture => {
                    // TYPECHECK: NO
                    typestate.stack.push(Type::String);
                }
                Instruction::ApplyFilter(name, arg_count, _local_id) => {
                    // TYPECHECK: NO
                    let name_filter = name.to_string();
                    typestate.drop_top(arg_count.unwrap_or(0) as usize);

                    let result_type = if name_filter == "map" {
                        Type::Map(BTreeMap::default())
                    } else if name_filter == "list" {
                        Type::Seq {
                            field1: Box::new(Type::Any),
                        }
                    } else {
                        Type::Any
                    };

                    typestate.stack.push(result_type);
                }
                Instruction::PerformTest(_name, arg_count, _local_id) => {
                    // TYPECHECK: NO
                    typestate.drop_top(arg_count.unwrap_or(0) as usize);
                    typestate.stack.push(Type::Bool);
                }
                Instruction::CallFunction(name, arg_count, span) => {
                    // TYPECHECK: YES
                    // check the parameter types
                    // For internal rust functions
                    // if let Some(func) = state.lookup(name).filter(|func| !func.is_undefined()) {
                    //     let mut rv_type: String;
                    //     if let Some(arg_cnt) = arg_count {
                    //         let _args = typestate.get_call_args(*arg_cnt);
                    //     }
                    //     rv_type = func.sign().to_string();
                    //     if let Some(pos) = rv_type.find("->") {
                    //         rv_type = rv_type[pos + 2..].trim().to_string();
                    //     } else {
                    //         rv_type = "value".to_string(); // default return type
                    //     }
                    //     let mut set = HashSet::new();
                    //     set.insert(parse_type(&rv_type));
                    //     typestate.stack.push(set);
                    // }
                    // // else if search the name in funcsigns, for defined macros
                    // else {
                    // TYPECHECK: YES
                    // check the parameter types

                    if *name == "return" {
                        if let Some(arg) = typestate.stack.pop() {
                            return Err(crate::Error::abrupt_return(
                                Value::from_object(arg),
                                *span,
                            ));
                        }
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "Stack underflow on return",
                        ));
                    }
                    if *name == "caller" {
                        // judge whether current block is a macro
                        if let Some(block) = self.cfg.get_block(bb_id) {
                            if let Some(macro_name) = &block.current_macro {
                                if let Some(arg_cnt) = arg_count {
                                    let args = typestate.get_call_args(*arg_cnt);

                                    match InternalCaller::default().resolve_arguments(&args) {
                                        Ok(ret_type) => {
                                            typestate.stack.push(ret_type);
                                        }
                                        Err(msg) => {
                                            warning_printer.warn(
                                                span,
                                                &format!(
                                                    "Type error for function 'caller' in macro {macro_name}: {msg}"
                                                ),
                                            );
                                            typestate.stack.push(Type::Any);
                                        }
                                    }
                                } else {
                                    return Err(crate::Error::new(
                                        crate::error::ErrorKind::InvalidOperation,
                                        "Function 'caller' requires an argument count",
                                    ));
                                }
                            }
                        }
                    } else if let Some(funcsign) = self.function_registry.get(*name) {
                        if funcsign.has_signature {
                            if let Some(arg_cnt) = arg_count {
                                let args = typestate.get_call_args(*arg_cnt);
                                let function_type =
                                    DynFunctionType::new(Arc::new(funcsign.clone()));
                                match function_type.resolve_arguments(&args) {
                                    Ok(ret_type) => {
                                        typestate.stack.push(ret_type.clone());
                                    }
                                    Err(msg) => {
                                        warning_printer.warn(
                                            span,
                                            &format!("Type mismatch for function '{name}': {msg}"),
                                        );
                                        typestate.stack.push(Type::Any);
                                    }
                                }
                            }
                        } else {
                            // Macro defined without a signature, should report a warning

                            if let Some(macro_def) = self.function_registry.get(*name) {
                                warning_printer.warn(
                                    span,
                                    &format!(
                                        "Macro '{}'({}) needs a signature",
                                        name, macro_def.location
                                    ),
                                );
                            }
                            typestate.stack.push(Type::Any);
                        }
                    } else if let Some(arg_cnt) = arg_count {
                        let _args = typestate.get_call_args(*arg_cnt);
                        warning_printer.warn(
                            span,
                            &format!("Potential TypeError: Function '{name}' is not defined."),
                        );
                        typestate.stack.push(Type::Any);
                    } else {
                        // TODO: handle the case when arg_count is None
                        warning_printer.warn(
                            span,
                            &format!("Potential TypeError: Function '{name}' is not defined."),
                        );
                        typestate.stack.push(Type::Any);
                    }
                }
                Instruction::CallMethod(name, arg_count, span) => {
                    // TYPECHECK: NO? (Maybe add method check later)

                    let count = arg_count.unwrap_or(0);
                    if count > 0 {
                        // Pop (arg_count - 1) arguments
                        let method_args = typestate.get_call_args(count - 1);
                        // Pop the last one as 'self'
                        let self_type = match typestate.stack.pop() {
                            Some(val) => val,
                            None => {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Stack underflow on call method",
                                ))
                            }
                        };
                        if self_type.is_any() {
                            typestate.stack.push(Type::Any);
                            continue;
                        }

                        let function = match self_type.get_attribute(name) {
                            Ok(rv) => rv,
                            Err(e) => {
                                warning_printer.warn(
                                    span,
                                    &format!("Unknown method '{self_type}.{name}': {e}"),
                                );
                                typestate.stack.push(Type::Any);
                                continue;
                            }
                        };

                        if function.is_any() {
                            warning_printer.warn(
                                span,
                                &format!("Potential TypeError: Method '{self_type}.{name}' is not defined."),
                            );
                            typestate.stack.push(Type::Any);
                            continue;
                        }

                        let result = match function.call(&method_args) {
                            Ok(rv) => rv,
                            Err(e) => {
                                if !function.is_any() {
                                    warning_printer.warn(
                                        span,
                                        &format!("Method call failed '{self_type}.{name}': {e}"),
                                    );
                                }
                                Type::Any
                            }
                        };

                        if result.is_any() {
                            warning_printer.warn(
                                span,
                                &format!("Method call result is not defined '{self_type}.{name}'"),
                            );
                        }
                        typestate.stack.push(result);
                    } else {
                        // TODO: handle the case when arg_count is None
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "The first argument(self) of method call is missing",
                        ));
                    }
                }
                Instruction::CallObject(arg_count) => {
                    // TYPECHECK: NO
                    typestate.drop_top(arg_count.unwrap_or(0) as usize);
                    typestate.stack.push(Type::Any);
                }
                Instruction::DupTop => {
                    // TYPECHECK: NO
                    // if no item on the stack, do nothing
                    if typestate.stack.is_empty() {
                        // DO NOTHING
                    } else {
                        typestate.stack.push(typestate.peek().clone());
                    }
                }
                Instruction::DiscardTop => {
                    // TYPECHECK: NO
                    typestate.stack.pop();
                }
                Instruction::FastSuper => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::FastRecurse => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                #[cfg(feature = "multi_template")]
                Instruction::LoadBlocks(span) => {
                    // TYPECHECK: YES
                    // the operand must be a string
                    let a = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on load blocks",
                            ))
                        }
                    };

                    if a.coerce(&Type::String).is_none() {
                        warning_printer
                            .warn(span, &format!("Type mismatch for block name: type = {a}"));
                    }
                    // LoadBlocks does not change the stack, it just loads blocks
                }
                #[cfg(feature = "multi_template")]
                Instruction::Include(_ignore_missing) => {
                    // TYPECHECK: NO
                    let _item_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on include",
                            ))
                        }
                    };
                }
                #[cfg(feature = "multi_template")]
                Instruction::ExportLocals => {
                    // TYPECHECK: NO
                    typestate.stack.push(Type::Any);
                }
                #[cfg(feature = "macros")]
                Instruction::BuildMacro(_name, _offset, _flags) => {
                    // TYPECHECK: NO?

                    typestate.stack.push(Type::Any);
                }
                #[cfg(feature = "macros")]
                Instruction::Return => {
                    // TYPECHECK: NO
                    // do nothing instead of break because we want to cover all instructions
                }
                #[cfg(feature = "macros")]
                Instruction::Enclose(_name) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                #[cfg(feature = "macros")]
                Instruction::GetClosure => {
                    // TYPECHECK: NO?
                    typestate.stack.push(Type::Any);
                }
                Instruction::MacroStart(_line, _col, _index, _, _, _) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::MacroStop(_line, _col, _index) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::ModelReference(
                    _name,
                    _start_line,
                    _start_col,
                    _start_offset,
                    _end_line,
                    _end_col,
                    _end_offset,
                ) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::MacroName(_name) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
            }
        }
        Ok(typestate)
    }

    /// Merges the source typecheck state into the destination state at the merge point.
    fn merge_into(dst: &mut TypecheckState, src: &TypecheckState) -> bool {
        let mut changed = false;

        let min_len = dst.stack.len().min(src.stack.len());
        dst.stack.truncate(min_len);

        if dst.cur_loop_obj_type != src.cur_loop_obj_type {
            dst.cur_loop_obj_type = match (&dst.cur_loop_obj_type, &src.cur_loop_obj_type) {
                (Some(a), Some(b)) if a == b => Some(a.clone()),
                (None, Some(t)) => Some(t.clone()),
                (Some(t), None) => Some(t.clone()),
                (None, None) => None,
                _ => Some(Type::Any),
            };
            changed = true;
        }

        for i in 0..min_len {
            let dst_type = dst.stack[i].clone();
            let union_type = dst_type.union(&src.stack[i]);
            if union_type != dst_type {
                changed = true;
            }
        }

        // Union all keys from both locals
        let all_keys: std::collections::HashSet<_> = dst
            .locals
            .keys()
            .chain(src.locals.keys())
            .cloned()
            .collect();

        for name in all_keys {
            match (dst.locals.get_mut(&name), src.locals.get(&name)) {
                (Some(dst_type), Some(src_type)) => {
                    let union_type = dst_type.union(src_type);
                    if union_type != *dst_type {
                        *dst_type = union_type;
                        changed = true;
                    }
                }
                (Some(_), None) => {}
                (None, Some(src_type)) => {
                    dst.locals.insert(name.clone(), src_type.clone());
                    changed = true;
                }
                (None, None) => {}
            }
        }

        changed
    }
}
