use crate::compiler::cfg::CFG;
use crate::compiler::codegen::{TypeConstraintOperation, Variable};
use crate::compiler::instructions::Instruction;
use crate::compiler::typecheck::FunctionRegistry;
use crate::types::adapter::AdapterType;
use crate::types::agate_table::AgateTableType;
use crate::types::api::{ApiColumnType, ApiType};
use crate::types::builtin::Type;
use crate::types::class::DynClassType;
use crate::types::config::ConfigType;
use crate::types::dbt::DbtType;
use crate::types::dict::DictType;
use crate::types::exceptions::ExceptionsType;
use crate::types::flags::FlagsType;
use crate::types::function::{
    CastFunctionType, DiffOfTwoDictsFunctionType, DynFunctionType, EnvVarFunctionType,
    FirstFunctionType, FunctionType, GetColumnSchemaFromQueryFunction, JoinFunctionType,
    LengthFunctionType, ListFunctionType, LoadResultFunctionType, LogFunctionType,
    LowerFunctionType, MapFunctionType, PrintFunctionType, RangeFunctionType, RefFunctionType,
    RenderFunctionType, ReplaceFunctionType, SelectAttrFunctionType, SourceFunctionType,
    StoreRawResultFunctionType, StoreResultFunctionType, StringFunctionType,
    SubmitPythonJobFunctionType, ToJsonFunctionType, TrimFunctionType,
    TryOrCompilerErrorFunctionType, UpperFunctionType, UserDefinedFunctionType, WriteFunctionType,
};
use crate::types::hook::HookType;
use crate::types::internal_func::InternalCaller;
use crate::types::list::ListType;
use crate::types::loop_::LoopType;
use crate::types::model::ModelType;
use crate::types::modules::ModulesType;
use crate::types::relation::RelationType;
use crate::types::struct_::StructType;
use crate::types::tuple::TupleType;
use crate::types::utils::{infer_type_from_const_value, instr_name, CodeLocation};
use crate::vm::listeners::TypecheckingEventListener;
use crate::{ErrorKind, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::ops::RangeBounds;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct TypeWithConstraint {
    pub inner: Type,
    pub constraint: BTreeMap<String, TypeWithConstraint>,
}

impl std::fmt::Display for TypeWithConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl From<Type> for TypeWithConstraint {
    fn from(type_: Type) -> Self {
        TypeWithConstraint {
            inner: type_,
            constraint: BTreeMap::new(),
        }
    }
}

impl TypeWithConstraint {
    pub fn get_attribute(&self, name: &str) -> Result<TypeWithConstraint, crate::Error> {
        if let Some(constraint) = self.constraint.get(name) {
            Ok(constraint.clone())
        } else {
            self.inner.get_attribute(name).map(TypeWithConstraint::from)
        }
    }

    pub fn subscript(
        &self,
        index: &TypeWithConstraint,
    ) -> Result<TypeWithConstraint, crate::Error> {
        self.inner
            .subscript(&index.inner)
            .map(TypeWithConstraint::from)
    }

    pub fn is_subtype_of(&self, other: &TypeWithConstraint) -> bool {
        // TODO: do we need to check the constraint?
        self.inner.is_subtype_of(&other.inner)
    }

    pub fn union(&self, other: &TypeWithConstraint) -> TypeWithConstraint {
        TypeWithConstraint {
            inner: self.inner.union(&other.inner),
            constraint: BTreeMap::new(),
        }
    }

    pub fn can_binary_op_with(
        &self,
        other: &TypeWithConstraint,
        op: &'static str,
    ) -> Option<TypeWithConstraint> {
        self.inner
            .can_binary_op_with(&other.inner, op)
            .map(TypeWithConstraint::from)
    }

    pub fn can_compare_with(&self, other: &TypeWithConstraint, op: &'static str) -> bool {
        self.inner.can_compare_with(&other.inner, op)
    }

    pub fn is_condition(&self) -> bool {
        self.inner.is_condition()
    }

    pub fn is_any(&self) -> bool {
        self.inner.is_any()
    }

    pub fn call(
        &self,
        positional_args: &[Type],
        kwargs: &BTreeMap<String, Type>,
    ) -> Result<Type, crate::Error> {
        self.inner.call(positional_args, kwargs)
    }

    pub fn is_optional(&self) -> bool {
        self.inner.is_optional()
    }

    pub fn get_non_optional_type(&self) -> Type {
        self.inner.get_non_optional_type()
    }

    pub fn exclude(&self, other: &Type) -> Type {
        self.inner.exclude(other)
    }

    // Add convenient method to extract inner Type
    pub fn into_inner(self) -> Type {
        self.inner
    }

    #[allow(unconditional_recursion)]
    pub fn insert(&mut self, path: &[String], type_: Type) {
        if let Some((item, rest)) = path.split_first() {
            if let Some(attribute_type) = self.constraint.get_mut(item) {
                attribute_type.insert(rest, type_);
            } else if let Ok(mut attribute_type) = self.get_attribute(item) {
                attribute_type.insert(rest, type_);
                self.constraint.insert(item.to_string(), attribute_type);
            } else {
                let mut attribute_type: TypeWithConstraint = Type::Any { hard: false }.into();
                attribute_type.insert(rest, type_);
                self.constraint.insert(item.to_string(), attribute_type);
            }
        } else {
            self.inner = type_;
        }
    }
}

/// symbol table mapping local variable names to their types
#[derive(Clone, Debug)]
pub struct SymbolTable(BTreeMap<String, TypeWithConstraint>);

impl SymbolTable {
    pub fn get(&self, variable: impl Into<Variable>) -> Result<TypeWithConstraint, crate::Error> {
        let variable = variable.into();
        match variable {
            Variable::String(name) => self.0.get(&name).cloned().ok_or_else(|| {
                crate::Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Variable not found: {name}"),
                )
            }),
            Variable::GetAttr(path) => {
                let mut type_ = self.0.get(&path[0]).cloned().ok_or_else(|| {
                    crate::Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Variable not found: {}", path[0]),
                    )
                })?;
                for name in path.iter().skip(1) {
                    type_ = type_.get_attribute(name)?;
                }
                Ok(type_)
            }
        }
    }

    pub fn insert(
        &mut self,
        variable: impl Into<Variable>,
        value: Type,
    ) -> Result<(), crate::Error> {
        let variable = variable.into();
        match variable {
            Variable::String(name) => {
                self.0.insert(name, value.into());
                Ok(())
            }
            Variable::GetAttr(path) => {
                let type_ = self.0.get_mut(&path[0]).ok_or_else(|| {
                    crate::Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Variable not found: {}", path[0]),
                    )
                })?;
                type_.insert(&path[1..], value);
                Ok(())
            }
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut TypeWithConstraint> {
        self.0.get_mut(name)
    }

    pub fn get_ref(&self, name: &str) -> Option<&TypeWithConstraint> {
        self.0.get(name)
    }
}

impl<const N: usize> From<[(String, Type); N]> for SymbolTable {
    fn from(array: [(String, Type); N]) -> Self {
        SymbolTable(BTreeMap::from_iter(array.iter().map(|(name, type_)| {
            (
                name.clone(),
                TypeWithConstraint {
                    inner: type_.clone(),
                    constraint: BTreeMap::new(),
                },
            )
        })))
    }
}

#[derive(Clone, Debug, Default)]
pub struct TypecheckStack(Vec<TypeWithConstraint>);

impl TypecheckStack {
    pub fn push(&mut self, type_: impl Into<TypeWithConstraint>) {
        self.0.push(type_.into());
    }

    pub fn truncate(&mut self, n: usize) {
        self.0.truncate(n);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn last(&self) -> Option<&TypeWithConstraint> {
        self.0.last()
    }

    pub fn drain<R>(&mut self, range: R) -> std::vec::Drain<TypeWithConstraint>
    where
        R: RangeBounds<usize>,
    {
        self.0.drain(range)
    }

    pub fn pop(&mut self) -> Option<TypeWithConstraint> {
        self.0.pop()
    }

    // Add convenient method to pop inner Type directly
    pub fn pop_inner(&mut self) -> Option<Type> {
        self.0.pop().map(|t| t.inner)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn get(&self, index: usize) -> Option<&TypeWithConstraint> {
        self.0.get(index)
    }
}

/// The states of the type checker
#[derive(Clone, Debug)]
pub struct TypecheckState {
    pub stack: TypecheckStack,
    pub locals: SymbolTable,
    pub frame_base: usize,
    pub cur_loop_obj_type: Option<Type>,
    pub single_branch_definition_vars: BTreeSet<String>,
}

impl TypecheckState {
    pub fn new() -> Self {
        TypecheckState {
            stack: TypecheckStack::default(),
            locals: SymbolTable::from([
                (
                    "this".to_string(),
                    Type::Class(DynClassType::new(Arc::new(RelationType::default()))),
                ),
                ("database".to_string(), Type::String(None)),
                ("schema".to_string(), Type::String(None)),
                ("identifier".to_string(), Type::String(None)),
                (
                    "config".to_string(),
                    Type::Class(DynClassType::new(Arc::new(ConfigType::default()))),
                ),
                (
                    "model".to_string(),
                    Type::Class(DynClassType::new(Arc::new(ModelType::default()))),
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
                ("TARGET_PACKAGE_NAME".to_string(), Type::String(None)),
                ("TARGET_UNIQUE_ID".to_string(), Type::String(None)),
                (
                    "api".to_string(),
                    Type::Class(DynClassType::new(Arc::new(ApiType::default()))),
                ),
                (
                    "adapter".to_string(),
                    Type::Class(DynClassType::new(Arc::new(AdapterType::default()))),
                ),
                (
                    "ref".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(RefFunctionType::default()))),
                ),
                (
                    "source".to_string(),
                    Type::Function(DynFunctionType::new(
                        Arc::new(SourceFunctionType::default()),
                    )),
                ),
                (
                    "diff_of_two_dicts".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        DiffOfTwoDictsFunctionType::default(),
                    ))),
                ),
                (
                    "log".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(LogFunctionType::default()))),
                ),
                (
                    "exceptions".to_string(),
                    Type::Class(DynClassType::new(Arc::new(ExceptionsType::default()))),
                ),
                (
                    "length".to_string(),
                    Type::Function(DynFunctionType::new(
                        Arc::new(LengthFunctionType::default()),
                    )),
                ),
                (
                    "join".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(JoinFunctionType::default()))),
                ),
                (
                    "map".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(MapFunctionType::default()))),
                ),
                (
                    "list".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(ListFunctionType::default()))),
                ),
                (
                    "string".to_string(),
                    Type::Function(DynFunctionType::new(
                        Arc::new(StringFunctionType::default()),
                    )),
                ),
                (
                    "replace".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        ReplaceFunctionType::default(),
                    ))),
                ),
                (
                    "cast".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(CastFunctionType::default()))),
                ),
                (
                    "trim".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(TrimFunctionType::default()))),
                ),
                (
                    "upper".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(UpperFunctionType::default()))),
                ),
                (
                    "lower".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(LowerFunctionType::default()))),
                ),
                (
                    "loop".to_string(),
                    Type::Class(DynClassType::new(Arc::new(LoopType::default()))),
                ),
                (
                    "env_var".to_string(),
                    Type::Function(DynFunctionType::new(
                        Arc::new(EnvVarFunctionType::default()),
                    )),
                ),
                (
                    "pre_hooks".to_string(),
                    Type::List(ListType::new(Type::Class(DynClassType::new(Arc::new(
                        HookType::default(),
                    ))))),
                ),
                (
                    "post_hooks".to_string(),
                    Type::List(ListType::new(Type::Class(DynClassType::new(Arc::new(
                        HookType::default(),
                    ))))),
                ),
                ("sql".to_string(), Type::String(None)),
                (
                    "target".to_string(),
                    Type::Class(DynClassType::new(Arc::new(RelationType::default()))),
                ),
                ("compiled_code".to_string(), Type::String(None)),
                (
                    "modules".to_string(),
                    Type::Class(DynClassType::new(Arc::new(ModulesType::default()))),
                ),
                (
                    "range".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(RangeFunctionType::default()))),
                ),
                ("execute".to_string(), Type::Bool),
                (
                    "context".to_string(),
                    Type::Dict(DictType::new(Type::String(None), Type::Any { hard: true })),
                ),
                (
                    "defer_relation".to_string(),
                    Type::Class(DynClassType::new(Arc::new(RelationType::default()))),
                ),
                (
                    "dbt".to_string(),
                    Type::Class(DynClassType::new(Arc::new(DbtType::default()))),
                ),
                ("null".to_string(), Type::None),
                (
                    "get_column_schema_from_query".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        GetColumnSchemaFromQueryFunction::default(),
                    ))),
                ),
                (
                    "try_or_compiler_error".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        TryOrCompilerErrorFunctionType::default(),
                    ))),
                ),
                (
                    "write".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(WriteFunctionType::default()))),
                ),
                (
                    "submit_python_job".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        SubmitPythonJobFunctionType::default(),
                    ))),
                ),
                (
                    "flags".to_string(),
                    Type::Class(DynClassType::new(Arc::new(FlagsType::default()))),
                ),
                (
                    "selectattr".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(
                        SelectAttrFunctionType::default(),
                    ))),
                ),
                (
                    "tojson".to_string(),
                    Type::Function(DynFunctionType::new(
                        Arc::new(ToJsonFunctionType::default()),
                    )),
                ),
                (
                    "render".to_string(),
                    Type::Function(DynFunctionType::new(
                        Arc::new(RenderFunctionType::default()),
                    )),
                ),
                (
                    "print".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(PrintFunctionType::default()))),
                ),
                (
                    "first".to_string(),
                    Type::Function(DynFunctionType::new(Arc::new(FirstFunctionType::default()))),
                ),
            ]),
            frame_base: 0,
            cur_loop_obj_type: None,
            single_branch_definition_vars: BTreeSet::new(),
        }
    }

    pub fn drop_top(&mut self, n: usize) {
        self.stack.truncate(self.stack.len().saturating_sub(n));
    }

    #[track_caller]
    pub fn peek(&self) -> &TypeWithConstraint {
        self.stack.last().unwrap()
    }

    pub fn push_frame(&mut self) {
        self.frame_base = self.stack.len();
    }

    pub fn get_call_args(&mut self, n: u16) -> (Vec<Type>, BTreeMap<String, Type>) {
        // get n items from the stack
        let all_args = self
            .stack
            .drain(self.stack.len().saturating_sub(n as usize)..)
            .collect::<Vec<_>>();
        if let Some(Type::Kwargs(kwargs)) = all_args.last().cloned().map(|t| t.inner) {
            let len = all_args.len();
            (
                all_args
                    .into_iter()
                    .take(len - 1)
                    .map(|t| t.inner)
                    .collect::<Vec<_>>(),
                kwargs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_ref().clone()))
                    .collect(),
            )
        } else {
            (
                all_args.into_iter().map(|t| t.inner).collect::<Vec<_>>(),
                BTreeMap::new(),
            )
        }
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
    pub instr: &'src [Instruction<'src>], // TODO: put instr and &function_registry into in_states
    pub cfg: CFG,
    pub in_states: Vec<TypecheckState>,
    pub function_registry: Arc<FunctionRegistry>,
}

/// Typecheck logic implementation
impl<'src> TypeChecker<'src> {
    pub fn new(
        instr: &'src [Instruction<'src>],
        cfg: CFG,
        funcsigns: Arc<FunctionRegistry>,
    ) -> Self {
        let in_states = vec![TypecheckState::default(); cfg.blocks.len()];
        Self {
            instr,
            cfg,
            in_states,
            function_registry: funcsigns,
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
                            Self::merge_into(&mut self.in_states[*succ], &out_state, visited[*succ])
                        };
                        if !visited[*succ] || changed {
                            worklist.push_back(*succ);
                            visited[*succ] = true;
                        }
                    }
                    if let Some(macro_block) = self.cfg.get_block(bb_id) {
                        // find the last block in a macro
                        if macro_block.successor.is_empty() {
                            if let Some(macro_name) = macro_block.current_macro.as_ref() {
                                if let Some(funcsign) = self.function_registry.get(macro_name) {
                                    if let Some(user_defined_func) =
                                        funcsign.downcast_ref::<UserDefinedFunctionType>()
                                    {
                                        let expected_ret_type = user_defined_func.ret_type.clone();
                                        if !expected_ret_type.is_subtype_of(&Type::String(None)) {
                                            warning_printer.warn(
                                                &macro_block.span.unwrap_or_default(),
                                                &format!("Type mismatch: expected return type {expected_ret_type}, got String"),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => match e.try_abrupt_return() {
                    Some(rv) => {
                        let rv_type = rv
                            .downcast_object_ref::<Type>()
                            .cloned()
                            .unwrap_or(Type::Any { hard: false });
                        if matches!(rv_type, Type::Exception) {
                            continue;
                        }
                        if let Some(macro_block) = self.cfg.get_block(bb_id) {
                            if let Some(macro_name) = macro_block.current_macro.as_ref() {
                                if let Some(funcsign) = self.function_registry.get(macro_name) {
                                    if let Some(user_defined_func) =
                                        funcsign.downcast_ref::<UserDefinedFunctionType>()
                                    {
                                        let expected_ret_type = user_defined_func.ret_type.clone();
                                        // try match rv with registry_ret_type
                                        let rv_type = rv
                                            .downcast_object_ref::<Type>()
                                            .cloned()
                                            .unwrap_or(Type::Any { hard: false });
                                        let span = e.get_abrupt_return_span();
                                        if !rv_type.is_subtype_of(&expected_ret_type) {
                                            warning_printer.warn(
                                                &span,
                                                &format!(
                                                    "Type mismatch: expected return type {expected_ret_type}, got {rv_type}"
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
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
                Instruction::StoreLocal(name, span) => {
                    // TYPECHECK: NO
                    let value_type = match typestate.stack.pop_inner() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                format!("Stack underflow on store local {name} {span:?}"),
                            ));
                        }
                    };
                    typestate
                        .locals
                        .insert(name.to_string(), value_type.clone())?;
                }
                Instruction::Lookup(name, span) => {
                    // TYPECHECK: NO
                    let name_str: &str = name;
                    // first try to search in self.cfg.get_block(bb_id).type_narrow
                    if let Ok(ty) = typestate.locals.get(name_str) {
                        if typestate.single_branch_definition_vars.contains(name_str) {
                            warning_printer.warn(span, &format!("Variable '{name_str}' is not defined in one of its predecessor blocks."));
                            typestate.stack.push(Type::Any { hard: false });
                        } else {
                            typestate.stack.push(ty.clone());
                        }
                    } else if let Some(function) = self.function_registry.get(name_str) {
                        typestate.stack.push(Type::Function(function.clone()));
                    } else {
                        warning_printer.warn(
                            span,
                            &format!("Potential TypeError: Unknown local variable '{name_str}'"),
                        );
                        typestate.stack.push(Type::Any { hard: false });
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
                        Ok(rv) => {
                            typestate.stack.push(rv);
                        }
                        Err(e) => {
                            warning_printer.warn(
                                span,
                                &format!("Unknown attribute '{value_type}.{name}': {e}"),
                            );
                            typestate.stack.push(Type::Any { hard: false });
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
                Instruction::GetItem(span) => {
                    // TYPECHECK: YES
                    let index = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on get item",
                            ))
                        }
                    };
                    let base = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on get item",
                            ))
                        }
                    };
                    let rv = base.subscript(&index);
                    match rv {
                        Ok(rv) => typestate.stack.push(rv),
                        Err(e) => {
                            warning_printer
                                .warn(span, &format!("Failed to subscript {base}[{index}]: {e}"));
                            typestate.stack.push(Type::Any { hard: false });
                        }
                    }
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
                        if !slice_type.is_subtype_of(&Type::Integer(None).into()) {
                            warning_printer.warn(
                                span,
                                &format!("Type mismatch for slice {name}: type = {slice_type}"),
                            );
                        }
                    }

                    typestate.stack.push(Type::Any { hard: false });
                }
                Instruction::LoadConst(val) => {
                    // TYPECHECK: NO
                    typestate.stack.push(infer_type_from_const_value(val));
                }
                Instruction::BuildMap(pair_count) => {
                    // TYPECHECK: NO
                    let mut args_map = BTreeMap::new();
                    for _ in 0..*pair_count {
                        let v = match typestate.stack.pop_inner() {
                            Some(val) => val,
                            None => {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Stack underflow on build map value",
                                ))
                            }
                        };
                        let k = match typestate.stack.pop() {
                            Some(val) => val,
                            None => {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Stack underflow on build map key",
                                ))
                            }
                        };

                        args_map.insert(k.to_string(), v);
                    }
                    typestate
                        .stack
                        .push(Type::Struct(StructType::new(args_map)));
                }
                Instruction::BuildKwargs(pair_count) => {
                    // TYPECHECK: NO
                    let mut args_map = BTreeMap::new();
                    for _ in 0..*pair_count {
                        let value = match typestate.stack.pop_inner() {
                            Some(val) => val,
                            None => Type::Any { hard: false },
                        };
                        let key = match typestate.stack.pop_inner() {
                            Some(val) => val,
                            None => Type::Any { hard: false },
                        };
                        if let Type::String(Some(key)) = key {
                            args_map.insert(key.to_string(), Box::new(value));
                        }
                    }
                    typestate.stack.push(Type::Kwargs(args_map));
                }
                Instruction::MergeKwargs(count) => {
                    // TYPECHECK: NO
                    let mut args_map = BTreeMap::new();
                    for _ in 0..*count {
                        let kwargs = match typestate.stack.pop_inner() {
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
                Instruction::BuildList(n, _span) => {
                    // TODO
                    // We need to modify BuildList to make the arg mandatory
                    // Consider add the loopstart instruction at the start of a loop with a filter
                    // When calling the loopstart instruction we backup stack
                    // When calling the BuildList instruction we restore the stack

                    let count = n.unwrap_or(0);
                    if count == 0 {
                        typestate
                            .stack
                            .push(Type::List(ListType::new(Type::Any { hard: true })));
                    } else {
                        // Collect the types of the items to be popped
                        if let Some(mut item_type) = typestate.stack.pop() {
                            for _ in 1..count {
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
                            typestate
                                .stack
                                .push(Type::List(ListType::new(item_type.into_inner())));
                        } else {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on build list",
                            ));
                        }
                    }
                }
                Instruction::BuildTuple(n, span) => {
                    if let Some(n) = n {
                        let mut item_types = Vec::new();
                        for _ in 0..*n {
                            let item_type = match typestate.stack.pop_inner() {
                                Some(val) => val,
                                None => {
                                    return Err(crate::Error::new(
                                        crate::error::ErrorKind::InvalidOperation,
                                        "Stack underflow on build tuple",
                                    ));
                                }
                            };
                            item_types.push(item_type);
                        }
                        item_types.reverse();
                        typestate
                            .stack
                            .push(Type::Tuple(TupleType::new(item_types)));
                    } else {
                        warning_printer.warn(
                            span,
                            "Type mismatch for build tuple: expected tuple with a fixed number of elements, got None",
                        );
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }
                Instruction::UnpackList(count, span) => {
                    let tuple_type = match typestate.stack.pop_inner() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on unpack list",
                            ))
                        }
                    };
                    // if tuple_type is not a Tuple, we have a type error
                    match &tuple_type {
                        Type::Tuple(tuple) if tuple.fields.len() == *count => {
                            for field_type in tuple.fields.iter().rev() {
                                typestate.stack.push(field_type.clone());
                            }
                        }
                        _ => {
                            for _ in 0..*count {
                                typestate.stack.push(Type::Any { hard: false });
                            }
                            warning_printer.warn(
                                span,
                                &format!(
                                    "Type mismatch for unpack list: expected Tuple with {count} elements, got {tuple_type}"
                                ),
                            );
                        }
                    };
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

                    let result_type = lhs_type.can_binary_op_with(&rhs_type, op);
                    if let Some(result_type) = result_type {
                        typestate.stack.push(result_type);
                    } else {
                        warning_printer.warn(
                            span,
                            &format!("Type mismatch for {op}: lhs = {lhs_type}, rhs = {rhs_type}"),
                        );
                        typestate.stack.push(Type::Any { hard: false });
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
                    if !lhs_type.is_subtype_of(&Type::String(None).into())
                        && rhs_type.is_subtype_of(
                            &Type::List(ListType {
                                element: Box::new(Type::None),
                            })
                            .into(),
                        )
                    {
                        typestate.stack.push(Type::String(None));
                        continue;
                    }

                    let result_type = lhs_type.can_binary_op_with(&rhs_type, op);
                    if let Some(result_type) = result_type {
                        typestate.stack.push(result_type);
                    } else {
                        warning_printer.warn(
                            span,
                            &format!("Type mismatch for {op}: lhs = {lhs_type}, rhs = {rhs_type}"),
                        );
                        typestate.stack.push(Type::Any { hard: false });
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
                            &format!("Type mismatch for {op}: lhs = {lhs_type}, rhs = {rhs_type}"),
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
                    let rhs_type = match typestate.stack.pop_inner() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on string concat operation",
                            ))
                        }
                    };
                    let lhs_type = match typestate.stack.pop_inner() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on string concat operation",
                            ))
                        }
                    };

                    typestate.stack.push(Type::String(
                        if let (Type::String(Some(lhs_value)), Type::String(Some(rhs_value))) =
                            (lhs_type, rhs_type)
                        {
                            Some(format!("{lhs_value}{rhs_value}"))
                        } else {
                            None
                        },
                    ));
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
                        typestate.stack.push(Type::Any { hard: false });
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
                    if let Some(iterable) = typestate.stack.pop_inner() {
                        let element_type = match iterable {
                            Type::List(list) => *list.element.clone(),
                            Type::Iterable(iterable) => *iterable.element.clone(),
                            Type::Dict(dict) => *dict.key.clone(),
                            Type::Any { hard: true } => Type::Any { hard: true },
                            Type::Class(class) if class.is::<AgateTableType>() => {
                                Type::Class(DynClassType::new(Arc::new(ApiColumnType::default())))
                            }
                            _ => {
                                warning_printer.warn(
                                    span,
                                    &format!(
                                        "Type mismatch for push loop: expected a iterable type, found {iterable:?}"
                                    ),
                                );
                                Type::Any { hard: false }
                            }
                        };
                        typestate.cur_loop_obj_type = Some(element_type);
                    } else {
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "Stack underflow on push loop",
                        ));
                    }

                    typestate.push_frame();
                }
                Instruction::Iterate(_jump_target) => {
                    // TYPECHECK: NO
                    if let Some(element_type) = typestate.cur_loop_obj_type.clone() {
                        typestate.stack.push(element_type);
                    } else {
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "current loop object type is not set",
                        ));
                    }
                }
                Instruction::PushDidNotIterate => {
                    // TYPECHECK: NO
                    typestate.stack.push(Type::Any { hard: false });
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
                            &format!("Type mismatch for jump condition: type = {a}"),
                        );
                    }
                }
                Instruction::JumpIfTrueOrPop(_jump_target, span) => {
                    // TYPECHECK: YES
                    // the operand must be a boolean
                    let a = typestate.peek().clone();

                    if !a.is_condition() {
                        warning_printer.warn(
                            span,
                            &format!("Type mismatch for jump condition: type = {a}"),
                        );
                    }
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

                    if !a.is_subtype_of(&Type::String(None).into()) {
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
                    typestate.stack.push(Type::String(None));
                }
                Instruction::ApplyFilter(name, arg_count, _local_id, span) => {
                    // TYPECHECK: NO

                    if let Ok(Type::Function(funcsign)) =
                        typestate.locals.get(name).map(|t| t.inner)
                    {
                        if let Some(arg_cnt) = arg_count {
                            let funcsign = funcsign.clone();
                            let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                            match funcsign.resolve_arguments(&args, &kwargs) {
                                Ok(ret_type) => {
                                    typestate.stack.push(ret_type.clone());
                                }
                                Err(msg) => {
                                    warning_printer.warn(
                                        span,
                                        &format!("Type mismatch for function '{name}': {msg}"),
                                    );
                                    typestate.stack.push(Type::Any { hard: false });
                                }
                            }
                        }
                    } else {
                        // TODO: handle the case when arg_count is None
                        warning_printer.warn(
                            span,
                            &format!("Potential TypeError: Filter '{name}' is not defined."),
                        );
                        typestate.stack.push(Type::Any { hard: false });
                    }
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
                        if let Some(arg) = typestate.stack.pop_inner() {
                            return Err(crate::Error::abrupt_return(
                                Value::from_object(arg),
                                *span,
                            ));
                        }
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "Stack underflow on return",
                        ));
                    } else if *name == "caller" {
                        // judge whether current block is a macro
                        if let Some(block) = self.cfg.get_block(bb_id) {
                            if let Some(macro_name) = &block.current_macro {
                                if let Some(arg_cnt) = arg_count {
                                    let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                                    match InternalCaller::default()
                                        .resolve_arguments(&args, &kwargs)
                                    {
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
                                            typestate.stack.push(Type::Any { hard: false });
                                        }
                                    }
                                } else {
                                    return Err(crate::Error::new(
                                        crate::error::ErrorKind::InvalidOperation,
                                        "Function 'caller' requires an argument count",
                                    ));
                                }
                            } else {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Function 'caller' requires a macro block",
                                ));
                            }
                        }
                    } else if *name == "source" || *name == "ref" {
                        if let Some(arg_cnt) = arg_count {
                            let (args, kwargs) = typestate.get_call_args(*arg_cnt);
                            let function_type = match *name {
                                "source" => {
                                    DynFunctionType::new(Arc::new(SourceFunctionType::default()))
                                }
                                "ref" => DynFunctionType::new(Arc::new(RefFunctionType::default())),
                                _ => unreachable!(),
                            };
                            match function_type.resolve_arguments(&args, &kwargs) {
                                Ok(ret_type) => {
                                    typestate.stack.push(ret_type.clone());
                                }
                                Err(msg) => {
                                    warning_printer.warn(
                                        span,
                                        &format!("Type mismatch for function '{name}': {msg}"),
                                    );
                                    typestate.stack.push(Type::Any { hard: false });
                                }
                            }
                        }
                    } else if let Ok(Type::Function(funcsign)) =
                        typestate.locals.get(name).map(|t| t.inner)
                    {
                        if let Some(arg_cnt) = arg_count {
                            let funcsign = funcsign.clone();
                            let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                            match funcsign.resolve_arguments(&args, &kwargs) {
                                Ok(ret_type) => {
                                    typestate.stack.push(ret_type.clone());
                                }
                                Err(msg) => {
                                    warning_printer.warn(
                                        span,
                                        &format!("Type mismatch for function '{name}': {msg}"),
                                    );
                                    typestate.stack.push(Type::Any { hard: false });
                                }
                            }
                        }
                    } else if let Ok(Type::Any { hard: true }) =
                        typestate.locals.get(name).map(|t| t.inner)
                    {
                        typestate.stack.push(Type::Any { hard: true });
                    } else if let Some(funcsign) = self.function_registry.get(name.to_owned()) {
                        if let Some(arg_cnt) = arg_count {
                            let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                            match funcsign.resolve_arguments(&args, &kwargs) {
                                Ok(ret_type) => {
                                    typestate.stack.push(ret_type.clone());
                                }
                                Err(msg) => {
                                    warning_printer.warn(
                                        span,
                                        &format!("Type mismatch for function '{name}': {msg}"),
                                    );
                                    typestate.stack.push(Type::Any { hard: false });
                                }
                            }
                        }
                    } else if let Some(arg_cnt) = arg_count {
                        let _ = typestate.get_call_args(*arg_cnt);
                        warning_printer.warn(
                            span,
                            &format!("Potential TypeError: Function '{name}' is not defined."),
                        );
                        typestate.stack.push(Type::Any { hard: false });
                    } else {
                        // TODO: handle the case when arg_count is None
                        warning_printer.warn(
                            span,
                            &format!("Potential TypeError: Function '{name}' is not defined."),
                        );
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }
                Instruction::CallMethod(name, arg_count, span) => {
                    // TYPECHECK: NO? (Maybe add method check later)

                    let count = arg_count.unwrap_or(0);
                    if count > 0 {
                        // Pop (arg_count - 1) arguments
                        let (method_args, kwargs) = typestate.get_call_args(count - 1);
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
                            typestate.stack.push(self_type);
                            continue;
                        }

                        let function = match self_type.get_attribute(name) {
                            Ok(rv) => rv,
                            Err(e) => {
                                warning_printer.warn(
                                    span,
                                    &format!("Unknown method '{self_type:?}.{name}': {e}"),
                                );
                                typestate.stack.push(Type::Any { hard: false });
                                continue;
                            }
                        };

                        if function.is_any() {
                            typestate.stack.push(function);
                            continue;
                        }

                        let result = match function.call(&method_args, &kwargs) {
                            Ok(rv) => {
                                if *name == "raise_not_implemented"
                                    || *name == "raise_compiler_error"
                                    || *name == "column_type_missing"
                                    || *name == "warn"
                                {
                                    return Err(crate::Error::abrupt_return(
                                        Value::from_object(Type::Exception),
                                        *span,
                                    ));
                                } else {
                                    rv
                                }
                            }
                            Err(e) => {
                                warning_printer.warn(
                                    span,
                                    &format!("Method call failed '{self_type}.{name}': {e}"),
                                );
                                Type::Any { hard: false }
                            }
                        };

                        typestate.stack.push(result);
                    } else {
                        // TODO: handle the case when arg_count is None
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            format!(
                                "The first argument(self) of method call is missing at {span:?}"
                            ),
                        ));
                    }
                }
                Instruction::CallObject(arg_count, span) => {
                    // TYPECHECK: YES
                    let count = arg_count.unwrap_or(0);
                    if count > 0 {
                        // Pop (arg_count - 1) arguments
                        let (args, kwargs) = typestate.get_call_args(count - 1);
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
                            typestate.stack.push(self_type);
                            continue;
                        }

                        let result = match self_type.call(&args, &kwargs) {
                            Ok(rv) => rv,
                            Err(e) => {
                                warning_printer.warn(
                                    span,
                                    &format!("Object call failed '{self_type:?}()': {e}"),
                                );
                                Type::Any { hard: false }
                            }
                        };

                        typestate.stack.push(result);
                    } else {
                        // TODO: handle the case when arg_count is None
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            format!(
                                "The first argument(self) of method call is missing at {span:?}"
                            ),
                        ));
                    }
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

                    if !a.is_subtype_of(&Type::String(None).into()) {
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
                    typestate.stack.push(Type::Any { hard: false });
                }
                #[cfg(feature = "macros")]
                Instruction::BuildMacro(name, _offset, _flags, span) => {
                    // TYPECHECK: NO?
                    // BuildMacro consume the parameter names in the stack
                    if typestate.stack.pop().is_none() {
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "Stack underflow on build macro",
                        ));
                    }
                    // BuildMacro consume the closure in the stack
                    if typestate.stack.pop().is_none() {
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "Stack underflow on build macro",
                        ));
                    }
                    // look up the function in the function registry
                    if let Some(function) = self.function_registry.get(*name) {
                        typestate.stack.push(Type::Function(function.clone()));
                    } else if *name == "caller" {
                        use crate::types::function::CallerFunctionType;

                        typestate
                            .stack
                            .push(Type::Function(DynFunctionType::new(Arc::new(
                                CallerFunctionType::default(),
                            ))));
                    } else {
                        warning_printer.warn(
                            span,
                            &format!("Function '{name}' is not defined in the function registry."),
                        );
                        typestate.stack.push(Type::Any { hard: false });
                    }
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
                    typestate.stack.push(Type::Any { hard: false });
                }
                Instruction::MacroStart(_line, _col, _index, _, _, _) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::MacroStop(_line, _col, _index) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::MacroName(_name, _span) => {
                    // TYPECHECK: NO
                }
                Instruction::TypeConstraint(type_constraint, true_branch) => {
                    let name = &type_constraint.name;
                    match &type_constraint.operation {
                        TypeConstraintOperation::NotNull(is_true) => {
                            if is_true ^ true_branch {
                                if let Ok(type_) = typestate.locals.get(name) {
                                    if type_.is_optional() {
                                        typestate.locals.insert(name, Type::None)?;
                                    }
                                }
                            } else if let Ok(type_) = typestate.locals.get(name) {
                                if type_.is_optional() {
                                    let non_optional_type = type_.get_non_optional_type();
                                    typestate.locals.insert(name, non_optional_type)?;
                                }
                            }
                        }
                        TypeConstraintOperation::Is(test_name, is_true) => {
                            let test_type = Type::from_str(test_name)?;
                            if is_true ^ true_branch {
                                if let Ok(type_) = typestate.locals.get(name) {
                                    if type_.is_optional() {
                                        typestate.locals.insert(name, type_.exclude(&test_type))?;
                                    }
                                }
                            } else if let Ok(type_) = typestate.locals.get(name) {
                                if type_.is_optional() {
                                    typestate.locals.insert(name, test_type)?;
                                }
                            }
                        }
                    }
                }
                Instruction::LoadType(value) => {
                    if let Some(object) = value.as_object() {
                        if let Some(type_) = object.downcast_ref::<Type>() {
                            let type_: TypeWithConstraint = type_.clone().into();
                            typestate.stack.push(type_);
                        } else {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Invalid type",
                            ));
                        }
                    } else {
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "Invalid type",
                        ));
                    }
                }
                Instruction::UnionType => {
                    let a = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on union type",
                            ));
                        }
                    };
                    let b = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on union type",
                            ));
                        }
                    };
                    let union_type = a.union(&b);
                    typestate.stack.push(union_type);
                }
            }
        }
        Ok(typestate)
    }

    /// Merges the source typecheck state into the destination state at the merge point.
    fn merge_into(dst: &mut TypecheckState, src: &TypecheckState, visited: bool) -> bool {
        let mut changed = false;

        let min_len = dst.stack.len().min(src.stack.len());
        dst.stack.truncate(min_len);

        if dst.cur_loop_obj_type != src.cur_loop_obj_type {
            dst.cur_loop_obj_type = match (&dst.cur_loop_obj_type, &src.cur_loop_obj_type) {
                (Some(a), Some(b)) if a == b => Some(a.clone()),
                (None, Some(t)) => Some(t.clone()),
                (Some(t), None) => Some(t.clone()),
                (None, None) => None,
                _ => Some(Type::Any { hard: false }),
            };
            changed = true;
        }

        for i in 0..min_len {
            let dst_type = dst.stack.get(i).unwrap().clone();

            let union_type = dst_type.union(&src.stack.get(i).unwrap().clone());
            if union_type.inner != dst_type.inner {
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
            match (dst.locals.get_mut(&name), src.locals.get_ref(&name)) {
                (Some(dst_type), Some(src_type)) => {
                    let union_type = dst_type.union(src_type);
                    if union_type.inner != dst_type.inner {
                        *dst_type = union_type;
                        changed = true;
                    }
                }
                (Some(_), None) => {}
                (None, Some(_src_type)) => {
                    if !visited {
                        dst.single_branch_definition_vars.insert(name.clone());
                    }
                    dst.locals
                        .insert(name.clone(), Type::Any { hard: true })
                        .unwrap();
                    changed = true;
                }
                (None, None) => {}
            }
        }

        changed
    }
}
