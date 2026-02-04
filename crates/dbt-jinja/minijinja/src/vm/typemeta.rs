use dashmap::DashMap;

use crate::compiler::cfg::CFG;
use crate::compiler::codegen::{TypeConstraintOperation, Variable};
use crate::compiler::instructions::Instruction;
use crate::compiler::tokens::Span;
use crate::compiler::typecheck::FunctionRegistry;
use crate::constants::{DBT_AND_ADAPTERS_NAMESPACE, ROOT_PACKAGE_NAME, TARGET_PACKAGE_NAME};
use crate::types::function::{LambdaType, UserDefinedFunctionType};
use crate::types::list::ListType;
use crate::types::struct_::StructType;
use crate::types::tuple::TupleType;
use crate::types::utils::{infer_type_from_const_value, instr_name};
use crate::types::DynObject;
use crate::types::Type;
use crate::value::ValueMap;
use crate::vm::listeners::{DefaultTypecheckingEventListener, TypecheckingEventListener};
use crate::{ErrorKind, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::Hash;
use std::ops::RangeBounds;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, vec};

/// A tiny abstract value domain for dependency extraction.
///
/// We only track whether a stack/local value is a concrete namespace (for resolving
/// `namespace.method(...)` calls), or unknown.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
enum DepValue {
    Namespace(String),
    StringLiteral(String),
    Unknown,
}

#[allow(dead_code)]
impl DepValue {
    fn union(&self, other: &DepValue) -> DepValue {
        match (self, other) {
            (DepValue::Namespace(a), DepValue::Namespace(b)) if a == b => {
                DepValue::Namespace(a.clone())
            }
            (DepValue::StringLiteral(a), DepValue::StringLiteral(b)) if a == b => {
                DepValue::StringLiteral(a.clone())
            }
            _ => DepValue::Unknown,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
struct DepStack(Vec<DepValue>);

#[allow(dead_code)]
impl DepStack {
    fn push(&mut self, v: DepValue) {
        self.0.push(v);
    }

    fn pop(&mut self) -> Option<DepValue> {
        self.0.pop()
    }

    fn last(&self) -> Option<&DepValue> {
        self.0.last()
    }

    fn truncate(&mut self, n: usize) {
        self.0.truncate(n);
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
struct DepState {
    stack: DepStack,
    locals: BTreeMap<String, DepValue>,
}

/// CFG-based dependency analyzer.
///
/// This mirrors the `TypeChecker`'s CFG worklist iteration, but uses a dramatically simpler
/// domain (namespaces + unknown) and emits `on_function_call` events without computing types.
#[allow(dead_code)]
pub struct DependencyAnalyzer<'src> {
    instr: &'src [Instruction<'src>],
    cfg: CFG,
    in_states: Vec<DepState>,
    function_registry: Arc<FunctionRegistry>,
    builtins: Arc<DashMap<String, Type>>,
}

#[allow(dead_code)]
impl<'src> DependencyAnalyzer<'src> {
    pub fn new(
        instr: &'src [Instruction<'src>],
        cfg: CFG,
        funcsigns: Arc<FunctionRegistry>,
        builtins: Arc<DashMap<String, Type>>,
    ) -> Self {
        let in_states = vec![DepState::default(); cfg.blocks.len()];
        Self {
            instr,
            cfg,
            in_states,
            function_registry: funcsigns,
            builtins,
        }
    }

    /// Runs a dependency-only analysis and emits dependency edges via `listener.on_function_call`.
    ///
    /// This does **not** compute types. The only tracked information is a minimal namespace domain
    /// so we can resolve `namespace.method(...)` calls similarly to the typechecker.
    pub fn check(
        &mut self,
        listener: Rc<dyn TypecheckingEventListener>,
        typecheck_resolved_context: BTreeMap<String, Value>,
    ) -> Result<(), crate::Error> {
        let mut worklist = VecDeque::new();
        let mut visited = vec![false; self.cfg.blocks.len()];
        let mut first_merge = vec![true; self.cfg.blocks.len()];

        for (i, block) in self.cfg.blocks.iter().enumerate() {
            if block.predecessor.is_empty() {
                self.in_states[i] = DepState::default();
                worklist.push_back(i);
                visited[i] = true;
                first_merge[i] = false;
            }
        }

        while let Some(bb_id) = worklist.pop_front() {
            listener.clone().new_block(bb_id);
            let out_state =
                self.transfer_block(bb_id, listener.clone(), typecheck_resolved_context.clone())?;

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
        Ok(())
    }

    fn merge_into(dst: &mut DepState, src: &DepState) -> bool {
        let mut changed = false;

        let min_len = dst.stack.len().min(src.stack.len());
        dst.stack.truncate(min_len);

        for i in 0..min_len {
            let dst_v = dst.stack.0[i].clone();
            let union_v = dst_v.union(&src.stack.0[i]);
            if union_v != dst_v {
                dst.stack.0[i] = union_v;
                changed = true;
            }
        }

        let all_keys: std::collections::HashSet<_> = dst
            .locals
            .keys()
            .chain(src.locals.keys())
            .cloned()
            .collect();
        for k in all_keys {
            let new_v = match (dst.locals.get(&k), src.locals.get(&k)) {
                (Some(a), Some(b)) => a.union(b),
                (Some(_a), None) => continue,
                (None, Some(b)) => b.clone(),
                (None, None) => continue,
            };
            if dst.locals.get(&k) != Some(&new_v) {
                dst.locals.insert(k, new_v);
                changed = true;
            }
        }

        changed
    }

    fn dep_from_name(&self, state: &DepState, name: &str) -> DepValue {
        if let Some(v) = state.locals.get(name) {
            return v.clone();
        }
        if let Some(t) = self.builtins.get(name) {
            if let Type::Namespace(ns) = t.value() {
                return DepValue::Namespace(ns.clone());
            }
        }
        DepValue::Unknown
    }

    fn stack_pop(stack: &mut DepStack, kind: &'static str) -> Result<DepValue, crate::Error> {
        stack.pop().ok_or_else(|| {
            crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Stack underflow on {kind}"),
            )
        })
    }

    fn stack_pop_n(stack: &mut DepStack, n: usize, kind: &'static str) -> Result<(), crate::Error> {
        if stack.len() < n {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Stack underflow on {kind}"),
            ));
        }
        for _ in 0..n {
            stack.pop();
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn transfer_block(
        &mut self,
        bb_id: usize,
        listener: Rc<dyn TypecheckingEventListener>,
        typecheck_resolved_context: BTreeMap<String, Value>,
    ) -> Result<DepState, crate::Error> {
        let mut state = self.in_states[bb_id].clone();
        let slice = self.cfg.instructions(bb_id, self.instr);
        let attempts: &mut Vec<String> = &mut Vec::new();

        for inst in slice.iter() {
            match inst {
                Instruction::Swap => {
                    let a = Self::stack_pop(&mut state.stack, "swap")?;
                    let b = Self::stack_pop(&mut state.stack, "swap")?;
                    state.stack.push(b);
                    state.stack.push(a);
                }
                Instruction::EmitRaw(_, _) => {}
                Instruction::Emit(_) => {
                    let _ = Self::stack_pop(&mut state.stack, "emit")?;
                }
                Instruction::StoreLocal(name, _span) => {
                    let v = Self::stack_pop(&mut state.stack, "store local")?;
                    state.locals.insert((*name).to_string(), v);
                }
                Instruction::Lookup(name, _span) => {
                    let v = self.dep_from_name(&state, name);
                    state.stack.push(v);
                }
                Instruction::GetAttr(_name, _span) => {
                    let _ = Self::stack_pop(&mut state.stack, "get attr")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::SetAttr(_name, _span) => {
                    let _ = Self::stack_pop(&mut state.stack, "set attr value")?;
                    let _ = Self::stack_pop(&mut state.stack, "set attr object")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::GetItem(_span) => {
                    let _ = Self::stack_pop(&mut state.stack, "get item key")?;
                    let _ = Self::stack_pop(&mut state.stack, "get item object")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::Slice(_span) => {
                    // slice consumes 4 items (a, b, step, stop) in typechecker
                    let _ = Self::stack_pop(&mut state.stack, "slice stop")?;
                    let _ = Self::stack_pop(&mut state.stack, "slice step")?;
                    let _ = Self::stack_pop(&mut state.stack, "slice b")?;
                    let _ = Self::stack_pop(&mut state.stack, "slice a")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::LoadConst(val) => {
                    if let Some(s) = val.as_str() {
                        state.stack.push(DepValue::StringLiteral(s.to_string()));
                    } else {
                        state.stack.push(DepValue::Unknown);
                    }
                }
                Instruction::BuildMap(pair_count, _span) => {
                    // pop 2 * pair_count, push result
                    Self::stack_pop_n(&mut state.stack, pair_count.saturating_mul(2), "build map")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::BuildKwargs(pair_count) => {
                    Self::stack_pop_n(
                        &mut state.stack,
                        pair_count.saturating_mul(2),
                        "build kwargs",
                    )?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::MergeKwargs(count) => {
                    Self::stack_pop_n(&mut state.stack, *count, "merge kwargs")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::BuildList(n, _span) => {
                    let count = n.unwrap_or(0);
                    Self::stack_pop_n(&mut state.stack, count, "build list")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::BuildTuple(n, _span) => {
                    let count = n.unwrap_or(0);
                    Self::stack_pop_n(&mut state.stack, count, "build tuple")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::UnpackList(count, _span) => {
                    let _ = Self::stack_pop(&mut state.stack, "unpack list")?;
                    for _ in 0..*count {
                        state.stack.push(DepValue::Unknown);
                    }
                }
                Instruction::UnpackLists(_count, _span) => {
                    // TODO: keep it conservative; treat as unknown stack effect
                }
                Instruction::Add(_)
                | Instruction::Sub(_)
                | Instruction::Mul(_)
                | Instruction::Div(_)
                | Instruction::IntDiv(_)
                | Instruction::Rem(_)
                | Instruction::Pow(_)
                | Instruction::Eq(_)
                | Instruction::Ne(_)
                | Instruction::Gt(_)
                | Instruction::Gte(_)
                | Instruction::Lt(_)
                | Instruction::Lte(_)
                | Instruction::In(_)
                | Instruction::StringConcat(_) => {
                    let _ = Self::stack_pop(&mut state.stack, "binary op rhs")?;
                    let _ = Self::stack_pop(&mut state.stack, "binary op lhs")?;
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::Neg(_)
                | Instruction::Pos(_)
                | Instruction::Not(_)
                | Instruction::DupTop
                | Instruction::DiscardTop => {
                    // handle individually below
                    match inst {
                        Instruction::Neg(_) | Instruction::Pos(_) | Instruction::Not(_) => {
                            let _ = Self::stack_pop(&mut state.stack, "unary op")?;
                            state.stack.push(DepValue::Unknown);
                        }
                        Instruction::DupTop => {
                            let v = state.stack.last().cloned().unwrap_or(DepValue::Unknown);
                            state.stack.push(v);
                        }
                        Instruction::DiscardTop => {
                            let _ = Self::stack_pop(&mut state.stack, "discard top")?;
                        }
                        _ => {}
                    }
                }
                Instruction::PushLoop(_flags, _span) => {
                    let _ = Self::stack_pop(&mut state.stack, "push loop iterable")?;
                }
                Instruction::PushWith(_span) => {}
                Instruction::Iterate(_jump_target, _span) => {
                    // the typechecker pushes element type; we push unknown placeholder
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::PushDidNotIterate => {
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::PopFrame => {}
                Instruction::Jump(_, _) => {}
                Instruction::JumpIfFalse(_, _) | Instruction::JumpIfTrue(_, _) => {
                    let _ = Self::stack_pop(&mut state.stack, "jump condition")?;
                }
                Instruction::JumpIfFalseOrPop(_, _span)
                | Instruction::JumpIfTrueOrPop(_, _span) => {
                    // the typechecker peeks; we do nothing
                }
                Instruction::PushAutoEscape(_span) => {
                    let _ = Self::stack_pop(&mut state.stack, "push auto escape")?;
                }
                Instruction::PopAutoEscape => {}
                Instruction::BeginCapture(_mode) => {}
                Instruction::EndCapture => {
                    // capture pushes a string at runtime; be conservative
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::ApplyFilter(_name, arg_count, _local_id, _span)
                | Instruction::PerformTest(_name, arg_count, _local_id, _span) => {
                    let count = arg_count.unwrap_or(0) as usize;
                    if count > 0 {
                        Self::stack_pop_n(&mut state.stack, count, "filter/test args")?;
                    }
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::CallFunction(
                    name,
                    arg_count,
                    identifier_span,
                    _span,
                    ref_or_source_span,
                ) => {
                    let count = arg_count.unwrap_or(0) as usize;
                    let mut args = Vec::with_capacity(count);
                    for _ in 0..count {
                        args.push(Self::stack_pop(&mut state.stack, "call function args")?);
                    }
                    args.reverse();

                    // Emit model reference/source reference events (used by LSP semantic tokens).
                    // We only support common literal-string arguments here; dynamic cases are skipped.
                    if *name == "ref" || *name == "source" {
                        if let Some(ref_or_source_span) = ref_or_source_span.as_deref() {
                            if *name == "ref" {
                                if let Some(DepValue::StringLiteral(model_name)) = args.first() {
                                    listener.on_model_reference(
                                        model_name,
                                        identifier_span,
                                        &ref_or_source_span.start_line,
                                        &ref_or_source_span.start_col,
                                        &ref_or_source_span.start_offset,
                                        &ref_or_source_span.end_line,
                                        &ref_or_source_span.end_col,
                                        &ref_or_source_span.end_offset,
                                    );
                                }
                            } else if let Some(DepValue::StringLiteral(source_name)) = args
                                .iter()
                                .rev()
                                .find(|v| matches!(v, DepValue::StringLiteral(_)))
                            {
                                listener.on_model_source_reference(
                                    source_name,
                                    identifier_span,
                                    &ref_or_source_span.start_line,
                                    &ref_or_source_span.start_col,
                                    &ref_or_source_span.start_offset,
                                    &ref_or_source_span.end_line,
                                    &ref_or_source_span.end_col,
                                    &ref_or_source_span.end_offset,
                                );
                            }
                        }

                        state.stack.push(DepValue::Unknown);
                        continue;
                    }

                    // Resolve and emit dependency edge if this targets a registered macro/function.
                    if let Some(funcsign) = self.function_registry.get(*name) {
                        if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                            funcsign.get_span(),
                            funcsign.get_path(),
                            funcsign.get_unique_id(),
                        ) {
                            listener.on_function_call(
                                identifier_span,
                                &def_span,
                                &def_path,
                                &def_unique_id,
                            );
                        }
                    } else if let Some(template_name) = macro_namespace_template_resolver(
                        &typecheck_resolved_context,
                        self.function_registry.clone(),
                        name,
                        attempts,
                    ) {
                        if let Some(funcsign) = self.function_registry.get(&template_name) {
                            if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                                funcsign.get_span(),
                                funcsign.get_path(),
                                funcsign.get_unique_id(),
                            ) {
                                listener.on_function_call(
                                    identifier_span,
                                    &def_span,
                                    &def_path,
                                    &def_unique_id,
                                );
                            }
                        }
                    }

                    state.stack.push(DepValue::Unknown);
                }
                Instruction::CallMethod(name, arg_count, identifier_span, _span) => {
                    let count = arg_count.unwrap_or(0) as usize;
                    if count == 0 {
                        return Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            "The first argument(self) of method call is missing",
                        ));
                    }

                    // Pop (arg_count - 1) method args
                    if count > 1 {
                        Self::stack_pop_n(&mut state.stack, count - 1, "call method args")?;
                    }
                    // Pop receiver
                    let recv = Self::stack_pop(&mut state.stack, "call method receiver")?;

                    if let DepValue::Namespace(ns) = recv {
                        let qualified_name = format!("{ns}.{name}");
                        if let Some(funcsign) = self.function_registry.get(&qualified_name) {
                            if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                                funcsign.get_span(),
                                funcsign.get_path(),
                                funcsign.get_unique_id(),
                            ) {
                                listener.on_function_call(
                                    identifier_span,
                                    &def_span,
                                    &def_path,
                                    &def_unique_id,
                                );
                            }
                        }
                    }

                    state.stack.push(DepValue::Unknown);
                }
                Instruction::CallObject(arg_count, _span) => {
                    let count = arg_count.unwrap_or(0) as usize;
                    if count > 0 {
                        // Pop (arg_count - 1) args, then pop the callable
                        if count > 1 {
                            Self::stack_pop_n(&mut state.stack, count - 1, "call object args")?;
                        }
                        let _ = Self::stack_pop(&mut state.stack, "call object receiver")?;
                    }
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::FastSuper(_) | Instruction::FastRecurse(_) => {
                    state.stack.push(DepValue::Unknown);
                }
                Instruction::Return { .. } => {
                    // Return consumes a stack top at runtime; keep conservative.
                    if !state.stack.is_empty() {
                        let _ = state.stack.pop();
                    }
                }
                // For all other instructions, conservatively do nothing. If we ever rely on stack
                // balance for new bytecode patterns, we can add specific handling.
                _ => {}
            }
        }

        Ok(state)
    }
}

#[derive(Clone, Debug)]
pub struct TypeWithConstraint {
    pub inner: Type,
    pub constraint: BTreeMap<Part, TypeWithConstraint>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Part {
    String(String),
    Subscript(String),
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
    pub fn get_attribute(
        &self,
        name: &str,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<TypeWithConstraint, crate::Error> {
        if let Some(constraint) = self.constraint.get(&Part::String(name.to_string())) {
            Ok(constraint.clone())
        } else {
            self.inner
                .get_attribute(name, listener)
                .map(TypeWithConstraint::from)
        }
    }

    pub fn subscript(
        &self,
        index: &TypeWithConstraint,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<TypeWithConstraint, crate::Error> {
        if let Type::String(Some(idx)) = &index.inner {
            if let Some(constraint) = self.constraint.get(&Part::Subscript(idx.clone())) {
                Ok(constraint.clone())
            } else {
                self.inner
                    .subscript(&index.inner, listener)
                    .map(TypeWithConstraint::from)
            }
        } else {
            self.inner
                .subscript(&index.inner, listener)
                .map(TypeWithConstraint::from)
        }
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
        registry: Arc<DashMap<String, Type>>,
    ) -> Option<TypeWithConstraint> {
        self.inner
            .can_binary_op_with(&other.inner, op, registry)
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

    pub fn is_namespace(&self) -> bool {
        self.inner.is_namespace()
    }

    pub fn call(
        &self,
        positional_args: &[Type],
        kwargs: &BTreeMap<String, Type>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        self.inner.call(positional_args, kwargs, listener)
    }

    pub fn is_optional(&self) -> bool {
        self.inner.is_optional()
    }

    pub fn is_none(&self) -> bool {
        self.inner.is_none()
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
    pub fn insert(
        &mut self,
        path: &[Part],
        type_: Type,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<(), crate::Error> {
        if let Some((item, rest)) = path.split_first() {
            if let Some(attribute_type) = self.constraint.get_mut(item) {
                attribute_type.insert(rest, type_, listener)?;
            } else {
                let mut attribute_type = match item {
                    Part::String(s) => self.get_attribute(s, listener.clone())?,
                    Part::Subscript(s) => {
                        let idx_type = TypeWithConstraint::from(Type::String(Some(s.clone())));
                        self.subscript(&idx_type, listener.clone())?
                    }
                };
                attribute_type.insert(rest, type_, listener)?;
                self.constraint.insert(item.clone(), attribute_type);
            }
        } else {
            self.inner = type_;
        }
        Ok(())
    }

    pub fn get_simple_name(&self) -> String {
        match &self.inner {
            Type::String(_) => "String".to_string(),
            Type::Integer(_) => "Integer".to_string(),
            Type::Float => "Float".to_string(),
            Type::Bool => "Bool".to_string(),
            Type::Bytes => "Bytes".to_string(),
            Type::TimeStamp => "TimeStamp".to_string(),
            Type::Tuple(_) => "Tuple".to_string(),
            Type::List(_) => "List".to_string(),
            Type::Struct(_) => "Struct".to_string(),
            Type::Iterable(_) => "Iterable".to_string(),
            Type::Dict(_) => "Dict".to_string(),
            Type::Plain => "Plain".to_string(),
            Type::None => "None".to_string(),
            Type::Undefined => "Undefined".to_string(),
            Type::Invalid => "Invalid".to_string(),
            Type::Exception => "Exception".to_string(),
            Type::Union(_) => "Union".to_string(),
            Type::Any { .. } => "Any".to_string(),
            Type::Kwargs(_) => "Kwargs".to_string(),
            Type::Frame => "Frame".to_string(),
            Type::Object(arg0) => {
                if arg0.downcast_ref::<LambdaType>().is_some() {
                    "Lambda".to_string()
                } else {
                    format!("{arg0:?}")
                }
            }
            Type::Column => "Column".to_string(),
            Type::Namespace(_) => "Namespace".to_string(),
        }
    }
}

/// symbol table mapping local variable names to their types
#[derive(Clone, Debug, Default)]
pub struct SymbolTable {
    pub builtins: Arc<DashMap<String, Type>>,
    pub locals: BTreeMap<String, TypeWithConstraint>,
    pub locals_definitions_location: BTreeMap<String, Vec<Span>>,
}

impl SymbolTable {
    pub fn new(builtins: Arc<DashMap<String, Type>>) -> Self {
        Self {
            builtins,
            locals: BTreeMap::new(),
            locals_definitions_location: BTreeMap::new(),
        }
    }

    pub fn get(
        &self,
        variable: impl Into<Variable>,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<TypeWithConstraint, crate::Error> {
        let variable = variable.into();
        match variable {
            Variable::String(name) => self
                .locals
                .get(&name)
                .cloned()
                .or_else(|| {
                    self.builtins
                        .get(&name)
                        .map(|type_| type_.value().clone().into())
                })
                .ok_or_else(|| {
                    crate::Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Variable not found: {name}"),
                    )
                }),
            Variable::GetAttr(path) => {
                // The first element must be Part::String
                let base_name = match &path[0] {
                    Part::String(s) => s,
                    _ => {
                        return Err(crate::Error::new(
                            ErrorKind::InvalidOperation,
                            format!("Base variable must be a string: {:?}", path[0]),
                        ));
                    }
                };
                let mut type_ = self
                    .locals
                    .get(base_name)
                    .cloned()
                    .or_else(|| {
                        self.builtins
                            .get(base_name)
                            .map(|type_| type_.value().clone().into())
                    })
                    .ok_or_else(|| {
                        crate::Error::new(
                            ErrorKind::InvalidOperation,
                            format!("Variable not found: {base_name}"),
                        )
                    })?;
                for part in path.iter().skip(1) {
                    match part {
                        Part::String(attr) => {
                            type_ = type_.get_attribute(attr, listener.clone())?;
                        }
                        Part::Subscript(idx) => {
                            let idx_type =
                                TypeWithConstraint::from(Type::String(Some(idx.clone())));
                            type_ = type_.subscript(&idx_type, listener.clone())?;
                        }
                    }
                }
                Ok(type_)
            }
        }
    }

    pub fn insert(
        &mut self,
        variable: impl Into<Variable>,
        value: Type,
        listener: Rc<dyn TypecheckingEventListener>,
        span_location: Option<Span>,
    ) -> Result<(), crate::Error> {
        let variable = variable.into();
        match variable {
            Variable::String(name) => {
                self.locals.insert(name.clone(), value.into());
                if let Some(span_location) = span_location {
                    self.locals_definitions_location
                        .insert(name, vec![span_location]);
                }
                Ok(())
            }
            Variable::GetAttr(path) => {
                let type_ = match self.locals.get_mut(match &path[0] {
                    Part::String(s) => s,
                    _ => unreachable!(),
                }) {
                    Some(type_) => type_,
                    None => if let Some(type_) = self.builtins.get(match &path[0] {
                        Part::String(s) => s,
                        _ => unreachable!(),
                    }) {
                        self.locals.insert(
                            match &path[0] {
                                Part::String(s) => s.clone(),
                                _ => unreachable!(),
                            },
                            type_.value().clone().into(),
                        );
                        if let Some(span_location) = span_location {
                            self.locals_definitions_location.insert(
                                match &path[0] {
                                    Part::String(s) => s.clone(),
                                    _ => unreachable!(),
                                },
                                vec![span_location],
                            );
                        }
                        self.locals.get_mut(&match &path[0] {
                            Part::String(s) => s.clone(),
                            _ => unreachable!(),
                        })
                    } else {
                        None
                    }
                    .ok_or_else(|| {
                        crate::Error::new(
                            ErrorKind::InvalidOperation,
                            format!("Variable not found: {:?}", path[0]),
                        )
                    })?,
                };
                type_.insert(&path[1..], value, listener)?;
                Ok(())
            }
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.locals.keys()
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut TypeWithConstraint> {
        self.locals.get_mut(name)
    }

    pub fn get_ref(&self, name: &str) -> Option<&TypeWithConstraint> {
        self.locals.get(name)
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

    pub fn drain<R>(&mut self, range: R) -> std::vec::Drain<'_, TypeWithConstraint>
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
    pub rv_type: Type,
    pub return_span: Span,
}

impl TypecheckState {
    pub fn new(builtins: Arc<DashMap<String, Type>>) -> Self {
        TypecheckState {
            stack: TypecheckStack::default(),
            locals: SymbolTable::new(builtins),
            frame_base: 0,
            cur_loop_obj_type: None,
            single_branch_definition_vars: BTreeSet::new(),
            rv_type: Type::None,
            return_span: Span::default(),
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

/// CFG-based type checker
pub struct TypeChecker<'src> {
    pub instr: &'src [Instruction<'src>], // TODO: put instr and &function_registry into in_states
    pub cfg: CFG,
    pub in_states: Vec<TypecheckState>,
    pub function_registry: Arc<FunctionRegistry>,
    pub builtins: Arc<DashMap<String, Type>>,
}

/// Typecheck logic implementation
impl<'src> TypeChecker<'src> {
    pub fn new(
        instr: &'src [Instruction<'src>],
        cfg: CFG,
        funcsigns: Arc<FunctionRegistry>,
        builtins: Arc<DashMap<String, Type>>,
    ) -> Self {
        let in_states = vec![TypecheckState::new(builtins.clone()); cfg.blocks.len()];
        Self {
            instr,
            cfg,
            in_states,
            function_registry: funcsigns,
            builtins,
        }
    }

    pub fn check(
        &mut self,
        listener: Rc<dyn TypecheckingEventListener>,
        typecheck_resolved_context: BTreeMap<String, Value>,
    ) -> Result<(), crate::Error> {
        // println!("{}", self.cfg.dump_blocks(self.instr));
        // println!("{}", self.cfg.to_dot());
        let mut worklist = VecDeque::new();
        let mut visited = vec![false; self.cfg.blocks.len()];
        let mut first_merge = vec![true; self.cfg.blocks.len()];

        // Find all roots (blocks with no predecessors)
        for (i, block) in self.cfg.blocks.iter().enumerate() {
            // dbg!(i, block);
            if block.predecessor.is_empty() {
                self.in_states[i] = TypecheckState::new(self.builtins.clone());
                worklist.push_back(i);
                visited[i] = true;
                first_merge[i] = false;
            }
        }

        while let Some(bb_id) = worklist.pop_front() {
            // dbg!(bb_id);
            listener.clone().new_block(bb_id);
            let out_state =
                self.transfer_block(bb_id, listener.clone(), typecheck_resolved_context.clone())?;

            let rv_type = out_state.rv_type.clone();
            if let Some(macro_block) = self.cfg.get_block(bb_id) {
                if let Some(macro_name) = macro_block.current_macro.as_ref() {
                    if let Some(funcsign) = self.function_registry.get(macro_name) {
                        if let Some(user_defined_func) =
                            funcsign.downcast_ref::<UserDefinedFunctionType>()
                        {
                            let expected_ret_type = user_defined_func.ret_type.clone();
                            // try match rv with registry_ret_type
                            let span = out_state.return_span;
                            if !rv_type.is_subtype_of(&expected_ret_type) {
                                listener.set_span(&span);
                                listener.warn(
                                    &format!(
                                        "Type mismatch: expected return type {expected_ret_type}, got {rv_type}"
                                    ),
                                );
                            }
                        }
                    }
                }
            }

            for (succ, _) in self.cfg.successor(bb_id) {
                let changed = if first_merge[*succ] {
                    self.in_states[*succ] = out_state.clone();
                    first_merge[*succ] = false;
                    true
                } else {
                    Self::merge_into(
                        &mut self.in_states[*succ],
                        &out_state,
                        visited[*succ],
                        listener.clone(),
                    )
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
                                    listener.set_span(&macro_block.span.unwrap_or_default());
                                    listener.warn(
                                        &format!("Type mismatch: expected return type {expected_ret_type}, got String"),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The internal function typechecking a single block.
    #[allow(clippy::too_many_arguments)]
    fn transfer_block(
        &mut self,
        bb_id: usize,
        listener: Rc<dyn TypecheckingEventListener>,
        typecheck_resolved_context: BTreeMap<String, Value>,
    ) -> Result<TypecheckState, crate::Error> {
        let suppressed_listener = Rc::new(DefaultTypecheckingEventListener::default());
        let mut typestate = self.in_states[bb_id].clone();
        let slice = self.cfg.instructions(bb_id, self.instr);
        let attempts: &mut Vec<std::string::String> = &mut Vec::new();

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
                Instruction::EmitRaw(_, _) => {
                    // TYPECHECK: NO
                    // no need to update the type stack
                }
                Instruction::Emit(_) => {
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
                    listener.set_span(span);
                    let value_type = match typestate.stack.pop() {
                        Some(val) => {
                            if *name != "_internal_tmp"
                                && macro_namespace_template_resolver(
                                    &typecheck_resolved_context,
                                    self.function_registry.clone(),
                                    name,
                                    attempts,
                                )
                                .is_none()
                            {
                                listener.on_lookup(
                                    span,
                                    &val.get_simple_name(),
                                    &format!("{val}"),
                                    vec![*span],
                                );
                            }
                            val.inner
                        }
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                format!("Stack underflow on store local {name} {span:?}"),
                            ));
                        }
                    };
                    let block = self.cfg.get_block(bb_id).unwrap();
                    let span_location = if let Some(macro_name) = &block.current_macro {
                        if *name == macro_name {
                            None
                        } else {
                            Some(*span)
                        }
                    } else {
                        None
                    };
                    typestate.locals.insert(
                        (*name).to_string(),
                        value_type.clone(),
                        listener.clone(),
                        span_location,
                    )?;
                }
                Instruction::Lookup(name, span) => {
                    // TYPECHECK: NO
                    listener.set_span(span);
                    let name_str: &str = name;
                    // first try to search in self.cfg.get_block(bb_id).type_narrow
                    if let Ok(ty) = typestate.locals.get(name_str, listener.clone()) {
                        if typestate.single_branch_definition_vars.contains(name_str) {
                            listener.warn(
                                &format!("Variable '{name_str}' is not defined in one of its predecessor blocks."),
                            );
                            typestate.stack.push(Type::Any { hard: false });
                            if name_str != "_internal_tmp" {
                                // get the spans from locals_definitions_location
                                if let Some(spans) =
                                    typestate.locals.locals_definitions_location.get(name_str)
                                {
                                    listener.on_lookup(span, "any", "any", spans.clone());
                                }
                            }
                        } else {
                            typestate.stack.push(ty.clone());
                            if name_str != "_internal_tmp" {
                                if let Some(spans) =
                                    typestate.locals.locals_definitions_location.get(name_str)
                                {
                                    listener.on_lookup(
                                        span,
                                        &ty.get_simple_name(),
                                        &format!("{ty}"),
                                        spans.clone(),
                                    );
                                }
                            }
                        }
                    } else if let Some(function) = self.function_registry.get(name_str) {
                        typestate.stack.push(Type::Object(function.clone()));
                    } else {
                        listener.warn(&format!(
                            "Potential TypeError: Unknown local variable '{name_str}'"
                        ));
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }
                Instruction::GetAttr(name, span) => {
                    // TYPECHECK: YES
                    listener.set_span(span);
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
                    typestate
                        .stack
                        .push(value_type.get_attribute(name, listener.clone())?);
                }

                Instruction::SetAttr(_name, _span) => {
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
                    listener.set_span(span);
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
                    typestate
                        .stack
                        .push(base.subscript(&index, listener.clone())?);
                }
                Instruction::Slice(span) => {
                    // TYPECHECK: YES
                    listener.set_span(span);
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
                            listener.warn(&format!(
                                "Type mismatch for slice {name}: type = {slice_type}"
                            ));
                        }
                    }

                    typestate.stack.push(Type::Any { hard: false });
                }
                Instruction::LoadConst(val) => {
                    // TYPECHECK: NO
                    typestate.stack.push(infer_type_from_const_value(val));
                }
                Instruction::BuildMap(pair_count, span) => {
                    // TYPECHECK: NO
                    listener.set_span(span);
                    let mut args_map_types = vec![];
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
                        args_map_types.push((k, v));
                    }
                    let mut args_map = BTreeMap::new();
                    let mut success = true;
                    for (k, v) in args_map_types {
                        if let TypeWithConstraint {
                            inner: Type::String(Some(k)),
                            ..
                        } = k
                        {
                            args_map.insert(k, v);
                        } else {
                            success = false;
                            break;
                        }
                    }
                    if success {
                        typestate
                            .stack
                            .push(Type::Struct(StructType::new(args_map)));
                    } else {
                        typestate.stack.push(Type::Any { hard: true });
                    }
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
                Instruction::BuildList(n, span) => {
                    listener.set_span(span);

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
                    listener.set_span(span);
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
                        listener.warn(
                            "Type mismatch for build tuple: expected tuple with a fixed number of elements, got None",
                        );
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }
                Instruction::UnpackList(count, span) => {
                    listener.set_span(span);
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
                        Type::List(list_type) => {
                            // get list_type.element
                            let element_type = list_type.element.clone();
                            for _ in 0..*count {
                                typestate.stack.push(*element_type.clone());
                            }
                        }
                        _ => {
                            for _ in 0..*count {
                                typestate.stack.push(Type::Any { hard: false });
                            }
                            listener.warn(&format!(
                                "Type mismatch for unpack list: expected Tuple with {count} elements, got {tuple_type}"
                            ));
                        }
                    };
                }
                Instruction::UnpackLists(_count, _span) => {
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
                    listener.set_span(span);
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

                    let result_type =
                        lhs_type.can_binary_op_with(&rhs_type, op, self.builtins.clone());
                    if let Some(result_type) = result_type {
                        typestate.stack.push(result_type);
                    } else {
                        listener.warn(&format!(
                            "Type mismatch for {op}: lhs = {lhs_type}, rhs = {rhs_type}"
                        ));
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }

                Instruction::Rem(span) => {
                    // TYPECHECK: YES
                    listener.set_span(span);
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

                    let result_type =
                        lhs_type.can_binary_op_with(&rhs_type, op, self.builtins.clone());
                    if let Some(result_type) = result_type {
                        typestate.stack.push(result_type);
                    } else {
                        listener.warn(&format!(
                            "Type mismatch for {op}: lhs = {lhs_type}, rhs = {rhs_type}"
                        ));
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
                    listener.set_span(span);
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
                        listener.warn(&format!(
                            "Type mismatch for {op}: lhs = {lhs_type}, rhs = {rhs_type}"
                        ));
                    }
                    typestate.stack.push(Type::Bool);
                }

                Instruction::Not(span) => {
                    // TYPECHECK: NO
                    listener.set_span(span);
                    let item_type = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on not operation",
                            ))
                        }
                    };
                    if item_type.is_optional() {
                        typestate.stack.push(item_type.exclude(&Type::None));
                    } else {
                        typestate.stack.push(Type::Bool);
                    }
                }
                Instruction::StringConcat(span) => {
                    // TYPECHECK: NO
                    listener.set_span(span);
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
                Instruction::In(span) => {
                    // TYPECHECK: NO
                    listener.set_span(span);
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
                Instruction::Neg(span) => {
                    // TYPECHECK: YES
                    listener.set_span(span);
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
                Instruction::Pos(span) => {
                    // TYPECHECK: YES
                    listener.set_span(span);
                    // The operand must be a number
                    let a = match typestate.stack.pop() {
                        Some(val) => val,
                        None => {
                            return Err(crate::Error::new(
                                crate::error::ErrorKind::InvalidOperation,
                                "Stack underflow on unary plus",
                            ))
                        }
                    };

                    typestate.stack.push(a);
                }
                Instruction::PushWith(_) => {
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
                    listener.set_span(span);
                    if let Some(iterable) = typestate.stack.pop_inner() {
                        let element_type = match iterable {
                            Type::List(list) => *list.element.clone(),
                            Type::Iterable(iterable) => *iterable.element.clone(),
                            Type::Dict(dict) => *dict.key.clone(),
                            Type::Any { hard: true } => Type::Any { hard: true },

                            _ => {
                                let func = iterable.get_attribute("__iter__", listener.clone())?;
                                func.call(&[], &BTreeMap::new(), listener.clone())?
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
                Instruction::Iterate(_jump_target, _span) => {
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
                Instruction::Jump(_jump_target, _) => {
                    // TYPECHECK: NO
                    // have nothing to do with the stack
                }
                Instruction::JumpIfFalse(_else_label, _span) => {
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
                Instruction::JumpIfTrue(_else_label, _) => {
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
                    listener.set_span(span);
                    // the operand must be a boolean
                    let a = typestate.peek().clone();

                    if !a.is_condition() {
                        listener.warn(&format!("Type mismatch for jump condition: type = {a}"));
                    }
                }
                Instruction::JumpIfTrueOrPop(_jump_target, span) => {
                    // TYPECHECK: YES
                    listener.set_span(span);
                    // the operand must be a boolean
                    let a = typestate.peek().clone();

                    if !a.is_condition() {
                        listener.warn(&format!("Type mismatch for jump condition: type = {a}"));
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
                    listener.set_span(span);
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
                        listener.warn(&format!("Type mismatch for auto escape: type = {a}"));
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
                    // TYPECHECK: YES - filter types are registered in function_registry
                    listener.set_span(span);

                    if let Some(arg_cnt) = arg_count {
                        let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                        // Try to call the filter if it's registered in locals
                        if let Ok(Type::Object(funcsign)) = typestate
                            .locals
                            .get(name, listener.clone())
                            .map(|t| t.inner)
                        {
                            let funcsign = funcsign.clone();
                            typestate.stack.push(funcsign.call(
                                &args,
                                &kwargs,
                                listener.clone(),
                            )?);
                        } else if let Some(funcsign) = self.function_registry.get(name.to_owned()) {
                            // Try function_registry as fallback (for built-in filters like as_bool, as_number, etc.)
                            typestate.stack.push(funcsign.call(
                                &args,
                                &kwargs,
                                listener.clone(),
                            )?);
                        } else {
                            // Filter not registered - warn and push Any type
                            listener.warn(&format!(
                                "Potential TypeError: Filter '{name}' is not defined."
                            ));
                            typestate.stack.push(Type::Any { hard: false });
                        }
                    } else {
                        // TODO: handle the case when arg_count is None
                        listener.warn(&format!(
                            "Potential TypeError: Filter '{name}' requires arguments."
                        ));
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }
                Instruction::PerformTest(_name, arg_count, _local_id, _span) => {
                    // TYPECHECK: NO
                    typestate.drop_top(arg_count.unwrap_or(0) as usize);
                    typestate.stack.push(Type::Bool);
                }
                Instruction::CallFunction(
                    name,
                    arg_count,
                    identifier_span,
                    span,
                    ref_or_source_span,
                ) => {
                    // TYPECHECK: YES
                    listener.set_span(span);

                    if *name == "caller" {
                        // judge whether current block is a macro
                        if let Some(block) = self.cfg.get_block(bb_id) {
                            if let Some(_macro_name) = &block.current_macro {
                                if let Some(arg_cnt) = arg_count {
                                    let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                                    typestate.stack.push(
                                        self.builtins.get("caller").unwrap().call(
                                            &args,
                                            &kwargs,
                                            listener.clone(),
                                        )?,
                                    );
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
                                "source" => self.builtins.get("source").unwrap(),
                                "ref" => self.builtins.get("ref").unwrap(),
                                _ => unreachable!(),
                            };
                            typestate.stack.push(function_type.call(
                                &args,
                                &kwargs,
                                listener.clone(),
                            )?);

                            if *name == "ref" {
                                if let Some(ref_or_source_span) = ref_or_source_span {
                                    if let Type::String(Some(name)) = &args[0] {
                                        listener.on_model_reference(
                                            name,
                                            identifier_span,
                                            &ref_or_source_span.start_line,
                                            &ref_or_source_span.start_col,
                                            &ref_or_source_span.start_offset,
                                            &ref_or_source_span.end_line,
                                            &ref_or_source_span.end_col,
                                            &ref_or_source_span.end_offset,
                                        );
                                    }
                                }
                            } else if *name == "source" {
                                if let Some(ref_or_source_span) = ref_or_source_span {
                                    if let Type::String(Some(name)) = args.last().unwrap() {
                                        listener.on_model_source_reference(
                                            name,
                                            identifier_span,
                                            &ref_or_source_span.start_line,
                                            &ref_or_source_span.start_col,
                                            &ref_or_source_span.start_offset,
                                            &ref_or_source_span.end_line,
                                            &ref_or_source_span.end_col,
                                            &ref_or_source_span.end_offset,
                                        );
                                    }
                                }
                            }
                        }
                    } else if let Ok(Type::Object(funcsign)) = typestate
                        .locals
                        .get(name, listener.clone())
                        .map(|t| t.inner)
                    {
                        if let Some(arg_cnt) = arg_count {
                            let funcsign = funcsign.clone();
                            let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                            typestate.stack.push(funcsign.call(
                                &args,
                                &kwargs,
                                listener.clone(),
                            )?);
                            if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                                funcsign.get_span(),
                                funcsign.get_path(),
                                funcsign.get_unique_id(),
                            ) {
                                listener.on_function_call(
                                    identifier_span,
                                    &def_span,
                                    &def_path,
                                    &def_unique_id,
                                );
                            }
                        }
                    } else if let Ok(Type::Any { hard: true }) = typestate
                        .locals
                        .get(name, listener.clone())
                        .map(|t| t.inner)
                    {
                        typestate.stack.push(Type::Any { hard: true });
                    } else if let Some(funcsign) = self.function_registry.get(name.to_owned()) {
                        if let Some(arg_cnt) = arg_count {
                            let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                            typestate.stack.push(funcsign.call(
                                &args,
                                &kwargs,
                                listener.clone(),
                            )?);
                            if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                                funcsign.get_span(),
                                funcsign.get_path(),
                                funcsign.get_unique_id(),
                            ) {
                                listener.on_function_call(
                                    identifier_span,
                                    &def_span,
                                    &def_path,
                                    &def_unique_id,
                                );
                            }
                        }
                    } else if let Some(template_name) = macro_namespace_template_resolver(
                        &typecheck_resolved_context,
                        self.function_registry.clone(),
                        name,
                        attempts,
                    ) {
                        if let Some(funcsign) = self.function_registry.get(&template_name) {
                            if let Some(arg_cnt) = arg_count {
                                let (args, kwargs) = typestate.get_call_args(*arg_cnt);

                                typestate.stack.push(funcsign.call(
                                    &args,
                                    &kwargs,
                                    listener.clone(),
                                )?);
                                if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                                    funcsign.get_span(),
                                    funcsign.get_path(),
                                    funcsign.get_unique_id(),
                                ) {
                                    listener.on_function_call(
                                        identifier_span,
                                        &def_span,
                                        &def_path,
                                        &def_unique_id,
                                    );
                                }
                            }
                        }
                    } else if let Some(arg_cnt) = arg_count {
                        let _ = typestate.get_call_args(*arg_cnt);
                        // Don't warn for known built-in functions
                        let known_functions = ["doc", "var"];
                        if !known_functions.contains(name) {
                            listener.warn(&format!(
                                "Potential TypeError: Function '{name}' is not defined."
                            ));
                        }
                        typestate.stack.push(Type::Any { hard: false });
                    } else {
                        // TODO: handle the case when arg_count is None
                        // Don't warn for known built-in functions
                        let known_functions = ["doc", "var"];
                        if !known_functions.contains(name) {
                            listener.warn(&format!(
                                "Potential TypeError: Function '{name}' is not defined."
                            ));
                        }
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }
                Instruction::CallMethod(name, arg_count, identifier_span, span) => {
                    // TYPECHECK: NO? (Maybe add method check later)
                    listener.set_span(span);

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

                        if self_type.is_namespace() {
                            let namespace_name = match self_type.inner.clone() {
                                Type::Namespace(name) => name,
                                _ => unreachable!(),
                            };
                            let qualified_name = format!("{namespace_name}.{name}");
                            if let Some(funcsign) = self.function_registry.get(&qualified_name) {
                                typestate.stack.push(funcsign.call(
                                    &method_args,
                                    &kwargs,
                                    listener.clone(),
                                )?);
                                if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                                    funcsign.get_span(),
                                    funcsign.get_path(),
                                    funcsign.get_unique_id(),
                                ) {
                                    listener.on_function_call(
                                        identifier_span,
                                        &def_span,
                                        &def_path,
                                        &def_unique_id,
                                    );
                                }
                            } else {
                                typestate.stack.push(Type::Any { hard: false });
                            }
                            continue;
                        }

                        if self_type.is_any() {
                            typestate.stack.push(self_type);
                            continue;
                        }

                        let function = self_type.get_attribute(name, listener.clone())?;

                        if function.is_any() {
                            typestate.stack.push(function);
                            continue;
                        }

                        let result = match function.call(&method_args, &kwargs, listener.clone()) {
                            Ok(rv) => {
                                if let Type::Object(funcsign) = &rv {
                                    if let (Some(def_span), Some(def_path), Some(def_unique_id)) = (
                                        funcsign.get_span(),
                                        funcsign.get_path(),
                                        funcsign.get_unique_id(),
                                    ) {
                                        listener.on_function_call(
                                            identifier_span,
                                            &def_span,
                                            &def_path,
                                            &def_unique_id,
                                        );
                                    }
                                }
                                rv
                            }
                            Err(e) => {
                                listener
                                    .warn(&format!("Method call failed '{self_type}.{name}': {e}"));
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
                    listener.set_span(span);
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

                        let result = self_type.call(&args, &kwargs, listener.clone())?;

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
                Instruction::FastSuper(_) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::FastRecurse(_) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                #[cfg(feature = "multi_template")]
                Instruction::LoadBlocks(span) => {
                    // TYPECHECK: YES
                    listener.set_span(span);
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
                        listener.warn(&format!("Type mismatch for block name: type = {a}"));
                    }
                    // LoadBlocks does not change the stack, it just loads blocks
                }
                #[cfg(feature = "multi_template")]
                Instruction::Include(_ignore_missing, _span) => {
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
                    listener.set_span(span);
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
                    if let Some(macro_qualified_name) = macro_namespace_template_resolver(
                        &typecheck_resolved_context,
                        self.function_registry.clone(),
                        name,
                        attempts,
                    ) {
                        if let Some(function) = self.function_registry.get(&macro_qualified_name) {
                            typestate.stack.push(Type::Object(function.clone()));
                        } else {
                            listener.warn(&format!(
                                "Macro '{macro_qualified_name}' is not defined in the function registry."
                            ));
                            typestate.stack.push(Type::Any { hard: false });
                        }
                    } else if let Some(function) = self.function_registry.get(*name) {
                        typestate.stack.push(Type::Object(function.clone()));
                    } else if *name == "caller" {
                        typestate
                            .stack
                            .push(self.builtins.get("caller").unwrap().clone());
                    } else {
                        listener.warn(&format!(
                            "Function '{name}' is not defined in the function registry."
                        ));
                        typestate.stack.push(Type::Any { hard: false });
                    }
                }
                #[cfg(feature = "macros")]
                Instruction::Return { explicit } => {
                    // TYPECHECK: NO
                    // do nothing instead of break because we want to cover all instructions
                    if *explicit {
                        // pop the stack as the return value
                        let rv_type = match typestate.stack.pop() {
                            Some(val) => val.inner,
                            None => {
                                return Err(crate::Error::new(
                                    crate::error::ErrorKind::InvalidOperation,
                                    "Stack underflow on return",
                                ))
                            }
                        };
                        typestate.rv_type = rv_type;
                    }
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
                Instruction::MacroStart(_line, _col, _index) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::MacroStop(_line, _col, _index) => {
                    // TYPECHECK: NO
                    // Nothing to do with the stack
                }
                Instruction::MacroName(_name, span) => {
                    // TYPECHECK: NO
                    listener.set_span(span);
                }
                Instruction::TypeConstraint(type_constraint, _true_branch, span) => {
                    listener.set_span(span);
                    let name = &type_constraint.name;
                    match &type_constraint.operation {
                        TypeConstraintOperation::NotNull(is_true) => {
                            if *is_true {
                                if let Ok(type_) =
                                    typestate.locals.get(name, suppressed_listener.clone())
                                {
                                    if type_.is_optional() {
                                        let non_optional_type = type_.get_non_optional_type();
                                        typestate.locals.insert(
                                            name,
                                            non_optional_type,
                                            listener.clone(),
                                            None,
                                        )?;
                                    } else if type_.is_none() {
                                        typestate.locals.insert(
                                            name,
                                            Type::Any { hard: true },
                                            listener.clone(),
                                            None,
                                        )?;
                                    }
                                }
                            } else if let Ok(type_) =
                                typestate.locals.get(name, suppressed_listener.clone())
                            {
                                if type_.is_optional() {
                                    typestate.locals.insert(
                                        name,
                                        Type::None,
                                        listener.clone(),
                                        None,
                                    )?;
                                }
                            }
                        }
                        TypeConstraintOperation::Is(test_name, is_true) => {
                            let test_type = if test_name == "true" || test_name == "false" {
                                Type::Bool
                            } else {
                                Type::from_str(test_name)?
                            };
                            if !is_true {
                                if let Ok(type_) =
                                    typestate.locals.get(name, suppressed_listener.clone())
                                {
                                    typestate.locals.insert(
                                        name,
                                        type_.exclude(&test_type),
                                        listener.clone(),
                                        None,
                                    )?;
                                }
                            } else if let Ok(_type_) =
                                typestate.locals.get(name, suppressed_listener.clone())
                            {
                                typestate
                                    .locals
                                    .insert(name, test_type, listener.clone(), None)?;
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
            // println!(
            //     "After instruction {:?}, locals_temp_relation: {:?}",
            //     inst,
            //     typestate
            //         .locals
            //         .get("temp_relation", suppressed_listener.clone())
            // );
        }
        Ok(typestate)
    }

    /// Merges the source typecheck state into the destination state at the merge point.
    fn merge_into(
        dst: &mut TypecheckState,
        src: &TypecheckState,
        visited: bool,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> bool {
        let mut changed = false;

        let min_len = dst.stack.len().min(src.stack.len());
        dst.stack.truncate(min_len);

        if dst.cur_loop_obj_type != src.cur_loop_obj_type {
            dst.cur_loop_obj_type = match (&dst.cur_loop_obj_type, &src.cur_loop_obj_type) {
                (Some(a), Some(b)) => {
                    if a.is_subtype_of(b) {
                        Some(b.clone())
                    } else if b.is_subtype_of(a) {
                        Some(a.clone())
                    } else {
                        Some(Type::Any { hard: false })
                    }
                }
                (None, Some(t)) => Some(t.clone()),
                (Some(t), None) => Some(t.clone()),
                (None, None) => None,
            };
            changed = false;
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
            dst.locals
                .locals_definitions_location
                .entry(name.clone())
                .or_default()
                .extend(
                    src.locals
                        .locals_definitions_location
                        .get(&name)
                        .cloned()
                        .unwrap_or_default(),
                );
            // remove duplicated locations
            dst.locals
                .locals_definitions_location
                .get_mut(&name)
                .unwrap()
                .sort_unstable();
            dst.locals
                .locals_definitions_location
                .get_mut(&name)
                .unwrap()
                .dedup();
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
                        .insert(
                            name.clone(),
                            Type::Any { hard: true },
                            listener.clone(),
                            None,
                        )
                        .unwrap();
                    changed = true;
                }
                (None, None) => {}
            }
        }

        changed
    }
}

pub fn macro_namespace_template_resolver(
    typecheck_resolved_context: &BTreeMap<String, Value>,
    function_registry: Arc<BTreeMap<String, DynObject>>,
    search_name: &str,
    attempts: &mut Vec<String>,
) -> Option<String> {
    // Get necessary values from state
    let current_package_name = typecheck_resolved_context
        .get(TARGET_PACKAGE_NAME)
        .cloned()
        .unwrap_or_else(|| Value::from("dbt"));
    let current_package_name = current_package_name.as_str().unwrap();
    let root_package = typecheck_resolved_context
        .get(ROOT_PACKAGE_NAME)
        .cloned()
        .unwrap_or_else(|| Value::from("dbt"));
    let root_package = root_package.as_str().unwrap();
    let dbt_and_adapters = typecheck_resolved_context
        .get(DBT_AND_ADAPTERS_NAMESPACE)
        .cloned()
        .unwrap_or_default();
    let dbt_and_adapters = dbt_and_adapters
        .as_object()
        .unwrap()
        .downcast_ref::<ValueMap>()
        .unwrap();

    // 1. Local namespace (current package)
    let template_name = format!("{current_package_name}.{search_name}");
    attempts.push(template_name.clone());
    if function_registry.contains_key(&template_name) {
        return Some(template_name);
    }

    // 2. Root package namespace
    let template_name = format!("{root_package}.{search_name}");
    attempts.push(template_name.clone());
    if function_registry.contains_key(&template_name) {
        return Some(template_name);
    }

    // 3. Internal packages
    let search_name_value = Value::from(search_name);
    if let Some(pkg) = dbt_and_adapters.get(&search_name_value) {
        let template_name = format!("{pkg}.{search_name}");
        attempts.push(template_name.clone());
        if function_registry.contains_key(&template_name) {
            return Some(template_name);
        }
    }

    // No template found
    None
}

#[cfg(test)]
mod dependency_analyzer_tests {
    use super::{DependencyAnalyzer, TypeChecker};
    use crate::compiler::cfg::build_cfg;
    use crate::compiler::typecheck::FunctionRegistry;
    use crate::constants::{DBT_AND_ADAPTERS_NAMESPACE, ROOT_PACKAGE_NAME, TARGET_PACKAGE_NAME};
    use crate::environment::Environment;
    use crate::machinery::Span;
    use crate::types::function::UserDefinedFunctionType;
    use crate::types::{DynObject, Type};
    use crate::value::{Value, ValueMap};
    use crate::vm::listeners::TypecheckingEventListener;
    use dashmap::DashMap;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::Arc;

    #[derive(Default)]
    struct CaptureCallsListener {
        calls: RefCell<BTreeSet<String>>,
    }

    impl TypecheckingEventListener for CaptureCallsListener {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn warn(&self, _message: &str) {}

        fn set_span(&self, _span: &Span) {}

        fn new_block(&self, _block_id: usize) {}

        fn flush(&self) {}

        fn on_lookup(
            &self,
            _span: &Span,
            _simple_name: &str,
            _full_name: &str,
            _def_spans: Vec<Span>,
        ) {
        }

        fn on_function_call(
            &self,
            _source_span: &Span,
            _def_span: &Span,
            _def_path: &std::path::Path,
            def_unique_id: &str,
        ) {
            self.calls.borrow_mut().insert(def_unique_id.to_string());
        }
    }

    #[test]
    fn dependency_analyzer_matches_typechecker_on_calls() {
        // A template that triggers both CallFunction and CallMethod bytecode instructions:
        // - `baz()` should resolve directly by name.
        // - `foo.bar()` should resolve as a namespace call to `my_ns.bar`.
        let src = "{{ foo.bar() }} {{ baz() }}";

        let mut env = Environment::new();
        env.add_template("dep_test", src).unwrap();
        let tmpl = env.get_template("dep_test").unwrap();

        let instructions = &tmpl.compiled.instructions.instructions;
        let cfg = build_cfg(instructions);

        // Builtins: provide `foo` as a namespace so CallMethod can resolve.
        let builtins: Arc<DashMap<String, Type>> = Arc::new(DashMap::new());
        builtins.insert("foo".to_string(), Type::Namespace("my_ns".to_string()));

        // Function registry: provide the direct function and the namespaced method target.
        let span = Span::default();
        let path = PathBuf::from("macros/tests.sql");
        let mut registry: FunctionRegistry = BTreeMap::new();
        registry.insert(
            "baz".to_string(),
            DynObject::new(Arc::new(UserDefinedFunctionType::new(
                "baz",
                vec![],
                Type::Any { hard: false },
                path.as_path(),
                &span,
                "uid_baz",
            ))),
        );
        registry.insert(
            "my_ns.bar".to_string(),
            DynObject::new(Arc::new(UserDefinedFunctionType::new(
                "my_ns.bar",
                vec![],
                Type::Any { hard: false },
                path.as_path(),
                &span,
                "uid_bar",
            ))),
        );
        let registry = Arc::new(registry);

        // Minimal resolved context required by macro namespace resolver paths.
        // In particular, DBT_AND_ADAPTERS_NAMESPACE must be an object ValueMap.
        let mut resolved_context: BTreeMap<String, Value> = BTreeMap::new();
        resolved_context.insert(ROOT_PACKAGE_NAME.to_string(), Value::from("root"));
        resolved_context.insert(TARGET_PACKAGE_NAME.to_string(), Value::from("pkg"));
        resolved_context.insert(
            DBT_AND_ADAPTERS_NAMESPACE.to_string(),
            Value::from_object(ValueMap::new()),
        );

        // Run typechecker and capture dependencies.
        let typechecker_listener = Rc::new(CaptureCallsListener::default());
        let mut typechecker = TypeChecker::new(
            instructions,
            cfg.clone(),
            registry.clone(),
            builtins.clone(),
        );
        typechecker
            .check(typechecker_listener.clone(), resolved_context.clone())
            .unwrap();
        let typechecker_calls = typechecker_listener.calls.borrow().clone();

        // Run dependency analyzer and capture dependencies.
        let analyzer_listener = Rc::new(CaptureCallsListener::default());
        let mut analyzer = DependencyAnalyzer::new(instructions, cfg, registry, builtins);
        analyzer
            .check(analyzer_listener.clone(), resolved_context)
            .unwrap();
        let analyzer_calls = analyzer_listener.calls.borrow().clone();

        // They must match and be correct.
        let expected: BTreeSet<String> = ["uid_bar".to_string(), "uid_baz".to_string()]
            .into_iter()
            .collect();
        assert_eq!(typechecker_calls, expected);
        assert_eq!(analyzer_calls, expected);
    }

    #[derive(Default)]
    struct CaptureRefSourceListener {
        refs: RefCell<BTreeSet<String>>,
        sources: RefCell<BTreeSet<String>>,
    }

    impl TypecheckingEventListener for CaptureRefSourceListener {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn warn(&self, _message: &str) {}
        fn set_span(&self, _span: &Span) {}
        fn new_block(&self, _block_id: usize) {}
        fn flush(&self) {}

        fn on_lookup(
            &self,
            _span: &Span,
            _simple_name: &str,
            _full_name: &str,
            _def_spans: Vec<Span>,
        ) {
        }

        fn on_model_reference(
            &self,
            name: &str,
            _identifier_span: &Span,
            _start_line: &u32,
            _start_col: &u32,
            _start_offset: &u32,
            _end_line: &u32,
            _end_col: &u32,
            _end_offset: &u32,
        ) {
            self.refs.borrow_mut().insert(name.to_string());
        }

        fn on_model_source_reference(
            &self,
            name: &str,
            _identifier_span: &Span,
            _start_line: &u32,
            _start_col: &u32,
            _start_offset: &u32,
            _end_line: &u32,
            _end_col: &u32,
            _end_offset: &u32,
        ) {
            self.sources.borrow_mut().insert(name.to_string());
        }
    }

    #[test]
    fn dependency_analyzer_matches_typechecker_on_ref_and_source() {
        let src = "{{ ref('A') }} {{ source('pkg', 'T') }}";

        let mut env = Environment::new();
        env.add_template("dep_ref_source", src).unwrap();
        let tmpl = env.get_template("dep_ref_source").unwrap();

        let instructions = &tmpl.compiled.instructions.instructions;
        let cfg = build_cfg(instructions);

        // Builtins: we only need `ref`/`source` to be present for TypeChecker's special-case path.
        // Any non-object type is fine; `Type::call` will just warn and return Any.
        let builtins: Arc<DashMap<String, Type>> = Arc::new(DashMap::new());
        builtins.insert("ref".to_string(), Type::Any { hard: false });
        builtins.insert("source".to_string(), Type::Any { hard: false });

        // Empty registry is fine; we are testing ref/source callbacks only.
        let registry: FunctionRegistry = BTreeMap::new();
        let registry = Arc::new(registry);

        let mut resolved_context: BTreeMap<String, Value> = BTreeMap::new();
        resolved_context.insert(ROOT_PACKAGE_NAME.to_string(), Value::from("root"));
        resolved_context.insert(TARGET_PACKAGE_NAME.to_string(), Value::from("pkg"));
        resolved_context.insert(
            DBT_AND_ADAPTERS_NAMESPACE.to_string(),
            Value::from_object(ValueMap::new()),
        );

        let typechecker_listener = Rc::new(CaptureRefSourceListener::default());
        let mut typechecker = TypeChecker::new(
            instructions,
            cfg.clone(),
            registry.clone(),
            builtins.clone(),
        );
        typechecker
            .check(typechecker_listener.clone(), resolved_context.clone())
            .unwrap();

        let analyzer_listener = Rc::new(CaptureRefSourceListener::default());
        let mut analyzer = DependencyAnalyzer::new(instructions, cfg, registry, builtins);
        analyzer
            .check(analyzer_listener.clone(), resolved_context)
            .unwrap();

        let expected_refs: BTreeSet<String> = ["A".to_string()].into_iter().collect();
        let expected_sources: BTreeSet<String> = ["T".to_string()].into_iter().collect();

        assert_eq!(*typechecker_listener.refs.borrow(), expected_refs);
        assert_eq!(*typechecker_listener.sources.borrow(), expected_sources);
        assert_eq!(*analyzer_listener.refs.borrow(), expected_refs);
        assert_eq!(*analyzer_listener.sources.borrow(), expected_sources);
    }
}
