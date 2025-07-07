use std::collections::BTreeSet;

use crate::vm::types::builtin::Type;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnionType {
    pub types: BTreeSet<Type>,
}

impl UnionType {
    pub fn union(&self, other: &Type) -> Type {
        if other.is_any() {
            return Type::Any;
        }
        let mut result = BTreeSet::new();
        for self_type in self.types.iter() {
            for other_type in other.flatten().unwrap() {
                if !self_type.coerce(&other_type).is_none() {
                    result.insert(other_type);
                }
            }
        }
        Type::Union(UnionType { types: result })
    }
}
