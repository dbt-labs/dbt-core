use std::collections::BTreeSet;

use crate::types::builtin::Type;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnionType {
    pub types: BTreeSet<Type>,
}

impl UnionType {
    pub fn new<I>(types: I) -> Self
    where
        I: IntoIterator<Item = Type>,
    {
        Self {
            types: types.into_iter().collect(),
        }
    }
}

impl UnionType {
    pub fn union(&self, other: &Type) -> Type {
        match other {
            Type::Any { hard: true } => Type::Any { hard: true },
            Type::Union(other_union) => {
                // Merge all types from both unions
                let mut all_types = BTreeSet::new();
                all_types.extend(self.types.iter().cloned());
                all_types.extend(other_union.types.iter().cloned());

                // Remove Any type if present
                all_types.remove(&Type::Any { hard: true });

                // Remove types that are subtypes of other types
                let filtered_types = Self::remove_subtypes(all_types);

                if filtered_types.is_empty() {
                    Type::Any { hard: true }
                } else if filtered_types.len() == 1 {
                    filtered_types.into_iter().next().unwrap()
                } else {
                    Type::Union(UnionType {
                        types: filtered_types,
                    })
                }
            }
            _ => {
                // Merge union with a single type
                let mut all_types = self.types.clone();
                all_types.insert(other.clone());

                // Remove types that are subtypes of other types
                let filtered_types = Self::remove_subtypes(all_types);

                if filtered_types.is_empty() {
                    Type::Any { hard: true }
                } else if filtered_types.len() == 1 {
                    filtered_types.into_iter().next().unwrap()
                } else {
                    Type::Union(UnionType {
                        types: filtered_types,
                    })
                }
            }
        }
    }

    /// Remove types that are subtypes of other types in the set
    fn remove_subtypes(types: BTreeSet<Type>) -> BTreeSet<Type> {
        let mut result = BTreeSet::new();

        for candidate in &types {
            let mut is_subtype_of_another = false;

            for other in &types {
                if candidate != other && candidate.is_subtype_of(other) {
                    is_subtype_of_another = true;
                    break;
                }
            }

            if !is_subtype_of_another {
                result.insert(candidate.clone());
            }
        }

        result
    }
}
