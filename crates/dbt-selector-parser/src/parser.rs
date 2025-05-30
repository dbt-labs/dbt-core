//! Convert YAML selectors (as parsed by `dbt-schemas`) into the
//! `SelectExpression` + *optional* `exclude` expression that the
//! scheduler understands.
//

use std::{collections::BTreeMap, str::FromStr};

use dbt_common::{
    err, fs_err,
    node_selector::{
        parse_model_specifiers, IndirectSelection, MethodName, SelectExpression, SelectionCriteria,
    },
    ErrorCode, FsResult,
};

use dbt_schemas::schemas::selectors::{
    AtomExpr, CompositeExpr, CompositeKind, ResolvedSelector, SelectorDefinition,
    SelectorDefinitionValue, SelectorExpr,
};

fn atom_to_select_expression(atom: AtomExpr) -> FsResult<SelectExpression> {
    match atom {
        AtomExpr::Method {
            method,
            value,
            childrens_parents,
            parents,
            children,
            parents_depth,
            children_depth,
            indirect_selection,
            exclude: _,
        } => {
            let (name, args) = {
                let mut parts = method.split('.').map(|s| s.to_string());
                let head = parts.next().unwrap();
                let nm =
                    MethodName::from_str(&head).unwrap_or_else(|_| MethodName::default_for(&value));
                (nm, parts.collect())
            };
            let pd = if parents && parents_depth.is_none() {
                Some(u32::MAX)
            } else {
                parents_depth
            };
            let cd = if children && children_depth.is_none() {
                Some(u32::MAX)
            } else {
                children_depth
            };
            Ok(SelectExpression::Atom(SelectionCriteria::new(
                name,
                args,
                value,
                childrens_parents,
                pd,
                cd,
                indirect_selection,
            )))
        }
        AtomExpr::MethodKey(method_value) => {
            let (m, v) = method_value.into_iter().next().unwrap();
            let (name, args) = {
                let mut parts = m.split('.').map(|s| s.to_string());
                let head = parts.next().unwrap();
                let nm =
                    MethodName::from_str(&head).unwrap_or_else(|_| MethodName::default_for(&v));
                (nm, parts.collect())
            };
            Ok(SelectExpression::Atom(SelectionCriteria::new(
                name,
                args,
                v,
                false,
                None,
                None,
                Some(IndirectSelection::default()),
            )))
        }
        AtomExpr::Exclude { .. } => {
            err!(ErrorCode::SelectorError, "Top level exclude not allowed")
        }
    }
}

#[derive(Debug, Clone)]
pub struct SelectorParser {
    defs: BTreeMap<String, SelectorDefinition>,
}

impl SelectorParser {
    pub fn new(defs: BTreeMap<String, SelectorDefinition>) -> Self {
        Self { defs }
    }

    pub fn parse_named(&self, name: &str) -> FsResult<ResolvedSelector> {
        let def = self
            .defs
            .get(name)
            .ok_or_else(|| fs_err!(ErrorCode::SelectorError, "Unknown selector `{}`", name))?;
        self.parse_definition(&def.definition)
    }

    pub fn parse_definition(&self, def: &SelectorDefinitionValue) -> FsResult<ResolvedSelector> {
        match def {
            SelectorDefinitionValue::String(s) => Ok(ResolvedSelector {
                include: Some(parse_model_specifiers(&[s.clone()])?),
                exclude: None,
            }),
            SelectorDefinitionValue::Full(expr) => self.parse_expr(expr),
        }
    }

    pub fn parse_expr(&self, expr: &SelectorExpr) -> FsResult<ResolvedSelector> {
        match expr {
            SelectorExpr::Composite(comp) => self.parse_composite(comp),
            SelectorExpr::Atom(atom) => self.parse_atom(atom),
        }
    }

    pub fn parse_composite(&self, comp: &CompositeExpr) -> FsResult<ResolvedSelector> {
        let mut includes = Vec::new();
        let mut excludes = Vec::new();

        // Get the values from the CompositeKind directly
        let values = match &comp.kind {
            CompositeKind::Union(vals) => vals,
            CompositeKind::Intersection(vals) => vals,
        };

        for value in values {
            let ResolvedSelector {
                include,
                exclude: sub_ex,
            } = self.parse_definition(value)?;
            if let Some(i) = include {
                includes.push(i);
            }
            if let Some(e) = sub_ex {
                excludes.push(e);
            }
        }

        // Build the boolean operator over includes/excludes
        let include_expr = match &comp.kind {
            CompositeKind::Union(_) => SelectExpression::Or(includes),
            CompositeKind::Intersection(_) => SelectExpression::And(includes),
        };
        let exclude_expr = if excludes.is_empty() {
            None
        } else {
            Some(SelectExpression::Or(excludes))
        };

        Ok(ResolvedSelector {
            include: Some(include_expr),
            exclude: exclude_expr,
        })
    }

    fn parse_atom(&self, atom: &AtomExpr) -> FsResult<ResolvedSelector> {
        match atom {
            AtomExpr::Method {
                method,
                value,
                childrens_parents,
                parents,
                children,
                parents_depth,
                children_depth,
                indirect_selection,
                exclude,
            } => {
                // Build include atom (dropping its nested exclude)
                let include = {
                    let wrapper = AtomExpr::Method {
                        method: method.clone(),
                        value: value.clone(),
                        childrens_parents: *childrens_parents,
                        parents: *parents,
                        children: *children,
                        parents_depth: *parents_depth,
                        children_depth: *children_depth,
                        indirect_selection: *indirect_selection,
                        exclude: None,
                    };
                    atom_to_select_expression(wrapper)?
                };

                // Parse any per-method excludes
                let exclude_expr = exclude
                    .as_ref()
                    .map(|list| {
                        let exprs = self.collect_definition_includes(list)?;
                        Ok::<SelectExpression, Box<dbt_common::FsError>>(SelectExpression::Or(
                            exprs,
                        ))
                    })
                    .transpose()?;

                Ok(ResolvedSelector {
                    include: Some(include),
                    exclude: exclude_expr,
                })
            }

            AtomExpr::MethodKey(method_value) => {
                if method_value.len() != 1 {
                    return Err(fs_err!(
                        ErrorCode::SelectorError,
                        "MethodKey must have exactly one key-value pair"
                    ));
                }
                let (m, v) = method_value.iter().next().unwrap();
                let wrapper = AtomExpr::Method {
                    method: m.clone(),
                    value: v.clone(),
                    childrens_parents: false,
                    parents: false,
                    children: false,
                    parents_depth: None,
                    children_depth: None,
                    indirect_selection: Some(IndirectSelection::default()),
                    exclude: None,
                };
                let include = atom_to_select_expression(wrapper)?;
                Ok(ResolvedSelector {
                    include: Some(include),
                    exclude: None,
                })
            }

            AtomExpr::Exclude { exclude } => {
                // A standalone exclude atom
                let exprs = self.collect_definition_includes(exclude)?;
                let exclude_expr = SelectExpression::Or(exprs);
                Ok(ResolvedSelector {
                    include: None,
                    exclude: Some(exclude_expr),
                })
            }
        }
    }

    fn collect_definition_includes(
        &self,
        defs: &[SelectorDefinitionValue],
    ) -> FsResult<Vec<SelectExpression>> {
        defs.iter()
            .map(|dv| {
                let resolved = self.parse_definition(dv)?;
                resolved.include.ok_or_else(|| {
                    fs_err!(
                        ErrorCode::SelectorError,
                        "No include expression found in nested definition"
                    )
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_selector() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);
        let result =
            parser.parse_definition(&SelectorDefinitionValue::String("model_a".to_string()))?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::Atom(criteria)) = result.include {
            assert_eq!(criteria.method, MethodName::Fqn);
            assert_eq!(criteria.value, "model_a");
            assert!(!criteria.childrens_parents);
            assert!(criteria.parents_depth.is_none());
            assert!(criteria.children_depth.is_none());
        } else {
            panic!("Expected Atom expression");
        }
        Ok(())
    }

    #[test]
    fn test_method_key_selector() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let mut method_value = BTreeMap::new();
        method_value.insert("tag".to_string(), "nightly".to_string());

        let result = parser.parse_atom(&AtomExpr::MethodKey(method_value))?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::Atom(criteria)) = result.include {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "nightly");
            assert!(!criteria.childrens_parents);
            assert!(criteria.parents_depth.is_none());
            assert!(criteria.children_depth.is_none());
            assert_eq!(criteria.indirect, Some(IndirectSelection::default()));
        } else {
            panic!("Expected Atom expression");
        }
        Ok(())
    }

    #[test]
    fn test_method_selector_with_exclude() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let exclude = vec![SelectorDefinitionValue::String("model_b".to_string())];

        let result = parser.parse_atom(&AtomExpr::Method {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: Some(IndirectSelection::default()),
            exclude: Some(exclude),
        })?;

        assert!(result.include.is_some());
        if let Some(SelectExpression::Or(exprs)) = result.exclude {
            assert_eq!(exprs.len(), 1);
            if let SelectExpression::Atom(criteria) = &exprs[0] {
                assert_eq!(criteria.method, MethodName::Fqn);
                assert_eq!(criteria.value, "model_b");
            } else {
                panic!("Expected Atom expression in exclude");
            }
        } else {
            panic!("Expected Or expression in exclude");
        }
        Ok(())
    }

    #[test]
    fn test_union_selector() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let values = vec![
            SelectorDefinitionValue::String("model_a".to_string()),
            SelectorDefinitionValue::String("model_b".to_string()),
        ];

        let result = parser.parse_composite(&CompositeExpr {
            kind: CompositeKind::Union(values),
        })?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::Or(exprs)) = result.include {
            assert_eq!(exprs.len(), 2);
            if let (SelectExpression::Atom(a), SelectExpression::Atom(b)) = (&exprs[0], &exprs[1]) {
                assert_eq!(a.method, MethodName::Fqn);
                assert_eq!(a.value, "model_a");
                assert_eq!(b.method, MethodName::Fqn);
                assert_eq!(b.value, "model_b");
            } else {
                panic!("Expected Atom expressions");
            }
        } else {
            panic!("Expected Or expression");
        }
        Ok(())
    }

    #[test]
    fn test_intersection_selector() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let values = vec![
            SelectorDefinitionValue::String("model_a".to_string()),
            SelectorDefinitionValue::String("model_b".to_string()),
        ];

        let result = parser.parse_composite(&CompositeExpr {
            kind: CompositeKind::Intersection(values),
        })?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::And(exprs)) = result.include {
            assert_eq!(exprs.len(), 2);
            if let (SelectExpression::Atom(a), SelectExpression::Atom(b)) = (&exprs[0], &exprs[1]) {
                assert_eq!(a.method, MethodName::Fqn);
                assert_eq!(a.value, "model_a");
                assert_eq!(b.method, MethodName::Fqn);
                assert_eq!(b.value, "model_b");
            } else {
                panic!("Expected Atom expressions");
            }
        } else {
            panic!("Expected And expression");
        }
        Ok(())
    }

    #[test]
    fn test_named_selector() -> FsResult<()> {
        let mut defs = BTreeMap::new();
        defs.insert(
            "nightly_models".to_string(),
            SelectorDefinition {
                name: "nightly_models".to_string(),
                description: None,
                default: None,
                definition: SelectorDefinitionValue::String("tag:nightly".to_string()),
            },
        );

        let parser = SelectorParser::new(defs);
        let result = parser.parse_named("nightly_models")?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::Atom(criteria)) = result.include {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "nightly");
        } else {
            panic!("Expected Atom expression");
        }
        Ok(())
    }

    #[test]
    fn test_unknown_named_selector() {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);
        let result = parser.parse_named("unknown");
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.code, ErrorCode::SelectorError);
            assert!(e.to_string().contains("Unknown selector"));
        }
    }

    #[test]
    fn test_method_key_multiple_pairs() {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let mut method_value = BTreeMap::new();
        method_value.insert("tag".to_string(), "nightly".to_string());
        method_value.insert("path".to_string(), "models/".to_string());

        let result = parser.parse_atom(&AtomExpr::MethodKey(method_value));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.code, ErrorCode::SelectorError);
            assert!(e
                .to_string()
                .contains("MethodKey must have exactly one key-value pair"));
        }
    }

    #[test]
    fn test_method_selector_with_graph_operators() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let result = parser.parse_atom(&AtomExpr::Method {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: true,
            parents: true,
            children: true,
            parents_depth: Some(2),
            children_depth: Some(3),
            indirect_selection: Some(IndirectSelection::Cautious),
            exclude: None,
        })?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::Atom(criteria)) = result.include {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "nightly");
            assert!(criteria.childrens_parents);
            assert_eq!(criteria.parents_depth, Some(2));
            assert_eq!(criteria.children_depth, Some(3));
            assert_eq!(criteria.indirect, Some(IndirectSelection::Cautious));
        } else {
            panic!("Expected Atom expression");
        }
        Ok(())
    }

    #[test]
    fn test_complex_selector_with_excludes() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let exclude = vec![
            SelectorDefinitionValue::String("model_a".to_string()),
            SelectorDefinitionValue::String("model_b".to_string()),
        ];

        let result = parser.parse_atom(&AtomExpr::Method {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: Some(IndirectSelection::default()),
            exclude: Some(exclude),
        })?;

        assert!(result.include.is_some());
        if let Some(SelectExpression::Or(exprs)) = result.exclude {
            assert_eq!(exprs.len(), 2);
            if let (SelectExpression::Atom(a), SelectExpression::Atom(b)) = (&exprs[0], &exprs[1]) {
                assert_eq!(a.method, MethodName::Fqn);
                assert_eq!(a.value, "model_a");
                assert_eq!(b.method, MethodName::Fqn);
                assert_eq!(b.value, "model_b");
            } else {
                panic!("Expected Atom expressions in exclude");
            }
        } else {
            panic!("Expected Or expression in exclude");
        }
        Ok(())
    }

    #[test]
    fn test_parse_definition_with_full_expression() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let expr = SelectorExpr::Atom(AtomExpr::Method {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: Some(IndirectSelection::default()),
            exclude: None,
        });

        let result = parser.parse_definition(&SelectorDefinitionValue::Full(expr))?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::Atom(criteria)) = result.include {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "nightly");
        } else {
            panic!("Expected Atom expression");
        }
        Ok(())
    }

    #[test]
    fn test_nested_composite_expressions() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        // Create a nested expression: (tag:nightly + (model_a, model_b))
        let inner_union = SelectorExpr::Composite(CompositeExpr {
            kind: CompositeKind::Union(vec![
                SelectorDefinitionValue::String("model_a".to_string()),
                SelectorDefinitionValue::String("model_b".to_string()),
            ]),
        });

        let outer_intersection = SelectorExpr::Composite(CompositeExpr {
            kind: CompositeKind::Intersection(vec![
                SelectorDefinitionValue::String("tag:nightly".to_string()),
                SelectorDefinitionValue::Full(inner_union),
            ]),
        });

        let result = parser.parse_expr(&outer_intersection)?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::And(exprs)) = result.include {
            assert_eq!(exprs.len(), 2);
            // First expression should be the tag selector
            if let SelectExpression::Atom(tag_criteria) = &exprs[0] {
                assert_eq!(tag_criteria.method, MethodName::Tag);
                assert_eq!(tag_criteria.value, "nightly");
            } else {
                panic!("Expected Atom expression for tag");
            }
            // Second expression should be the OR of model_a and model_b
            if let SelectExpression::Or(model_exprs) = &exprs[1] {
                assert_eq!(model_exprs.len(), 2);
                if let (SelectExpression::Atom(a), SelectExpression::Atom(b)) =
                    (&model_exprs[0], &model_exprs[1])
                {
                    assert_eq!(a.method, MethodName::Fqn);
                    assert_eq!(a.value, "model_a");
                    assert_eq!(b.method, MethodName::Fqn);
                    assert_eq!(b.value, "model_b");
                } else {
                    panic!("Expected Atom expressions for models");
                }
            } else {
                panic!("Expected Or expression for models");
            }
        } else {
            panic!("Expected And expression");
        }
        Ok(())
    }

    #[test]
    fn test_collect_definition_includes() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let definitions = vec![
            SelectorDefinitionValue::String("model_a".to_string()),
            SelectorDefinitionValue::String("model_b".to_string()),
        ];

        let result = parser.collect_definition_includes(&definitions)?;

        assert_eq!(result.len(), 2);
        if let (SelectExpression::Atom(a), SelectExpression::Atom(b)) = (&result[0], &result[1]) {
            assert_eq!(a.method, MethodName::Fqn);
            assert_eq!(a.value, "model_a");
            assert_eq!(b.method, MethodName::Fqn);
            assert_eq!(b.value, "model_b");
        } else {
            panic!("Expected Atom expressions");
        }
        Ok(())
    }

    #[test]
    fn test_set_indirect_selection() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        // Create a complex expression with nested AND/OR
        let expr = SelectorExpr::Composite(CompositeExpr {
            kind: CompositeKind::Intersection(vec![
                SelectorDefinitionValue::String("model_a".to_string()),
                SelectorDefinitionValue::String("model_b".to_string()),
            ]),
        });

        let mut result = parser.parse_expr(&expr)?;

        // Set indirect selection mode
        if let Some(include) = &mut result.include {
            include.set_indirect_selection(IndirectSelection::Cautious);
        }

        // Verify the change propagated to all nested expressions
        if let Some(SelectExpression::And(exprs)) = &result.include {
            for expr in exprs {
                if let SelectExpression::Atom(criteria) = expr {
                    assert_eq!(criteria.indirect, Some(IndirectSelection::Cautious));
                } else {
                    panic!("Expected Atom expression");
                }
            }
        } else {
            panic!("Expected And expression");
        }
        Ok(())
    }

    #[test]
    fn test_parse_definition_with_empty_exclude() -> FsResult<()> {
        let defs = BTreeMap::new();
        let parser = SelectorParser::new(defs);

        let result = parser.parse_atom(&AtomExpr::Method {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: Some(IndirectSelection::default()),
            exclude: None,
        })?;

        assert!(result.exclude.is_none());
        if let Some(SelectExpression::Atom(criteria)) = result.include {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "nightly");
        } else {
            panic!("Expected Atom expression");
        }
        Ok(())
    }
}
