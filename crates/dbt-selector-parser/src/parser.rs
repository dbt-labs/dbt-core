//! Convert YAML selectors (as parsed by `dbt-schemas`) into the
//! `SelectExpression` + *optional* `exclude` expression that the
//! scheduler understands.
//

use std::{collections::BTreeMap, str::FromStr};

use dbt_common::{
    err, fs_err,
    io_args::IoArgs,
    node_selector::{
        parse_model_specifiers, IndirectSelection, MethodName, SelectExpression, SelectionCriteria,
    },
    show_warning, ErrorCode, FsResult,
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
pub struct SelectorParser<'a> {
    defs: BTreeMap<String, SelectorDefinition>,
    io_args: &'a IoArgs,
}

impl<'a> SelectorParser<'a> {
    pub fn new(defs: BTreeMap<String, SelectorDefinition>, io_args: &'a IoArgs) -> Self {
        Self { defs, io_args }
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
        let exclude_expr = match excludes.len() {
            0 => None,
            1 => Some(excludes.into_iter().next().unwrap()),
            _ => Some(SelectExpression::Or(excludes)),
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
                // Special handling for selector method - recursively resolve the referenced selector
                if method == "selector" {
                    // Parse any per-method excludes first
                    let exclude_expr = exclude
                        .as_ref()
                        .map(|list| {
                            let exprs = self.collect_definition_includes(list)?;
                            Ok::<SelectExpression, Box<dbt_common::FsError>>(match exprs.len() {
                                0 => {
                                    return Err(fs_err!(
                                        ErrorCode::SelectorError,
                                        "Empty exclude list"
                                    ))
                                }
                                1 => exprs.into_iter().next().unwrap(),
                                _ => SelectExpression::Or(exprs),
                            })
                        })
                        .transpose()?;

                    // Recursively resolve the referenced selector
                    let referenced_selector = self.parse_named(value)?;

                    // Combine with any method-level excludes
                    let final_exclude = match (referenced_selector.exclude, exclude_expr) {
                        (Some(ref_ex), Some(method_ex)) => {
                            Some(SelectExpression::Or(vec![ref_ex, method_ex]))
                        }
                        (Some(ref_ex), None) => Some(ref_ex),
                        (None, Some(method_ex)) => Some(method_ex),
                        (None, None) => None,
                    };

                    // Note: Per the docs, graph operators (parents, children, etc.) are NOT
                    // supported for selector inheritance, so we ignore them and return the
                    // referenced selector's include expression as-is
                    if *childrens_parents
                        || *parents
                        || *children
                        || parents_depth.is_some()
                        || children_depth.is_some()
                    {
                        let warning = fs_err!(
                            ErrorCode::SelectorError,
                            "Graph operators (parents, children, etc.) are not supported with selector inheritance and will be ignored"
                        );
                        show_warning!(self.io_args, warning);
                    }

                    return Ok(ResolvedSelector {
                        include: referenced_selector.include,
                        exclude: final_exclude,
                    });
                }

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
                        Ok::<SelectExpression, Box<dbt_common::FsError>>(match exprs.len() {
                            0 => {
                                return Err(fs_err!(ErrorCode::SelectorError, "Empty exclude list"))
                            }
                            1 => exprs.into_iter().next().unwrap(),
                            _ => SelectExpression::Or(exprs),
                        })
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
                let exclude_expr = match exprs.len() {
                    0 => return Err(fs_err!(ErrorCode::SelectorError, "Empty exclude list")),
                    1 => exprs.into_iter().next().unwrap(),
                    _ => SelectExpression::Or(exprs),
                };
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
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);
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
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

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
    fn test_method_key_multiple_pairs() {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

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
    fn test_exclude_handling() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        // Test single exclude - should not be wrapped in Or
        let single_result = parser.parse_atom(&AtomExpr::Method {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: Some(IndirectSelection::default()),
            exclude: Some(vec![SelectorDefinitionValue::String(
                "model_to_exclude".to_string(),
            )]),
        })?;

        if let Some(SelectExpression::Atom(criteria)) = single_result.exclude {
            assert_eq!(criteria.method, MethodName::Fqn);
            assert_eq!(criteria.value, "model_to_exclude");
        } else {
            panic!("Expected single exclude to be an Atom");
        }

        // Test multiple excludes - should be wrapped in Or
        let multiple_result = parser.parse_atom(&AtomExpr::Method {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: Some(IndirectSelection::default()),
            exclude: Some(vec![
                SelectorDefinitionValue::String("model_a".to_string()),
                SelectorDefinitionValue::String("model_b".to_string()),
            ]),
        })?;

        if let Some(SelectExpression::Or(exprs)) = multiple_result.exclude {
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
            panic!("Expected multiple excludes to be wrapped in Or");
        }
        Ok(())
    }

    #[test]
    fn test_standalone_exclude() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        let result = parser.parse_atom(&AtomExpr::Exclude {
            exclude: vec![SelectorDefinitionValue::String("model_exclude".to_string())],
        })?;

        assert!(result.include.is_none());
        if let Some(SelectExpression::Atom(criteria)) = result.exclude {
            assert_eq!(criteria.method, MethodName::Fqn);
            assert_eq!(criteria.value, "model_exclude");
        } else {
            panic!("Expected standalone exclude to be an Atom");
        }
        Ok(())
    }

    #[test]
    fn test_composite_operations() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        // Test union
        let union_result = parser.parse_composite(&CompositeExpr {
            kind: CompositeKind::Union(vec![
                SelectorDefinitionValue::String("model_a".to_string()),
                SelectorDefinitionValue::String("model_b".to_string()),
            ]),
        })?;

        if let Some(SelectExpression::Or(exprs)) = union_result.include {
            assert_eq!(exprs.len(), 2);
        } else {
            panic!("Expected Or expression for union");
        }

        // Test intersection
        let intersection_result = parser.parse_composite(&CompositeExpr {
            kind: CompositeKind::Intersection(vec![
                SelectorDefinitionValue::String("model_a".to_string()),
                SelectorDefinitionValue::String("model_b".to_string()),
            ]),
        })?;

        if let Some(SelectExpression::And(exprs)) = intersection_result.include {
            assert_eq!(exprs.len(), 2);
        } else {
            panic!("Expected And expression for intersection");
        }

        // Test composite with excludes
        let composite_with_exclude = parser.parse_composite(&CompositeExpr {
            kind: CompositeKind::Union(vec![
                SelectorDefinitionValue::String("tag:bar".to_string()),
                SelectorDefinitionValue::Full(SelectorExpr::Atom(AtomExpr::Method {
                    method: "tag".to_string(),
                    value: "baz".to_string(),
                    childrens_parents: false,
                    parents: false,
                    children: false,
                    parents_depth: None,
                    children_depth: None,
                    indirect_selection: None,
                    exclude: Some(vec![SelectorDefinitionValue::String(
                        "single_exclude".to_string(),
                    )]),
                })),
            ]),
        })?;

        // Single exclude at composite level should not be wrapped
        if let Some(SelectExpression::Atom(criteria)) = composite_with_exclude.exclude {
            assert_eq!(criteria.method, MethodName::Fqn);
            assert_eq!(criteria.value, "single_exclude");
        } else {
            panic!("Expected single exclude from composite to be an Atom");
        }

        Ok(())
    }

    #[test]
    fn test_selector_inheritance() -> FsResult<()> {
        let mut defs = BTreeMap::new();
        defs.insert(
            "foo_and_bar".to_string(),
            SelectorDefinition {
                name: "foo_and_bar".to_string(),
                description: None,
                default: None,
                definition: SelectorDefinitionValue::Full(SelectorExpr::Composite(CompositeExpr {
                    kind: CompositeKind::Intersection(vec![
                        SelectorDefinitionValue::String("tag:foo".to_string()),
                        SelectorDefinitionValue::String("tag:bar".to_string()),
                    ]),
                })),
            },
        );

        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        // Test basic inheritance with additional exclude
        let result = parser.parse_atom(&AtomExpr::Method {
            method: "selector".to_string(),
            value: "foo_and_bar".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: None,
            exclude: Some(vec![SelectorDefinitionValue::String(
                "tag:buzz".to_string(),
            )]),
        })?;

        // Should inherit the intersection from foo_and_bar
        if let Some(SelectExpression::And(exprs)) = result.include {
            assert_eq!(exprs.len(), 2);
            let mut tag_values = Vec::new();
            for expr in &exprs {
                if let SelectExpression::Atom(criteria) = expr {
                    assert_eq!(criteria.method, MethodName::Tag);
                    tag_values.push(criteria.value.clone());
                }
            }
            tag_values.sort();
            assert_eq!(tag_values, vec!["bar", "foo"]);
        } else {
            panic!("Expected And expression from inherited selector");
        }

        // Should have the exclude from the referencing selector
        if let Some(SelectExpression::Atom(criteria)) = result.exclude {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "buzz");
        } else {
            panic!("Expected exclude expression");
        }

        Ok(())
    }

    #[test]
    fn test_selector_inheritance_with_exclude_combination() -> FsResult<()> {
        let mut defs = BTreeMap::new();
        defs.insert(
            "base_with_exclude".to_string(),
            SelectorDefinition {
                name: "base_with_exclude".to_string(),
                description: None,
                default: None,
                definition: SelectorDefinitionValue::Full(SelectorExpr::Atom(AtomExpr::Method {
                    method: "tag".to_string(),
                    value: "production".to_string(),
                    childrens_parents: false,
                    parents: false,
                    children: false,
                    parents_depth: None,
                    children_depth: None,
                    indirect_selection: None,
                    exclude: Some(vec![SelectorDefinitionValue::String(
                        "base_exclude".to_string(),
                    )]),
                })),
            },
        );

        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        // Reference the base selector and add more excludes
        let result = parser.parse_atom(&AtomExpr::Method {
            method: "selector".to_string(),
            value: "base_with_exclude".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: None,
            exclude: Some(vec![SelectorDefinitionValue::String(
                "additional_exclude".to_string(),
            )]),
        })?;

        // Should combine excludes from base selector and referencing selector
        if let Some(SelectExpression::Or(exprs)) = result.exclude {
            assert_eq!(exprs.len(), 2); // base exclude + additional exclude
        } else {
            panic!("Expected combined excludes to be wrapped in Or");
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

        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);
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
    fn test_error_handling() {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        // Test unknown selector
        let result = parser.parse_named("unknown");
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.code, ErrorCode::SelectorError);
            assert!(e.to_string().contains("Unknown selector"));
        }

        // Test unknown selector in inheritance
        let inheritance_result = parser.parse_atom(&AtomExpr::Method {
            method: "selector".to_string(),
            value: "unknown_selector".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: None,
            exclude: None,
        });
        assert!(inheritance_result.is_err());
    }

    #[test]
    fn test_graph_operators() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

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
    fn test_full_vs_string_definitions() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

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
    fn test_indirect_selection_propagation() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

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
}
