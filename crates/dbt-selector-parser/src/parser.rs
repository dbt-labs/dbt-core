//! Convert YAML selectors (as parsed by `dbt-schemas`) into the
//! `SelectExpression` + *optional* `exclude` expression that the
//! scheduler understands.
//

use std::{collections::BTreeMap, str::FromStr};

use dbt_common::{
    ErrorCode, FsResult, err, fs_err,
    io_args::IoArgs,
    node_selector::{
        IndirectSelection, MethodName, SelectExpression, SelectionCriteria, parse_model_specifiers,
    },
    show_warning,
};

use dbt_schemas::schemas::selectors::{
    AtomExpr, CompositeExpr, CompositeKind, MethodAtomExpr, SelectorDefinition,
    SelectorDefinitionValue, SelectorExpr,
};

#[derive(Debug, Clone)]
pub struct SelectorParser<'a> {
    defs: BTreeMap<String, SelectorDefinition>,
    io_args: &'a IoArgs,
}

impl<'a> SelectorParser<'a> {
    pub fn new(defs: BTreeMap<String, SelectorDefinition>, io_args: &'a IoArgs) -> Self {
        Self { defs, io_args }
    }

    pub fn parse_named(&self, name: &str) -> FsResult<SelectExpression> {
        let def = self
            .defs
            .get(name)
            .ok_or_else(|| fs_err!(ErrorCode::SelectorError, "Unknown selector `{}`", name))?;
        self.parse_definition(&def.definition)
    }

    pub fn parse_definition(&self, def: &SelectorDefinitionValue) -> FsResult<SelectExpression> {
        match def {
            SelectorDefinitionValue::String(s) => Ok(parse_model_specifiers(&[s.clone()])?),
            SelectorDefinitionValue::Full(expr) => self.parse_expr(expr),
        }
    }

    pub fn parse_expr(&self, expr: &SelectorExpr) -> FsResult<SelectExpression> {
        match expr {
            SelectorExpr::Composite(comp) => self.parse_composite(comp),
            SelectorExpr::Atom(atom) => self.parse_atom(atom),
        }
    }

    pub fn parse_composite(&self, comp: &CompositeExpr) -> FsResult<SelectExpression> {
        let mut includes = Vec::new();
        let mut exclude_expr: Option<SelectExpression> = None;

        // Get the values from the CompositeKind directly
        let values = match &comp.kind {
            CompositeKind::Union(vals) => vals,
            CompositeKind::Intersection(vals) => vals,
        };

        for value in values {
            // Check if this value is an exclude expression
            if let SelectorDefinitionValue::Full(SelectorExpr::Atom(AtomExpr::Exclude(exclude))) =
                value
            {
                // Handle exclude as a special case within composite expressions
                let exprs = self.collect_definition_includes(&exclude.exclude)?;
                let exclude_expression = match exprs.len() {
                    0 => return Err(fs_err!(ErrorCode::SelectorError, "Empty exclude list")),
                    1 => exprs.into_iter().next().unwrap(),
                    _ => SelectExpression::Or(exprs),
                };
                exclude_expr = Some(exclude_expression);
            } else {
                // Handle regular include expressions
                let resolved = self.parse_definition(value)?;
                includes.push(resolved);
            }
        }

        // Build the boolean operator over includes
        let include_expr = match &comp.kind {
            CompositeKind::Union(_) => SelectExpression::Or(includes),
            CompositeKind::Intersection(_) => SelectExpression::And(includes),
        };

        // If we have an exclude expression, we need to handle this specially
        // For now, let's create a structure that represents "include this but exclude that"
        // We'll use a custom approach that the scheduler can handle
        if let Some(exclude) = exclude_expr {
            // Create a structure that represents the composite with exclude
            // We'll use a special marker that the scheduler can recognize
            // For now, let's just return the include expression and handle the exclude in the scheduler
            return Ok(SelectExpression::And(vec![
                include_expr,
                SelectExpression::Exclude(Box::new(exclude)),
            ]));
        }

        Ok(include_expr)
    }

    fn parse_atom(&self, atom: &AtomExpr) -> FsResult<SelectExpression> {
        match atom {
            AtomExpr::Method(expr) => {
                // Special handling for selector method - recursively resolve the referenced selector
                if expr.method == "selector" {
                    // Recursively resolve the referenced selector
                    let referenced_selector = self.parse_named(&expr.value)?;

                    // Note: Per the docs, graph operators (parents, children, etc.) are NOT
                    // supported for selector inheritance, so we ignore them and return the
                    // referenced selector's include expression as-is
                    if expr.childrens_parents
                        || expr.parents
                        || expr.children
                        || expr.parents_depth.is_some()
                        || expr.children_depth.is_some()
                    {
                        let warning = fs_err!(
                            ErrorCode::SelectorError,
                            "Graph operators (parents, children, etc.) are not supported with selector inheritance and will be ignored"
                        );
                        show_warning!(self.io_args, warning);
                    }

                    // Return the referenced selector's include expression
                    Ok(referenced_selector)
                } else {
                    // Use atom_to_select_expression which handles the exclude field properly
                    self.atom_to_select_expression(AtomExpr::Method(MethodAtomExpr {
                        method: expr.method.clone(),
                        value: expr.value.clone(),
                        childrens_parents: expr.childrens_parents,
                        parents: expr.parents,
                        children: expr.children,
                        parents_depth: expr.parents_depth,
                        children_depth: expr.children_depth,
                        indirect_selection: expr.indirect_selection,
                        exclude: expr.exclude.clone(),
                    }))
                }
            }

            AtomExpr::MethodKey(method_value) => {
                if method_value.len() != 1 {
                    return Err(fs_err!(
                        ErrorCode::SelectorError,
                        "MethodKey must have exactly one key-value pair"
                    ));
                }
                let (m, v) = method_value.iter().next().unwrap();
                let wrapper = AtomExpr::Method(MethodAtomExpr {
                    method: m.clone(),
                    value: v.clone(),
                    childrens_parents: false,
                    parents: false,
                    children: false,
                    parents_depth: None,
                    children_depth: None,
                    indirect_selection: Some(IndirectSelection::default()),
                    exclude: None,
                });
                self.atom_to_select_expression(wrapper)
            }

            AtomExpr::Exclude(_) => {
                err!(
                    ErrorCode::SelectorError,
                    "Top level exclude not allowed in YAML selectors"
                )
            }
        }
    }

    fn collect_definition_includes(
        &self,
        defs: &[SelectorDefinitionValue],
    ) -> FsResult<Vec<SelectExpression>> {
        defs.iter().map(|dv| self.parse_definition(dv)).collect()
    }

    fn atom_to_select_expression(&self, atom: AtomExpr) -> FsResult<SelectExpression> {
        match atom {
            AtomExpr::Method(expr) => {
                let method = expr.method.clone();
                let value = expr.value.clone();
                let childrens_parents = expr.childrens_parents;
                let parents = expr.parents;
                let children = expr.children;
                let parents_depth = expr.parents_depth;
                let children_depth = expr.children_depth;
                let indirect_selection = expr.indirect_selection;
                let exclude = expr.exclude;
                // ── 1️⃣  resolve method / args ────────────────────────────────
                let (name, args) = {
                    let mut parts = method.split('.').map(|s| s.to_string());
                    let head = parts.next().unwrap();
                    let nm = MethodName::from_str(&head)
                        .unwrap_or_else(|_| MethodName::default_for(&value));
                    (nm, parts.collect())
                };

                // ── 2️⃣  normalise depth flags ────────────────────────────────
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

                // ── 3️⃣  build *nested* exclude expression (if present) ───────
                let exclude_expr: Option<Box<SelectExpression>> = if let Some(defs) = &exclude {
                    let exprs = defs
                        .iter()
                        .map(|d| self.parse_definition(d))
                        .collect::<FsResult<Vec<_>>>()?;
                    match exprs.len() {
                        0 => None,
                        1 => Some(Box::new(exprs.into_iter().next().unwrap())),
                        _ => Some(Box::new(SelectExpression::Or(exprs))),
                    }
                } else {
                    None
                };

                // ── 4️⃣  assemble criteria & return ───────────────────────────
                let criteria = SelectionCriteria::new(
                    name,
                    args,
                    value,
                    childrens_parents,
                    pd,
                    cd,
                    indirect_selection,
                    exclude_expr,
                );
                Ok(SelectExpression::Atom(criteria))
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
                    None,
                )))
            }
            AtomExpr::Exclude(expr) => {
                // A standalone exclude atom - this becomes a top-level exclude
                let exprs = self.collect_definition_includes(&expr.exclude)?;
                let exclude_expr = match exprs.len() {
                    0 => return Err(fs_err!(ErrorCode::SelectorError, "Empty exclude list")),
                    1 => exprs.into_iter().next().unwrap(),
                    _ => SelectExpression::Or(exprs),
                };
                Ok(SelectExpression::Exclude(Box::new(exclude_expr)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use dbt_schemas::schemas::selectors::ExcludeAtomExpr;

    use super::*;

    #[test]
    fn test_string_selector() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);
        let result =
            parser.parse_definition(&SelectorDefinitionValue::String("model_a".to_string()))?;

        if let SelectExpression::Atom(criteria) = result {
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

        if let SelectExpression::Atom(criteria) = result {
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
            assert!(
                e.to_string()
                    .contains("MethodKey must have exactly one key-value pair")
            );
        }
    }

    #[test]
    fn test_exclude_handling() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        // Test single exclude - should be nested within SelectionCriteria
        let single_result = parser.parse_atom(&AtomExpr::Method(MethodAtomExpr {
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
        }))?;

        // The result should be an Atom with nested exclude
        if let SelectExpression::Atom(criteria) = single_result {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "nightly");
            // Check that exclude is nested within the criteria
            if let Some(exclude_expr) = criteria.exclude {
                if let SelectExpression::Atom(exclude_criteria) = *exclude_expr {
                    assert_eq!(exclude_criteria.method, MethodName::Fqn);
                    assert_eq!(exclude_criteria.value, "model_to_exclude");
                } else {
                    panic!("Expected Atom expression inside nested exclude");
                }
            } else {
                panic!("Expected nested exclude in criteria");
            }
        } else {
            panic!("Expected Atom expression");
        }

        // Test multiple excludes - should be nested within SelectionCriteria as Or
        let multiple_result = parser.parse_atom(&AtomExpr::Method(MethodAtomExpr {
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
        }))?;

        // The result should be an Atom with nested exclude containing Or
        if let SelectExpression::Atom(criteria) = multiple_result {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "nightly");
            // Check that exclude is nested within the criteria as Or
            if let Some(exclude_expr) = criteria.exclude {
                if let SelectExpression::Or(exprs) = *exclude_expr {
                    assert_eq!(exprs.len(), 2);
                    if let (SelectExpression::Atom(a), SelectExpression::Atom(b)) =
                        (&exprs[0], &exprs[1])
                    {
                        assert_eq!(a.method, MethodName::Fqn);
                        assert_eq!(a.value, "model_a");
                        assert_eq!(b.method, MethodName::Fqn);
                        assert_eq!(b.value, "model_b");
                    } else {
                        panic!("Expected Atom expressions in nested exclude");
                    }
                } else {
                    panic!("Expected Or expression inside nested exclude");
                }
            } else {
                panic!("Expected nested exclude in criteria");
            }
        } else {
            panic!("Expected Atom expression");
        }
        Ok(())
    }

    #[test]
    fn test_standalone_exclude() {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        let result = parser.parse_atom(&AtomExpr::Exclude(ExcludeAtomExpr {
            exclude: vec![SelectorDefinitionValue::String("model_exclude".to_string())],
        }));

        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.code, ErrorCode::SelectorError);
            assert!(
                e.to_string()
                    .contains("Top level exclude not allowed in YAML selectors")
            );
        }
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

        if let SelectExpression::Or(exprs) = union_result {
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

        if let SelectExpression::And(exprs) = intersection_result {
            assert_eq!(exprs.len(), 2);
        } else {
            panic!("Expected And expression for intersection");
        }

        // Test composite with excludes - excludes should be nested within the include
        let composite_with_exclude = parser.parse_composite(&CompositeExpr {
            kind: CompositeKind::Union(vec![
                SelectorDefinitionValue::String("tag:bar".to_string()),
                SelectorDefinitionValue::Full(SelectorExpr::Atom(AtomExpr::Method(
                    MethodAtomExpr {
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
                    },
                ))),
            ]),
        })?;

        // The result should be an Or with one regular atom and one atom with nested exclude
        if let SelectExpression::Or(exprs) = composite_with_exclude {
            assert_eq!(exprs.len(), 2);
            // First should be a regular atom
            if let SelectExpression::Atom(criteria) = &exprs[0] {
                assert_eq!(criteria.method, MethodName::Tag);
                assert_eq!(criteria.value, "bar");
            } else {
                panic!("Expected first expression to be Atom");
            }
            // Second should be an Atom with nested exclude
            if let SelectExpression::Atom(criteria) = &exprs[1] {
                assert_eq!(criteria.method, MethodName::Tag);
                assert_eq!(criteria.value, "baz");
                // Check that exclude is nested within the criteria
                if let Some(exclude_expr) = &criteria.exclude {
                    if let SelectExpression::Atom(exclude_criteria) = &**exclude_expr {
                        assert_eq!(exclude_criteria.method, MethodName::Fqn);
                        assert_eq!(exclude_criteria.value, "single_exclude");
                    } else {
                        panic!("Expected Atom inside nested exclude");
                    }
                } else {
                    panic!("Expected nested exclude in criteria");
                }
            } else {
                panic!("Expected second expression to be Atom with nested exclude");
            }
        } else {
            panic!("Expected Or expression for composite");
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
        let result = parser.parse_atom(&AtomExpr::Method(MethodAtomExpr {
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
        }))?;

        // Should inherit the intersection from foo_and_bar
        if let SelectExpression::And(exprs) = result {
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
                definition: SelectorDefinitionValue::Full(SelectorExpr::Atom(AtomExpr::Method(
                    MethodAtomExpr {
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
                    },
                ))),
            },
        );

        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        // Reference the base selector and add more excludes
        let result = parser.parse_atom(&AtomExpr::Method(MethodAtomExpr {
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
        }))?;

        // Should return the base selector's include expression (which has nested exclude)
        if let SelectExpression::Atom(criteria) = result {
            assert_eq!(criteria.method, MethodName::Tag);
            assert_eq!(criteria.value, "production");
            // Check that the base exclude is preserved in the nested exclude
            if let Some(exclude_expr) = criteria.exclude {
                if let SelectExpression::Atom(exclude_criteria) = *exclude_expr {
                    assert_eq!(exclude_criteria.method, MethodName::Fqn);
                    assert_eq!(exclude_criteria.value, "base_exclude");
                } else {
                    panic!("Expected Atom expression inside nested exclude");
                }
            } else {
                panic!("Expected nested exclude in criteria");
            }
        } else {
            panic!("Expected Atom expression");
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

        if let SelectExpression::Atom(criteria) = result {
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
        let inheritance_result = parser.parse_atom(&AtomExpr::Method(MethodAtomExpr {
            method: "selector".to_string(),
            value: "unknown_selector".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: None,
            exclude: None,
        }));
        assert!(inheritance_result.is_err());
    }

    #[test]
    fn test_graph_operators() -> FsResult<()> {
        let defs = BTreeMap::new();
        let io_args = IoArgs::default();
        let parser = SelectorParser::new(defs, &io_args);

        let result = parser.parse_atom(&AtomExpr::Method(MethodAtomExpr {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: true,
            parents: true,
            children: true,
            parents_depth: Some(2),
            children_depth: Some(3),
            indirect_selection: Some(IndirectSelection::Cautious),
            exclude: None,
        }))?;

        if let SelectExpression::Atom(criteria) = result {
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

        let expr = SelectorExpr::Atom(AtomExpr::Method(MethodAtomExpr {
            method: "tag".to_string(),
            value: "nightly".to_string(),
            childrens_parents: false,
            parents: false,
            children: false,
            parents_depth: None,
            children_depth: None,
            indirect_selection: Some(IndirectSelection::default()),
            exclude: None,
        }));

        let result = parser.parse_definition(&SelectorDefinitionValue::Full(expr))?;

        if let SelectExpression::Atom(criteria) = result {
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
        result.set_indirect_selection(IndirectSelection::Cautious);

        // Verify the change propagated to all nested expressions
        if let SelectExpression::And(exprs) = &result {
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
