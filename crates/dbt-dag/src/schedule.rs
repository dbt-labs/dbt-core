use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use dbt_common::{node_selector::SelectExpression, pretty_table::DisplayFormat};
use dbt_schemas::schemas::Nodes;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default)]
pub struct Schedule<T> {
    // This is the dependency DAG
    // - note: all values are also defined as keys
    // - note: T is always a String, namely the unique id of a node
    pub deps: BTreeMap<T, BTreeSet<T>>,
    // this is a topological sort of the selected dag (selected & frontier -> upstreams)
    pub sorted_nodes: Vec<T>,
    // this is a topological sort of the self-contained dag (i.e. selected + frontier)
    pub self_contained_nodes: Vec<T>,
    // these are the selected nodes, they are a superset of deps/sorted
    pub selected_nodes: BTreeSet<T>,
    // these are the nodes in the frontier, a subset of deps (where deps are not closed)
    pub frontier_nodes: BTreeSet<T>,
    // Unused source nodes (these are excluded from sorted_nodes)
    pub unused_nodes: BTreeSet<T>,
    // normalized select expressions
    pub select: Option<SelectExpression>,
    // normalized exclude expressions
    pub exclude: Option<SelectExpression>,
}

impl Schedule<String> {
    /// Show the selected nodes as the type.package.name
    pub fn show_nodes(&self) -> String {
        let mut res = "".to_string();
        if let Some(select) = &self.select {
            res.push_str(&format!("    [--select: {select}]\n"));
        }
        if let Some(exclude) = &self.exclude {
            res.push_str(&format!("    [--exclude: {exclude}]\n"));
        }
        res.push_str(
            &self
                .selected_nodes
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        );
        res.push('\n');
        res
    }

    /// Generate JSON output for a single node based on the specified keys
    fn generate_json_output(node_value: &Value, output_keys: &[String]) -> String {
        let mut json_map = Map::new();

        // If output_keys is empty, use a default set
        // https://github.com/dbt-labs/dbt-core/blob/65d428004a76071d58d6234841bf8b18f9cd3100/core/dbt/task/list.py#L42
        let keys_to_use = if output_keys.is_empty() {
            vec![
                "alias",
                "name",
                "package_name",
                "depends_on",
                "tags",
                "config",
                "resource_type",
                "source_name",
                "original_file_path",
                "unique_id",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        } else {
            output_keys.to_vec()
        };

        // Convert the serialized node to a map for easier manipulation
        let node_map = node_value
            .as_object()
            .expect("Failed to convert node to map, should not happen");

        // Handle depends_on field specially (remove nodes_with_ref_location)
        if keys_to_use.contains(&"depends_on".to_string()) {
            if let Some(depends_on_value) = node_map.get("depends_on") {
                if let Some(depends_on_obj) = depends_on_value.as_object() {
                    let mut cleaned_depends_on = depends_on_obj.clone();
                    cleaned_depends_on.remove("nodes_with_ref_location");
                    json_map.insert("depends_on".to_string(), Value::Object(cleaned_depends_on));
                } else {
                    json_map.insert("depends_on".to_string(), depends_on_value.clone());
                }
            }
        }

        // Add all other requested keys that exist in the node
        for key in &keys_to_use {
            if key != "depends_on" && node_map.contains_key(key) {
                json_map.insert(key.clone(), node_map[key].clone());
            }
        }

        serde_json::to_string(&json_map).unwrap()
    }

    /// Show the selected nodes in the specified format.
    /// For JSON output, each node is a separate JSON object on a new line,
    /// containing keys specified in `output_keys`.
    pub fn show_dbt_nodes(
        &self,
        nodes: &Nodes,
        output_format: &DisplayFormat,
        output_keys: &[String],
    ) -> Vec<String> {
        let mut res = Vec::new();
        for selected_id in &self.selected_nodes {
            let node = nodes
                .get_node(selected_id)
                .expect("selected node not in manifest");
            let fqn = node.common().fqn.join(".");
            let node_value = node.serialize();
            match output_format {
                DisplayFormat::Json => {
                    let json_string = Self::generate_json_output(&node_value, output_keys);
                    res.push(json_string);
                }
                // Handle other DisplayFormat variants if necessary (Csv, Markdown, Html, Table)
                // For now, maybe default them to Text output or return an error?
                _ => {
                    // Default to Text for other formats for now
                    res.push(fqn);
                }
            }
        }
        res
    }
}

impl fmt::Display for Schedule<String> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let show_frontier = !self.frontier_nodes.is_empty();
        let max_key_len = self.deps.keys().map(|key| key.len()).max().unwrap_or(0);

        if show_frontier {
            writeln!(
                f,
                "{:<width$} | {:<8} | Depends-on",
                "Unique Id",
                "Frontier",
                width = max_key_len
            )?;
        } else {
            writeln!(
                f,
                "{:<width$} |  Depends-on",
                "Unique Id",
                width = max_key_len
            )?;
        }
        let total_width = max_key_len + 3 + 8 + 3 + max_key_len; // 3 is for the spaces and separators

        writeln!(f, "{}", "-".repeat(total_width))?;

        for key in &self.sorted_nodes {
            if let Some(value_set) = self.deps.get(key) {
                let values_str = value_set
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                if show_frontier {
                    let frontier_marker = if self.frontier_nodes.contains(key) {
                        "*"
                    } else {
                        ""
                    };
                    writeln!(
                        f,
                        "{key:<max_key_len$} | {frontier_marker:<8} | {values_str}"
                    )?;
                } else {
                    writeln!(f, "{key:<max_key_len$} | {values_str}")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn test_filter_test_and_frontier_nodes() {
        // Create a dependency graph
        let mut deps = BTreeMap::new();

        // Add a model
        deps.insert("model.project.model1".to_string(), BTreeSet::new());

        // Add a test that depends on the model
        deps.insert("test.project.test1".to_string(), {
            let mut deps = BTreeSet::new();
            deps.insert("model.project.model1".to_string());
            deps
        });

        // Add a standalone test
        deps.insert("test.project.standalone_test".to_string(), BTreeSet::new());

        // Create a topological sort
        let sorted_nodes = vec![
            "model.project.model1".to_string(),
            "test.project.test1".to_string(),
            "test.project.standalone_test".to_string(),
        ];

        // All nodes are self-contained
        let self_contained_nodes = sorted_nodes.clone();

        // All nodes are selected
        let selected_nodes: BTreeSet<String> = sorted_nodes.iter().cloned().collect();

        // Add a frontier node
        let mut frontier_nodes = BTreeSet::new();
        frontier_nodes.insert("model.project.model1".to_string());

        let schedule = Schedule {
            deps,
            sorted_nodes,
            self_contained_nodes,
            selected_nodes,
            frontier_nodes,
            select: None,
            exclude: None,
            unused_nodes: BTreeSet::new(),
        };

        // Check that standalone test is kept even though it has no dependencies
        assert!(schedule
            .sorted_nodes
            .contains(&"test.project.standalone_test".to_string()));

        // Check that model1 is kept because test1 depends on it
        assert!(schedule
            .sorted_nodes
            .contains(&"model.project.model1".to_string()));

        // Check that test1 is kept
        assert!(schedule
            .sorted_nodes
            .contains(&"test.project.test1".to_string()));
    }

    #[test]
    fn test_revise_for_unit_tests_with_test_command_and_unit_tests() {
        // Create a dependency graph
        let mut deps = BTreeMap::new();

        // Add a model
        deps.insert("model.project.model1".to_string(), BTreeSet::new());

        // Add a test that depends on the model
        deps.insert("test.project.test1".to_string(), {
            let mut deps = BTreeSet::new();
            deps.insert("model.project.model1".to_string());
            deps
        });

        // Add a unit_test
        deps.insert("unit_test.project.unit_test1".to_string(), {
            let mut deps = BTreeSet::new();
            deps.insert("model.project.model1".to_string());
            deps
        });

        // Create a topological sort
        let sorted_nodes = vec![
            "model.project.model1".to_string(),
            "test.project.test1".to_string(),
            "unit_test.project.unit_test1".to_string(),
        ];

        // All nodes are selected
        let selected_nodes: BTreeSet<String> = sorted_nodes.iter().cloned().collect();

        // All nodes are self-contained
        let self_contained_nodes = sorted_nodes.clone();

        // No frontier nodes
        let frontier_nodes = BTreeSet::new();

        let schedule = Schedule {
            deps,
            sorted_nodes,
            self_contained_nodes,
            selected_nodes,
            frontier_nodes,
            select: None,
            exclude: None,
            unused_nodes: BTreeSet::new(),
        };

        // Check that model1 is kept because test1 and unit_test1 depend on it
        assert!(schedule
            .sorted_nodes
            .contains(&"model.project.model1".to_string()));

        // Check that test1 is kept
        assert!(schedule
            .sorted_nodes
            .contains(&"test.project.test1".to_string()));

        assert!(schedule
            .sorted_nodes
            .contains(&"unit_test.project.unit_test1".to_string()));
    }
}
