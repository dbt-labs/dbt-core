//! Author-side agent specs and the v0 "manifest-adjacent" plumbing.
//!
//! Agents are declared in `<project>/agents/**/*.yml` and pushed to a Fivetran AI
//! MCP contextset by the `dbt agent deploy` command. In v0 they are not first-class
//! nodes in the manifest — the parser here reads the YAML, resolves selectors
//! against an already-produced `target/manifest.json`, and emits a companion
//! `target/agents.json` artifact.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};

use dbt_common::{ErrorCode, FsResult, fs_err};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

/// Top-level container matching the `agents:` list in a YAML file.
#[derive(Debug, Clone, Deserialize)]
pub struct AgentsFile {
    #[serde(default)]
    pub agents: Vec<AgentSpec>,
}

/// One agent as authored in YAML.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentSpec {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// `public` | `protected` | `private`. Metadata only in v0.
    #[serde(default)]
    pub access: Option<String>,
    pub scope: AgentScope,
    #[serde(default)]
    pub tools: Option<AgentTools>,
    /// Name of an mcp-server entry. Metadata only in v0 — the deploy command
    /// receives its URL/token/group_id from CLI flags or environment variables.
    #[serde(default)]
    pub mcp_server: Option<String>,
}

/// Node-selector expressions defining the agent's reachable scope.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentScope {
    /// Union of dbt selector expressions (e.g. `group:sales,tag:certified`, `dim_customers+`).
    pub select: Vec<String>,
    #[serde(default)]
    pub exclude: Option<Vec<String>>,
}

/// Tool allowlist / denylist. Accepted in v0 but not enforced server-side.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentTools {
    #[serde(default)]
    pub include: Option<Vec<String>>,
    #[serde(default)]
    pub exclude: Option<Vec<String>>,
}

/// An agent plus its resolved warehouse scope, ready for deploy.
#[derive(Debug, Clone, Serialize)]
pub struct ResolvedAgent {
    #[serde(flatten)]
    pub spec: AgentSpec,
    pub resolved_scope: ResolvedScope,
}

/// The concrete warehouse targets the agent is allowed to touch.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ResolvedScope {
    pub schema_fqns: Vec<String>,
    pub table_fqns: Vec<String>,
    pub model_unique_ids: Vec<String>,
}

/// Walk `<project>/agents/` recursively and load every `*.yml` / `*.yaml` file.
///
/// Returns the discovered specs in the order encountered. Fails if two specs share
/// a `name`.
pub fn discover_agent_specs(project_dir: &Path) -> FsResult<Vec<AgentSpec>> {
    let agents_dir = project_dir.join("agents");
    if !agents_dir.exists() {
        return Ok(Vec::new());
    }

    let mut specs: Vec<AgentSpec> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for entry in WalkDir::new(&agents_dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !matches!(ext, "yml" | "yaml") {
            continue;
        }
        let file = load_agents_file(path)?;
        for spec in file.agents {
            validate_spec(&spec, path)?;
            if !seen.insert(spec.name.clone()) {
                return Err(fs_err!(
                    ErrorCode::Generic,
                    "Duplicate agent name '{}' (found again in {})",
                    spec.name,
                    path.display()
                ));
            }
            specs.push(spec);
        }
    }

    Ok(specs)
}

fn load_agents_file(path: &Path) -> FsResult<AgentsFile> {
    let content = std::fs::read_to_string(path).map_err(|err| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to read agent file {}: {}",
            path.display(),
            err
        )
    })?;
    dbt_yaml::from_str::<AgentsFile>(&content).map_err(|err| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to parse agent file {}: {}",
            path.display(),
            err
        )
    })
}

fn validate_spec(spec: &AgentSpec, path: &Path) -> FsResult<()> {
    if spec.name.trim().is_empty() {
        return Err(fs_err!(
            ErrorCode::Generic,
            "Agent in {} is missing a name",
            path.display()
        ));
    }
    if spec.scope.select.is_empty() {
        return Err(fs_err!(
            ErrorCode::Generic,
            "Agent '{}' in {} has an empty scope.select — at least one selector is required",
            spec.name,
            path.display()
        ));
    }
    if let Some(access) = &spec.access
        && !matches!(access.as_str(), "public" | "protected" | "private")
    {
        return Err(fs_err!(
            ErrorCode::Generic,
            "Agent '{}' has invalid access '{}' (expected public|protected|private)",
            spec.name,
            access
        ));
    }
    Ok(())
}

/// Convenience: the default artifact path for `target/agents.json`.
pub fn agents_json_path(target_dir: &Path) -> PathBuf {
    target_dir.join("agents.json")
}

// ------------------------------------------------------------------------------------------------
// Selector resolution
//
// v0 supports a pragmatic subset of dbt selector syntax against a deserialized manifest.json:
//
//   - bare name          `dim_customers`
//   - resource type      `resource_type:model`
//   - tag / group        `tag:certified`, `group:sales`
//   - graph ops          `+name`, `name+`, `+N+name`, `name+N`, `+N+name+M` (numeric depths)
//   - `,` inside a single expression is intersection (AND)
//   - each entry in `scope.select` (or `scope.exclude`) is a separate expression (OR / subtract)
//
// v0 explicitly does *not* implement: `state:`, `source:`, `test:`, `path:`, wildcard globbing,
// version modifiers, or the full parenthesized boolean grammar. The demo project stays inside
// the supported subset.

/// A lean, forgiving projection of `manifest.json` — just the fields the agent
/// resolver needs. Deliberately does not use `DbtManifestV11` from `dbt-schemas`,
/// because that shape requires internal fields (e.g. `__other__`) that don't
/// appear in the serialized artifact.
#[derive(Debug, Deserialize)]
pub struct ManifestView {
    #[serde(default)]
    pub nodes: BTreeMap<String, ManifestNode>,
    #[serde(default)]
    pub parent_map: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub child_map: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestNode {
    /// dbt writes `resource_type` on every node. We only care about `"model"` for v0.
    #[serde(default)]
    pub resource_type: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub schema: String,
    #[serde(default)]
    pub relation_name: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub config: Option<ManifestNodeConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestNodeConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub tags: Option<serde_json::Value>,
    #[serde(default)]
    pub group: Option<String>,
}

impl ManifestNode {
    fn is_model(&self) -> bool {
        self.resource_type == "model"
    }

    fn effective_group(&self) -> Option<&str> {
        self.group
            .as_deref()
            .or_else(|| self.config.as_ref().and_then(|c| c.group.as_deref()))
    }

    fn has_tag(&self, tag: &str) -> bool {
        if self.tags.iter().any(|t| t == tag) {
            return true;
        }
        match self.config.as_ref().and_then(|c| c.tags.as_ref()) {
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .any(|t| t == tag),
            Some(serde_json::Value::String(s)) => s == tag,
            _ => false,
        }
    }

    fn is_enabled(&self) -> bool {
        self.config
            .as_ref()
            .and_then(|c| c.enabled)
            .unwrap_or(true)
    }
}

/// Load and deserialize `target/manifest.json` into the lean view.
pub fn load_manifest(manifest_path: &Path) -> FsResult<ManifestView> {
    let content = std::fs::read_to_string(manifest_path).map_err(|err| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to read manifest at {}: {} (run `dbt parse` or `dbt compile` first)",
            manifest_path.display(),
            err
        )
    })?;
    serde_json::from_str::<ManifestView>(&content).map_err(|err| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to parse manifest at {}: {}",
            manifest_path.display(),
            err
        )
    })
}

/// Resolve every agent spec against a loaded manifest, producing warehouse-level scope.
pub fn resolve_all(
    specs: Vec<AgentSpec>,
    manifest: &ManifestView,
) -> FsResult<Vec<ResolvedAgent>> {
    specs
        .into_iter()
        .map(|spec| {
            let resolved_scope = resolve_scope(&spec, manifest)?;
            Ok(ResolvedAgent {
                spec,
                resolved_scope,
            })
        })
        .collect()
}

fn resolve_scope(spec: &AgentSpec, manifest: &ManifestView) -> FsResult<ResolvedScope> {
    let mut included: BTreeSet<String> = BTreeSet::new();
    for expr in &spec.scope.select {
        let matched = evaluate_expression(expr, manifest).map_err(|err| {
            fs_err!(
                ErrorCode::Generic,
                "Agent '{}': failed to resolve select expression `{}`: {}",
                spec.name,
                expr,
                err
            )
        })?;
        included.extend(matched);
    }

    if let Some(exclude_exprs) = &spec.scope.exclude {
        for expr in exclude_exprs {
            let matched = evaluate_expression(expr, manifest).map_err(|err| {
                fs_err!(
                    ErrorCode::Generic,
                    "Agent '{}': failed to resolve exclude expression `{}`: {}",
                    spec.name,
                    expr,
                    err
                )
            })?;
            for id in matched {
                included.remove(&id);
            }
        }
    }

    let mut table_fqns: BTreeSet<String> = BTreeSet::new();
    let mut schema_fqns: BTreeSet<String> = BTreeSet::new();
    let mut model_unique_ids: Vec<String> = Vec::new();

    for unique_id in &included {
        let Some(node) = manifest.nodes.get(unique_id) else {
            continue;
        };
        if !node.is_model() || !node.is_enabled() {
            continue;
        }
        let database = node.database.trim();
        let schema = node.schema.trim();
        let name = node.name.trim();
        let relation = node
            .relation_name
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        let has_relation = relation.is_some()
            || (!database.is_empty() && !schema.is_empty() && !name.is_empty());
        if !has_relation {
            // ephemeral / not-materialized-to-a-relation nodes are skipped.
            continue;
        }
        model_unique_ids.push(unique_id.clone());
        if let Some(r) = relation {
            table_fqns.insert(strip_quotes(r));
        } else {
            table_fqns.insert(format!("{}.{}.{}", database, schema, name));
        }
        if !database.is_empty() && !schema.is_empty() {
            schema_fqns.insert(format!("{}.{}", database, schema));
        }
    }

    if table_fqns.is_empty() {
        return Err(fs_err!(
            ErrorCode::Generic,
            "Agent '{}': scope resolved to zero deployable models",
            spec.name
        ));
    }

    Ok(ResolvedScope {
        schema_fqns: schema_fqns.into_iter().collect(),
        table_fqns: table_fqns.into_iter().collect(),
        model_unique_ids,
    })
}

/// Evaluate a single dbt-style selector expression (with optional `,`-intersect and graph ops).
fn evaluate_expression(expr: &str, manifest: &ManifestView) -> FsResult<BTreeSet<String>> {
    let mut term_results: Vec<BTreeSet<String>> = Vec::new();
    for term in expr.split(',') {
        let term = term.trim();
        if term.is_empty() {
            continue;
        }
        term_results.push(evaluate_term(term, manifest)?);
    }
    if term_results.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut iter = term_results.into_iter();
    let mut acc = iter.next().unwrap();
    for next in iter {
        acc = acc.intersection(&next).cloned().collect();
    }
    Ok(acc)
}

fn evaluate_term(term: &str, manifest: &ManifestView) -> FsResult<BTreeSet<String>> {
    let (leading_parents, core, trailing_children) = strip_graph_ops(term);

    let base_ids = if let Some((method, value)) = core.split_once(':') {
        match method {
            "resource_type" => match_by_resource_type(value, manifest),
            "tag" => match_by_tag(value, manifest),
            "group" => match_by_group(value, manifest),
            "name" => match_by_name(value, manifest),
            other => {
                return Err(fs_err!(
                    ErrorCode::Generic,
                    "Selector method `{}:` is not supported in v0",
                    other
                ));
            }
        }
    } else {
        match_by_name(core, manifest)
    };

    let mut result = base_ids.clone();
    if let Some(depth) = leading_parents {
        for id in &base_ids {
            expand_ancestors(id, depth, &manifest.parent_map, &mut result);
        }
    }
    if let Some(depth) = trailing_children {
        for id in &base_ids {
            expand_descendants(id, depth, &manifest.child_map, &mut result);
        }
    }
    Ok(result)
}

/// Parse dbt-style graph ops from a selector term.
///
/// Returns `(leading_parents, core, trailing_children)` where:
/// - `Some(None)` = unbounded (`+name` or `name+`)
/// - `Some(Some(n))` = bounded depth (`N+name` or `name+N`)
/// - `None` = no graph op on that side
#[allow(clippy::type_complexity)]
fn strip_graph_ops(term: &str) -> (Option<Option<usize>>, &str, Option<Option<usize>>) {
    let mut s = term;
    let mut leading: Option<Option<usize>> = None;
    let mut trailing: Option<Option<usize>> = None;

    // Leading: `<digits>+` (bounded) or plain `+` (unbounded)
    let (n_leading, after_digits) = leading_number(s);
    if let Some(rest) = after_digits.strip_prefix('+') {
        leading = Some(n_leading);
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        leading = Some(None);
        s = rest;
    }

    // Trailing: `+<digits>` (bounded) or trailing `+` (unbounded)
    let bytes = s.as_bytes();
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1].is_ascii_digit() {
        end -= 1;
    }
    if end < bytes.len() && end > 0 && bytes[end - 1] == b'+' {
        let depth: usize = s[end..].parse().unwrap_or(0);
        trailing = Some(Some(depth));
        s = &s[..end - 1];
    } else if let Some(rest) = s.strip_suffix('+') {
        trailing = Some(None);
        s = rest;
    }

    (leading, s, trailing)
}

fn leading_number(s: &str) -> (Option<usize>, &str) {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 {
        (None, s)
    } else {
        let n: usize = s[..i].parse().unwrap_or(0);
        (Some(n), &s[i..])
    }
}

fn match_by_name(name: &str, manifest: &ManifestView) -> BTreeSet<String> {
    manifest
        .nodes
        .iter()
        .filter_map(|(uid, node)| {
            if node.is_model() && node.name == name {
                Some(uid.clone())
            } else {
                None
            }
        })
        .collect()
}

fn match_by_resource_type(value: &str, manifest: &ManifestView) -> BTreeSet<String> {
    manifest
        .nodes
        .iter()
        .filter_map(|(uid, node)| {
            if node.resource_type == value {
                Some(uid.clone())
            } else {
                None
            }
        })
        .collect()
}

fn match_by_tag(tag: &str, manifest: &ManifestView) -> BTreeSet<String> {
    manifest
        .nodes
        .iter()
        .filter_map(|(uid, node)| {
            if node.is_model() && node.has_tag(tag) {
                Some(uid.clone())
            } else {
                None
            }
        })
        .collect()
}

fn match_by_group(group: &str, manifest: &ManifestView) -> BTreeSet<String> {
    manifest
        .nodes
        .iter()
        .filter_map(|(uid, node)| {
            if node.is_model() && node.effective_group() == Some(group) {
                Some(uid.clone())
            } else {
                None
            }
        })
        .collect()
}

fn expand_ancestors(
    start: &str,
    depth: Option<usize>,
    parent_map: &BTreeMap<String, Vec<String>>,
    out: &mut BTreeSet<String>,
) {
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    queue.push_back((start.to_string(), 0));
    while let Some((id, d)) = queue.pop_front() {
        if let Some(limit) = depth
            && d >= limit
        {
            continue;
        }
        if let Some(parents) = parent_map.get(&id) {
            for p in parents {
                if out.insert(p.clone()) {
                    queue.push_back((p.clone(), d + 1));
                }
            }
        }
    }
}

fn expand_descendants(
    start: &str,
    depth: Option<usize>,
    child_map: &BTreeMap<String, Vec<String>>,
    out: &mut BTreeSet<String>,
) {
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    queue.push_back((start.to_string(), 0));
    while let Some((id, d)) = queue.pop_front() {
        if let Some(limit) = depth
            && d >= limit
        {
            continue;
        }
        if let Some(children) = child_map.get(&id) {
            for c in children {
                if out.insert(c.clone()) {
                    queue.push_back((c.clone(), d + 1));
                }
            }
        }
    }
}

fn strip_quotes(fqn: &str) -> String {
    fqn.replace(['"', '`'], "")
}
