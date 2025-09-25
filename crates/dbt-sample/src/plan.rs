use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    vec,
};

use dbt_common::{ErrorCode, FsResult, fs_err};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SamplerPlan {
    // optional metadata
    pub version: Option<u32>,
    pub name: Option<String>,

    // keyset entries
    #[serde(default)]
    pub keysets: BTreeMap<String, Keyset>,

    // sampling entries
    #[serde(default)]
    pub entries: Vec<Entry>,

    // this exists after resolution of selectors, it should be the same as in entries
    #[serde(skip, default)]
    pub entry_unique_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplerRel {
    pub database: String,
    pub schema: String,
    pub identifier: String, // aka table, dbt naming convention
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keyset {
    pub cols: Vec<String>,
    pub from: KeyFrom,

    // this exists after resolution of selectors, it is the resolved relation
    pub read: Option<SamplerRel>,
    pub write: Option<SamplerRel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KeyFrom {
    Seed { seed: String },
    FromRoot { root: String, strategy: Strategy },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Entry {
    /// select expression (only sources for now)
    #[serde(default)]
    pub select: Option<String>,

    /// The strategy to apply to this entry
    pub strategy: Strategy,
    /// Optional join filter
    #[serde(default)]
    pub filters: Vec<Filter>,

    // this exists after resolution of selectors, it is the fqn of the source/ref/ext
    pub unique_id: Option<String>,
    pub read: Option<SamplerRel>,
    pub write: Option<SamplerRel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    #[serde(rename = "use")]
    pub use_key: String,
    pub on: Mapping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Mapping {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", content = "args", rename_all = "lowercase")]
pub enum Strategy {
    #[default]
    Clone,
    Pass,
    Random {
        frac: f64,
        seed: i64,
    },
    Limit {
        n: u64,
    },
    Time {
        by: String,
        last: Option<String>,
        start: Option<String>,
        end: Option<String>,
    },
    Hash {
        by: String,
        r#mod: u64,
        keep: u64,
        seed: i64,
    },
    Group {
        by: String,
        n_per_group: u64,
    },
    Stratified {
        by: String,
        frac: BTreeMap<String, f64>,
        seed: i64,
    },
    Branch {
        predicates: Vec<String>,
        alloc: BranchAlloc,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchAlloc {
    pub total: u64,
    pub weights: Vec<f64>,
}

impl Entry {
    pub fn pass(unique_id: String, read: SamplerRel, write: SamplerRel) -> Self {
        Entry {
            select: None,
            unique_id: Some(unique_id),
            read: Some(read),
            write: Some(write),
            strategy: Strategy::Pass {},
            filters: Vec::new(),
        }
    }
}

impl SamplerPlan {
    /// Returns a marker plan indicating the CLI requested the default behaviour.
    pub fn default_marker() -> Self {
        SamplerPlan {
            version: Some(1),
            name: Some("DEFAULT".to_string()),
            keysets: BTreeMap::new(),
            entries: Vec::new(),
            entry_unique_ids: BTreeSet::new(),
        }
    }

    pub fn to_macro_json(&self) -> FsResult<serde_json::Value> {
        serde_json::to_value(self).map_err(|e| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Failed to serialize sampler plan to JSON for macro: {}",
                e
            )
        })
    }
}

pub fn parse_yaml(input: &str) -> FsResult<SamplerPlan> {
    let mut plan: SamplerPlan = serde_yaml::from_str(input).map_err(|e| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Invalid sample plan (YAML): {}",
            e
        )
    })?;
    fill_defaults(&mut plan, None);
    validate(&plan)?;

    Ok(plan)
}

pub fn parse_json(input: &str) -> FsResult<SamplerPlan> {
    let mut plan: SamplerPlan = serde_json::from_str(input).map_err(|e| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Invalid sample plan (JSON): {}",
            e
        )
    })?;
    fill_defaults(&mut plan, None);
    validate(&plan)?;

    Ok(plan)
}

pub fn parse_file(path: &Path) -> FsResult<SamplerPlan> {
    let s = fs::read_to_string(path).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to read plan '{}': {}",
            path.display(),
            e
        )
    })?;
    let mut plan = if path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
    {
        parse_json(&s)?
    } else {
        parse_yaml(&s)?
    };
    fill_defaults(&mut plan, Some(path));
    validate(&plan)?;

    Ok(plan)
}

fn fill_defaults(plan: &mut SamplerPlan, path: Option<&Path>) {
    if plan.version.is_none() {
        plan.version = Some(1);
    }
    if plan
        .name
        .as_ref()
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        let default = match path.and_then(|p| p.file_stem().and_then(|s| s.to_str())) {
            Some(stem) if !stem.is_empty() => stem.to_string(),
            _ => "DEFAULT".to_string(),
        };
        plan.name = Some(default);
    }
}

fn validate(plan: &SamplerPlan) -> FsResult<()> {
    // Validate keys
    for (k, key) in &plan.keysets {
        if key.cols.is_empty() {
            return Err(fs_err!(
                ErrorCode::InvalidConfig,
                "Keyset '{}' must specify at least one column in 'cols'",
                k
            ));
        }
        // Strategy-specific requirements
        if let KeyFrom::FromRoot { strategy, .. } = &key.from {
            validate_strategy(strategy, Some(&format!("keys.{k}")))?;
        }
    }

    // Validate entries
    // Note: Empty entries means identity (no rewiring) and is allowed.
    for (idx, e) in plan.entries.iter().enumerate() {
        let has_name = e
            .unique_id
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        let has_select = e
            .select
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if has_name == has_select {
            // both true or both false
            return Err(fs_err!(
                ErrorCode::InvalidConfig,
                "entries[{}] must specify exactly one of 'name' or 'select'",
                idx
            ));
        }
        if has_name {
            let n = e.unique_id.as_ref().unwrap();
            if !(n.starts_with("src.") || n.starts_with("ref.") || n.starts_with("ext.")) {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "entries[{}].name must start with 'src.', 'ref.' or 'ext.'",
                    idx
                ));
            }
        }
        validate_strategy(&e.strategy, Some(&format!("entries[{idx}]",)))?;
        for (fidx, f) in e.filters.iter().enumerate() {
            if !plan.keysets.contains_key(&f.use_key) {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "entries[{idx}].filters[{fidx}].use references unknown key '{}': defined keys are {}",
                    f.use_key,
                    plan.keysets.keys().cloned().collect_vec().join(", ")
                ));
            }
            let mapping_pairs = normalize_mapping(&f.on)?;
            if mapping_pairs.is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "entries[{idx}].filters[{fidx}].on must contain at least one mapping",
                ));
            }
        }
    }

    Ok(())
}

fn validate_strategy(s: &Strategy, ctx: Option<&str>) -> FsResult<()> {
    let where_ = |field: &str| match ctx {
        Some(c) => format!("{c}.strategy.{field}"),
        None => field.to_string(),
    };
    match s {
        Strategy::Clone => Ok(()),
        Strategy::Pass => Ok(()),
        Strategy::Random { frac, seed: _ } => {
            if !(*frac > 0.0 && *frac <= 1.0) {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must have 0 < frac ≤ 1",
                    where_("random.frac")
                ));
            }
            Ok(())
        }
        Strategy::Limit { n } => {
            if *n == 0 {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must be > 0",
                    where_("limit.n")
                ));
            }
            Ok(())
        }
        Strategy::Time {
            by: col,
            last,
            start,
            end,
        } => {
            if col.trim().is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must be non-empty",
                    where_("time.col")
                ));
            }
            if last.is_none() && (start.is_none() || end.is_none()) {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must specify either 'last' or both 'start' and 'end'",
                    where_("time")
                ));
            }
            Ok(())
        }
        Strategy::Hash {
            by,
            r#mod,
            keep,
            seed: _,
        } => {
            if by.trim().is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must be non-empty",
                    where_("hash.by")
                ));
            }
            if *r#mod == 0 || *keep == 0 || keep > r#mod {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must satisfy 0 < keep ≤ mod and mod > 0",
                    where_("hash.{mod,keep}")
                ));
            }
            Ok(())
        }
        Strategy::Group { by, n_per_group } => {
            if by.trim().is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must be non-empty",
                    where_("group.by")
                ));
            }
            if *n_per_group == 0 {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must be > 0",
                    where_("group.n_per_group")
                ));
            }
            Ok(())
        }
        Strategy::Stratified { by, frac, seed: _ } => {
            if by.trim().is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must be non-empty",
                    where_("stratified.by")
                ));
            }
            if frac.is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must contain at least one group",
                    where_("stratified.frac")
                ));
            }
            if frac.values().any(|v| !(*v > 0.0 && *v <= 1.0)) {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} entries must satisfy 0 < frac ≤ 1",
                    where_("stratified.frac")
                ));
            }
            Ok(())
        }
        Strategy::Branch { predicates, alloc } => {
            if predicates.is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must have at least one predicate",
                    where_("branch.predicates")
                ));
            }
            if alloc.weights.is_empty() {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must have at least one weight",
                    where_("branch.alloc.weights")
                ));
            }
            let sum: f64 = alloc.weights.iter().sum();
            if sum <= 0.0 {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} weights must sum to > 0",
                    where_("branch.alloc.weights")
                ));
            }
            if alloc.total == 0 {
                return Err(fs_err!(
                    ErrorCode::InvalidConfig,
                    "{} must be > 0",
                    where_("branch.alloc.total")
                ));
            }
            Ok(())
        }
    }
}

pub fn normalize_mapping(m: &Mapping) -> FsResult<Vec<(String, String)>> {
    let to_pair = |s: &str| -> FsResult<(String, String)> {
        let s = s.trim();
        if s.is_empty() {
            return Err(fs_err!(
                ErrorCode::InvalidConfig,
                "Empty mapping entry in 'on'"
            ));
        }
        if let Some((a, b)) = s.split_once("->") {
            Ok((a.trim().to_string(), b.trim().to_string()))
        } else {
            // shorthand a === a->a
            Ok((s.to_string(), s.to_string()))
        }
    };
    match m {
        Mapping::One(s) => Ok(vec![to_pair(s)?]),
        Mapping::Many(v) => {
            let mut res = Vec::with_capacity(v.len());
            for s in v {
                res.push(to_pair(s)?);
            }
            Ok(res)
        }
    }
}

pub fn write_as(database: &str, schema: &str, identifier: &str, suffix: &str) -> SamplerRel {
    SamplerRel {
        database: database.to_string(),
        schema: format!("{schema}{suffix}"),
        identifier: identifier.to_string(),
    }
}

/// Create a map for remapping unique_id to (database, schema, table) when sampling is enabled and read and write are populated
pub fn sample_renaming_map(plan: &SamplerPlan) -> BTreeMap<String, (String, String, String)> {
    let mut map = BTreeMap::new();
    for entry in &plan.entries {
        if let (Some(eid), Some(read), Some(write)) = (
            entry.unique_id.as_ref(),
            entry.read.as_ref(),
            entry.write.as_ref(),
        ) && (read.database != write.database
            || read.schema != write.schema
            || read.identifier != write.identifier)
        {
            map.insert(
                eid.clone(),
                (
                    write.database.clone(),
                    write.schema.clone(),
                    write.identifier.clone(),
                ),
            );
        }
    }
    map
}
