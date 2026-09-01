use crate::hash::{NodeHashError, node_state_hashes};
use crate::proto::query_cache::{DbtNodeData, SelectorCriteria, SelectorRequest};
use crate::service_client::{RunCacheServiceError, SharedRunCacheServiceClient};
use crate::service_config::RunCacheServiceConfigError;
use dbt_common::path::DbtPath;
use dbt_schemas::schemas::{Nodes, macros::DbtMacro};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::LazyLock;

/// Inputs shared by requests to the run-cache state selector service.
///
/// This struct carries an already-initialized service client from the run-cache
/// lifecycle, ensuring that the selector path uses the same enablement and
/// fail-open policy as the rest of the run-cache integration.
#[derive(Clone)]
pub struct RunCacheStateSelectorArgs {
    pub client: SharedRunCacheServiceClient,
    pub defer_to: String,
    pub project_id: Option<String>,
    pub macros: BTreeMap<String, DbtMacro>,
    pub project_root: DbtPath,
}

static SELECTOR_CRITERIA_BY_SELECTOR: LazyLock<HashMap<&str, SelectorCriteria>> =
    LazyLock::new(|| {
        HashMap::from([
            ("new", SelectorCriteria::New),
            ("old", SelectorCriteria::Old),
            ("modified", SelectorCriteria::Modified),
            ("unmodified", SelectorCriteria::Unmodified),
            ("modified.body", SelectorCriteria::Body),
            ("modified.configs", SelectorCriteria::Configs),
            (
                "modified.persisted_descriptions",
                SelectorCriteria::PersistedDescriptions,
            ),
            ("modified.relation", SelectorCriteria::Relation),
            ("modified.macros", SelectorCriteria::Macros),
            ("modified.contract", SelectorCriteria::Contract),
        ])
    });

pub fn is_service_supported_state_selector(selector: &str) -> bool {
    parse_selector_criteria(selector).is_ok()
}

fn parse_selector_criteria(selector: &str) -> Result<SelectorCriteria, SelectorServiceError> {
    SELECTOR_CRITERIA_BY_SELECTOR.get(selector).copied().ok_or({
        let valid_selectors: Vec<_> = SELECTOR_CRITERIA_BY_SELECTOR.keys().copied().collect();
        SelectorServiceError::InvalidSelector(selector.to_string(), valid_selectors.join(", "))
    })
}

pub async fn evaluate_state_selector(
    nodes: &Nodes,
    args: &RunCacheStateSelectorArgs,
    selector: &str,
) -> Result<BTreeSet<String>, SelectorServiceError> {
    let project_id = args
        .project_id
        .as_deref()
        .ok_or(RunCacheServiceConfigError::ProjectIdRequired)?;

    let macro_resolver = |macro_id: &str| args.macros.get(macro_id);
    let mut node_data_list = Vec::new();
    for (unique_id, node) in nodes.iter() {
        let hashes = node_state_hashes(node, &args.project_root, macro_resolver)?;

        let database = node.database();
        let schema = node.schema();
        let alias = node.alias();
        let node_database_representation: Option<String> =
            if !database.is_empty() && !schema.is_empty() && !alias.is_empty() {
                Some(format!("{database}.{schema}.{alias}"))
            } else {
                None
            };
        node_data_list.push(DbtNodeData {
            node_unique_id: unique_id.clone(),
            node_hash: hashes.node_hash,
            node_body_hash: hashes.node_body_hash,
            node_configs_hash: hashes.node_configs_hash,
            node_persisted_descriptions_hash: hashes.node_persisted_descriptions_hash,
            node_macros_hash: hashes.node_macros_hash,
            node_contract_hash: hashes.node_contract_hash,
            node_database_representation,
        });
    }

    let criteria = parse_selector_criteria(selector)?;

    let request = SelectorRequest {
        target: args.defer_to.clone(),
        project_id: project_id.to_string(),
        selector_criteria: criteria as i32,
        nodes: node_data_list,
    };
    let response = args.client.get_state_selection(request).await?;

    Ok(response.node_unique_ids.into_iter().collect())
}

#[derive(Debug, thiserror::Error)]
pub enum SelectorServiceError {
    #[error("Invalid state selector {0}. Valid selectors are: {1}")]
    InvalidSelector(String, String),
    #[error("Hash calculation failed: {0}")]
    HashError(#[from] NodeHashError),
    #[error("Service error: {0}")]
    ServiceError(#[from] RunCacheServiceError),
    #[error("Config error: {0}")]
    ConfigError(#[from] RunCacheServiceConfigError),
}

#[cfg(test)]
mod tests {
    use super::{
        SelectorServiceError, is_service_supported_state_selector, parse_selector_criteria,
    };

    #[test]
    fn supports_only_the_service_backed_state_selector_values() {
        for selector in [
            "modified",
            "new",
            "old",
            "unmodified",
            "modified.body",
            "modified.contract",
            "modified.configs",
            "modified.relation",
            "modified.persisted_descriptions",
            "modified.macros",
        ] {
            assert!(is_service_supported_state_selector(selector));
        }

        for selector in ["modified.foo", "new.foo", "Modified", "state:modified"] {
            assert!(!is_service_supported_state_selector(selector));
        }
    }

    #[test]
    fn rejects_unsupported_state_selectors() {
        assert!(matches!(
            parse_selector_criteria("modified.foo"),
            Err(SelectorServiceError::InvalidSelector(selector, _)) if selector == "modified.foo"
        ));
    }
}
