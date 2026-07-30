use std::collections::{BTreeMap, BTreeSet};

/// Databricks ADBC Connection Options
/// Referenced from: github.com/dbt-labs/arrow-adbc/go/driver/databricks/driver.go
/// Authentication type options
pub const AUTH_TYPE: &str = "databricks.auth_type";

pub mod auth_type {
    /// OAuth M2M authentication
    pub const OAUTH_M2M: &str = "oauth-m2m";
    /// Personal Access Token authentication
    pub const PAT: &str = "pat";
    /// External Browser authentication
    pub const EXTERNAL_BROWSER: &str = "external-browser";
    /// Azure service principal (Microsoft Entra ID) authentication
    pub const AZURE_CLIENT_SECRET: &str = "azure-client-secret";
}

/// HTTP Path to connect
pub const HTTP_PATH: &str = "databricks.http_path";

/// Optional default catalog to use when executing SQL statements
pub const CATALOG: &str = "databricks.catalog";
/// Optional default schema to use when executing SQL statements
pub const SCHEMA: &str = "databricks.schema";

/// Databricks host (either of workspace endpoint or Accounts API endpoint)
pub const HOST: &str = "databricks.server_hostname";

/// Databricks token
pub const TOKEN: &str = "databricks.access_token";

/// The Databricks service principal's client ID
pub const CLIENT_ID: &str = "databricks.oauth.client_id";
/// The Databricks service principal's client secret
pub const CLIENT_SECRET: &str = "databricks.oauth.client_secret";
/// Timeout for U2M OAuth
pub const OAUTH_TIMEOUT: &str = "databricks.oauth.external_browser.timeout";

/// The Azure service principal's (Microsoft Entra ID) client ID
pub const AZURE_CLIENT_ID: &str = "databricks.azure.client_id";
/// The Azure service principal's (Microsoft Entra ID) client secret
pub const AZURE_CLIENT_SECRET: &str = "databricks.azure.client_secret";
/// The Azure tenant ID (optional; discovered from the workspace when absent)
pub const AZURE_TENANT_ID: &str = "databricks.azure.tenant_id";

/// TLS/SSL options
pub const SSL_MODE: &str = "databricks.ssl_mode";
pub const SSL_ROOT_CERT: &str = "databricks.ssl_root_cert";

/// User agent string for dbt attribution by databricks
pub const USER_AGENT: &str = "databricks.user_agent";

/// Query tags applied when the Databricks session is created.
pub const QUERY_TAGS: &str = "databricks.session_param.QUERY_TAGS";

pub(crate) fn query_tags_update_sql(
    current: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
) -> Option<String> {
    let keys = current
        .keys()
        .chain(desired.keys())
        .collect::<BTreeSet<_>>();
    let assignments = keys
        .into_iter()
        .filter_map(|key| {
            let current_value = current.get(key);
            let desired_value = desired.get(key);
            if current_value == desired_value {
                return None;
            }

            let key = quote_query_tag_literal(key);
            Some(match desired_value {
                Some(value) => {
                    format!("QUERY_TAGS[{key}] = {}", quote_query_tag_literal(value))
                }
                None => format!("QUERY_TAGS[{key}] = UNSET"),
            })
        })
        .collect::<Vec<_>>();

    (!assignments.is_empty()).then(|| format!("SET {}", assignments.join(", ")))
}

fn quote_query_tag_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', r"\\").replace('\'', r"\'"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::query_tags_update_sql;

    #[test]
    fn query_tags_update_sets_changed_values_and_unsets_stale_keys() {
        let current = BTreeMap::from([
            ("keep".to_string(), "same".to_string()),
            ("remove".to_string(), "old".to_string()),
            ("update".to_string(), "old".to_string()),
        ]);
        let desired = BTreeMap::from([
            ("add".to_string(), "new".to_string()),
            ("keep".to_string(), "same".to_string()),
            ("update".to_string(), "new".to_string()),
        ]);

        assert_eq!(
            query_tags_update_sql(&current, &desired).as_deref(),
            Some(
                "SET QUERY_TAGS['add'] = 'new', QUERY_TAGS['remove'] = UNSET, \
                 QUERY_TAGS['update'] = 'new'"
            )
        );
    }

    #[test]
    fn query_tags_update_is_noop_when_tags_are_unchanged() {
        let tags = BTreeMap::from([("key".to_string(), "value".to_string())]);
        assert_eq!(query_tags_update_sql(&tags, &tags), None);
    }

    #[test]
    fn query_tags_update_quotes_sql_literals() {
        let desired = BTreeMap::from([
            ("quote'key".to_string(), "a'b\\c".to_string()),
            ("unicode".to_string(), "हैलो".to_string()),
        ]);

        assert_eq!(
            query_tags_update_sql(&BTreeMap::new(), &desired).as_deref(),
            Some(
                "SET QUERY_TAGS['quote\\'key'] = 'a\\'b\\\\c', \
                 QUERY_TAGS['unicode'] = 'हैलो'"
            )
        );
    }
}
