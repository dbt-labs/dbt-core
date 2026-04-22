use crate::{AdapterConfig, Auth, AuthError, AuthOutcome};
use dbt_xdbc::Backend;

pub struct AthenaAuth;

impl Auth for AthenaAuth {
    fn backend(&self) -> Backend {
        Backend::Athena
    }

    fn configure(&self, _config: &AdapterConfig) -> Result<AuthOutcome, AuthError> {
        Err(AuthError::config(
            "AthenaAuth::configure is not yet implemented; full implementation is tracked in the follow-up dbt-auth PR",
        ))
    }
}
