//! This mod is not used currently, but we'll bring this back when we can let the guard to use TypedBaseAdapter instead, then we can remove the adapter.restore_warehouse
//! This requires us to move the ConnectionGuard to TypedBaseAdapter
//! More details see https://github.com/dbt-labs/fs/pull/4039#discussion_r2159864154

use crate::{AdapterType, AdapterTyping, BridgeAdapter};

use dbt_common::{FsError, FsResult};
use dbt_xdbc::QueryCtx;

pub struct UseWarehouseGuard<'a> {
    adapter: &'a BridgeAdapter,
    original_wh: String,
    node_id: String,
}

impl<'a> UseWarehouseGuard<'a> {
    pub fn new(adapter: &'a BridgeAdapter, original_wh: String, node_id: &str) -> Self {
        Self {
            adapter,
            original_wh,
            node_id: node_id.to_string(),
        }
    }
}

impl Drop for UseWarehouseGuard<'_> {
    fn drop(&mut self) {
        if self.adapter.adapter_type() == AdapterType::Snowflake {
            // This is best effort
            let _ = use_warehouse_inner(self.adapter, &self.original_wh, &self.node_id);
        }
    }
}

pub fn use_warehouse_inner(
    adapter: &BridgeAdapter,
    warehouse: &str,
    node_id: &str,
) -> FsResult<()> {
    let mut conn = adapter
        .borrow_tlocal_connection()
        .map_err(|e| FsError::from_jinja_err(e, "Failed to create a connection"))?;

    let query_ctx = QueryCtx::new(adapter.adapter_type().to_string())
        .with_sql(format!("use warehouse {warehouse}"))
        .with_node_id(node_id);
    adapter
        .typed_adapter
        .exec_stmt(conn.as_mut(), &query_ctx, false)?;
    Ok(())
}
