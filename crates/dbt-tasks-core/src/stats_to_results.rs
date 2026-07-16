use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use dbt_common::stats::Stat;
use dbt_schemas::schemas::nodes::Nodes;
use dbt_schemas::schemas::{ContextRunResult, TimingInfo};
use dbt_schemas::stats::Stats;

pub fn stats_to_results(stat: &Stat, stats: &Stats) -> ContextRunResult {
    let status = stat.result_status_string();
    let execution_time = stat.get_duration().as_secs_f64();
    let started_at: DateTime<Utc> = DateTime::from(stat.start_time);
    let completed_at: DateTime<Utc> = DateTime::from(stat.end_time);

    // TODO: Differentiate between compile and execute timing
    let timing = vec![
        TimingInfo {
            name: "compile".to_string(),
            started_at: Some(started_at),
            completed_at: Some(completed_at),
        },
        TimingInfo {
            name: "execute".to_string(),
            started_at: Some(started_at),
            completed_at: Some(completed_at),
        },
    ];

    let nodes = stats
        .nodes
        .as_ref()
        .expect("stats should have nodes for results generation");
    let node_arc = nodes.get_node_owned(&stat.unique_id);

    // Determine failures for tests
    let failures =
        if stat.unique_id.starts_with("test.") || stat.unique_id.starts_with("unit_test.") {
            stat.num_rows.map(|n| n as i64)
        } else {
            None
        };

    // Get static_analysis_off_reason from the node if available
    let static_analysis_off_reason = node_arc
        .as_ref()
        .and_then(|node| node.static_analysis_off_reason());

    let batch_results = stats.batch_results.get(&stat.unique_id).cloned();

    ContextRunResult {
        status,
        timing,
        thread_id: stat.thread_id.clone(),
        execution_time,
        adapter_response: {
            let mut map = BTreeMap::new();
            if let Some(ra) = stat.rows_affected {
                if let Ok(v) = dbt_yaml::to_value(ra) {
                    map.insert("rows_affected".to_string(), v);
                }
            }
            map
        },
        message: stat.message.clone(),
        failures,
        node: node_arc,
        unique_id: stat.unique_id.clone(),
        batch_results,
        static_analysis_off_reason,
    }
}

/// Simplified version for contexts that don't have full Stats (e.g., parquet metadata).
pub fn stat_to_result(stat: &Stat, nodes: &Nodes) -> ContextRunResult {
    let stats = Stats {
        stats: vec![],
        nodes: Some(nodes.clone()),
        batch_results: Default::default(),
    };
    stats_to_results(stat, &stats)
}
