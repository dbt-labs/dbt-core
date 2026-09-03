//! `views.sql` generation.
//!
//! Driven off `INFO_SCHEMA`, so a table cannot be added without its view.

use std::fmt::Write;
use std::path::Path;

use crate::IndexError;

use super::schema::INFO_SCHEMA;
use super::spec::Ns;

/// The one derived view worth shipping: the latest result per node.
///
/// Compile-only error rows (`status = 'error'` with no execution time) are
/// excluded — they record a failed compile, not a node execution.
const RUN_RESULTS_LATEST: &str = "
CREATE OR REPLACE VIEW dbt_rt.run_results_latest AS
SELECT * FROM dbt_rt.run_results
WHERE NOT (status = 'error' AND execution_time = 0)
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY created_at DESC) = 1;
";

/// Write `views.sql` next to the parquet files.
pub fn write_views_sql(dir: &Path) -> Result<(), IndexError> {
    let mut sql = String::from(
        "-- dbt information schema. Generated; do not edit.\n\
         --\n\
         -- Query with:\n\
         --   duckdb -cmd \".read views.sql\"\n\
         --\n\
         -- Objects in dbt_internal are not part of the public contract and may\n\
         -- change without notice.\n\n",
    );

    for ns in Ns::ALL {
        writeln!(sql, "CREATE SCHEMA IF NOT EXISTS {};", ns.prefix()).unwrap();
    }
    sql.push('\n');

    for ns in Ns::ALL {
        for table in INFO_SCHEMA.iter().filter(|t| t.ns == *ns) {
            writeln!(
                sql,
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}');",
                table.qualified_name(),
                table.file_name(),
            )
            .unwrap();
        }
        sql.push('\n');
    }

    sql.push_str(RUN_RESULTS_LATEST);

    std::fs::write(dir.join("views.sql"), sql)?;
    Ok(())
}
