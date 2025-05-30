use dbt_schemas::schemas::common::{Constraint, ConstraintType};

use crate::adapters::base_adapter::AdapterType;

/// Render the given constraint as DDL text. Should be overridden by adapters which need custom constraint
/// default: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1849-L1850
/// bigquery override: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L958-L959
pub fn render_model_constraint(
    adapter_type: AdapterType,
    constraint: Constraint,
) -> Option<String> {
    let constraint_prefix = if let Some(name) = constraint.name {
        format!("constraint {} ", name)
    } else {
        String::new()
    };

    let column_list = constraint.columns.unwrap_or_default().join(", ");
    let rendered = match constraint.type_ {
        ConstraintType::Check => constraint
            .expression
            .map(|expr| format!("{}check ({})", constraint_prefix, expr)),
        ConstraintType::Unique => {
            let expr = constraint
                .expression
                .map_or(String::new(), |e| format!(" {}", e));
            Some(format!(
                "{}unique{} ({})",
                constraint_prefix, expr, column_list
            ))
        }
        ConstraintType::PrimaryKey => {
            let expr = constraint
                .expression
                .map_or(String::new(), |e| format!(" {}", e));
            Some(format!(
                "{}primary key{} ({})",
                constraint_prefix, expr, column_list
            ))
        }
        ConstraintType::ForeignKey => {
            if let (Some(to), Some(to_columns)) = (constraint.to, constraint.to_columns) {
                Some(format!(
                    "{}foreign key ({}) references {} ({})",
                    constraint_prefix,
                    column_list,
                    to,
                    to_columns.join(", ")
                ))
            } else {
                constraint.expression.map(|expr| {
                    format!(
                        "{}foreign key ({}) references {}",
                        constraint_prefix, column_list, expr
                    )
                })
            }
        }
        ConstraintType::Custom => constraint
            .expression
            .map(|expr| format!("{}{}", constraint_prefix, expr)),
        ConstraintType::NotNull => None,
    };

    rendered.and_then(|rendered| {
        if adapter_type == AdapterType::Bigquery
            && (constraint.type_ == ConstraintType::PrimaryKey
                || constraint.type_ == ConstraintType::ForeignKey)
        {
            Some(format!("{} not enforced", rendered))
        } else if adapter_type == AdapterType::Bigquery {
            None
        } else {
            Some(rendered)
        }
    })
}

/// Render the given constraint as DDL text. Should be overridden by adapters which need custom constraint
/// default: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-adapters/src/dbt/adapters/base/impl.py#L1751-L1752
/// bigquery override: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L958-L959
pub fn render_column_constraint(
    adapter_type: AdapterType,
    constraint: Constraint,
) -> Option<String> {
    let constraint_expression = constraint.expression.unwrap_or_default();

    let rendered = match constraint.type_ {
        ConstraintType::Check if !constraint_expression.is_empty() => {
            Some(format!("check ({})", constraint_expression))
        }
        ConstraintType::NotNull => Some(format!("not null {}", constraint_expression)),
        ConstraintType::Unique => Some(format!("unique {}", constraint_expression)),
        ConstraintType::PrimaryKey => Some(format!("primary key {}", constraint_expression)),
        ConstraintType::ForeignKey => {
            if let (Some(to), Some(to_columns)) = (constraint.to, constraint.to_columns) {
                Some(format!("references {} ({})", to, to_columns.join(", ")))
            } else if !constraint_expression.is_empty() {
                Some(format!("references {}", constraint_expression))
            } else {
                None
            }
        }
        ConstraintType::Custom if !constraint_expression.is_empty() => Some(constraint_expression),
        _ => None,
    };

    rendered.and_then(|r| {
        if adapter_type == AdapterType::Bigquery
            && (constraint.type_ == ConstraintType::PrimaryKey
                || constraint.type_ == ConstraintType::ForeignKey)
        {
            Some(format!("{} not enforced", r))
        } else if adapter_type == AdapterType::Bigquery {
            None
        } else {
            Some(r.trim().to_string())
        }
    })
}
