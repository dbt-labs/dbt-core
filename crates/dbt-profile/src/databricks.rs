use crate::error::{ProfileError, Result};
use crate::resolve::{ProfileEnvironment, render_string, render_value_recursive};

pub(crate) fn render_value(
    key: &dbt_yaml::Value,
    value: &dbt_yaml::Value,
    penv: &ProfileEnvironment,
) -> Result<dbt_yaml::Value> {
    if key.as_str() != Some("query_tags") {
        return render_value_recursive(&penv.env, &penv.ctx, value);
    }

    if matches!(value, dbt_yaml::Value::Null(_)) {
        return Ok(value.clone());
    }

    render_string(&penv.env, &penv.ctx, value)?.ok_or_else(|| {
        ProfileError::Other("Databricks profile query_tags must be a JSON string".to_owned())
    })
}
