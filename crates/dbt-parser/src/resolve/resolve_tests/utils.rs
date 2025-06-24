use std::collections::BTreeMap;

use dbt_common::{err, ErrorCode, FsResult};
use dbt_schemas::schemas::{data_tests::DataTests, dbt_column::ColumnProperties};

type DataTestVec = Vec<DataTests>;

pub fn base_tests_inner(
    tests: Option<&[DataTests]>,
    data_tests: Option<&[DataTests]>,
) -> FsResult<Option<DataTestVec>> {
    if tests.is_some() && data_tests.is_some() {
        return err!(
            ErrorCode::InvalidSchema,
            "Cannot have both 'tests' and 'data_tests' defined"
        );
    }
    if let Some(data_tests) = data_tests {
        Ok(Some(data_tests.to_vec()))
    } else if let Some(tests) = tests {
        Ok(Some(tests.to_vec()))
    } else {
        Ok(None) // Return an empty map if there are no tests
    }
}

pub fn column_tests_inner(
    columns: &Option<Vec<ColumnProperties>>,
) -> FsResult<Option<BTreeMap<String, (bool, DataTestVec)>>> {
    if columns.is_some()
        && columns
            .as_ref()
            .unwrap()
            .iter()
            .any(|col| col.tests.is_some() && col.data_tests.is_some())
    {
        return err!(
            ErrorCode::InvalidSchema,
            "Cannot have both 'tests' and 'data_tests' defined"
        );
    }
    if let Some(columns) = columns {
        let column_tests = columns
            .iter()
            .filter_map(|col| {
                // Check for both tests and data_tests, and handle them appropriately
                if col.tests.is_some() && col.data_tests.is_some() {
                    return None; // Error is handled above
                }
                col.tests.as_ref().or(col.data_tests.as_ref()).map(|tests| {
                    (
                        col.name.clone(),
                        (col.quote.unwrap_or(false), tests.clone()),
                    )
                })
            })
            .collect();
        Ok(Some(column_tests))
    } else {
        Ok(None) // Return an empty map if there are no columns
    }
}
