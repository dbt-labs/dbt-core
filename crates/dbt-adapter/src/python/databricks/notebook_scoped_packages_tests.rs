use super::{build_libraries, prepare_code_with_notebook_scoped_packages};
use serde_json::json;

const SEPARATOR: &str = "\n\n# COMMAND ----------\n\n";

#[test]
fn notebook_scoped_packages_prepend_install_restart_and_preserve_package_order() {
    let packages = vec![
        "pandas".to_string(),
        "numpy==1.24.0".to_string(),
        "scikit-learn>=1.0".to_string(),
    ];

    let actual = prepare_code_with_notebook_scoped_packages("compiled_code", &packages, true, None);

    assert_eq!(
        actual,
        [
            "%pip install -q pandas numpy==1.24.0 scikit-learn>=1.0",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        .join(SEPARATOR)
    );
}

#[test]
fn notebook_scoped_packages_render_index_url_before_quiet_flag() {
    let packages = vec!["pandas".to_string()];

    let actual = prepare_code_with_notebook_scoped_packages(
        "compiled_code",
        &packages,
        true,
        Some("https://example.com/pypi/simple"),
    );

    assert_eq!(
        actual,
        [
            "%pip install --index-url https://example.com/pypi/simple -q pandas",
            "dbutils.library.restartPython()",
            "compiled_code",
        ]
        .join(SEPARATOR)
    );
}

#[test]
fn notebook_scoped_packages_leave_code_unchanged_when_disabled_or_empty() {
    let packages = vec!["pandas".to_string()];
    assert_eq!(
        prepare_code_with_notebook_scoped_packages("compiled_code", &packages, false, None),
        "compiled_code"
    );
    assert_eq!(
        prepare_code_with_notebook_scoped_packages("compiled_code", &[], true, None),
        "compiled_code"
    );
}

#[test]
fn notebook_scoped_packages_are_excluded_from_job_libraries_but_additional_libs_remain() {
    let packages = vec!["package1".to_string(), "package2".to_string()];
    let additional = vec![json!({"jar": "s3://mybucket/myjar.jar"})];

    assert_eq!(
        build_libraries(&packages, None, &additional, true),
        additional
    );
    assert_eq!(
        build_libraries(&packages, None, &[], false),
        vec![
            json!({"pypi": {"package": "package1"}}),
            json!({"pypi": {"package": "package2"}}),
        ]
    );
}
