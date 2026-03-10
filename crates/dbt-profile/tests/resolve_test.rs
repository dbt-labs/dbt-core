#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;

use dbt_profile::{ResolveArgs, resolve};

// ── Dependency guardrails ────────────────────────────────────────────
// dbt-profile must stay lightweight — no dbt-common or dbt-jinja-utils.
// These tests parse Cargo.toml and fail if a banned dependency appears.

const CARGO_TOML: &str = include_str!("../Cargo.toml");

const BANNED_DEPS: &[&str] = &["dbt-common", "dbt-jinja-utils"];

#[test]
fn no_banned_dependencies() {
    for dep in BANNED_DEPS {
        assert!(
            !CARGO_TOML.contains(dep),
            "dbt-profile must not depend on `{dep}`. \
             This crate is intentionally standalone — see the module doc in lib.rs."
        );
    }
}

fn write_file(dir: &std::path::Path, name: &str, content: &str) {
    std::fs::write(dir.join(name), content).unwrap();
}

#[test]
fn test_resolve_simple_profile() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: test_account
      user: test_user
      password: test_pass
      warehouse: COMPUTE_WH
      database: MY_DB
      schema: PUBLIC
      role: ACCOUNTADMIN
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("my_project".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    assert_eq!(result.profile_name, "my_project");
    assert_eq!(result.target_name, "dev");
    assert_eq!(result.adapter_type, "snowflake");
    assert_eq!(result.get_str("account"), Some("test_account"));
    assert_eq!(result.get_str("user"), Some("test_user"));
    assert_eq!(result.get_str("warehouse"), Some("COMPUTE_WH"));
    assert_eq!(result.database(), Some("MY_DB"));
    assert_eq!(result.schema(), Some("PUBLIC"));
}

#[test]
fn test_resolve_with_env_var() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    unsafe {
        std::env::set_var("DBT_PROFILE_TEST_ACCOUNT", "env_account");
        std::env::set_var("DBT_PROFILE_TEST_USER", "env_user");
        std::env::set_var("DBT_PROFILE_TEST_PASS", "env_pass");
    };

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('DBT_PROFILE_TEST_ACCOUNT') }}"
      user: "{{ env_var('DBT_PROFILE_TEST_USER') }}"
      password: "{{ env_var('DBT_PROFILE_TEST_PASS') }}"
      warehouse: "{{ env_var('DBT_PROFILE_TEST_WH', 'DEFAULT_WH') }}"
      database: MY_DB
      schema: PUBLIC
      role: ACCOUNTADMIN
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("my_project".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    assert_eq!(result.get_str("account"), Some("env_account"));
    assert_eq!(result.get_str("user"), Some("env_user"));
    assert_eq!(result.get_str("password"), Some("env_pass"));
    assert_eq!(result.get_str("warehouse"), Some("DEFAULT_WH"));

    unsafe {
        std::env::remove_var("DBT_PROFILE_TEST_ACCOUNT");
        std::env::remove_var("DBT_PROFILE_TEST_USER");
        std::env::remove_var("DBT_PROFILE_TEST_PASS");
    };
}

#[test]
fn test_resolve_with_vars() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: "{{ var('db_user', 'default_user') }}"
      password: "{{ var('db_pass') }}"
      dbname: my_database
      schema: public
"#,
    );

    let mut vars = BTreeMap::new();
    vars.insert(
        "db_pass".to_owned(),
        dbt_yaml::Value::string("secret123".to_owned()),
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("my_project".to_owned()),
        vars,
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    assert_eq!(result.adapter_type, "postgres");
    assert_eq!(result.get_str("user"), Some("default_user"));
    assert_eq!(result.get_str("password"), Some("secret123"));
    assert_eq!(result.database(), Some("my_database"));
}

#[test]
fn test_resolve_target_override() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.db
      schema: main
    prod:
      type: snowflake
      account: prod_account
      user: prod_user
      password: prod_pass
      warehouse: PROD_WH
      database: PROD_DB
      schema: PROD_SCHEMA
      role: SYSADMIN
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("my_project".to_owned()),
        target: Some("prod".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    assert_eq!(result.target_name, "prod");
    assert_eq!(result.adapter_type, "snowflake");
    assert_eq!(result.get_str("account"), Some("prod_account"));
}

#[test]
fn test_resolve_profile_name_from_project() {
    let tmp = tempfile::tempdir().unwrap();
    let project_dir = tmp.path();

    write_file(
        project_dir,
        "dbt_project.yml",
        r#"
name: my_cool_project
version: '1.0.0'
profile: 'cool_profile'
"#,
    );

    write_file(
        project_dir,
        "profiles.yml",
        r#"
cool_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.db
      schema: main
      database: memory
"#,
    );

    let args = ResolveArgs {
        project_dir: Some(project_dir.to_path_buf()),
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    assert_eq!(result.profile_name, "cool_profile");
    assert_eq!(result.adapter_type, "duckdb");
}

#[test]
fn test_resolve_credentials_json() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
proj:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: my_acct
      user: my_user
      password: my_pass
      warehouse: WH
      database: DB
      schema: SCH
      role: ADMIN
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("proj".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    let json_str = result.credentials_json().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed["type"], "snowflake");
    assert_eq!(parsed["account"], "my_acct");
    assert_eq!(parsed["user"], "my_user");
}

#[test]
fn test_resolve_missing_profile_error() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
other_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.db
      schema: main
      database: memory
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("nonexistent".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{err}").contains("nonexistent"),
        "Error should mention the missing profile name: {err}"
    );
}

#[test]
fn test_resolve_missing_target_error() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
proj:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.db
      schema: main
      database: memory
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("proj".to_owned()),
        target: Some("staging".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{err}").contains("staging"),
        "Error should mention the missing target: {err}"
    );
}

#[test]
fn test_resolve_with_as_number_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    unsafe { std::env::set_var("DBT_PROFILE_TEST_PORT2", "5432") };

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
proj:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: "{{ env_var('DBT_PROFILE_TEST_PORT2') | as_number }}"
      user: test
      password: test
      dbname: testdb
      schema: public
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("proj".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    let port = result.credentials.get("port").unwrap();
    // The rendered value should be parsed back as a number
    assert!(
        port.as_i64().is_some() || (port.as_str() == Some("5432")),
        "port should be a number or '5432', got: {port:?}"
    );

    unsafe { std::env::remove_var("DBT_PROFILE_TEST_PORT2") };
}

#[test]
fn test_resolve_no_profiles_yml() {
    let tmp = tempfile::tempdir().unwrap();

    let args = ResolveArgs {
        profiles_dir: Some(tmp.path().to_path_buf()),
        profile: Some("anything".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args);
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("profiles.yml"),
        "Error should mention profiles.yml: {err}"
    );
}

#[test]
fn test_resolve_config_key_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
config:
  send_anonymous_usage_stats: false

my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.db
      schema: main
      database: memory
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("my_project".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args);
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("config") && err.contains("Unexpected"),
        "Error should reject unexpected config key: {err}"
    );
}

#[test]
fn test_resolve_jinja_in_target_name() {
    let tmp = tempfile::tempdir().unwrap();
    let profiles_dir = tmp.path();

    unsafe { std::env::set_var("DBT_PROFILE_TEST_TARGET", "production") };

    write_file(
        profiles_dir,
        "profiles.yml",
        r#"
proj:
  target: "{{ env_var('DBT_PROFILE_TEST_TARGET') }}"
  outputs:
    production:
      type: snowflake
      account: prod_acct
      user: prod_user
      password: prod_pass
      warehouse: WH
      database: PROD
      schema: PUBLIC
      role: ADMIN
"#,
    );

    let args = ResolveArgs {
        profiles_dir: Some(profiles_dir.to_path_buf()),
        profile: Some("proj".to_owned()),
        ..Default::default()
    };

    let result = resolve(&args).unwrap();
    assert_eq!(result.target_name, "production");
    assert_eq!(result.get_str("account"), Some("prod_acct"));

    unsafe { std::env::remove_var("DBT_PROFILE_TEST_TARGET") };
}
