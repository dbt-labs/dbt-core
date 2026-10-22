use std::collections::BTreeMap;

use dbt_adapter_core::AdapterType;
use minijinja::Value;

use crate::macro_test_harness::MacroTestHarness;

fn build_harness() -> MacroTestHarness {
    let harness = MacroTestHarness::for_adapter(AdapterType::MariaDB)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");
    harness.mock().on("quote", |args| {
        let identifier = args
            .first()
            .and_then(Value::as_str)
            .expect("quote receives an identifier");
        Ok(Value::from(format!("`{identifier}`")))
    });
    harness
}

fn render(harness: &MacroTestHarness, template: &str) -> String {
    let sql = harness
        .render(template, BTreeMap::<String, Value>::new())
        .expect("macro should render");
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn concat_uses_concat_function() {
    let harness = build_harness();
    assert_eq!(
        render(&harness, r#"{{ concat(["a", "'-'", "b"]) }}"#),
        "concat(a, '-', b)"
    );
}

#[test]
fn hash_casts_to_char() {
    let harness = build_harness();
    assert_eq!(
        render(&harness, r#"{{ hash("id") }}"#),
        "md5(cast(id as char))"
    );
}

#[test]
fn snapshot_hash_arguments_joins_with_concat_ws() {
    let harness = build_harness();
    assert_eq!(
        render(
            &harness,
            r#"{{ snapshot_hash_arguments(["id", "updated_at"]) }}"#
        ),
        "md5(concat_ws('|',coalesce(cast(id as char), ''), coalesce(cast(updated_at as char), '')))"
    );
}

#[test]
fn split_part_handles_negative_parts() {
    let harness = build_harness();
    assert_eq!(
        render(&harness, r#"{{ split_part("s", "','", 2) }}"#),
        "substring_index(substring_index(s, ',', 2), ',', -1)"
    );
    assert_eq!(
        render(&harness, r#"{{ split_part("s", "','", -2) }}"#),
        "substring_index(substring_index(s, ',', -2), ',', 1)"
    );
}

#[test]
fn listagg_uses_group_concat() {
    let harness = build_harness();
    assert_eq!(
        render(&harness, r#"{{ listagg("v", "'|'", "order by id", 3) }}"#),
        "group_concat( v order by id separator '|' limit 3 )"
    );
}

#[test]
fn delete_insert_deletes_without_alias() {
    let harness = build_harness();
    let sql = render(
        &harness,
        r#"{{ mariadb__get_delete_insert_merge_sql('`s`.`t`', '`s`.`t__dbt_tmp`', 'id', [{'name': 'id'}, {'name': 'v'}], ["DBT_INTERNAL_DEST.v > 0"]) }}"#,
    );
    assert!(
        sql.starts_with(
            "delete from `s`.`t` where (id) in ( select distinct id from `s`.`t__dbt_tmp` as DBT_INTERNAL_SOURCE ) and `s`.`t`.v > 0 ;"
        ),
        "{sql}"
    );
    assert!(
        sql.ends_with("insert into `s`.`t` (`id`, `v`) ( select `id`, `v` from `s`.`t__dbt_tmp` )"),
        "{sql}"
    );
}
