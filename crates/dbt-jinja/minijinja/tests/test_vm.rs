#![cfg(feature = "unstable_machinery")]
use minijinja::compiler::codegen::CodeGenerationProfile;
use minijinja::machinery::{CodeGenerator, Instruction, Instructions, Span, Vm};
use minijinja::value::Value;
use minijinja::{AutoEscape, Environment, Error, Output, OutputTracker};
use std::collections::BTreeMap;

use similar_asserts::assert_eq;

pub fn simple_eval<S: serde::Serialize>(
    instructions: &Instructions<'_>,
    ctx: S,
) -> Result<String, Error> {
    let env = Environment::new();
    let empty_blocks = BTreeMap::new();
    let vm = Vm::new(&env);
    let root = Value::from_serialize(&ctx);
    let mut rv = String::new();
    let mut output_tracker = OutputTracker::new(&mut rv);
    let current_location = output_tracker.location.clone();
    let mut output = Output::with_write(&mut output_tracker);
    vm.eval(
        instructions,
        root,
        &empty_blocks,
        &mut output,
        current_location,
        AutoEscape::None,
        &[],
    )?;
    Ok(rv)
}

#[test]
fn test_loop() {
    let mut ctx = std::collections::BTreeMap::new();
    ctx.insert("items", Value::from((1..=9).collect::<Vec<_>>()));

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::Lookup("items", Span::new_file_default()));
    c.start_for_loop(false, false, Span::new_file_default());
    c.add(Instruction::Emit(Span::new_file_default()));
    c.end_for_loop(false);
    c.add(Instruction::EmitRaw("!", Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ctx).unwrap();

    assert_eq!(output, "123456789!");
}

#[test]
fn test_if() {
    for &(val, expectation) in [(true, "true"), (false, "false")].iter() {
        let mut ctx = std::collections::BTreeMap::new();
        ctx.insert("cond", Value::from(val));

        let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
        c.add(Instruction::Lookup("cond", Span::new_file_default()));
        c.start_if(Span::new_file_default());
        c.add(Instruction::EmitRaw("true", Span::new_file_default()));
        c.start_else();
        c.add(Instruction::EmitRaw("false", Span::new_file_default()));
        c.end_if();

        let output = simple_eval(&c.finish().0, ctx).unwrap();

        assert_eq!(output, expectation);
    }
}

#[test]
fn test_if_branches() {
    let mut ctx = std::collections::BTreeMap::new();
    ctx.insert("false", Value::from(false));
    ctx.insert("nil", Value::from(()));

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::Lookup("false", Span::new_file_default()));
    c.start_if(Span::new_file_default());
    c.add(Instruction::EmitRaw("nope1", Span::new_file_default()));
    c.start_else();
    c.add(Instruction::Lookup("nil", Span::new_file_default()));
    c.start_if(Span::new_file_default());
    c.add(Instruction::EmitRaw("nope1", Span::new_file_default()));
    c.start_else();
    c.add(Instruction::EmitRaw("yes", Span::new_file_default()));
    c.end_if();
    c.end_if();

    let output = simple_eval(&c.finish().0, ctx).unwrap();

    assert_eq!(output, "yes");
}

#[test]
fn test_basic() {
    let mut user = std::collections::BTreeMap::new();
    user.insert("name", "Peter");
    let mut ctx = std::collections::BTreeMap::new();
    ctx.insert("user", Value::from(user));
    ctx.insert("a", Value::from(42));
    ctx.insert("b", Value::from(23));

    let mut i = Instructions::new("", "", None);
    i.add(Instruction::EmitRaw("Hello ", Span::new_file_default()));
    i.add(Instruction::Lookup("user", Span::new_file_default()));
    i.add(Instruction::GetAttr("name", Span::new_file_default()));
    i.add(Instruction::Emit(Span::new_file_default()));
    i.add(Instruction::Lookup("a", Span::new_file_default()));
    i.add(Instruction::Lookup("b", Span::new_file_default()));
    i.add(Instruction::Add(Span::new_file_default()));
    i.add(Instruction::Neg(Span::new_file_default()));
    i.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&i, ctx).unwrap();
    assert_eq!(output, "Hello Peter-65");
}

#[ignore = "zhong is refactoring vm https://github.com/dbt-labs/fs/issues/4808"]
#[test]
fn test_error_info() {
    let mut c = CodeGenerator::new("hello.html", "", CodeGenerationProfile::Render);
    c.set_line(1);
    c.add(Instruction::EmitRaw(
        "<h1>Hello</h1>\n",
        Span::new_file_default(),
    ));
    c.set_line(2);
    c.add(Instruction::Lookup("a_string", Span::new_file_default()));
    c.add(Instruction::Lookup("an_int", Span::new_file_default()));
    c.add(Instruction::Add(Span::new_file_default()));

    let mut ctx = std::collections::BTreeMap::new();
    ctx.insert("a_string", Value::from("foo"));
    ctx.insert("an_int", Value::from(42));

    let err = simple_eval(&c.finish().0, ctx).unwrap_err();
    assert_eq!(err.name(), Some(""));
    assert_eq!(err.line(), Some(2));
}

#[test]
fn test_op_eq() {
    let mut c = CodeGenerator::new("hello.html", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::Eq(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "true");

    let mut c = CodeGenerator::new("hello.html", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from(2)));
    c.add(Instruction::Eq(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "false");
}

#[test]
fn test_op_ne() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from("foo")));
    c.add(Instruction::Ne(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "true");

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from("foo")));
    c.add(Instruction::LoadConst(Value::from("foo")));
    c.add(Instruction::Ne(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "false");
}

#[test]
fn test_op_lt() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from(2)));
    c.add(Instruction::Lt(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "true");

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(2)));
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::Lt(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "false");
}

#[test]
fn test_op_gt() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from(2)));
    c.add(Instruction::Gt(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "false");

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(2)));
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::Gt(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "true");
}

#[test]
fn test_op_lte() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::Lte(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "true");

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(2)));
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::Lte(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "false");
}

#[test]
fn test_op_gte() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from(2)));
    c.add(Instruction::Gte(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "false");

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::LoadConst(Value::from(1)));
    c.add(Instruction::Gte(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "true");
}

#[test]
fn test_op_not() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(0)));
    c.add(Instruction::Not(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "true");

    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(true)));
    c.add(Instruction::Not(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "false");
}

#[test]
fn test_string_concat() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from("foo")));
    c.add(Instruction::LoadConst(Value::from(42)));
    c.add(Instruction::StringConcat(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "foo42");
}

#[test]
fn test_unpacking() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from(vec!["bar", "foo"])));
    c.add(Instruction::UnpackList(2, Span::new_file_default()));
    c.add(Instruction::StringConcat(Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "foobar");
}

#[test]
fn test_call_object() {
    let mut c = CodeGenerator::new("<unknown>", "", CodeGenerationProfile::Render);
    c.add(Instruction::LoadConst(Value::from_function(|a: u64| {
        42 + a
    })));
    c.add(Instruction::LoadConst(Value::from(23i32)));
    c.add(Instruction::CallObject(Some(2), Span::new_file_default()));
    c.add(Instruction::Emit(Span::new_file_default()));

    let output = simple_eval(&c.finish().0, ()).unwrap();
    assert_eq!(output, "65");
}

#[test]
#[cfg(feature = "stacker")]
fn test_deep_recursion() {
    use minijinja::context;

    let mut env = Environment::new();
    let limit = if cfg!(target_arch = "wasm32") {
        500
    } else {
        10000
    };
    env.set_recursion_limit(limit * 7);
    let tmpl = env
        .template_from_str(
            r#"
        {%- macro foo(i) -%}
            {%- if i < limit %}{{ i }}|{{ foo(i + 1) }}{%- endif -%}
        {%- endmacro -%}
        {{- foo(0) -}}
    "#,
            &[],
        )
        .unwrap();
    let rv = tmpl.render(context!(limit), &[]).unwrap();
    let pieces = rv
        .split('|')
        .filter_map(|x| x.parse::<usize>().ok())
        .collect::<Vec<_>>();
    assert_eq!(pieces, (0..limit).collect::<Vec<_>>());
}
