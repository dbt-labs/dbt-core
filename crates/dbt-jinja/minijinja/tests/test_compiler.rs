#![cfg(feature = "unstable_machinery")]
use minijinja::machinery::{CodeGenerator, Instruction};
use minijinja::value::Value;

#[test]
fn test_for_loop() {
    let mut c = CodeGenerator::new("<unknown>", "");
    c.add(Instruction::Lookup("items"));
    c.start_for_loop(true, false);
    c.add(Instruction::Emit);
    c.end_for_loop(false);
    c.add(Instruction::EmitRaw("!"));

    insta::assert_debug_snapshot!(&c.finish());
}

#[test]
fn test_if_branches() {
    let mut c = CodeGenerator::new("<unknown>", "");
    c.add(Instruction::Lookup("false"));
    c.start_if();
    c.add(Instruction::EmitRaw("nope1"));
    c.start_else();
    c.add(Instruction::Lookup("nil"));
    c.start_if();
    c.add(Instruction::EmitRaw("nope1"));
    c.start_else();
    c.add(Instruction::EmitRaw("yes"));
    c.end_if();
    c.end_if();

    insta::assert_debug_snapshot!(&c.finish());
}

#[test]
fn test_bool_ops() {
    let mut c = CodeGenerator::new("<unknown>", "");

    c.start_sc_bool();
    c.add(Instruction::Lookup("first"));
    c.sc_bool(true);
    c.add(Instruction::Lookup("second"));
    c.sc_bool(false);
    c.add(Instruction::Lookup("third"));
    c.end_sc_bool();

    insta::assert_debug_snapshot!(&c.finish());
}

#[test]
fn test_const() {
    let mut c = CodeGenerator::new("<unknown>", "");

    c.add(Instruction::LoadConst(Value::from("a")));
    c.add(Instruction::LoadConst(Value::from(42)));
    c.add(Instruction::StringConcat);

    insta::assert_debug_snapshot!(&c.finish());
}

#[test]
fn test_referenced_names_empty_bug() {
    let c = CodeGenerator::new("<unknown>", "");
    let instructions = c.finish().0;
    let rv = instructions.get_referenced_names(0);
    assert!(rv.is_empty());
}
