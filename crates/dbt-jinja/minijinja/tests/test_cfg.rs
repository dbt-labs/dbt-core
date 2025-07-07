use minijinja::compiler::cfg::build_cfg;
use minijinja::compiler::instructions::Instruction;

#[cfg(test)]
#[test]
fn simple_cfg() {
    let code = vec![
        Instruction::LoadConst(1.into()),
        Instruction::JumpIfFalse(4),
        Instruction::LoadConst(2.into()),
        Instruction::Jump(5),
        Instruction::LoadConst(3.into()),
        Instruction::Return,
    ];

    let cfg = build_cfg(&code);

    assert!(cfg.blocks.len() == 4);
}
