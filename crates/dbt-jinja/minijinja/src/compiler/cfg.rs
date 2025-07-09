// cfg.rs
use crate::compiler::instructions::Instruction;
use std::collections::BTreeSet;

pub type BlockId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    FallThrough,
    Uncond,
    Cond(bool),
}

#[derive(Debug, Clone)]
pub struct BasicBlock {
    pub id: BlockId,
    pub start: usize,
    pub end: usize,
    pub successor: Vec<(BlockId, EdgeKind)>,
    pub predecessor: Vec<BlockId>,
    pub current_macro: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CFG {
    pub blocks: Vec<BasicBlock>,
    instruction_to_basic_block: Vec<BlockId>,
    pub entry: BlockId,
}

impl CFG {
    #[inline]
    pub fn block_of(&self, inst_idx: usize) -> BlockId {
        self.instruction_to_basic_block[inst_idx]
    }

    pub fn successor(&self, bb: BlockId) -> &[(BlockId, EdgeKind)] {
        &self.blocks[bb].successor
    }

    pub fn predecessor(&self, bb: BlockId) -> &[BlockId] {
        &self.blocks[bb].predecessor
    }

    pub fn instructions<'code>(
        &self,
        bb: BlockId,
        code: &'code [Instruction<'code>],
    ) -> &'code [Instruction<'code>] {
        let b = &self.blocks[bb];
        &code[b.start..=b.end]
    }

    pub fn to_dot(&self) -> String {
        let mut s = String::from("digraph cfg {  node [shape=box];");
        for b in &self.blocks {
            s.push_str(&format!("  B{} [label=\"B{}\"];", b.id, b.id));
            for (succ, kind) in &b.successor {
                s.push_str(&format!("  B{} -> B{} [label=\"{:?}\"];", b.id, succ, kind));
            }
        }
        s.push('}');
        s
    }

    pub fn dump_blocks(&self, code: &[Instruction]) -> String {
        let mut ret = String::new();
        for block in &self.blocks {
            ret.push_str(&format!(
                "Block B{} ({}..={}):\n",
                block.id, block.start, block.end
            ));

            for (offset, inst) in code[block.start..=block.end].iter().enumerate() {
                ret.push_str(&format!("  {:>4}: {:?}\n", block.start + offset, inst));
            }
            ret.push('\n');
        }
        ret
    }

    /// return a reference to the block with the given id
    pub fn get_block(&self, id: BlockId) -> Option<&BasicBlock> {
        self.blocks.get(id)
    }
}

fn is_block_terminator(inst: &Instruction) -> bool {
    matches!(
        inst,
        Instruction::Jump(_)
            | Instruction::JumpIfFalse(_)
            | Instruction::JumpIfFalseOrPop(_, _)
            | Instruction::JumpIfTrueOrPop(_, _)
            | Instruction::Iterate(_)
            | Instruction::FastRecurse
            | Instruction::PopFrame
            | Instruction::Return
    )
}

fn branch_targets(cur_idx: usize, inst: &Instruction) -> Vec<(usize, EdgeKind)> {
    use EdgeKind::*;
    match inst {
        Instruction::Jump(t) => vec![(*t, Uncond)],
        Instruction::FastRecurse => vec![(/*loop-head*/ 0, Uncond)],
        Instruction::Iterate(t) => vec![(cur_idx + 1, Cond(true)), (*t, Cond(false))],
        Instruction::JumpIfFalse(t) | Instruction::JumpIfFalseOrPop(t, _) => {
            vec![(cur_idx + 1, Cond(true)), (*t, Cond(false))]
        }
        Instruction::JumpIfTrueOrPop(t, _) => vec![(*t, Cond(true)), (cur_idx + 1, Cond(false))],
        Instruction::PopFrame => vec![(cur_idx + 1, EdgeKind::FallThrough)],
        Instruction::Return => vec![],
        _ => vec![(cur_idx + 1, FallThrough)],
    }
}

#[allow(clippy::needless_range_loop)]
pub fn build_cfg(code: &[Instruction]) -> CFG {
    use EdgeKind::*;

    let mut leaders: BTreeSet<usize> = BTreeSet::new();
    leaders.insert(0);
    for (index, instruction) in code.iter().enumerate() {
        for (target, kind) in branch_targets(index, instruction) {
            if !matches!(kind, FallThrough) && target < code.len() {
                leaders.insert(target);
            }
        }
        if is_block_terminator(instruction) && index + 1 < code.len() {
            leaders.insert(index + 1);
        }
    }

    let leader_vec: Vec<_> = leaders.into_iter().collect();
    let mut blocks = Vec::<BasicBlock>::new();
    let mut instruction_to_basic_block = vec![0; code.len()];
    for (i, &start) in leader_vec.iter().enumerate() {
        let end = if i + 1 < leader_vec.len() {
            leader_vec[i + 1] - 1
        } else {
            code.len() - 1
        };
        // Find cur_macro for this block (if any)
        let mut cur_macro: Option<String> = None;
        for idx in start..=end {
            if let Instruction::MacroName(name) = &code[idx] {
                cur_macro = Some(name.to_string());
                break;
            }
        }
        blocks.push(BasicBlock {
            id: i,
            start,
            end,
            successor: Vec::new(),
            predecessor: Vec::new(),
            current_macro: cur_macro,
        });
        for index in instruction_to_basic_block
            .iter_mut()
            .take(end + 1)
            .skip(start)
        {
            *index = i;
        }
    }

    for block in &mut blocks {
        let last = block.end;
        for (target, kind) in branch_targets(last, &code[last]) {
            if target < code.len() {
                block
                    .successor
                    .push((instruction_to_basic_block[target], kind));
            }
        }
    }

    let mut tmp_preds: Vec<Vec<BlockId>> = vec![Vec::new(); blocks.len()];
    for from in &blocks {
        for (to, _) in &from.successor {
            tmp_preds[*to].push(from.id);
        }
    }
    for (i, preds) in tmp_preds.into_iter().enumerate() {
        blocks[i].predecessor = preds;
    }

    CFG {
        blocks,
        instruction_to_basic_block,
        entry: 0,
    }
}
