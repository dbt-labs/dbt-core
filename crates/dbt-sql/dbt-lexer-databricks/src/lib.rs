#[rustfmt::skip]
pub mod generated {
    #![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::restriction)]
    #![allow(unused_parens)]
    pub mod databricks {
        pub mod databrickslexer;

        pub use databrickslexer::DatabricksLexer as Lexer;
    }
}

pub use generated::databricks::*;

pub(crate) mod lexer_support {
    use super::*;
    use antlr_rust::BaseLexer;
    use antlr_rust::char_stream::CharStream;
    use antlr_rust::token_factory::TokenFactory;
    use databrickslexer::{DatabricksLexerActions, LocalTokenFactory};

    type From<'a> = <LocalTokenFactory<'a> as TokenFactory<'a>>::From;

    // Follows https://github.com/apache/spark/blob/26dbf651bf8c2389aeea950816288e1db666c611/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseLexer.g4#L25-L45
    pub fn is_valid_decimal_boundary<'input, Input: CharStream<From<'input>>>(
        recog: &mut BaseLexer<'input, DatabricksLexerActions, Input, LocalTokenFactory<'input>>,
    ) -> bool {
        let input = recog.input.as_mut().expect("Input not set");
        let next = input.la(1);

        if next >= 'A' as isize && next <= 'Z' as isize {
            return false;
        }
        if next >= '0' as isize && next <= '9' as isize {
            return false;
        }
        if next == '_' as isize {
            return false;
        }
        true
    }
}
