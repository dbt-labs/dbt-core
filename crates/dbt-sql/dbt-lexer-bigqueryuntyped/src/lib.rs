#[rustfmt::skip]
pub mod generated {
    #![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::restriction)]
    #![allow(unused_parens)]
    pub mod bigqueryuntyped {
        pub mod bigqueryuntypedlexer;

        pub use bigqueryuntypedlexer::BigqueryUntypedLexer as Lexer;
    }
}

pub use generated::bigqueryuntyped::*;
