#[rustfmt::skip]
pub mod generated {
    #![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::restriction)]
    #![allow(unused_parens)]
    pub mod bigquery {
        pub mod bigquerylexer;
        pub mod bigquerylistener;
        pub mod bigqueryparser;
        pub mod bigqueryvisitor;

        pub use bigquerylexer::BigqueryLexer as Lexer;
    }
}

pub use generated::bigquery::*;
