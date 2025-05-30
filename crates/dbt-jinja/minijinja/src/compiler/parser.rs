use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use crate::compiler::ast::{self, Comment, MacroKind, Spanned};
use crate::compiler::lexer::{Tokenizer, WhitespaceConfig};
use crate::compiler::tokens::{Span, Token};
use crate::error::{Error, ErrorKind};
use crate::syntax::SyntaxConfig;
use crate::value::Value;

const MAX_RECURSION: usize = 100;
const RESERVED_NAMES: [&str; 8] = [
    "true", "True", "false", "False", "none", "None", "loop", "self",
];

fn unexpected<D: fmt::Display>(unexpected: D, expected: &str) -> Error {
    Error::new(
        ErrorKind::SyntaxError,
        format!("unexpected {unexpected}, expected {expected}"),
    )
}

fn unexpected_eof(expected: &str) -> Error {
    unexpected("end of input", expected)
}

fn make_const(value: Value, span: Span) -> ast::Expr<'static> {
    ast::Expr::Const(Spanned::new(ast::Const { value }, span))
}

fn syntax_error(msg: Cow<'static, str>) -> Error {
    Error::new(ErrorKind::SyntaxError, msg)
}

macro_rules! syntax_error {
    ($msg:expr) => {{
        return Err(syntax_error(Cow::Borrowed($msg)));
    }};
    ($msg:expr, $($tt:tt)*) => {{
        return Err(syntax_error(Cow::Owned(format!($msg, $($tt)*))));
    }};
}

macro_rules! expect_token {
    ($parser:expr, $expectation:expr) => {{
        match ok!($parser.stream.next()) {
            Some(rv) => rv,
            None => return Err(unexpected_eof($expectation)),
        }
    }};
    ($parser:expr, $match:pat, $expectation:expr) => {{
        match ok!($parser.stream.next()) {
            Some((token @ $match, span)) => (token, span),
            Some((token, _)) => return Err(unexpected(token, $expectation)),
            None => return Err(unexpected_eof($expectation)),
        }
    }};
    ($parser:expr, $match:pat => $target:expr, $expectation:expr) => {{
        match ok!($parser.stream.next()) {
            Some(($match, span)) => ($target, span),
            Some((token, _)) => return Err(unexpected(token, $expectation)),
            None => return Err(unexpected_eof($expectation)),
        }
    }};
}

macro_rules! matches_token {
    ($p:expr, $match:pat) => {
        match $p.stream.current() {
            Err(err) => return Err(err),
            Ok(Some(($match, _))) => true,
            _ => false,
        }
    };
}

macro_rules! skip_token {
    ($p:expr, $match:pat) => {
        match $p.stream.current() {
            Err(err) => return Err(err),
            Ok(Some(($match, _))) => {
                let _ = $p.stream.next();
                true
            }
            _ => false,
        }
    };
}

enum SetParseResult<'a> {
    Set(ast::Set<'a>),
    SetBlock(ast::SetBlock<'a>),
}

pub struct TokenStream<'a> {
    tokenizer: Tokenizer<'a>,
    current: Option<Result<(Token<'a>, Span), Error>>,
    last_span: Span,
}

impl<'a> TokenStream<'a> {
    /// Tokenize a template
    pub fn new(
        source: &'a str,
        filename: &'a str,
        in_expr: bool,
        syntax_config: SyntaxConfig,
        whitespace_config: WhitespaceConfig,
    ) -> TokenStream<'a> {
        let mut tokenizer =
            Tokenizer::new(source, filename, in_expr, syntax_config, whitespace_config);
        let current = tokenizer.next_token().transpose();
        TokenStream {
            tokenizer,
            current,
            last_span: Span::default(),
        }
    }

    /// Advance the stream.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(Token<'a>, Span)>, Error> {
        let rv = self.current.take();
        self.current = self.tokenizer.next_token().transpose();
        if let Some(Ok((_, span))) = &rv {
            self.last_span = *span;
        }
        rv.transpose()
    }

    /// Look at the current token
    pub fn current(&mut self) -> Result<Option<(&Token<'a>, Span)>, Error> {
        match self.current {
            Some(Ok(ref tok)) => Ok(Some((&tok.0, tok.1))),
            Some(Err(_)) => Err(self.current.take().unwrap().unwrap_err()),
            None => Ok(None),
        }
    }

    /// Expands the span
    #[inline(always)]
    pub fn expand_span(&self, mut span: Span) -> Span {
        span.end_line = self.last_span.end_line;
        span.end_col = self.last_span.end_col;
        span.end_offset = self.last_span.end_offset;
        span
    }

    /// Returns the current span.
    #[inline(always)]
    pub fn current_span(&self) -> Span {
        if let Some(Ok((_, span))) = &self.current {
            *span
        } else {
            self.last_span
        }
    }

    /// Returns the last seen span.
    #[inline(always)]
    pub fn last_span(&self) -> Span {
        self.last_span
    }
}

pub struct Parser<'a> {
    stream: TokenStream<'a>,
    in_macro: bool,
    in_loop: bool,
    blocks: BTreeSet<&'a str>,
    depth: usize,
    string_interner: HashMap<String, &'a str>,
}

macro_rules! binop {
    ($func:ident, $next:ident, { $($tok:tt)* }) => {
        fn $func(&mut self) -> Result<ast::Expr<'a>, Error> {
            let span = self.stream.current_span();
            let mut left = ok!(self.$next());
            loop {
                let op = match ok!(self.stream.current()) {
                    $($tok)*
                    _ => break,
                };
                ok!(self.stream.next());
                let right = ok!(self.$next());
                left = ast::Expr::BinOp(Spanned::new(
                    ast::BinOp { op, left, right, },
                    self.stream.expand_span(span),
                ));
            }
            Ok(left)
        }
    };
}

macro_rules! unaryop {
    ($func:ident, $next:ident, { $($tok:tt)* }) => {
        fn $func(&mut self) -> Result<ast::Expr<'a>, Error> {
            let span = self.stream.current_span();
            let op = match ok!(self.stream.current()) {
                $($tok)*
                _ => return self.$next()
            };
            ok!(self.stream.next());
            let mut expr = ok!(self.$next());
            expr = ok!(self.parse_postfix(expr, span));
            Ok(ast::Expr::UnaryOp(Spanned::new(
                ast::UnaryOp { op, expr },
                self.stream.expand_span(span),
            )))
        }
    };
}

macro_rules! with_recursion_guard {
    ($parser:expr, $expr:expr) => {{
        $parser.depth += 1;
        if $parser.depth > MAX_RECURSION {
            return Err(syntax_error(Cow::Borrowed(
                "template exceeds maximum recursion limits",
            )));
        }
        let rv = $expr;
        $parser.depth -= 1;
        rv
    }};
}

impl<'a> Parser<'a> {
    /// Creates a new parser.
    ///
    /// `in_expr` is necessary to parse within an expression context.  Otherwise
    /// the parser starts out in template context.  This means that when
    /// [`parse`](Self::parse) is to be called, the `in_expr` argument must be
    /// `false` and for [`parse_standalone_expr`](Self::parse_standalone_expr)
    /// it must be `true`.
    pub fn new(
        source: &'a str,
        filename: &'a str,
        in_expr: bool,
        syntax_config: SyntaxConfig,
        whitespace_config: WhitespaceConfig,
    ) -> Parser<'a> {
        Parser {
            stream: TokenStream::new(source, filename, in_expr, syntax_config, whitespace_config),
            in_macro: false,
            in_loop: false,
            blocks: BTreeSet::new(),
            depth: 0,
            string_interner: HashMap::new(),
        }
    }

    /// Parses a template.
    pub fn parse(&mut self) -> Result<ast::Stmt<'a>, Error> {
        let span = self.stream.last_span();
        self.subparse(&|_| false)
            .map(|children| {
                ast::Stmt::Template(Spanned::new(
                    ast::Template { children },
                    self.stream.expand_span(span),
                ))
            })
            .map_err(|err| self.attach_location_to_error(err))
    }

    /// Parses an expression and asserts that there is no more input after it.
    pub fn parse_standalone_expr(&mut self) -> Result<ast::Expr<'a>, Error> {
        self.parse_expr()
            .and_then(|result| {
                if ok!(self.stream.next()).is_some() {
                    syntax_error!("unexpected input after expression")
                } else {
                    Ok(result)
                }
            })
            .map_err(|err| self.attach_location_to_error(err))
    }

    /// Returns the current filename.
    pub fn filename(&self) -> &str {
        self.stream.tokenizer.filename()
    }

    fn parse_ifexpr(&mut self) -> Result<ast::Expr<'a>, Error> {
        let mut span = self.stream.last_span();
        let mut expr = ok!(self.parse_or());
        loop {
            if skip_token!(self, Token::Ident("if")) {
                let expr2 = ok!(self.parse_or());
                let expr3 = if skip_token!(self, Token::Ident("else")) {
                    Some(ok!(self.parse_ifexpr()))
                } else {
                    None
                };
                expr = ast::Expr::IfExpr(Spanned::new(
                    ast::IfExpr {
                        test_expr: expr2,
                        true_expr: expr,
                        false_expr: expr3,
                    },
                    self.stream.expand_span(span),
                ));
                span = self.stream.last_span();
            } else {
                break;
            }
        }
        Ok(expr)
    }

    binop!(parse_or, parse_and, {
        Some((Token::Ident("or"), _)) => ast::BinOpKind::ScOr,
    });
    binop!(parse_and, parse_not, {
        Some((Token::Ident("and"), _)) => ast::BinOpKind::ScAnd,
    });
    unaryop!(parse_not, parse_compare, {
        Some((Token::Ident("not"), _)) => ast::UnaryOpKind::Not,
    });

    fn parse_compare(&mut self) -> Result<ast::Expr<'a>, Error> {
        let mut span = self.stream.last_span();
        let mut expr = ok!(self.parse_math1());
        loop {
            let mut negated = false;
            let op = match ok!(self.stream.current()) {
                Some((Token::Eq, _)) => ast::BinOpKind::Eq,
                Some((Token::Ne, _)) => ast::BinOpKind::Ne,
                Some((Token::Lt, _)) => ast::BinOpKind::Lt,
                Some((Token::Lte, _)) => ast::BinOpKind::Lte,
                Some((Token::Gt, _)) => ast::BinOpKind::Gt,
                Some((Token::Gte, _)) => ast::BinOpKind::Gte,
                Some((Token::Ident("in"), _)) => ast::BinOpKind::In,
                Some((Token::Ident("not"), _)) => {
                    ok!(self.stream.next());
                    expect_token!(self, Token::Ident("in"), "in");
                    negated = true;
                    ast::BinOpKind::In
                }
                _ => break,
            };
            if !negated {
                ok!(self.stream.next());
            }
            expr = ast::Expr::BinOp(Spanned::new(
                ast::BinOp {
                    op,
                    left: expr,
                    right: ok!(self.parse_math1()),
                },
                self.stream.expand_span(span),
            ));
            if negated {
                expr = ast::Expr::UnaryOp(Spanned::new(
                    ast::UnaryOp {
                        op: ast::UnaryOpKind::Not,
                        expr,
                    },
                    self.stream.expand_span(span),
                ));
            }
            span = self.stream.last_span();
        }
        Ok(expr)
    }

    binop!(parse_math1, parse_concat, {
        Some((Token::Plus, _)) => ast::BinOpKind::Add,
        Some((Token::Minus, _)) => ast::BinOpKind::Sub,
    });
    binop!(parse_concat, parse_math2, {
        Some((Token::Tilde, _)) => ast::BinOpKind::Concat,
    });
    binop!(parse_math2, parse_pow, {
        Some((Token::Mul, _)) => ast::BinOpKind::Mul,
        Some((Token::Div, _)) => ast::BinOpKind::Div,
        Some((Token::FloorDiv, _)) => ast::BinOpKind::FloorDiv,
        Some((Token::Mod, _)) => ast::BinOpKind::Rem,
    });
    binop!(parse_pow, parse_unary, {
        Some((Token::Pow, _)) => ast::BinOpKind::Pow,
    });
    unaryop!(parse_unary_only, parse_primary, {
        Some((Token::Minus, _)) => ast::UnaryOpKind::Neg,
    });

    fn parse_unary(&mut self) -> Result<ast::Expr<'a>, Error> {
        let span = self.stream.current_span();
        let mut expr = ok!(self.parse_unary_only());
        expr = ok!(self.parse_postfix(expr, span));
        self.parse_filter_expr(expr)
    }

    fn parse_postfix(
        &mut self,
        expr: ast::Expr<'a>,
        mut span: Span,
    ) -> Result<ast::Expr<'a>, Error> {
        let mut expr = expr;
        loop {
            let next_span = self.stream.current_span();
            match ok!(self.stream.current()) {
                Some((Token::Dot, _)) => {
                    ok!(self.stream.next());
                    let (tok, _) = {
                        match ok!(self.stream.next()) {
                            Some((Token::Ident(name), span)) => (Token::Ident(name), span),
                            Some((Token::Int(number), span)) => (Token::Int(number), span),
                            Some((token, _)) => {
                                return Err(unexpected(token, "identifier or integer"))
                            }
                            None => return Err(unexpected_eof("identifier or integer")),
                        }
                    };
                    match tok {
                        Token::Ident(name) => {
                            expr = ast::Expr::GetAttr(Spanned::new(
                                ast::GetAttr { name, expr },
                                self.stream.expand_span(span),
                            ));
                        }
                        Token::Int(number) => {
                            let subscript_expr = make_const(Value::from(number), span);
                            expr = ast::Expr::GetItem(Spanned::new(
                                ast::GetItem {
                                    expr,
                                    subscript_expr,
                                },
                                self.stream.expand_span(span),
                            ));
                        }
                        _ => {}
                    }
                }
                Some((Token::BracketOpen, _)) => {
                    ok!(self.stream.next());

                    let mut start = None;
                    let mut stop = None;
                    let mut step = None;
                    let mut items = vec![];
                    let mut is_slice = false;
                    let mut is_tuple = false;

                    if !matches_token!(self, Token::Colon) {
                        start = Some(ok!(self.parse_expr()));
                    }

                    if skip_token!(self, Token::Colon) {
                        is_slice = true;
                        if !matches_token!(self, Token::BracketClose | Token::Colon) {
                            stop = Some(ok!(self.parse_expr()));
                        }
                        if skip_token!(self, Token::Colon)
                            && !matches_token!(self, Token::BracketClose)
                        {
                            step = Some(ok!(self.parse_expr()));
                        }
                    } else if skip_token!(self, Token::Comma) {
                        is_tuple = true;
                        loop {
                            if matches_token!(self, Token::BracketClose) {
                                break;
                            }
                            items.push(ok!(self.parse_expr()));
                            if !skip_token!(self, Token::Comma) {
                                break;
                            }
                        }
                    }

                    expect_token!(self, Token::BracketClose, "`]`");

                    if is_slice {
                        expr = ast::Expr::Slice(Spanned::new(
                            ast::Slice {
                                expr,
                                start,
                                stop,
                                step,
                            },
                            self.stream.expand_span(span),
                        ));
                    } else if is_tuple {
                        let items = start.into_iter().chain(items).collect();
                        expr = ast::Expr::GetItem(Spanned::new(
                            ast::GetItem {
                                expr,
                                subscript_expr: ast::Expr::List(Spanned::new(
                                    ast::List { items },
                                    self.stream.expand_span(span),
                                )),
                            },
                            self.stream.expand_span(span),
                        ));
                    } else {
                        assert!(items.is_empty());
                        expr = ast::Expr::GetItem(Spanned::new(
                            ast::GetItem {
                                expr,
                                subscript_expr: ok!(start.ok_or_else(|| {
                                    syntax_error(Cow::Borrowed("empty subscript"))
                                })),
                            },
                            self.stream.expand_span(span),
                        ));
                    }
                }
                Some((Token::ParenOpen, _)) => {
                    let args = ok!(self.parse_args());
                    expr = ast::Expr::Call(Spanned::new(
                        ast::Call { expr, args },
                        self.stream.expand_span(span),
                    ));
                }
                _ => break,
            }
            span = next_span;
        }
        Ok(expr)
    }

    fn parse_filter_expr(&mut self, expr: ast::Expr<'a>) -> Result<ast::Expr<'a>, Error> {
        let mut expr = expr;
        loop {
            match ok!(self.stream.current()) {
                Some((Token::Pipe, _)) => {
                    ok!(self.stream.next());
                    let (name, span) =
                        expect_token!(self, Token::Ident(name) => name, "identifier");
                    let args = if matches_token!(self, Token::ParenOpen) {
                        ok!(self.parse_args())
                    } else {
                        Vec::new()
                    };
                    expr = ast::Expr::Filter(Spanned::new(
                        ast::Filter {
                            name,
                            expr: Some(expr),
                            args,
                        },
                        self.stream.expand_span(span),
                    ));
                }
                Some((Token::Ident("is"), _)) => {
                    ok!(self.stream.next());
                    let negated = skip_token!(self, Token::Ident("not"));
                    let (name, span) =
                        expect_token!(self, Token::Ident(name) => name, "identifier");
                    let args = if matches_token!(self, Token::ParenOpen) {
                        ok!(self.parse_args())
                    } else if matches_token!(
                        self,
                        Token::Ident(_)
                            | Token::Str(_)
                            | Token::String(_)
                            | Token::Int(_)
                            | Token::Int128(_)
                            | Token::Float(_)
                            | Token::Plus
                            | Token::Minus
                            | Token::BracketOpen
                            | Token::BraceOpen
                    ) && !matches_token!(
                        self,
                        Token::Ident("and")
                            | Token::Ident("or")
                            | Token::Ident("else")
                            | Token::Ident("is")
                    ) {
                        let span = self.stream.current_span();
                        let mut expr = ok!(self.parse_unary_only());
                        expr = ok!(self.parse_postfix(expr, span));
                        vec![ast::CallArg::Pos(expr)]
                    } else {
                        Vec::new()
                    };
                    expr = ast::Expr::Test(Spanned::new(
                        ast::Test { name, expr, args },
                        self.stream.expand_span(span),
                    ));
                    if negated {
                        expr = ast::Expr::UnaryOp(Spanned::new(
                            ast::UnaryOp {
                                op: ast::UnaryOpKind::Not,
                                expr,
                            },
                            self.stream.expand_span(span),
                        ));
                    }
                }
                _ => break,
            }
        }
        Ok(expr)
    }

    fn parse_args(&mut self) -> Result<Vec<ast::CallArg<'a>>, Error> {
        let mut args = Vec::new();
        let mut first_span = None;
        let mut has_kwargs = false;

        enum ArgType {
            Regular,
            Splat,
            KwargsSplat,
        }

        expect_token!(self, Token::ParenOpen, "`(`");
        loop {
            if skip_token!(self, Token::ParenClose) {
                break;
            }
            if !args.is_empty() || has_kwargs {
                expect_token!(self, Token::Comma, "`,`");
                if skip_token!(self, Token::ParenClose) {
                    break;
                }
            }

            let arg_type = if skip_token!(self, Token::Pow) {
                ArgType::KwargsSplat
            } else if skip_token!(self, Token::Mul) {
                ArgType::Splat
            } else {
                ArgType::Regular
            };

            let expr = ok!(self.parse_expr());

            match arg_type {
                ArgType::Regular => {
                    // keyword argument
                    match expr {
                        ast::Expr::Var(ref var) if skip_token!(self, Token::Assign) => {
                            if first_span.is_none() {
                                first_span = Some(var.span());
                            }
                            has_kwargs = true;
                            args.push(ast::CallArg::Kwarg(var.id, ok!(self.parse_expr())));
                        }
                        _ if has_kwargs => {
                            return Err(syntax_error(Cow::Borrowed(
                                "non-keyword arg after keyword arg",
                            )));
                        }
                        _ => {
                            args.push(ast::CallArg::Pos(expr));
                        }
                    }
                }
                ArgType::Splat => {
                    args.push(ast::CallArg::PosSplat(expr));
                }
                ArgType::KwargsSplat => {
                    args.push(ast::CallArg::KwargSplat(expr));
                    has_kwargs = true;
                }
            }

            // Set an arbitrary limit of max function parameters.  This is done
            // in parts because the opcodes can only express 2**16 as argument
            // count.
            if args.len() > 2000 {
                syntax_error!("Too many arguments in function call")
            }
        }

        Ok(args)
    }

    fn parse_primary(&mut self) -> Result<ast::Expr<'a>, Error> {
        with_recursion_guard!(self, self.parse_primary_impl())
    }

    fn parse_primary_impl(&mut self) -> Result<ast::Expr<'a>, Error> {
        let (token, span) = expect_token!(self, "expression");
        macro_rules! const_val {
            ($expr:expr) => {
                make_const(Value::from($expr), self.stream.expand_span(span))
            };
        }

        match token {
            Token::Ident("true" | "True") => Ok(const_val!(true)),
            Token::Ident("false" | "False") => Ok(const_val!(false)),
            Token::Ident("none" | "None") => Ok(const_val!(())),
            Token::Ident(name) => Ok(ast::Expr::Var(Spanned::new(ast::Var { id: name }, span))),
            Token::Str(_) | Token::String(_) => {
                let mut buf = match token {
                    Token::Str(s) => s.to_owned(),
                    Token::String(s) => s,
                    _ => unreachable!(),
                };
                loop {
                    match ok!(self.stream.current()) {
                        Some((Token::Str(s), _)) => buf.push_str(s),
                        Some((Token::String(s), _)) => buf.push_str(s),
                        _ => break,
                    }
                    ok!(self.stream.next());
                }
                Ok(const_val!(buf))
            }
            Token::Int(val) => Ok(const_val!(val)),
            Token::Int128(val) => Ok(const_val!(val)),
            Token::Float(val) => Ok(const_val!(val)),
            Token::ParenOpen => self.parse_tuple_or_expression(span),
            Token::BracketOpen => self.parse_list_expr(span),
            Token::BraceOpen => self.parse_map_expr(span),
            token => syntax_error!("unexpected {}", token),
        }
    }

    fn parse_list_expr(&mut self, span: Span) -> Result<ast::Expr<'a>, Error> {
        let mut items = Vec::new();
        loop {
            if skip_token!(self, Token::BracketClose) {
                break;
            }
            if !items.is_empty() {
                expect_token!(self, Token::Comma, "`,`");
                if skip_token!(self, Token::BracketClose) {
                    break;
                }
            }
            items.push(ok!(self.parse_expr()));
        }
        Ok(ast::Expr::List(Spanned::new(
            ast::List { items },
            self.stream.expand_span(span),
        )))
    }

    fn parse_map_expr(&mut self, span: Span) -> Result<ast::Expr<'a>, Error> {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        loop {
            if skip_token!(self, Token::BraceClose) {
                break;
            }
            if !keys.is_empty() {
                expect_token!(self, Token::Comma, "`,`");
                if skip_token!(self, Token::BraceClose) {
                    break;
                }
            }
            keys.push(ok!(self.parse_expr()));
            expect_token!(self, Token::Colon, "`:`");
            values.push(ok!(self.parse_expr()));
        }
        Ok(ast::Expr::Map(Spanned::new(
            ast::Map { keys, values },
            self.stream.expand_span(span),
        )))
    }

    fn parse_tuple_or_expression(&mut self, span: Span) -> Result<ast::Expr<'a>, Error> {
        // MiniJinja does not really have tuples, but it treats the tuple
        // syntax the same as lists.
        if skip_token!(self, Token::ParenClose) {
            return Ok(ast::Expr::Tuple(Spanned::new(
                ast::Tuple { items: vec![] },
                self.stream.expand_span(span),
            )));
        }
        let mut expr = ok!(self.parse_expr());
        if matches_token!(self, Token::Comma) {
            let mut items = vec![expr];
            loop {
                if skip_token!(self, Token::ParenClose) {
                    break;
                }
                expect_token!(self, Token::Comma, "`,`");
                if skip_token!(self, Token::ParenClose) {
                    break;
                }
                items.push(ok!(self.parse_expr()));
            }
            expr = ast::Expr::Tuple(Spanned::new(
                ast::Tuple { items },
                self.stream.expand_span(span),
            ));
        } else {
            expect_token!(self, Token::ParenClose, "`)`");
        }
        Ok(expr)
    }

    fn parse_expr(&mut self) -> Result<ast::Expr<'a>, Error> {
        with_recursion_guard!(self, self.parse_ifexpr())
    }

    fn parse_expr_or_implied_tuple(&mut self) -> Result<ast::Expr<'a>, Error> {
        let first = ok!(self.parse_expr());
        if skip_token!(self, Token::Comma) {
            let mut items = vec![first];
            loop {
                if matches_token!(self, Token::VariableEnd) {
                    break;
                }
                items.push(ok!(self.parse_expr()));
                if !skip_token!(self, Token::Comma) {
                    break;
                }
            }
            Ok(ast::Expr::Tuple(Spanned::new(
                ast::Tuple { items },
                self.stream.expand_span(self.stream.last_span()),
            )))
        } else {
            Ok(first)
        }
    }

    fn parse_expr_noif(&mut self) -> Result<ast::Expr<'a>, Error> {
        self.parse_or()
    }

    fn parse_stmt(&mut self) -> Result<ast::Stmt<'a>, Error> {
        with_recursion_guard!(self, self.parse_stmt_unprotected())
    }

    fn parse_stmt_unprotected(&mut self) -> Result<ast::Stmt<'a>, Error> {
        let span = self.stream.last_span();
        let (token, _span) = expect_token!(self, "block keyword");

        macro_rules! respan {
            ($expr:expr) => {{
                let node = $expr;
                expect_token!(self, Token::BlockEnd, "end of block");
                Spanned::new(node, self.stream.expand_span(span))
            }};
        }

        let ident = match token {
            Token::Ident(ident) => ident,
            token => syntax_error!("unknown {}, expected statement", token),
        };

        Ok(match ident {
            "for" => ast::Stmt::ForLoop(respan!(ok!(self.parse_for_stmt()))),
            "if" => ast::Stmt::IfCond(respan!(ok!(self.parse_if_cond()))),
            "with" => ast::Stmt::WithBlock(respan!(ok!(self.parse_with_block()))),
            "set" => match ok!(self.parse_set()) {
                SetParseResult::Set(rv) => ast::Stmt::Set(respan!(rv)),
                SetParseResult::SetBlock(rv) => ast::Stmt::SetBlock(respan!(rv)),
            },
            "autoescape" => ast::Stmt::AutoEscape(respan!(ok!(self.parse_auto_escape()))),
            "filter" => ast::Stmt::FilterBlock(respan!(ok!(self.parse_filter_block()))),
            #[cfg(feature = "multi_template")]
            "block" => ast::Stmt::Block(respan!(ok!(self.parse_block()))),
            #[cfg(feature = "multi_template")]
            "extends" => ast::Stmt::Extends(respan!(ok!(self.parse_extends()))),
            #[cfg(feature = "multi_template")]
            "include" => ast::Stmt::Include(respan!(ok!(self.parse_include()))),
            #[cfg(feature = "multi_template")]
            "import" => ast::Stmt::Import(respan!(ok!(self.parse_import()))),
            #[cfg(feature = "multi_template")]
            "from" => ast::Stmt::FromImport(respan!(ok!(self.parse_from_import()))),
            #[cfg(feature = "macros")]
            "macro" => ast::Stmt::Macro((
                respan!(ok!(self.parse_macro())),
                MacroKind::Macro,
                BTreeMap::new(),
            )),
            #[cfg(feature = "macros")]
            "test" => ast::Stmt::Macro((
                respan!(ok!(self.parse_test())),
                MacroKind::Test,
                BTreeMap::new(),
            )),
            #[cfg(feature = "macros")]
            "snapshot" => ast::Stmt::Macro((
                respan!(ok!(self.parse_snapshot())),
                MacroKind::Snapshot,
                BTreeMap::new(),
            )),
            #[cfg(feature = "macros")]
            "docs" => ast::Stmt::Macro((
                respan!(ok!(self.parse_doc())),
                MacroKind::Doc,
                BTreeMap::new(),
            )),
            #[cfg(feature = "macros")]
            "materialization" => {
                let (macro_, maybe_adapter) = ok!(self.parse_materialization());
                ast::Stmt::Macro((
                    respan!(macro_),
                    MacroKind::Materialization,
                    BTreeMap::from([(
                        "adapter".to_string(),
                        Value::from(maybe_adapter.unwrap_or("default".to_string())),
                    )]),
                ))
            }
            #[cfg(feature = "macros")]
            "call" => ast::Stmt::CallBlock(respan!(ok!(self.parse_call_block()))),
            #[cfg(feature = "loop_controls")]
            "continue" => {
                if !self.in_loop {
                    syntax_error!("'continue' must be placed inside a loop");
                }
                ast::Stmt::Continue(respan!(ast::Continue))
            }
            #[cfg(feature = "loop_controls")]
            "break" => {
                if !self.in_loop {
                    syntax_error!("'break' must be placed inside a loop");
                }
                ast::Stmt::Break(respan!(ast::Break))
            }
            "do" => ast::Stmt::Do(respan!(ok!(self.parse_do()))),
            "print" => {
                // remark, this is an error in Jinja2, but dbt swallows it...
                let ignore = ast::Stmt::Do(respan!(ok!(self.parse_do())));
                // ... so we translate it in a semantic skip!
                ast::Stmt::WithBlock(Spanned::new(
                    ast::WithBlock {
                        assignments: vec![],
                        body: vec![ignore],
                    },
                    self.stream.expand_span(span),
                ))
            }
            name => syntax_error!("unknown statement {}", name),
        })
    }

    fn parse_assign_name(&mut self, dotted: bool) -> Result<ast::Expr<'a>, Error> {
        let (id, span) = expect_token!(self, Token::Ident(name) => name, "identifier");
        if RESERVED_NAMES.contains(&id) {
            syntax_error!("cannot assign to reserved variable name {}", id);
        }
        let mut rv = ast::Expr::Var(ast::Spanned::new(ast::Var { id }, span));
        if dotted {
            while skip_token!(self, Token::Dot) {
                let (attr, span) = expect_token!(self, Token::Ident(name) => name, "identifier");
                rv = ast::Expr::GetAttr(ast::Spanned::new(
                    ast::GetAttr {
                        expr: rv,
                        name: attr,
                    },
                    span,
                ));
            }
        }
        Ok(rv)
    }

    fn parse_assignment(&mut self) -> Result<ast::Expr<'a>, Error> {
        let span = self.stream.current_span();
        let mut items = Vec::new();
        let mut is_tuple = false;

        loop {
            if !items.is_empty() {
                expect_token!(self, Token::Comma, "`,`");
            }
            if matches_token!(
                self,
                Token::ParenClose | Token::VariableEnd | Token::BlockEnd | Token::Ident("in")
            ) {
                break;
            }
            items.push(if skip_token!(self, Token::ParenOpen) {
                let rv = ok!(self.parse_assignment());
                expect_token!(self, Token::ParenClose, "`)`");
                rv
            } else {
                ok!(self.parse_assign_name(false))
            });
            if matches_token!(self, Token::Comma) {
                is_tuple = true;
            } else {
                break;
            }
        }

        if !is_tuple && items.len() == 1 {
            Ok(items.into_iter().next().unwrap())
        } else {
            Ok(ast::Expr::List(Spanned::new(
                ast::List { items },
                self.stream.expand_span(span),
            )))
        }
    }

    fn parse_for_stmt(&mut self) -> Result<ast::ForLoop<'a>, Error> {
        let old_in_loop = std::mem::replace(&mut self.in_loop, true);
        let target = ok!(self.parse_assignment());
        expect_token!(self, Token::Ident("in"), "in");
        let mut iter = ok!(self.parse_expr_noif());
        if skip_token!(self, Token::Comma) {
            let mut items = vec![iter];
            loop {
                if matches_token!(self, Token::BlockEnd) {
                    break;
                }
                items.push(ok!(self.parse_expr_noif()));
                if !skip_token!(self, Token::Comma) {
                    break;
                }
            }
            iter = ast::Expr::List(Spanned::new(
                ast::List { items },
                self.stream.current_span(),
            ));
        }
        let filter_expr = if skip_token!(self, Token::Ident("if")) {
            Some(ok!(self.parse_expr()))
        } else {
            None
        };
        let recursive = skip_token!(self, Token::Ident("recursive"));
        skip_token!(self, Token::Colon);

        expect_token!(self, Token::BlockEnd, "end of block");
        let body = ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endfor" | "else"))));
        let else_body = if skip_token!(self, Token::Ident("else")) {
            expect_token!(self, Token::BlockEnd, "end of block");
            ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endfor"))))
        } else {
            Vec::new()
        };
        ok!(self.stream.next());
        self.in_loop = old_in_loop;
        Ok(ast::ForLoop {
            target,
            iter,
            filter_expr,
            recursive,
            body,
            else_body,
        })
    }

    fn parse_if_cond(&mut self) -> Result<ast::IfCond<'a>, Error> {
        let expr = ok!(self.parse_expr_noif());
        skip_token!(self, Token::Colon);
        expect_token!(self, Token::BlockEnd, "end of block");
        let true_body =
            ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endif" | "else" | "elif"))));
        let false_body = match ok!(self.stream.next()) {
            Some((Token::Ident("else"), _)) => {
                expect_token!(self, Token::BlockEnd, "end of block");
                let rv = ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endif"))));
                ok!(self.stream.next());
                rv
            }
            Some((Token::Ident("elif"), span)) => vec![ast::Stmt::IfCond(Spanned::new(
                ok!(self.parse_if_cond()),
                self.stream.expand_span(span),
            ))],
            _ => Vec::new(),
        };

        Ok(ast::IfCond {
            expr,
            true_body,
            false_body,
        })
    }

    fn parse_with_block(&mut self) -> Result<ast::WithBlock<'a>, Error> {
        let mut assignments = Vec::new();

        while !matches_token!(self, Token::BlockEnd) {
            if !assignments.is_empty() {
                expect_token!(self, Token::Comma, "comma");
            }
            let target = if skip_token!(self, Token::ParenOpen) {
                let assign = ok!(self.parse_assignment());
                expect_token!(self, Token::ParenClose, "`)`");
                assign
            } else {
                ok!(self.parse_assign_name(false))
            };
            expect_token!(self, Token::Assign, "assignment operator");
            let expr = ok!(self.parse_expr());
            assignments.push((target, expr));
        }

        expect_token!(self, Token::BlockEnd, "end of block");
        let body = ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endwith"))));
        ok!(self.stream.next());
        Ok(ast::WithBlock { assignments, body })
    }

    // both the left hand side and right hand side can be a list
    fn parse_set(&mut self) -> Result<SetParseResult<'a>, Error> {
        let in_paren = skip_token!(self, Token::ParenOpen);
        let mut targets = Vec::new();

        if in_paren {
            let assign = ok!(self.parse_assignment());
            expect_token!(self, Token::ParenClose, "`)`");
            targets.push(assign);
        } else {
            // support {{set a, b = [1,2]}}
            loop {
                targets.push(ok!(self.parse_assign_name(true)));
                if skip_token!(self, Token::Comma) {
                    continue;
                } else {
                    break;
                }
            }
        };
        if !in_paren && matches_token!(self, Token::BlockEnd | Token::Pipe) {
            let filter = if skip_token!(self, Token::Pipe) {
                Some(ok!(self.parse_filter_chain()))
            } else {
                None
            };
            expect_token!(self, Token::BlockEnd, "end of block");
            let body = ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endset"))));
            ok!(self.stream.next());
            Ok(SetParseResult::SetBlock(ast::SetBlock {
                target: targets.into_iter().next().unwrap(),
                filter,
                body,
            }))
        } else {
            expect_token!(self, Token::Assign, "assignment operator");
            let mut exprs: Vec<ast::Expr<'a>> = Vec::new();
            // parse multiple righthand side expressions
            loop {
                exprs.push(ok!(self.parse_expr()));
                if skip_token!(self, Token::Comma) {
                    continue;
                } else {
                    break;
                }
            }
            Ok(SetParseResult::Set(ast::Set {
                target: if targets.len() == 1 {
                    targets.into_iter().next().unwrap()
                } else {
                    ast::Expr::List(Spanned::new(
                        ast::List { items: targets },
                        self.stream.current_span(),
                    ))
                },
                expr: if exprs.len() == 1 {
                    exprs.into_iter().next().unwrap()
                } else {
                    ast::Expr::List(Spanned::new(
                        ast::List { items: exprs },
                        self.stream.current_span(),
                    ))
                },
            }))
        }
    }

    #[cfg(feature = "multi_template")]
    fn parse_block(&mut self) -> Result<ast::Block<'a>, Error> {
        if self.in_macro {
            syntax_error!("block tags in macros are not allowed");
        }
        let old_in_loop = std::mem::replace(&mut self.in_loop, false);
        let (name, _) = expect_token!(self, Token::Ident(name) => name, "identifier");
        if !self.blocks.insert(name) {
            syntax_error!("block '{}' defined twice", name);
        }

        expect_token!(self, Token::BlockEnd, "end of block");
        let body = ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endblock"))));
        ok!(self.stream.next());

        if let Some((Token::Ident(trailing_name), _)) = ok!(self.stream.current()) {
            if *trailing_name != name {
                syntax_error!(
                    "mismatching name on block. Got `{}`, expected `{}`",
                    *trailing_name,
                    name
                );
            }
            ok!(self.stream.next());
        }
        self.in_loop = old_in_loop;

        Ok(ast::Block { name, body })
    }
    fn parse_auto_escape(&mut self) -> Result<ast::AutoEscape<'a>, Error> {
        let enabled = ok!(self.parse_expr());
        expect_token!(self, Token::BlockEnd, "end of block");
        let body = ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endautoescape"))));
        ok!(self.stream.next());
        Ok(ast::AutoEscape { enabled, body })
    }

    fn parse_filter_chain(&mut self) -> Result<ast::Expr<'a>, Error> {
        let mut filter = None;

        while !matches_token!(self, Token::BlockEnd) {
            if filter.is_some() {
                expect_token!(self, Token::Pipe, "`|`");
            }
            let (name, span) = expect_token!(self, Token::Ident(name) => name, "identifier");
            let args = if matches_token!(self, Token::ParenOpen) {
                ok!(self.parse_args())
            } else {
                Vec::new()
            };
            filter = Some(ast::Expr::Filter(Spanned::new(
                ast::Filter {
                    name,
                    expr: filter,
                    args,
                },
                self.stream.expand_span(span),
            )));
        }

        filter.ok_or_else(|| syntax_error(Cow::Borrowed("expected a filter")))
    }

    fn parse_filter_block(&mut self) -> Result<ast::FilterBlock<'a>, Error> {
        let filter = ok!(self.parse_filter_chain());
        expect_token!(self, Token::BlockEnd, "end of block");
        let body = ok!(self.subparse(&|tok| matches!(tok, Token::Ident("endfilter"))));
        ok!(self.stream.next());
        Ok(ast::FilterBlock { filter, body })
    }

    #[cfg(feature = "multi_template")]
    fn parse_extends(&mut self) -> Result<ast::Extends<'a>, Error> {
        let name = ok!(self.parse_expr());
        Ok(ast::Extends { name })
    }

    #[cfg(feature = "multi_template")]
    fn parse_include(&mut self) -> Result<ast::Include<'a>, Error> {
        let name = ok!(self.parse_expr());

        // with/without context is without meaning in MiniJinja, but for syntax
        // compatibility it's supported.
        if skip_token!(self, Token::Ident("without" | "with")) {
            expect_token!(self, Token::Ident("context"), "missing keyword");
        }

        let ignore_missing = if skip_token!(self, Token::Ident("ignore")) {
            expect_token!(self, Token::Ident("missing"), "missing keyword");
            if skip_token!(self, Token::Ident("without" | "with")) {
                expect_token!(self, Token::Ident("context"), "missing keyword");
            }
            true
        } else {
            false
        };
        Ok(ast::Include {
            name,
            ignore_missing,
        })
    }

    #[cfg(feature = "multi_template")]
    fn parse_import(&mut self) -> Result<ast::Import<'a>, Error> {
        let expr = ok!(self.parse_expr());
        expect_token!(self, Token::Ident("as"), "as");
        let name = ok!(self.parse_expr());
        Ok(ast::Import { expr, name })
    }

    #[cfg(feature = "multi_template")]
    fn parse_from_import(&mut self) -> Result<ast::FromImport<'a>, Error> {
        let expr = ok!(self.parse_expr());
        let mut names = Vec::new();
        expect_token!(self, Token::Ident("import"), "import");
        loop {
            if matches_token!(self, Token::BlockEnd) {
                break;
            }
            if !names.is_empty() {
                expect_token!(self, Token::Comma, "`,`");
            }
            if matches_token!(self, Token::BlockEnd) {
                break;
            }
            let name = ok!(self.parse_assign_name(false));
            let alias = if skip_token!(self, Token::Ident("as")) {
                Some(ok!(self.parse_assign_name(false)))
            } else {
                None
            };
            names.push((name, alias));
        }
        Ok(ast::FromImport { expr, names })
    }

    #[cfg(feature = "macros")]
    fn parse_macro_args_and_defaults(
        &mut self,
        args: &mut Vec<ast::Expr<'a>>,
        defaults: &mut Vec<ast::Expr<'a>>,
    ) -> Result<(), Error> {
        loop {
            if skip_token!(self, Token::ParenClose) {
                break;
            }
            if !args.is_empty() {
                expect_token!(self, Token::Comma, "`,`");
                if skip_token!(self, Token::ParenClose) {
                    break;
                }
            }
            args.push(ok!(self.parse_assign_name(false)));
            if skip_token!(self, Token::Assign) {
                defaults.push(ok!(self.parse_expr()));
            } else if !defaults.is_empty() {
                expect_token!(self, Token::Assign, "`=`");
            }
        }
        Ok(())
    }

    /// reference: https://docs.getdbt.com/guides/create-new-materializations?step=2
    /// syntax: {% materialization [materialization name], ["specified adapter" | default] %}
    #[cfg(feature = "macros")]
    fn parse_materialization_adapter_languages(
        &mut self,
        adapter: &mut Option<String>,
        supported_languages: &mut Option<ast::Expr<'a>>,
    ) -> Result<(), Error> {
        loop {
            // First check if ',' is specified
            if skip_token!(self, Token::Comma) {
                // Check if default is specified
                if matches_token!(self, Token::Ident("default")) {
                    *adapter = Some("default".to_string());
                    skip_token!(self, Token::Ident("default"));
                    break;
                }

                // Check if the adapter is specified
                if skip_token!(self, Token::Ident("adapter")) {
                    expect_token!(self, Token::Assign, "`=`");
                    let (adapter_name, _) = expect_token!(self, Token::Str(name) => name, "str");
                    *adapter = Some(adapter_name.to_string());
                // Else, check if default expression is specified (i.e. {% materialization mat_name, default %})
                } else if skip_token!(self, Token::Ident("supported_languages")) {
                    expect_token!(self, Token::Assign, "`=`");
                    *supported_languages = Some(ok!(self.parse_expr()));
                } else {
                    break;
                };
            } else {
                break;
            }
        }
        Ok(())
    }

    #[cfg(feature = "macros")]
    fn parse_macro_or_call_block_body(
        &mut self,
        args: Vec<ast::Expr<'a>>,
        defaults: Vec<ast::Expr<'a>>,
        name: Option<&'a str>,
    ) -> Result<ast::Macro<'a>, Error> {
        expect_token!(self, Token::BlockEnd, "end of block");
        let old_in_loop = std::mem::replace(&mut self.in_loop, false);
        let old_in_macro = std::mem::replace(&mut self.in_macro, true);
        let body = ok!(self.subparse(&|tok| match tok {
            Token::Ident("endmacro") if name.is_some() => true,
            Token::Ident("endcall") if name.is_none() => true,
            _ => false,
        }));
        self.in_macro = old_in_macro;
        self.in_loop = old_in_loop;
        ok!(self.stream.next());
        Ok(ast::Macro {
            name: name.unwrap_or("caller"),
            args,
            defaults,
            body,
        })
    }

    #[cfg(feature = "macros")]
    fn parse_test_or_call_block_body(
        &mut self,
        args: Vec<ast::Expr<'a>>,
        defaults: Vec<ast::Expr<'a>>,
        name: Option<&'a str>,
    ) -> Result<ast::Macro<'a>, Error> {
        expect_token!(self, Token::BlockEnd, "end of block");
        let old_in_loop = std::mem::replace(&mut self.in_loop, false);
        let old_in_macro = std::mem::replace(&mut self.in_macro, true);
        let body = ok!(self.subparse(&|tok| match tok {
            Token::Ident("endtest") if name.is_some() => true,
            Token::Ident("endcall") if name.is_none() => true,
            _ => false,
        }));
        self.in_macro = old_in_macro;
        self.in_loop = old_in_loop;
        ok!(self.stream.next());
        Ok(ast::Macro {
            name: name.unwrap_or("caller"),
            args,
            defaults,
            body,
        })
    }

    #[cfg(feature = "macros")]
    fn parse_snapshot_or_call_block_body(
        &mut self,
        name: Option<&'a str>,
    ) -> Result<ast::Macro<'a>, Error> {
        expect_token!(self, Token::BlockEnd, "end of block");
        let old_in_loop = std::mem::replace(&mut self.in_loop, false);
        let old_in_macro = std::mem::replace(&mut self.in_macro, true);
        let body = ok!(self.subparse(&|tok| match tok {
            Token::Ident("endsnapshot") if name.is_some() => true,
            Token::Ident("endcall") if name.is_none() => true,
            _ => false,
        }));
        self.in_macro = old_in_macro;
        self.in_loop = old_in_loop;
        ok!(self.stream.next());
        Ok(ast::Macro {
            name: name.unwrap_or("caller"),
            args: Vec::new(),
            defaults: Vec::new(),
            body,
        })
    }

    #[cfg(feature = "macros")]
    fn parse_doc_or_call_block_body(
        &mut self,
        args: Vec<ast::Expr<'a>>,
        defaults: Vec<ast::Expr<'a>>,
        name: Option<&'a str>,
    ) -> Result<ast::Macro<'a>, Error> {
        let old_in_loop = std::mem::replace(&mut self.in_loop, false);
        let old_in_macro = std::mem::replace(&mut self.in_macro, true);
        let body = ok!(self.subparse(&|tok| match tok {
            Token::Ident("enddocs") if name.is_some() => true,
            Token::Ident("endcall") if name.is_none() => true,
            _ => false,
        }));
        self.in_macro = old_in_macro;
        self.in_loop = old_in_loop;
        ok!(self.stream.next());
        Ok(ast::Macro {
            name: name.unwrap_or("caller"),
            args,
            defaults,
            body,
        })
    }

    #[cfg(feature = "macros")]
    fn parse_materialization_or_call_block_body(
        &mut self,
        name: Option<&'a str>,
    ) -> Result<ast::Macro<'a>, Error> {
        expect_token!(self, Token::BlockEnd, "end of block");
        let old_in_loop = std::mem::replace(&mut self.in_loop, false);
        let old_in_macro = std::mem::replace(&mut self.in_macro, true);
        let body = ok!(self.subparse(&|tok| match tok {
            Token::Ident("endmaterialization") if name.is_some() => true,
            Token::Ident("endcall") if name.is_none() => true,
            _ => false,
        }));
        self.in_macro = old_in_macro;
        self.in_loop = old_in_loop;
        ok!(self.stream.next());
        Ok(ast::Macro {
            name: name.unwrap_or("caller"),
            args: Vec::new(),
            defaults: Vec::new(),
            body,
        })
    }

    #[cfg(feature = "macros")]
    fn parse_macro(&mut self) -> Result<ast::Macro<'a>, Error> {
        let (name, _) = expect_token!(self, Token::Ident(name) => name, "identifier");
        expect_token!(self, Token::ParenOpen, "`(`");
        let mut args = Vec::new();
        let mut defaults = Vec::new();
        ok!(self.parse_macro_args_and_defaults(&mut args, &mut defaults));
        self.parse_macro_or_call_block_body(args, defaults, Some(name))
    }

    #[cfg(feature = "macros")]
    fn parse_test(&mut self) -> Result<ast::Macro<'a>, Error> {
        let (name, _) = expect_token!(self, Token::Ident(name) => name, "identifier");
        // Assuming self has access to an arena or string interner
        let macro_name = self.intern_string(&format!("test_{}", name));
        expect_token!(self, Token::ParenOpen, "`(`");
        let mut args = Vec::new();
        let mut defaults = Vec::new();
        ok!(self.parse_macro_args_and_defaults(&mut args, &mut defaults));
        self.parse_test_or_call_block_body(args, defaults, Some(macro_name))
    }

    #[cfg(feature = "macros")]
    fn parse_snapshot(&mut self) -> Result<ast::Macro<'a>, Error> {
        let (name, _) = expect_token!(self, Token::Ident(name) => name, "identifier");
        // Assuming self has access to an arena or string interner
        let macro_name = self.intern_string(&format!("snapshot_{}", name));
        self.parse_snapshot_or_call_block_body(Some(macro_name))
    }

    #[cfg(feature = "macros")]
    fn parse_doc(&mut self) -> Result<ast::Macro<'a>, Error> {
        let (name, _) = expect_token!(self, Token::Ident(name) => name, "identifier");

        // Skip everything until BlockEnd, advancing on Token Errors
        // This is specifically because doc macros can have random
        // tokens, characters, spaces, etc. in the names
        // (i.e. {% doc package.doc_name %%% $$$$ hehehe %} is valid)
        // TODO(alex): One day deprecate this nonsense as it really a parsing bug in dbt
        // that is unreported (but let's bug for bug repro fs for now)
        loop {
            match self.stream.next() {
                Ok(Some((Token::BlockEnd, _))) => break,
                Ok(_) => continue,
                Err(_) => {
                    self.stream.tokenizer.force_advance(1)?;
                    continue;
                }
            }
        }

        self.parse_doc_or_call_block_body(Vec::new(), Vec::new(), Some(name))
    }

    #[cfg(feature = "macros")]
    fn parse_materialization(&mut self) -> Result<(ast::Macro<'a>, Option<String>), Error> {
        let (name, _) = expect_token!(self, Token::Ident(name) => name, "identifier");
        let mut adapter = None;
        let mut supported_languages = None;
        ok!(self.parse_materialization_adapter_languages(&mut adapter, &mut supported_languages));
        let macro_name = self.intern_string(&format!(
            "materialization_{}_{}",
            name,
            // TODO: This can be cleaned up to add better error messages for miss-formatted materialization macros
            adapter.as_deref().unwrap_or("default")
        ));
        Ok((
            ok!(self.parse_materialization_or_call_block_body(Some(macro_name))),
            adapter,
        ))
    }

    #[cfg(feature = "macros")]
    fn parse_call_block(&mut self) -> Result<ast::CallBlock<'a>, Error> {
        let span = self.stream.last_span();
        let mut args = Vec::new();
        let mut defaults = Vec::new();
        if skip_token!(self, Token::ParenOpen) {
            ok!(self.parse_macro_args_and_defaults(&mut args, &mut defaults));
        }
        let call = match ok!(self.parse_expr()) {
            ast::Expr::Call(call) => call,
            expr => syntax_error!(
                "expected call expression in call block, got {}",
                expr.description()
            ),
        };
        let macro_decl = ok!(self.parse_macro_or_call_block_body(args, defaults, None));
        Ok(ast::CallBlock {
            call,
            macro_decl: Spanned::new(macro_decl, self.stream.expand_span(span)),
        })
    }

    fn parse_do(&mut self) -> Result<ast::Do<'a>, Error> {
        let expr = ok!(self.parse_expr());
        Ok(ast::Do { expr })
    }

    fn subparse(
        &mut self,
        end_check: &dyn Fn(&Token) -> bool,
    ) -> Result<Vec<ast::Stmt<'a>>, Error> {
        let mut rv = Vec::new();
        while let Some((token, span)) = ok!(self.stream.next()) {
            match token {
                Token::TemplateData(raw) => {
                    rv.push(ast::Stmt::EmitRaw(Spanned::new(ast::EmitRaw { raw }, span)))
                }
                Token::VariableStart => {
                    let expr = ok!(self.parse_expr_or_implied_tuple());
                    expect_token!(self, Token::VariableEnd, "end of variable block");
                    macro_rules! expand_span {
                        ($expr:expr, $variant:path) => {{
                            let mut spanned = $expr;
                            spanned.span = self.stream.expand_span(span);
                            $variant(spanned)
                        }};
                    }

                    macro_rules! match_expr_variants {
                        ($expr:expr, $($variant:ident),*) => {
                            match $expr {
                                $(
                                    ast::Expr::$variant(spanned) => expand_span!(spanned, ast::Expr::$variant),
                                )*
                            }
                        };
                    }

                    let expr = match_expr_variants!(
                        expr, Var, Const, Slice, UnaryOp, BinOp, IfExpr, Filter, Test, GetAttr,
                        GetItem, Call, List, Map, Tuple
                    );
                    rv.push(ast::Stmt::EmitExpr(Spanned::new(
                        ast::EmitExpr { expr },
                        self.stream.expand_span(span),
                    )));
                }
                Token::BlockStart => {
                    let (tok, _span) = match ok!(self.stream.current()) {
                        Some(rv) => rv,
                        None => syntax_error!("unexpected end of input, expected keyword"),
                    };
                    if end_check(tok) {
                        return Ok(rv);
                    }
                    rv.push(ok!(self.parse_stmt()));
                    // WARNING: this is a dangerous change.
                    // expect_token!(self, Token::BlockEnd, "end of block");
                }
                Token::Comment => {
                    rv.push(ast::Stmt::Comment(Spanned::new(
                        Comment,
                        self.stream.expand_span(span),
                    )));
                }

                _ => unreachable!("lexer produced garbage"),
            }
        }
        Ok(rv)
    }

    #[inline]
    fn attach_location_to_error(&mut self, mut err: Error) -> Error {
        if err.line().is_none() {
            err.insert_filename_and_span(self.filename(), self.stream.last_span())
        }
        err
    }

    fn intern_string(&mut self, s: &str) -> &'a str {
        if let Some(interned) = self.string_interner.get(s) {
            return interned;
        }

        // Create a new string in the same allocation as source
        let new_str = {
            let owned = s.to_string();
            let boxed = Box::leak(owned.into_boxed_str());
            let reference: &'a str = boxed;
            reference
        };

        self.string_interner.insert(s.to_string(), new_str);
        new_str
    }

    /// Parses only top-level statements of a specific type.
    /// Ignores all other statements and expressions between the start and end tags.
    pub fn parse_top_level_statements(
        &mut self,
        statement_type: &[&str],
    ) -> Result<ast::Stmt<'a>, Error> {
        let span = self.stream.last_span();
        self.subparse_top_level(&|_tok| false, statement_type)
            .map(|children| {
                ast::Stmt::Template(Spanned::new(
                    ast::Template { children },
                    self.stream.expand_span(span),
                ))
            })
            .map_err(|err| self.attach_location_to_error(err))
    }

    fn subparse_top_level(
        &mut self,
        end_check: &dyn Fn(&Token) -> bool,
        statement_type: &[&str],
    ) -> Result<Vec<ast::Stmt<'a>>, Error> {
        let mut rv = Vec::new();
        while let Some((token, _span)) = ok!(self.stream.next()) {
            match token {
                Token::BlockStart => {
                    let (tok, _span) = match ok!(self.stream.current()) {
                        Some(rv) => rv,
                        None => syntax_error!("unexpected end of input, expected keyword"),
                    };

                    if end_check(tok) {
                        return Ok(rv);
                    }

                    // Only parse statements that match our target type
                    if let Token::Ident(ident) = tok {
                        if statement_type.contains(ident) {
                            rv.push(ok!(self.parse_stmt()));
                        } else {
                            // Skip until we find the end of this block
                            self.skip_until_block_end()?;
                        }
                    } else {
                        // Skip non-identifier tokens
                        self.skip_until_block_end()?;
                    }
                }
                // Skip all other token types
                _ => continue,
            }
        }
        Ok(rv)
    }

    fn skip_until_block_end(&mut self) -> Result<(), Error> {
        let mut depth = 1;
        while let Some((token, _)) = ok!(self.stream.next()) {
            match token {
                Token::BlockStart => depth += 1,
                Token::BlockEnd => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => continue,
            }
        }
        Ok(())
    }
}

/// Parses a template.
pub fn parse<'source>(
    source: &'source str,
    filename: &'source str,
    syntax_config: SyntaxConfig,
    whitespace_config: WhitespaceConfig,
) -> Result<ast::Stmt<'source>, Error> {
    Parser::new(source, filename, false, syntax_config, whitespace_config).parse()
}

/// Parses a standalone expression.
pub fn parse_expr(source: &str) -> Result<ast::Expr<'_>, Error> {
    Parser::new(
        source,
        "<expression>",
        true,
        Default::default(),
        Default::default(),
    )
    .parse_standalone_expr()
}
