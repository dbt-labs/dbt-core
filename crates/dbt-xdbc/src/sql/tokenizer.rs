use core::fmt;

#[derive(Debug)]
pub enum Token<'source> {
    LParen,
    RParen,
    /// Left bracket: [.
    LBracket,
    /// Right bracket: ].
    RBracket,
    /// Left angled bracket: <.
    LAngle,
    /// Right angled bracket: >.
    RAngle,
    Comma,
    Word(&'source str),
}

impl Eq for Token<'_> {}

impl PartialEq for Token<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Token::LParen, Token::LParen) => true,
            (Token::RParen, Token::RParen) => true,
            (Token::LBracket, Token::LBracket) => true,
            (Token::RBracket, Token::RBracket) => true,
            (Token::LAngle, Token::LAngle) => true,
            (Token::RAngle, Token::RAngle) => true,
            (Token::Comma, Token::Comma) => true,
            (Token::Word(a), Token::Word(b)) => a.eq_ignore_ascii_case(b),
            _ => false,
        }
    }
}

impl fmt::Display for Token<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::LAngle => write!(f, "<"),
            Token::RAngle => write!(f, ">"),
            Token::Comma => write!(f, ","),
            Token::Word(w) => write!(f, "{w}"),
        }
    }
}

fn is_whitespace(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\n' || c == b'\r'
}

pub struct Tokenizer<'source> {
    input: &'source str,
    position: usize,
}

impl<'source> Tokenizer<'source> {
    pub fn new(input: &'source str) -> Self {
        Tokenizer { input, position: 0 }
    }

    /// Looks at the current byte without consuming it.
    fn _peek_byte(&self) -> Option<u8> {
        let input = self.input.as_bytes();
        input.get(self.position).copied()
    }

    /// Consumes the current byte and returns it.
    fn _next_byte(&mut self) -> Option<u8> {
        self._peek_byte().inspect(|_| {
            self.position += 1;
        })
    }

    fn skip_whitespace(&mut self) {
        while let Some(b) = self._peek_byte() {
            if is_whitespace(b) {
                self.position += 1;
            } else {
                break;
            }
        }
    }

    /// Consumes the next token from the input.
    pub fn next(&mut self) -> Option<Token<'source>> {
        self.skip_whitespace();
        let start = self.position;
        if let Some(b) = self._next_byte() {
            match b {
                b'(' => return Some(Token::LParen),
                b')' => return Some(Token::RParen),
                b'[' => return Some(Token::LBracket),
                b']' => return Some(Token::RBracket),
                b'<' => return Some(Token::LAngle),
                b'>' => return Some(Token::RAngle),
                b',' => return Some(Token::Comma),
                _ => (),
            }
        }
        while let Some(b) = self._peek_byte() {
            match b {
                b'(' | b')' | b'[' | b']' | b'<' | b'>' | b',' => break,
                _ if is_whitespace(b) => break,
                _ => {
                    self.position += 1;
                    continue;
                }
            }
        }
        // SAFETY: this is a valid UTF8 slice because breaks
        // only occur on whitespece or delimiter characters.
        let word = &self.input[start..self.position];
        if start == self.position {
            None
        } else {
            Some(Token::Word(word))
        }
    }

    /// Consumes the next token if and only if it matches the provided token.
    pub fn match_(&mut self, pat: Token) -> bool {
        let old_pos = self.position;
        if let Some(tok) = self.next() {
            if tok == pat {
                return true;
            }
        }
        self.position = old_pos;
        false
    }

    /// Peeks at the next token and applies the provided function to it. If the function
    /// returns `None`, the tokenizer's position is reset to its previous state.
    pub fn peek_and_then<T>(&mut self, f: impl FnOnce(Token<'source>) -> Option<T>) -> Option<T> {
        let old_pos = self.position;
        if let Some(tok) = self.next() {
            let res = f(tok);
            if res.is_some() {
                return res;
            }
        }
        self.position = old_pos;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer() {
        let mut tokenizer = Tokenizer::new("");
        assert_eq!(tokenizer.next(), None);

        let mut tokenizer = Tokenizer::new("  ");
        assert_eq!(tokenizer.next(), None);

        let mut tokenizer = Tokenizer::new("BOOLEAN");
        assert_eq!(tokenizer.next(), Some(Token::Word("BOOLEAN")));

        let mut tokenizer = Tokenizer::new("  BOOLEAN");
        assert_eq!(tokenizer.next(), Some(Token::Word("BOOLEAN")));

        let mut tokenizer = Tokenizer::new("BOOLEAN  ");
        assert_eq!(tokenizer.next(), Some(Token::Word("BOOLEAN")));

        let mut tokenizer = Tokenizer::new("  BOOLEAN  ");
        assert_eq!(tokenizer.next(), Some(Token::Word("BOOLEAN")));

        let mut tokenizer = Tokenizer::new("TIMESTAMP (3) WITH TIME ZONE");
        assert_eq!(tokenizer.next(), Some(Token::Word("TIMESTAMP")));
        assert_eq!(tokenizer.next(), Some(Token::LParen));
        assert_eq!(tokenizer.next(), Some(Token::Word("3")));
        assert_eq!(tokenizer.next(), Some(Token::RParen));
        assert_eq!(tokenizer.next(), Some(Token::Word("WITH")));
        assert_eq!(tokenizer.next(), Some(Token::Word("TIME")));
        assert_eq!(tokenizer.next(), Some(Token::Word("ZONE")));

        let mut tokenizer = Tokenizer::new("STRUCT<a FLOAT64>");
        assert_eq!(tokenizer.next(), Some(Token::Word("STRUCT")));
        assert_eq!(tokenizer.next(), Some(Token::LAngle));
        assert_eq!(tokenizer.next(), Some(Token::Word("a")));
        assert_eq!(tokenizer.next(), Some(Token::Word("FLOAT64")));
        assert_eq!(tokenizer.next(), Some(Token::RAngle));

        let mut tokenizer = Tokenizer::new("☃");
        assert_eq!(tokenizer.next(), Some(Token::Word("☃")));

        let mut tokenizer = Tokenizer::new("☃☃");
        assert_eq!(tokenizer.next(), Some(Token::Word("☃☃")));

        let mut tokenizer = Tokenizer::new("☃SNOWMAN☃(1)");
        assert_eq!(tokenizer.next(), Some(Token::Word("☃SNOWMAN☃")));
        assert_eq!(tokenizer.next(), Some(Token::LParen));
        assert_eq!(tokenizer.next(), Some(Token::Word("1")));
        assert_eq!(tokenizer.next(), Some(Token::RParen));

        let mut tokenizer = Tokenizer::new("S☃NOWMA☃N");
        assert_eq!(tokenizer.next(), Some(Token::Word("S☃NOWMA☃N")));
    }
}
