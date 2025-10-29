use regex::Regex;
#[derive(Clone)]
pub(crate) struct Token {
    pub value: String,
    pub(crate) maybe_hash: bool,
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl Token {
    pub fn new() -> Self {
        Self {
            value: String::new(),
            maybe_hash: true,
        }
    }

    pub fn append(&mut self, c: char) {
        self.value.push(c);
        // check if c is 0-9 or a-f
        if !(c.is_ascii_digit() || ('a'..='f').contains(&c)) {
            self.maybe_hash = false;
        }
    }

    pub fn is_very_likely_hash(&self) -> bool {
        self.maybe_hash && self.value.len() == 32
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn matches(&self, other: &str) -> bool {
        self.value == other
    }
}

pub(crate) fn tokenize(actual: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut current_token = Token::new();

    for c in actual.chars() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                }
                current_token = Token::new();
            }
            '.' | '_' => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                }
                let mut single_char_token = Token::new();
                single_char_token.append(c);
                tokens.push(single_char_token);
                current_token = Token::new();
            }
            _ => {
                current_token.append(c);
                if current_token.is_very_likely_hash() {
                    tokens.push(current_token);
                    current_token = Token::new();
                }
            }
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    tokens
}

#[derive(Clone)]
pub(crate) enum AbstractToken {
    Token(Token),
    Hash { prefix: String, hash: String },
    Timestamp { value: String },
}

impl std::fmt::Debug for AbstractToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Token(arg0) => write!(f, "{}", arg0.value),
            Self::Hash { prefix, hash } => write!(f, "{prefix}_{hash}"),
            Self::Timestamp { value } => write!(f, "{value}"),
        }
    }
}

/// abstract tokens is to concatenate the prefix and hash together
pub(crate) fn abstract_tokenize(tokens: Vec<Token>) -> Vec<AbstractToken> {
    let mut abstract_tokens = Vec::new();
    let mut index = 0;
    let timestamp_regex = Regex::new(r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}$").unwrap();
    while index < tokens.len() {
        let token = tokens.get(index).unwrap();
        if token.matches("_") {
            // check if the next token is a hash
            if tokens
                .get(index + 1)
                .map(|t| t.is_very_likely_hash())
                .unwrap_or(false)
            {
                // look back and find 30 characters
                let mut prefix = String::new();
                let mut i = index - 1;
                while prefix.len() < 30 {
                    if let Some(token_in_the_past) = tokens.get(i) {
                        if token_in_the_past.matches(".") {
                            break;
                        }
                        prefix = token_in_the_past.value.clone() + &prefix;
                        i -= 1;
                    } else {
                        break;
                    }
                }
                if prefix.len() >= 30 {
                    // pop index - i tokens from abstract_tokens
                    abstract_tokens.truncate(abstract_tokens.len() - (index - i - 1));
                    if prefix.len() > 30 {
                        let token = AbstractToken::Token(Token {
                            value: prefix[..prefix.len() - 30].to_string(),
                            maybe_hash: false,
                        });
                        abstract_tokens.push(token);
                        prefix = prefix[prefix.len() - 30..].to_string();
                    }
                    index += 1;
                    let hash_token = tokens.get(index).unwrap();
                    let token = AbstractToken::Hash {
                        prefix,
                        hash: hash_token.value.clone(),
                    };
                    abstract_tokens.push(token);
                } else {
                    abstract_tokens.push(AbstractToken::Token(token.clone()));
                }
            } else {
                abstract_tokens.push(AbstractToken::Token(token.clone()));
            }
            index += 1;
        } else if token.value.len() >= 19
            && timestamp_regex.is_match(&token.value[token.value.len() - 19..])
        {
            let (first, last) = token.value.split_at(token.value.len() - 19);
            if !first.is_empty() {
                abstract_tokens.push(AbstractToken::Token(Token {
                    value: first.to_string(),
                    maybe_hash: false,
                }));
            }

            let mut timestamp_token = last.to_string();
            if let Some(next_token) = tokens.get(index + 1)
                && next_token.matches(".")
                && let Some(next_next_token) = tokens.get(index + 2)
            {
                timestamp_token = timestamp_token + &next_token.value + &next_next_token.value;
                index += 3;
            } else {
                index += 1;
            }
            abstract_tokens.push(AbstractToken::Timestamp {
                value: timestamp_token,
            });
        } else if token.matches("--EPHEMERAL-SELECT-WRAPPER-START") {
            assert!(tokens.get(index + 1).unwrap().matches("select"));
            assert!(tokens.get(index + 2).unwrap().matches("*"));
            assert!(tokens.get(index + 3).unwrap().matches("from"));
            assert!(tokens.get(index + 4).unwrap().matches("("));
            index += 5;
            if tokens.get(index).unwrap().matches("with") {
                index += 1;
                abstract_tokens.push(AbstractToken::Token(Token {
                    value: ",".to_string(),
                    maybe_hash: false,
                }));
            }
        } else if token.matches("--EPHEMERAL-SELECT-WRAPPER-END") {
            assert!(tokens.get(index + 1).unwrap().matches(")"));
            index += 2;
        } else {
            abstract_tokens.push(AbstractToken::Token(token.clone()));
            index += 1;
        }
    }

    abstract_tokens
}
