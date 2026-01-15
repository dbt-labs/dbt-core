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
            ' ' | '\t' | '\r' => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                }
                current_token = Token::new();
            }
            '\n' => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                }
                tokens.push(Token {
                    value: "\n".to_string(),
                    maybe_hash: false,
                });
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
#[allow(clippy::cognitive_complexity)]
pub(crate) fn abstract_tokenize(tokens: Vec<Token>) -> Vec<AbstractToken> {
    let mut abstract_tokens = Vec::new();
    let mut index = 0;
    let timestamp_regex = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}").unwrap();
    // Same as `timestamp_regex`, but for finding timestamps embedded in larger tokens
    // (e.g. when surrounded by quotes/parens).
    let timestamp_find_regex = Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}").unwrap();
    let timestamp_regex_ignore_t = Regex::new(r"^\d{4}-\d{2}-\d{2}\d{2}:\d{2}:\d{2}$").unwrap();
    let date_regex = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    let time_regex = Regex::new(r"^\d{2}:\d{2}:\d{2}$").unwrap();
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
        } else if let Some(m) = timestamp_find_regex.find(&token.value) {
            // Handle timestamps that don't start at a fixed suffix boundary, e.g.:
            //   cast('2025-12-23T07:06:03' as timestamp)
            // where the token may include leading '(' / '\'' and trailing '\''.
            let (first, last) = token.value.split_at(m.start());
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
        } else if token.value.chars().count() >= 19
            && let Some(start_byte) = token.value.char_indices().rev().nth(18).map(|(i, _)| i)
            && timestamp_regex.is_match(&token.value[start_byte..])
        {
            let (first, last) = token.value.split_at(start_byte);
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
        } else if token.value.chars().count() >= 18
            && let Some(start_byte) = token.value.char_indices().rev().nth(17).map(|(i, _)| i)
            && timestamp_regex_ignore_t.is_match(&token.value[start_byte..])
        {
            let (first, last) = token.value.split_at(start_byte);
            if !first.is_empty() {
                abstract_tokens.push(AbstractToken::Token(Token {
                    value: first.to_string(),
                    maybe_hash: false,
                }));
            }
            let timestamp_token = format!("{}T{}", &last[..10], &last[10..]);
            if let Some(next_token) = tokens.get(index + 1)
                && next_token.matches(".")
                && let Some(next_next_token) = tokens.get(index + 2)
            {
                let timestamp_token = timestamp_token + &next_token.value + &next_next_token.value;
                abstract_tokens.push(AbstractToken::Timestamp {
                    value: timestamp_token,
                });
                index += 3;
            } else {
                abstract_tokens.push(AbstractToken::Timestamp {
                    value: timestamp_token,
                });
                index += 1;
            }
        } else if token.value.chars().count() >= 10
            && let Some(start_byte) = token.value.char_indices().rev().nth(9).map(|(i, _)| i)
            && date_regex.is_match(&token.value[start_byte..])
        {
            let (first, last) = token.value.split_at(start_byte);
            if !first.is_empty() {
                abstract_tokens.push(AbstractToken::Token(Token {
                    value: first.to_string(),
                    maybe_hash: false,
                }));
            }

            let date_token = last.to_string();
            if let Some(next_token) = tokens.get(index + 1)
                && time_regex.is_match(&next_token.value)
            {
                let timestamp_token = format!("{date_token}T{}", next_token.value);
                if let Some(next2_token) = tokens.get(index + 2)
                    && next2_token.matches(".")
                    && let Some(next3_token) = tokens.get(index + 3)
                {
                    let timestamp_token = timestamp_token + &next2_token.value + &next3_token.value;
                    abstract_tokens.push(AbstractToken::Timestamp {
                        value: timestamp_token,
                    });
                    index += 4;
                } else {
                    abstract_tokens.push(AbstractToken::Timestamp {
                        value: timestamp_token,
                    });
                    index += 2;
                }
            } else {
                abstract_tokens.push(AbstractToken::Token(Token {
                    value: date_token,
                    maybe_hash: false,
                }));
                index += 1;
            }
        } else {
            abstract_tokens.push(AbstractToken::Token(token.clone()));
            index += 1;
        }
    }

    abstract_tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_basic() {
        let input = "hello world";
        let tokens = tokenize(input);
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].value, "hello");
        assert_eq!(tokens[1].value, "world");
    }

    #[test]
    fn test_abstract_tokenize_with_unicode_date() {
        // Test that unicode characters in tokens don't cause boundary errors
        // when checking date patterns at the end
        let tokens = vec![
            Token {
                value: "prefix日2023-01-01".to_string(),
                maybe_hash: false,
            },
            Token {
                value: "12:34:56".to_string(),
                maybe_hash: false,
            },
        ];
        let abstract_tokens = abstract_tokenize(tokens);
        // Should split into Token("prefix日") and Timestamp("2023-01-01T12:34:56")
        assert_eq!(abstract_tokens.len(), 2);
        match &abstract_tokens[0] {
            AbstractToken::Token(t) => assert_eq!(t.value, "prefix日"),
            _ => panic!("Expected Token"),
        }
        match &abstract_tokens[1] {
            AbstractToken::Timestamp { value } => assert_eq!(value, "2023-01-01T12:34:56"),
            _ => panic!("Expected Timestamp"),
        }
    }

    #[test]
    fn test_abstract_tokenize_with_unicode_timestamp() {
        // Test timestamp with unicode in prefix
        let token = Token {
            value: "prefix日2023-01-01T12:34:56".to_string(),
            maybe_hash: false,
        };
        let abstract_tokens = abstract_tokenize(vec![token]);
        assert_eq!(abstract_tokens.len(), 2);
        match &abstract_tokens[0] {
            AbstractToken::Token(t) => assert_eq!(t.value, "prefix日"),
            _ => panic!("Expected Token"),
        }
        match &abstract_tokens[1] {
            AbstractToken::Timestamp { value } => assert_eq!(value, "2023-01-01T12:34:56"),
            _ => panic!("Expected Timestamp"),
        }
    }
}
