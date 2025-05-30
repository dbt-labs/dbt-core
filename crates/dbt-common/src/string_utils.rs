/// Splits the input string into words, but treats every {..} as one word
pub fn split_into_whitespace_and_brackets(input: &str) -> Vec<String> {
    // Used to prepare programmatic commandline parsing
    let mut results = Vec::new();
    let input = input.replace('\n', " ");
    let mut braces_ct = 0;
    let mut segment = String::new();
    // splut input into sections of  any str, {, any string '}'
    for ch in input.chars() {
        match ch {
            '{' => {
                if braces_ct > 0 {
                    segment.push(ch);
                    braces_ct += 1;
                } else {
                    if !segment.is_empty() {
                        results.push(segment);
                        segment = String::new();
                    }
                    segment.push(ch);
                    braces_ct += 1;
                }
            }
            '}' => match braces_ct.cmp(&1) {
                std::cmp::Ordering::Greater => {
                    segment.push(ch);
                    braces_ct -= 1;
                }
                std::cmp::Ordering::Equal => {
                    segment.push(ch);
                    results.push(segment);
                    segment = String::new();
                    braces_ct -= 1;
                }
                std::cmp::Ordering::Less => {
                    segment.push(ch);
                }
            },
            _ => segment.push(ch),
        }
    }
    if !segment.is_empty() {
        results.push(segment);
    }
    let segments = results;
    let mut results = Vec::new();
    for segment in segments {
        // let segment = segment.trim().to_string();
        if segment.starts_with('{') {
            results.push(segment);
            continue;
        }
        let words = segment.split_whitespace();
        for word in words {
            results.push(word.to_string());
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_simple() {
        let input = "hello world";
        let expected = vec!["hello", "world"];
        assert_eq!(split_into_whitespace_and_brackets(input), expected);
    }

    #[test]
    fn test_split_with_braces() {
        let input = "hello {world}";
        let expected = vec!["hello", "{world}"];
        assert_eq!(split_into_whitespace_and_brackets(input), expected);
    }

    #[test]
    fn test_split_with_quotes() {
        let input = r#"hello "world""#;
        let expected = vec!["hello", "\"world\""];
        assert_eq!(split_into_whitespace_and_brackets(input), expected);
    }

    #[test]
    fn test_split_with_braces_and_quotes() {
        let input = r#"hello { "world "  }"#;
        let expected = vec!["hello", "{ \"world \"  }"];
        assert_eq!(split_into_whitespace_and_brackets(input), expected);
    }
}
