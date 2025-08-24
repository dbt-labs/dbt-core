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

/// Truncates a test name to 63 characters if it's too long, following dbt-core's logic.
/// This is done by including the first 30 identifying chars plus a 32-character hash of the full contents.
/// See the function `synthesize_generic_test_name` in `dbt-core`:
/// https://github.com/dbt-labs/dbt-core/blob/9010537499980743503ed3b462eb1952be4d2b38/core/dbt/parser/generic_test_builders.py
pub fn maybe_truncate_test_name(test_identifier: &str, full_name: &str) -> String {
    if full_name.len() >= 64 {
        let test_trunc_identifier: String = test_identifier.chars().take(30).collect();
        let hash = md5::compute(full_name);
        let res: String = format!("{test_trunc_identifier}_{hash:x}");
        res
    } else {
        full_name.to_string()
    }
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
