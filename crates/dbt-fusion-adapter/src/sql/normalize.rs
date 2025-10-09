pub(crate) fn strip_sql_comments(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut last_output_whitespace = true;
    let mut pending_space = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                result.push('\n');
                last_output_whitespace = true;
                pending_space = false;
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && chars.peek().is_some_and(|next| *next == '/') {
                let _ = chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if pending_space {
            if ch == '\n' || ch.is_whitespace() {
                pending_space = false;
            } else if !result.is_empty() {
                result.push(' ');
                last_output_whitespace = true;
                pending_space = false;
            }
        }

        if in_single {
            result.push(ch);
            if ch == '\'' {
                if chars.peek().is_some_and(|next| *next == '\'') {
                    result.push('\'');
                    let _ = chars.next();
                } else {
                    in_single = false;
                }
            }
            last_output_whitespace = false;
            continue;
        }

        if in_double {
            result.push(ch);
            if ch == '"' {
                if chars.peek().is_some_and(|next| *next == '"') {
                    result.push('"');
                    let _ = chars.next();
                } else {
                    in_double = false;
                }
            }
            last_output_whitespace = false;
            continue;
        }

        match ch {
            '-' if chars.peek().is_some_and(|next| *next == '-') => {
                let _ = chars.next();
                in_line_comment = true;
                if !last_output_whitespace && !result.is_empty() {
                    pending_space = true;
                }
            }
            '/' if chars.peek().is_some_and(|next| *next == '*') => {
                let _ = chars.next();
                in_block_comment = true;
                if !last_output_whitespace && !result.is_empty() {
                    pending_space = true;
                }
            }
            '\'' => {
                in_single = true;
                result.push('\'');
                last_output_whitespace = false;
            }
            '"' => {
                in_double = true;
                result.push('"');
                last_output_whitespace = false;
            }
            '\n' => {
                result.push('\n');
                last_output_whitespace = true;
                pending_space = false;
            }
            _ if ch.is_whitespace() => {
                if !last_output_whitespace {
                    result.push(' ');
                    last_output_whitespace = true;
                }
            }
            _ => {
                result.push(ch);
                last_output_whitespace = false;
            }
        }
    }

    result
}
