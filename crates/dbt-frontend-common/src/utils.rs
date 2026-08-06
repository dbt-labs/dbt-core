use itertools::Itertools;

const HASH_SIZE: usize = 16;
const VERSION_SIZE: usize = 17;

pub fn canonicalize_relation_name(relation: &str) -> String {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut quoted = false;
    for c in relation.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => current.push(c),
            None if c == '"' || c == '`' => {
                quote = Some(c);
                quoted = true;
            }
            None if c == '.' => {
                let segment = std::mem::take(&mut current);
                segments.push(if quoted {
                    segment
                } else {
                    segment.to_uppercase()
                });
                quoted = false;
            }
            None => current.push(c),
        }
    }
    segments.push(if quoted {
        current
    } else {
        current.to_uppercase()
    });
    segments.join(".")
}

pub fn strip_version_hash(
    table_name: &str,
    version: &Option<String>,
    hash: &Option<String>,
) -> String {
    if let Some(version) = version {
        let suffix = if let Some(hash) = hash {
            format!("_{version}_{hash}")
        } else {
            format!("_{version}")
        };

        table_name
            .strip_suffix(&suffix)
            .unwrap_or(table_name)
            .to_string()
    } else {
        table_name.to_string()
    }
}

pub fn get_version_hash(table_name: &str) -> (Option<String>, Option<String>) {
    let parts = table_name.split('_').collect_vec();
    if parts.len() > 1 {
        let suffix = (*parts.last().unwrap()).to_string();
        if suffix.len() == HASH_SIZE {
            // Get second to last part
            if let Some(maybe_version) = parts.get(parts.len() - 2) {
                if maybe_version.len() == VERSION_SIZE
                    && maybe_version.chars().all(|c| c.is_ascii_digit())
                {
                    (Some((*maybe_version).to_string()), Some(suffix))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        } else if suffix.len() == VERSION_SIZE  // we have a version but no hash
            && suffix.chars().all(|c| c.is_ascii_digit())
        {
            (Some(suffix), None)
        } else {
            (None, None)
        }
    } else {
        (None, None)
    }
}

#[cfg(test)]
mod canonicalize_relation_name_tests {
    use super::canonicalize_relation_name;

    #[test]
    fn unquoted_segments_fold_to_uppercase() {
        assert_eq!(canonicalize_relation_name("foo.bar"), "FOO.BAR");
    }

    #[test]
    fn quoted_segments_preserve_case() {
        assert_eq!(canonicalize_relation_name(r#""Foo"."Bar""#), "Foo.Bar");
    }

    #[test]
    fn mixed_quoted_and_unquoted_segments() {
        assert_eq!(
            canonicalize_relation_name("`MyProject`.dataset.`MyTable`"),
            "MyProject.DATASET.MyTable"
        );
    }

    #[test]
    fn backtick_quoted_full_path_with_internal_dots_is_not_re_split() {
        assert_eq!(
            canonicalize_relation_name("`project.dataset.table`"),
            "project.dataset.table"
        );
    }

    #[test]
    fn case_distinct_quoted_relations_no_longer_collide() {
        assert_ne!(
            canonicalize_relation_name(r#""Users""#),
            canonicalize_relation_name(r#""users""#)
        );
    }
}
