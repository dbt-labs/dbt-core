use dbt_frontend_common::{
    ident::{Ident, Identifier},
    Dialect, Qualified, QualifiedName,
};
use itertools::Itertools;
use std::sync::Arc;

pub const SDF_IMPLICIT_QUALIFIER: &str = "__sdf_implicit_qualifier";
pub const SDF_IMPLICIT_UNAVAILABLE_QUALIFIER: &str = "__sdf_implicit_unavailable_qualifier";

pub struct NameCandidate {
    pub name: Identifier,
    pub qualifier: Option<QualifiedName>,
}

impl
    From<(
        Option<&datafusion::sql::TableReference>,
        &Arc<arrow_schema::Field>,
    )> for NameCandidate
{
    fn from(
        (qualifier, field): (
            Option<&datafusion::sql::TableReference>,
            &Arc<arrow_schema::Field>,
        ),
    ) -> Self {
        NameCandidate {
            name: field.name().to_string().into(),
            qualifier: qualifier.map(|q| QualifiedName::from(q.clone())),
        }
    }
}

impl From<String> for NameCandidate {
    fn from(name: String) -> Self {
        NameCandidate {
            name: name.into(),
            qualifier: None,
        }
    }
}

pub fn format_candidates(
    candidates: Vec<NameCandidate>,
    target: &str,
    limit: Option<usize>,
) -> String {
    let target = target.to_ascii_lowercase();
    let truncated = if let Some(limit) = limit {
        if candidates.len() > limit {
            Some(candidates.len() - limit)
        } else {
            None
        }
    } else {
        None
    };

    let mut candidates = candidates
        .into_iter()
        .sorted_by_cached_key(|c| {
            (
                stringmetrics::levenshtein(c.name.to_ascii_lowercase().as_str(), target.as_str()),
                c.name.to_string(),
            )
        })
        .map(|c| {
            if let Some(qualifier) = c.qualifier {
                if qualifier == Into::<Identifier>::into(SDF_IMPLICIT_QUALIFIER).into()
                    || qualifier
                        == Into::<Identifier>::into(SDF_IMPLICIT_UNAVAILABLE_QUALIFIER).into()
                {
                    format_ident_default(&c.name)
                } else {
                    format!(
                        "{}.{}",
                        match &qualifier {
                            Qualified::Bare { table } => format_ident_default(table),
                            Qualified::Partial { schema, table } => {
                                format!(
                                    "{}.{}",
                                    format_ident_default(schema),
                                    format_ident_default(table)
                                )
                            }
                            Qualified::Full {
                                catalog,
                                schema,
                                table,
                            } => format!(
                                "{}.{}.{}",
                                format_ident_default(catalog),
                                format_ident_default(schema),
                                format_ident_default(table)
                            ),
                        },
                        format_ident_default(&c.name)
                    )
                }
            } else {
                format_ident_default(&c.name)
            }
        });
    let candidates = if let Some(limit) = limit {
        candidates.take(limit).join(", ")
    } else {
        candidates.join(", ")
    };

    if let Some(truncated) = truncated {
        format!("Available are {} ..(and {} more)", candidates, truncated)
    } else {
        format!("Available are {}", candidates)
    }
}

pub fn format_ident_default(id: &Ident<'_>) -> String {
    let dialect = Dialect::default();
    if need_quotes(id, &dialect) {
        format!(
            "{quote}{name}{quote}",
            quote = dialect.quote_char(),
            name = dialect.escape_identifier(id.name())
        )
    } else {
        id.name().to_string()
    }
}

fn need_quotes(id: &Ident<'_>, dialect: &Dialect) -> bool {
    // Empty identifiers have to be quoted
    id.is_empty()
        // If the first character is not alphabetic, the identifier has to be quoted
        || !id.name().chars().next().is_some_and( |c| c=='_' || c.is_ascii_alphabetic())
        // Invalid characters has to be quoted
        || id.name().chars().any(|c| !dialect.is_valid_identifier_char(c)
            // BigQuery allows hyphens in unquoted identifiers in certain
            // contexts (e.g. table names), but we still quote them here
            || (dialect == &Dialect::Bigquery && c == '-'))
        // In Snowflake, unquoted identifiers are normalized to uppercase,
        // therefore if the identifier contains any lowercase characters, it
        // needs to be quoted to preserve the original casing.
        || (dialect == &Dialect::Snowflake && id.name().chars().any(|c| c.is_ascii_lowercase()))
        // In Redshift, unquoted identifiers are normalized to lowercase,
        // therefore if the identifier contains any uppercase characters, it
        // needs to be quoted to preserve the original casing.
        || (dialect == &Dialect::Redshift && id.name().chars().any(|c| c.is_ascii_uppercase()))
}
