//! Resolving `enabled` for a skill, and picking a winner when two enabled
//! skills want the same flat install destination.
//!
//! This is the shared helper the spec's risk R1 calls for: the deps-time install
//! pass and (in a later phase) the parser must resolve `enabled` identically, so
//! both go through here. It reuses the same FQN config machinery every other
//! resource type uses, with the same precedence — a package's own `skills:`
//! block is the base and the root project's `skills:` block overrides it.

use std::collections::BTreeMap;

use dbt_common::ErrorCode;
use dbt_common::tracing::dbt_emit::emit_warn_log_message;
use dbt_common::{FsResult, fs_err};
use dbt_schemas::schemas::project::{
    DbtProjectConfig, ProjectConfigResolver, ResolvedConfig, SkillConfig, init_project_config,
};

use crate::discover::{DiscoveredSkill, SkillOrigin, SkillSourceProject};
use crate::provenance::ShadowedSkill;

/// A skill that survived config resolution and collision handling, plus the
/// skills it shadowed.
#[derive(Debug, Clone)]
pub struct SelectedSkill {
    pub skill: DiscoveredSkill,
    pub shadowed: Vec<ShadowedSkill>,
}

/// Resolve `enabled` for every discovered skill and drop the disabled ones.
///
/// `projects` must be the same slice, in the same order, that produced
/// `skills`; `precedence` indexes into it.
pub fn filter_enabled(
    skills: Vec<DiscoveredSkill>,
    projects: &[SkillSourceProject],
) -> FsResult<Vec<DiscoveredSkill>> {
    let root_project = projects.first().ok_or_else(|| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Cannot resolve skill config without a root project"
        )
    })?;
    let root_config: DbtProjectConfig<SkillConfig> =
        init_project_config(&root_project.skills_config, (), None)?;

    // One resolver per source project, built once and reused across its skills.
    let mut resolvers: BTreeMap<usize, ProjectConfigResolver<SkillConfig>> = BTreeMap::new();
    for (index, project) in projects.iter().enumerate() {
        let resolver = if index == 0 {
            ProjectConfigResolver::for_root(root_config.clone())
        } else {
            let local = init_project_config(
                &project.skills_config,
                (),
                Some(project.package_name.as_str()),
            )?;
            ProjectConfigResolver::for_dependency(local, root_config.clone())
        };
        resolvers.insert(index, resolver);
    }

    let mut enabled = Vec::new();
    for skill in skills {
        let Some(resolver) = resolvers.get(&skill.precedence) else {
            continue;
        };
        if resolver
            .resolve_with_configs(&skill.fqn, &skill.fqn, &[])
            .enabled()
        {
            enabled.push(skill);
        }
    }

    Ok(enabled)
}

/// Resolve same-name collisions into one winner per name.
///
/// The install layout is flat and unnamespaced, so two enabled skills with the
/// same name want the same directory. dbt does not fail: it warns, installs a
/// deterministic winner, and records the losers so `.provenance` can explain
/// what happened. Precedence is the order source projects were supplied in —
/// the project first, then packages in `packages.yml` declaration order.
pub fn resolve_collisions(skills: Vec<DiscoveredSkill>) -> Vec<SelectedSkill> {
    let mut by_name: BTreeMap<String, Vec<DiscoveredSkill>> = BTreeMap::new();
    for skill in skills {
        by_name.entry(skill.name.clone()).or_default().push(skill);
    }

    let mut selected = Vec::new();
    for (name, mut candidates) in by_name {
        // Stable sort keeps discovery order within a single project, so two
        // same-named skills inside one package resolve deterministically too.
        candidates.sort_by_key(|skill| skill.precedence);
        let mut candidates = candidates.into_iter();
        let winner = candidates.next().expect("group is never empty");
        let losers: Vec<DiscoveredSkill> = candidates.collect();

        if !losers.is_empty() {
            warn_collision(&name, &winner, &losers);
        }

        selected.push(SelectedSkill {
            shadowed: losers
                .iter()
                .map(|loser| ShadowedSkill {
                    package: loser.origin.package_name().map(str::to_string),
                    source_path: loser.source_path.to_string_lossy().replace('\\', "/"),
                })
                .collect(),
            skill: winner,
        });
    }

    selected
}

fn warn_collision(name: &str, winner: &DiscoveredSkill, losers: &[DiscoveredSkill]) {
    let loser_labels: Vec<String> = losers.iter().map(|l| l.origin.label()).collect();
    let disable_hint = match losers.first().map(|l| &l.origin) {
        Some(SkillOrigin::Package {
            name: package_name, ..
        }) => format!(
            " To install a different one, disable this one in dbt_project.yml: \
             skills: {{{package_name}: {{{name}: {{+enabled: false}}}}}}."
        ),
        _ => String::new(),
    };

    emit_warn_log_message(
        ErrorCode::SkillNameCollision,
        format!(
            "Skill name collision on '{name}': installing the one from {}, skipping {}.{disable_hint}",
            winner.origin.label(),
            loser_labels.join(", ")
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate::SkillFrontmatter;
    use std::path::PathBuf;

    fn skill(name: &str, precedence: usize, origin: SkillOrigin) -> DiscoveredSkill {
        DiscoveredSkill {
            name: name.to_string(),
            dir: PathBuf::from(format!("/tmp/{name}")),
            source_path: PathBuf::from(format!("skills/{name}")),
            fqn: vec![
                origin.package_name().unwrap_or("root_project").to_string(),
                name.to_string(),
            ],
            origin,
            frontmatter: SkillFrontmatter {
                name: name.to_string(),
                description: "A skill.".to_string(),
            },
            precedence,
        }
    }

    fn package(name: &str) -> SkillOrigin {
        SkillOrigin::Package {
            name: name.to_string(),
            version: None,
        }
    }

    fn source_project(name: &str, skills_yaml: Option<&str>) -> SkillSourceProject {
        SkillSourceProject {
            root: PathBuf::from("/tmp"),
            package_name: name.to_string(),
            version: None,
            origin_is_project: name == "root_project",
            skill_paths: vec!["skills".to_string()],
            skills_config: skills_yaml.map(|yaml| crate::yaml::from_str(yaml).unwrap()),
        }
    }

    #[test]
    fn skills_are_enabled_by_default() {
        let projects = [source_project("root_project", None)];
        let skills = vec![skill("alpha", 0, SkillOrigin::Project)];
        assert_eq!(filter_enabled(skills, &projects).unwrap().len(), 1);
    }

    #[test]
    fn the_root_project_can_disable_its_own_skill() {
        let projects = [source_project(
            "root_project",
            Some("root_project:\n  alpha:\n    +enabled: false\n"),
        )];
        let skills = vec![
            skill("alpha", 0, SkillOrigin::Project),
            skill("beta", 0, SkillOrigin::Project),
        ];
        let enabled = filter_enabled(skills, &projects).unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "beta");
    }

    #[test]
    fn the_root_project_can_disable_a_whole_package() {
        let projects = [
            source_project("root_project", Some("transitive_pkg:\n  +enabled: false\n")),
            source_project("transitive_pkg", None),
        ];
        let skills = vec![skill("bloat", 1, package("transitive_pkg"))];
        assert!(filter_enabled(skills, &projects).unwrap().is_empty());
    }

    #[test]
    fn the_root_project_overrides_a_packages_own_config() {
        // The package disables its skill; the consuming project turns it back on.
        let projects = [
            source_project(
                "root_project",
                Some("some_pkg:\n  useful:\n    +enabled: true\n"),
            ),
            source_project(
                "some_pkg",
                Some("some_pkg:\n  useful:\n    +enabled: false\n"),
            ),
        ];
        let skills = vec![skill("useful", 1, package("some_pkg"))];
        assert_eq!(filter_enabled(skills, &projects).unwrap().len(), 1);
    }

    #[test]
    fn a_package_can_disable_its_own_skill() {
        let projects = [
            source_project("root_project", None),
            source_project(
                "some_pkg",
                Some("some_pkg:\n  internal:\n    +enabled: false\n"),
            ),
        ];
        let skills = vec![skill("internal", 1, package("some_pkg"))];
        assert!(filter_enabled(skills, &projects).unwrap().is_empty());
    }

    #[test]
    fn the_project_beats_packages_on_a_collision() {
        let selected = resolve_collisions(vec![
            skill("shared", 1, package("package_a")),
            skill("shared", 0, SkillOrigin::Project),
        ]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].skill.origin, SkillOrigin::Project);
        assert_eq!(
            selected[0].shadowed,
            vec![ShadowedSkill {
                package: Some("package_a".to_string()),
                source_path: "skills/shared".to_string(),
            }]
        );
    }

    #[test]
    fn the_first_declared_package_wins_among_packages() {
        let selected = resolve_collisions(vec![
            skill("nunchuck-skills", 2, package("package_b")),
            skill("nunchuck-skills", 1, package("package_a")),
        ]);

        assert_eq!(selected[0].skill.origin.package_name(), Some("package_a"));
        assert_eq!(
            selected[0].shadowed[0].package.as_deref(),
            Some("package_b")
        );
    }

    #[test]
    fn non_colliding_skills_are_all_kept_with_no_shadows() {
        let selected = resolve_collisions(vec![
            skill("alpha", 0, SkillOrigin::Project),
            skill("beta", 1, package("package_a")),
        ]);

        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|s| s.shadowed.is_empty()));
    }
}
