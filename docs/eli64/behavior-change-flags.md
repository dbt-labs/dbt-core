# Playbook: Behavior Change Flags

User documentation: https://docs.getdbt.com/reference/global-configs/legacy-behaviors

## Rules for introducing a new flag

1. **Naming.** All behavior change flags should be named so that their default value changes from **False → True**. This makes it significantly easier to document, talk about, and understand.
    * If the flag is prohibiting something that we previously allowed, use the verb “require.” Examples:
        * `require_resource_names_without_spaces`
        * `require_explicit_package_overrides_for_builtin_materializations`
    * All flags should be of boolean type, and False by default when introduced: `bool = False`.
2. **Documentation.** Start with the docs! What is the change? Who might be affected? What action will users need to take to mitigate this change? At this point, the dates for flag Introduction + Maturity are “TBD.”
3. **Deprecation warnings**. As a general rule, **all** behavior changes should be accompanied by a deprecation warning.
    * Always use our standard deprecations module: [https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/deprecations.py](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/deprecations.py)
    * This serves two purposes: Signalling the change to the user, and collecting telemetry so we can understand blast radius among users with telemtry enabled.
    * These warning messages should link back to documentation: [https://docs.getdbt.com/reference/global-configs/legacy-behaviors](https://docs.getdbt.com/reference/global-configs/legacy-behaviors#deprecate_package_materialization_builtin_override)
    * Even for additive behaviors that are not “breaking changes,” there is still an opportunity to signal these changes for users, and to gather an estimate of the impact. E.g. `source_freshness_run_project_hooks` should still include a proactive message any time someone runs the `source freshness` command in a project that has `on-run-*` hooks defined.
    * The call site for these deprecation warnings should be as close as possible to the place where we’re evaluating conditional logic based on the project flag. Essentially, any time we check the flag value and it returns `False`, we should raise a deprecation warning while preserving the legacy behavior. (In the future, we might be able to streamline more of this boilerplate code.)
    * If users want to silence these deprecation warnings, they can do so via `warn_error_options.silence`. Explicitly setting the flag to `False` in `dbt_project.yml` is not sufficient to silence the warning.
4. **Exceptions.** If the behavior change is to raise an exception that prohibits behavior which was previously permitted (e.g. spaces in model names), the exception message should also link to the docs on legacy behaviors.
5. **Backports.** Whenever possible, we should backport both the deprecation warning and the flag to the previous version of dbt Core.
6. **Open a GitHub issue** in the dbt-core repository that is the implementation ticket for switching the default from `false` to `true`. Add the `behavior_change_flag` issue label, and add it to the GitHub milestone for the next minor version. (This is true in most cases, see below for exceptional considerations.) During planning, we will bundle up the “introduced” behavior changes into an epic/tasklist that schedules their maturation.

## After introduction

1. **Mature flag(s) by switching value from `False` → `True` in dbt-core `main`.**
    * This should land in **the next minor (`1.X.0`) release of dbt-core**.
    If the behavior change is mitigating a security vulnerability, and the next minor release is still planned for several months away, we still backport the fix + flag (off by default) to supported OSS versions, and we strongly advise all users to opt into the flag sooner.
2. **Removing support for legacy behavior.**
    * As a general rule, we will not entirely remove support for any legacy behaviors until dbt v2.0.
        * We are not committing to supporting them forever (à la Rust editions). But we are also not taking them away willy-nilly.
        * On a case-by-case basis, if there is a strong compelling reason to remove a legacy behavior and we see minimal in-the-wild usage (<1% of relevant projects), we can remove it entirely. This needs to be communicated well in advance — at least 2 minor versions after introduction in dbt Core.
        * These are *project configurations*, not feature flags. While they add complexity to our codebase, such is the price of maintaining v1.* software.
