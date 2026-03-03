# dbt-core Open Issues Categorization (2020–2022)

Date: 2026-03-03
Total open issues from 2020-01-01 to 2022-12-31: **48**

---

## Category 1: Stale (Feature requests / enhancements with no traction or relevance)

Issues that are feature requests, enhancements, or internal tech debt items that have not gained meaningful traction, are no longer relevant, or have gone dormant.

| # | Title | Created | Labels | Reason |
|---|-------|---------|--------|--------|
| [#2365](https://github.com/dbt-labs/dbt-core/issues/2365) | Support newline-delimited JSON for seeds | 2020-04-28 | enhancement, seeds, paper_cut | Niche seed format request; no implementation progress in 6 years |
| [#2515](https://github.com/dbt-labs/dbt-core/issues/2515) | [Feature] Respect `XDG_CONFIG_HOME` on Linux | 2020-06-08 | enhancement, stale | Stale-labeled; config directory convention request with no adoption |
| [#2986](https://github.com/dbt-labs/dbt-core/issues/2986) | Rationalize quoting configs + properties | 2020-12-31 | enhancement, paper_cut | Large design effort to unify quoting semantics; no implementation progress |
| [#3878](https://github.com/dbt-labs/dbt-core/issues/3878) | dbt snapshots to handle append tables | 2021-09-13 | enhancement, snapshots, stale | Stale-labeled; SCD2 enhancement for append-only sources; no traction |
| [#4775](https://github.com/dbt-labs/dbt-core/issues/4775) | [Feature] Improve mismatching package errors | 2022-02-24 | enhancement, stale, deps, paper_cut | Stale-labeled; better error messages for dependency conflicts |
| [#4957](https://github.com/dbt-labs/dbt-core/issues/4957) | [Feature] Manage full schema content by dropping unmanaged tables/views | 2022-03-25 | enhancement | Ambitious schema-management feature request; no adoption path |
| [#5236](https://github.com/dbt-labs/dbt-core/issues/5236) | Node configs: tech debt | 2022-05-12 | stale, tech_debt, design_needed | Stale-labeled; internal refactoring proposal with no movement |
| [#5316](https://github.com/dbt-labs/dbt-core/issues/5316) | Review `dbt_utils` helper methods: `_is_relation` + `_is_ephemeral` | 2022-06-01 | stale, tech_debt, utils | Stale-labeled; internal code review task; no body or progress |
| [#5509](https://github.com/dbt-labs/dbt-core/issues/5509) | Make unique ids of document records consistent | 2022-07-21 | stale, tech_debt | Stale-labeled; cosmetic consistency fix for doc unique IDs |
| [#5617](https://github.com/dbt-labs/dbt-core/issues/5617) | [Feature] Support `dbt.var` + `dbt.env_var` in Python models | 2022-08-05 | enhancement, stale, python_models | Stale-labeled; Python model context enhancement with no progress |
| [#5707](https://github.com/dbt-labs/dbt-core/issues/5707) | [Feature] Change background color of graph area in dbt docs | 2022-08-24 | enhancement, dbt-docs, stale | Stale-labeled; cosmetic dbt-docs UI request |
| [#5958](https://github.com/dbt-labs/dbt-core/issues/5958) | [Spike] Integrate dbt exceptions into structured logging | 2022-09-28 | stale, logging | Stale-labeled; internal spike with no progress |
| [#6008](https://github.com/dbt-labs/dbt-core/issues/6008) | Validate top level keys in schema file patches | 2022-10-05 | enhancement, stale | Stale-labeled; schema validation enhancement with limited traction |
| [#6026](https://github.com/dbt-labs/dbt-core/issues/6026) | Investigate MacroGenerator performance in contexts | 2022-10-07 | performance, tech_debt | Internal perf investigation; 0 comments, no follow-up |
| [#6039](https://github.com/dbt-labs/dbt-core/issues/6039) | [Feature] Warning for documented columns that don't exist | 2022-10-10 | enhancement, help_wanted | Docs generation enhancement; minimal traction since 2022 |
| [#6259](https://github.com/dbt-labs/dbt-core/issues/6259) | General solution for accessing flag object attributes safely | 2022-11-16 | tech_debt | Internal tech debt for CLI flag handling; no progress |
| [#6323](https://github.com/dbt-labs/dbt-core/issues/6323) | Smarter handling of `--vars` in partial parsing | 2022-11-28 | performance, partial_parsing | Performance improvement idea; no implementation progress |
| [#6353](https://github.com/dbt-labs/dbt-core/issues/6353) | Unify secret management | 2022-12-01 | spike, tech_debt | Internal spike; 0 comments, never acted on |
| [#6391](https://github.com/dbt-labs/dbt-core/issues/6391) | Create protobuf definitions to match node objects | 2022-12-06 | stale, logging | Stale-labeled; protobuf logging expansion; no progress |
| [#6448](https://github.com/dbt-labs/dbt-core/issues/6448) | [Feature] Add a new `UnsupportedException` exception | 2022-12-15 | enhancement, user docs, Refinement | Exception class proposal for adapter compatibility; no implementation |

**Count: 20**

---

## Category 2: Bug (Requests for bug fixes)

Issues that report incorrect behavior, errors, or regressions in dbt-core.

| # | Title | Created | Labels | Description |
|---|-------|---------|--------|-------------|
| [#2793](https://github.com/dbt-labs/dbt-core/issues/2793) | `ref()` in `set_sql_header` resolves to current model instead of referenced model | 2020-09-26 | bug, user docs | `ref('anything')` in `set_sql_header` incorrectly resolves to the current model on BigQuery |
| [#4364](https://github.com/dbt-labs/dbt-core/issues/4364) | [Bug] Partial parsing should handle volatile variables like `invocation_id` | 2021-11-30 | bug, partial_parsing, jira | Partial parsing breaks when using dynamic vars like `run_started_at` in freshness checks |
| [#4785](https://github.com/dbt-labs/dbt-core/issues/4785) | [Bug] `on-run-end` hooks execute before `on-run-start` for `dbt docs generate` | 2022-02-25 | bug, S2, Refinement | Hook execution order is reversed when running `dbt docs generate` |
| [#5938](https://github.com/dbt-labs/dbt-core/issues/5938) | `dbt docs serve` locks `dbt.log` file on Windows | 2022-09-27 | bug, windows, stale, tech_debt, logging | On Windows, `dbt docs serve` locks `dbt.log`, preventing concurrent `dbt run` |
| [#6219](https://github.com/dbt-labs/dbt-core/issues/6219) | [Bug] 1.1.latest test fails | 2022-11-04 | bug, stale, tech_debt | CI test failures on the 1.1.latest branch |
| [#6486](https://github.com/dbt-labs/dbt-core/issues/6486) | [Bug] SQLFluff dbt templater fails with TypeError on `ManifestLoader.load()` | 2022-12-24 | bug, stale | SQLFluff integration breaks when calling `ManifestLoader.load()` due to API changes |

**Count: 6**

---

## Category 3: Adapters (Requests needing adapter-specific changes)

Issues that require changes in database adapters or the adapter interface rather than (or in addition to) dbt-core itself.

| # | Title | Created | Labels | Description |
|---|-------|---------|--------|-------------|
| [#5500](https://github.com/dbt-labs/dbt-core/issues/5500) | Tuple concurrently updated (Postgres) | 2022-07-20 | bug, stale, postgres, Team:Adapters | Postgres-specific concurrency bug: `rename_relation` causes `tuple concurrently updated` during parallel model runs |
| [#5967](https://github.com/dbt-labs/dbt-core/issues/5967) | Deprecate `adapter.date_function` + `sql_now` | 2022-09-28 | stale, tech_debt, Team:Adapters, utils | Adapter interface cleanup: stop requiring every adapter to implement `date_function` |
| [#6013](https://github.com/dbt-labs/dbt-core/issues/6013) | `full_refresh: true` config should respect `--full-refresh` flag | 2022-10-06 | enhancement, incremental, Team:Adapters | Behavior change in how incremental models handle `full_refresh` config vs. CLI flag; requires adapter coordination |

**Count: 3**

---

## Category 4: Already Implemented (Features/improvements now part of dbt-core)

Issues requesting features or improvements that have since been implemented in later versions of dbt-core.

| # | Title | Created | Labels | Evidence of Implementation |
|---|-------|---------|--------|---------------------------|
| [#4304](https://github.com/dbt-labs/dbt-core/issues/4304) | `state:modified` should detect changes in used variables | 2021-11-18 | state, paper_cut | **Implemented**: `state_modified_compare_vars` behavior change flag added in `core/dbt/contracts/project.py`. When enabled, variable changes are detected during `state:modified` selection |
| [#4557](https://github.com/dbt-labs/dbt-core/issues/4557) | [Feature] Invalidate packages folder when `packages.yml` changes | 2022-01-07 | enhancement, packages, deps | **Implemented**: dbt-core now generates a `package-lock.yml` file during `dbt deps` and validates installed packages against locked deps (see `core/dbt/task/deps.py`, `load_package_lock_config`) |
| [#4613](https://github.com/dbt-labs/dbt-core/issues/4613) | Write all store test failures into one table | 2022-01-24 | enhancement, dbt tests | **Partially implemented**: `store_failures_as` config was added (see `core/dbt/artifacts/resources/v1/config.py`) allowing control over how test failures are stored (table vs view). The single-table consolidation was not fully done but the config infrastructure is in place |
| [#4723](https://github.com/dbt-labs/dbt-core/issues/4723) | [Feature] Allow tests to warn/fail based on percentage | 2022-02-14 | enhancement, dbt tests | **Implemented**: `warn_if` and `error_if` config parameters in `TestConfig` class (`core/dbt/artifacts/resources/v1/config.py`) support custom expressions (e.g., `>= 0.05`). Combined with `fail_calc`, users can compute percentage-based thresholds |
| [#5886](https://github.com/dbt-labs/dbt-core/issues/5886) | Update profiler to remove "enabled" arg | 2022-09-20 | enhancement, good_first_issue | **Already refactored**: The profiler in `core/dbt/profiler.py` now uses a clean `profiler(enable, outfile)` context manager — the old `enabled` argument pattern referenced in the issue has been simplified |
| [#6391](https://github.com/dbt-labs/dbt-core/issues/6391) | Create protobuf definitions to match node objects | 2022-12-06 | stale, logging | **Implemented**: Protobuf definitions for structured logging events exist at `core/dbt/events/core_types_pb2.py` |

**Count: 6**

---

## Category 5: Uncategorized / Cross-cutting (Items that don't cleanly fit one category)

Issues that span multiple categories or are internal CI/CD / repo maintenance tasks.

### Feature Requests with Active Community Interest (not stale)

| # | Title | Created | Labels | Notes |
|---|-------|---------|--------|-------|
| [#2142](https://github.com/dbt-labs/dbt-core/issues/2142) | Option to disable "skip downstream models on failure" | 2020-02-17 | enhancement, help_wanted | 35 comments; active community demand; still open feature request |
| [#3484](https://github.com/dbt-labs/dbt-core/issues/3484) | [Feature] Support `.jinja`/`.jinja2`/`.j2` file extensions | 2021-06-22 | enhancement, help_wanted | 16 comments; IDE integration improvement; active interest |
| [#4868](https://github.com/dbt-labs/dbt-core/issues/4868) | [Feature] Ignore/exclude large files in dbt packages | 2022-03-15 | enhancement, tech_debt, deps | 7 comments; package size optimization |
| [#5009](https://github.com/dbt-labs/dbt-core/issues/5009) | Combine `--select`/`--exclude` with `--selector` | 2022-04-07 | enhancement, help_wanted, node selection | 25 comments; highly requested selector behavior change |
| [#5410](https://github.com/dbt-labs/dbt-core/issues/5410) | `dbt deps` doesn't resolve nested local dependencies | 2022-06-27 | enhancement, deps, multi_project | 17 comments; important for multi-project setups |
| [#6248](https://github.com/dbt-labs/dbt-core/issues/6248) | Freshness checks should retry errored tests at end | 2022-11-14 | enhancement, good_first_issue | 6 comments; updated recently (2026-03-03); still relevant |

### Internal / CI/CD Tech Debt

| # | Title | Created | Labels | Notes |
|---|-------|---------|--------|-------|
| [#4704](https://github.com/dbt-labs/dbt-core/issues/4704) | Fix typing issues from pre-commit work | 2022-02-09 | stale, repo ci/cd, tech_debt | Internal mypy/typing cleanup |
| [#4988](https://github.com/dbt-labs/dbt-core/issues/4988) | Easy integration test triggering for adapter plugins | 2022-04-03 | stale, repo ci/cd, tech_debt | CI/CD workflow improvement |
| [#5632](https://github.com/dbt-labs/dbt-core/issues/5632) | Grants testing requires user-supplied `test.env` | 2022-08-09 | stale, repo ci/cd, tech_debt | Test infrastructure issue |
| [#5639](https://github.com/dbt-labs/dbt-core/issues/5639) | Should adapters be loaded earlier? | 2022-08-10 | init, tech_debt | Architectural question about adapter loading timing |
| [#5642](https://github.com/dbt-labs/dbt-core/issues/5642) | Backport workflow does not work for forks | 2022-08-10 | stale, tech_debt | CI/CD workflow bug |
| [#6060](https://github.com/dbt-labs/dbt-core/issues/6060) | Upgrade GHA workflows to latest Node version | 2022-10-13 | stale, repo ci/cd, tech_debt | GHA Node.js version migration |
| [#6217](https://github.com/dbt-labs/dbt-core/issues/6217) | Automatic Backport fails | 2022-11-04 | repo ci/cd, tech_debt | CI/CD backport automation issue |
| [#6364](https://github.com/dbt-labs/dbt-core/issues/6364) | Ubuntu-22.04 causes permission denied error in actions | 2022-12-02 | bug, tech_debt | CI/CD environment issue |

---

## Summary

| Category | Count | Percentage |
|----------|-------|------------|
| Stale | 20 | 41.7% |
| Bug | 6 | 12.5% |
| Adapters | 3 | 6.3% |
| Already Implemented | 6 | 12.5% |
| Active Feature Requests (not stale) | 6 | 12.5% |
| Internal CI/CD Tech Debt | 7 | 14.6% |
| **Total** | **48** | **100%** |

### Key Takeaways

1. **Stale issues dominate** — Over 40% of open issues from this era are stale feature requests or tech debt items that never gained traction.
2. **Only 6 true bugs remain open** — Most bugs from this period have been resolved; the remaining ones are edge cases (Windows file locking, Postgres concurrency, hook ordering).
3. **6 issues have been effectively addressed** — Package lock files, `store_failures_as` config, profiler cleanup, protobuf logging, `state:modified` with vars comparison, and percentage-based test thresholds (`warn_if`/`error_if`) are now in the codebase.
4. **6 feature requests still have active community interest** — Notably selector combination (#5009, 25 comments) and skipping downstream models (#2142, 35 comments) remain highly requested.
5. **7 issues are internal CI/CD tech debt** — These are repo maintenance items (GHA upgrades, backport workflows, test infrastructure) that are only relevant to dbt-core contributors.
