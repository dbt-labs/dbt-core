# dbt-core Open Issues Categorization (2023–2024)

Date: 2026-03-03
Total open issues from 2023-01-01 to 2024-12-31: **377**

---

## Category 1: Stale (Feature requests with no traction)

Issues that are feature requests, enhancements, spikes, or tech debt items that have not gained
meaningful traction, are labeled `stale`/`wontfix`, or have gone dormant with minimal engagement.

| # | Title | Created | Last Updated | Comments | Labels |
| --- | --- | --- | --- | --- | --- |
| [#6511](https://github.com/dbt-labs/dbt-core/issues/6511) | [CT-1747] Enable flagging nodes/configs for deferred validation | 2023-01-04 | 2023-07-19 | 1 | tech_debt |
| [#6562](https://github.com/dbt-labs/dbt-core/issues/6562) | [SPIKE] Testing PR #6322 (build performance update) | 2023-01-10 | 2024-02-17 | 4 | performance, spike |
| [#6583](https://github.com/dbt-labs/dbt-core/issues/6583) | [CT-1802] [Spike] Determine plan for pinning in codebase | 2023-01-11 | 2023-08-03 | 2 | dependencies, tech_debt |
| [#6647](https://github.com/dbt-labs/dbt-core/issues/6647) | [CT-1847] Move `MP_CONTEXT` somewhere else. | 2023-01-18 | 2023-07-19 | 1 | tech_debt |
| [#6840](https://github.com/dbt-labs/dbt-core/issues/6840) | [CT-2016] [Spike] Static artifact for CLI validation | 2023-02-02 | 2024-02-20 | 4 | cli, spike |
| [#6857](https://github.com/dbt-labs/dbt-core/issues/6857) | [CT-2028] A general solution for setting attributes in flag objects to consistent types | 2023-02-03 | 2023-07-19 | 0 | spike, tech_debt |
| [#6879](https://github.com/dbt-labs/dbt-core/issues/6879) | [CT-2050] Use of "path" and "original_file_path" in nodes is confused | 2023-02-06 | 2023-07-19 | 1 | tech_debt, file_system |
| [#6999](https://github.com/dbt-labs/dbt-core/issues/6999) | [CT-2140] [Feature] Parse versions with specific version standards | 2023-02-17 | 2023-07-19 | 2 | dependencies, tech_debt, deps |
| [#7073](https://github.com/dbt-labs/dbt-core/issues/7073) | [CT-2211] [Feature] Update instances of `{{ sql }}` to `{{ model.compiled_code }}` | 2023-02-28 | 2023-02-28 | 0 | enhancement, tech_debt |
| [#7156](https://github.com/dbt-labs/dbt-core/issues/7156) | [CT-2295] Refactor `expected_duplicate_params` | 2023-03-10 | 2023-07-19 | 0 | tech_debt |
| [#7157](https://github.com/dbt-labs/dbt-core/issues/7157) | [CT-2296] [Spike] Remove the distinction between configs and non-configs | 2023-03-13 | 2023-11-13 | 5 | spike, tech_debt |
| [#7425](https://github.com/dbt-labs/dbt-core/issues/7425) | [CT-2455] [Feature] Less strict secret env vars: allow elsewhere besides just `profiles.yml` + `packages.yml` | 2023-04-20 | 2025-09-23 | 9 | enhancement, wontfix |
| [#7581](https://github.com/dbt-labs/dbt-core/issues/7581) | [CT-2558] [Feature] Make `run_started_at` an aware Python datetime | 2023-05-09 | 2023-05-10 | 0 | enhancement, tech_debt |
| [#7588](https://github.com/dbt-labs/dbt-core/issues/7588) | [CT-2560] [Feature] Establish a set of interface tests for adapters | 2023-05-10 | 2023-08-03 | 1 | enhancement, tech_debt |
| [#7759](https://github.com/dbt-labs/dbt-core/issues/7759) | [CT-2625] Support model contracts + constraints with nested fields in core | 2023-06-01 | 2024-10-30 | 1 | tech_debt, multi_project, model_contracts |
| [#7787](https://github.com/dbt-labs/dbt-core/issues/7787) | [CT-2640] To-do: Investigate if `RuntimeConfig.from_args` can be optimized to avoid reading from disk if `profile` and ` | 2023-06-05 | 2024-01-02 | 2 | tech_debt |
| [#8047](https://github.com/dbt-labs/dbt-core/issues/8047) | [CT-2802] [Feature] For built-in node configs: if not defined, do not add `None` value to `config` | 2023-07-06 | 2024-02-20 | 9 | enhancement, stale, artifacts, possible_v2.0 |
| [#8133](https://github.com/dbt-labs/dbt-core/issues/8133) | [CT-2836] extend fire_event to handle warn-error and warn-error-options for WarnLevel exceptions, deprecate warn_or_erro | 2023-07-18 | 2023-08-28 | 3 | tech_debt |
| [#8253](https://github.com/dbt-labs/dbt-core/issues/8253) | [CT-2890] [Feature] Enable generating docs when two sources differ only by case | 2023-07-29 | 2023-08-03 | 1 | enhancement, help_wanted |
| [#8269](https://github.com/dbt-labs/dbt-core/issues/8269) | [CT-2899] [Epic] Improve tests/unit coverage | 2023-08-01 | 2024-02-21 | 0 | tech_debt |
| [#8270](https://github.com/dbt-labs/dbt-core/issues/8270) | [CT-2900] [Epic] Parsing Refactoring | 2023-08-01 | 2023-08-09 | 0 | tech_debt |
| [#8271](https://github.com/dbt-labs/dbt-core/issues/8271) | [CT-2901] [Epic] Jinja Context Simplifications | 2023-08-01 | 2023-08-01 | 0 | tech_debt |
| [#8273](https://github.com/dbt-labs/dbt-core/issues/8273) | [CT-2903] [Feature] Use PostgreSQL 15 for functional testing | 2023-08-01 | 2024-01-29 | 1 | enhancement |
| [#8278](https://github.com/dbt-labs/dbt-core/issues/8278) | [CT-2907] [Feature] Exception messages in `run_results.json` from `run-operation` invocations | 2023-08-02 | 2023-09-20 | 1 | enhancement, help_wanted |
| [#8291](https://github.com/dbt-labs/dbt-core/issues/8291) | [CT-2918] Implement inline fixture reuse, in a "fixtures" yaml file section | 2023-08-02 | 2025-04-29 | 1 | enhancement, unit tests |
| [#8292](https://github.com/dbt-labs/dbt-core/issues/8292) | [CT-2919] Improved way of generating unit test yaml, via codegen or other techniques | 2023-08-02 | 2024-09-20 | 0 | enhancement, unit tests |
| [#8293](https://github.com/dbt-labs/dbt-core/issues/8293) | [CT-2920] Unit testing python models | 2023-08-02 | 2024-09-20 | 0 | enhancement, unit tests |
| [#8347](https://github.com/dbt-labs/dbt-core/issues/8347) | [CT-2954] [Epic] Static Parsing Performance | 2023-08-09 | 2023-09-25 | 0 | tech_debt |
| [#8425](https://github.com/dbt-labs/dbt-core/issues/8425) | [CT-2999] Unit testing of CTEs | 2023-08-16 | 2024-09-20 | 0 | enhancement, unit tests |
| [#8526](https://github.com/dbt-labs/dbt-core/issues/8526) | [CT-3063] [Feature] state:modified doesn't detect changes to macros passed as variables | 2023-08-31 | 2024-02-07 | 1 | enhancement, state: modified |
| [#8599](https://github.com/dbt-labs/dbt-core/issues/8599) | [CT-3096] Refactor selection + `dbt list` output, for better consistency across node types | 2023-09-08 | 2024-12-02 | 1 | tech_debt, paper_cut, Impact: Exp, behavior_change_flag |
| [#8618](https://github.com/dbt-labs/dbt-core/issues/8618) | [CT-3106] Refactor ManifestLoader.load method | 2023-09-11 | 2023-10-10 | 2 | tech_debt |
| [#8674](https://github.com/dbt-labs/dbt-core/issues/8674) | [CT-3133] Re-evaluate Performance Advantages of dbt-extractor | 2023-09-19 | 2023-09-22 | 0 | performance, tech_debt |
| [#8691](https://github.com/dbt-labs/dbt-core/issues/8691) | [CT-3143] [rulsesets] Convert `dbt-core` to GitHub Rulesets | 2023-09-22 | 2023-09-25 | 2 | tech_debt |
| [#8758](https://github.com/dbt-labs/dbt-core/issues/8758) | [CT-3177] [CLI] Raise deprecation warning for legacy MultiOption syntax | 2023-10-03 | 2023-10-03 | 0 | cli, tech_debt, Impact: CLI |
| [#8837](https://github.com/dbt-labs/dbt-core/issues/8837) | [CT-3211] [Feature] Globally unique temporary tables for incremental models | 2023-10-12 | 2025-02-14 | 3 | enhancement, help_wanted, stale |
| [#8900](https://github.com/dbt-labs/dbt-core/issues/8900) | [CT-3263] [Feature] User space Jinja macro for converting DB API 2.0 `type_code`s into `data_type`s | 2023-10-25 | 2024-02-01 | 1 | enhancement, Refinement, model_contracts |
| [#8935](https://github.com/dbt-labs/dbt-core/issues/8935) | [CT-3280] [Feature] Successful partial parsing logs need to be sent to INFO instead of DEBUG | 2023-10-27 | 2023-10-27 | 1 | enhancement, logging |
| [#8936](https://github.com/dbt-labs/dbt-core/issues/8936) | [CT-3281] [Feature] Add a `--perf-info` flag to write out `perf_info.json` for all commands that write a manifest | 2023-10-27 | 2023-10-27 | 1 | enhancement, user docs, Refinement |
| [#8947](https://github.com/dbt-labs/dbt-core/issues/8947) | [CT-3293] [Feature] Use `RelationType` enum values in `get_catalog_relations` / Catalog artifacts | 2023-10-31 | 2023-11-02 | 1 | enhancement, Refinement |
| [#8949](https://github.com/dbt-labs/dbt-core/issues/8949) | [Feature] `dbt deps` is slow, suggest parallel package loading | 2023-10-31 | 2025-02-14 | 3 | enhancement, help_wanted, stale |
| [#9079](https://github.com/dbt-labs/dbt-core/issues/9079) | [CT-3384] [Feature] Improve error message when metadata freshness last modified relation is not found | 2023-11-15 | 2023-11-15 | 0 | enhancement |
| [#9110](https://github.com/dbt-labs/dbt-core/issues/9110) | [CT-3399] [Epic] Improve Writability and Coverage of Functional Tests | 2023-11-16 | 2023-11-30 | 0 | tech_debt |
| [#9111](https://github.com/dbt-labs/dbt-core/issues/9111) | [CT-3400] Document testing best practices in CONTRIBUTING.md | 2023-11-16 | 2023-12-11 | 1 | tech_debt |
| [#9174](https://github.com/dbt-labs/dbt-core/issues/9174) | [CT-3432] [spike] investigation - event system should completely control stdout | 2023-11-29 | 2024-08-29 | 1 | enhancement, logging, Impact: Exp, Impact: CLI |
| [#9178](https://github.com/dbt-labs/dbt-core/issues/9178) | [CT-3436] [Feature] Relax PyYAML dependency requirement | 2023-11-29 | 2023-11-29 | 0 | enhancement |
| [#9223](https://github.com/dbt-labs/dbt-core/issues/9223) | [CT-3462] [Feature] Additional configurability of incremental merge strategy | 2023-12-05 | 2024-01-10 | 1 | enhancement, Refinement |
| [#9238](https://github.com/dbt-labs/dbt-core/issues/9238) | [CT-3468] allow creating unit testing fixtures in non-root projects | 2023-12-06 | 2024-09-20 | 1 | enhancement, unit tests |
| [#9260](https://github.com/dbt-labs/dbt-core/issues/9260) | [CT-3480] Remove the "test" Directory | 2023-12-08 | 2023-12-08 | 0 | tech_debt |
| [#9272](https://github.com/dbt-labs/dbt-core/issues/9272) | [CT-3488] [Feature] Generate manifest at build time or in PR | 2023-12-12 | 2025-02-14 | 6 | enhancement, stale, awaiting_response |
| [#9306](https://github.com/dbt-labs/dbt-core/issues/9306) | [CT-3508] Consolidate `system.rmdir` and `system.rmtree` | 2023-12-20 | 2023-12-20 | 0 | tech_debt |
| [#9325](https://github.com/dbt-labs/dbt-core/issues/9325) | [CT-3517] consistent policy around cli args, env vars, etc. for `dbt retry` | 2024-01-02 | 2024-01-03 | 1 | enhancement, user docs, retry |
| [#9355](https://github.com/dbt-labs/dbt-core/issues/9355) | [CT-3536] Use ignore paths to only run tests on PRs as needed | 2024-01-10 | 2024-01-23 | 0 | tech_debt |
| [#9358](https://github.com/dbt-labs/dbt-core/issues/9358) | [CT-3539] [Feature] Input following generic test name is ignored - hides incorrectly configured tests | 2024-01-10 | 2025-02-14 | 3 | enhancement, help_wanted, stale |
| [#9359](https://github.com/dbt-labs/dbt-core/issues/9359) | [CT-3540] Clean up rpc related code | 2024-01-10 | 2024-01-28 | 0 | artifacts, tech_debt |
| [#9395](https://github.com/dbt-labs/dbt-core/issues/9395) | [CT-3563] [source] configuration of "database" in dbt_project.yml or source.yml | 2024-01-17 | 2025-02-14 | 2 | enhancement, stale, awaiting_response |
| [#9445](https://github.com/dbt-labs/dbt-core/issues/9445) | [Feature] Run integration test in CLI fashion | 2024-01-24 | 2024-01-25 | 0 | enhancement |
| [#9472](https://github.com/dbt-labs/dbt-core/issues/9472) | [Feature] Postgres 15+ unique index NULLS NOT DISTINCT | 2024-01-29 | 2025-02-14 | 5 | enhancement, help_wanted, stale |
| [#9501](https://github.com/dbt-labs/dbt-core/issues/9501) | [Feature] DBT clone should output more useful information to the log | 2024-01-31 | 2025-05-13 | 3 | enhancement, help_wanted, stale, clone |
| [#9526](https://github.com/dbt-labs/dbt-core/issues/9526) | Emit error message to stdout when the `logs` directory is not writable | 2024-02-06 | 2024-02-06 | 1 | enhancement |
| [#9531](https://github.com/dbt-labs/dbt-core/issues/9531) | Update the "Ask the community for help" link in issue templates | 2024-02-07 | 2024-02-07 | 0 | tech_debt |
| [#9558](https://github.com/dbt-labs/dbt-core/issues/9558) | [Feature] Fully anonymize `graph_summary.json` | 2024-02-12 | 2024-02-12 | 0 | enhancement, performance |
| [#9594](https://github.com/dbt-labs/dbt-core/issues/9594) | [Feature] Enable YAML Intersection Selector Independently From Union | 2024-02-16 | 2024-11-12 | 0 | enhancement, node selection |
| [#9607](https://github.com/dbt-labs/dbt-core/issues/9607) | [Feature] Raise a clearer error message for resource types that don't support enforcing contracts | 2024-02-20 | 2025-02-14 | 1 | enhancement, snapshots, stale, model_contracts |
| [#9632](https://github.com/dbt-labs/dbt-core/issues/9632) | unit testing logging for folks who are color blind  | 2024-02-22 | 2024-09-20 | 0 | enhancement, unit tests |
| [#9654](https://github.com/dbt-labs/dbt-core/issues/9654) | [Feature] Upgrade `Dockerfile` to 3.12 | 2024-02-23 | 2025-02-14 | 1 | enhancement, stale |
| [#9731](https://github.com/dbt-labs/dbt-core/issues/9731) | [Feature] Allow curly braces in python model code but don't render it as Jinja | 2024-03-06 | 2024-04-05 | 1 | enhancement, help_wanted |
| [#9892](https://github.com/dbt-labs/dbt-core/issues/9892) | [Feature] Historical Backfilling and Rebasing of Snapshots | 2024-04-11 | 2026-02-26 | 13 | enhancement, snapshots, stale, Refinement |
| [#9957](https://github.com/dbt-labs/dbt-core/issues/9957) | Refactor manifest validations at the end of `get_full_manifest` into rules that are iterated over | 2024-04-16 | 2024-04-16 | 1 | tech_debt |
| [#10100](https://github.com/dbt-labs/dbt-core/issues/10100) | Consolidate mechanism for reading environment variables | 2024-05-07 | 2024-05-07 | 0 | tech_debt |
| [#10268](https://github.com/dbt-labs/dbt-core/issues/10268) | Minor main.yml Improvements | 2024-06-06 | 2024-06-06 | 0 | tech_debt |
| [#10300](https://github.com/dbt-labs/dbt-core/issues/10300) | [spike+] option to generate dbt_scd_id as an integer column instead of a string for performance improvements | 2024-06-12 | 2024-10-17 | 1 | enhancement, snapshots |
| [#10349](https://github.com/dbt-labs/dbt-core/issues/10349) | Make DSI Protocol Tests more reliable | 2024-06-20 | 2024-07-22 | 1 | tech_debt, paper_cut |
| [#10351](https://github.com/dbt-labs/dbt-core/issues/10351) | [Feature] Allow mapping to be used in addition to sequence in YAML to define model columns | 2024-06-21 | 2024-08-23 | 1 | enhancement, triage |
| [#10465](https://github.com/dbt-labs/dbt-core/issues/10465) | Build and check source distributions and wheels independently in build packages | 2024-07-18 | 2024-07-18 | 1 | tech_debt |
| [#10470](https://github.com/dbt-labs/dbt-core/issues/10470) | [Feature] Support epsilon / tolerance for float values in unit test expected values. | 2024-07-19 | 2024-11-22 | 11 | enhancement, wontfix, triage, unit tests |
| [#10471](https://github.com/dbt-labs/dbt-core/issues/10471) | Remove unused `add_ephemeral_model_prefix` method | 2024-07-20 | 2024-07-20 | 0 | tech_debt |
| [#10562](https://github.com/dbt-labs/dbt-core/issues/10562) | Create tests for all disablable resource types with the same name | 2024-08-13 | 2025-02-10 | 1 | stale |
| [#10569](https://github.com/dbt-labs/dbt-core/issues/10569) | [Feature] Clone tables from deps in the same environment | 2024-08-15 | 2024-08-20 | 0 | enhancement, triage, packages, deps, clone |
| [#10573](https://github.com/dbt-labs/dbt-core/issues/10573) | [Feature] Support unit testing for unordered arrays | 2024-08-15 | 2025-10-01 | 2 | enhancement, triage, stale, unit tests |
| [#10579](https://github.com/dbt-labs/dbt-core/issues/10579) | [Feature] Extend partial parsing behavior for env vars to `{{ target }}` also | 2024-08-16 | 2024-10-25 | 1 | enhancement |
| [#10604](https://github.com/dbt-labs/dbt-core/issues/10604) | [Feature] dbt Cloud CLI: rename binary to `dbt-cloud` or make it `pipx`-compatible | 2024-08-25 | 2024-08-27 | 1 | enhancement, triage |
| [#10655](https://github.com/dbt-labs/dbt-core/issues/10655) | [Feature] Create a flexible delete+insert incremental strategy without relying on primary/unique keys | 2024-09-03 | 2024-09-04 | 0 | enhancement, triage, performance, incremental |
| [#10660](https://github.com/dbt-labs/dbt-core/issues/10660) | [Tech debt] Add automation to auto-generate core_types_pb2.py on branches | 2024-09-03 | 2024-09-03 | 0 | user docs, tech_debt |
| [#10696](https://github.com/dbt-labs/dbt-core/issues/10696) | [Tech debt] Speed up windows integration tests | 2024-09-11 | 2024-09-11 | 0 | tech_debt |
| [#10733](https://github.com/dbt-labs/dbt-core/issues/10733) | [Feature] Update `ignore` behaviour for `on_schema_change` to backfill nulls for removed columns, instead of failing | 2024-09-18 | 2024-09-19 | 0 | enhancement, triage, incremental |
| [#10746](https://github.com/dbt-labs/dbt-core/issues/10746) | [Bug] Console logs emitting CANCEL for the last model ran by each thread even if those models finished building when rec | 2024-09-20 | 2024-09-20 | 0 | enhancement, triage |
| [#10759](https://github.com/dbt-labs/dbt-core/issues/10759) | [Feature] Introspective queries that refer to mocked given inputs | 2024-09-23 | 2024-10-29 | 1 | enhancement, unit tests |
| [#10796](https://github.com/dbt-labs/dbt-core/issues/10796) | [Feature] Add tests for installing local package when symlinks are not supported by the OS | 2024-09-30 | 2024-09-30 | 0 | enhancement, tech_debt |
| [#10803](https://github.com/dbt-labs/dbt-core/issues/10803) | [Feature] "Env var required but not provided" should say where it was required | 2024-09-30 | 2024-09-30 | 0 | enhancement, triage |
| [#10816](https://github.com/dbt-labs/dbt-core/issues/10816) | [Feature] enable on-run-end hook and associated contexts to be made available via `dbt clone` | 2024-10-03 | 2024-12-11 | 1 | enhancement, triage |
| [#10841](https://github.com/dbt-labs/dbt-core/issues/10841) | [Feature] Add a flag to clone to skip selected models that don't exist | 2024-10-10 | 2024-10-15 | 1 | enhancement, triage, clone |
| [#10864](https://github.com/dbt-labs/dbt-core/issues/10864) | Improve error messaging for snapshots: unique-key error  | 2024-10-16 | 2024-10-16 | 0 | enhancement, triage |
| [#10899](https://github.com/dbt-labs/dbt-core/issues/10899) | [Feature] Microbatch should support individual lookback windows on both sides of the batch | 2024-10-22 | 2024-10-22 | 0 | enhancement, triage |
| [#10908](https://github.com/dbt-labs/dbt-core/issues/10908) | [Feature] Allow overriding dynamic variables in Unit Tests | 2024-10-23 | 2024-10-23 | 0 | enhancement, triage |
| [#10923](https://github.com/dbt-labs/dbt-core/issues/10923) | [Feature] Raise an error when a custom `dbt_valid_to_current` is configured for a pre-existing snapshot | 2024-10-25 | 2024-10-30 | 1 | enhancement, user docs, snapshots, triage, pre-release |
| [#10925](https://github.com/dbt-labs/dbt-core/issues/10925) | [Feature] Constraint names | 2024-10-25 | 2024-10-28 | 1 | enhancement, triage, model_contracts |
| [#10933](https://github.com/dbt-labs/dbt-core/issues/10933) | Add update_non_tracked_columns flag for snapshot materialization | 2024-10-29 | 2024-12-10 | 0 | enhancement, snapshots, triage |
| [#10942](https://github.com/dbt-labs/dbt-core/issues/10942) | [Tech Debt] Improve unit testing and internal documentation on Contexts and providers | 2024-10-30 | 2024-10-31 | 1 | tech_debt |
| [#10945](https://github.com/dbt-labs/dbt-core/issues/10945) | [Feature] define groups in root-level yml file | 2024-10-30 | 2024-11-07 | 1 | enhancement, user docs, model_groups_access |
| [#10948](https://github.com/dbt-labs/dbt-core/issues/10948) | [Feature] Skip creating a log directory when file logging is disabled | 2024-10-30 | 2024-11-01 | 1 | enhancement, triage |
| [#10956](https://github.com/dbt-labs/dbt-core/issues/10956) | [Feature] Return `agate_table` in `dbt` `run-operation` result | 2024-10-31 | 2024-10-31 | 0 | enhancement, triage |
| [#10992](https://github.com/dbt-labs/dbt-core/issues/10992) | [Feature] add `selector:` method to combine YAML selectors with command line selection | 2024-11-13 | 2024-11-13 | 1 | enhancement, triage |
| [#11004](https://github.com/dbt-labs/dbt-core/issues/11004) | [Feature] Ignore parts of profile_hash and/or env_var_hash in "is_partial_parsable" when running dbt parse | 2024-11-15 | 2024-11-15 | 0 | enhancement, triage |
| [#11013](https://github.com/dbt-labs/dbt-core/issues/11013) | [Feature] Cleaner Call of DBT CLI as Python Module | 2024-11-19 | 2024-11-19 | 0 | enhancement, triage |
| [#11042](https://github.com/dbt-labs/dbt-core/issues/11042) | [Feature] Automatically attempt a full parse before returning a parse error | 2024-11-25 | 2024-11-25 | 0 | enhancement, triage |
| [#11043](https://github.com/dbt-labs/dbt-core/issues/11043) | [Feature] Update .yml file for a model and access that value in `INFORMATION_SCHEMA`.`JOBS` table | 2024-11-25 | 2024-11-26 | 1 | enhancement, awaiting_response |
| [#11059](https://github.com/dbt-labs/dbt-core/issues/11059) | [Feature] Could micro-batch strategy solve most of my windowing problems for incremental models? | 2024-11-27 | 2024-11-27 | 0 | enhancement, triage |
| [#11061](https://github.com/dbt-labs/dbt-core/issues/11061) | [Feature] FK Constraints should respect `--indirect-selection` to avoid trying to attach a constraint to an unbuilt mode | 2024-11-27 | 2024-11-27 | 0 | enhancement, triage |
| [#11070](https://github.com/dbt-labs/dbt-core/issues/11070) | [Feature] Paper cut: include reference to `execute` mode check when introspective queries like `run_query` are skipped | 2024-11-28 | 2024-11-28 | 0 | enhancement, triage |
| [#11074](https://github.com/dbt-labs/dbt-core/issues/11074) | [Enhancement] Align database error line numbers with compiled SQL in `target` | 2024-11-29 | 2024-12-04 | 1 | enhancement, triage |
| [#11077](https://github.com/dbt-labs/dbt-core/issues/11077) | [Feature] Allow jinja templating YML just like we can jinja template SQL | 2024-11-30 | 2024-12-12 | 0 | enhancement, triage, yaml |
| [#11091](https://github.com/dbt-labs/dbt-core/issues/11091) | [Feature] Move component types (like ColumnInfo) to dbt_common | 2024-12-03 | 2024-12-03 | 0 | enhancement, triage |
| [#11093](https://github.com/dbt-labs/dbt-core/issues/11093) | [Feature] Allow defining unit tests in a directory within `test-paths` | 2024-12-04 | 2024-12-06 | 0 | enhancement, paper_cut |
| [#11095](https://github.com/dbt-labs/dbt-core/issues/11095) | [Feature] Make `DBT_SECRET_ENV` prefixes available in run-operation macros | 2024-12-04 | 2024-12-04 | 0 | enhancement, triage |
| [#11126](https://github.com/dbt-labs/dbt-core/issues/11126) | [Feature] UnitTests: Mocking the run_query macro to retrieve data from a given Mocked Source/Reference | 2024-12-11 | 2024-12-11 | 0 | enhancement, triage |
| [#11129](https://github.com/dbt-labs/dbt-core/issues/11129) | [Feature] dbt microbatch, max event_time alternative to now()-lookback | 2024-12-11 | 2024-12-12 | 0 | enhancement, triage, microbatch |
| [#11142](https://github.com/dbt-labs/dbt-core/issues/11142) | [Tech debt] Remove duplicated definition of `default_profiles_dir` (without adding circular dependency) | 2024-12-12 | 2024-12-23 | 1 | tech_debt |
| [#11143](https://github.com/dbt-labs/dbt-core/issues/11143) | [Feature] an option to add freshness: null for tables with filter | 2024-12-12 | 2024-12-12 | 0 | enhancement, triage |
| [#11189](https://github.com/dbt-labs/dbt-core/issues/11189) | [Feature] Execute `post_hook` after `apply_grants` and `persist_docs` | 2024-12-27 | 2025-01-06 | 0 | enhancement |

**Count: 120**

---

## Category 2: Bug (Requests for bug fixes)

Issues that report incorrect behavior, errors, or regressions in dbt-core.

| # | Title | Created | Last Updated | Comments | Labels |
| --- | --- | --- | --- | --- | --- |
| [#7036](https://github.com/dbt-labs/dbt-core/issues/7036) | [CT-2169] [Bug] All global configs should also be settable in ProjectFlags | 2023-02-23 | 2024-02-05 | 7 | bug, user docs |
| [#7084](https://github.com/dbt-labs/dbt-core/issues/7084) | [CT-2230] [Bug] snapshots not working properly with hive when specified column is not lowercase | 2023-02-28 | 2023-03-27 | 5 | bug, help_wanted, snapshots |
| [#7976](https://github.com/dbt-labs/dbt-core/issues/7976) | [CT-2756] [Bug] `relation_name` is always `None` on `SeedNode` | 2023-06-28 | 2024-02-14 | 4 | bug |
| [#8184](https://github.com/dbt-labs/dbt-core/issues/8184) | [CT-2858] [Bug] Ephemeral model exits if there are errors but still results in success | 2023-07-21 | 2023-09-19 | 7 | bug |
| [#8472](https://github.com/dbt-labs/dbt-core/issues/8472) | [CT-3021] [Spike+] Interactive `dbt show` should not require model `name` to be present in `--select` arg | 2023-08-23 | 2025-11-25 | 4 | bug, user docs, Impact: Exp, Medium Severity |
| [#8541](https://github.com/dbt-labs/dbt-core/issues/8541) | [CT-3068] [Bug] relative node paths incl. "../../../" in dbt_project.yml completely destroy repository | 2023-09-02 | 2023-09-14 | 4 | bug |
| [#8562](https://github.com/dbt-labs/dbt-core/issues/8562) | [CT-3079] [Bug] When file is prefixed with dbt_ in the file name it will run the python script twice | 2023-09-06 | 2025-06-02 | 4 | bug, help_wanted, stale |
| [#8679](https://github.com/dbt-labs/dbt-core/issues/8679) | [CT-3135] [Bug] pip install dev-requirements only works on the second attempt | 2023-09-20 | 2023-12-13 | 2 | bug |
| [#8727](https://github.com/dbt-labs/dbt-core/issues/8727) | [CT-3166] [Bug] [Spike+] State modified --select state:modified ignores new sources | 2023-09-27 | 2025-11-07 | 3 | bug, Refinement, state: modified |
| [#8844](https://github.com/dbt-labs/dbt-core/issues/8844) | [CT-3215] Could not parse version "1.0.0.32.5" | 2023-10-12 | 2023-10-13 | 5 | bug |
| [#8848](https://github.com/dbt-labs/dbt-core/issues/8848) | [CT-3217] [Bug] Multiple calls of `dbt-retry` fail with exit code 2 | 2023-10-13 | 2023-12-19 | 5 | bug, retry |
| [#8862](https://github.com/dbt-labs/dbt-core/issues/8862) | [CT-3229] Materialized views are not recreated after configuration change | 2023-10-16 | 2025-06-11 | 8 | bug, Refinement, materialized_views |
| [#8959](https://github.com/dbt-labs/dbt-core/issues/8959) | [CT-3300] [Bug] dbt core process would stuck if error happens at certain stage | 2023-11-01 | 2024-07-16 | 2 | bug |
| [#9055](https://github.com/dbt-labs/dbt-core/issues/9055) | [CT-3372] [Bug] Syntax error in `is_type` macro within test suite (only when test fails) | 2023-11-11 | 2023-11-14 | 5 | bug |
| [#9066](https://github.com/dbt-labs/dbt-core/issues/9066) | [CT-3379] [Bug] Test failures on Windows | 2023-11-13 | 2025-02-14 | 8 | bug, stale, awaiting_response |
| [#9241](https://github.com/dbt-labs/dbt-core/issues/9241) | [CT-3470] [Bug] `dbt deps` fails silently if user has no write perms to project directory | 2023-12-06 | 2025-02-14 | 3 | bug, help_wanted, stale, deps |
| [#9261](https://github.com/dbt-labs/dbt-core/issues/9261) | [CT-3481] [Bug] Manifest parsing yields None macro dependency | 2023-12-08 | 2023-12-12 | 3 | bug |
| [#9304](https://github.com/dbt-labs/dbt-core/issues/9304) | [CT-3507] [Bug] Installing packages to cache location fails with "Cannot call rmtree on a symbolic link" | 2023-12-19 | 2023-12-20 | 7 | bug |
| [#9336](https://github.com/dbt-labs/dbt-core/issues/9336) | [CT-3525] [Bug] Option `--no-send-anonymous-usage-stats` creates different behavior compared to `DBT_SEND_ANONYMOUS_USAG | 2024-01-04 | 2024-04-20 | 4 | bug |
| [#9354](https://github.com/dbt-labs/dbt-core/issues/9354) | [CT-3535] [Bug] If there are no actual sql statements in a run_query(sql) embedded in a pre_hook macro - then the query  | 2024-01-09 | 2024-01-10 | 0 | bug |
| [#9414](https://github.com/dbt-labs/dbt-core/issues/9414) | [Bug] `--no-write-json` does not suppress output to `run_results.json` in dbt-core | 2024-01-22 | 2024-01-23 | 4 | bug, Medium Severity |
| [#9435](https://github.com/dbt-labs/dbt-core/issues/9435) | [Bug] materialized configuration parameter not respected when meta tag is used in model yaml file | 2024-01-24 | 2025-02-14 | 6 | bug, stale, awaiting_response |
| [#9436](https://github.com/dbt-labs/dbt-core/issues/9436) | [Bug] UnboundLocalError: local variable 'summary_message' referenced before assignment | 2024-01-24 | 2024-01-24 | 0 | bug |
| [#9447](https://github.com/dbt-labs/dbt-core/issues/9447) | Nest Macro node `meta` under `config` | 2024-01-24 | 2026-02-24 | 3 | bug, user docs, Impact: CA, dep warnings |
| [#9525](https://github.com/dbt-labs/dbt-core/issues/9525) | [Bug] BigQuery labels in model and dbt_project.yml are not additive | 2024-02-06 | 2025-08-12 | 1 | bug |
| [#9537](https://github.com/dbt-labs/dbt-core/issues/9537) | [Bug] Unexpected error message when `git` is not installed | 2024-02-07 | 2024-02-07 | 0 | bug, Medium Severity |
| [#9604](https://github.com/dbt-labs/dbt-core/issues/9604) | Error while using  `--target-path` option while running `dbt snapshot` | 2024-02-20 | 2025-02-14 | 2 | bug, stale, awaiting_response |
| [#9617](https://github.com/dbt-labs/dbt-core/issues/9617) | [Bug] Missing query-comments on insert statements for models with enforced contracts | 2024-02-21 | 2025-02-14 | 15 | bug, stale, model_contracts |
| [#9647](https://github.com/dbt-labs/dbt-core/issues/9647) | [Bug] Missing query-comments on insert statements for seeds | 2024-02-23 | 2025-02-14 | 3 | bug, stale |
| [#9719](https://github.com/dbt-labs/dbt-core/issues/9719) | [Bug] dbt deps automatically recognizes projects in subdirectories | 2024-03-01 | 2025-06-05 | 8 | bug, windows |
| [#9753](https://github.com/dbt-labs/dbt-core/issues/9753) | [Bug] Bad argument sent to logging in tracking function | 2024-03-12 | 2024-09-11 | 5 | bug, Medium Severity |
| [#9789](https://github.com/dbt-labs/dbt-core/issues/9789) | [Bug] Full refresh model config not respected when coming from a macro and partial parsing is used | 2024-03-21 | 2025-04-18 | 6 | bug, partial_parsing, Medium Severity |
| [#10241](https://github.com/dbt-labs/dbt-core/issues/10241) | [Bug] the run_query macro causes unit tests to fail with a SQL Compilation error | 2024-05-29 | 2026-01-28 | 7 | bug, unit tests |
| [#10254](https://github.com/dbt-labs/dbt-core/issues/10254) | [Bug] Error running unit tests that use the `dbt_utils.star` macro | 2024-06-03 | 2025-11-27 | 9 | bug, unit tests |
| [#10267](https://github.com/dbt-labs/dbt-core/issues/10267) | [Bug] Not able to select unit tests via the `--resource-type` flag | 2024-06-06 | 2024-09-22 | 8 | bug, unit tests |
| [#10296](https://github.com/dbt-labs/dbt-core/issues/10296) | [Bug] `clone` creating "view pointers" instead of "cloned tables" on 1.8 / "Keep on latest version" | 2024-06-12 | 2025-07-22 | 7 | bug, clone |
| [#10345](https://github.com/dbt-labs/dbt-core/issues/10345) | [Bug] Unit test fixture is returning syntax errors for sources with hyphens ('-') in their name | 2024-06-20 | 2026-02-19 | 5 | bug, unit tests |
| [#10402](https://github.com/dbt-labs/dbt-core/issues/10402) | [Bug] Lack of migration path from user configs defined in `profiles.yml` to the 'flags' key in `dbt_project.yml` | 2024-07-03 | 2024-11-27 | 4 | bug |
| [#10422](https://github.com/dbt-labs/dbt-core/issues/10422) | [Regression] Unable to access environment variables on Windows using lowercase variable names | 2024-07-09 | 2026-02-26 | 15 | bug, windows, regression |
| [#10461](https://github.com/dbt-labs/dbt-core/issues/10461) | [Bug] If a test / model config is added (to a `schema.yml`) before a model is written (`model.sql`), partial parsing wil | 2024-07-17 | 2025-09-29 | 5 | bug, dbt tests, partial_parsing |
| [#10527](https://github.com/dbt-labs/dbt-core/issues/10527) | [Bug] dbt's custom exceptions inside a multiprocessing context hangs | 2024-08-06 | 2024-08-07 | 3 | bug, help_wanted |
| [#10574](https://github.com/dbt-labs/dbt-core/issues/10574) | [Bug] `state:modified` not working for windows machine | 2024-08-15 | 2026-01-15 | 10 | bug, windows, triage, state: modified |
| [#10631](https://github.com/dbt-labs/dbt-core/issues/10631) | [Bug] Running `dbt docs generate` gives "Access is denied" error with a read-only asset | 2024-08-29 | 2024-08-29 | 1 | bug |
| [#10652](https://github.com/dbt-labs/dbt-core/issues/10652) | [Bug] Tests for models in installed packages are triggered for models with the same name in the current package | 2024-09-02 | 2024-09-12 | 2 | bug, packages, dbt tests |
| [#10740](https://github.com/dbt-labs/dbt-core/issues/10740) | [Bug] Duplication of CTEs during unit tests when two referenced models have the same custom `alias` | 2024-09-19 | 2026-02-09 | 1 | bug, unit tests |
| [#10741](https://github.com/dbt-labs/dbt-core/issues/10741) | [Bug] `dbt retry` does not work for `dbt docs generate` | 2024-09-19 | 2024-09-19 | 1 | bug |
| [#10768](https://github.com/dbt-labs/dbt-core/issues/10768) | [Bug] Bump the lower bound of `dbt-adapters` within `1.8.latest` | 2024-09-24 | 2024-09-24 | 0 | bug |
| [#10811](https://github.com/dbt-labs/dbt-core/issues/10811) | [Bug] Structured log messages for test results are missing meta properties | 2024-10-02 | 2024-10-07 | 1 | bug, logging |
| [#10886](https://github.com/dbt-labs/dbt-core/issues/10886) | [Bug] Running dbt compile can overwrite seed files | 2024-10-18 | 2024-10-23 | 3 | bug, seeds |
| [#10893](https://github.com/dbt-labs/dbt-core/issues/10893) | [Bug] Version Check Fails on Windows | 2024-10-21 | 2024-10-22 | 0 | bug, windows |
| [#10904](https://github.com/dbt-labs/dbt-core/issues/10904) | [Bug] return in finally can swallow exceptions | 2024-10-22 | 2024-10-23 | 3 | bug |
| [#10913](https://github.com/dbt-labs/dbt-core/issues/10913) | [Bug] package-lock.yml changes sha1-hash value after running dbt deps post package installation | 2024-10-24 | 2026-03-01 | 10 | bug, triage, deps |
| [#11016](https://github.com/dbt-labs/dbt-core/issues/11016) | [Bug] Unexpected behaviour when using config.get() and exceptions.raise_compiler_error() in same model | 2024-11-20 | 2024-11-22 | 2 | bug, awaiting_response |
| [#11032](https://github.com/dbt-labs/dbt-core/issues/11032) | get_manifest_artifacts: Compilation Error in downstream project when upstream project adds versioning to public model re | 2024-11-21 | 2025-06-17 | 4 | bug, multi_project, model_versions |
| [#11040](https://github.com/dbt-labs/dbt-core/issues/11040) | [Bug]  ParsedNode for seed node dup | 2024-11-24 | 2024-11-24 | 1 | bug, tidy_first |
| [#11044](https://github.com/dbt-labs/dbt-core/issues/11044) | mashumaro incompatibility: node.external.partitions returned as string instead of dict | 2024-11-25 | 2025-10-20 | 7 | bug |
| [#11067](https://github.com/dbt-labs/dbt-core/issues/11067) | [Bug] Test for uniqueness passes when duplicates are present in BigQuery column | 2024-11-28 | 2024-12-10 | 2 | bug, awaiting_response |
| [#11075](https://github.com/dbt-labs/dbt-core/issues/11075) | [Regression] BigQuery: Unit tests using sources with sharded tables no longer work after CTE naming change | 2024-11-29 | 2024-12-04 | 3 | bug, regression, unit tests |
| [#11139](https://github.com/dbt-labs/dbt-core/issues/11139) | [Bug] Intermittent errors when unit testing versioned models | 2024-12-12 | 2025-05-15 | 5 | bug, model_versions, unit tests |
| [#11150](https://github.com/dbt-labs/dbt-core/issues/11150) | [Bug] `state:modified` detection incorrect when config() block generated by macro with dynamic `sql_header` | 2024-12-13 | 2025-01-15 | 2 | bug, state, sql_header |
| [#11151](https://github.com/dbt-labs/dbt-core/issues/11151) | [Bug] Docs markdown not working with versioned model  | 2024-12-13 | 2024-12-14 | 1 | bug, dbt-docs, model_versions, cloud |
| [#11176](https://github.com/dbt-labs/dbt-core/issues/11176) | [Bug] Setting model-level `deprecation_date` does not apply to model `versions` | 2024-12-23 | 2024-12-23 | 1 | bug, model_versions |

**Count: 62**

---

## Category 3: Adapters (Requests needing adapter-specific changes)

Issues that require changes in database adapters or the adapter interface.

| # | Title | Created | Last Updated | Comments | Labels |
| --- | --- | --- | --- | --- | --- |
| [#6658](https://github.com/dbt-labs/dbt-core/issues/6658) | [CT-1851] [Feature] Re-render `incremental_predicates` at run-time, similar to hooks | 2023-01-19 | 2026-02-27 | 21 | enhancement, incremental, Team:Adapters |
| [#6795](https://github.com/dbt-labs/dbt-core/issues/6795) | [CT-1976] [Feature] Allow setting postgres port for integration tests | 2023-01-31 | 2023-03-13 | 4 | enhancement, help_wanted, Team:Adapters |
| [#6862](https://github.com/dbt-labs/dbt-core/issues/6862) | [CT-2033] Move all tests into `tests/`, and ensure pre-commit checks are turned on for tests | 2023-02-04 | 2023-08-03 | 5 | tech_debt, Team:Adapters |
| [#6970](https://github.com/dbt-labs/dbt-core/issues/6970) | [CT-2115] In-repo documentation: glossary of terms | 2023-02-14 | 2024-02-13 | 0 | enhancement, python_api, Team:Adapters |
| [#7648](https://github.com/dbt-labs/dbt-core/issues/7648) | [CT-2587] [Feature] Support Query Tagging in Base Materializations | 2023-05-17 | 2026-01-13 | 2 | enhancement, help_wanted, Team:Adapters |
| [#7909](https://github.com/dbt-labs/dbt-core/issues/7909) | [CT-2720] [Feature] `table_type` mapping interface that is adapter-overrideable | 2023-06-20 | 2023-09-28 | 4 | enhancement, adapter_plugins, Refinement |
| [#8487](https://github.com/dbt-labs/dbt-core/issues/8487) | [CT-3027] [Bug] Create test cases for each materialization type that end in a SQL comment (`-- like this`) | 2023-08-24 | 2024-05-07 | 1 | tech_debt, Team:Adapters |
| [#8655](https://github.com/dbt-labs/dbt-core/issues/8655) | [CT-3124] [Bug] Compilation Error when altering an index on a materialized view | 2023-09-16 | 2025-10-01 | 3 | bug, Team:Adapters, materialized_views |
| [#8678](https://github.com/dbt-labs/dbt-core/issues/8678) | [CT-3134] Update pip install instructions in contributing instructions | 2023-09-20 | 2023-09-20 | 0 | Team:Adapters |
| [#8968](https://github.com/dbt-labs/dbt-core/issues/8968) | [CT-3304] [Feature] Clarify error messaging for quoting-related syntax error with source freshness on Snowflake | 2023-11-01 | 2023-11-01 | 1 | enhancement, snowflake, Refinement |
| [#8981](https://github.com/dbt-labs/dbt-core/issues/8981) | [CT-3310] Support `--empty` flag for dbt seed | 2023-11-01 | 2025-12-09 | 5 | user docs, Impact: Adapters, unit tests, empty |
| [#9046](https://github.com/dbt-labs/dbt-core/issues/9046) | [CT-3365] [Feature] Never enumerate all tables in a database/schema | 2023-11-09 | 2024-01-18 | 4 | enhancement, performance, Team:Adapters |
| [#9290](https://github.com/dbt-labs/dbt-core/issues/9290) | [CT-3501] [Epic] Streamline Incremental Strategies | 2023-12-14 | 2024-04-03 | 0 | Epic, Team:Adapters |
| [#9419](https://github.com/dbt-labs/dbt-core/issues/9419) | [Bug] postgres `get_columns_in_relation` does not include MVs | 2024-01-22 | 2024-11-26 | 3 | bug, Team:Adapters, materialized_views |

**Count: 14**

---

## Category 4: Already Implemented (Features/improvements now part of dbt-core)

Issues requesting features or improvements that have since been implemented in later versions of dbt-core.

| # | Title | Created | Labels | Evidence of Implementation |
| --- | --- | --- | --- | --- |
| [#7869](https://github.com/dbt-labs/dbt-core/issues/7869) | Snapshot `dbt_updated_at` not updated | 2023-06-12 | user docs, snapshots, behavior_change_flag | `SnapshotMetaColumnNames` class in `core/dbt/artifacts/resources/v1/snapshot.py` supports configurable `dbt_updated_at` column names |
| [#8664](https://github.com/dbt-labs/dbt-core/issues/8664) | Ability to test incremental behavior of models in unit tests | 2023-09-18 | enhancement, unit tests | Unit test parser in `core/dbt/parser/unit_tests.py` supports `is_incremental` boolean override via `overrides.macros` and validates it is provided for incremental models |
| [#8825](https://github.com/dbt-labs/dbt-core/issues/8825) | Automate creation of `metricflow_time_spine` if the project defines semantic objects | 2023-10-11 | user docs, Impact: SL | `TimeSpine` class in `core/dbt/artifacts/resources/v1/model.py` and comprehensive tests in `tests/functional/time_spines/test_time_spines.py` |
| [#9724](https://github.com/dbt-labs/dbt-core/issues/9724) | Migrate to `Protobuf>=5.26.0,<6` | 2024-03-04 | user docs, dependencies | Superseded — `core/pyproject.toml` now specifies `protobuf>=6.0,<7.0`, a major version beyond what was requested |
| [#10227](https://github.com/dbt-labs/dbt-core/issues/10227) | Unit Tests Should Support `ref` & `source` statements when specifying rows with SQL | 2024-06-03 | enhancement, triage, unit tests | Unit test parser in `core/dbt/parser/unit_tests.py` handles versioned refs (line 158) and source definitions (line 167) in fixture inputs |
| [#10959](https://github.com/dbt-labs/dbt-core/issues/10959) | Add Behavior Change for legacy time spine configuration | 2024-10-31 | enhancement, awaiting_response | `require_yaml_configuration_for_mf_time_spines` behavior change flag in `core/dbt/contracts/project.py` and enforced in `core/dbt/contracts/graph/semantic_manifest.py` |

**Count: 6**

---

## Category 5: Active Feature Requests (not stale)

Feature requests and enhancements with meaningful community engagement that remain relevant.

| # | Title | Created | Last Updated | Comments | Labels |
| --- | --- | --- | --- | --- | --- |
| [#6693](https://github.com/dbt-labs/dbt-core/issues/6693) | [CT-1877] [Bug] Jinja expression is not parsed under tests meta | 2023-01-22 | 2023-09-21 | 3 | enhancement, dbt tests |
| [#6746](https://github.com/dbt-labs/dbt-core/issues/6746) | [CT-1914] Store links to tests on model node in manifest | 2023-01-26 | 2023-11-28 | 4 | performance |
| [#6789](https://github.com/dbt-labs/dbt-core/issues/6789) | [CT-1951] [Feature] get_relations_by_pattern should move from utils to Core | 2023-01-31 | 2024-02-13 | 4 | enhancement, Refinement, utils |
| [#6794](https://github.com/dbt-labs/dbt-core/issues/6794) | [CT-1952] [Feature] Provide additional Jinja tests on top of the built-in ones | 2023-01-31 | 2024-02-06 | 8 | enhancement, Refinement, paper_cut |
| [#6845](https://github.com/dbt-labs/dbt-core/issues/6845) | [CT-2020] [Bug] replace bump version dependency with bump2version | 2023-02-02 | 2023-11-16 | 4 | tech_debt, release |
| [#7099](https://github.com/dbt-labs/dbt-core/issues/7099) | [CT-2251] Inline model documentation + tests | 2023-03-01 | 2025-08-06 | 6 | enhancement, paper_cut |
| [#7109](https://github.com/dbt-labs/dbt-core/issues/7109) | [CT-2260] [Feature] Explainable state modified | 2023-03-02 | 2024-02-07 | 2 | enhancement, help_wanted, state, node selection, state: modified |
| [#7117](https://github.com/dbt-labs/dbt-core/issues/7117) | [CT-2266] [Feature] Make MAXIMUM_SEED_SIZE configurable | 2023-03-03 | 2026-03-03 | 3 | enhancement, good_first_issue |
| [#7124](https://github.com/dbt-labs/dbt-core/issues/7124) | [CT-2271] [Feature] Compute seed file hashes incrementally | 2023-03-05 | 2026-03-03 | 4 | enhancement, help_wanted |
| [#7138](https://github.com/dbt-labs/dbt-core/issues/7138) | [CT-2280] Tracking changes in the non-chronological insertions of data in dbt snapshots (check strategy) | 2023-03-08 | 2023-09-21 | 6 | enhancement, snapshots |
| [#7316](https://github.com/dbt-labs/dbt-core/issues/7316) | [CT-2393] [Bug] On MacOS, running dbt functional tests fails with errors opening files | 2023-04-11 | 2023-10-10 | 7 | enhancement |
| [#7391](https://github.com/dbt-labs/dbt-core/issues/7391) | [CT-2429] `dbt show`: option to use already-materialized model instead of rerunning SQL | 2023-04-18 | 2026-02-24 | 11 | enhancement, Refinement, paper_cut |
| [#7432](https://github.com/dbt-labs/dbt-core/issues/7432) | [CT-2460] [Feature] Infer schema from prod, to enforce contract & detect breaking changes in dev | 2023-04-21 | 2026-02-25 | 22 | enhancement, state, spike, Refinement, multi_project, model_contracts |
| [#7434](https://github.com/dbt-labs/dbt-core/issues/7434) | [CT-2463] Add “enabled” to packages config | 2023-04-22 | 2026-03-02 | 12 | enhancement, help_wanted, deps |
| [#7442](https://github.com/dbt-labs/dbt-core/issues/7442) | [CT-2468] [Feature] For versioned models, automatically create view/clone of latest version in unsuffixed database locat | 2023-04-24 | 2025-11-07 | 34 | enhancement, multi_project, model_versions |
| [#7513](https://github.com/dbt-labs/dbt-core/issues/7513) | [CT-2532] [Feature] Validate selector nodes like any other selection | 2023-05-04 | 2026-02-16 | 2 | enhancement, help_wanted, node selection |
| [#7516](https://github.com/dbt-labs/dbt-core/issues/7516) | [CT-2534] [Feature] Define generic tests on many models at once | 2023-05-05 | 2024-01-03 | 8 | enhancement, dbt tests |
| [#7613](https://github.com/dbt-labs/dbt-core/issues/7613) | [CT-2573] [Feature] Support column-level tests on nested data, natively | 2023-05-12 | 2026-01-23 | 15 | enhancement, help_wanted, dbt tests |
| [#7703](https://github.com/dbt-labs/dbt-core/issues/7703) | [CT-2599] [Feature] childrens_parents depth  | 2023-05-25 | 2023-09-27 | 8 | enhancement, good_first_issue, node selection |
| [#7732](https://github.com/dbt-labs/dbt-core/issues/7732) | [CT-2609] [Feature] Allow defining a default backfill value for incremental models with constraints | 2023-05-29 | 2025-09-23 | 9 | enhancement, incremental, Refinement, model_contracts |
| [#7749](https://github.com/dbt-labs/dbt-core/issues/7749) | [CT-2618] [Feature] List available tags | 2023-06-01 | 2026-03-02 | 6 | enhancement, awaiting_response |
| [#7781](https://github.com/dbt-labs/dbt-core/issues/7781) | [CT-2636] TestVersionedModels::test_pp_versioned_models is flaky | 2023-06-05 | 2025-11-25 | 6 | tech_debt, flaky_test |
| [#7867](https://github.com/dbt-labs/dbt-core/issues/7867) | [CT-2699] [Feature] "interactive" compile should include the compiled code of a snapshot | 2023-06-14 | 2026-03-02 | 4 | enhancement, help_wanted, snapshots |
| [#7945](https://github.com/dbt-labs/dbt-core/issues/7945) | [CT-2738] [Feature] Programmatic invocations with supplied manifest should perform partial parsing state check | 2023-06-25 | 2024-07-25 | 2 | enhancement, python_api, partial_parsing |
| [#7994](https://github.com/dbt-labs/dbt-core/issues/7994) | [CT-2767] [Feature] Use `--quiet` by default for `dbt list --output json` | 2023-06-29 | 2024-01-29 | 3 | enhancement, cli, Refinement |
| [#8031](https://github.com/dbt-labs/dbt-core/issues/8031) | [CT-2790] Support dispatch for materialization macros | 2023-07-05 | 2025-06-23 | 2 | tech_debt |
| [#8061](https://github.com/dbt-labs/dbt-core/issues/8061) | [CT-2808] [Feature] Support hiding all resources from a package (not just models) | 2023-07-10 | 2025-04-13 | 9 | enhancement, dbt-docs |
| [#8169](https://github.com/dbt-labs/dbt-core/issues/8169) | [CT-2854] [Feature] `.dbtignore` doesn't ignore files in dbt_packages | 2023-07-20 | 2024-10-16 | 10 | enhancement, Refinement |
| [#8205](https://github.com/dbt-labs/dbt-core/issues/8205) | [CT-2868] [Feature] Enable `dbt ls` to access run results state (related to `retry`) | 2023-07-25 | 2023-12-19 | 3 | enhancement, retry |
| [#8207](https://github.com/dbt-labs/dbt-core/issues/8207) | [CT-2869] Add new snapshot records when source removes a field | 2023-07-25 | 2024-09-16 | 10 | enhancement, snapshots, Refinement |
| [#8223](https://github.com/dbt-labs/dbt-core/issues/8223) | [CT-2876] [Feature] add possibility to copy dbt local packages instead of make it symlink | 2023-07-27 | 2026-02-23 | 10 | enhancement, help_wanted, deps, Refinement |
| [#8266](https://github.com/dbt-labs/dbt-core/issues/8266) | [CT-2897] [Feature] Suppress stack trace when a model contract is violated | 2023-08-01 | 2024-02-05 | 4 | enhancement, multi_project, model_contracts |
| [#8274](https://github.com/dbt-labs/dbt-core/issues/8274) | [CT-2904] [Bug] Should clone with --full-refresh respect the model config? | 2023-08-01 | 2024-02-01 | 2 | enhancement, Refinement, clone |
| [#8276](https://github.com/dbt-labs/dbt-core/issues/8276) | [CT-2905] [Feature] Give node information when raising ValidationError | 2023-08-02 | 2023-11-08 | 5 | enhancement, help_wanted, snapshots, paper_cut |
| [#8284](https://github.com/dbt-labs/dbt-core/issues/8284) | [CT-2912] Backport #8210 | 2023-08-02 | 2023-09-06 | 1 | logging, Impact: Exp, Impact: Customer Support |
| [#8356](https://github.com/dbt-labs/dbt-core/issues/8356) | [CT-2958] [Feature] Cancel open connections on SIGTERM (in addition to SIGINT) | 2023-08-10 | 2025-05-26 | 10 | enhancement |
| [#8387](https://github.com/dbt-labs/dbt-core/issues/8387) | [CT-2972] [Epic] MyPy Cleanup | 2023-08-14 | 2025-12-03 | 0 | tech_debt |
| [#8467](https://github.com/dbt-labs/dbt-core/issues/8467) | [CT-3017] [Feature] Support `show` of python model | 2023-08-22 | 2025-12-04 | 2 | enhancement, Refinement |
| [#8499](https://github.com/dbt-labs/dbt-core/issues/8499) | [CT-3033] [Spike] Explore support multiple unit test materialization strategies: CTE vs 'seed'-based | 2023-08-25 | 2025-12-22 | 5 | enhancement, unit tests |
| [#8563](https://github.com/dbt-labs/dbt-core/issues/8563) | [CT-3080] [Bug] Improve Error Messages for Grants | 2023-09-06 | 2025-02-07 | 5 | enhancement, paper_cut, grants |
| [#8586](https://github.com/dbt-labs/dbt-core/issues/8586) | [CT-3088] [Feature] Automatically grant usage on schema | 2023-09-07 | 2025-05-28 | 13 | enhancement, paper_cut |
| [#8690](https://github.com/dbt-labs/dbt-core/issues/8690) | [CT-3142] [Feature] Make the current git branch (if any) available in the dbt Jinja context | 2023-09-22 | 2025-07-30 | 9 | enhancement, help_wanted |
| [#8708](https://github.com/dbt-labs/dbt-core/issues/8708) | [CT-3153] [implementation] limit the size of fixtures for unit testing | 2023-09-25 | 2024-09-20 | 1 | user docs, unit tests |
| [#8712](https://github.com/dbt-labs/dbt-core/issues/8712) | [CT-3156] [Developer Portal] Define Simple Service Maturity Matrix | 2023-09-26 | 2023-10-11 | 2 |  |
| [#8722](https://github.com/dbt-labs/dbt-core/issues/8722) | [CT-3162] [Feature] Ability to `dbt clone` sources | 2023-09-26 | 2024-02-01 | 4 | enhancement, clone |
| [#8725](https://github.com/dbt-labs/dbt-core/issues/8725) | [CT-3164] [Feature] add OpenLineage Datasets standard names in the Manifest | 2023-09-26 | 2024-04-09 | 12 | enhancement |
| [#8769](https://github.com/dbt-labs/dbt-core/issues/8769) | [CT-3183] [Feature] custom incremental strategy from dbt package | 2023-10-04 | 2024-01-07 | 7 | enhancement, Refinement |
| [#8796](https://github.com/dbt-labs/dbt-core/issues/8796) | [CT-3193] [Feature] Raise an error or warning if snapshot block name and file name do not match | 2023-10-09 | 2023-10-11 | 3 | enhancement, snapshots, Refinement |
| [#8852](https://github.com/dbt-labs/dbt-core/issues/8852) | [CT-3221] [Feature] remove compiled code of first selected node when running `dbt docs generate --select` | 2023-10-13 | 2025-11-25 | 2 | enhancement |
| [#8858](https://github.com/dbt-labs/dbt-core/issues/8858) | [CT-3225] [Feature] permit python models to declare `ref` and `source` models independently of data-access | 2023-10-14 | 2026-02-28 | 13 | enhancement, triage |
| [#8874](https://github.com/dbt-labs/dbt-core/issues/8874) | [CT-3240] SonarQube Integration to the DBT | 2023-10-20 | 2023-10-20 | 0 | user docs |
| [#8879](https://github.com/dbt-labs/dbt-core/issues/8879) | [CT-3245] [Feature] expected_references_catalog assumes column indexing starts at 1 | 2023-10-23 | 2023-10-23 | 2 | enhancement |
| [#8951](https://github.com/dbt-labs/dbt-core/issues/8951) | [CT-3296] [Feature] Option to enforce 2-argument `ref()` when referencing models from other packages/projects | 2023-10-31 | 2023-11-29 | 9 | enhancement, multi_project |
| [#8955](https://github.com/dbt-labs/dbt-core/issues/8955) | [CT-3298] [Materialized Views] Move catalog relation_type tests into the Adapter Zone to allow reuse of the test case | 2023-10-31 | 2025-11-25 | 0 |  |
| [#8986](https://github.com/dbt-labs/dbt-core/issues/8986) | [CT-3314] [Feature] Support `project` as an alias for `package` selection method | 2023-11-02 | 2026-02-16 | 2 | enhancement, good_first_issue, multi_project |
| [#8993](https://github.com/dbt-labs/dbt-core/issues/8993) | [CT-3318] [Feature] Support the `group` resource type within `dbt list` | 2023-11-03 | 2023-11-07 | 2 | enhancement |
| [#9006](https://github.com/dbt-labs/dbt-core/issues/9006) | [CT-3324] [Spike] Investigate process_docs for performance issues | 2023-11-06 | 2023-11-06 | 0 | performance |
| [#9011](https://github.com/dbt-labs/dbt-core/issues/9011) | [CT-3327] [Feature] remove `version` and `config-version` from the starter project in dbt-core | 2023-11-06 | 2024-10-10 | 2 | enhancement |
| [#9095](https://github.com/dbt-labs/dbt-core/issues/9095) | [CT-3393] Unpin ddtrace in dev-requirements | 2023-11-15 | 2024-01-23 | 1 | user docs |
| [#9164](https://github.com/dbt-labs/dbt-core/issues/9164) | [CT-3430] [Feature] dbt should tell me if a custom macro in my project overrides one in the global project or my adapter | 2023-11-28 | 2025-08-11 | 9 | enhancement, paper_cut |
| [#9220](https://github.com/dbt-labs/dbt-core/issues/9220) | [CT-3460] [unit testing] Update unique id for data tests | 2023-12-05 | 2024-09-20 | 3 | user docs, Impact: CA, unit tests |
| [#9235](https://github.com/dbt-labs/dbt-core/issues/9235) | Make config names all use underscores | 2023-12-06 | 2025-02-04 | 4 | user docs |
| [#9246](https://github.com/dbt-labs/dbt-core/issues/9246) | [CT-3473] Deadlocks in postgres related to drop cascade of `__dbt_backup` tables with downstream dependencies. | 2023-12-07 | 2025-06-16 | 4 |  |
| [#9262](https://github.com/dbt-labs/dbt-core/issues/9262) | [CT-3482] Support a "time spine" aggregation time dimension type | 2023-12-09 | 2024-09-30 | 1 | user docs, Refinement, semantic, Impact: CA, Impact: SL |
| [#9263](https://github.com/dbt-labs/dbt-core/issues/9263) | [CT-3483] Support Semantic Layer timezone configs | 2023-12-09 | 2024-09-30 | 3 | user docs, Refinement, semantic, Impact: CA, Impact: SL |
| [#9278](https://github.com/dbt-labs/dbt-core/issues/9278) | [CT-3491] Secret-Handling Review | 2023-12-13 | 2023-12-13 | 0 | user docs |
| [#9280](https://github.com/dbt-labs/dbt-core/issues/9280) | [CT-3495] I want to use spacing to make my csv input/expected output mock data more readable for unit tests | 2023-12-13 | 2024-11-22 | 3 | enhancement, paper_cut, unit tests |
| [#9282](https://github.com/dbt-labs/dbt-core/issues/9282) | [CT-3496] allow "missing" inputs for inputs where you want to use empty mock data for unit test | 2023-12-13 | 2024-09-20 | 2 | enhancement, unit tests |
| [#9283](https://github.com/dbt-labs/dbt-core/issues/9283) | [CT-3497] I want to add a description/label to each of the rows in my unit test to explicitly call out the edge cases I' | 2023-12-13 | 2024-09-20 | 2 | enhancement, unit tests |
| [#9339](https://github.com/dbt-labs/dbt-core/issues/9339) | [CT-3527] [implementation] extend access and groups to exposures & source | 2024-01-04 | 2026-02-20 | 3 | enhancement, user docs, multi_project, model_groups_access |
| [#9340](https://github.com/dbt-labs/dbt-core/issues/9340) | [CT-3528] [Feature] Define a "yeslist" of downstream projects that can reference specific `protected` model(s) | 2024-01-05 | 2026-01-23 | 8 | enhancement, Refinement, multi_project, model_groups_access |
| [#9351](https://github.com/dbt-labs/dbt-core/issues/9351) | [CT-3532] [DEV UX] Have a guide for how to add tests in dbt-core | 2024-01-08 | 2026-02-26 | 0 |  |
| [#9363](https://github.com/dbt-labs/dbt-core/issues/9363) | [CT-3543] [Feature] Global config to apply pretty-print formatting to JSON artifacts | 2024-01-10 | 2024-10-11 | 3 | enhancement, help_wanted, paper_cut, Impact: CA |
| [#9506](https://github.com/dbt-labs/dbt-core/issues/9506) | [SPIKE] Experiment with performance of catalog queries | 2024-02-01 | 2024-11-11 | 3 | enhancement |
| [#9510](https://github.com/dbt-labs/dbt-core/issues/9510) | [Feature] Create indexes also without full refresh | 2024-02-02 | 2025-02-10 | 3 | enhancement, materialized_views, index |
| [#9524](https://github.com/dbt-labs/dbt-core/issues/9524) | [Feature] Improve memory performance of the `dbt seed` command | 2024-02-06 | 2024-10-15 | 14 | enhancement, help_wanted, performance |
| [#9575](https://github.com/dbt-labs/dbt-core/issues/9575) | [Feature] Add `target_path` to `global_flags` | 2024-02-14 | 2024-02-20 | 2 | enhancement, retry |
| [#9595](https://github.com/dbt-labs/dbt-core/issues/9595) | [Feature] Include column-level information in dbt's adapter cache, for faster get_columns_in_relation | 2024-02-17 | 2024-05-20 | 4 | enhancement, performance |
| [#9599](https://github.com/dbt-labs/dbt-core/issues/9599) | [Feature] Be able to `--favor-state` for sources | 2024-02-19 | 2024-08-02 | 5 | enhancement, state |
| [#9602](https://github.com/dbt-labs/dbt-core/issues/9602) | ExposureType - custom extension of EnumType | 2024-02-20 | 2025-01-11 | 5 | enhancement, exposures, Refinement, paper_cut |
| [#9656](https://github.com/dbt-labs/dbt-core/issues/9656) | [Feature] Warn if duplicate columns are found in `check` Snapshot strategy | 2024-02-23 | 2025-03-31 | 4 | enhancement, help_wanted, snapshots |
| [#9683](https://github.com/dbt-labs/dbt-core/issues/9683) | [Feature] I want my unit tests to optionally execute pre/post hooks | 2024-02-27 | 2025-12-03 | 3 | enhancement, dbt tests |
| [#9692](https://github.com/dbt-labs/dbt-core/issues/9692) | [Feature] Include sources in `dbt list -s "fqn:*"` | 2024-02-28 | 2024-10-21 | 5 | Impact: CA, list |
| [#9695](https://github.com/dbt-labs/dbt-core/issues/9695) | [Feature] Add ability to import/include YAML from other files | 2024-02-28 | 2025-07-09 | 10 | enhancement, Refinement, paper_cut, yaml |
| [#9740](https://github.com/dbt-labs/dbt-core/issues/9740) | [Feature] Custom test name not honoured in "config" blocks | 2024-03-08 | 2024-04-03 | 4 | enhancement, help_wanted |
| [#9775](https://github.com/dbt-labs/dbt-core/issues/9775) | [Feature] Make `sql_header` configuration available on tests | 2024-03-19 | 2026-03-03 | 13 | enhancement, help_wanted, dbt tests |
| [#9843](https://github.com/dbt-labs/dbt-core/issues/9843) | [Feature] Use `--quiet` by default for `dbt show` | 2024-04-02 | 2025-02-11 | 4 | enhancement, Refinement, show |
| [#9884](https://github.com/dbt-labs/dbt-core/issues/9884) | [Unit Testing] Allow explicit precision testing in unit tests | 2024-04-09 | 2024-09-20 | 5 | enhancement, user docs, unit tests |
| [#9989](https://github.com/dbt-labs/dbt-core/issues/9989) | [Feature] Quick connection timeout for anonymous snowplow tracking | 2024-04-20 | 2024-04-21 | 2 | enhancement, help_wanted |
| [#10090](https://github.com/dbt-labs/dbt-core/issues/10090) | [Feature] Support `dispatch` for materializations to use implementations defined in installed packages | 2024-05-05 | 2025-09-11 | 10 | enhancement, user docs |
| [#10139](https://github.com/dbt-labs/dbt-core/issues/10139) | [Enhancement] Enable `adapter.get_columns_in_relation()` macro to return columns during unit testing | 2024-05-14 | 2025-05-07 | 10 | enhancement, unit tests |
| [#10161](https://github.com/dbt-labs/dbt-core/issues/10161) | [Feature] Support Semi-Structured Data Columns in Unit Test dict fixture data | 2024-05-16 | 2025-07-22 | 12 | enhancement, user docs, triage, unit tests |
| [#10164](https://github.com/dbt-labs/dbt-core/issues/10164) | [Feature] When a test fails, `dbt retry` should also rebuild the model(s) against which the test failed instead of re-ru | 2024-05-16 | 2025-12-25 | 4 | enhancement, triage, dbt tests, retry |
| [#10195](https://github.com/dbt-labs/dbt-core/issues/10195) | [Feature] Support constraints independently from enforcing a full model contract | 2024-05-21 | 2024-12-06 | 14 | enhancement, paper_cut, model_contracts |
| [#10219](https://github.com/dbt-labs/dbt-core/issues/10219) | [Feature] populate model['constraints'] even if the contract is not enforced | 2024-05-23 | 2026-02-06 | 6 | enhancement, model_contracts |
| [#10236](https://github.com/dbt-labs/dbt-core/issues/10236) | New snapshot config to validate uniqueness before merge | 2024-05-28 | 2024-10-02 | 1 | user docs, snapshots |
| [#10279](https://github.com/dbt-labs/dbt-core/issues/10279) | [Feature] Make warning and error classes more discoverable | 2024-06-07 | 2026-02-28 | 2 | enhancement, triage |
| [#10306](https://github.com/dbt-labs/dbt-core/issues/10306) | [Feature] Check `config` macros keys for validity, possible typos | 2024-06-13 | 2026-02-28 | 3 | enhancement, triage |
| [#10313](https://github.com/dbt-labs/dbt-core/issues/10313) | [Feature] `flags.INVOCATION_COMMAND` for programmatic dbt invocations | 2024-06-14 | 2025-02-26 | 4 | enhancement, help_wanted |
| [#10369](https://github.com/dbt-labs/dbt-core/issues/10369) | [Feature] Show branch names / version tags in logs when running `dbt deps` | 2024-06-26 | 2026-02-28 | 2 | enhancement, triage, deps |
| [#10381](https://github.com/dbt-labs/dbt-core/issues/10381) | Use `git tag --no-column` for listing tags for `dbt deps` | 2024-06-28 | 2024-07-03 | 2 | enhancement, help_wanted, deps |
| [#10388](https://github.com/dbt-labs/dbt-core/issues/10388) | [Feature] Warn or error when intersection selection syntax includes a hanging `+ ` | 2024-07-01 | 2024-07-08 | 5 | enhancement, help_wanted |
| [#10403](https://github.com/dbt-labs/dbt-core/issues/10403) | [Feature] Support doc block in meta | 2024-07-04 | 2026-02-28 | 2 | enhancement, triage |
| [#10431](https://github.com/dbt-labs/dbt-core/issues/10431) | [Feature]  Optional Node Selection for --favor-state | 2024-07-11 | 2026-02-28 | 2 | enhancement, triage |
| [#10438](https://github.com/dbt-labs/dbt-core/issues/10438) | [Feature] exclude option for check_cols snapshot | 2024-07-12 | 2025-05-22 | 8 | enhancement, snapshots |
| [#10441](https://github.com/dbt-labs/dbt-core/issues/10441) | Feature Request: Support for Default Model Profiles | 2024-07-13 | 2026-02-26 | 4 | enhancement, Refinement |
| [#10447](https://github.com/dbt-labs/dbt-core/issues/10447) | [Feature] Allow adapters to override unspecified values in unit tests | 2024-07-15 | 2026-02-28 | 3 | enhancement, triage, unit tests |
| [#10460](https://github.com/dbt-labs/dbt-core/issues/10460) | [Feature] Add new global flag `--no-write-manifest`  | 2024-07-17 | 2026-02-28 | 2 | enhancement, python_api, triage |
| [#10464](https://github.com/dbt-labs/dbt-core/issues/10464) | DBT External tables don't work with the `--empty` flag | 2024-07-18 | 2025-11-07 | 1 | awaiting_response, unit tests, empty |
| [#10476](https://github.com/dbt-labs/dbt-core/issues/10476) | [Feature] `docs generate` - retrieve column descriptions from DB when available - for Sources | 2024-07-23 | 2026-01-26 | 5 | enhancement, dbt-docs, triage |
| [#10485](https://github.com/dbt-labs/dbt-core/issues/10485) | [Feature] Accept `None` as a default in `env_var` | 2024-07-25 | 2024-09-05 | 3 | enhancement, triage |
| [#10490](https://github.com/dbt-labs/dbt-core/issues/10490) | [Feature] Support contracts in Redshift even when the first row has `NULL` values | 2024-07-26 | 2025-02-25 | 6 | enhancement, triage, model_contracts |
| [#10492](https://github.com/dbt-labs/dbt-core/issues/10492) | [Feature] Set vars via environment variables | 2024-07-27 | 2026-02-28 | 2 | enhancement, triage |
| [#10503](https://github.com/dbt-labs/dbt-core/issues/10503) | [Feature] Improve error messages when calling a non-existent macro in the config block of a model | 2024-07-30 | 2026-02-28 | 3 | enhancement, triage |
| [#10546](https://github.com/dbt-labs/dbt-core/issues/10546) | [Feature] Apply grants to dbt_test__audit when storing test failures | 2024-08-08 | 2025-05-26 | 5 | enhancement, triage, store_failures |
| [#10547](https://github.com/dbt-labs/dbt-core/issues/10547) | [Feature] Unit test support for macros | 2024-08-09 | 2025-10-22 | 7 | enhancement, triage, unit tests |
| [#10578](https://github.com/dbt-labs/dbt-core/issues/10578) | [Feature] Extend partial parsing behavior for env vars to project vars also | 2024-08-16 | 2024-09-04 | 2 | enhancement |
| [#10587](https://github.com/dbt-labs/dbt-core/issues/10587) | [Feature] Support Snowflake Database Roles in grants | 2024-08-21 | 2025-01-23 | 2 | enhancement, grants |
| [#10592](https://github.com/dbt-labs/dbt-core/issues/10592) | [Feature] Disable `on-run-start` and `on-run-end` hooks from installed packages | 2024-08-22 | 2026-03-02 | 4 | enhancement, triage, Refinement |
| [#10593](https://github.com/dbt-labs/dbt-core/issues/10593) | [Feature] Apply selection only once when running `dbt build` | 2024-08-22 | 2026-02-26 | 0 | enhancement, help_wanted, performance |
| [#10596](https://github.com/dbt-labs/dbt-core/issues/10596) | [Feature] Resource selection: allow to specify intersection with union | 2024-08-23 | 2025-09-15 | 9 | enhancement, triage, node selection |
| [#10632](https://github.com/dbt-labs/dbt-core/issues/10632) | [Feature] Configuration for runtime "priority" among models with satisfied dependencies | 2024-08-29 | 2025-05-02 | 6 | enhancement, triage, performance |
| [#10645](https://github.com/dbt-labs/dbt-core/issues/10645) | [Feature] Add precision and scale option for data type constraints | 2024-08-29 | 2025-10-27 | 1 | enhancement, triage, model_contracts |
| [#10661](https://github.com/dbt-labs/dbt-core/issues/10661) | [Feature] No Metadata check supported for SQL input type under Unit Test | 2024-09-03 | 2025-02-02 | 3 | enhancement, triage, unit tests |
| [#10692](https://github.com/dbt-labs/dbt-core/issues/10692) | [Feature] Summary stats for source freshness run | 2024-09-11 | 2024-09-30 | 2 | enhancement, triage |
| [#10702](https://github.com/dbt-labs/dbt-core/issues/10702) | Make `begin` on microbatch incremental models optional by calculating min of mins | 2024-09-11 | 2025-02-10 | 1 | microbatch |
| [#10708](https://github.com/dbt-labs/dbt-core/issues/10708) | [Feature] Support for Bi temporality in dbt snapshot | 2024-09-13 | 2026-02-27 | 0 | enhancement, snapshots, triage |
| [#10760](https://github.com/dbt-labs/dbt-core/issues/10760) | [Feature] Fail during `dbt parse` when any dependency is not installed via `dbt deps` | 2024-09-23 | 2024-10-01 | 7 | enhancement, Impact: Exp, behavior_change_flag |
| [#10808](https://github.com/dbt-labs/dbt-core/issues/10808) | [Feature] Support calling `dbt.config.get()` inside another `dbt.config()` call | 2024-09-25 | 2024-10-29 | 3 | enhancement, python |
| [#10834](https://github.com/dbt-labs/dbt-core/issues/10834) | [Feature] Allow node level dependency deprecation warnings | 2024-10-08 | 2025-11-07 | 2 | enhancement, paper_cut, model_versions |
| [#10838](https://github.com/dbt-labs/dbt-core/issues/10838) | [Feature] "Full Refresh" on_schema_change strategy | 2024-10-09 | 2025-03-15 | 2 | enhancement, triage, incremental |
| [#10844](https://github.com/dbt-labs/dbt-core/issues/10844) | [Feature]  unit testing: dbt should tell me why it couldn't get columns of this (the model doesn't yet exist) on increme | 2024-10-11 | 2024-11-19 | 2 | enhancement, triage, unit tests |
| [#10858](https://github.com/dbt-labs/dbt-core/issues/10858) | [Feature] Implement Locking to Prevent Simultaneous Runs of the Same dbt Project Across Hosts | 2024-10-16 | 2025-06-05 | 2 | enhancement, triage |
| [#10875](https://github.com/dbt-labs/dbt-core/issues/10875) | [Feature] Improvements to source properties handling | 2024-10-17 | 2024-11-22 | 2 | enhancement, triage |
| [#10877](https://github.com/dbt-labs/dbt-core/issues/10877) | [Feature] new configuration to run tests on only the "new" data for snapshots and incremental models | 2024-10-17 | 2025-06-10 | 3 | enhancement, cardboard_cut |
| [#10891](https://github.com/dbt-labs/dbt-core/issues/10891) | [Feature] Enable unit testing for models that use `adapter.get_relation` | 2024-10-21 | 2024-10-29 | 5 | enhancement, triage, unit tests |
| [#10896](https://github.com/dbt-labs/dbt-core/issues/10896) | [Feature] Support for `clone` as a model materialization type | 2024-10-21 | 2025-07-22 | 6 | enhancement, triage |
| [#10912](https://github.com/dbt-labs/dbt-core/issues/10912) | [Feature] allow defer of sources | 2024-10-24 | 2026-02-26 | 1 | enhancement, awaiting_response |
| [#10914](https://github.com/dbt-labs/dbt-core/issues/10914) | Create dbt.var.get() for python models to retrieve project variables without a yaml file  | 2024-10-24 | 2026-02-26 | 3 | enhancement, vars, python_models |
| [#10918](https://github.com/dbt-labs/dbt-core/issues/10918) | [Feature] Add --force-incremental flag to dbt compile | 2024-10-25 | 2025-08-28 | 4 | enhancement, triage |
| [#10920](https://github.com/dbt-labs/dbt-core/issues/10920) | Alternative strategy for `dbt_valid_to` | 2024-10-25 | 2026-02-28 | 0 | enhancement, snapshots, triage |
| [#10922](https://github.com/dbt-labs/dbt-core/issues/10922) | [Feature] Support Jinja within custom `dbt_valid_to_current` configurations | 2024-10-25 | 2024-10-28 | 2 | enhancement, user docs, snapshots, triage |
| [#10946](https://github.com/dbt-labs/dbt-core/issues/10946) | [Feature] Allow a new MergeBehavior for `config.meta` | 2024-10-30 | 2025-07-17 | 1 | enhancement, triage |
| [#10963](https://github.com/dbt-labs/dbt-core/issues/10963) | [Feature] Time Aware Freshness Checks | 2024-10-31 | 2025-09-22 | 1 | enhancement, triage, freshness |
| [#10985](https://github.com/dbt-labs/dbt-core/issues/10985) | [Feature] dbt seeds - STRING as default for the entire project | 2024-11-10 | 2026-02-05 | 2 | enhancement, triage |
| [#10993](https://github.com/dbt-labs/dbt-core/issues/10993) | [Feature] Enable the `--no-warn-error` CLI flag | 2024-11-13 | 2024-11-13 | 2 | enhancement, triage |
| [#11066](https://github.com/dbt-labs/dbt-core/issues/11066) | [Feature] Add `time_zone` configuration for source freshness checks | 2024-11-28 | 2024-12-11 | 8 | enhancement, performance, awaiting_response, freshness |
| [#11076](https://github.com/dbt-labs/dbt-core/issues/11076) | [Feature] `state:modified.relation` should detect a change of schema in `dbt_profile.yml` | 2024-11-29 | 2024-12-04 | 2 | enhancement, triage, state, state: modified |
| [#11097](https://github.com/dbt-labs/dbt-core/issues/11097) | [Feature] Support `.jinja`, `.jinja2`, `.j2` file extensions for `.md` docs files | 2024-12-05 | 2026-02-11 | 1 | enhancement, help_wanted |
| [#11102](https://github.com/dbt-labs/dbt-core/issues/11102) | [Feature] Add a behaviour change flag to disallow the legacy target_* configs on snapshots | 2024-12-06 | 2025-12-04 | 7 | enhancement |
| [#11124](https://github.com/dbt-labs/dbt-core/issues/11124) | [Adaptive Job] Model freshness spec validation | 2024-12-11 | 2024-12-11 | 0 | user docs |
| [#11141](https://github.com/dbt-labs/dbt-core/issues/11141) | [Feature] Microbatch should have a weekly batch size option | 2024-12-12 | 2026-02-26 | 0 | enhancement, Refinement, microbatch |
| [#11160](https://github.com/dbt-labs/dbt-core/issues/11160) | cold_storage configuration - I can limit how much old data we need to maintain as active or readily available | 2024-12-17 | 2024-12-19 | 1 | microbatch |

**Count: 153**

---

## Category 6: Internal / CI/CD Tech Debt

Repository maintenance items — CI/CD workflows, internal tooling, epics, and cleanup tasks.

| # | Title | Created | Last Updated | Comments | Labels |
| --- | --- | --- | --- | --- | --- |
| [#7786](https://github.com/dbt-labs/dbt-core/issues/7786) | [CT-2639] Split out long partial parsing integration tests into smaller pieces | 2023-06-05 | 2023-08-21 | 1 | repo ci/cd, tech_debt |
| [#8313](https://github.com/dbt-labs/dbt-core/issues/8313) | [CT-2932] [Spike] Consider removing Structured Logging Schema Check | 2023-08-03 | 2023-08-03 | 0 | repo ci/cd, tech_debt |
| [#8323](https://github.com/dbt-labs/dbt-core/issues/8323) | [CT-2941] [Applied State] Develop a Performance Analysis GitHub Action | 2023-08-04 | 2023-11-22 | 1 |  |
| [#8386](https://github.com/dbt-labs/dbt-core/issues/8386) | [CT-2971] Write a test for connection failures in `dbt debug` | 2023-08-14 | 2024-01-02 | 0 |  |
| [#8624](https://github.com/dbt-labs/dbt-core/issues/8624) | [CT-3109] Problem installing old version | 2023-09-12 | 2023-09-12 | 1 |  |
| [#8663](https://github.com/dbt-labs/dbt-core/issues/8663) | [CT-3128] [SPIKE] Investigate issues related to GitHub outages in #dev-core-alerts | 2023-09-18 | 2023-09-26 | 0 |  |
| [#8706](https://github.com/dbt-labs/dbt-core/issues/8706) | [CT-3151] [EPIC] Developer Platform Pilot | 2023-09-25 | 2023-10-11 | 0 | Epic |
| [#8759](https://github.com/dbt-labs/dbt-core/issues/8759) | [CT-3178] Scheduled dbt-extractor Builds | 2023-10-03 | 2023-10-06 | 0 | repo ci/cd, tech_debt |
| [#8801](https://github.com/dbt-labs/dbt-core/issues/8801) | [CT-3197] Capture additional parsing performance information | 2023-10-10 | 2023-10-10 | 0 |  |
| [#8823](https://github.com/dbt-labs/dbt-core/issues/8823) | [CT-3202] [Developer Portal] Set Up Users | 2023-10-11 | 2023-10-11 | 0 |  |
| [#8982](https://github.com/dbt-labs/dbt-core/issues/8982) | [CT-3311] Handle various edge cases with partially parsing yaml-only nodes | 2023-11-01 | 2023-11-02 | 1 |  |
| [#9254](https://github.com/dbt-labs/dbt-core/issues/9254) | [CT-3478] [Bug] Editing profiles as part of tests is not thread-safe | 2023-12-07 | 2023-12-08 | 1 | repo ci/cd, adapter_plugins, tech_debt |
| [#9481](https://github.com/dbt-labs/dbt-core/issues/9481) | Test docs generate catalog includes external nodes if available in 1.6.latest | 2024-01-29 | 2024-01-29 | 0 |  |
| [#10125](https://github.com/dbt-labs/dbt-core/issues/10125) | [Epic] Multi-project collaboration: one year in | 2024-05-10 | 2026-02-28 | 2 | Epic, multi_project |
| [#10606](https://github.com/dbt-labs/dbt-core/issues/10606) | [TIDY FIRST] Add typing to `BaseRunner` class | 2024-08-26 | 2024-08-26 | 0 |  |
| [#10621](https://github.com/dbt-labs/dbt-core/issues/10621) | Add missing type hints to `dbt/core/task/show.py` | 2024-08-27 | 2024-08-27 | 0 |  |
| [#10642](https://github.com/dbt-labs/dbt-core/issues/10642) | [SPIKE] Will writes of partioned data automatically be parallelized? | 2024-08-29 | 2024-08-29 | 0 |  |
| [#10771](https://github.com/dbt-labs/dbt-core/issues/10771) | Add a macro to get the last modified time of a relation | 2024-09-25 | 2024-09-25 | 0 |  |
| [#10895](https://github.com/dbt-labs/dbt-core/issues/10895) | Add upper bound pins for 1.8.latest | 2024-10-21 | 2024-10-21 | 0 |  |
| [#10938](https://github.com/dbt-labs/dbt-core/issues/10938) | [Tidy First] Refactor `safe_run_hooks` | 2024-10-30 | 2024-10-30 | 0 | cleanup |
| [#11018](https://github.com/dbt-labs/dbt-core/issues/11018) | Ensure internal nodes with resource_class properties only serialize keys defined in resource_class | 2024-11-20 | 2024-11-20 | 0 |  |
| [#11088](https://github.com/dbt-labs/dbt-core/issues/11088) | MicrobatchExecutionDebug structured log includes method identifier, not formatted batch id | 2024-12-02 | 2024-12-02 | 0 |  |

**Count: 22**

---

## Summary

| Category | Count | Percentage |
| --- | --- | --- |
| Stale | 120 | 31.8% |
| Bug | 62 | 16.4% |
| Adapters | 14 | 3.7% |
| Already Implemented | 6 | 1.6% |
| Active Feature Requests | 153 | 40.6% |
| Internal / CI/CD Tech Debt | 22 | 5.8% |
| **Total** | **377** | **100%** |

### Key Takeaways

1. **Stale issues are the largest bucket** — 32% of open issues from this period are dormant feature requests, spikes, tech debt, or items explicitly labeled `stale`/`wontfix`.
2. **Bugs are significant** — 62 open bugs from 2023-2024, many related to partial parsing, snapshots, unit tests, and incremental models.
3. **6 issues have been implemented** — Snapshot meta columns, incremental unit testing, time spine automation, Protobuf migration, unit test ref/source in SQL, and legacy time spine behavior change flag.
4. **Active feature requests dominate by count** — 153 issues with meaningful engagement remain open, reflecting dbt-core's active community. High-impact requests include versioned model views (#7442, 34 comments), schema inference for contracts (#7432, 22 comments), and column-level nested tests (#7613, 15 comments).
5. **Unit testing is a hot area** — Many open issues relate to unit test improvements (macros, incremental, precision, fixtures, CTEs).
6. **Adapter-specific issues** — 14 issues require adapter-level changes, spanning Postgres, Redshift, and the adapter plugin interface.
