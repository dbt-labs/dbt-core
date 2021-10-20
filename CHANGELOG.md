## dbt-core 1.0.0 (Release TBD)

### Breaking changes

- Enable `on-run-start` and `on-run-end` hooks for `dbt test`. Add `flags.WHICH` to execution context, representing current task ([#3463](https://github.com/dbt-labs/dbt-core/issues/3463), [#4004](https://github.com/dbt-labs/dbt-core/pull/4004))

### Features
- Normalize global CLI arguments/flags ([#2990](https://github.com/dbt-labs/dbt/issues/2990), [#3839](https://github.com/dbt-labs/dbt/pull/3839))
- Turns on the static parser by default and adds the flag `--no-static-parser` to disable it. ([#3377](https://github.com/dbt-labs/dbt/issues/3377), [#3939](https://github.com/dbt-labs/dbt/pull/3939))
- Generic test FQNs have changed to include the relative path, resource, and column (if applicable) where they are defined. This makes it easier to configure them from the `tests` block in `dbt_project.yml` ([#3259](https://github.com/dbt-labs/dbt/pull/3259), [#3880](https://github.com/dbt-labs/dbt/pull/3880)
- Turn on partial parsing by default ([#3867](https://github.com/dbt-labs/dbt/issues/3867), [#3989](https://github.com/dbt-labs/dbt/issues/3989))
- Add `result:<status>` selectors to automatically rerun failed tests and erroneous models. This makes it easier to rerun failed dbt jobs with a simple selector flag instead of restarting from the beginning or manually running the dbt models in scope. ([#3859](https://github.com/dbt-labs/dbt/issues/3891), [#4017](https://github.com/dbt-labs/dbt/pull/4017))
- `dbt init` is now interactive, generating profiles.yml when run inside existing project ([#3625](https://github.com/dbt-labs/dbt/pull/3625))

### Under the hood
- Fix intermittent errors in partial parsing tests ([#4060](https://github.com/dbt-labs/dbt-core/issues/4060), [#4068](https://github.com/dbt-labs/dbt-core/pull/4068))
- Make finding disabled nodes more consistent ([#4069](https://github.com/dbt-labs/dbt-core/issues/4069), [#4073](https://github.com/dbt-labas/dbt-core/pull/4073))
- Remove connection from `render_with_context` during parsing, thereby removing misleading log message ([#3137](https://github.com/dbt-labs/dbt-core/issues/3137), [#4062](https://github.com/dbt-labas/dbt-core/pull/4062))

Contributors:
- [@sungchun12](https://github.com/sungchun12) ([#4017](https://github.com/dbt-labs/dbt/pull/4017))
- [@matt-winkler](https://github.com/matt-winkler) ([#4017](https://github.com/dbt-labs/dbt/pull/4017))
- [@NiallRees](https://github.com/NiallRees) ([#3625](https://github.com/dbt-labs/dbt/pull/3625))

## dbt-core 1.0.0b1 (October 11, 2021)

### Breaking changes

- The two type of test definitions are now "singular" and "generic" (instead of "data" and "schema", respectively). The `test_type:` selection method accepts `test_type:singular` and `test_type:generic`. (It will also accept `test_type:schema` and `test_type:data` for backwards compatibility) ([#3234](https://github.com/dbt-labs/dbt-core/issues/3234), [#3880](https://github.com/dbt-labs/dbt-core/pull/3880)). **Not backwards compatible:** The `--data` and `--schema` flags to `dbt test` are no longer supported, and tests no longer have the tags `'data'` and `'schema'` automatically applied.
- Deprecated the use of the `packages` arg `adapter.dispatch` in favor of the `macro_namespace` arg. ([#3895](https://github.com/dbt-labs/dbt-core/issues/3895))

### Features
- Normalize global CLI arguments/flags ([#2990](https://github.com/dbt-labs/dbt-core/issues/2990), [#3839](https://github.com/dbt-labs/dbt-core/pull/3839))
- Turns on the static parser by default and adds the flag `--no-static-parser` to disable it. ([#3377](https://github.com/dbt-labs/dbt-core/issues/3377), [#3939](https://github.com/dbt-labs/dbt-core/pull/3939))
- Generic test FQNs have changed to include the relative path, resource, and column (if applicable) where they are defined. This makes it easier to configure them from the `tests` block in `dbt_project.yml` ([#3259](https://github.com/dbt-labs/dbt-core/pull/3259), [#3880](https://github.com/dbt-labs/dbt-core/pull/3880)
- Turn on partial parsing by default ([#3867](https://github.com/dbt-labs/dbt-core/issues/3867), [#3989](https://github.com/dbt-labs/dbt-core/issues/3989))

### Fixes
- Add generic tests defined on sources to the manifest once, not twice ([#3347](https://github.com/dbt-labs/dbt/issues/3347), [#3880](https://github.com/dbt-labs/dbt/pull/3880))
- Skip partial parsing if certain macros have changed ([#3810](https://github.com/dbt-labs/dbt/issues/3810), [#3982](https://github.com/dbt-labs/dbt/pull/3892))
- Enable cataloging of unlogged Postgres tables ([3961](https://github.com/dbt-labs/dbt/issues/3961), [#3993](https://github.com/dbt-labs/dbt/pull/3993))
- Fix multiple disabled nodes ([#4013](https://github.com/dbt-labs/dbt/issues/4013), [#4018](https://github.com/dbt-labs/dbt/pull/4018))
- Fix multiple partial parsing errors ([#3996](https://github.com/dbt-labs/dbt/issues/3006), [#4020](https://github.com/dbt-labs/dbt/pull/4018))
- Return an error instead of a warning when runing with `--warn-error` and no models are selected ([#4006](https://github.com/dbt-labs/dbt/issues/4006), [#4019](https://github.com/dbt-labs/dbt/pull/4019))
- Fixed bug with `error_if` test option ([#4070](https://github.com/dbt-labs/dbt-core/pull/4070))

### Under the hood

- Enact deprecation for `materialization-return` and replace deprecation warning with an exception. ([#3896](https://github.com/dbt-labs/dbt-core/issues/3896))
- Build catalog for only relational, non-ephemeral nodes in the graph ([#3920](https://github.com/dbt-labs/dbt-core/issues/3920))
- Enact deprecation to remove the `release` arg from the `execute_macro` method. ([#3900](https://github.com/dbt-labs/dbt-core/issues/3900))
- Enact deprecation for default quoting to be True.  Override for the `dbt-snowflake` adapter so it stays `False`. ([#3898](https://github.com/dbt-labs/dbt-core/issues/3898))
- Enact deprecation for object used as dictionaries when they should be dataclasses. Replace deprecation warning with an exception for the dunder methods of `__iter__` and `__len__` for all superclasses of FakeAPIObject. ([#3897](https://github.com/dbt-labs/dbt-core/issues/3897))
- Enact deprecation for `adapter-macro` and replace deprecation warning with an exception. ([#3901](https://github.com/dbt-labs/dbt-core/issues/3901))
- Add warning when trying to put a node under the wrong key.  ie. A seed under models in a `schema.yml` file. ([#3899](https://github.com/dbt-labs/dbt-core/issues/3899))
- Plugins for `redshift`, `snowflake`, and `bigquery` have moved to separate repos: [`dbt-redshift`](https://github.com/dbt-labs/dbt-redshift), [`dbt-snowflake`](https://github.com/dbt-labs/dbt-snowflake), [`dbt-bigquery`](https://github.com/dbt-labs/dbt-bigquery)
- Change the default dbt packages installation directory to `dbt_packages` from `dbt_modules`.  Also rename `module-path` to `packages-install-path` to allow default overrides of package install directory.  Deprecation warning added for projects using the old `dbt_modules` name without specifying a `packages-install-path`.  ([#3523](https://github.com/dbt-labs/dbt-core/issues/3523))
- Update the default project paths to be `analysis-paths = ['analyses']` and `test-paths = ['tests]`. Also have starter project set `analysis-paths: ['analyses']` from now on.  ([#2659](https://github.com/dbt-labs/dbt-core/issues/2659))
- Define the data type of `sources` as an array of arrays of string in the manifest artifacts. ([#3966](https://github.com/dbt-labs/dbt-core/issues/3966), [#3967](https://github.com/dbt-labs/dbt-core/pull/3967))
- Marked `source-paths` and `data-paths` as deprecated keys in `dbt_project.yml` in favor of `model-paths` and `seed-paths` respectively.([#1607](https://github.com/dbt-labs/dbt-core/issues/1607))

Contributors:

- [@dave-connors-3](https://github.com/dave-connors-3) ([#3920](https://github.com/dbt-labs/dbt-core/pull/3922))
- [@kadero](https://github.com/kadero) ([#3952](https://github.com/dbt-labs/dbt-core/pull/3953))
- [@samlader](https://github.com/samlader) ([#3993](https://github.com/dbt-labs/dbt-core/pull/3993))
- [@yu-iskw](https://github.com/yu-iskw) ([#3967](https://github.com/dbt-labs/dbt-core/pull/3967))
- [@laxjesse](https://github.com/laxjesse) ([#4019](https://github.com/dbt-labs/dbt-core/pull/4019))

## dbt 0.21.1 (Release TBD)

### Fixes
- Performance: Use child_map to find tests for nodes in resolve_graph ([#4012](https://github.com/dbt-labs/dbt/issues/4012), [#4022](https://github.com/dbt-labs/dbt/pull/4022))
- Switch `unique_field` from abstractproperty to optional property. Add docstring ([#4025](https://github.com/dbt-labs/dbt/issues/4025), [#4028](https://github.com/dbt-labs/dbt/pull/4028))
- Include only relational nodes in `database_schema_set` ([#4063](https://github.com/dbt-labs/dbt-core/issues/4063), [#4077](https://github.com/dbt-labs/dbt-core/pull/4077))

Contributors:
- [@ljhopkins2](https://github.com/ljhopkins2) ([#4077](https://github.com/dbt-labs/dbt-core/pull/4077))

## dbt 0.21.0 (October 04, 2021)

## dbt 0.21.0rc2 (September 27, 2021)

### Fixes
- Fix batching for large seeds on Snowflake ([#3941](https://github.com/dbt-labs/dbt-core/issues/3941), [#3942](https://github.com/dbt-labs/dbt-core/pull/3942))
- Avoid infinite recursion in `state:modified.macros` check ([#3904](https://github.com/dbt-labs/dbt-core/issues/3904), [#3957](https://github.com/dbt-labs/dbt-core/pull/3957))
- Cast log messages to strings before scrubbing of prefixed env vars ([#3971](https://github.com/dbt-labs/dbt-core/issues/3971), [#3972](https://github.com/dbt-labs/dbt-core/pull/3972))

### Under the hood
- Bump artifact schema versions for 0.21.0 ([#3945](https://github.com/dbt-labs/dbt-core/pull/3945))

## dbt 0.21.0rc1 (September 20, 2021)

### Features

- Experimental parser now detects macro overrides of ref, source, and config builtins. ([#3581](https://github.com/dbt-labs/dbt-core/issues/3866), [#3582](https://github.com/dbt-labs/dbt-core/pull/3877))
- Add connect_timeout profile configuration for Postgres and Redshift adapters. ([#3581](https://github.com/dbt-labs/dbt-core/issues/3581), [#3582](https://github.com/dbt-labs/dbt-core/pull/3582))
- Enhance BigQuery copy materialization ([#3570](https://github.com/dbt-labs/dbt-core/issues/3570), [#3606](https://github.com/dbt-labs/dbt-core/pull/3606)):
  - to simplify config (default usage of `copy_materialization='table'` if is is not found in global or local config)
  - to let copy several source tables into single target table at a time. ([Google doc reference](https://cloud.google.com/bigquery/docs/managing-tables#copying_multiple_source_tables))
- Customize ls task JSON output by adding new flag `--output-keys` ([#3778](https://github.com/dbt-labs/dbt-core/issues/3778), [#3395](https://github.com/dbt-labs/dbt-core/issues/3395))
- Add support for execution project on BigQuery through profile configuration ([#3707](https://github.com/dbt-labs/dbt-core/issues/3707), [#3708](https://github.com/dbt-labs/dbt-core/issues/3708))
- Skip downstream nodes during the `build` task when a test fails. ([#3597](https://github.com/dbt-labs/dbt-core/issues/3597), [#3792](https://github.com/dbt-labs/dbt-core/pull/3792))
- Added default field in the `selectors.yml` to allow user to define default selector ([#3448](https://github.com/dbt-labs/dbt-core/issues/3448), [#3875](https://github.com/dbt-labs/dbt-core/issues/3875), [#3892](https://github.com/dbt-labs/dbt-core/issues/3892))
- Added timing and thread information to sources.json artifact ([#3804](https://github.com/dbt-labs/dbt-core/issues/3804), [#3894](https://github.com/dbt-labs/dbt-core/pull/3894))
- Update cli and rpc flags for the `build` task to align with other commands (`--resource-type`, `--store-failures`) ([#3596](https://github.com/dbt-labs/dbt-core/issues/3596), [#3884](https://github.com/dbt-labs/dbt-core/pull/3884))
- Log tests that are not indirectly selected. Add `--greedy` flag to `test`, `list`, `build` and `greedy` property in yaml selectors ([#3723](https://github.com/dbt-labs/dbt-core/pull/3723), [#3833](https://github.com/dbt-labs/dbt-core/pull/3833))

### Fixes

- Support BigQuery-specific aliases `target_dataset` and `target_project` in snapshot configs ([#3694](https://github.com/dbt-labs/dbt-core/issues/3694), [#3834](https://github.com/dbt-labs/dbt-core/pull/3834))
- `dbt debug` shows a summary of whether all checks passed or not ([#3831](https://github.com/dbt-labs/dbt-core/issues/3831), [#3832](https://github.com/dbt-labs/dbt-core/issues/3831))
- Fix issue when running the `deps` task after the `list` task in the RPC server ([#3846](https://github.com/dbt-labs/dbt-core/issues/3846), [#3848](https://github.com/dbt-labs/dbt-core/pull/3848), [#3850](https://github.com/dbt-labs/dbt-core/pull/3850))
- Fix bug with initializing a dataclass that inherits from `typing.Protocol`, specifically for `dbt.config.profile.Profile` ([#3843](https://github.com/dbt-labs/dbt-core/issues/3843), [#3855](https://github.com/dbt-labs/dbt-core/pull/3855))
- Introduce a macro, `get_where_subquery`, for tests that use `where` config. Alias filtering subquery as `dbt_subquery` instead of resource identifier ([#3857](https://github.com/dbt-labs/dbt-core/issues/3857), [#3859](https://github.com/dbt-labs/dbt-core/issues/3859))
- Use group by column_name in accepted_values test for compatibility with most database engines ([#3905](https://github.com/dbt-labs/dbt-core/issues/3905), [#3906](https://github.com/dbt-labs/dbt-core/pull/3906))
- Separated table vs view configuration for BigQuery since some configuration is not possible to set for tables vs views. ([#3682](https://github.com/dbt-labs/dbt-core/issues/3682), [#3691](https://github.com/dbt-labs/dbt-core/issues/3682))

### Under the hood

- Use GitHub Actions for CI ([#3688](https://github.com/dbt-labs/dbt-core/issues/3688), [#3669](https://github.com/dbt-labs/dbt-core/pull/3669))
- Better dbt hub registry packages version logging that prompts the user for upgrades to relevant packages ([#3560](https://github.com/dbt-labs/dbt-core/issues/3560), [#3763](https://github.com/dbt-labs/dbt-core/issues/3763), [#3759](https://github.com/dbt-labs/dbt-core/pull/3759))
- Allow the default seed macro's SQL parameter, `%s`, to be replaced by dispatching a new macro, `get_binding_char()`. This enables adapters with parameter marker characters such as `?` to not have to override `basic_load_csv_rows`. ([#3622](https://github.com/dbt-labs/dbt-core/issues/3622), [#3623](https://github.com/dbt-labs/dbt-core/pull/3623))
- Alert users on package rename ([hub.getdbt.com#180](https://github.com/dbt-labs/hub.getdbt.com/issues/810), [#3825](https://github.com/dbt-labs/dbt-core/pull/3825))
- Add `adapter_unique_id` to invocation context in anonymous usage tracking, to better understand dbt adoption ([#3713](https://github.com/dbt-labs/dbt-core/issues/3713), [#3796](https://github.com/dbt-labs/dbt-core/issues/3796))
- Specify `macro_namespace = 'dbt'` for all dispatched macros in the global project, making it possible to dispatch to macro implementations defined in packages. Dispatch `generate_schema_name` and `generate_alias_name` ([#3456](https://github.com/dbt-labs/dbt-core/issues/3456), [#3851](https://github.com/dbt-labs/dbt-core/issues/3851))
- Retry transient GitHub failures during download ([#3729](https://github.com/dbt-labs/dbt-core/pull/3729))

Contributors:

- [@xemuliam](https://github.com/xemuliam) ([#3606](https://github.com/dbt-labs/dbt-core/pull/3606))
- [@sungchun12](https://github.com/sungchun12) ([#3759](https://github.com/dbt-labs/dbt-core/pull/3759))
- [@dbrtly](https://github.com/dbrtly) ([#3834](https://github.com/dbt-labs/dbt-core/pull/3834))
- [@swanderz](https://github.com/swanderz) [#3623](https://github.com/dbt-labs/dbt-core/pull/3623)
- [@JasonGluck](https://github.com/JasonGluck) ([#3582](https://github.com/dbt-labs/dbt-core/pull/3582))
- [@joellabes](https://github.com/joellabes) ([#3669](https://github.com/dbt-labs/dbt-core/pull/3669), [#3833](https://github.com/dbt-labs/dbt-core/pull/3833))
- [@juma-adoreme](https://github.com/juma-adoreme) ([#3838](https://github.com/dbt-labs/dbt-core/pull/3838))
- [@annafil](https://github.com/annafil) ([#3825](https://github.com/dbt-labs/dbt-core/pull/3825))
- [@AndreasTA-AW](https://github.com/AndreasTA-AW) ([#3691](https://github.com/dbt-labs/dbt-core/pull/3691))
- [@Kayrnt](https://github.com/Kayrnt) ([3707](https://github.com/dbt-labs/dbt-core/pull/3707))
- [@TeddyCr](https://github.com/TeddyCr) ([#3448](https://github.com/dbt-labs/dbt-core/pull/3865))
- [@sdebruyn](https://github.com/sdebruyn) ([#3906](https://github.com/dbt-labs/dbt-core/pull/3906))

## dbt 0.21.0b2 (August 19, 2021)

### Features

- Capture changes to macros in `state:modified`. Introduce new `state:` sub-selectors: `modified.body`, `modified.configs`, `modified.persisted_descriptions`, `modified.relation`, `modified.macros` ([#2704](https://github.com/dbt-labs/dbt-core/issues/2704), [#3278](https://github.com/dbt-labs/dbt-core/issues/3278), [#3559](https://github.com/dbt-labs/dbt-core/issues/3559))
- Enable setting configs in schema files for models, seeds, snapshots, analyses, tests ([#2401](https://github.com/dbt-labs/dbt-core/issues/2401), [#3616](https://github.com/dbt-labs/dbt-core/pull/3616))

### Fixes

- Fix for RPC requests that raise a RecursionError when serializing Undefined values as JSON ([#3464](https://github.com/dbt-labs/dbt-core/issues/3464), [#3687](https://github.com/dbt-labs/dbt-core/pull/3687))
- Avoid caching schemas for tests when `store_failures` is not enabled ([#3715](https://github.com/dbt-labs/dbt-core/issues/3715), [#3716](https://github.com/dbt-labs/dbt-core/pull/3716))

### Under the hood

- Add `build` RPC method, and a subset of flags for `build` task ([#3595](https://github.com/dbt-labs/dbt-core/issues/3595), [#3674](https://github.com/dbt-labs/dbt-core/pull/3674))
- Get more information on partial parsing version mismatches ([#3757](https://github.com/dbt-labs/dbt-core/issues/3757), [#3758](https://github.com/dbt-labs/dbt-core/pull/3758))

## dbt 0.21.0b1 (August 03, 2021)

### Breaking changes

- Add full node selection to source freshness command and align selection syntax with other tasks (`dbt source freshness --select source_name` --> `dbt source freshness --select source:souce_name`) and rename `dbt source snapshot-freshness` -> `dbt source freshness`. ([#2987](https://github.com/dbt-labs/dbt-core/issues/2987), [#3554](https://github.com/dbt-labs/dbt-core/pull/3554))
- **dbt-snowflake:** Turn off transactions and turn on `autocommit` by default. Explicitly specify `begin` and `commit` for DML statements in incremental and snapshot materializations. Note that this may affect user-space code that depends on transactions.

### Features

- Add `dbt build` command to run models, tests, seeds, and snapshots in DAG order. ([#2743](https://github.com/dbt-labs/dbt-core/issues/2743), [#3490](https://github.com/dbt-labs/dbt-core/issues/3490), [#3608](https://github.com/dbt-labs/dbt-core/issues/3608))
- Introduce `on_schema_change` config to detect and handle schema changes on incremental models ([#1132](https://github.com/dbt-labs/dbt-core/issues/1132), [#3387](https://github.com/dbt-labs/dbt-core/issues/3387))

### Fixes

- Fix docs generation for cross-db sources in REDSHIFT RA3 node ([#3236](https://github.com/dbt-labs/dbt-core/issues/3236), [#3408](https://github.com/dbt-labs/dbt-core/pull/3408))
- Fix type coercion issues when fetching query result sets ([#2984](https://github.com/dbt-labs/dbt-core/issues/2984), [#3499](https://github.com/dbt-labs/dbt-core/pull/3499))
- Handle whitespace after a plus sign on the project config ([#3526](https://github.com/dbt-labs/dbt-core/pull/3526))
- Fix table and view materialization issue when switching from one to the other ([#2161](https://github.com/dbt-labs/dbt-core/issues/2161)), [#3547](https://github.com/dbt-labs/dbt-core/pull/3547))

### Under the hood

- Add performance regression testing [#3602](https://github.com/dbt-labs/dbt-core/pull/3602)
- Improve default view and table materialization performance by checking relational cache before attempting to drop temp relations ([#3112](https://github.com/dbt-labs/dbt-core/issues/3112), [#3468](https://github.com/dbt-labs/dbt-core/pull/3468))
- Add optional `sslcert`, `sslkey`, and `sslrootcert` profile arguments to the Postgres connector. ([#3472](https://github.com/dbt-labs/dbt-core/pull/3472), [#3473](https://github.com/dbt-labs/dbt-core/pull/3473))
- Move the example project used by `dbt init` into `dbt` repository, to avoid cloning an external repo ([#3005](https://github.com/dbt-labs/dbt-core/pull/3005), [#3474](https://github.com/dbt-labs/dbt-core/pull/3474), [#3536](https://github.com/dbt-labs/dbt-core/pull/3536))
- Better interaction between `dbt init` and adapters. Avoid raising errors while initializing a project ([#2814](https://github.com/dbt-labs/dbt-core/pull/2814), [#3483](https://github.com/dbt-labs/dbt-core/pull/3483))
- Update `create_adapter_plugins` script to include latest accessories, and stay up to date with latest dbt-core version ([#3002](https://github.com/dbt-labs/dbt-core/issues/3002), [#3509](https://github.com/dbt-labs/dbt-core/pull/3509))
- Scrub environment secrets from logs and console output ([#3617](https://github.com/dbt-labs/dbt-core/pull/3617))

### Dependencies

- Require `werkzeug>=1` ([#3590](https://github.com/dbt-labs/dbt-core/pull/3590))

Contributors:

- [@kostek-pl](https://github.com/kostek-pl) ([#3236](https://github.com/dbt-labs/dbt-core/pull/3408))
- [@matt-winkler](https://github.com/matt-winkler) ([#3387](https://github.com/dbt-labs/dbt-core/pull/3387))
- [@tconbeer](https://github.com/tconbeer) [#3468](https://github.com/dbt-labs/dbt-core/pull/3468))
- [@JLDLaughlin](https://github.com/JLDLaughlin) ([#3473](https://github.com/dbt-labs/dbt-core/pull/3473))
- [@jmriego](https://github.com/jmriego) ([#3526](https://github.com/dbt-labs/dbt-core/pull/3526))
- [@danielefrigo](https://github.com/danielefrigo) ([#3547](https://github.com/dbt-labs/dbt-core/pull/3547))

## dbt 0.20.2 (Release TBD)

### Under the hood

- Better error handling for BigQuery job labels that are too long. ([#3612](https://github.com/dbt-labs/dbt-core/pull/3612), [#3703](https://github.com/dbt-labs/dbt-core/pull/3703))
- Get more information on partial parsing version mismatches ([#3757](https://github.com/dbt-labs/dbt-core/issues/3757), [#3758](https://github.com/dbt-labs/dbt-core/pull/3758))
- Switch to full reparse on partial parsing exceptions. Log and report exception information. ([#3725](https://github.com/dbt-labs/dbt-core/issues/3725), [#3733](https://github.com/dbt-labs/dbt-core/pull/3733))

### Fixes

- Fix bug in finding analysis nodes when applying analysis patch ([#3764](https://github.com/dbt-labs/dbt-core/issues/3764), [#3767](https://github.com/dbt-labs/dbt-core/pull/3767))
- Rewrite built-in generic tests to support `column_name` expressions ([#3790](https://github.com/dbt-labs/dbt-core/issues/3790), [#3811](https://github.com/dbt-labs/dbt-core/pull/3811))

Contributors:

- [@sungchun12](https://github.com/sungchun12) ([#3703](https://github.com/dbt-labs/dbt-core/pull/3703))

## dbt 0.20.2rc1 (August 16, 2021)

### Under the hood

- Switch to full reparse on partial parsing exceptions. Log and report exception information. ([#3725](https://github.com/dbt-labs/dbt-core/issues/3725), [#3733](https://github.com/dbt-labs/dbt-core/pull/3733))
- Check for existence of test node when removing. ([#3711](https://github.com/dbt-labs/dbt-core/issues/3711), [#3750](https://github.com/dbt-labs/dbt-core/pull/3750))

## dbt 0.20.1 (August 11, 2021)

## dbt 0.20.1rc1 (August 02, 2021)

### Features

- Adds `install-prerelease` parameter to hub packages in `packages.yml`. When set to `True`, allows prerelease packages to be installed. By default, this parameter is False unless explicitly set to True.

### Fixes

- Fix config merge behavior with experimental parser [3637](https://github.com/dbt-labs/dbt-core/pull/3637)
- Fix exception on yml files with all comments [3568](https://github.com/dbt-labs/dbt-core/issues/3568)
- Fix `store_failures` config when defined as a modifier for `unique` and `not_null` tests ([#3575](https://github.com/dbt-labs/dbt-core/issues/3575), [#3577](https://github.com/dbt-labs/dbt-core/pull/3577))
- Fix `where` config with `relationships` test by refactoring test SQL. Note: The default `relationships` test now includes CTEs, and may need reimplementing on adapters that don't support CTEs nested inside subqueries. ([#3579](https://github.com/dbt-labs/dbt-core/issues/3579), [#3583](https://github.com/dbt-labs/dbt-core/pull/3583))
- Partial parsing: don't reprocess SQL file already scheduled ([#3589](https://github.com/dbt-labs/dbt-core/issues/3589), [#3620](https://github.com/dbt-labs/dbt-core/pull/3620))
- Handle interator functions in model config ([#3573](https://github.com/dbt-labs/dbt-core/issues/3573))
- Partial parsing: fix error after changing empty yaml file ([#3567](https://gith7ub.com/dbt-labs/dbt-core/issues/3567), [#3618](https://github.com/dbt-labs/dbt-core/pull/3618))
- Partial parsing: handle source tests when changing test macro ([#3584](https://github.com/dbt-labs/dbt-core/issues/3584), [#3620](https://github.com/dbt-labs/dbt-core/pull/3620))
- Fix `dbt deps` version comparison logic which was causing incorrect pre-release package versions to be installed. ([#3578](https://github.com/dbt-labs/dbt-core/issues/3578), [#3609](https://github.com/dbt-labs/dbt-core/issues/3609))
- Partial parsing: schedule new macro file for parsing when macro patching ([#3627](https://github.com/dbt-labs/dbt-core/issues/3627), [#3627](https://github.com/dbt-labs/dbt-core/pull/3627))
- Use `SchemaParser`'s render context to render test configs in order to support `var()` configured at the project level and passed in from the cli ([#3564](https://github.com/dbt-labs/dbt-core/issues/3564). [#3646](https://github.com/dbt-labs/dbt-core/pull/3646))
- Partial parsing: check unique_ids when recursively removing macros ([#3636](https://github.com/dbt-labs/dbt-core/issues/3636))

### Docs

- Fix docs site crash if `relationships` test has one dependency instead of two ([docs#207](https://github.com/dbt-labs/dbt-docs/issues/207), ([docs#208](https://github.com/dbt-labs/dbt-docs/issues/208)))

### Under the hood

- Handle exceptions from anonymous usage tracking for users of `dbt-snowflake` on Apple M1 chips ([#3162](https://github.com/dbt-labs/dbt-core/issues/3162), [#3661](https://github.com/dbt-labs/dbt-core/issues/3661))
- Add tracking for determine why `dbt` needs to re-parse entire project when partial parsing is enabled ([#3572](https://github.com/dbt-labs/dbt-core/issues/3572), [#3652](https://github.com/dbt-labs/dbt-core/pull/3652))

Contributors:

- [@NiallRees](https://github.com/NiallRees) ([#3624](https://github.com/dbt-labs/dbt-core/pull/3624))

## dbt 0.20.0 (July 12, 2021)

### Fixes

- Avoid slowdown in column-level `persist_docs` on Snowflake, while preserving the error-avoidance from [#3149](https://github.com/dbt-labs/dbt-core/issues/3149) ([#3541](https://github.com/dbt-labs/dbt-core/issues/3541), [#3543](https://github.com/dbt-labs/dbt-core/pull/3543))
- Partial parsing: handle already deleted nodes when schema block also deleted ([#3516](http://github.com/fishown-analystics/dbt/issues/3516), [#3522](http://github.com/fishown-analystics/dbt/issues/3522))

### Docs

- Update dbt logo and links ([docs#197](https://github.com/dbt-labs/dbt-docs/issues/197))

### Under the hood

- Add tracking for experimental parser accuracy ([3503](https://github.com/dbt-labs/dbt-core/pull/3503), [3553](https://github.com/dbt-labs/dbt-core/pull/3553))

## dbt 0.20.0rc2 (June 30, 2021)

### Fixes

- Handle quoted values within test configs, such as `where` ([#3458](https://github.com/dbt-labs/dbt-core/issues/3458), [#3459](https://github.com/dbt-labs/dbt-core/pull/3459))

### Docs

- Display `tags` on exposures ([docs#194](https://github.com/dbt-labs/dbt-docs/issues/194), [docs#195](https://github.com/dbt-labs/dbt-docs/issues/195))

### Under the hood

- Swap experimental parser implementation to use Rust [#3497](https://github.com/dbt-labs/dbt-core/pull/3497)
- Dispatch the core SQL statement of the new test materialization, to benefit adapter maintainers ([#3465](https://github.com/dbt-labs/dbt-core/pull/3465), [#3461](https://github.com/dbt-labs/dbt-core/pull/3461))
- Minimal validation of yaml dictionaries prior to partial parsing ([#3246](https://github.com/dbt-labs/dbt-core/issues/3246), [#3460](https://github.com/dbt-labs/dbt-core/pull/3460))
- Add partial parsing tests and improve partial parsing handling of macros ([#3449](https://github.com/dbt-labs/dbt-core/issues/3449), [#3505](https://github.com/dbt-labs/dbt-core/pull/3505))
- Update project loading event data to include experimental parser information. ([#3438](https://github.com/dbt-labs/dbt-core/issues/3438), [#3495](https://github.com/dbt-labs/dbt-core/pull/3495))

Contributors:

- [@swanderz](https://github.com/swanderz) ([#3461](https://github.com/dbt-labs/dbt-core/pull/3461))
- [@stkbailey](https://github.com/stkbailey) ([docs#195](https://github.com/dbt-labs/dbt-docs/issues/195))

## dbt 0.20.0rc1 (June 04, 2021)

### Breaking changes

- Fix adapter.dispatch macro resolution when statically extracting macros. Introduce new project-level `dispatch` config. The `packages` argument to `dispatch` no longer supports macro calls; there is backwards compatibility for existing packages. The argument will no longer be supported in a future release, instead provide the `macro_namespace` argument. ([#3362](https://github.com/dbt-labs/dbt-core/issues/3362), [#3363](https://github.com/dbt-labs/dbt-core/pull/3363), [#3383](https://github.com/dbt-labs/dbt-core/pull/3383), [#3403](https://github.com/dbt-labs/dbt-core/pull/3403))

### Features

- Support optional `updated_at` config parameter with `check` strategy snapshots. If not supplied, will use current timestamp (default). ([#1844](https://github.com/dbt-labs/dbt-core/issues/1844), [#3376](https://github.com/dbt-labs/dbt-core/pull/3376))
- Add the opt-in `--use-experimental-parser` flag ([#3307](https://github.com/dbt-labs/dbt-core/issues/3307), [#3374](https://github.com/dbt-labs/dbt-core/issues/3374))
- Store test failures in the database ([#517](https://github.com/dbt-labs/dbt-core/issues/517), [#903](https://github.com/dbt-labs/dbt-core/issues/903), [#2593](https://github.com/dbt-labs/dbt-core/issues/2593), [#3316](https://github.com/dbt-labs/dbt-core/issues/3316))
- Add new test configs: `where`, `limit`, `warn_if`, `error_if`, `fail_calc` ([#3258](https://github.com/dbt-labs/dbt-core/issues/3258), [#3321](https://github.com/dbt-labs/dbt-core/issues/3321), [#3336](https://github.com/dbt-labs/dbt-core/pull/3336))
- Move partial parsing to end of parsing and implement new partial parsing method. ([#3217](https://github.com/dbt-labs/dbt-core/issues/3217), [#3364](https://github.com/dbt-labs/dbt-core/pull/3364))
- Save doc file node references and use in partial parsing. ([#3425](https://github.com/dbt-labs/dbt-core/issues/3425), [#3432](https://github.com/dbt-labs/dbt-core/pull/3432))

### Fixes

- Fix compiled sql for ephemeral models ([#3317](https://github.com/dbt-labs/dbt-core/issues/3317), [#3318](https://github.com/dbt-labs/dbt-core/pull/3318))
- Now generating `run_results.json` even when no nodes are selected ([#3313](https://github.com/dbt-labs/dbt-core/issues/3313), [#3315](https://github.com/dbt-labs/dbt-core/pull/3315))
- Add missing `packaging` dependency ([#3312](https://github.com/dbt-labs/dbt-core/issues/3312), [#3339](https://github.com/dbt-labs/dbt-core/pull/3339))
- Fix references to macros with package names when rendering schema tests ([#3324](https://github.com/dbt-labs/dbt-core/issues/3324), [#3345](https://github.com/dbt-labs/dbt-core/pull/3345))
- Stop clobbering default keyword arguments for jinja test definitions ([#3329](https://github.com/dbt-labs/dbt-core/issues/3329), [#3340](https://github.com/dbt-labs/dbt-core/pull/3340))
- Fix unique_id generation for generic tests so tests with the same FQN but different configuration will run. ([#3254](https://github.com/dbt-labs/dbt-core/issues/3254), [#3335](https://github.com/dbt-labs/dbt-core/issues/3335))
- Update the snowflake adapter to only comment on a column if it exists when using the persist_docs config ([#3039](https://github.com/dbt-labs/dbt-core/issues/3039), [#3149](https://github.com/dbt-labs/dbt-core/pull/3149))
- Add a better error messages for undefined macros and when there are less packages installed than specified in `packages.yml`. ([#2999](https://github.com/dbt-labs/dbt-core/issues/2999))
- Separate `compiled_path` from `build_path`, and print the former alongside node error messages ([#1985](https://github.com/dbt-labs/dbt-core/issues/1985), [#3327](https://github.com/dbt-labs/dbt-core/pull/3327))
- Fix exception caused when running `dbt debug` with BigQuery connections ([#3314](https://github.com/dbt-labs/dbt-core/issues/3314), [#3351](https://github.com/dbt-labs/dbt-core/pull/3351))
- Raise better error if snapshot is missing required configurations ([#3381](https://github.com/dbt-labs/dbt-core/issues/3381), [#3385](https://github.com/dbt-labs/dbt-core/pull/3385))
- Fix `dbt run` errors caused from receiving non-JSON responses from Snowflake with Oauth ([#3350](https://github.com/dbt-labs/dbt-core/issues/3350))
- Fix deserialization of Manifest lock attribute ([#3435](https://github.com/dbt-labs/dbt-core/issues/3435), [#3445](https://github.com/dbt-labs/dbt-core/pull/3445))
- Fix `dbt run` errors caused from receiving non-JSON responses from Snowflake with Oauth ([#3350](https://github.com/dbt-labs/dbt-core/issues/3350)
- Fix infinite recursion when parsing schema tests due to loops in macro calls ([#3444](https://github.com/dbt-labs/dbt-core/issues/3344), [#3454](https://github.com/dbt-labs/dbt-core/pull/3454))

### Docs

- Reversed the rendering direction of relationship tests so that the test renders in the model it is defined in ([docs#181](https://github.com/dbt-labs/dbt-docs/issues/181), [docs#183](https://github.com/dbt-labs/dbt-docs/pull/183))
- Support dots in model names: display them in the graphs ([docs#184](https://github.com/dbt-labs/dbt-docs/issues/184), [docs#185](https://github.com/dbt-labs/dbt-docs/issues/185))
- Render meta tags for sources ([docs#192](https://github.com/dbt-labs/dbt-docs/issues/192), [docs#193](https://github.com/dbt-labs/dbt-docs/issues/193))

### Under the hood

- Added logic for registry requests to raise a timeout error after a response hangs out for 30 seconds and 5 attempts have been made to reach the endpoint ([#3177](https://github.com/dbt-labs/dbt-core/issues/3177), [#3275](https://github.com/dbt-labs/dbt-core/pull/3275))
- Added support for invoking the `list` task via the RPC server ([#3311](https://github.com/dbt-labs/dbt-core/issues/3311), [#3384](https://github.com/dbt-labs/dbt-core/pull/3384))
- Added `unique_id` and `original_file_path` as keys to json responses from the `list` task ([#3356](https://github.com/dbt-labs/dbt-core/issues/3356), [#3384](https://github.com/dbt-labs/dbt-core/pull/3384))
- Use shutil.which so Windows can pick up git.bat as a git executable ([#3035](https://github.com/dbt-labs/dbt-core/issues/3035), [#3134](https://github.com/dbt-labs/dbt-core/issues/3134))
- Add `ssh-client` and update `git` version (using buster backports) in Docker image ([#3337](https://github.com/dbt-labs/dbt-core/issues/3337), [#3338](https://github.com/dbt-labs/dbt-core/pull/3338))
- Add `tags` and `meta` properties to the exposure resource schema. ([#3404](https://github.com/dbt-labs/dbt-core/issues/3404), [#3405](https://github.com/dbt-labs/dbt-core/pull/3405))
- Update test sub-query alias ([#3398](https://github.com/dbt-labs/dbt-core/issues/3398), [#3414](https://github.com/dbt-labs/dbt-core/pull/3414))
- Bump schema versions for run results and manifest artifacts ([#3422](https://github.com/dbt-labs/dbt-core/issues/3422), [#3421](https://github.com/dbt-labs/dbt-core/pull/3421))
- Add deprecation warning for using `packages` argument with `adapter.dispatch` ([#3419](https://github.com/dbt-labs/dbt-core/issues/3419), [#3420](https://github.com/dbt-labs/dbt-core/pull/3420))

Contributors:

- [@TeddyCr](https://github.com/TeddyCr) ([#3275](https://github.com/dbt-labs/dbt-core/pull/3275))
- [@panasenco](https://github.com/panasenco) ([#3315](https://github.com/dbt-labs/dbt-core/pull/3315))
- [@dmateusp](https://github.com/dmateusp) ([#3338](https://github.com/dbt-labs/dbt-core/pull/3338))
- [@peiwangdb](https://github.com/peiwangdb) ([#3344](https://github.com/dbt-labs/dbt-core/pull/3344))
- [@elikastelein](https://github.com/elikastelein) ([#3149](https://github.com/dbt-labs/dbt-core/pull/3149))
- [@majidaldo](https://github.com/majidaldo) ([#3134](https://github.com/dbt-labs/dbt-core/issues/3134))
- [@jaypeedevlin](https://github.com/jaypeedevlin) ([#2999](https://github.com/dbt-labs/dbt-core/issues/2999))
- [@PJGaetan](https://github.com/PJGaetan) ([#3315](https://github.com/dbt-labs/dbt-core/pull/3376))
- [@jnatkins](https://github.com/jnatkins) ([#3385](https://github.com/dbt-labs/dbt-core/pull/3385))
- [@matt-winkler](https://github.com/matt-winkler) ([#3365](https://github.com/dbt-labs/dbt-core/pull/3365))
- [@stkbailey](https://github.com/stkbailey) ([#3404](https://github.com/dbt-labs/dbt-core/pull/3405))
- [@mascah](https://github.com/mascah) ([docs#181](https://github.com/dbt-labs/dbt-docs/issues/181), [docs#183](https://github.com/dbt-labs/dbt-docs/pull/183))
- [@monti-python](https://github.com/monti-python) ([docs#184](https://github.com/dbt-labs/dbt-docs/issues/184))
- [@diegodewilde](https://github.com/diegodewilde) ([docs#193](https://github.com/dbt-labs/dbt-docs/issues/193))

## dbt 0.20.0b1 (May 03, 2021)

### Breaking changes

- Add Jinja tag for generic test definitions. Replacement for macros prefixed `test_` ([#1173](https://github.com/dbt-labs/dbt-core/issues/1173), [#3261](https://github.com/dbt-labs/dbt-core/pull/3261))
- Update schema/generic tests to expect a set of rows instead of a single numeric value, and to use test materialization when executing. ([#3192](https://github.com/dbt-labs/dbt-core/issues/3192), [#3286](https://github.com/dbt-labs/dbt-core/pull/3286))
- **Plugin maintainers:** For adapters that inherit from other adapters (e.g. `dbt-postgres` &rarr; `dbt-redshift`), `adapter.dispatch()` will now include parent macro implementations as viable candidates ([#2923](https://github.com/dbt-labs/dbt-core/issues/2923), [#3296](https://github.com/dbt-labs/dbt-core/pull/3296))

### Features

- Support commit hashes in dbt deps package revision ([#3268](https://github.com/dbt-labs/dbt-core/issues/3268), [#3270](https://github.com/dbt-labs/dbt-core/pull/3270))
- Add optional `subdirectory` key to install dbt packages that are not hosted at the root of a Git repository ([#275](https://github.com/dbt-labs/dbt-core/issues/275), [#3267](https://github.com/dbt-labs/dbt-core/pull/3267))
- Add optional configs for `require_partition_filter` and `partition_expiration_days` in BigQuery ([#1843](https://github.com/dbt-labs/dbt-core/issues/1843), [#2928](https://github.com/dbt-labs/dbt-core/pull/2928))
- Fix for EOL SQL comments prevent entire line execution ([#2731](https://github.com/dbt-labs/dbt-core/issues/2731), [#2974](https://github.com/dbt-labs/dbt-core/pull/2974))
- Add optional `merge_update_columns` config to specify columns to update for `merge` statements in BigQuery and Snowflake ([#1862](https://github.com/dbt-labs/dbt-core/issues/1862), [#3100](https://github.com/dbt-labs/dbt-core/pull/3100))
- Use query comment JSON as job labels for BigQuery adapter when `query-comment.job-label` is set to `true` ([#2483](https://github.com/dbt-labs/dbt-core/issues/2483)), ([#3145](https://github.com/dbt-labs/dbt-core/pull/3145))
- Set application_name for Postgres connections ([#885](https://github.com/dbt-labs/dbt-core/issues/885), [#3182](https://github.com/dbt-labs/dbt-core/pull/3182))
- Support disabling schema tests, and configuring tests from `dbt_project.yml` ([#3252](https://github.com/dbt-labs/dbt-core/issues/3252),
  [#3253](https://github.com/dbt-labs/dbt-core/issues/3253), [#3257](https://github.com/dbt-labs/dbt-core/pull/3257))
- Add native support for Postgres index creation ([#804](https://github.com/dbt-labs/dbt-core/issues/804), [3106](https://github.com/dbt-labs/dbt-core/pull/3106))
- Less greedy test selection: expand to select unselected tests if and only if all parents are selected ([#2891](https://github.com/dbt-labs/dbt-core/issues/2891), [#3235](https://github.com/dbt-labs/dbt-core/pull/3235))
- Prevent locks in Redshift during full refresh in incremental materialization. ([#2426](https://github.com/dbt-labs/dbt-core/issues/2426), [#2998](https://github.com/dbt-labs/dbt-core/pull/2998))

### Fixes

- Fix exit code from dbt debug not returning a failure when one of the tests fail ([#3017](https://github.com/dbt-labs/dbt-core/issues/3017), [#3018](https://github.com/dbt-labs/dbt-core/issues/3018))
- Auto-generated CTEs in tests and ephemeral models have lowercase names to comply with dbt coding conventions ([#3027](https://github.com/dbt-labs/dbt-core/issues/3027), [#3028](https://github.com/dbt-labs/dbt-core/issues/3028))
- Fix incorrect error message when a selector does not match any node [#3036](https://github.com/dbt-labs/dbt-core/issues/3036))
- Fix variable `_dbt_max_partition` declaration and initialization for BigQuery incremental models ([#2940](https://github.com/dbt-labs/dbt-core/issues/2940), [#2976](https://github.com/dbt-labs/dbt-core/pull/2976))
- Moving from 'master' to 'HEAD' default branch in git ([#3057](https://github.com/dbt-labs/dbt-core/issues/3057), [#3104](https://github.com/dbt-labs/dbt-core/issues/3104), [#3117](https://github.com/dbt-labs/dbt-core/issues/3117)))
- Requirement on `dataclasses` is relaxed to be between `>=0.6,<0.9` allowing dbt to cohabit with other libraries which required higher versions. ([#3150](https://github.com/dbt-labs/dbt-core/issues/3150), [#3151](https://github.com/dbt-labs/dbt-core/pull/3151))
- Add feature to add `_n` alias to same column names in SQL query ([#3147](https://github.com/dbt-labs/dbt-core/issues/3147), [#3158](https://github.com/dbt-labs/dbt-core/pull/3158))
- Raise a proper error message if dbt parses a macro twice due to macro duplication or misconfiguration. ([#2449](https://github.com/dbt-labs/dbt-core/issues/2449), [#3165](https://github.com/dbt-labs/dbt-core/pull/3165))
- Fix exposures missing in graph context variable. ([#3241](https://github.com/dbt-labs/dbt-core/issues/3241), [#3243](https://github.com/dbt-labs/dbt-core/issues/3243))
- Ensure that schema test macros are properly processed ([#3229](https://github.com/dbt-labs/dbt-core/issues/3229), [#3272](https://github.com/dbt-labs/dbt-core/pull/3272))
- Use absolute path for profiles directory instead of a path relative to the project directory. Note: If a user supplies a relative path to the profiles directory, the value of `args.profiles_dir` will still be absolute. ([#3133](https://github.com/dbt-labs/dbt-core/issues/3133), [#3176](https://github.com/dbt-labs/dbt-core/issues/3176))
- Fix FQN selector unable to find models whose name contains dots ([#3246](https://github.com/dbt-labs/dbt-core/issues/3246), [#3247](https://github.com/dbt-labs/dbt-core/issues/3247))

### Under the hood

- Add dependabot configuration for alerting maintainers about keeping dependencies up to date and secure. ([#3061](https://github.com/dbt-labs/dbt-core/issues/3061), [#3062](https://github.com/dbt-labs/dbt-core/pull/3062))
- Update script to collect and write json schema for dbt artifacts ([#2870](https://github.com/dbt-labs/dbt-core/issues/2870), [#3065](https://github.com/dbt-labs/dbt-core/pull/3065))
- Relax Google Cloud dependency pins to major versions. ([#3155](https://github.com/dbt-labs/dbt-core/pull/3156), [#3155](https://github.com/dbt-labs/dbt-core/pull/3156))
- Bump `snowflake-connector-python` and releated dependencies, support Python 3.9 ([#2985](https://github.com/dbt-labs/dbt-core/issues/2985), [#3148](https://github.com/dbt-labs/dbt-core/pull/3148))
- General development environment clean up and improve experience running tests locally ([#3194](https://github.com/dbt-labs/dbt-core/issues/3194), [#3204](https://github.com/dbt-labs/dbt-core/pull/3204), [#3228](https://github.com/dbt-labs/dbt-core/pull/3228))
- Add a new materialization for tests, update data tests to use test materialization when executing. ([#3154](https://github.com/dbt-labs/dbt-core/issues/3154), [#3181](https://github.com/dbt-labs/dbt-core/pull/3181))
- Switch from externally storing parsing state in ParseResult object to using Manifest ([#3163](http://github.com/dbt-labs/dbt-core/issues/3163), [#3219](https://github.com/dbt-labs/dbt-core/pull/3219))
- Switch from loading project files in separate parsers to loading in one place([#3244](http://github.com/dbt-labs/dbt-core/issues/3244), [#3248](https://github.com/dbt-labs/dbt-core/pull/3248))

Contributors:

- [@yu-iskw](https://github.com/yu-iskw) ([#2928](https://github.com/dbt-labs/dbt-core/pull/2928))
- [@sdebruyn](https://github.com/sdebruyn) ([#3018](https://github.com/dbt-labs/dbt-core/pull/3018))
- [@rvacaru](https://github.com/rvacaru) ([#2974](https://github.com/dbt-labs/dbt-core/pull/2974))
- [@NiallRees](https://github.com/NiallRees) ([#3028](https://github.com/dbt-labs/dbt-core/pull/3028))
- [@ran-eh](https://github.com/ran-eh) ([#3036](https://github.com/dbt-labs/dbt-core/pull/3036))
- [@pcasteran](https://github.com/pcasteran) ([#2976](https://github.com/dbt-labs/dbt-core/pull/2976))
- [@VasiliiSurov](https://github.com/VasiliiSurov) ([#3104](https://github.com/dbt-labs/dbt-core/pull/3104))
- [@jmcarp](https://github.com/jmcarp) ([#3145](https://github.com/dbt-labs/dbt-core/pull/3145))
- [@bastienboutonnet](https://github.com/bastienboutonnet) ([#3151](https://github.com/dbt-labs/dbt-core/pull/3151))
- [@max-sixty](https://github.com/max-sixty) ([#3156](https://github.com/dbt-labs/dbt-core/pull/3156)
- [@prratek](https://github.com/prratek) ([#3100](https://github.com/dbt-labs/dbt-core/pull/3100))
- [@techytushar](https://github.com/techytushar) ([#3158](https://github.com/dbt-labs/dbt-core/pull/3158))
- [@cgopalan](https://github.com/cgopalan) ([#3165](https://github.com/dbt-labs/dbt-core/pull/3165), [#3182](https://github.com/dbt-labs/dbt-core/pull/3182))
- [@fux](https://github.com/fuchsst) ([#3243](https://github.com/dbt-labs/dbt-core/issues/3243))
- [@arzavj](https://github.com/arzavj) ([3106](https://github.com/dbt-labs/dbt-core/pull/3106))
- [@JCZuurmond](https://github.com/JCZuurmond) ([#3176](https://github.com/dbt-labs/dbt-core/pull/3176))
- [@dmateusp](https://github.com/dmateusp) ([#3270](https://github.com/dbt-labs/dbt-core/pull/3270), [#3267](https://github.com/dbt-labs/dbt-core/pull/3267))
- [@monti-python](https://github.com/monti-python) ([#3247](https://github.com/dbt-labs/dbt-core/issues/3247))
- [@drkarthi](https://github.com/drkarthi) ([#2426](https://github.com/dbt-labs/dbt-core/issues/2426), [#2998](https://github.com/dbt-labs/dbt-core/pull/2998))

## dbt 0.19.2 (June 28, 2021)

### Fixes

- Fix infinite recursion when parsing schema tests due to loops in macro calls ([#3444](https://github.com/dbt-labs/dbt-core/issues/3344), [#3454](https://github.com/dbt-labs/dbt-core/pull/3454))

## dbt 0.19.2rc2 (June 03, 2021)

### Breaking changes

- Fix adapter.dispatch macro resolution when statically extracting macros. Introduce new project-level `dispatch` config. The `packages` argument to `dispatch` no longer supports macro calls; there is backwards compatibility for existing packages. The argument will no longer be supported in a future release, instead provide the `macro_namespace` argument. ([#3362](https://github.com/dbt-labs/dbt-core/issues/3362), [#3363](https://github.com/dbt-labs/dbt-core/pull/3363), [#3383](https://github.com/dbt-labs/dbt-core/pull/3383), [#3403](https://github.com/dbt-labs/dbt-core/pull/3403))

### Fixes

- Fix references to macros with package names when rendering schema tests ([#3324](https://github.com/dbt-labs/dbt-core/issues/3324), [#3345](https://github.com/dbt-labs/dbt-core/pull/3345))

## dbt 0.19.2rc1 (April 28, 2021)

### Fixes

- Ensure that schema test macros are properly processed ([#3229](https://github.com/dbt-labs/dbt-core/issues/3229), [#3272](https://github.com/dbt-labs/dbt-core/pull/3272))
- Fix regression for default project/database for BigQuery connections ([#3218](https://github.com/dbt-labs/dbt-core/issues/3218), [#3305](https://github.com/dbt-labs/dbt-core/pull/3305))

## dbt 0.19.1 (March 31, 2021)

## dbt 0.19.1rc2 (March 25, 2021)

### Fixes

- Pass service-account scopes to gcloud-based oauth ([#3040](https://github.com/dbt-labs/dbt-core/issues/3040), [#3041](https://github.com/dbt-labs/dbt-core/pull/3041))

Contributors:

- [@yu-iskw](https://github.com/yu-iskw) ([#3041](https://github.com/dbt-labs/dbt-core/pull/3041))

## dbt 0.19.1rc1 (March 15, 2021)

### Under the hood

- Update code to use Mashumaro 2.0 ([#3138](https://github.com/dbt-labs/dbt-core/pull/3138))
- Pin `agate<1.6.2` to avoid installation errors relating to its new dependency `PyICU` ([#3160](https://github.com/dbt-labs/dbt-core/issues/3160), [#3161](https://github.com/dbt-labs/dbt-core/pull/3161))
- Add an event to track resource counts ([#3050](https://github.com/dbt-labs/dbt-core/issues/3050), [#3157](https://github.com/dbt-labs/dbt-core/pull/3157))

### Fixes

- Fix compiled sql for ephemeral models ([#3139](https://github.com/dbt-labs/dbt-core/pull/3139), [#3056](https://github.com/dbt-labs/dbt-core/pull/3056))

## dbt 0.19.1b2 (February 15, 2021)

## dbt 0.19.1b1 (February 12, 2021)

### Fixes

- On BigQuery, fix regressions for `insert_overwrite` incremental strategy with `int64` and `timestamp` partition columns ([#3063](https://github.com/dbt-labs/dbt-core/issues/3063), [#3095](https://github.com/dbt-labs/dbt-core/issues/3095), [#3098](https://github.com/dbt-labs/dbt-core/issues/3098))

### Under the hood

- Bump werkzeug upper bound dependency to `<v2.0` ([#3011](https://github.com/dbt-labs/dbt-core/pull/3011))
- Performance fixes for many different things ([#2862](https://github.com/dbt-labs/dbt-core/issues/2862), [#3034](https://github.com/dbt-labs/dbt-core/pull/3034))

Contributors:

- [@Bl3f](https://github.com/Bl3f) ([#3011](https://github.com/dbt-labs/dbt-core/pull/3011))

## dbt 0.19.0 (January 27, 2021)

## dbt 0.19.0rc3 (January 27, 2021)

### Under the hood

- Cleanup docker resources, use single `docker/Dockerfile` for publishing dbt as a docker image ([dbt-release#3](https://github.com/dbt-labs/dbt-release/issues/3), [#3019](https://github.com/dbt-labs/dbt-core/pull/3019))

## dbt 0.19.0rc2 (January 14, 2021)

### Fixes

- Fix regression with defining exposures and other resources with the same name ([#2969](https://github.com/dbt-labs/dbt-core/issues/2969), [#3009](https://github.com/dbt-labs/dbt-core/pull/3009))
- Remove ellipses printed while parsing ([#2971](https://github.com/dbt-labs/dbt-core/issues/2971), [#2996](https://github.com/dbt-labs/dbt-core/pull/2996))

### Under the hood

- Rewrite macro for snapshot_merge_sql to make compatible with other SQL dialects ([#3003](https://github.com/dbt-labs/dbt-core/pull/3003)
- Rewrite logic in `snapshot_check_strategy()` to make compatible with other SQL dialects ([#3000](https://github.com/dbt-labs/dbt-core/pull/3000), [#3001](https://github.com/dbt-labs/dbt-core/pull/3001))
- Remove version restrictions on `botocore` ([#3006](https://github.com/dbt-labs/dbt-core/pull/3006))
- Include `exposures` in start-of-invocation stdout summary: `Found ...` ([#3007](https://github.com/dbt-labs/dbt-core/pull/3007), [#3008](https://github.com/dbt-labs/dbt-core/pull/3008))

Contributors:

- [@mikaelene](https://github.com/mikaelene) ([#3003](https://github.com/dbt-labs/dbt-core/pull/3003))
- [@dbeatty10](https://github.com/dbeatty10) ([dbt-adapter-tests#10](https://github.com/dbt-labs/dbt-adapter-tests/pull/10))
- [@swanderz](https://github.com/swanderz) ([#3000](https://github.com/dbt-labs/dbt-core/pull/3000))
- [@stpierre](https://github.com/stpierre) ([#3006](https://github.com/dbt-labs/dbt-core/pull/3006))

## dbt 0.19.0rc1 (December 29, 2020)

### Breaking changes

- Defer if and only if upstream reference does not exist in current environment namespace ([#2909](https://github.com/dbt-labs/dbt-core/issues/2909), [#2946](https://github.com/dbt-labs/dbt-core/pull/2946))
- Rationalize run result status reporting and clean up artifact schema ([#2493](https://github.com/dbt-labs/dbt-core/issues/2493), [#2943](https://github.com/dbt-labs/dbt-core/pull/2943))
- Add adapter specific query execution info to run results and source freshness results artifacts. Statement call blocks return `response` instead of `status`, and the adapter method `get_status` is now `get_response` ([#2747](https://github.com/dbt-labs/dbt-core/issues/2747), [#2961](https://github.com/dbt-labs/dbt-core/pull/2961))

### Features

- Added macro `get_partitions_metadata(table)` to return partition metadata for BigQuery partitioned tables ([#2552](https://github.com/dbt-labs/dbt-core/pull/2552), [#2596](https://github.com/dbt-labs/dbt-core/pull/2596))
- Added `--defer` flag for `dbt test` as well ([#2701](https://github.com/dbt-labs/dbt-core/issues/2701), [#2954](https://github.com/dbt-labs/dbt-core/pull/2954))
- Added native python `re` module for regex in jinja templates ([#1755](https://github.com/dbt-labs/dbt-core/pull/2851), [#1755](https://github.com/dbt-labs/dbt-core/pull/2851))
- Store resolved node names in manifest ([#2647](https://github.com/dbt-labs/dbt-core/issues/2647), [#2837](https://github.com/dbt-labs/dbt-core/pull/2837))
- Save selectors dictionary to manifest, allow descriptions ([#2693](https://github.com/dbt-labs/dbt-core/issues/2693), [#2866](https://github.com/dbt-labs/dbt-core/pull/2866))
- Normalize cli-style-strings in manifest selectors dictionary ([#2879](https://github.com/dbt-labs/dbt-core/issues/2879), [#2895](https://github.com/dbt-labs/dbt-core/pull/2895))
- Hourly, monthly and yearly partitions available in BigQuery ([#2476](https://github.com/dbt-labs/dbt-core/issues/2476), [#2903](https://github.com/dbt-labs/dbt-core/pull/2903))
- Allow BigQuery to default to the environment's default project ([#2828](https://github.com/dbt-labs/dbt-core/pull/2828), [#2908](https://github.com/dbt-labs/dbt-core/pull/2908))
- Rationalize run result status reporting and clean up artifact schema ([#2493](https://github.com/dbt-labs/dbt-core/issues/2493), [#2943](https://github.com/dbt-labs/dbt-core/pull/2943))

### Fixes

- Respect `--project-dir` in `dbt clean` command ([#2840](https://github.com/dbt-labs/dbt-core/issues/2840), [#2841](https://github.com/dbt-labs/dbt-core/pull/2841))
- Fix Redshift adapter `get_columns_in_relation` macro to push schema filter down to the `svv_external_columns` view ([#2854](https://github.com/dbt-labs/dbt-core/issues/2854), [#2854](https://github.com/dbt-labs/dbt-core/issues/2854))
- Increased the supported relation name length in postgres from 29 to 51 ([#2850](https://github.com/dbt-labs/dbt-core/pull/2850))
- `dbt list` command always return `0` as exit code ([#2886](https://github.com/dbt-labs/dbt-core/issues/2886), [#2892](https://github.com/dbt-labs/dbt-core/issues/2892))
- Set default `materialized` for test node configs to `test` ([#2806](https://github.com/dbt-labs/dbt-core/issues/2806), [#2902](https://github.com/dbt-labs/dbt-core/pull/2902))
- Allow `docs` blocks in `exposure` descriptions ([#2913](https://github.com/dbt-labs/dbt-core/issues/2913), [#2920](https://github.com/dbt-labs/dbt-core/pull/2920))
- Use original file path instead of absolute path as checksum for big seeds ([#2927](https://github.com/dbt-labs/dbt-core/issues/2927), [#2939](https://github.com/dbt-labs/dbt-core/pull/2939))
- Fix KeyError if deferring to a manifest with a since-deleted source, ephemeral model, or test ([#2875](https://github.com/dbt-labs/dbt-core/issues/2875), [#2958](https://github.com/dbt-labs/dbt-core/pull/2958))

### Under the hood

- Add `unixodbc-dev` package to testing docker image ([#2859](https://github.com/dbt-labs/dbt-core/pull/2859))
- Add event tracking for project parser/load times ([#2823](https://github.com/dbt-labs/dbt-core/issues/2823),[#2893](https://github.com/dbt-labs/dbt-core/pull/2893))
- Bump `cryptography` version to `>= 3.2` and bump snowflake connector to `2.3.6` ([#2896](https://github.com/dbt-labs/dbt-core/issues/2896), [#2922](https://github.com/dbt-labs/dbt-core/issues/2922))
- Widen supported Google Cloud libraries dependencies ([#2794](https://github.com/dbt-labs/dbt-core/pull/2794), [#2877](https://github.com/dbt-labs/dbt-core/pull/2877)).
- Bump `hologram` version to `0.0.11`. Add `scripts/dtr.py` ([#2888](https://github.com/dbt-labs/dbt-core/issues/2840),[#2889](https://github.com/dbt-labs/dbt-core/pull/2889))
- Bump `hologram` version to `0.0.12`. Add testing support for python3.9 ([#2822](https://github.com/dbt-labs/dbt-core/issues/2822),[#2960](https://github.com/dbt-labs/dbt-core/pull/2960))
- Bump the version requirements for `boto3` in dbt-redshift to the upper limit `1.16` to match dbt-redshift and the `snowflake-python-connector` as of version `2.3.6`. ([#2931](https://github.com/dbt-labs/dbt-core/issues/2931), ([#2963](https://github.com/dbt-labs/dbt-core/issues/2963))

### Docs

- Fixed issue where data tests with tags were not showing up in graph viz ([docs#147](https://github.com/dbt-labs/dbt-docs/issues/147), [docs#157](https://github.com/dbt-labs/dbt-docs/pull/157))

Contributors:

- [@feluelle](https://github.com/feluelle) ([#2841](https://github.com/dbt-labs/dbt-core/pull/2841))
- [ran-eh](https://github.com/ran-eh) ([#2596](https://github.com/dbt-labs/dbt-core/pull/2596))
- [@hochoy](https://github.com/hochoy) ([#2851](https://github.com/dbt-labs/dbt-core/pull/2851))
- [@brangisom](https://github.com/brangisom) ([#2855](https://github.com/dbt-labs/dbt-core/pull/2855))
- [@elexisvenator](https://github.com/elexisvenator) ([#2850](https://github.com/dbt-labs/dbt-core/pull/2850))
- [@franloza](https://github.com/franloza) ([#2837](https://github.com/dbt-labs/dbt-core/pull/2837))
- [@max-sixty](https://github.com/max-sixty) ([#2877](https://github.com/dbt-labs/dbt-core/pull/2877), [#2908](https://github.com/dbt-labs/dbt-core/pull/2908))
- [@rsella](https://github.com/rsella) ([#2892](https://github.com/dbt-labs/dbt-core/issues/2892))
- [@joellabes](https://github.com/joellabes) ([#2913](https://github.com/dbt-labs/dbt-core/issues/2913))
- [@plotneishestvo](https://github.com/plotneishestvo) ([#2896](https://github.com/dbt-labs/dbt-core/issues/2896))
- [@db-magnus](https://github.com/db-magnus) ([#2892](https://github.com/dbt-labs/dbt-core/issues/2892))
- [@tyang209](https:/github.com/tyang209) ([#2931](https://github.com/dbt-labs/dbt-core/issues/2931))

## dbt 0.19.0b1 (October 21, 2020)

### Breaking changes

- The format for `sources.json`, `run-results.json`, `manifest.json`, and `catalog.json` has changed:
  - Each now has a common metadata dictionary ([#2761](https://github.com/dbt-labs/dbt-core/issues/2761), [#2778](https://github.com/dbt-labs/dbt-core/pull/2778)). The contents include: schema and dbt versions ([#2670](https://github.com/dbt-labs/dbt-core/issues/2670), [#2767](https://github.com/dbt-labs/dbt-core/pull/2767)); `invocation_id` ([#2763](https://github.com/dbt-labs/dbt-core/issues/2763), [#2784](https://github.com/dbt-labs/dbt-core/pull/2784)); custom environment variables prefixed with `DBT_ENV_CUSTOM_ENV_` ([#2764](https://github.com/dbt-labs/dbt-core/issues/2764), [#2785](https://github.com/dbt-labs/dbt-core/pull/2785)); cli and rpc arguments in the `run_results.json` ([#2510](https://github.com/dbt-labs/dbt-core/issues/2510), [#2813](https://github.com/dbt-labs/dbt-core/pull/2813)).
  - Remove `injected_sql` from manifest nodes, use `compiled_sql` instead ([#2762](https://github.com/dbt-labs/dbt-core/issues/2762), [#2834](https://github.com/dbt-labs/dbt-core/pull/2834))

### Features

- dbt will compare configurations using the un-rendered form of the config block in `dbt_project.yml` ([#2713](https://github.com/dbt-labs/dbt-core/issues/2713), [#2735](https://github.com/dbt-labs/dbt-core/pull/2735))
- Added state and defer arguments to the RPC client, matching the CLI ([#2678](https://github.com/dbt-labs/dbt-core/issues/2678), [#2736](https://github.com/dbt-labs/dbt-core/pull/2736))
- Added ability to snapshot hard-deleted records (opt-in with `invalidate_hard_deletes` config option). ([#249](https://github.com/dbt-labs/dbt-core/issues/249), [#2749](https://github.com/dbt-labs/dbt-core/pull/2749))
- Added revival for snapshotting hard-deleted records. ([#2819](https://github.com/dbt-labs/dbt-core/issues/2819), [#2821](https://github.com/dbt-labs/dbt-core/pull/2821))
- Improved error messages for YAML selectors ([#2700](https://github.com/dbt-labs/dbt-core/issues/2700), [#2781](https://github.com/dbt-labs/dbt-core/pull/2781))
- Added `dbt_invocation_id` for each BigQuery job to enable performance analysis ([#2808](https://github.com/dbt-labs/dbt-core/issues/2808), [#2809](https://github.com/dbt-labs/dbt-core/pull/2809))
- Added support for BigQuery connections using refresh tokens ([#2344](https://github.com/dbt-labs/dbt-core/issues/2344), [#2805](https://github.com/dbt-labs/dbt-core/pull/2805))

### Under the hood

- Save `manifest.json` at the same time we save the `run_results.json` at the end of a run ([#2765](https://github.com/dbt-labs/dbt-core/issues/2765), [#2799](https://github.com/dbt-labs/dbt-core/pull/2799))
- Added strategy-specific validation to improve the relevancy of compilation errors for the `timestamp` and `check` snapshot strategies. (([#2787](https://github.com/dbt-labs/dbt-core/issues/2787), [#2791](https://github.com/dbt-labs/dbt-core/pull/2791))
- Changed rpc test timeouts to avoid locally run test failures ([#2803](https://github.com/dbt-labs/dbt-core/issues/2803),[#2804](https://github.com/dbt-labs/dbt-core/pull/2804))
- Added a `debug_query` on the base adapter that will allow plugin authors to create custom debug queries ([#2751](https://github.com/dbt-labs/dbt-core/issues/2751),[#2871](https://github.com/dbt-labs/dbt-core/pull/2817))

### Docs

- Add select/deselect option in DAG view dropups. ([docs#98](https://github.com/dbt-labs/dbt-docs/issues/98), [docs#138](https://github.com/dbt-labs/dbt-docs/pull/138))
- Fixed issue where sources with tags were not showing up in graph viz ([docs#93](https://github.com/dbt-labs/dbt-docs/issues/93), [docs#139](https://github.com/dbt-labs/dbt-docs/pull/139))
- Use `compiled_sql` instead of `injected_sql` for "Compiled" ([docs#146](https://github.com/dbt-labs/dbt-docs/issues/146), [docs#148](https://github.com/dbt-labs/dbt-docs/issues/148))

Contributors:

- [@joelluijmes](https://github.com/joelluijmes) ([#2749](https://github.com/dbt-labs/dbt-core/pull/2749), [#2821](https://github.com/dbt-labs/dbt-core/pull/2821))
- [@kingfink](https://github.com/kingfink) ([#2791](https://github.com/dbt-labs/dbt-core/pull/2791))
- [@zmac12](https://github.com/zmac12) ([#2817](https://github.com/dbt-labs/dbt-core/pull/2817))
- [@Mr-Nobody99](https://github.com/Mr-Nobody99) ([docs#138](https://github.com/dbt-labs/dbt-docs/pull/138))
- [@jplynch77](https://github.com/jplynch77) ([docs#139](https://github.com/dbt-labs/dbt-docs/pull/139))

## dbt 0.18.2 (March 22, 2021)

## dbt 0.18.2rc1 (March 12, 2021)

### Under the hood

- Pin `agate<1.6.2` to avoid installation errors relating to its new dependency
  `PyICU` ([#3160](https://github.com/dbt-labs/dbt-core/issues/3160),
  [#3161](https://github.com/dbt-labs/dbt-core/pull/3161))

## dbt 0.18.1 (October 13, 2020)

## dbt 0.18.1rc1 (October 01, 2020)

### Features

- Added retry support for rateLimitExceeded error from BigQuery, ([#2795](https://github.com/dbt-labs/dbt-core/issues/2795), [#2796](https://github.com/dbt-labs/dbt-core/issues/2796))

Contributors:

- [@championj-foxtel](https://github.com/championj-foxtel) ([#2796](https://github.com/dbt-labs/dbt-core/issues/2796))

## dbt 0.18.1b3 (September 25, 2020)

### Feature

- Added 'Last Modified' stat in snowflake catalog macro. Now should be available in docs. ([#2728](https://github.com/dbt-labs/dbt-core/issues/2728))

### Fixes

- `dbt compile` and `dbt run` failed with `KeyError: 'endpoint_resolver'` when threads > 1 and `method: iam` had been specified in the profiles.yaml ([#2756](https://github.com/dbt-labs/dbt-core/issues/2756), [#2766](https://github.com/dbt-labs/dbt-core/pull/2766))
- Fix Redshift adapter to include columns from external tables when using the get_columns_in_relation macro ([#2753](https://github.com/dbt-labs/dbt-core/issues/2753), [#2754](https://github.com/dbt-labs/dbt-core/pull/2754))

### Under the hood

- Require extra `snowflake-connector-python[secure-local-storage]` on all dbt-snowflake installations ([#2779](https://github.com/dbt-labs/dbt-core/issues/2779), [#2789](https://github.com/dbt-labs/dbt-core/pull/2789))

Contributors:

- [@Mr-Nobody99](https://github.com/Mr-Nobody99) ([#2732](https://github.com/dbt-labs/dbt-core/pull/2732))
- [@jweibel22](https://github.com/jweibel22) ([#2766](https://github.com/dbt-labs/dbt-core/pull/2766))
- [@aiguofer](https://github.com/aiguofer) ([#2754](https://github.com/dbt-labs/dbt-core/pull/2754))

## dbt 0.18.1b1 (September 17, 2020)

### Under the hood

- If column config says quote, use quoting in SQL for adding a comment. ([#2539](https://github.com/dbt-labs/dbt-core/issues/2539), [#2733](https://github.com/dbt-labs/dbt-core/pull/2733))
- Added support for running docker-based tests under Linux. ([#2739](https://github.com/dbt-labs/dbt-core/issues/2739))

### Features

- Specify all three logging levels (`INFO`, `WARNING`, `ERROR`) in result logs for commands `test`, `seed`, `run`, `snapshot` and `source snapshot-freshness` ([#2680](https://github.com/dbt-labs/dbt-core/pull/2680), [#2723](https://github.com/dbt-labs/dbt-core/pull/2723))
- Added "exposures" ([#2730](https://github.com/dbt-labs/dbt-core/issues/2730), [#2752](https://github.com/dbt-labs/dbt-core/pull/2752), [#2777](https://github.com/dbt-labs/dbt-core/issues/2777))

### Docs

- Add Exposure nodes ([docs#135](https://github.com/dbt-labs/dbt-docs/issues/135), [docs#136](https://github.com/dbt-labs/dbt-docs/pull/136), [docs#137](https://github.com/dbt-labs/dbt-docs/pull/137))

Contributors:

- [@tpilewicz](https://github.com/tpilewicz) ([#2723](https://github.com/dbt-labs/dbt-core/pull/2723))
- [@heisencoder](https://github.com/heisencoder) ([#2739](https://github.com/dbt-labs/dbt-core/issues/2739))

## dbt 0.18.0 (September 03, 2020)

### Under the hood

- Added 3 more adapter methods that the new dbt-adapter-test suite can use for testing. ([#2492](https://github.com/dbt-labs/dbt-core/issues/2492), [#2721](https://github.com/dbt-labs/dbt-core/pull/2721))
- It is now an error to attempt installing `dbt` with a Python version less than 3.6. (resolves [#2347](https://github.com/dbt-labs/dbt-core/issues/2347))
- Check for Postgres relation names longer than 63 and throw exception. ([#2197](https://github.com/dbt-labs/dbt-core/issues/2197), [#2727](https://github.com/dbt-labs/dbt-core/pull/2727))

### Fixes

- dbt now validates the require-dbt-version field before it validates the dbt_project.yml schema ([#2638](https://github.com/dbt-labs/dbt-core/issues/2638), [#2726](https://github.com/dbt-labs/dbt-core/pull/2726))

### Docs

- Add project level overviews ([docs#127](https://github.com/dbt-labs/dbt-docs/issues/127))

Contributors:

- [@genos](https://github.com/genos) ([#2722](https://github.com/dbt-labs/dbt-core/pull/2722))
- [@Mr-Nobody99](https://github.com/Mr-Nobody99) ([docs#129](https://github.com/dbt-labs/dbt-docs/pull/129))

## dbt 0.18.0rc1 (August 19, 2020)

### Breaking changes

- `adapter_macro` is no longer a macro, instead it is a builtin context method. Any custom macros that intercepted it by going through `context['dbt']` will need to instead access it via `context['builtins']` ([#2302](https://github.com/dbt-labs/dbt-core/issues/2302), [#2673](https://github.com/dbt-labs/dbt-core/pull/2673))
- `adapter_macro` is now deprecated. Use `adapter.dispatch` instead.
- Data tests are now written as CTEs instead of subqueries. Adapter plugins for adapters that don't support CTEs may require modification. ([#2712](https://github.com/dbt-labs/dbt-core/pull/2712))

### Under the hood

- Upgraded snowflake-connector-python dependency to 2.2.10 and enabled the SSO token cache ([#2613](https://github.com/dbt-labs/dbt-core/issues/2613), [#2689](https://github.com/dbt-labs/dbt-core/issues/2689), [#2698](https://github.com/dbt-labs/dbt-core/pull/2698))
- Add deprecation warnings to anonymous usage tracking ([#2688](https://github.com/dbt-labs/dbt-core/issues/2688), [#2710](https://github.com/dbt-labs/dbt-core/issues/2710))
- Data tests now behave like dbt CTEs ([#2609](https://github.com/dbt-labs/dbt-core/issues/2609), [#2712](https://github.com/dbt-labs/dbt-core/pull/2712))
- Adapter plugins can now override the CTE prefix by overriding their `Relation` attribute with a class that has a custom `add_ephemeral_prefix` implementation. ([#2660](https://github.com/dbt-labs/dbt-core/issues/2660), [#2712](https://github.com/dbt-labs/dbt-core/pull/2712))

### Features

- Add a BigQuery adapter macro to enable usage of CopyJobs ([#2709](https://github.com/dbt-labs/dbt-core/pull/2709))
- Support TTL for BigQuery tables([#2711](https://github.com/dbt-labs/dbt-core/pull/2711))
- Add better retry support when using the BigQuery adapter ([#2694](https://github.com/dbt-labs/dbt-core/pull/2694), follow-up to [#1963](https://github.com/dbt-labs/dbt-core/pull/1963))
- Added a `dispatch` method to the context adapter and deprecated `adapter_macro`. ([#2302](https://github.com/dbt-labs/dbt-core/issues/2302), [#2679](https://github.com/dbt-labs/dbt-core/pull/2679))
- The built-in schema tests now use `adapter.dispatch`, so they can be overridden for adapter plugins ([#2415](https://github.com/dbt-labs/dbt-core/issues/2415), [#2684](https://github.com/dbt-labs/dbt-core/pull/2684))
- Add support for impersonating a service account using `impersonate_service_account` in the BigQuery profile configuration ([#2677](https://github.com/dbt-labs/dbt-core/issues/2677)) ([docs](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#service-account-impersonation))
- Macros in the current project can override internal dbt macros that are called through `execute_macros`. ([#2301](https://github.com/dbt-labs/dbt-core/issues/2301), [#2686](https://github.com/dbt-labs/dbt-core/pull/2686))
- Add state:modified and state:new selectors ([#2641](https://github.com/dbt-labs/dbt-core/issues/2641), [#2695](https://github.com/dbt-labs/dbt-core/pull/2695))
- Add two new flags `--use-colors` and `--no-use-colors` to `dbt run` command to enable or disable log colorization from the command line ([#2708](https://github.com/dbt-labs/dbt-core/pull/2708))

### Fixes

- Fix Redshift table size estimation; e.g. 44 GB tables are no longer reported as 44 KB. [#2702](https://github.com/dbt-labs/dbt-core/issues/2702)
- Fix issue where jinja that only contained jinja comments wasn't rendered. ([#2707](https://github.com/dbt-labs/dbt-core/issues/2707), [#2178](https://github.com/dbt-labs/dbt-core/pull/2178))

### Docs

- Add "Referenced By" and "Depends On" sections for each node ([docs#106](https://github.com/dbt-labs/dbt-docs/pull/106))
- Add Name, Description, Column, SQL, Tags filters to site search ([docs#108](https://github.com/dbt-labs/dbt-docs/pull/108))
- Add relevance criteria to site search ([docs#113](https://github.com/dbt-labs/dbt-docs/pull/113))
- Support new selector methods, intersection, and arbitrary parent/child depth in DAG selection syntax ([docs#118](https://github.com/dbt-labs/dbt-docs/pull/118))
- Revise anonymous event tracking: simpler URL fuzzing; differentiate between Cloud-hosted and non-Cloud docs ([docs#121](https://github.com/dbt-labs/dbt-docs/pull/121))
  Contributors:
- [@bbhoss](https://github.com/bbhoss) ([#2677](https://github.com/dbt-labs/dbt-core/pull/2677))
- [@kconvey](https://github.com/kconvey) ([#2694](https://github.com/dbt-labs/dbt-core/pull/2694), [#2709](https://github.com/dbt-labs/dbt-core/pull/2709)), [#2711](https://github.com/dbt-labs/dbt-core/pull/2711))
- [@vogt4nick](https://github.com/vogt4nick) ([#2702](https://github.com/dbt-labs/dbt-core/issues/2702))
- [@stephen8chang](https://github.com/stephen8chang) ([docs#106](https://github.com/dbt-labs/dbt-docs/pull/106), [docs#108](https://github.com/dbt-labs/dbt-docs/pull/108), [docs#113](https://github.com/dbt-labs/dbt-docs/pull/113))
- [@rsenseman](https://github.com/rsenseman) ([#2708](https://github.com/dbt-labs/dbt-core/pull/2708))

## dbt 0.18.0b2 (July 30, 2020)

### Features

- Added `--defer` and `--state` flags to `dbt run`, to defer to a previously generated manifest for unselected nodes in a run. ([#2527](https://github.com/dbt-labs/dbt-core/issues/2527), [#2656](https://github.com/dbt-labs/dbt-core/pull/2656))

### Breaking changes

- Previously, dbt put macros from all installed plugins into the namespace. This version of dbt will not include adapter plugin macros unless they are from the currently-in-use adapter or one of its dependencies [#2590](https://github.com/dbt-labs/dbt-core/pull/2590)

### Features

- Added option "--adapter" to `dbt init` to create a sample `profiles.yml` based on the chosen adapter ([#2533](https://github.com/dbt-labs/dbt-core/issues/2533), [#2594](https://github.com/dbt-labs/dbt-core/pull/2594))
- Added support for Snowflake query tags at the connection and model level ([#1030](https://github.com/dbt-labs/dbt-core/issues/1030), [#2555](https://github.com/dbt-labs/dbt-core/pull/2555/))
- Added new node selector methods (`config`, `test_type`, `test_name`, `package`) ([#2425](https://github.com/dbt-labs/dbt-core/issues/2425), [#2629](https://github.com/dbt-labs/dbt-core/pull/2629))
- Added option to specify profile when connecting to Redshift via IAM ([#2437](https://github.com/dbt-labs/dbt-core/issues/2437), [#2581](https://github.com/dbt-labs/dbt-core/pull/2581))
- Add more helpful error message for misconfiguration in profiles.yml ([#2569](https://github.com/dbt-labs/dbt-core/issues/2569), [#2627](https://github.com/dbt-labs/dbt-core/pull/2627))
- Added support for setting policy tags for BigQuery columns ([#2586](https://github.com/dbt-labs/dbt-core/issues/2586), [#2589](https://github.com/dbt-labs/dbt-core/pull/2589))

### Fixes

- Adapter plugins can once again override plugins defined in core ([#2548](https://github.com/dbt-labs/dbt-core/issues/2548), [#2590](https://github.com/dbt-labs/dbt-core/pull/2590))
- Added `--selector` argument and support for `selectors.yml` file to define selection mechanisms. ([#2172](https://github.com/dbt-labs/dbt-core/issues/2172), [#2640](https://github.com/dbt-labs/dbt-core/pull/2640))
- Compile assets as part of docs generate ([#2072](https://github.com/dbt-labs/dbt-core/issues/2072), [#2623](https://github.com/dbt-labs/dbt-core/pull/2623))

Contributors:

- [@brunomurino](https://github.com/brunomurino) ([#2581](https://github.com/dbt-labs/dbt-core/pull/2581), [#2594](https://github.com/dbt-labs/dbt-core/pull/2594))
- [@DrMcTaco](https://github.com/DrMcTaco) ([#1030](https://github.com/dbt-labs/dbt-core/issues/1030)),[#2555](https://github.com/dbt-labs/dbt-core/pull/2555/))
- [@kning](https://github.com/kning) ([#2627](https://github.com/dbt-labs/dbt-core/pull/2627))
- [@azhard](https://github.com/azhard) ([#2588](https://github.com/dbt-labs/dbt-core/pull/2588))

## dbt 0.18.0b1 (June 08, 2020)

### Features

- Made project-level warnings more apparent ([#2545](https://github.com/dbt-labs/dbt-core/issues/2545))
- Added a `full_refresh` config item that overrides the behavior of the `--full-refresh` flag ([#1009](https://github.com/dbt-labs/dbt-core/issues/1009), [#2348](https://github.com/dbt-labs/dbt-core/pull/2348))
- Added a "docs" field to macros, with a "show" subfield to allow for hiding macros from the documentation site ([#2430](https://github.com/dbt-labs/dbt-core/issues/2430))
- Added intersection syntax for model selector ([#2167](https://github.com/dbt-labs/dbt-core/issues/2167), [#2417](https://github.com/dbt-labs/dbt-core/pull/2417))
- Extends model selection syntax with at most n-th parent/children `dbt run --models 3+m1+2` ([#2052](https://github.com/dbt-labs/dbt-core/issues/2052), [#2485](https://github.com/dbt-labs/dbt-core/pull/2485))
- Added support for renaming BigQuery relations ([#2520](https://github.com/dbt-labs/dbt-core/issues/2520), [#2521](https://github.com/dbt-labs/dbt-core/pull/2521))
- Added support for BigQuery authorized views ([#1718](https://github.com/dbt-labs/dbt-core/issues/1718), [#2517](https://github.com/dbt-labs/dbt-core/pull/2517))
- Added support for altering BigQuery column types ([#2546](https://github.com/dbt-labs/dbt-core/issues/2546), [#2547](https://github.com/dbt-labs/dbt-core/pull/2547))
- Include row counts and bytes processed in log output for all BigQuery statement types ([#2526](https://github.com/dbt-labs/dbt-core/issues/2526))

### Fixes

- Fixed an error in create_adapter_plugins.py script when -dependency arg not passed ([#2507](https://github.com/dbt-labs/dbt-core/issues/2507), [#2508](https://github.com/dbt-labs/dbt-core/pull/2508))
- Remove misleading "Opening a new connection" log message in set_connection_name. ([#2511](https://github.com/dbt-labs/dbt-core/issues/2511))
- Now all the BigQuery statement types return the number of bytes processed ([#2526](https://github.com/dbt-labs/dbt-core/issues/2526)).

Contributors:

- [@raalsky](https://github.com/Raalsky) ([#2417](https://github.com/dbt-labs/dbt-core/pull/2417), [#2485](https://github.com/dbt-labs/dbt-core/pull/2485))
- [@alf-mindshift](https://github.com/alf-mindshift) ([#2431](https://github.com/dbt-labs/dbt-core/pull/2431))
- [@scarrucciu](https://github.com/scarrucciu) ([#2508](https://github.com/dbt-labs/dbt-core/pull/2508))
- [@southpolemonkey](https://github.com/southpolemonkey) ([#2511](https://github.com/dbt-labs/dbt-core/issues/2511))
- [@azhard](https://github.com/azhard) ([#2517](https://github.com/dbt-labs/dbt-core/pull/2517), ([#2521](https://github.com/dbt-labs/dbt-core/pull/2521)), [#2547](https://github.com/dbt-labs/dbt-core/pull/2547))
- [@alepuccetti](https://github.com/alepuccetti) ([#2526](https://github.com/dbt-labs/dbt-core/issues/2526))

## dbt 0.17.2 (July 29, 2020)

### Fixes

- The redshift catalog now contains information for all schemas in a project, not just the default ([#2653](https://github.com/dbt-labs/dbt-core/issues/2653), [#2654](https://github.com/dbt-labs/dbt-core/pull/2654))

### Docs

- Fix background appearance of markdown ` ``` ` code blocks ([docs#114](https://github.com/dbt-labs/dbt-docs/pull/114), [docs#115](https://github.com/dbt-labs/dbt-docs/pull/115))

## dbt 0.17.2rc1 (July 28, 2020)

### Breaking changes (for plugins)

- The `release` argument to adapter.execute_macro no longer has any effect. It will be removed in a future release of dbt (likely 0.18.0) ([#2650](https://github.com/dbt-labs/dbt-core/pull/2650))

### Fixes

- fast-fail option with adapters that don't support cancelling queries will now passthrough the original error messages ([#2644](https://github.com/dbt-labs/dbt-core/issues/2644), [#2646](https://github.com/dbt-labs/dbt-core/pull/2646))
- `dbt clean` no longer requires a profile ([#2620](https://github.com/dbt-labs/dbt-core/issues/2620), [#2649](https://github.com/dbt-labs/dbt-core/pull/2649))
- Close all connections so snowflake's keepalive thread will exit. ([#2645](https://github.com/dbt-labs/dbt-core/issues/2645), [#2650](https://github.com/dbt-labs/dbt-core/pull/2650))

Contributors:

- [@joshpeng-quibi](https://github.com/joshpeng-quibi) ([#2646](https://github.com/dbt-labs/dbt-core/pull/2646))

## dbt 0.17.2b1 (July 21, 2020)

### Features

- Added environment variables for debug-level logging ([#2633](https://github.com/dbt-labs/dbt-core/issues/2633), [#2635](https://github.com/dbt-labs/dbt-core/pull/2635))

## dbt 0.17.1 (July 20, 2020)

## dbt 0.17.1rc4 (July 08, 2020)

### Fixes

- dbt native rendering now requires an opt-in with the `as_native` filter. Added `as_bool` and `as_number` filters, which are like `as_native` but also type-check. ([#2612](https://github.com/dbt-labs/dbt-core/issues/2612), [#2618](https://github.com/dbt-labs/dbt-core/pull/2618))

## dbt 0.17.1rc3 (July 01, 2020)

### Fixes

- dbt native rendering now avoids turning quoted strings into unquoted strings ([#2597](https://github.com/dbt-labs/dbt-core/issues/2597), [#2599](https://github.com/dbt-labs/dbt-core/pull/2599))
- Hash name of local packages ([#2600](https://github.com/dbt-labs/dbt-core/pull/2600))
- On bigquery, also persist docs for seeds ([#2598](https://github.com/dbt-labs/dbt-core/issues/2598), [#2601](https://github.com/dbt-labs/dbt-core/pull/2601))
- Swallow all file-writing related errors on Windows, regardless of path length or exception type. ([#2603](https://github.com/dbt-labs/dbt-core/pull/2603))

## dbt 0.17.1rc2 (June 25, 2020)

### Fixes

- dbt config-version: 2 now properly defers rendering `+pre-hook` and `+post-hook` fields. ([#2583](https://github.com/dbt-labs/dbt-core/issues/2583), [#2854](https://github.com/dbt-labs/dbt-core/pull/2854))
- dbt handles too-long paths on windows that do not report that the path is too long ([#2591](https://github.com/dbt-labs/dbt-core/pull/2591))

## dbt 0.17.1rc1 (June 19, 2020)

### Fixes

- dbt compile and ls no longer create schemas if they don't already exist ([#2525](https://github.com/dbt-labs/dbt-core/issues/2525), [#2528](https://github.com/dbt-labs/dbt-core/pull/2528))
- `dbt deps` now respects the `--project-dir` flag, so using `dbt deps --project-dir=/some/path` and then `dbt run --project-dir=/some/path` will properly find dependencies ([#2519](https://github.com/dbt-labs/dbt-core/issues/2519), [#2534](https://github.com/dbt-labs/dbt-core/pull/2534))
- `packages.yml` revision/version fields can be float-like again (`revision: '1.0'` is valid). ([#2518](https://github.com/dbt-labs/dbt-core/issues/2518), [#2535](https://github.com/dbt-labs/dbt-core/pull/2535))
- dbt again respects config aliases in config() calls ([#2557](https://github.com/dbt-labs/dbt-core/issues/2557), [#2559](https://github.com/dbt-labs/dbt-core/pull/2559))

- Parallel RPC requests no longer step on each others' arguments ([[#2484](https://github.com/dbt-labs/dbt-core/issues/2484), [#2554](https://github.com/dbt-labs/dbt-core/pull/2554)])
- `persist_docs` now takes into account descriptions for nested columns in bigquery ([#2549](https://github.com/dbt-labs/dbt-core/issues/2549), [#2550](https://github.com/dbt-labs/dbt-core/pull/2550))
- On windows (depending upon OS support), dbt no longer fails with errors when writing artifacts ([#2558](https://github.com/dbt-labs/dbt-core/issues/2558), [#2566](https://github.com/dbt-labs/dbt-core/pull/2566))
- dbt again respects config aliases in config() calls and dbt_project.yml ([#2557](https://github.com/dbt-labs/dbt-core/issues/2557), [#2559](https://github.com/dbt-labs/dbt-core/pull/2559), [#2575](https://github.com/dbt-labs/dbt-core/pull/2575))
- fix unclickable nodes in the dbt Docs DAG viz ([#101](https://github.com/dbt-labs/dbt-docs/pull/101))
- fix null database names for Spark projects in dbt Docs site ([#96](https://github.com/dbt-labs/dbt-docs/pull/96))

Contributors:

- [@bodschut](https://github.com/bodschut) ([#2550](https://github.com/dbt-labs/dbt-core/pull/2550))

## dbt 0.17.0 (June 08, 2020)

### Fixes

- Removed `pytest-logbook` dependency from `dbt-core` ([#2505](https://github.com/dbt-labs/dbt-core/pull/2505))

Contributors:

- [@aburgel](https://github.com/aburgel) ([#2505](https://github.com/dbt-labs/dbt-core/pull/2505))

## dbt 0.17.0rc4 (June 2, 2020)

### Fixes

- On snowflake, get_columns_in_relation now returns an empty list again if the relation does not exist, instead of raising an exception. ([#2504](https://github.com/dbt-labs/dbt-core/issues/2504), [#2509](https://github.com/dbt-labs/dbt-core/pull/2509))
- Added filename, project, and the value that failed to render to the exception raised when rendering fails. ([#2499](https://github.com/dbt-labs/dbt-core/issues/2499), [#2501](https://github.com/dbt-labs/dbt-core/pull/2501))

### Under the hood

- Lock protobufs to the last version that had fully functioning releases on all supported platforms ([#2490](https://github.com/dbt-labs/dbt-core/issues/2490), [#2491](https://github.com/dbt-labs/dbt-core/pull/2491))

### dbt 0.17.0rc3 (May 27, 2020)

### Fixes

- When no columns are documented and persist_docs.columns is True, skip creating comments instead of failing with errors ([#2439](https://github.com/dbt-labs/dbt-core/issues/2439), [#2440](https://github.com/dbt-labs/dbt-core/pull/2440))
- Fixed an argument issue with the `create_schema` macro on bigquery ([#2445](https://github.com/dbt-labs/dbt-core/issues/2445), [#2448](https://github.com/dbt-labs/dbt-core/pull/2448))
- dbt now logs using the adapter plugin's ideas about how relations should be displayed ([dbt-spark/#74](https://github.com/dbt-labs/dbt-spark/issues/74), [#2450](https://github.com/dbt-labs/dbt-core/pull/2450))
- The create_adapter_plugin.py script creates a version 2 dbt_project.yml file ([#2451](https://github.com/dbt-labs/dbt-core/issues/2451), [#2455](https://github.com/dbt-labs/dbt-core/pull/2455))
- Fixed dbt crashing with an AttributeError on duplicate sources ([#2463](https://github.com/dbt-labs/dbt-core/issues/2463), [#2464](https://github.com/dbt-labs/dbt-core/pull/2464))
- Fixed a number of issues with globally-scoped vars ([#2473](https://github.com/dbt-labs/dbt-core/issues/2473), [#2472](https://github.com/dbt-labs/dbt-core/issues/2472), [#2469](https://github.com/dbt-labs/dbt-core/issues/2469), [#2477](https://github.com/dbt-labs/dbt-core/pull/2477))
- Fixed DBT Docker entrypoint ([#2470](https://github.com/dbt-labs/dbt-core/issues/2470), [#2475](https://github.com/dbt-labs/dbt-core/pull/2475))
- Fixed a performance regression that occurred even when a user was not using the relevant feature ([#2474](https://github.com/dbt-labs/dbt-core/issues/2474), [#2478](https://github.com/dbt-labs/dbt-core/pull/2478))
- Substantial performance improvements for parsing on large projects, especially projects with many docs definition. ([#2480](https://github.com/dbt-labs/dbt-core/issues/2480), [#2481](https://github.com/dbt-labs/dbt-core/pull/2481))
- Expose Snowflake query id in case of an exception raised by connector ([#2201](https://github.com/dbt-labs/dbt-core/issues/2201), [#2358](https://github.com/dbt-labs/dbt-core/pull/2358))

### Under the hood

- Better support for optional database fields in adapters ([#2487](https://github.com/dbt-labs/dbt-core/issues/2487) [#2489](https://github.com/dbt-labs/dbt-core/pull/2489))

Contributors:

- [@dmateusp](https://github.com/dmateusp) ([#2475](https://github.com/dbt-labs/dbt-core/pull/2475))
- [@ChristianKohlberg](https://github.com/ChristianKohlberg) (#2358](https://github.com/dbt-labs/dbt-core/pull/2358))

## dbt 0.17.0rc1 (May 12, 2020)

### Breaking changes

- The `list_relations_without_caching`, `drop_schema`, and `create_schema` macros and methods now accept a single argument of a Relation object with no identifier field. ([#2411](https://github.com/dbt-labs/dbt-core/pull/2411))

### Features

- Added warning to nodes selector if nothing was matched ([#2115](https://github.com/dbt-labs/dbt-core/issues/2115), [#2343](https://github.com/dbt-labs/dbt-core/pull/2343))
- Suport column descriptions for BigQuery models ([#2335](https://github.com/dbt-labs/dbt-core/issues/2335), [#2402](https://github.com/dbt-labs/dbt-core/pull/2402))
- Added BigQuery option maximum_bytes_billed to set an upper limit for query costs ([#2346](https://github.com/dbt-labs/dbt-core/issues/2346), [#2427](https://github.com/dbt-labs/dbt-core/pull/2427))

### Fixes

- When tracking is disabled due to errors, do not reset the invocation ID ([#2398](https://github.com/dbt-labs/dbt-core/issues/2398), [#2400](https://github.com/dbt-labs/dbt-core/pull/2400))
- Fix for logic error in compilation errors for duplicate data test names ([#2406](https://github.com/dbt-labs/dbt-core/issues/2406), [#2407](https://github.com/dbt-labs/dbt-core/pull/2407))
- Fix list_schemas macro failing for BigQuery ([#2412](https://github.com/dbt-labs/dbt-core/issues/2412), [#2413](https://github.com/dbt-labs/dbt-core/issues/2413))
- When plugins are installed in the same folder as dbt core, report their versions. ([#2410](https://github.com/dbt-labs/dbt-core/issues/2410), [#2418](https://github.com/dbt-labs/dbt-core/pull/2418))
- Fix for making schema tests work for community plugin [dbt-sqlserver](https://github.com/mikaelene/dbt-sqlserver) [#2414](https://github.com/dbt-labs/dbt-core/pull/2414)
- Fix a bug where quoted uppercase schemas on snowflake were not processed properly during cache building. ([#2403](https://github.com/dbt-labs/dbt-core/issues/2403), [#2411](https://github.com/dbt-labs/dbt-core/pull/2411))
- Fix for extra spacing and parentheses when creating views in BigQuery ([#2421](https://github.com/dbt-labs/dbt-core/issues/2421), [#2422](https://github.com/dbt-labs/dbt-core/issues/2422))

### Docs

- Do not render hidden models in the search bar ([docs#89](https://github.com/dbt-labs/dbt-docs/issues/89), [docs#90](https://github.com/dbt-labs/dbt-docs/pull/90))

### Under the hood

- Track distinct project hashes in anonymous usage metrics for package downloads ([#2351](https://github.com/dbt-labs/dbt-core/issues/2351), [#2429](https://github.com/dbt-labs/dbt-core/pull/2429))

Contributors:

- [@azhard](https://github.com/azhard) ([#2413](https://github.com/dbt-labs/dbt-core/pull/2413), [#2422](https://github.com/dbt-labs/dbt-core/pull/2422))
- [@mikaelene](https://github.com/mikaelene) [#2414](https://github.com/dbt-labs/dbt-core/pull/2414)
- [@raalsky](https://github.com/Raalsky) ([#2343](https://github.com/dbt-labs/dbt-core/pull/2343))
- [@haukeduden](https://github.com/haukeduden) ([#2427](https://github.com/dbt-labs/dbt-core/pull/2427))
- [@alf-mindshift](https://github.com/alf-mindshift) ([docs#90](https://github.com/dbt-labs/dbt-docs/pull/90))

## dbt 0.17.0b1 (May 5, 2020)

### Breaking changes

- Added a new dbt_project.yml version format. This emits a deprecation warning currently, but support for the existing version will be removed in a future dbt version ([#2300](https://github.com/dbt-labs/dbt-core/issues/2300), [#2312](https://github.com/dbt-labs/dbt-core/pull/2312))
- The `graph` object available in some dbt contexts now has an additional member `sources` (along side the existing `nodes`). Sources have been removed from `nodes` and added to `sources` instead ([#2312](https://github.com/dbt-labs/dbt-core/pull/2312))
- The 'location' field has been removed from bigquery catalogs ([#2382](https://github.com/dbt-labs/dbt-core/pull/2382))

### Features

- Added --fail-fast argument for dbt run and dbt test to fail on first test failure or runtime error. ([#1649](https://github.com/dbt-labs/dbt-core/issues/1649), [#2224](https://github.com/dbt-labs/dbt-core/pull/2224))
- Support for appending query comments to SQL queries. ([#2138](https://github.com/dbt-labs/dbt-core/issues/2138), [#2199](https://github.com/dbt-labs/dbt-core/pull/2199))
- Added a `get-manifest` API call. ([#2168](https://github.com/dbt-labs/dbt-core/issues/2168), [#2232](https://github.com/dbt-labs/dbt-core/pull/2232))
- Support adapter-specific aliases (like `project` and `dataset` on BigQuery) in source definitions. ([#2133](https://github.com/dbt-labs/dbt-core/issues/2133), [#2244](https://github.com/dbt-labs/dbt-core/pull/2244))
- Users can now use jinja as arguments to tests. Test arguments are rendered in the native context and injected into the test execution context directly. ([#2149](https://github.com/dbt-labs/dbt-core/issues/2149), [#2220](https://github.com/dbt-labs/dbt-core/pull/2220))
- Added support for `db_groups` and `autocreate` flags in Redshift configurations. ([#1995](https://github.com/dbt-labs/dbt-core/issues/1995), [#2262](https://github.com/dbt-labs/dbt-core/pull/2262))
- Users can supply paths as arguments to `--models` and `--select`, either explicitily by prefixing with `path:` or implicitly with no prefix. ([#454](https://github.com/dbt-labs/dbt-core/issues/454), [#2258](https://github.com/dbt-labs/dbt-core/pull/2258))
- dbt now builds the relation cache for "dbt compile" and "dbt ls" as well as "dbt run" ([#1705](https://github.com/dbt-labs/dbt-core/issues/1705), [#2319](https://github.com/dbt-labs/dbt-core/pull/2319))
- Snowflake now uses "show terse objects" to build the relations cache instead of selecting from the information schema ([#2174](https://github.com/dbt-labs/dbt-core/issues/2174), [#2322](https://github.com/dbt-labs/dbt-core/pull/2322))
- Snowflake now uses "describe table" to get the columns in a relation ([#2260](https://github.com/dbt-labs/dbt-core/issues/2260), [#2324](https://github.com/dbt-labs/dbt-core/pull/2324))
- Add a 'depends_on' attribute to the log record extra field ([#2316](https://github.com/dbt-labs/dbt-core/issues/2316), [#2341](https://github.com/dbt-labs/dbt-core/pull/2341))
- Added a '--no-browser' argument to "dbt docs serve" so you can serve docs in an environment that only has a CLI browser which would otherwise deadlock dbt ([#2004](https://github.com/dbt-labs/dbt-core/issues/2004), [#2364](https://github.com/dbt-labs/dbt-core/pull/2364))
- Snowflake now uses "describe table" to get the columns in a relation ([#2260](https://github.com/dbt-labs/dbt-core/issues/2260), [#2324](https://github.com/dbt-labs/dbt-core/pull/2324))
- Sources (and therefore freshness tests) can be enabled and disabled via dbt_project.yml ([#2283](https://github.com/dbt-labs/dbt-core/issues/2283), [#2312](https://github.com/dbt-labs/dbt-core/pull/2312), [#2357](https://github.com/dbt-labs/dbt-core/pull/2357))
- schema.yml files are now fully rendered in a context that is aware of vars declared in from dbt_project.yml files ([#2269](https://github.com/dbt-labs/dbt-core/issues/2269), [#2357](https://github.com/dbt-labs/dbt-core/pull/2357))
- Sources from dependencies can be overridden in schema.yml files ([#2287](https://github.com/dbt-labs/dbt-core/issues/2287), [#2357](https://github.com/dbt-labs/dbt-core/pull/2357))
- Implement persist_docs for both `relation` and `comments` on postgres and redshift, and extract them when getting the catalog. ([#2333](https://github.com/dbt-labs/dbt-core/issues/2333), [#2378](https://github.com/dbt-labs/dbt-core/pull/2378))
- Added a filter named `as_text` to the native environment rendering code that allows users to mark a value as always being a string ([#2384](https://github.com/dbt-labs/dbt-core/issues/2384), [#2395](https://github.com/dbt-labs/dbt-core/pull/2395))
- Relation comments supported for Snowflake tables and views. Column comments supported for tables. ([#1722](https://github.com/dbt-labs/dbt-core/issues/1722), [#2321](https://github.com/dbt-labs/dbt-core/pull/2321))

### Fixes

- When a jinja value is undefined, give a helpful error instead of failing with cryptic "cannot pickle ParserMacroCapture" errors ([#2110](https://github.com/dbt-labs/dbt-core/issues/2110), [#2184](https://github.com/dbt-labs/dbt-core/pull/2184))
- Added timeout to registry download call ([#2195](https://github.com/dbt-labs/dbt-core/issues/2195), [#2228](https://github.com/dbt-labs/dbt-core/pull/2228))
- When a macro is called with invalid arguments, include the calling model in the output ([#2073](https://github.com/dbt-labs/dbt-core/issues/2073), [#2238](https://github.com/dbt-labs/dbt-core/pull/2238))
- When a warn exception is not in a jinja do block, return an empty string instead of None ([#2222](https://github.com/dbt-labs/dbt-core/issues/2222), [#2259](https://github.com/dbt-labs/dbt-core/pull/2259))
- Add dbt plugin versions to --version([#2272](https://github.com/dbt-labs/dbt-core/issues/2272), [#2279](https://github.com/dbt-labs/dbt-core/pull/2279))
- When a Redshift table is defined as "auto", don't provide diststyle ([#2246](https://github.com/dbt-labs/dbt-core/issues/2246), [#2298](https://github.com/dbt-labs/dbt-core/pull/2298))
- Made file names lookups case-insensitve (.sql, .SQL, .yml, .YML) and if .yaml files are found, raise a warning indicating dbt will parse these files in future releases. ([#1681](https://github.com/dbt-labs/dbt-core/issues/1681), [#2263](https://github.com/dbt-labs/dbt-core/pull/2263))
- Return error message when profile is empty in profiles.yml. ([#2292](https://github.com/dbt-labs/dbt-core/issues/2292), [#2297](https://github.com/dbt-labs/dbt-core/pull/2297))
- Fix skipped node count in stdout at the end of a run ([#2095](https://github.com/dbt-labs/dbt-core/issues/2095), [#2310](https://github.com/dbt-labs/dbt-core/pull/2310))
- Fix an issue where BigQuery incorrectly used a relation's quote policy as the basis for the information schema's include policy, instead of the relation's include policy. ([#2188](https://github.com/dbt-labs/dbt-core/issues/2188), [#2325](https://github.com/dbt-labs/dbt-core/pull/2325))
- Fix "dbt deps" command so it respects the "--project-dir" arg if specified. ([#2338](https://github.com/dbt-labs/dbt-core/issues/2338), [#2339](https://github.com/dbt-labs/dbt-core/issues/2339))
- On `run_cli` API calls that are passed `--vars` differing from the server's `--vars`, the RPC server rebuilds the manifest for that call. ([#2265](https://github.com/dbt-labs/dbt-core/issues/2265), [#2363](https://github.com/dbt-labs/dbt-core/pull/2363))
- Remove the query job SQL from bigquery exceptions ([#2383](https://github.com/dbt-labs/dbt-core/issues/2383), [#2393](https://github.com/dbt-labs/dbt-core/pull/2393))
- Fix "Object of type Decimal is not JSON serializable" error when BigQuery queries returned numeric types in nested data structures ([#2336](https://github.com/dbt-labs/dbt-core/issues/2336), [#2348](https://github.com/dbt-labs/dbt-core/pull/2348))
- No longer query the information_schema.schemata view on bigquery ([#2320](https://github.com/dbt-labs/dbt-core/issues/2320), [#2382](https://github.com/dbt-labs/dbt-core/pull/2382))
- Preserve original subdirectory structure in compiled files. ([#2173](https://github.com/dbt-labs/dbt-core/issues/2173), [#2349](https://github.com/dbt-labs/dbt-core/pull/2349))
- Add support for `sql_header` config in incremental models ([#2136](https://github.com/dbt-labs/dbt-core/issues/2136), [#2200](https://github.com/dbt-labs/dbt-core/pull/2200))
- The ambiguous alias check now examines the node's database value as well as the schema/identifier ([#2326](https://github.com/dbt-labs/dbt-core/issues/2326), [#2387](https://github.com/dbt-labs/dbt-core/pull/2387))
- Postgres array types can now be returned via `run_query` macro calls ([#2337](https://github.com/dbt-labs/dbt-core/issues/2337), [#2376](https://github.com/dbt-labs/dbt-core/pull/2376))
- Add missing comma to `dbt compile` help text ([#2388](https://github.com/dbt-labs/dbt-core/issues/2388) [#2389](https://github.com/dbt-labs/dbt-core/pull/2389))
- Fix for non-atomic snapshot staging table creation ([#1884](https://github.com/dbt-labs/dbt-core/issues/1884), [#2390](https://github.com/dbt-labs/dbt-core/pull/2390))
- Fix for snapshot errors when strategy changes from `check` to `timestamp` between runs ([#2350](https://github.com/dbt-labs/dbt-core/issues/2350), [#2391](https://github.com/dbt-labs/dbt-core/pull/2391))

### Under the hood

- Added more tests for source inheritance ([#2264](https://github.com/dbt-labs/dbt-core/issues/2264), [#2291](https://github.com/dbt-labs/dbt-core/pull/2291))
- Update documentation website for 0.17.0 ([#2284](https://github.com/dbt-labs/dbt-core/issues/2284))

Contributors:

- [@raalsky](https://github.com/Raalsky) ([#2224](https://github.com/dbt-labs/dbt-core/pull/2224), [#2228](https://github.com/dbt-labs/dbt-core/pull/2228))
- [@ilkinulas](https://github.com/ilkinulas) [#2199](https://github.com/dbt-labs/dbt-core/pull/2199)
- [@kyleabeauchamp](https://github.com/kyleabeauchamp) [#2262](https://github.com/dbt-labs/dbt-core/pull/2262)
- [@jeremyyeo](https://github.com/jeremyyeo) [#2259](https://github.com/dbt-labs/dbt-core/pull/2259)
- [@rodrigodelmonte](https://github.com/rodrigodelmonte) [#2298](https://github.com/dbt-labs/dbt-core/pull/2298)
- [@sumanau7](https://github.com/sumanau7) ([#2279](https://github.com/dbt-labs/dbt-core/pull/2279), [#2263](https://github.com/dbt-labs/dbt-core/pull/2263), [#2297](https://github.com/dbt-labs/dbt-core/pull/2297))
- [@nickwu241](https://github.com/nickwu241) [#2339](https://github.com/dbt-labs/dbt-core/issues/2339)
- [@Fokko](https://github.com/Fokko) [#2361](https://github.com/dbt-labs/dbt-core/pull/2361)
- [@franloza](https://github.com/franloza) [#2349](https://github.com/dbt-labs/dbt-core/pull/2349)
- [@sethwoodworth](https://github.com/sethwoodworth) [#2389](https://github.com/dbt-labs/dbt-core/pull/2389)
- [@snowflakeseitz](https://github.com/snowflakeseitz) [#2321](https://github.com/dbt-labs/dbt-core/pull/2321)

## dbt 0.16.1 (April 14, 2020)

### Features

- Support for appending query comments to SQL queries. ([#2138](https://github.com/dbt-labs/dbt-core/issues/2138) [#2199](https://github.com/dbt-labs/dbt-core/issues/2199))

### Fixes

- dbt now renders the project name in the "base" context, in particular giving it access to `var` and `env_var` ([#2230](https://github.com/dbt-labs/dbt-core/issues/2230), [#2251](https://github.com/dbt-labs/dbt-core/pull/2251))
- Fix an issue with raw blocks where multiple raw blocks in the same file resulted in an error ([#2241](https://github.com/dbt-labs/dbt-core/issues/2241), [#2252](https://github.com/dbt-labs/dbt-core/pull/2252))
- Fix a redshift-only issue that caused an error when `dbt seed` found a seed with an entirely empty column that was set to a `varchar` data type. ([#2250](https://github.com/dbt-labs/dbt-core/issues/2250), [#2254](https://github.com/dbt-labs/dbt-core/pull/2254))
- Fix a bug where third party plugins that used the default `list_schemas` and `information_schema_name` macros with database quoting enabled double-quoted the database name in their queries ([#2267](https://github.com/dbt-labs/dbt-core/issues/2267), [#2281](https://github.com/dbt-labs/dbt-core/pull/2281))
- The BigQuery "partitions" config value can now be used in `dbt_project.yml` ([#2256](https://github.com/dbt-labs/dbt-core/issues/2256), [#2280](https://github.com/dbt-labs/dbt-core/pull/2280))
- dbt deps once again does not require a profile, but if profile-specific fields are accessed users will get an error ([#2231](https://github.com/dbt-labs/dbt-core/issues/2231), [#2290](https://github.com/dbt-labs/dbt-core/pull/2290))
- Macro name collisions between dbt and plugins now raise an appropriate exception, instead of an AttributeError ([#2288](https://github.com/dbt-labs/dbt-core/issues/2288), [#2293](https://github.com/dbt-labs/dbt-core/pull/2293))
- The create_adapter_plugin.py script has been updated to support 0.16.X adapters ([#2145](https://github.com/dbt-labs/dbt-core/issues/2145), [#2294](https://github.com/dbt-labs/dbt-core/pull/2294))

### Under the hood

- Pin google libraries to higher minimum values, add more dependencies as explicit ([#2233](https://github.com/dbt-labs/dbt-core/issues/2233), [#2249](https://github.com/dbt-labs/dbt-core/pull/2249))

Contributors:

- [@ilkinulas](https://github.com/ilkinulas) [#2199](https://github.com/dbt-labs/dbt-core/pull/2199)

## dbt 0.16.0 (March 23, 2020)

## dbt 0.16.0rc4 (March 20, 2020)

### Fixes

- When dbt encounters databases, schemas, or tables with names that look like numbers, treat them as strings ([#2206](https://github.com/dbt-labs/dbt-core/issues/2206), [#2208](https://github.com/dbt-labs/dbt-core/pull/2208))
- Increased the lower bound for google-cloud-bigquery ([#2213](https://github.com/dbt-labs/dbt-core/issues/2213), [#2214](https://github.com/dbt-labs/dbt-core/pull/2214))

## dbt 0.16.0rc3 (March 11, 2020)

### Fixes

- If database quoting is enabled, do not attempt to create schemas that already exist ([#2186](https://github.com/dbt-labs/dbt-core/issues/2186), [#2187](https://github.com/dbt-labs/dbt-core/pull/2187))

### Features

- Support for appending query comments to SQL queries. ([#2138](https://github.com/dbt-labs/dbt-core/issues/2138))

## dbt 0.16.0rc2 (March 4, 2020)

### Under the hood

- Pin cffi to <1.14 to avoid a version conflict with snowflake-connector-python ([#2180](https://github.com/dbt-labs/dbt-core/issues/2180), [#2181](https://github.com/dbt-labs/dbt-core/pull/2181))

## dbt 0.16.0rc1 (March 4, 2020)

### Breaking changes

- When overriding the snowflake\_\_list_schemas macro, you must now run a result with a column named 'name' instead of the first column ([#2171](https://github.com/dbt-labs/dbt-core/pull/2171))
- dbt no longer supports databases with greater than 10,000 schemas ([#2171](https://github.com/dbt-labs/dbt-core/pull/2171))

### Features

- Remove the requirement to have a passphrase when using Snowflake key pair authentication ([#1805](https://github.com/dbt-labs/dbt-core/issues/1805), [#2164](https://github.com/dbt-labs/dbt-core/pull/2164))
- Adding optional "sslmode" parameter for postgres ([#2152](https://github.com/dbt-labs/dbt-core/issues/2152), [#2154](https://github.com/dbt-labs/dbt-core/pull/2154))
- Docs website changes:
  - Handle non-array `accepted_values` test arguments ([dbt-docs#70](https://github.com/dbt-labs/dbt-docs/pull/70))
  - Support filtering by resource type ([dbt-docs#77](https://github.com/dbt-labs/dbt-docs/pull/77))
  - Render analyses, macros, and custom data tests ([dbt-docs#72](https://github.com/dbt-labs/dbt-docs/pull/72), [dbt-docs#77](https://github.com/dbt-labs/dbt-docs/pull/77), [dbt-docs#69](https://github.com/dbt-labs/dbt-docs/pull/69))
  - Support hiding models from the docs (these nodes still render in the DAG view as "hidden") ([dbt-docs#71](https://github.com/dbt-labs/dbt-docs/pull/71))
  - Render `meta` fields as "details" in node views ([dbt-docs#73](https://github.com/dbt-labs/dbt-docs/pull/73))
  - Default to lower-casing Snowflake columns specified in all-caps ([dbt-docs#74](https://github.com/dbt-labs/dbt-docs/pull/74))
  - Upgrade site dependencies
- Support `insert_overwrite` materializtion for BigQuery incremental models ([#2153](https://github.com/dbt-labs/dbt-core/pull/2153))

### Under the hood

- Use `show terse schemas in database` (chosen based on data collected by Michael Weinberg) instead of `select ... from information_schema.schemata` when collecting the list of schemas in a database ([#2166](https://github.com/dbt-labs/dbt-core/issues/2166), [#2171](https://github.com/dbt-labs/dbt-core/pull/2171))
- Parallelize filling the cache and listing schemas in each database during startup ([#2127](https://github.com/dbt-labs/dbt-core/issues/2127), [#2157](https://github.com/dbt-labs/dbt-core/pull/2157))

Contributors:

- [@mhmcdonald](https://github.com/mhmcdonald) ([#2164](https://github.com/dbt-labs/dbt-core/pull/2164))
- [@dholleran-lendico](https://github.com/dholleran-lendico) ([#2154](https://github.com/dbt-labs/dbt-core/pull/2154))

## dbt 0.16.0b3 (February 26, 2020)

### Breaking changes

- Arguments to source tests are not parsed in the config-rendering context, and are passed as their literal unparsed values to macros ([#2150](https://github.com/dbt-labs/dbt-core/pull/2150))
- `generate_schema_name` macros that accept a single argument are no longer supported ([#2143](https://github.com/dbt-labs/dbt-core/pull/2143))

### Features

- Add a "docs" field to models, with a "show" subfield ([#1671](https://github.com/dbt-labs/dbt-core/issues/1671), [#2107](https://github.com/dbt-labs/dbt-core/pull/2107))
- Add an optional "sslmode" parameter for postgres ([#2152](https://github.com/dbt-labs/dbt-core/issues/2152), [#2154](https://github.com/dbt-labs/dbt-core/pull/2154))
- Remove the requirement to have a passphrase when using Snowflake key pair authentication ([#1804](https://github.com/dbt-labs/dbt-core/issues/1805), [#2164](https://github.com/dbt-labs/dbt-core/pull/2164))
- Support a cost-effective approach for incremental models on BigQuery using scription ([#1034](https://github.com/dbt-labs/dbt-core/issues/1034), [#2140](https://github.com/dbt-labs/dbt-core/pull/2140))
- Add a dbt-{dbt_version} user agent field to the bigquery connector ([#2121](https://github.com/dbt-labs/dbt-core/issues/2121), [#2146](https://github.com/dbt-labs/dbt-core/pull/2146))
- Add support for `generate_database_name` macro ([#1695](https://github.com/dbt-labs/dbt-core/issues/1695), [#2143](https://github.com/dbt-labs/dbt-core/pull/2143))
- Expand the search path for schema.yml (and by extension, the default docs path) to include macro-paths and analysis-paths (in addition to source-paths, data-paths, and snapshot-paths) ([#2155](https://github.com/dbt-labs/dbt-core/issues/2155), [#2160](https://github.com/dbt-labs/dbt-core/pull/2160))

### Fixes

- Fix issue where dbt did not give an error in the presence of duplicate doc names ([#2054](https://github.com/dbt-labs/dbt-core/issues/2054), [#2080](https://github.com/dbt-labs/dbt-core/pull/2080))
- Include vars provided to the cli method when running the actual method ([#2092](https://github.com/dbt-labs/dbt-core/issues/2092), [#2104](https://github.com/dbt-labs/dbt-core/pull/2104))
- Improved error messages with malformed packages.yml ([#2017](https://github.com/dbt-labs/dbt-core/issues/2017), [#2078](https://github.com/dbt-labs/dbt-core/pull/2078))
- Fix an issue where dbt rendered source test args, fix issue where dbt ran an extra compile pass over the wrapped SQL. ([#2114](https://github.com/dbt-labs/dbt-core/issues/2114), [#2150](https://github.com/dbt-labs/dbt-core/pull/2150))
- Set more upper bounds for jinja2,requests, and idna dependencies, upgrade snowflake-connector-python ([#2147](https://github.com/dbt-labs/dbt-core/issues/2147), [#2151](https://github.com/dbt-labs/dbt-core/pull/2151))

Contributors:

- [@bubbomb](https://github.com/bubbomb) ([#2080](https://github.com/dbt-labs/dbt-core/pull/2080))
- [@sonac](https://github.com/sonac) ([#2078](https://github.com/dbt-labs/dbt-core/pull/2078))

## dbt 0.16.0b1 (February 11, 2020)

### Breaking changes

- Update the debug log format ([#2099](https://github.com/dbt-labs/dbt-core/pull/2099))
- Removed `docrefs` from output ([#2096](https://github.com/dbt-labs/dbt-core/pull/2096))
- Contexts updated to be more consistent and well-defined ([#1053](https://github.com/dbt-labs/dbt-core/issues/1053), [#1981](https://github.com/dbt-labs/dbt-core/issues/1981), [#1255](https://github.com/dbt-labs/dbt-core/issues/1255), [#2085](https://github.com/dbt-labs/dbt-core/pull/2085))
- The syntax of the `get_catalog` macro has changed ([#2037](https://github.com/dbt-labs/dbt-core/pull/2037))
- Agate type inference is no longer locale-specific. Only a small number of date/datetime formats are supported. If a seed has a specified column type, agate will not perform any type inference (it will instead be cast from a string). ([#999](https://github.com/dbt-labs/dbt-core/issues/999), [#1639](https://github.com/dbt-labs/dbt-core/issues/1639), [#1920](https://github.com/dbt-labs/dbt-core/pull/1920))

### Features

- Add column-level quoting control for tests ([#2106](https://github.com/dbt-labs/dbt-core/issues/2106), [#2047](https://github.com/dbt-labs/dbt-core/pull/2047))
- Add the macros every node uses to its `depends_on.macros` list ([#2082](https://github.com/dbt-labs/dbt-core/issues/2082), [#2103](https://github.com/dbt-labs/dbt-core/pull/2103))
- Add `arguments` field to macros ([#2081](https://github.com/dbt-labs/dbt-core/issues/2081), [#2083](https://github.com/dbt-labs/dbt-core/issues/2083), [#2096](https://github.com/dbt-labs/dbt-core/pull/2096))
- Batch the anonymous usage statistics requests to improve performance ([#2008](https://github.com/dbt-labs/dbt-core/issues/2008), [#2089](https://github.com/dbt-labs/dbt-core/pull/2089))
- Add documentation for macros/analyses ([#1041](https://github.com/dbt-labs/dbt-core/issues/1041), [#2068](https://github.com/dbt-labs/dbt-core/pull/2068))
- Search for docs in 'data' and 'snapshots' folders, in addition to 'models' ([#1832](https://github.com/dbt-labs/dbt-core/issues/1832), [#2058](https://github.com/dbt-labs/dbt-core/pull/2058))
- Add documentation for snapshots and seeds ([#1974](https://github.com/dbt-labs/dbt-core/issues/1974), [#2051](https://github.com/dbt-labs/dbt-core/pull/2051))
- Add `Column.is_number`/`Column.is_float` methods ([#1969](https://github.com/dbt-labs/dbt-core/issues/1969), [#2046](https://github.com/dbt-labs/dbt-core/pull/2046))
- Detect duplicate macros and cause an error when they are detected ([#1891](https://github.com/dbt-labs/dbt-core/issues/1891), [#2045](https://github.com/dbt-labs/dbt-core/pull/2045))
- Add support for `--select` on `dbt seed` ([#1711](https://github.com/dbt-labs/dbt-core/issues/1711), [#2042](https://github.com/dbt-labs/dbt-core/pull/2042))
- Add tags for sources (like model tags) and columns (tags apply to tests of that column) ([#1906](https://github.com/dbt-labs/dbt-core/issues/1906), [#1586](https://github.com/dbt-labs/dbt-core/issues/1586), [#2039](https://github.com/dbt-labs/dbt-core/pull/2039))
- Improve the speed of catalog generation by performing multiple smaller queries instead of one huge query ([#2009](https://github.com/dbt-labs/dbt-core/issues/2009), [#2037](https://github.com/dbt-labs/dbt-core/pull/2037))
- Add`toyaml` and `fromyaml` methods to the base context ([#1911](https://github.com/dbt-labs/dbt-core/issues/1911), [#2036](https://github.com/dbt-labs/dbt-core/pull/2036))
- Add `database_schemas` to the on-run-end context ([#1924](https://github.com/dbt-labs/dbt-core/issues/1924), [#2031](https://github.com/dbt-labs/dbt-core/pull/2031))
- Add the concept of `builtins` to the dbt context, make it possible to override functions like `ref` ([#1603](https://github.com/dbt-labs/dbt-core/issues/1603), [#2028](https://github.com/dbt-labs/dbt-core/pull/2028))
- Add a `meta` key to most `schema.yml` objects ([#1362](https://github.com/dbt-labs/dbt-core/issues/1362), [#2015](https://github.com/dbt-labs/dbt-core/pull/2015))
- Add clickable docs URL link in CLI output ([#2027](https://github.com/dbt-labs/dbt-core/issues/2027), [#2131](https://github.com/dbt-labs/dbt-core/pull/2131))
- Add `role` parameter in Postgres target configuration ([#1955](https://github.com/dbt-labs/dbt-core/issues/1955), [#2137](https://github.com/dbt-labs/dbt-core/pull/2137))
- Parse model hooks and collect `ref` statements ([#1957](https://github.com/dbt-labs/dbt-core/issues/1957), [#2025](https://github.com/dbt-labs/dbt-core/pull/2025))

### Fixes

- Fix the help output for `dbt docs` and `dbt source` to not include misleading flags ([#2038](https://github.com/dbt-labs/dbt-core/issues/2038), [#2105](https://github.com/dbt-labs/dbt-core/pull/2105))
- Allow `dbt debug` from subdirectories ([#2086](https://github.com/dbt-labs/dbt-core/issues/2086), [#2094](https://github.com/dbt-labs/dbt-core/pull/2094))
- Fix the `--no-compile` flag to `dbt docs generate` not crash dbt ([#2090](https://github.com/dbt-labs/dbt-core/issues/2090), [#2093](https://github.com/dbt-labs/dbt-core/pull/2093))
- Fix issue running `dbt debug` with an empty `dbt_project.yml` file ([#2116](https://github.com/dbt-labs/dbt-core/issues/2116), [#2120](https://github.com/dbt-labs/dbt-core/pull/2120))
- Ovewrwrite source config fields that should clobber, rather than deep merging them ([#2049](https://github.com/dbt-labs/dbt-core/issues/2049), [#2062](https://github.com/dbt-labs/dbt-core/pull/2062))
- Fix a bug in macro search where built-in macros could not be overridden for `dbt run-operation` ([#2032](https://github.com/dbt-labs/dbt-core/issues/2032), [#2035](https://github.com/dbt-labs/dbt-core/pull/2035))
- dbt now detects dependencies with the same name as the current project as an error instead of silently clobbering each other ([#2029](https://github.com/dbt-labs/dbt-core/issues/2029), [#2030](https://github.com/dbt-labs/dbt-core/pull/2030))
- Exclude tests of disabled models in compile statistics ([#1804](https://github.com/dbt-labs/dbt-core/issues/1804), [#2026](https://github.com/dbt-labs/dbt-core/pull/2026))
- Do not show ephemeral models as being cancelled during ctrl+c ([#1993](https://github.com/dbt-labs/dbt-core/issues/1993), [#2024](https://github.com/dbt-labs/dbt-core/pull/2024))
- Improve errors on plugin import failure ([#2006](https://github.com/dbt-labs/dbt-core/issues/2006), [#2022](https://github.com/dbt-labs/dbt-core/pull/2022))
- Fix the behavior of the `project-dir` argument when running `dbt debug` ([#1733](https://github.com/dbt-labs/dbt-core/issues/1733), [#1989](https://github.com/dbt-labs/dbt-core/pull/1989))

### Under the hood

- Improve the CI process for externally-contributed PRs ([#2033](https://github.com/dbt-labs/dbt-core/issues/2033), [#2097](https://github.com/dbt-labs/dbt-core/pull/2097))
- lots and lots of mypy/typing fixes ([#2010](https://github.com/dbt-labs/dbt-core/pull/2010))

Contributors:

- [@aaronsteers](https://github.com/aaronsteers) ([#2131](https://github.com/dbt-labs/dbt-core/pull/2131))
- [@alanmcruickshank](https://github.com/alanmcruickshank) ([#2028](https://github.com/dbt-labs/dbt-core/pull/2028))
- [@franloza](https://github.com/franloza) ([#1989](https://github.com/dbt-labs/dbt-core/pull/1989))
- [@heisencoder](https://github.com/heisencoder) ([#2099](https://github.com/dbt-labs/dbt-core/pull/2099))
- [@nchammas](https://github.com/nchammas) ([#2120](https://github.com/dbt-labs/dbt-core/pull/2120))
- [@NiallRees](https://github.com/NiallRees) ([#2026](https://github.com/dbt-labs/dbt-core/pull/2026))
- [@shooka](https://github.com/shooka) ([#2137](https://github.com/dbt-labs/dbt-core/pull/2137))
- [@tayloramurphy](https://github.com/tayloramurphy) ([#2015](https://github.com/dbt-labs/dbt-core/pull/2015))

## dbt 0.15.3 (February 19, 2020)

This is a bugfix release.

### Fixes

- Use refresh tokens in snowflake instead of access tokens ([#2126](https://github.com/dbt-labs/dbt-core/issues/2126), [#2141](https://github.com/dbt-labs/dbt-core/pull/2141))

## dbt 0.15.2 (February 2, 2020)

This is a bugfix release.

### Features

- Add support for Snowflake OAuth authentication ([#2050](https://github.com/dbt-labs/dbt-core/issues/2050), [#2069](https://github.com/dbt-labs/dbt-core/pull/2069))
- Add a -t flag as an alias for `dbt run --target` ([#1281](https://github.com/dbt-labs/dbt-core/issues/1281), [#2057](https://github.com/dbt-labs/dbt-core/pull/2057))

### Fixes

- Fix for UnicodeDecodeError when installing dbt via pip ([#1771](https://github.com/dbt-labs/dbt-core/issues/1771), [#2076](https://github.com/dbt-labs/dbt-core/pull/2076))
- Fix for ability to clean "protected" paths in the `dbt clean` command and improve logging ([#2059](https://github.com/dbt-labs/dbt-core/issues/2059), [#2060](https://github.com/dbt-labs/dbt-core/pull/2060))
- Fix for dbt server error when `{% docs %}` tags are malformed ([#2066](https://github.com/dbt-labs/dbt-core/issues/2066), [#2067](https://github.com/dbt-labs/dbt-core/pull/2067))
- Fix for errant duplicate resource errors when models are disabled and partial parsing is enabled ([#2055](https://github.com/dbt-labs/dbt-core/issues/2055), [#2056](https://github.com/dbt-labs/dbt-core/pull/2056))
- Fix for errant duplicate resource errors when a resource is included in multiple source paths ([#2064](https://github.com/dbt-labs/dbt-core/issues/2064), [#2065](https://github.com/dbt-labs/dbt-core/pull/2065/files))

Contributors:

- [@markberger](https://github.com/markeberger) ([#2076](https://github.com/dbt-labs/dbt-core/pull/2076))
- [@emilieschario](https://github.com/emilieschario) ([#2060](https://github.com/dbt-labs/dbt-core/pull/2060))

## dbt 0.15.1 (January 17, 2020)

This is a bugfix release.

### Features

- Lazily load database connections ([#1584](https://github.com/dbt-labs/dbt-core/issues/1584), [#1992](https://github.com/dbt-labs/dbt-core/pull/1992))
- Support raising warnings in user-space ([#1970](https://github.com/dbt-labs/dbt-core/issues/1970), [#1977](https://github.com/dbt-labs/dbt-core/pull/1977))
- Suppport BigQuery label configuration for models ([#1942](https://github.com/dbt-labs/dbt-core/issues/1942), [#1964](https://github.com/dbt-labs/dbt-core/pull/1964))
- Support retrying when BigQuery models fail with server errors ([#1579](https://github.com/dbt-labs/dbt-core/issues/1579), [#1963](https://github.com/dbt-labs/dbt-core/pull/1963))
- Support sql headers in create table/view statements ([#1879](https://github.com/dbt-labs/dbt-core/issues/1879), [#1967](https://github.com/dbt-labs/dbt-core/pull/1967))
- Add source snapshot-freshness to dbt rpc ([#2040](https://github.com/dbt-labs/dbt-core/issues/2040), [#2041](https://github.com/dbt-labs/dbt-core/pull/2041))

### Fixes

- Fix for catalog generation error when datasets are missing on BigQuery ([#1984](https://github.com/dbt-labs/dbt-core/issues/1984), [#2005](https://github.com/dbt-labs/dbt-core/pull/2005))
- Fix for invalid SQL generated when "check" strategy is used in Snapshots with changing schemas ([#1797](https://github.com/dbt-labs/dbt-core/issues/1797), [#2001](https://github.com/dbt-labs/dbt-core/pull/2001)(
- Fix for gaps in valid_from and valid_to timestamps when "check" strategy is used in Snapshots on some databases ([#1736](https://github.com/dbt-labs/dbt-core/issues/1736), [#1994](https://github.com/dbt-labs/dbt-core/pull/1994))
- Fix incorrect thread names in dbt server logs ([#1905](https://github.com/dbt-labs/dbt-core/issues/1905), [#2002](https://github.com/dbt-labs/dbt-core/pull/2002))
- Fix for ignored catalog data when user schemas begin with `pg*` on Postgres and Redshift ([#1960](https://github.com/dbt-labs/dbt-core/issues/1960), [#2003](https://github.com/dbt-labs/dbt-core/pull/2003))
- Fix for poorly defined materialization resolution logic ([#1962](https://github.com/dbt-labs/dbt-core/issues/1962), [#1976](https://github.com/dbt-labs/dbt-core/pull/1976))
- Fix missing `drop_schema` method in adapter namespace ([#1980](https://github.com/dbt-labs/dbt-core/issues/1980), [#1983](https://github.com/dbt-labs/dbt-core/pull/1983))
- Fix incorrect `generated_at` value in the catalog ([#1988](https://github.com/dbt-labs/dbt-core/pull/1988))

### Under the hood

- Fail more gracefully at install time when setuptools is downlevel ([#1975](https://github.com/dbt-labs/dbt-core/issues/1975), [#1978](https://github.com/dbt-labs/dbt-core/pull/1978))
- Make the `DBT_TEST_ALT` integration test warehouse configurable on Snowflake ([#1939](https://github.com/dbt-labs/dbt-core/issues/1939), [#1979](https://github.com/dbt-labs/dbt-core/pull/1979))
- Pin upper bound on `google-cloud-bigquery` dependency to `1.24.0`. ([#2007](https://github.com/dbt-labs/dbt-core/pull/2007))
- Remove duplicate `get_context_modules` method ([#1996](https://github.com/dbt-labs/dbt-core/pull/1996))
- Add type annotations to base adapter code ([#1982](https://github.com/dbt-labs/dbt-core/pull/1982))

Contributors:

- [@Fokko](https://github.com/Fokko) ([#1996](https://github.com/dbt-labs/dbt-core/pull/1996), [#1988](https://github.com/dbt-labs/dbt-core/pull/1988), [#1982](https://github.com/dbt-labs/dbt-core/pull/1982))
- [@kconvey](https://github.com/kconvey) ([#1967](https://github.com/dbt-labs/dbt-core/pull/1967))

## dbt 0.15.0 (November 25, 2019)

### Breaking changes

- Support for Python 2.x has been dropped [as it will no longer be supported on January 1, 2020](https://www.python.org/dev/peps/pep-0373/)
- Compilation errors in .yml files are now treated as errors instead of warnings ([#1493](https://github.com/dbt-labs/dbt-core/issues/1493), [#1751](https://github.com/dbt-labs/dbt-core/pull/1751))
- The 'table_name' field field has been removed from Relations
- The existing `compile` and `execute` rpc tasks have been renamed to `compile_sql` and `execute_sql` ([#1779](https://github.com/dbt-labs/dbt-core/issues/1779), [#1798](https://github.com/dbt-labs/dbt-core/pull/1798)) ([docs](https://docs.getdbt.com/v0.15/docs/rpc))
- Custom materializations must now manage dbt's Relation cache ([docs](https://docs.getdbt.com/v0.15/docs/creating-new-materializations#section-6-update-the-relation-cache))

### Installation notes:

dbt v0.15.0 uses the `psycopg2-binary` dependency (instead of `psycopg2`) to simplify installation on platforms that do not have a compiler toolchain installed. If you experience segmentation faults, crashes, or installation errors, you can set the `DBT_PSYCOPG2_NAME` environment variable to `psycopg2` to change the dependency that dbt installs. This may require a compiler toolchain and development libraries.

```bash
$ DBT_PSYCOPG2_NAME=psycopg2 pip install dbt
```

You may also install specific dbt plugins directly by name. This has the advantage of only installing the Python requirements needed for your particular database:

```bash
$ pip install dbt-postgres
$ pip install dbt-redshift
$ pip install dbt-snowflake
$ pip install dbt-bigquery
```

### Core

#### Features

- Add a JSON logger ([#1237](https://github.com/dbt-labs/dbt-core/issues/1237), [#1791](https://github.com/dbt-labs/dbt-core/pull/1791)) ([docs](https://docs.getdbt.com/v0.15/docs/global-cli-flags#section-log-formatting))
- Add structured logging to dbt ([#1704](https://github.com/dbt-labs/dbt-core/issues/1704), [#1799](https://github.com/dbt-labs/dbt-core/issues/1799), [#1715](https://github.com/dbt-labs/dbt-core/pull/1715), [#1806](https://github.com/dbt-labs/dbt-core/pull/1806))
- Add partial parsing option to the profiles.yml file ([#1835](https://github.com/dbt-labs/dbt-core/issues/1835), [#1836](https://github.com/dbt-labs/dbt-core/pull/1836), [#1487](https://github.com/dbt-labs/dbt-core/issues/1487)) ([docs](https://docs.getdbt.com/v0.15/docs/configure-your-profile#section-partial-parsing))
- Support configurable query comments in SQL queries ([#1643](https://github.com/dbt-labs/dbt-core/issues/1643), [#1864](https://github.com/dbt-labs/dbt-core/pull/1864)) ([docs](https://docs.getdbt.com/v0.15/docs/configuring-query-comments))
- Support atomic full-refreshes for incremental models ([#525](https://github.com/dbt-labs/dbt-core/issues/525), [#1682](https://github.com/dbt-labs/dbt-core/pull/1682))
- Support snapshot configs in dbt_project.yml ([#1613](https://github.com/dbt-labs/dbt-core/issues/1613), [#1759](https://github.com/dbt-labs/dbt-core/pull/1759)) ([docs](https://docs.getdbt.com/v0.15/docs/snapshots#section-configuring-snapshots-in-dbt_project-yml))
- Support cache modifications in materializations ([#1683](https://github.com/dbt-labs/dbt-core/issues/1683), [#1770](https://github.com/dbt-labs/dbt-core/pull/1770)) ([docs](https://docs.getdbt.com/v0.15/docs/creating-new-materializations#section-6-update-the-relation-cache))
- Support `quote` parameter to Accepted Values schema tests ([#1873](https://github.com/dbt-labs/dbt-core/issues/1873), [#1876](https://github.com/dbt-labs/dbt-core/pull/1876)) ([docs](https://docs.getdbt.com/v0.15/docs/testing#section-accepted-values))
- Support Python 3.8 ([#1886](https://github.com/dbt-labs/dbt-core/pull/1886))
- Support filters in sources for `dbt source snapshot-freshness` invocation ([#1495](https://github.com/dbt-labs/dbt-core/issues/1495), [#1776](https://github.com/dbt-labs/dbt-core/pull/1776)) ([docs](https://docs.getdbt.com/v0.15/docs/using-sources#section-filtering-sources))
- Support external table configuration in yml source specifications ([#1784](https://github.com/dbt-labs/dbt-core/pull/1784))
- Improve CLI output when running snapshots ([#1768](https://github.com/dbt-labs/dbt-core/issues/1768), [#1769](https://github.com/dbt-labs/dbt-core/pull/1769))

#### Fixes

- Fix for unhelpful error message for malformed source/ref inputs ([#1660](https://github.com/dbt-labs/dbt-core/issues/1660), [#1809](https://github.com/dbt-labs/dbt-core/pull/1809))
- Fix for lingering backup tables when incremental models are full-refreshed ([#1933](https://github.com/dbt-labs/dbt-core/issues/1933), [#1931](https://github.com/dbt-labs/dbt-core/pull/1931))
- Fix for confusing error message when errors are encountered during compilation ([#1807](https://github.com/dbt-labs/dbt-core/issues/1807), [#1839](https://github.com/dbt-labs/dbt-core/pull/1839))
- Fix for logic error affecting the two-argument flavor of the `ref` function ([#1504](https://github.com/dbt-labs/dbt-core/issues/1504), [#1515](https://github.com/dbt-labs/dbt-core/pull/1515))
- Fix for invalid reference to dbt.exceptions ([#1569](https://github.com/dbt-labs/dbt-core/issues/1569), [#1609](https://github.com/dbt-labs/dbt-core/pull/1609))
- Fix for "cannot run empty query" error when pre/post-hooks are empty ([#1108](https://github.com/dbt-labs/dbt-core/issues/1108), [#1719](https://github.com/dbt-labs/dbt-core/pull/1719))
- Fix for confusing error when project names shadow context attributes ([#1696](https://github.com/dbt-labs/dbt-core/issues/1696), [#1748](https://github.com/dbt-labs/dbt-core/pull/1748))
- Fix for incorrect database logic in docs generation which resulted in columns being "merged" together across tables ([#1708](https://github.com/dbt-labs/dbt-core/issues/1708), [#1774](https://github.com/dbt-labs/dbt-core/pull/1774))
- Fix for seed errors located in dependency packages ([#1723](https://github.com/dbt-labs/dbt-core/issues/1723), [#1723](https://github.com/dbt-labs/dbt-core/issues/1723))
- Fix for confusing error when schema tests return unexpected results ([#1808](https://github.com/dbt-labs/dbt-core/issues/1808), [#1903](https://github.com/dbt-labs/dbt-core/pull/1903))
- Fix for twice-compiled `statement` block contents ([#1717](https://github.com/dbt-labs/dbt-core/issues/1717), [#1719](https://github.com/dbt-labs/dbt-core/pull/1719))
- Fix for inaccurate output in `dbt run-operation --help` ([#1767](https://github.com/dbt-labs/dbt-core/issues/1767), [#1777](https://github.com/dbt-labs/dbt-core/pull/1777))
- Fix for file rotation issues concerning the `logs/dbt.log` file ([#1863](https://github.com/dbt-labs/dbt-core/issues/1863), [#1865](https://github.com/dbt-labs/dbt-core/issues/1865), [#1871](https://github.com/dbt-labs/dbt-core/pull/1871))
- Fix for missing quotes in incremental model build queries ([#1847](https://github.com/dbt-labs/dbt-core/issues/1847), [#1888](https://github.com/dbt-labs/dbt-core/pull/1888))
- Fix for incorrect log level in `printer.print_run_result_error` ([#1818](https://github.com/dbt-labs/dbt-core/issues/1818), [#1823](https://github.com/dbt-labs/dbt-core/pull/1823))

### Docs

- Show seeds and snapshots in the Project and Database views ([docs#37](https://github.com/dbt-labs/dbt-docs/issues/37), [docs#25](https://github.com/dbt-labs/dbt-docs/issues/25), [docs#52](https://github.com/dbt-labs/dbt-docs/pull/52))
- Show sources in the Database tree view ([docs#20](https://github.com/dbt-labs/dbt-docs/issues/20), [docs#52](https://github.com/dbt-labs/dbt-docs/pull/52))
- Show edges in the DAG between models and seeds ([docs#15](https://github.com/dbt-labs/dbt-docs/issues/15), [docs#52](https://github.com/dbt-labs/dbt-docs/pull/52))
- Show Accepted Values tests and custom schema tests in the column list for models ([docs#52](https://github.com/dbt-labs/dbt-docs/pull/52))
- Fix links for "Refocus on node" and "View documentation" in DAG context menu for seeds ([docs#52](https://github.com/dbt-labs/dbt-docs/pull/52))

### Server

- Support docs generation ([#1781](https://github.com/dbt-labs/dbt-core/issues/1781), [#1801](https://github.com/dbt-labs/dbt-core/pull/1801))
- Support custom tags ([#1822](https://github.com/dbt-labs/dbt-core/issues/1822), [#1828](https://github.com/dbt-labs/dbt-core/pull/1828))
- Support invoking `deps` on the rpc server ([#1834](https://github.com/dbt-labs/dbt-core/issues/1834), [#1837](https://github.com/dbt-labs/dbt-core/pull/1837))
- Support invoking `run-operation` and `snapshot` on the rpc server ([#1875](https://github.com/dbt-labs/dbt-core/issues/1875), [#1878](https://github.com/dbt-labs/dbt-core/pull/1878))
- Suppport `--threads` argument to `cli_args` method ([#1897](https://github.com/dbt-labs/dbt-core/issues/1897), [#1909](https://github.com/dbt-labs/dbt-core/pull/1909))
- Support reloading the manifest when a SIGHUP signal is received ([#1684](https://github.com/dbt-labs/dbt-core/issues/1684), [#1699](https://github.com/dbt-labs/dbt-core/pull/1699))
- Support invoking `compile`, `run`, `test`, and `seed` on the rpc server ([#1488](https://github.com/dbt-labs/dbt-core/issues/1488), [#1652](https://github.com/dbt-labs/dbt-core/pull/1652))
- Support returning compilation logs from the last compile in the `status` method ([#1703](https://github.com/dbt-labs/dbt-core/issues/1703), [#1775](https://github.com/dbt-labs/dbt-core/pull/1715))
- Support asyncronous `compile_sql` and `run_sql` methods ([#1706](https://github.com/dbt-labs/dbt-core/issues/1706), [#1735](https://github.com/dbt-labs/dbt-core/pull/1735))
- Improve re-compilation performance ([#1824](https://github.com/dbt-labs/dbt-core/issues/1824), [#1830](https://github.com/dbt-labs/dbt-core/pull/1830))

### Postgres / Redshift

- Support running dbt against schemas which contain materialized views on Postgres ([#1698](https://github.com/dbt-labs/dbt-core/issues/1698), [#1833](https://github.com/dbt-labs/dbt-core/pull/1833))
- Support distyle AUTO in Redshift model configs ([#1882](https://github.com/dbt-labs/dbt-core/issues/1882), [#1885](https://github.com/dbt-labs/dbt-core/pull/1885)) ([docs](https://docs.getdbt.com/v0.15/docs/redshift-configs#section-using-sortkey-and-distkey))
- Fix for internal errors when run against mixed-case logical databases ([#1800](https://github.com/dbt-labs/dbt-core/issues/1800), [#1936](https://github.com/dbt-labs/dbt-core/pull/1936))

### Snowflake

- Support `copy grants` option in Snowflake model configs ([#1744](https://github.com/dbt-labs/dbt-core/issues/1744), [#1747](https://github.com/dbt-labs/dbt-core/pull/1747)) ([docs](https://docs.getdbt.com/v0.15/docs/snowflake-configs#section-copying-grants))
- Support warehouse configuration in Snowflake model configs ([#1358](https://github.com/dbt-labs/dbt-core/issues/1358), [#1899](https://github.com/dbt-labs/dbt-core/issues/1899), [#1788](https://github.com/dbt-labs/dbt-core/pull/1788), [#1901](https://github.com/dbt-labs/dbt-core/pull/1901)) ([docs](https://docs.getdbt.com/v0.15/docs/snowflake-configs#section-configuring-virtual-warehouses))
- Support secure views in Snowflake model configs ([#1730](https://github.com/dbt-labs/dbt-core/issues/1730), [#1743](https://github.com/dbt-labs/dbt-core/pull/1743)) ([docs](https://docs.getdbt.com/v0.15/docs/snowflake-configs#section-secure-views))
- Fix for unclosed connections preventing dbt from exiting when Snowflake is used with client_session_keep_alive ([#1271](https://github.com/dbt-labs/dbt-core/issues/1271), [#1749](https://github.com/dbt-labs/dbt-core/pull/1749))
- Fix for errors on Snowflake when dbt schemas contain `LOCAL TEMPORARY` tables ([#1869](https://github.com/dbt-labs/dbt-core/issues/1869), [#1872](https://github.com/dbt-labs/dbt-core/pull/1872))

### BigQuery

- Support KMS Encryption in BigQuery model configs ([#1829](https://github.com/dbt-labs/dbt-core/issues/1829), [#1851](https://github.com/dbt-labs/dbt-core/issues/1829)) ([docs](https://docs.getdbt.com/v0.15/docs/bigquery-configs#section-managing-kms-encryption))
- Improve docs generation speed by leveraging the information schema ([#1576](https://github.com/dbt-labs/dbt-core/issues/1576), [#1795](https://github.com/dbt-labs/dbt-core/pull/1795))
- Fix for cache errors on BigQuery when dataset names are capitalized ([#1810](https://github.com/dbt-labs/dbt-core/issues/1810), [#1881](https://github.com/dbt-labs/dbt-core/pull/1881))
- Fix for invalid query generation when multiple `options` are provided to a `create table|view` query ([#1786](https://github.com/dbt-labs/dbt-core/issues/1786), [#1787](https://github.com/dbt-labs/dbt-core/pull/1787))
- Use `client.delete_dataset` to drop BigQuery datasets atomically ([#1887](https://github.com/dbt-labs/dbt-core/issues/1887), [#1881](https://github.com/dbt-labs/dbt-core/pull/1881))

### Under the Hood

#### Dependencies

- Drop support for `networkx 1.x` ([#1577](https://github.com/dbt-labs/dbt-core/issues/1577), [#1814](https://github.com/dbt-labs/dbt-core/pull/1814))
- Upgrade `werkzeug` to 0.15.6 ([#1697](https://github.com/dbt-labs/dbt-core/issues/1697), [#1814](https://github.com/dbt-labs/dbt-core/pull/1814))
- Pin `psycopg2` dependency to 2.8.x to prevent segfaults ([#1221](https://github.com/dbt-labs/dbt-core/issues/1221), [#1898](https://github.com/dbt-labs/dbt-core/pull/1898))
- Set a strict upper bound for `jsonschema` dependency ([#1817](https://github.com/dbt-labs/dbt-core/issues/1817), [#1821](https://github.com/dbt-labs/dbt-core/pull/1821), [#1932](https://github.com/dbt-labs/dbt-core/pull/1932))

#### Everything else

- Provide test names and kwargs in the manifest ([#1154](https://github.com/dbt-labs/dbt-core/issues/1154), [#1816](https://github.com/dbt-labs/dbt-core/pull/1816))
- Replace JSON Schemas with data classes ([#1447](https://github.com/dbt-labs/dbt-core/issues/1447), [#1589](https://github.com/dbt-labs/dbt-core/pull/1589))
- Include test name and kwargs in test nodes in the manifest ([#1154](https://github.com/dbt-labs/dbt-core/issues/1154), [#1816](https://github.com/dbt-labs/dbt-core/pull/1816))
- Remove logic around handling `archive` blocks in the `dbt_project.yml` file ([#1580](https://github.com/dbt-labs/dbt-core/issues/1580), [#1581](https://github.com/dbt-labs/dbt-core/pull/1581))
- Remove the APIObject class ([#1762](https://github.com/dbt-labs/dbt-core/issues/1762), [#1780](https://github.com/dbt-labs/dbt-core/pull/1780))

## Contributors

Thanks all for your contributions to dbt! :tada:

- [@captainEli](https://github.com/captainEli) ([#1809](https://github.com/dbt-labs/dbt-core/pull/1809))
- [@clausherther](https://github.com/clausherther) ([#1876](https://github.com/dbt-labs/dbt-core/pull/1876))
- [@jtcohen6](https://github.com/jtcohen6) ([#1784](https://github.com/dbt-labs/dbt-core/pull/1784))
- [@tbescherer](https://github.com/tbescherer) ([#1515](https://github.com/dbt-labs/dbt-core/pull/1515))
- [@aminamos](https://github.com/aminamos) ([#1609](https://github.com/dbt-labs/dbt-core/pull/1609))
- [@JusLarsen](https://github.com/JusLarsen) ([#1903](https://github.com/dbt-labs/dbt-core/pull/1903))
- [@heisencoder](https://github.com/heisencoder) ([#1823](https://github.com/dbt-labs/dbt-core/pull/1823))
- [@tjengel](https://github.com/tjengel) ([#1885](https://github.com/dbt-labs/dbt-core/pull/1885))
- [@Carolus-Holman](https://github.com/tjengel) ([#1747](https://github.com/dbt-labs/dbt-core/pull/1747), [#1743](https://github.com/dbt-labs/dbt-core/pull/1743))
- [@kconvey](https://github.com/tjengel) ([#1851](https://github.com/dbt-labs/dbt-core/pull/1851))
- [@darrenhaken](https://github.com/darrenhaken) ([#1787](https://github.com/dbt-labs/dbt-core/pull/1787))

## dbt 0.14.4 (November 8, 2019)

This release changes the version ranges of some of dbt's dependencies. These changes address installation issues in 0.14.3 when dbt is installed from pip. You can view the full list of dependency version changes [in this commit](https://github.com/dbt-labs/dbt-core/commit/b4dd265cb433480a59bbd15d140d46ebf03644eb).

Note: If you are installing dbt into an environment alongside other Python libraries, you can install individual dbt plugins with:

```
pip install dbt-postgres
pip install dbt-redshift
pip install dbt-snowflake
pip install dbt-bigquery
```

Installing specific plugins may help mitigate issues regarding incompatible versions of dependencies between dbt and other libraries.

### Fixes:

- Fix dependency issues caused by a bad release of `snowflake-connector-python` ([#1892](https://github.com/dbt-labs/dbt-core/issues/1892), [#1895](https://github.com/dbt-labs/dbt-core/pull/1895/files))

## dbt 0.14.3 (October 10, 2019)

This is a bugfix release.

### Fixes:

- Fix for `dictionary changed size during iteration` race condition ([#1740](https://github.com/dbt-labs/dbt-core/issues/1740), [#1750](https://github.com/dbt-labs/dbt-core/pull/1750))
- Fix upper bound on jsonschema dependency to 3.1.1 ([#1817](https://github.com/dbt-labs/dbt-core/issues/1817), [#1819](https://github.com/dbt-labs/dbt-core/pull/1819))

### Under the hood:

- Provide a programmatic method for validating profile targets ([#1754](https://github.com/dbt-labs/dbt-core/issues/1754), [#1775](https://github.com/dbt-labs/dbt-core/pull/1775))

## dbt 0.14.2 (September 13, 2019)

### Overview

This is a bugfix release.

### Fixes:

- Fix for dbt hanging at the end of execution in `dbt source snapshot-freshness` tasks ([#1728](https://github.com/dbt-labs/dbt-core/issues/1728), [#1729](https://github.com/dbt-labs/dbt-core/pull/1729))
- Fix for broken "packages" and "tags" selector dropdowns in the dbt Documentation website ([docs#47](https://github.com/dbt-labs/dbt-docs/issues/47), [#1726](https://github.com/dbt-labs/dbt-core/pull/1726))

## dbt 0.14.1 (September 3, 2019)

### Overview

This is primarily a bugfix release which contains a few minor improvements too. Note: this release includes an important change in how the `check` snapshot strategy works. See [#1614](https://github.com/dbt-labs/dbt-core/pull/1614) for more information. If you are using snapshots with the `check` strategy on dbt v0.14.0, it is strongly recommended that you upgrade to 0.14.1 at your soonest convenience.

### Breaking changes

- The undocumented `macros` attribute was removed from the `graph` context variable ([#1615](https://github.com/dbt-labs/dbt-core/pull/1615))

### Features:

- Summarize warnings at the end of dbt runs ([#1597](https://github.com/dbt-labs/dbt-core/issues/1597), [#1654](https://github.com/dbt-labs/dbt-core/pull/1654))
- Speed up catalog generation on postgres by using avoiding use of the `information_schema` ([#1540](https://github.com/dbt-labs/dbt-core/pull/1540))
- Docs site updates ([#1621](https://github.com/dbt-labs/dbt-core/issues/1621))
  - Fix for incorrect node selection logic in DAG view ([docs#38](https://github.com/dbt-labs/dbt-docs/pull/38))
  - Update page title, meta tags, and favicon ([docs#39](https://github.com/dbt-labs/dbt-docs/pull/39))
  - Bump the version of `dbt-styleguide`, changing file tree colors from orange to black :)
- Add environment variables for macro debugging flags ([#1628](https://github.com/dbt-labs/dbt-core/issues/1628), [#1629](https://github.com/dbt-labs/dbt-core/pull/1629))
- Speed up node selection by making it linear, rather than quadratic, in complexity ([#1611](https://github.com/dbt-labs/dbt-core/issues/1611), [#1615](https://github.com/dbt-labs/dbt-core/pull/1615))
- Specify the `application` field in Snowflake connections ([#1622](https://github.com/dbt-labs/dbt-core/issues/1622), [#1623](https://github.com/dbt-labs/dbt-core/pull/1623))
- Add support for clustering on Snowflake ([#634](https://github.com/dbt-labs/dbt-core/issues/634), [#1591](https://github.com/dbt-labs/dbt-core/pull/1591), [#1689](https://github.com/dbt-labs/dbt-core/pull/1689)) ([docs](https://docs.getdbt.com/docs/snowflake-configs#section-configuring-table-clustering))
- Add support for job priority on BigQuery ([#1456](https://github.com/dbt-labs/dbt-core/issues/1456), [#1673](https://github.com/dbt-labs/dbt-core/pull/1673)) ([docs](https://docs.getdbt.com/docs/profile-bigquery#section-priority))
- Add `node.config` and `node.tags` to the `generate_schema_name` and `generate_alias_name` macro context ([#1700](https://github.com/dbt-labs/dbt-core/issues/1700), [#1701](https://github.com/dbt-labs/dbt-core/pull/1701))

### Fixes:

- Fix for reused `check_cols` values in snapshots ([#1614](https://github.com/dbt-labs/dbt-core/pull/1614), [#1709](https://github.com/dbt-labs/dbt-core/pull/1709))
- Fix for rendering column descriptions in sources ([#1619](https://github.com/dbt-labs/dbt-core/issues/1619), [#1633](https://github.com/dbt-labs/dbt-core/pull/1633))
- Fix for `is_incremental()` returning True for models that are not materialized as incremental models ([#1249](https://github.com/dbt-labs/dbt-core/issues/1249), [#1608](https://github.com/dbt-labs/dbt-core/pull/1608))
- Fix for serialization of BigQuery results which contain nested or repeated records ([#1626](https://github.com/dbt-labs/dbt-core/issues/1626), [#1638](https://github.com/dbt-labs/dbt-core/pull/1638))
- Fix for loading seed files which contain non-ascii characters ([#1632](https://github.com/dbt-labs/dbt-core/issues/1632), [#1644](https://github.com/dbt-labs/dbt-core/pull/1644))
- Fix for creation of user cookies in incorrect directories when `--profile-dir` or `$DBT_PROFILES_DIR` is provided ([#1645](https://github.com/dbt-labs/dbt-core/issues/1645), [#1656](https://github.com/dbt-labs/dbt-core/pull/1656))
- Fix for error handling when transactions are being rolled back ([#1647](https://github.com/dbt-labs/dbt-core/pull/1647))
- Fix for incorrect references to `dbt.exceptions` in jinja code ([#1569](https://github.com/dbt-labs/dbt-core/issues/1569), [#1609](https://github.com/dbt-labs/dbt-core/pull/1609))
- Fix for duplicated schema creation due to case-sensitive comparison ([#1651](https://github.com/dbt-labs/dbt-core/issues/1651), [#1663](https://github.com/dbt-labs/dbt-core/pull/1663))
- Fix for "schema stub" created automatically by dbt ([#913](https://github.com/dbt-labs/dbt-core/issues/913), [#1663](https://github.com/dbt-labs/dbt-core/pull/1663))
- Fix for incremental merge query on old versions of postgres (<=9.6) ([#1665](https://github.com/dbt-labs/dbt-core/issues/1665), [#1666](https://github.com/dbt-labs/dbt-core/pull/1666))
- Fix for serializing results of queries which return `TIMESTAMP_TZ` columns on Snowflake in the RPC server ([#1670](https://github.com/dbt-labs/dbt-core/pull/1670))
- Fix typo in InternalException ([#1640](https://github.com/dbt-labs/dbt-core/issues/1640), [#1672](https://github.com/dbt-labs/dbt-core/pull/1672))
- Fix typo in CLI help for snapshot migration subcommand ([#1664](https://github.com/dbt-labs/dbt-core/pull/1664))
- Fix for error handling logic when empty queries are submitted on Snowflake ([#1693](https://github.com/dbt-labs/dbt-core/issues/1693), [#1694](https://github.com/dbt-labs/dbt-core/pull/1694))
- Fix for non-atomic column expansion logic in Snowflake incremental models and snapshots ([#1687](https://github.com/dbt-labs/dbt-core/issues/1687), [#1690](https://github.com/dbt-labs/dbt-core/pull/1690))
- Fix for unprojected `count(*)` expression injected by custom data tests ([#1688](https://github.com/dbt-labs/dbt-core/pull/1688))
- Fix for `dbt run` and `dbt docs generate` commands when running against Panoply Redshift ([#1479](https://github.com/dbt-labs/dbt-core/issues/1479), [#1686](https://github.com/dbt-labs/dbt-core/pull/1686))

### Contributors:

Thanks for your contributions to dbt!

- [@levimalott](https://github.com/levimalott) ([#1647](https://github.com/dbt-labs/dbt-core/pull/1647))
- [@aminamos](https://github.com/aminamos) ([#1609](https://github.com/dbt-labs/dbt-core/pull/1609))
- [@elexisvenator](https://github.com/elexisvenator) ([#1540](https://github.com/dbt-labs/dbt-core/pull/1540))
- [@edmundyan](https://github.com/edmundyan) ([#1663](https://github.com/dbt-labs/dbt-core/pull/1663))
- [@vitorbaptista](https://github.com/vitorbaptista) ([#1664](https://github.com/dbt-labs/dbt-core/pull/1664))
- [@sjwhitworth](https://github.com/sjwhitworth) ([#1672](https://github.com/dbt-labs/dbt-core/pull/1672), [#1673](https://github.com/dbt-labs/dbt-core/pull/1673))
- [@mikaelene](https://github.com/mikaelene) ([#1688](https://github.com/dbt-labs/dbt-core/pull/1688), [#1709](https://github.com/dbt-labs/dbt-core/pull/1709))
- [@bastienboutonnet](https://github.com/bastienboutonnet) ([#1591](https://github.com/dbt-labs/dbt-core/pull/1591), [#1689](https://github.com/dbt-labs/dbt-core/pull/1689))

## dbt 0.14.0 - Wilt Chamberlain (July 10, 2019)

### Overview

- Replace Archives with Snapshots ([docs](https://docs.getdbt.com/v0.14/docs/snapshots), [migration guide](https://docs.getdbt.com/v0.14/docs/upgrading-to-014))
- Add three new top-level commands:
  - `dbt ls` ([docs](https://docs.getdbt.com/v0.14/docs/list))
  - `dbt run-operation` ([docs](https://docs.getdbt.com/v0.14/docs/run-operation))
  - `dbt rpc` ([docs](https://docs.getdbt.com/v0.14/docs/rpc))
- Support the specification of severity levels for schema and data tests ([docs](https://docs.getdbt.com/v0.14/docs/testing#section-test-severity))
- Many new quality of life improvements and bugfixes

### Breaking changes

- Stub out adapter methods at parse-time to speed up parsing ([#1413](https://github.com/dbt-labs/dbt-core/pull/1413))
- Removed support for the `--non-destructive` flag ([#1419](https://github.com/dbt-labs/dbt-core/pull/1419), [#1415](https://github.com/dbt-labs/dbt-core/issues/1415))
- Removed support for the `sql_where` config to incremental models ([#1408](https://github.com/dbt-labs/dbt-core/pull/1408), [#1351](https://github.com/dbt-labs/dbt-core/issues/1351))
- Changed `expand_target_column_types` to take a Relation instead of a string ([#1478](https://github.com/dbt-labs/dbt-core/pull/1478))
- Replaced Archives with Snapshots
  - Normalized meta-column names in Snapshot tables ([#1361](https://github.com/dbt-labs/dbt-core/pull/1361), [#251](https://github.com/dbt-labs/dbt-core/issues/251))

### Features

- Add `run-operation` command which invokes macros directly from the CLI ([#1328](https://github.com/dbt-labs/dbt-core/pull/1328)) ([docs](https://docs.getdbt.com/v0.14/docs/run-operation))
- Add a `dbt ls` command which lists resources in your project ([#1436](https://github.com/dbt-labs/dbt-core/pull/1436), [#467](https://github.com/dbt-labs/dbt-core/issues/467)) ([docs](https://docs.getdbt.com/v0.14/docs/list))
- Add Snapshots, an improvement over Archives ([#1361](https://github.com/dbt-labs/dbt-core/pull/1361), [#1175](https://github.com/dbt-labs/dbt-core/issues/1175)) ([docs](https://docs.getdbt.com/v0.14/docs/snapshots))
  - Add the 'check' snapshot strategy ([#1361](https://github.com/dbt-labs/dbt-core/pull/1361), [#706](https://github.com/dbt-labs/dbt-core/issues/706))
  - Support Snapshots across logical databases ([#1455](https://github.com/dbt-labs/dbt-core/issues/1455))
  - Implement Snapshots using a merge statement where supported ([#1478](https://github.com/dbt-labs/dbt-core/pull/1478))
  - Support Snapshot selection using `--select` ([#1520](https://github.com/dbt-labs/dbt-core/pull/1520), [#1512](https://github.com/dbt-labs/dbt-core/issues/1512))
- Add an RPC server via `dbt rpc` ([#1301](https://github.com/dbt-labs/dbt-core/pull/1301), [#1274](https://github.com/dbt-labs/dbt-core/issues/1274)) ([docs](https://docs.getdbt.com/v0.14/docs/rpc))
  - Add `ps` and `kill` commands to the rpc server ([#1380](https://github.com/dbt-labs/dbt-core/pull/1380/), [#1369](https://github.com/dbt-labs/dbt-core/issues/1369), [#1370](https://github.com/dbt-labs/dbt-core/issues/1370))
  - Add support for ephemeral nodes to the rpc server ([#1373](https://github.com/dbt-labs/dbt-core/pull/1373), [#1368](https://github.com/dbt-labs/dbt-core/issues/1368))
  - Add support for inline macros to the rpc server ([#1375](https://github.com/dbt-labs/dbt-core/pull/1375), [#1372](https://github.com/dbt-labs/dbt-core/issues/1372), [#1348](https://github.com/dbt-labs/dbt-core/pull/1348))
  - Improve error handling in the rpc server ([#1341](https://github.com/dbt-labs/dbt-core/pull/1341), [#1309](https://github.com/dbt-labs/dbt-core/issues/1309), [#1310](https://github.com/dbt-labs/dbt-core/issues/1310))
- Made printer width configurable ([#1026](https://github.com/dbt-labs/dbt-core/issues/1026), [#1247](https://github.com/dbt-labs/dbt-core/pull/1247)) ([docs](https://docs.getdbt.com/v0.14/docs/configure-your-profile#section-additional-profile-configurations))
- Retry package downloads from the hub.getdbt.com ([#1451](https://github.com/dbt-labs/dbt-core/issues/1451), [#1491](https://github.com/dbt-labs/dbt-core/pull/1491))
- Add a test "severity" level, presented as a keyword argument to schema tests ([#1410](https://github.com/dbt-labs/dbt-core/pull/1410), [#1005](https://github.com/dbt-labs/dbt-core/issues/1005)) ([docs](https://docs.getdbt.com/v0.14/docs/testing#section-test-severity))
- Add a `generate_alias_name` macro to configure alias names dynamically ([#1363](https://github.com/dbt-labs/dbt-core/pull/1363)) ([docs](https://docs.getdbt.com/v0.14/docs/using-custom-aliases#section-generate_alias_name))
- Add a `node` argument to `generate_schema_name` to configure schema names dynamically ([#1483](https://github.com/dbt-labs/dbt-core/pull/1483), [#1463](https://github.com/dbt-labs/dbt-core/issues/1463)) ([docs](https://docs.getdbt.com/v0.14/docs/using-custom-schemas#section-generate_schema_name-arguments))
- Use `create or replace` on Snowflake to rebuild tables and views atomically ([#1101](https://github.com/dbt-labs/dbt-core/issues/1101), [#1409](https://github.com/dbt-labs/dbt-core/pull/1409))
- Use `merge` statement for incremental models on Snowflake ([#1414](https://github.com/dbt-labs/dbt-core/issues/1414), [#1307](https://github.com/dbt-labs/dbt-core/pull/1307), [#1409](https://github.com/dbt-labs/dbt-core/pull/1409)) ([docs](https://docs.getdbt.com/v0.14/docs/snowflake-configs#section-merge-behavior-incremental-models-))
- Add support seed CSV files that start with a UTF-8 Byte Order Mark (BOM) ([#1452](https://github.com/dbt-labs/dbt-core/pull/1452), [#1177](https://github.com/dbt-labs/dbt-core/issues/1177))
- Add a warning when git packages are not pinned to a version ([#1453](https://github.com/dbt-labs/dbt-core/pull/1453), [#1446](https://github.com/dbt-labs/dbt-core/issues/1446))
- Add logging for `on-run-start` and `on-run-end hooks` to console output ([#1440](https://github.com/dbt-labs/dbt-core/pull/1440), [#696](https://github.com/dbt-labs/dbt-core/issues/696))
- Add modules and tracking information to the rendering context for configuration files ([#1441](https://github.com/dbt-labs/dbt-core/pull/1441), [#1320](https://github.com/dbt-labs/dbt-core/issues/1320))
- Add support for `null` vars, and distinguish `null` vars from unset vars ([#1426](https://github.com/dbt-labs/dbt-core/pull/1426), [#608](https://github.com/dbt-labs/dbt-core/issues/608))
- Add support for the `search_path` configuration in Postgres/Redshift profiles ([#1477](https://github.com/dbt-labs/dbt-core/issues/1477), [#1476](https://github.com/dbt-labs/dbt-core/pull/1476)) ([docs (postgres)](https://docs.getdbt.com/v0.14/docs/profile-postgres), [docs (redshift)](https://docs.getdbt.com/v0.14/docs/profile-redshift))
- Add support for persisting documentation as `descriptions` for tables and views on BigQuery ([#1031](https://github.com/dbt-labs/dbt-core/issues/1031), [#1285](https://github.com/dbt-labs/dbt-core/pull/1285)) ([docs](https://docs.getdbt.com/v0.14/docs/bigquery-configs#section-persisting-model-descriptions))
- Add a `--project-dir` path which will invoke dbt in the specified directory ([#1549](https://github.com/dbt-labs/dbt-core/pull/1549), [#1544](https://github.com/dbt-labs/dbt-core/issues/1544))

### dbt docs Changes

- Add searching by tag name ([#32](https://github.com/dbt-labs/dbt-docs/pull/32))
- Add context menu link to export graph viz as a PNG ([#34](https://github.com/dbt-labs/dbt-docs/pull/34))
- Fix for clicking models in left-nav while search results are open ([#31](https://github.com/dbt-labs/dbt-docs/pull/31))

### Fixes

- Fix for unduly long timeouts when anonymous event tracking is blocked ([#1445](https://github.com/dbt-labs/dbt-core/pull/1445), [#1063](https://github.com/dbt-labs/dbt-core/issues/1063))
- Fix for error with mostly-duplicate git urls in packages, picking the one that came first. ([#1428](https://github.com/dbt-labs/dbt-core/pull/1428), [#1084](https://github.com/dbt-labs/dbt-core/issues/1084))
- Fix for unrendered `description` field as jinja in top-level Source specification ([#1484](https://github.com/dbt-labs/dbt-core/issues/1484), [#1494](https://github.com/dbt-labs/dbt-core/issues/1494))
- Fix for API error when very large temp tables are created in BigQuery ([#1423](https://github.com/dbt-labs/dbt-core/issues/1423), [#1478](https://github.com/dbt-labs/dbt-core/pull/1478))
- Fix for compiler errors that occurred if jinja code was present outside of a docs blocks in .md files ([#1513](https://github.com/dbt-labs/dbt-core/pull/1513), [#988](https://github.com/dbt-labs/dbt-core/issues/988))
- Fix `TEXT` handling on postgres and redshift ([#1420](https://github.com/dbt-labs/dbt-core/pull/1420), [#781](https://github.com/dbt-labs/dbt-core/issues/781))
- Fix for compiler error when vars are undefined but only used in disabled models ([#1429](https://github.com/dbt-labs/dbt-core/pull/1429), [#434](https://github.com/dbt-labs/dbt-core/issues/434))
- Improved the error message when iterating over the results of a macro that doesn't exist ([#1425](https://github.com/dbt-labs/dbt-core/pull/1425), [#1424](https://github.com/dbt-labs/dbt-core/issues/1424))
- Improved the error message when tests have invalid parameter definitions ([#1427](https://github.com/dbt-labs/dbt-core/pull/1427), [#1325](https://github.com/dbt-labs/dbt-core/issues/1325))
- Improved the error message when a user tries to archive a non-existent table ([#1361](https://github.com/dbt-labs/dbt-core/pull/1361), [#1066](https://github.com/dbt-labs/dbt-core/issues/1066))
- Fix for archive logic which tried to create already-existing destination schemas ([#1398](https://github.com/dbt-labs/dbt-core/pull/1398), [#758](https://github.com/dbt-labs/dbt-core/issues/758))
- Fix for incorrect error codes when Operations exit with an error ([#1406](https://github.com/dbt-labs/dbt-core/pull/1406), [#1377](https://github.com/dbt-labs/dbt-core/issues/1377))
- Fix for missing compiled SQL when the rpc server encounters a database error ([#1381](https://github.com/dbt-labs/dbt-core/pull/1381), [#1371](https://github.com/dbt-labs/dbt-core/issues/1371))
- Fix for broken link in the profile.yml generated by `dbt init` ([#1366](https://github.com/dbt-labs/dbt-core/pull/1366), [#1344](https://github.com/dbt-labs/dbt-core/issues/1344))
- Fix the sample test.env file's redshift password field ([#1364](https://github.com/dbt-labs/dbt-core/pull/1364))
- Fix collisions on models running concurrently that have duplicate names but have distinguishing aliases ([#1342](https://github.com/dbt-labs/dbt-core/pull/1342), [#1321](https://github.com/dbt-labs/dbt-core/issues/1321))
- Fix for a bad error message when a `version` is missing from a package spec in `packages.yml` ([#1551](https://github.com/dbt-labs/dbt-core/pull/1551), [#1546](https://github.com/dbt-labs/dbt-core/issues/1546))
- Fix for wrong package scope when the two-arg method of `ref` is used ([#1515](https://github.com/dbt-labs/dbt-core/pull/1515), [#1504](https://github.com/dbt-labs/dbt-core/issues/1504))
- Fix missing import in test suite ([#1572](https://github.com/dbt-labs/dbt-core/pull/1572))
- Fix for a Snowflake error when an external table exists in a schema that dbt operates on ([#1571](https://github.com/dbt-labs/dbt-core/pull/1571), [#1505](https://github.com/dbt-labs/dbt-core/issues/1505))

### Under the hood

- Use pytest for tests ([#1417](https://github.com/dbt-labs/dbt-core/pull/1417))
- Use flake8 for linting ([#1361](https://github.com/dbt-labs/dbt-core/pull/1361), [#1333](https://github.com/dbt-labs/dbt-core/issues/1333))
- Added a flag for wrapping models and tests in jinja blocks ([#1407](https://github.com/dbt-labs/dbt-core/pull/1407), [#1400](https://github.com/dbt-labs/dbt-core/issues/1400))
- Connection management: Bind connections threads rather than to names ([#1336](https://github.com/dbt-labs/dbt-core/pull/1336), [#1312](https://github.com/dbt-labs/dbt-core/issues/1312))
- Add deprecation warning for dbt users on Python2 ([#1534](https://github.com/dbt-labs/dbt-core/pull/1534), [#1531](https://github.com/dbt-labs/dbt-core/issues/1531))
- Upgrade networkx to v2.x ([#1509](https://github.com/dbt-labs/dbt-core/pull/1509), [#1496](https://github.com/dbt-labs/dbt-core/issues/1496))
- Anonymously track adapter type and rpc requests when tracking is enabled ([#1575](https://github.com/dbt-labs/dbt-core/pull/1575), [#1574](https://github.com/dbt-labs/dbt-core/issues/1574))
- Fix for test warnings and general test suite cleanup ([#1578](https://github.com/dbt-labs/dbt-core/pull/1578))

### Contributors:

Over a dozen contributors wrote code for this release of dbt! Thanks for taking the time, and nice work y'all! :)

- [@nydnarb](https://github.com/nydnarb) ([#1363](https://github.com/dbt-labs/dbt-core/issues/1363))
- [@emilieschario](https://github.com/emilieschario) ([#1366](https://github.com/dbt-labs/dbt-core/pull/1366))
- [@bastienboutonnet](https://github.com/bastienboutonnet) ([#1409](https://github.com/dbt-labs/dbt-core/pull/1409))
- [@kasanchez](https://github.com/kasanchez) ([#1247](https://github.com/dbt-labs/dbt-core/pull/1247))
- [@Blakewell](https://github.com/Blakewell) ([#1307](https://github.com/dbt-labs/dbt-core/pull/1307))
- [@buremba](https://github.com/buremba) ([#1476](https://github.com/dbt-labs/dbt-core/pull/1476))
- [@darrenhaken](https://github.com/darrenhaken) ([#1285](https://github.com/dbt-labs/dbt-core/pull/1285))
- [@tbescherer](https://github.com/tbescherer) ([#1504](https://github.com/dbt-labs/dbt-core/issues/1504))
- [@heisencoder](https://github.com/heisencoder) ([#1509](https://github.com/dbt-labs/dbt-core/pull/1509), [#1549](https://github.com/dbt-labs/dbt-core/pull/1549). [#1578](https://github.com/dbt-labs/dbt-core/pull/1578))
- [@cclauss](https://github.com/cclauss) ([#1572](https://github.com/dbt-labs/dbt-core/pull/1572))
- [@josegalarza](https://github.com/josegalarza) ([#1571](https://github.com/dbt-labs/dbt-core/pull/1571))
- [@rmgpinto](https://github.com/rmgpinto) ([docs#31](https://github.com/dbt-labs/dbt-docs/pull/31), [docs#32](https://github.com/dbt-labs/dbt-docs/pull/32))
- [@groodt](https://github.com/groodt) ([docs#34](https://github.com/dbt-labs/dbt-docs/pull/34))
- [@dcereijodo](https://github.com/dcereijodo) ([#2341](https://github.com/dbt-labs/dbt-core/pull/2341))

## dbt 0.13.1 (May 13, 2019)

### Overview

This is a bugfix release.

### Bugfixes

- Add "MaterializedView" relation type to the Snowflake adapter ([#1430](https://github.com/dbt-labs/dbt-core/issues/1430), [#1432](https://github.com/dbt-labs/dbt-core/pull/1432)) ([@adriank-convoy](https://github.com/adriank-convoy))
- Quote databases properly ([#1396](https://github.com/dbt-labs/dbt-core/issues/1396), [#1402](https://github.com/dbt-labs/dbt-core/pull/1402))
- Use "ilike" instead of "=" for database equality when listing schemas ([#1411](https://github.com/dbt-labs/dbt-core/issues/1411), [#1412](https://github.com/dbt-labs/dbt-core/pull/1412))
- Pass the model name along in get_relations ([#1384](https://github.com/dbt-labs/dbt-core/issues/1384), [#1388](https://github.com/dbt-labs/dbt-core/pull/1388))
- Add logging to dbt clean ([#1261](https://github.com/dbt-labs/dbt-core/issues/1261), [#1383](https://github.com/dbt-labs/dbt-core/pull/1383), [#1391](https://github.com/dbt-labs/dbt-core/pull/1391)) ([@emilieschario](https://github.com/emilieschario))

### dbt Docs

- Search by columns ([dbt-docs#23](https://github.com/dbt-labs/dbt-docs/pull/23)) ([rmgpinto](https://github.com/rmgpinto))
- Support @ selector ([dbt-docs#27](https://github.com/dbt-labs/dbt-docs/pull/27))
- Fix number formatting on Snowflake and BQ in table stats ([dbt-docs#28](https://github.com/dbt-labs/dbt-docs/pull/28))

### Contributors:

Thanks for your contributions to dbt!

- [@emilieschario](https://github.com/emilieschario)
- [@adriank-convoy](https://github.com/adriank-convoy)
- [@rmgpinto](https://github.com/rmgpinto)

## dbt 0.13.0 - Stephen Girard (March 21, 2019)

### Overview

This release provides [a stable API for building new adapters](https://docs.getdbt.com/v0.13/docs/building-a-new-adapter) and reimplements dbt's adapters as "plugins". Additionally, a new adapter for [Presto](https://github.com/dbt-labs/dbt-presto) was added using this architecture. Beyond adapters, this release of dbt also includes [Sources](https://docs.getdbt.com/v0.13/docs/using-sources) which can be used to document and test source data tables. See the full list of features added in 0.13.0 below.

### Breaking Changes

- version 1 schema.yml specs are no longer implemented. Please use the version 2 spec instead ([migration guide](https://docs.getdbt.com/docs/upgrading-from-0-10-to-0-11#section-schema-yml-v2-syntax))
- `{{this}}` is no longer implemented for `on-run-start` and `on-run-end` hooks. Use `{{ target }}` or an [`on-run-end` context variable](https://docs.getdbt.com/docs/on-run-end-context#section-schemas) instead ([#1176](https://github.com/dbt-labs/dbt-core/pull/1176), implementing [#878](https://github.com/dbt-labs/dbt-core/issues/878))
- A number of materialization-specific adapter methods have changed in breaking ways. If you use these adapter methods in your macros or materializations, you may need to update your code accordingly.
  - query_for_existing - **removed**, use [get_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-relation) instead.
  - [get_missing_columns](https://docs.getdbt.com/v0.13/reference#adapter-get-missing-columns) - changed to take `Relation`s instead of schemas and identifiers
  - [expand_target_column_types](https://docs.getdbt.com/v0.13/reference#adapter-expand-target-column-types) - changed to take a `Relation` instead of schema, identifier
  - [get_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-relation) - added a `database` argument
  - [create_schema](https://docs.getdbt.com/v0.13/reference#adapter-create-schema) - added a `database` argument
  - [drop_schema](https://docs.getdbt.com/v0.13/reference#adapter-drop-schema) - added a `database` argument

### Deprecations

- The following adapter methods are now deprecated, and will be removed in a future release:
  - get_columns_in_table - deprecated in favor of [get_columns_in_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-columns-in-relation)
  - already_exists - deprecated in favor of [get_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-relation)

### Features

- Add `source`s to dbt, use them to calculate source data freshness ([docs](https://docs.getdbt.com/v0.13/docs/using-sources) ) ([#814](https://github.com/dbt-labs/dbt-core/issues/814), [#1240](https://github.com/dbt-labs/dbt-core/issues/1240))
- Add support for Presto ([docs](https://docs.getdbt.com/v0.13/docs/profile-presto), [repo](https://github.com/dbt-labs/dbt-presto)) ([#1106](https://github.com/dbt-labs/dbt-core/issues/1106))
- Add `require-dbt-version` option to `dbt_project.yml` to state the supported versions of dbt for packages ([docs](https://docs.getdbt.com/v0.13/docs/requiring-dbt-versions)) ([#581](https://github.com/dbt-labs/dbt-core/issues/581))
- Add an output line indicating the installed version of dbt to every run ([#1134](https://github.com/dbt-labs/dbt-core/issues/1134))
- Add a new model selector (`@`) which build models, their children, and their children's parents ([docs](https://docs.getdbt.com/v0.13/reference#section-the-at-operator)) ([#1156](https://github.com/dbt-labs/dbt-core/issues/1156))
- Add support for Snowflake Key Pair Authentication ([docs](https://docs.getdbt.com/v0.13/docs/profile-snowflake#section-key-pair-authentication)) ([#1232](https://github.com/dbt-labs/dbt-core/pull/1232))
- Support SSO Authentication for Snowflake ([docs](https://docs.getdbt.com/v0.13/docs/profile-snowflake#section-sso-authentication)) ([#1172](https://github.com/dbt-labs/dbt-core/issues/1172))
- Add support for Snowflake's transient tables ([docs](https://docs.getdbt.com/v0.13/docs/snowflake-configs#section-transient-tables)) ([#946](https://github.com/dbt-labs/dbt-core/issues/946))
- Capture build timing data in `run_results.json` to visualize project performance ([#1179](https://github.com/dbt-labs/dbt-core/issues/1179))
- Add CLI flag to toggle warnings as errors ([docs](https://docs.getdbt.com/v0.13/reference#section-treat-warnings-as-errors)) ([#1243](https://github.com/dbt-labs/dbt-core/issues/1243))
- Add tab completion script for Bash ([docs](https://github.com/dbt-labs/dbt-completion.bash)) ([#1197](https://github.com/dbt-labs/dbt-core/issues/1197))
- Added docs on how to build a new adapter ([docs](https://docs.getdbt.com/v0.13/docs/building-a-new-adapter)) ([#560](https://github.com/dbt-labs/dbt-core/issues/560))
- Use new logo ([#1349](https://github.com/dbt-labs/dbt-core/pull/1349))

### Fixes

- Fix for Postgres character columns treated as string types ([#1194](https://github.com/dbt-labs/dbt-core/issues/1194))
- Fix for hard to reach edge case in which dbt could hang ([#1223](https://github.com/dbt-labs/dbt-core/issues/1223))
- Fix for `dbt deps` in non-English shells ([#1222](https://github.com/dbt-labs/dbt-core/issues/1222))
- Fix for over eager schema creation when models are run with `--models` ([#1239](https://github.com/dbt-labs/dbt-core/issues/1239))
- Fix for `dbt seed --show` ([#1288](https://github.com/dbt-labs/dbt-core/issues/1288))
- Fix for `is_incremental()` which should only return `True` if the target relation is a `table` ([#1292](https://github.com/dbt-labs/dbt-core/issues/1292))
- Fix for error in Snowflake table materializations with custom schemas ([#1316](https://github.com/dbt-labs/dbt-core/issues/1316))
- Fix errored out concurrent transactions on Redshift and Postgres ([#1356](https://github.com/dbt-labs/dbt-core/pull/1356))
- Fix out of order execution on model select ([#1354](https://github.com/dbt-labs/dbt-core/issues/1354), [#1355](https://github.com/dbt-labs/dbt-core/pull/1355))
- Fix adapter macro namespace issue ([#1352](https://github.com/dbt-labs/dbt-core/issues/1352), [#1353](https://github.com/dbt-labs/dbt-core/pull/1353))
- Re-add CLI flag to toggle warnings as errors ([#1347](https://github.com/dbt-labs/dbt-core/pull/1347))
- Fix release candidate regression that runs run hooks on test invocations ([#1346](https://github.com/dbt-labs/dbt-core/pull/1346))
- Fix Snowflake source quoting ([#1338](https://github.com/dbt-labs/dbt-core/pull/1338), [#1317](https://github.com/dbt-labs/dbt-core/issues/1317), [#1332](https://github.com/dbt-labs/dbt-core/issues/1332))
- Handle unexpected max_loaded_at types ([#1330](https://github.com/dbt-labs/dbt-core/pull/1330))

### Under the hood

- Replace all SQL in Python code with Jinja in macros ([#1204](https://github.com/dbt-labs/dbt-core/issues/1204))
- Loosen restrictions of boto3 dependency ([#1234](https://github.com/dbt-labs/dbt-core/issues/1234))
- Rewrote Postgres introspective queries to be faster on large databases ([#1192](https://github.com/dbt-labs/dbt-core/issues/1192)

### Contributors:

Thanks for your contributions to dbt!

- [@patrickgoss](https://github.com/patrickgoss) [#1193](https://github.com/dbt-labs/dbt-core/issues/1193)
- [@brianhartsock](https://github.com/brianhartsock) [#1191](https://github.com/dbt-labs/dbt-core/pull/1191)
- [@alexyer](https://github.com/alexyer) [#1232](https://github.com/dbt-labs/dbt-core/pull/1232)
- [@adriank-convoy](https://github.com/adriank-convoy) [#1224](https://github.com/dbt-labs/dbt-core/pull/1224)
- [@mikekaminsky](https://github.com/mikekaminsky) [#1216](https://github.com/dbt-labs/dbt-core/pull/1216)
- [@vijaykiran](https://github.com/vijaykiran) [#1198](https://github.com/dbt-labs/dbt-core/pull/1198), [#1199](https://github.com/dbt-labs/dbt-core/pull/1199)

## dbt 0.12.2 - Grace Kelly (January 8, 2019)

### Overview

This release reduces the runtime of dbt projects by improving dbt's approach to model running. Additionally, a number of workflow improvements have been added.

### Deprecations

- Deprecate `sql_where` ([#744](https://github.com/dbt-labs/dbt-core/issues/744)) ([docs](https://docs.getdbt.com/v0.12/docs/configuring-incremental-models))

### Features

- More intelligently order and execute nodes in the graph. This _significantly_ speeds up the runtime of most dbt projects ([#813](https://github.com/dbt-labs/dbt-core/issues/813))
- Add `-m` flag as an alias for `--models` ([#1160](https://github.com/dbt-labs/dbt-core/issues/1160))
- Add `post_hook` and `pre_hook` as aliases for `post-hook` and `pre-hook`, respectively ([#1124](https://github.com/dbt-labs/dbt-core/issues/1124)) ([docs](https://docs.getdbt.com/v0.12/docs/using-hooks))
- Better handling of git errors in `dbt deps` + full support for Windows ([#994](https://github.com/dbt-labs/dbt-core/issues/994), [#778](https://github.com/dbt-labs/dbt-core/issues/778), [#895](https://github.com/dbt-labs/dbt-core/issues/895))
- Add support for specifying a `location` in BigQuery datasets ([#969](https://github.com/dbt-labs/dbt-core/issues/969)) ([docs](https://docs.getdbt.com/v0.12/docs/supported-databases#section-dataset-locations))
- Add support for Jinja expressions using the `{% do ... %}` block ([#1113](https://github.com/dbt-labs/dbt-core/issues/1113))
- The `dbt debug` command is actually useful now ([#1061](https://github.com/dbt-labs/dbt-core/issues/1061))
- The `config` function can now be called multiple times in a model ([#558](https://github.com/dbt-labs/dbt-core/issues/558))
- Source the latest version of dbt from PyPi instead of GitHub ([#1122](https://github.com/dbt-labs/dbt-core/issues/1122))
- Add a peformance profiling mechnanism to dbt ([#1001](https://github.com/dbt-labs/dbt-core/issues/1001))
- Add caching for dbt's macros-only manifest to speedup parsing ([#1098](https://github.com/dbt-labs/dbt-core/issues/1098))

### Fixes

- Fix for custom schemas used alongside the `generate_schema_name` macro ([#801](https://github.com/dbt-labs/dbt-core/issues/801))
- Fix for silent failure of tests that reference nonexistent models ([#968](https://github.com/dbt-labs/dbt-core/issues/968))
- Fix for `generate_schema_name` macros that return whitespace-padded schema names ([#1074](https://github.com/dbt-labs/dbt-core/issues/1074))
- Fix for incorrect relation type for backup tables on Snowflake ([#1103](https://github.com/dbt-labs/dbt-core/issues/1103))
- Fix for incorrectly cased values in the relation cache ([#1140](https://github.com/dbt-labs/dbt-core/issues/1140))
- Fix for JSON decoding error on Python2 installed with Anaconda ([#1155](https://github.com/dbt-labs/dbt-core/issues/1155))
- Fix for unhandled exceptions that occur in anonymous event tracking ([#1180](https://github.com/dbt-labs/dbt-core/issues/1180))
- Fix for analysis files that contain `raw` tags ([#1152](https://github.com/dbt-labs/dbt-core/issues/1152))
- Fix for packages which reference the [hubsite](hub.getdbt.com) ([#1095](https://github.com/dbt-labs/dbt-core/issues/1095))

## dbt 0.12.1 - (November 15, 2018)

### Overview

This is a bugfix release.

### Fixes

- Fix for relation caching when views outside of a dbt schema depend on relations inside of a dbt schema ([#1119](https://github.com/dbt-labs/dbt-core/issues/1119))

## dbt 0.12.0 - Guion Bluford (November 12, 2018)

### Overview

This release adds caching for some introspective queries on all adapters. Additionally, custom tags can be supplied for models, along with many other minor improvements and bugfixes.

### Breaking Changes

- Support for the `repositories:` block in `dbt_project.yml` (deprecated in 0.10.0) was removed.

### tl;dr

- Make runs faster by caching introspective queries
- Support [model tags](https://docs.getdbt.com/v0.12/docs/tags)
- Add a list of [schemas](https://docs.getdbt.com/v0.12/reference#schemas) to the `on-run-end` context
- Set your [profiles directory](https://docs.getdbt.com/v0.12/docs/configure-your-profile#section-using-the-dbt_profiles_dir-environment-variable) with an environment variable

### Features

- Cache the existence of relations to speed up dbt runs ([#1025](https://github.com/dbt-labs/dbt-core/pull/1025))
- Add support for tag configuration and selection ([#1014](https://github.com/dbt-labs/dbt-core/pull/1014))
  - Add tags to the model and graph views in the docs UI ([#7](https://github.com/dbt-labs/dbt-docs/pull/7))
- Add the set of schemas that dbt built models into in the `on-run-end` hook context ([#908](https://github.com/dbt-labs/dbt-core/issues/908))
- Warn for unused resource config paths in dbt_project.yml ([#725](https://github.com/dbt-labs/dbt-core/pull/725))
- Add more information to the `dbt --help` output ([#1058](https://github.com/dbt-labs/dbt-core/issues/1058))
- Add support for configuring the profiles directory with an env var ([#1055](https://github.com/dbt-labs/dbt-core/issues/1055))
- Add support for cli and env vars in most `dbt_project.yml` and `profiles.yml` fields ([#1033](https://github.com/dbt-labs/dbt-core/pull/1033))
- Provide a better error message when seed file loading fails on BigQuery ([#1079](https://github.com/dbt-labs/dbt-core/pull/1079))
- Improved error handling and messaging on Redshift ([#997](https://github.com/dbt-labs/dbt-core/issues/997))
- Include datasets with underscores when listing BigQuery datasets ([#954](https://github.com/dbt-labs/dbt-core/pull/954))
- Forgo validating the user's profile for `dbt deps` and `dbt clean` commands ([#947](https://github.com/dbt-labs/dbt-core/issues/947), [#1022](https://github.com/dbt-labs/dbt-core/issues/1022))
- Don't read/parse CSV files outside of the `dbt seed` command ([#1046](https://github.com/dbt-labs/dbt-core/pull/1046))

### Fixes

- Fix for incorrect model selection with the `--models` CLI flag when projects and directories share the same name ([#1023](https://github.com/dbt-labs/dbt-core/issues/1023))
- Fix for table clustering configuration with multiple columns on BigQuery ([#1013](https://github.com/dbt-labs/dbt-core/issues/1013))
- Fix for incorrect output when a single row fails validation in `dbt test` ([#1040](https://github.com/dbt-labs/dbt-core/issues/1040))
- Fix for unwieldly Jinja errors regarding undefined variables at parse time ([#1086](https://github.com/dbt-labs/dbt-core/pull/1086), [#1080](https://github.com/dbt-labs/dbt-core/issues/1080), [#935](https://github.com/dbt-labs/dbt-core/issues/935))
- Fix for incremental models that have a line comment on the last line of the file ([#1018](https://github.com/dbt-labs/dbt-core/issues/1018))
- Fix for error messages when ephemeral models fail to compile ([#1053](https://github.com/dbt-labs/dbt-core/pull/1053))

### Under the hood

- Create adapters as singleton objects instead of classes ([#961](https://github.com/dbt-labs/dbt-core/issues/961))
- Combine project and profile into a single, coherent object ([#973](https://github.com/dbt-labs/dbt-core/pull/973))
- Investigate approaches for providing more complete compilation output ([#588](https://github.com/dbt-labs/dbt-core/issues/588))

### Contributors

Thanks for contributing!

- [@mikekaminsky](https://github.com/mikekaminsky) ([#1049](https://github.com/dbt-labs/dbt-core/pull/1049), [#1060](https://github.com/dbt-labs/dbt-core/pull/1060))
- [@joshtemple](https://github.com/joshtemple) ([#1079](https://github.com/dbt-labs/dbt-core/pull/1079))
- [@k4y3ff](https://github.com/k4y3ff) ([#954](https://github.com/dbt-labs/dbt-core/pull/954))
- [@elexisvenator](https://github.com/elexisvenator) ([#1019](https://github.com/dbt-labs/dbt-core/pull/1019))
- [@clrcrl](https://github.com/clrcrl) ([#725](https://github.com/dbt-labs/dbt-core/pull/725)

## dbt 0.11.1 - Lucretia Mott (September 18, 2018)

### Overview

This is a patch release containing a few bugfixes and one quality of life change for dbt docs.

### Features

- dbt
  - Add `--port` parameter to dbt docs serve ([#987](https://github.com/dbt-labs/dbt-core/pull/987))

### Fixes

- dbt
  - Fix hooks in model configs not running ([#985](https://github.com/dbt-labs/dbt-core/pull/985))
  - Fix integration test on redshift catalog generation ([#977](https://github.com/dbt-labs/dbt-core/pull/977))
  - Snowflake: Fix docs generation errors when QUOTED_IDENTIFIER_IGNORE_CASE is set ([#998](https://github.com/dbt-labs/dbt-core/pull/998))
  - Translate empty strings to null in seeds ([#995](https://github.com/dbt-labs/dbt-core/pull/995))
  - Filter out null schemas during catalog generation ([#992](https://github.com/dbt-labs/dbt-core/pull/992))
  - Fix quoting on drop, truncate, and rename ([#991](https://github.com/dbt-labs/dbt-core/pull/991))
- dbt-docs
  - Fix for non-existent column in schema.yml ([#3](https://github.com/dbt-labs/dbt-docs/pull/3))
  - Fixes for missing tests in docs UI when columns are upcased ([#2](https://github.com/dbt-labs/dbt-docs/pull/2))
  - Fix "copy to clipboard" ([#4](https://github.com/dbt-labs/dbt-docs/issues/4))

## dbt 0.11.0 - Isaac Asimov (September 6, 2018)

### Overview

This release adds support for auto-generated dbt documentation, adds a new syntax for `schema.yml` files, and fixes a number of minor bugs. With the exception of planned changes to Snowflake's default quoting strategy, this release should not contain any breaking changes. Check out the [blog post](https://blog.fishtownanalytics.com/using-dbt-docs-fae6137da3c3) for more information about this release.

### Breaking Changes

- Change default Snowflake quoting strategy to "unquoted" ([docs](https://docs.getdbt.com/v0.11/docs/configuring-quoting)) ([#824](https://github.com/dbt-labs/dbt-core/issues/824))

### Features

- Add autogenerated dbt project documentation ([docs](https://docs.getdbt.com/v0.11/docs/testing-and-documentation)) ([#375](https://github.com/dbt-labs/dbt-core/issues/375), [#863](https://github.com/dbt-labs/dbt-core/issues/863), [#941](https://github.com/dbt-labs/dbt-core/issues/941), [#815](https://github.com/dbt-labs/dbt-core/issues/815))
- Version 2 of schema.yml, which allows users to create table and column comments that end up in the manifest ([docs](https://docs.getdbt.com/v0.11/docs/schemayml-files)) ([#880](https://github.com/dbt-labs/dbt-core/pull/880))
- Extend catalog and manifest to also support Snowflake, BigQuery, and Redshift, in addition to existing Postgres support ([#866](https://github.com/dbt-labs/dbt-core/pull/866), [#857](https://github.com/dbt-labs/dbt-core/pull/857), [#849](https://github.com/dbt-labs/dbt-core/pull/849))
- Add a 'generated_at' field to both the manifest and the catalog. ([#887](https://github.com/dbt-labs/dbt-core/pull/877))
- Add `docs` blocks that users can put into `.md` files and `doc()` value for schema v2 description fields ([#888](https://github.com/dbt-labs/dbt-core/pull/888))
- Write out a 'run_results.json' after dbt invocations. ([#904](https://github.com/dbt-labs/dbt-core/pull/904))
- Type inference for interpreting CSV data is now less aggressive ([#905](https://github.com/dbt-labs/dbt-core/pull/905))
- Remove distinction between `this.table` and `this.schema` by refactoring materialization SQL ([#940](https://github.com/dbt-labs/dbt-core/pull/940))

### Fixes

- Fix for identifier clashes in BigQuery merge statements ([#914](https://github.com/dbt-labs/dbt-core/issues/914))
- Fix for unneccessary downloads of `bumpversion.cfg`, handle failures gracefully ([#907](https://github.com/dbt-labs/dbt-core/issues/907))
- Fix for incompatible `boto3` requirements ([#959](https://github.com/dbt-labs/dbt-core/issues/959))
- Fix for invalid `relationships` test when the parent column contains null values ([#921](https://github.com/dbt-labs/dbt-core/pull/921))

### Contributors

Thanks for contributing!

- [@rsmichaeldunn](https://github.com/rsmichaeldunn) ([#799](https://github.com/dbt-labs/dbt-core/pull/799))
- [@lewish](https://github.com/dbt-labs/dbt-core/pull/915) ([#915](https://github.com/dbt-labs/dbt-core/pull/915))
- [@MartinLue](https://github.com/MartinLue) ([#872](https://github.com/dbt-labs/dbt-core/pull/872))

## dbt 0.10.2 - Betsy Ross (August 3, 2018)

### Overview

This release makes it possible to alias relation names, rounds out support for BigQuery with incremental, archival, and hook support, adds the IAM Auth method for Redshift, and builds the foundation for autogenerated dbt project documentation, to come in the next release.

Additionally, a number of bugs have been fixed including intermittent BigQuery 404 errors, Redshift "table dropped by concurrent query" errors, and a probable fix for Redshift connection timeout issues.

### Contributors

We want to extend a big thank you to our outside contributors for this release! You all are amazing.

- [@danielchalef](https://github.com/danielchalef) ([#818](https://github.com/dbt-labs/dbt-core/pull/818))
- [@mjumbewu](https://github.com/mjumbewu) ([#796](https://github.com/dbt-labs/dbt-core/pull/796))
- [@abelsonlive](https://github.com/abelsonlive) ([#800](https://github.com/dbt-labs/dbt-core/pull/800))
- [@jon-rtr](https://github.com/jon-rtr) ([#800](https://github.com/dbt-labs/dbt-core/pull/800))
- [@mturzanska](https://github.com/mturzanska) ([#797](https://github.com/dbt-labs/dbt-core/pull/797))
- [@cpdean](https://github.com/cpdean) ([#780](https://github.com/dbt-labs/dbt-core/pull/780))

### Features

- BigQuery
  - Support incremental models ([#856](https://github.com/dbt-labs/dbt-core/pull/856)) ([docs](https://docs.getdbt.com/docs/configuring-models#section-configuring-incremental-models))
  - Support archival ([#856](https://github.com/dbt-labs/dbt-core/pull/856)) ([docs](https://docs.getdbt.com/docs/archival))
  - Add pre/post hook support ([#836](https://github.com/dbt-labs/dbt-core/pull/836)) ([docs](https://docs.getdbt.com/docs/using-hooks))
- Redshift: IAM Auth ([#818](https://github.com/dbt-labs/dbt-core/pull/818)) ([docs](https://docs.getdbt.com/docs/supported-databases#section-iam-authentication))
- Model aliases ([#800](https://github.com/dbt-labs/dbt-core/pull/800))([docs](https://docs.getdbt.com/docs/using-custom-aliases))
- Write JSON manifest file to disk during compilation ([#761](https://github.com/dbt-labs/dbt-core/pull/761))
- Add forward and backward graph edges to the JSON manifest file ([#762](https://github.com/dbt-labs/dbt-core/pull/762))
- Add a 'dbt docs generate' command to generate a JSON catalog file ([#774](https://github.com/dbt-labs/dbt-core/pull/774), [#808](https://github.com/dbt-labs/dbt-core/pull/808))

### Bugfixes

- BigQuery: fix concurrent relation loads ([#835](https://github.com/dbt-labs/dbt-core/pull/835))
- BigQuery: support external relations ([#828](https://github.com/dbt-labs/dbt-core/pull/828))
- Redshift: set TCP keepalive on connections ([#826](https://github.com/dbt-labs/dbt-core/pull/826))
- Redshift: fix "table dropped by concurrent query" ([#825](https://github.com/dbt-labs/dbt-core/pull/825))
- Fix the error handling for profiles.yml validation ([#820](https://github.com/dbt-labs/dbt-core/pull/820))
- Make the `--threads` parameter actually change the number of threads used ([#819](https://github.com/dbt-labs/dbt-core/pull/819))
- Ensure that numeric precision of a column is not `None` ([#796](https://github.com/dbt-labs/dbt-core/pull/796))
- Allow for more complex version comparison ([#797](https://github.com/dbt-labs/dbt-core/pull/797))

### Changes

- Use a subselect instead of CTE when building incremental models ([#787](https://github.com/dbt-labs/dbt-core/pull/787))
- Internals
  - Improved dependency selection, rip out some unused dependencies ([#848](https://github.com/dbt-labs/dbt-core/pull/848))
  - Stop tracking `run_error` in tracking code ([#817](https://github.com/dbt-labs/dbt-core/pull/817))
  - Use Mapping instead of dict as the base class for APIObject ([#756](https://github.com/dbt-labs/dbt-core/pull/756))
  - Split out parsers ([#809](https://github.com/dbt-labs/dbt-core/pull/809))
  - Fix `__all__` parameter in submodules ([#780](https://github.com/dbt-labs/dbt-core/pull/780))
  - Switch to CircleCI 2.0 ([#843](https://github.com/dbt-labs/dbt-core/pull/843), [#850](https://github.com/dbt-labs/dbt-core/pull/850))
  - Added tox environments that have the user specify what tests should be run ([#837](https://github.com/dbt-labs/dbt-core/pull/837))

## dbt 0.10.1 (May 18, 2018)

This release focuses on achieving functional parity between all of dbt's adapters. With this release, most dbt functionality should work on every adapter except where noted [here](https://docs.getdbt.com/v0.10/docs/supported-databases#section-caveats).

### tl;dr

- Configure model schema and name quoting in your `dbt_project.yml` file ([Docs](https://docs.getdbt.com/v0.10/docs/configuring-quoting))
- Add a `Relation` object to the context to simplify model quoting [Docs](https://docs.getdbt.com/v0.10/reference#relation)
- Implement BigQuery materializations using new `create table as (...)` syntax, support `partition by` clause ([Docs](https://docs.getdbt.com/v0.10/docs/warehouse-specific-configurations#section-partition-clause))
- Override seed column types ([Docs](https://docs.getdbt.com/v0.10/reference#section-override-column-types))
- Add `get_columns_in_table` context function for BigQuery ([Docs](https://docs.getdbt.com/v0.10/reference#get_columns_in_table))

### Changes

- Consistent schema and identifier quoting ([#727](https://github.com/dbt-labs/dbt-core/pull/727))
  - Configure quoting settings in the `dbt_project.yml` file ([#742](https://github.com/dbt-labs/dbt-core/pull/742))
  - Add a `Relation` object to the context to make quoting consistent and simple ([#742](https://github.com/dbt-labs/dbt-core/pull/742))
- Use the new `create table as (...)` syntax on BigQuery ([#717](https://github.com/dbt-labs/dbt-core/pull/717))
  - Support `partition by` clause
- CSV Updates:
  - Use floating point as default seed column type to avoid issues with type inference ([#694](https://github.com/dbt-labs/dbt-core/pull/694))
  - Provide a mechanism for overriding seed column types in the `dbt_project.yml` file ([#708](https://github.com/dbt-labs/dbt-core/pull/708))
  - Fix seeding for files with more than 16k rows on Snowflake ([#694](https://github.com/dbt-labs/dbt-core/pull/694))
  - Implement seeds using a materialization
- Improve `get_columns_in_table` context function ([#709](https://github.com/dbt-labs/dbt-core/pull/709))
  - Support numeric types on Redshift, Postgres
  - Support BigQuery (including nested columns in `struct` types)
  - Support cross-database `information_schema` queries for Snowflake
  - Retain column ordinal positions

### Bugfixes

- Fix for incorrect var precendence when using `--vars` on the CLI ([#739](https://github.com/dbt-labs/dbt-core/pull/739))
- Fix for closed connections in `on-run-end` hooks for long-running dbt invocations ([#693](https://github.com/dbt-labs/dbt-core/pull/693))
- Fix: don't try to run empty hooks ([#620](https://github.com/dbt-labs/dbt-core/issues/620), [#693](https://github.com/dbt-labs/dbt-core/pull/693))
- Fix: Prevent seed data from being serialized into `graph.gpickle` file ([#720](https://github.com/dbt-labs/dbt-core/pull/720))
- Fix: Disallow seed and model files with the same name ([#737](https://github.com/dbt-labs/dbt-core/pull/737))

## dbt 0.10.0 (March 8, 2018)

This release overhauls dbt's package management functionality, makes seeding csv files work across all adapters, and adds date partitioning support for BigQuery.

### Upgrading Instructions:

- Check out full installation and upgrading instructions [here](https://docs.getdbt.com/docs/installation)
- Transition the `repositories:` section of your `dbt_project.yml` file to a `packages.yml` file as described [here](https://docs.getdbt.com/docs/package-management)
- You may need to clear out your `dbt_modules` directory if you use packages like [dbt-utils](https://github.com/dbt-labs/dbt-utils). Depending how your project is configured, you can do this by running `dbt clean`.
- We're using a new CSV parsing library, `agate`, so be sure to check that all of your seed tables are parsed as you would expect!

### Changes

- Support for variables defined on the CLI with `--vars` ([#640](https://github.com/dbt-labs/dbt-core/pull/640)) ([docs](https://docs.getdbt.com/docs/using-variables))
- Improvements to `dbt seed` ([docs](https://docs.getdbt.com/v0.10/reference#seed))
  - Support seeding csv files on all adapters ([#618](https://github.com/dbt-labs/dbt-core/pull/618))
  - Make seed csv's `ref()`-able in models ([#668](https://github.com/dbt-labs/dbt-core/pull/668))
  - Support seed file configuration (custom schemas, enabled / disabled) in the `dbt_project.yml` file ([#561](https://github.com/dbt-labs/dbt-core/issues/561))
  - Support `--full-refresh` instead of `--drop-existing` (deprecated) for seed files ([#515](https://github.com/dbt-labs/dbt-core/issues/515))
  - Add `--show` argument to `dbt seed` to display a sample of data in the CLI ([#74](https://github.com/dbt-labs/dbt-core/issues/74))
- Improvements to package management ([docs](https://docs.getdbt.com/docs/package-management))
  - Deprecated `repositories:` config option in favor of `packages:` ([#542](https://github.com/dbt-labs/dbt-core/pull/542))
  - Deprecated package listing in `dbt_project.yml` in favor of `packages.yml` ([#681](https://github.com/dbt-labs/dbt-core/pull/681))
  - Support stating local file paths as dependencies ([#542](https://github.com/dbt-labs/dbt-core/pull/542))
- Support date partitioning in BigQuery ([#641](https://github.com/dbt-labs/dbt-core/pull/641)) ([docs](https://docs.getdbt.com/docs/creating-date-partitioned-tables))
- Move schema creation to _after_ `on-run-start` hooks ([#652](https://github.com/dbt-labs/dbt-core/pull/652))
- Replace `csvkit` dependency with `agate` ([#598](https://github.com/dbt-labs/dbt-core/issues/598))
- Switch snowplow endpoint to pipe directly to Fishtown Analytics ([#682](https://github.com/dbt-labs/dbt-core/pull/682))

### Bugfixes

- Throw a compilation exception if a required test macro is not present in the context ([#655](https://github.com/dbt-labs/dbt-core/issues/655))
- Make the `adapter_macro` use the `return()` function ([#635](https://github.com/dbt-labs/dbt-core/issues/635))
- Fix bug for introspective query on late binding views (redshift) ([#647](https://github.com/dbt-labs/dbt-core/pull/647))
- Disable any non-dbt log output on the CLI ([#663](https://github.com/dbt-labs/dbt-core/pull/663))

## dbt 0.9.1 (January 2, 2018)

This release fixes bugs and adds supports for late binding views on Redshift.

### Changes

- Support late binding views on Redshift ([#614](https://github.com/dbt-labs/dbt-core/pull/614)) ([docs](https://docs.getdbt.com/docs/warehouse-specific-configurations#section-late-binding-views))
- Make `run_started_at` timezone-aware ([#553](https://github.com/dbt-labs/dbt-core/pull/553)) (Contributed by [@mturzanska](https://github.com/mturzanska)) ([docs](https://docs.getdbt.com/v0.9/reference#run_started_at))

### Bugfixes

- Include hook run time in reported model run time ([#607](https://github.com/dbt-labs/dbt-core/pull/607))
- Add warning for missing test constraints ([#600](https://github.com/dbt-labs/dbt-core/pull/600))
- Fix for schema tests used or defined in packages ([#599](https://github.com/dbt-labs/dbt-core/pull/599))
- Run hooks in defined order ([#601](https://github.com/dbt-labs/dbt-core/pull/601))
- Skip tests that depend on nonexistent models ([#617](https://github.com/dbt-labs/dbt-core/pull/617))
- Fix for `adapter_macro` called within a package ([#630](https://github.com/dbt-labs/dbt-core/pull/630))

## dbt 0.9.0 (October 25, 2017)

This release focuses on improvements to macros, materializations, and package management. Check out [the blog post](https://blog.fishtownanalytics.com/whats-new-in-dbt-0-9-0-dd36f3572ac6) to learn more about what's possible in this new version of dbt.

### Installation

Full installation instructions for macOS, Windows, and Linux can be found [here](https://docs.getdbt.com/v0.9/docs/installation). If you use Windows or Linux, installation works the same as with previous versions of dbt. If you use macOS and Homebrew to install dbt, note that installation instructions have changed:

#### macOS Installation Instructions

```bash
brew update
brew tap dbt-labs/dbt
brew install dbt
```

### Overview

- More powerful macros and materializations
- Custom model schemas
- BigQuery improvements
- Bugfixes
- Documentation (0.9.0 docs can be found [here](https://docs.getdbt.com/v0.9/))

### Breaking Changes

- `adapter` functions must be namespaced to the `adapter` context variable. To fix this error, use `adapter.already_exists` instead of just `already_exists`, or similar for other [adapter functions](https://docs.getdbt.com/docs/adapter).

### Bugfixes

- Handle lingering `__dbt_tmp` relations ([#511](https://github.com/dbt-labs/dbt-core/pull/511))
- Run tests defined in an ephemeral directory ([#509](https://github.com/dbt-labs/dbt-core/pull/509))

### Changes

- use `adapter`, `ref`, and `var` inside of macros ([#466](https://github.com/dbt-labs/dbt-core/pull/466/files))
- Build custom tests and materializations in dbt packages ([#466](https://github.com/dbt-labs/dbt-core/pull/466/files))
- Support pre- and post- hooks that run outside of a transaction ([#510](https://github.com/dbt-labs/dbt-core/pull/510))
- Support table materializations for BigQuery ([#507](https://github.com/dbt-labs/dbt-core/pull/507))
- Support querying external data sources in BigQuery ([#507](https://github.com/dbt-labs/dbt-core/pull/507))
- Override which schema models are materialized in ([#522](https://github.com/dbt-labs/dbt-core/pull/522)) ([docs](https://docs.getdbt.com/v0.9/docs/using-custom-schemas))
- Make `{{ ref(...) }}` return the same type of object as `{{ this }} `([#530](https://github.com/dbt-labs/dbt-core/pull/530))
- Replace schema test CTEs with subqueries to speed them up for Postgres ([#536](https://github.com/dbt-labs/dbt-core/pull/536)) ([@ronnyli](https://github.com/ronnyli))
- Bump Snowflake dependency, remove pyasn1 ([#570](https://github.com/dbt-labs/dbt-core/pull/570))

### Documentation

- Document how to [create a package](https://docs.getdbt.com/v0.9/docs/building-packages)
- Document how to [make a materialization](https://docs.getdbt.com/v0.9/docs/creating-new-materializations)
- Document how to [make custom schema tests](https://docs.getdbt.com/v0.9/docs/custom-schema-tests)
- Document how to [use hooks to vacuum](https://docs.getdbt.com/v0.9/docs/using-hooks#section-using-hooks-to-vacuum)
- Document [all context variables](https://docs.getdbt.com/v0.9/reference)

### New Contributors

- [@ronnyli](https://github.com/ronnyli) ([#536](https://github.com/dbt-labs/dbt-core/pull/536))

## dbt 0.9.0 Alpha 5 (October 24, 2017)

### Overview

- Bump Snowflake dependency, remove pyasn1 ([#570](https://github.com/dbt-labs/dbt-core/pull/570))

## dbt 0.9.0 Alpha 4 (October 3, 2017)

### Bugfixes

- Fix for federated queries on BigQuery with Service Account json credentials ([#547](https://github.com/dbt-labs/dbt-core/pull/547))

## dbt 0.9.0 Alpha 3 (October 3, 2017)

### Overview

- Bugfixes
- Faster schema tests on Postgres
- Fix for broken environment variables

### Improvements

- Replace schema test CTEs with subqueries to speed them up for Postgres ([#536](https://github.com/dbt-labs/dbt-core/pull/536)) ([@ronnyli](https://github.com/ronnyli))

### Bugfixes

- Fix broken integration tests ([#539](https://github.com/dbt-labs/dbt-core/pull/539))
- Fix for `--non-destructive` on views ([#539](https://github.com/dbt-labs/dbt-core/pull/539))
- Fix for package models materialized in the wrong schema ([#538](https://github.com/dbt-labs/dbt-core/pull/538))
- Fix for broken environment variables ([#543](https://github.com/dbt-labs/dbt-core/pull/543))

### New Contributors

- [@ronnyli](https://github.com/ronnyli)
  - https://github.com/dbt-labs/dbt-core/pull/536

## dbt 0.9.0 Alpha 2 (September 20, 2017)

### Overview

- Custom model schemas
- BigQuery updates
- `ref` improvements

### Bugfixes

- Parity for `statement` interface on BigQuery ([#526](https://github.com/dbt-labs/dbt-core/pull/526))

### Changes

- Override which schema models are materialized in ([#522](https://github.com/dbt-labs/dbt-core/pull/522)) ([docs](https://docs.getdbt.com/v0.9/docs/using-custom-schemas))
- Make `{{ ref(...) }}` return the same type of object as `{{ this }} `([#530](https://github.com/dbt-labs/dbt-core/pull/530))

## dbt 0.9.0 Alpha 1 (August 29, 2017)

### Overview

- More powerful macros
- BigQuery improvements
- Bugfixes
- Documentation (0.9.0 docs can be found [here](https://docs.getdbt.com/v0.9/))

### Breaking Changes

dbt 0.9.0 Alpha 1 introduces a number of new features intended to help dbt-ers write flexible, reusable code. The majority of these changes involve the `macro` and `materialization` Jinja blocks. As this is an alpha release, there may exist bugs or incompatibilites, particularly surrounding these two blocks. A list of known breaking changes is provided below. If you find new bugs, or have questions about dbt 0.9.0, please don't hesitate to reach out in [slack](http://community.getdbt.com/) or [open a new issue](https://github.com/dbt-labs/dbt-core/issues/new?milestone=0.9.0+alpha-1).

##### 1. Adapter functions must be namespaced to the `adapter` context variable

This will manifest as a compilation error that looks like:

```
Compilation Error in model {your_model} (models/path/to/your_model.sql)
  'already_exists' is undefined
```

To fix this error, use `adapter.already_exists` instead of just `already_exists`, or similar for other [adapter functions](https://docs.getdbt.com/docs/adapter).

### Bugfixes

- Handle lingering `__dbt_tmp` relations ([#511](https://github.com/dbt-labs/dbt-core/pull/511))
- Run tests defined in an ephemeral directory ([#509](https://github.com/dbt-labs/dbt-core/pull/509))

### Changes

- use `adapter`, `ref`, and `var` inside of macros ([#466](https://github.com/dbt-labs/dbt-core/pull/466/files))
- Build custom tests and materializations in dbt packages ([#466](https://github.com/dbt-labs/dbt-core/pull/466/files))
- Support pre- and post- hooks that run outside of a transaction ([#510](https://github.com/dbt-labs/dbt-core/pull/510))
- Support table materializations for BigQuery ([#507](https://github.com/dbt-labs/dbt-core/pull/507))
- Support querying external data sources in BigQuery ([#507](https://github.com/dbt-labs/dbt-core/pull/507))

### Documentation

- Document how to [create a package](https://docs.getdbt.com/v0.8/docs/building-packages)
- Document how to [make a materialization](https://docs.getdbt.com/v0.8/docs/creating-new-materializations)
- Document how to [make custom schema tests](https://docs.getdbt.com/v0.8/docs/custom-schema-tests)

## dbt 0.8.3 (July 14, 2017)

### Overview

- Add suppport for Google BigQuery
- Significant exit codes
- Load credentials from environment variables

### Bugfixes

- Fix errant warning for `dbt archive` commands ([#476](https://github.com/dbt-labs/dbt-core/pull/476))
- Show error (instead of backtrace) for failed hook statements ([#478](https://github.com/dbt-labs/dbt-core/pull/478))
- `dbt init` no longer leaves the repo in an invalid state ([#487](https://github.com/dbt-labs/dbt-core/pull/487))
- Fix bug which ignored git tag specs for package repos ([#463](https://github.com/dbt-labs/dbt-core/issues/463))

### Changes

- Support BigQuery as a target ([#437](https://github.com/dbt-labs/dbt-core/issues/437)) ([#438](https://github.com/dbt-labs/dbt-core/issues/438))
- Make dbt exit codes significant (0 = success, 1/2 = error) ([#297](https://github.com/dbt-labs/dbt-core/issues/297))
- Add context function to pull in environment variables ([#450](https://github.com/dbt-labs/dbt-core/issues/450))

### Documentation

- Document target configuration for BigQuery [here](https://docs.getdbt.com/v0.8/docs/supported-databases#section-bigquery)
- Document dbt exit codes [here](https://docs.getdbt.com/v0.8/reference#exit-codes)
- Document environment variable usage [here](https://docs.getdbt.com/v0.8/reference#env_var)

## dbt 0.8.2 (May 31, 2017)

### Overview

- UI/UX improvements (colorized output, failures summary, better error messages)
- Cancel running queries on ctrl+c
- Bugfixes
- Docs

### Bugfixes

- Fix bug for interleaved sort keys on Redshift ([#430](https://github.com/dbt-labs/dbt-core/pull/430))

### Changes

- Don't try to create schema if it already exists ([#446](https://github.com/dbt-labs/dbt-core/pull/446))
- Summarize failures for dbt invocations ([#443](https://github.com/dbt-labs/dbt-core/pull/443))
- Colorized dbt output ([#441](https://github.com/dbt-labs/dbt-core/pull/441))
- Cancel running queries on ctrl-c ([#444](https://github.com/dbt-labs/dbt-core/pull/444))
- Better error messages for common failure modes ([#445](https://github.com/dbt-labs/dbt-core/pull/445))
- Upgrade dependencies ([#431](https://github.com/dbt-labs/dbt-core/pull/431))
- Improvements to `dbt init` and first time dbt usage experience ([#439](https://github.com/dbt-labs/dbt-core/pull/439))

### Documentation

- Document full-refresh requirements for incremental models ([#417](https://github.com/dbt-labs/dbt-core/issues/417))
- Document archival ([#433](https://github.com/dbt-labs/dbt-core/issues/433))
- Document the two-version variant of `ref` ([#432](https://github.com/dbt-labs/dbt-core/issues/432))

## dbt 0.8.1 (May 10, 2017)

### Overview

- Bugfixes
- Reintroduce `compile` command
- Moved docs to [readme.io](https://docs.getdbt.com/)

### Bugfixes

- Fix bug preventing overriding a disabled package model in the current project ([#391](https://github.com/dbt-labs/dbt-core/pull/391))
- Fix bug which prevented multiple sort keys (provided as an array) on Redshift ([#397](https://github.com/dbt-labs/dbt-core/pull/397))
- Fix race condition while compiling schema tests in an empty `target` directory ([#398](https://github.com/dbt-labs/dbt-core/pull/398))

### Changes

- Reintroduce dbt `compile` command ([#407](https://github.com/dbt-labs/dbt-core/pull/407))
- Compile `on-run-start` and `on-run-end` hooks to a file ([#412](https://github.com/dbt-labs/dbt-core/pull/412))

### Documentation

- Move docs to readme.io ([#414](https://github.com/dbt-labs/dbt-core/pull/414))
- Add docs for event tracking opt-out ([#399](https://github.com/dbt-labs/dbt-core/issues/399))

## dbt 0.8.0 (April 17, 2017)

### Overview

- Bugfixes
- True concurrency
- More control over "advanced" incremental model configurations [more info](http://dbt.readthedocs.io/en/master/guide/configuring-models/)

### Bugfixes

- Fix ephemeral load order bug ([#292](https://github.com/dbt-labs/dbt-core/pull/292), [#285](https://github.com/dbt-labs/dbt-core/pull/285))
- Support composite unique key in archivals ([#324](https://github.com/dbt-labs/dbt-core/pull/324))
- Fix target paths ([#331](https://github.com/dbt-labs/dbt-core/pull/331), [#329](https://github.com/dbt-labs/dbt-core/issues/329))
- Ignore commented-out schema tests ([#330](https://github.com/dbt-labs/dbt-core/pull/330), [#328](https://github.com/dbt-labs/dbt-core/issues/328))
- Fix run levels ([#343](https://github.com/dbt-labs/dbt-core/pull/343), [#340](https://github.com/dbt-labs/dbt-core/issues/340), [#338](https://github.com/dbt-labs/dbt-core/issues/338))
- Fix concurrency, open a unique transaction per model ([#345](https://github.com/dbt-labs/dbt-core/pull/345), [#336](https://github.com/dbt-labs/dbt-core/issues/336))
- Handle concurrent `DROP ... CASCADE`s in Redshift ([#349](https://github.com/dbt-labs/dbt-core/pull/349))
- Always release connections (use `try .. finally`) ([#354](https://github.com/dbt-labs/dbt-core/pull/354))

### Changes

- Changed: different syntax for "relationships" schema tests ([#339](https://github.com/dbt-labs/dbt-core/pull/339))
- Added: `already_exists` context function ([#372](https://github.com/dbt-labs/dbt-core/pull/372))
- Graph refactor: fix common issues with load order ([#292](https://github.com/dbt-labs/dbt-core/pull/292))
- Graph refactor: multiple references to an ephemeral models should share a CTE ([#316](https://github.com/dbt-labs/dbt-core/pull/316))
- Graph refactor: macros in flat graph ([#332](https://github.com/dbt-labs/dbt-core/pull/332))
- Refactor: factor out jinja interactions ([#309](https://github.com/dbt-labs/dbt-core/pull/309))
- Speedup: detect cycles at the end of compilation ([#307](https://github.com/dbt-labs/dbt-core/pull/307))
- Speedup: write graph file with gpickle instead of yaml ([#306](https://github.com/dbt-labs/dbt-core/pull/306))
- Clone dependencies with `--depth 1` to make them more compact ([#277](https://github.com/dbt-labs/dbt-core/issues/277), [#342](https://github.com/dbt-labs/dbt-core/pull/342))
- Rewrite materializations as macros ([#356](https://github.com/dbt-labs/dbt-core/pull/356))

## dbt 0.7.1 (February 28, 2017)

### Overview

- [Improved graph selection](http://dbt.readthedocs.io/en/master/guide/usage/#run)
- A new home for dbt
- Snowflake improvements

#### New Features

- improved graph selection for `dbt run` and `dbt test` ([more information](http://dbt.readthedocs.io/en/master/guide/usage/#run)) ([#279](https://github.com/dbt-labs/dbt-core/pull/279))
- profiles.yml now supports Snowflake `role` as an option ([#291](https://github.com/dbt-labs/dbt-core/pull/291))

#### A new home for dbt

In v0.7.1, dbt was moved from the analyst-collective org to the dbt-labs org ([#300](https://github.com/dbt-labs/dbt-core/pull/300))

#### Bugfixes

- nicer error if `run-target` was not changed to `target` during upgrade to dbt>=0.7.0

## dbt 0.7.0 (February 9, 2017)

### Overview

- Snowflake Support
- Deprecations

### Snowflake Support

dbt now supports [Snowflake](https://www.snowflake.net/) as a target in addition to Postgres and Redshift! All dbt functionality is supported in this new warehouse. There is a sample snowflake profile in [sample.profiles.yml](https://github.com/dbt-labs/dbt-core/blob/development/sample.profiles.yml) -- you can start using it right away.

### Deprecations

There are a few deprecations in 0.7:

- `run-target` in profiles.yml is no longer supported. Use `target` instead.
- Project names (`name` in dbt_project.yml) can now only contain letters, numbers, and underscores, and must start with a letter. Previously they could contain any character.
- `--dry-run` is no longer supported.

### Notes

#### New Features

- dbt now supports [Snowflake](https://www.snowflake.net/) as a warehouse ([#259](https://github.com/dbt-labs/dbt-core/pull/259))

#### Bugfixes

- use adapter for sort/dist ([#274](https://github.com/dbt-labs/dbt-core/pull/274))

#### Deprecations

- run-target and name validations ([#280](https://github.com/dbt-labs/dbt-core/pull/280))
- dry-run removed ([#281](https://github.com/dbt-labs/dbt-core/pull/281))

#### Changes

- fixed a typo in the docs related to post-run hooks ([#271](https://github.com/dbt-labs/dbt-core/pull/271))
- refactored tracking code to refresh invocation id in a multi-run context ([#273](https://github.com/dbt-labs/dbt-core/pull/273))
- added unit tests for the graph ([#270](https://github.com/dbt-labs/dbt-core/pull/270))

## dbt 0.6.2 (January 16, 2017)

#### Changes

- condense error output when `--debug` is not set ([#265](https://github.com/dbt-labs/dbt-core/pull/265))

## dbt 0.6.1 (January 11, 2017)

#### Bugfixes

- respect `config` options in profiles.yml ([#255](https://github.com/dbt-labs/dbt-core/pull/255))
- use correct `on-run-end` option for post-run hooks ([#261](https://github.com/dbt-labs/dbt-core/pull/261))

#### Changes

- add `--debug` flag, replace calls to `print()` with a global logger ([#256](https://github.com/dbt-labs/dbt-core/pull/256))
- add pep8 check to continuous integration tests and bring codebase into compliance ([#257](https://github.com/dbt-labs/dbt-core/pull/257))

## dbt release 0.6.0

### tl;dr

- Macros
- More control over how models are materialized
- Minor improvements
- Bugfixes
- Connor McArthur

### Macros

Macros are snippets of SQL that can be called like functions in models. Macros make it possible to re-use SQL between models
in keeping with the engineering principle of DRY (Dont Repeat Yourself). Moreover, packages can expose Macros that you can use in your own dbt project.

For detailed information on how to use Macros, check out the pull request [here](https://github.com/dbt-labs/dbt-core/pull/245)

### Runtime Materialization Configs

DBT Version 0.6.0 introduces two new ways to control the materialization of models:

#### Non-destructive dbt run [more info](https://github.com/dbt-labs/dbt-core/issues/137)

If you provide the `--non-destructive` argument to `dbt run`, dbt will minimize the amount of time during which your models are unavailable. Specfically, dbt
will

1.  Ignore models materialized as `views`
2.  Truncate tables and re-insert data instead of dropping and re-creating

This flag is useful for recurring jobs which only need to update table models and incremental models.

```bash
dbt run --non-destructive
```

#### Incremental Model Full Refresh [more info](https://github.com/dbt-labs/dbt-core/issues/140)

If you provide the `--full-refresh` argument to `dbt run`, dbt will treat incremental models as table models. This is useful when

1. An incremental model schema changes and you need to recreate the table accordingly
2. You want to reprocess the entirety of the incremental model because of new logic in the model code

```bash
dbt run --full-refresh
```

Note that `--full-refresh` and `--non-destructive` can be used together!

For more information, run

```
dbt run --help
```

### Minor improvements [more info](https://github.com/dbt-labs/dbt-core/milestone/15?closed=1)

#### Add a `{{ target }}` variable to the dbt runtime [more info](https://github.com/dbt-labs/dbt-core/issues/149)

Use `{{ target }}` to interpolate profile variables into your model definitions. For example:

```sql
-- only use the last week of data in development
select * from events

{% if target.name == 'dev' %}
where created_at > getdate() - interval '1 week'
{% endif %}
```

#### User-specified `profiles.yml` dir [more info](https://github.com/dbt-labs/dbt-core/issues/213)

DBT looks for a file called `profiles.yml` in the `~/.dbt/` directory. You can now overide this directory with

```bash
$ dbt run --profiles-dir /path/to/my/dir
```

#### Add timestamp to console output [more info](https://github.com/dbt-labs/dbt-core/issues/125)

Informative _and_ pretty

#### Run dbt from subdirectory of project root [more info](https://github.com/dbt-labs/dbt-core/issues/129)

A story in three parts:

```bash
cd models/snowplow/sessions
vim sessions.sql
dbt run # it works!
```

#### Pre and post run hooks [more info](https://github.com/dbt-labs/dbt-core/issues/226)

```yaml
# dbt_project.yml
name: ...
version: ...

---
# supply either a string, or a list of strings
on-run-start: "create table public.cool_table (id int)"
on-run-end:
  - insert into public.cool_table (id) values (1), (2), (3)
  - insert into public.cool_table (id) values (4), (5), (6)
```

### Bug fixes

We fixed 10 bugs in this release! See the full list [here](https://github.com/dbt-labs/dbt-core/milestone/11?closed=1)

---

## dbt release 0.5.4

### tl;dr

- added support for custom SQL data tests
  - SQL returns 0 results --> pass
  - SQL returns > 0 results --> fail
- dbt-core integration tests
  - running in Continuous Integration environments
    - windows ([appveyor](https://ci.appveyor.com/project/DrewBanin/dbt/branch/development))
    - linux ([circle](https://circleci.com/gh/dbt-labs/dbt-core/tree/master))
  - with [code coverage](https://circleci.com/api/v1/project/dbt-labs/dbt-core/latest/artifacts/0/$CIRCLE_ARTIFACTS/htmlcov/index.html?branch=development)

### Custom SQL data tests

Schema tests have proven to be an essential part of a modern analytical workflow. These schema tests validate basic constraints about your data. Namely: not null, unique, accepted value, and foreign key relationship properties can be asserted using schema tests.

With dbt v0.5.4, you can now write your own custom "data tests". These data tests are SQL SELECT statements that return 0 rows on success, or > 0 rows on failure. A typical data test might look like:

```sql
-- tests/assert_less_than_5_pct_event_cookie_ids_are_null.sql

-- If >= 5% of cookie_ids are null, then the test returns 1 row (failure).
-- If < 5% of cookie_ids are null, then the test returns 0 rows (success)

with calc as (

    select
      sum(case when cookie_id is null then 1 else 0 end)::float / count(*)::float as fraction
    from {{ ref('events') }}

)

select * from calc where fraction < 0.05
```

To enable data tests, add the `test-paths` config to your `dbt_project.yml` file:

```yml
name: 'Vandelay Industries`
version: '1.0'

source-paths: ["models"]
target-path: "target"
test-paths: ["tests"]        # look for *.sql files in the "tests" directory
....
```

Any `.sql` file found in the `test-paths` director(y|ies) will be evaluated as data tests. These tests can be run with:

```bash
dbt test # run schema + data tests
dbt test --schema # run only schema tests
dbt test --data # run only data tests
dbt test --data --schema # run schema + data tests

# For more information, try
dbt test -h
```

### DBT-core integration tests

With the dbt 0.5.4 release, dbt now features a robust integration test suite. These integration tests will help mitigate the risk of software regressions, and in so doing, will help us develop dbt more quickly. You can check out the tests [here](https://github.com/dbt-labs/dbt-core/tree/development/test/integration), and the test results [here (linux/osx)](https://circleci.com/gh/dbt-labs/dbt-core/tree/master) and [here (windows)](https://ci.appveyor.com/project/DrewBanin/dbt/branch/development).

### The Future

You can check out the DBT roadmap [here](https://github.com/dbt-labs/dbt-core/milestones). In the next few weeks, we'll be working on [bugfixes](https://github.com/dbt-labs/dbt-core/milestone/11), [minor features](https://github.com/dbt-labs/dbt-core/milestone/15), [improved macro support](https://github.com/dbt-labs/dbt-core/milestone/14), and [expanded control over runtime materialization configs](https://github.com/dbt-labs/dbt-core/milestone/9).

As always, feel free to reach out to us on [Slack](http://community.getdbt.com/) with any questions or comments!

---

## dbt release 0.5.3

Bugfix release.

Fixes regressions introduced in 0.5.1 and 0.5.2.

### Fixed 0.5.1 regressions

Incremental models were broken by the new column expansion feature. Column expansion is implemented as

```sql
alter table ... add column tmp_col varchar({new_size});
update ... set tmp_col = existing_col
alter table ... drop column existing_col
alter table ... rename tmp_col to existing_col
```

This has the side-effect of moving the `existing_col` to the "end" of the table. When an incremental model tries to

```sql
insert into {table} (
   select * from tmp_table
)
```

suddenly the columns in `{table}` are incongruent with the columns in `tmp_table`. This insert subsequently fails.

The fix for this issue is twofold:

1. If the incremental model table DOES NOT already exist, avoid inserts altogether. Instead, run a `create table as (...)` statement
2. If the incremental model table DOES already exist, query for the columns in the existing table and use those to build the insert statement, eg:

```sql
insert into "dbt_dbanin"."sessions" ("session_end_tstamp", "session_start_tstamp", ...)
(
    select "session_end_tstamp", "session_start_tstamp", ...
    from "sessions__dbt_incremental_tmp"
);
```

In this way, the source and destination columns are guaranteed to be in the same order!

### Fixed 0.5.2 regressions

We attempted to refactor the way profiles work in dbt. Previously, a default `user` profile was loaded, and the profiles specified in `dbt_project.yml` or on the command line (`with --profile`) would be applied on top of the `user` config. This implementation is [some of the earliest code](https://github.com/dbt-labs/dbt-core/commit/430d12ad781a48af6a754442693834efdf98ffb1) that was committed to dbt.

As `dbt` has grown, we found this implementation to be a little unwieldy and hard to maintain. The 0.5.2 release made it so that only one profile could be loaded at a time. This profile needed to be specified in either `dbt_project.yml` or on the command line with `--profile`. A bug was errantly introduced during this change which broke the handling of dependency projects.

### The future

The additions of automated testing and a more comprehensive manual testing process will go a long way to ensuring the future stability of dbt. We're going to get started on these tasks soon, and you can follow our progress here: https://github.com/dbt-labs/dbt-core/milestone/16 .

As always, feel free to [reach out to us on Slack](http://community.getdbt.com/) with any questions or concerns:

---

## dbt release 0.5.2

Patch release fixing a bug that arises when profiles are overridden on the command line with the `--profile` flag.

See https://github.com/dbt-labs/dbt-core/releases/tag/v0.5.1

---

## dbt release 0.5.1

### 0. tl;dr

1. Raiders of the Lost Archive -- version your raw data to make historical queries more accurate
2. Column type resolution for incremental models (no more `Value too long for character type` errors)
3. Postgres support
4. Top-level configs applied to your project + all dependencies
5. --threads CLI option + better multithreaded output

### 1. Source table archival https://github.com/dbt-labs/dbt-core/pull/183

Commonly, analysts need to "look back in time" at some previous state of data in their mutable tables. Imagine a `users` table which is synced to your data warehouse from a production database. This `users` table is a representation of what your users look like _now_. Consider what happens if you need to look at revenue by city for each of your users trended over time. Specifically, what happens if a user moved from, say, Philadelphia to New York? To do this correctly, you need to archive snapshots of the `users` table on a recurring basis. With this release, dbt now provides an easy mechanism to store such snapshots.

To use this new feature, declare the tables you want to archive in your `dbt_project.yml` file:

```yaml
archive:
    - source_schema: synced_production_data  # schema to look for tables in (declared below)
      target_schema: dbt_archive             # where to archive the data to
      tables:                                # list of tables to archive
        - source_table: users                # table to archive
          target_table: users_archived       # table to insert archived data into
          updated_at: updated_at             # used to determine when data has changed
          unique_key: id                     # used to generate archival query

        - source_table: some_other_table
           target_table: some_other_table_archive
           updated_at: "updatedAt"
           unique_key: "expressions || work || LOWER(too)"

    - source_schema: some_other_schema
      ....
```

The archived tables will mirror the schema of the source tables they're generated from. In addition, three fields are added to the archive table:

1. `valid_from`: The timestamp when this archived row was inserted (and first considered valid)
1. `valid_to`: The timestamp when this archived row became invalidated. The first archived record for a given `unique_key` has `valid_to = NULL`. When newer data is archived for that `unique_key`, the `valid_to` field of the old record is set to the `valid_from` field of the new record!
1. `scd_id`: A unique key generated for each archive record. Scd = [Slowly Changing Dimension](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row).

dbt models can be built on top of these archived tables. The most recent record for a given `unique_key` is the one where `valid_to` is `null`.

To run this archive process, use the command `dbt archive`. After testing and confirming that the archival works, you should schedule this process through cron (or similar).

### 2. Incremental column expansion https://github.com/dbt-labs/dbt-core/issues/175

Incremental tables are a powerful dbt feature, but there was at least one edge case which makes working with them difficult. During the first run of an incremental model, Redshift will infer a type for every column in the table. Subsequent runs can insert new data which does not conform to the expected type. One example is a `varchar(16)` field which is inserted into a `varchar(8)` field.
In practice, this error looks like:

```
Value too long for character type
DETAIL:
  -----------------------------------------------
  error:  Value too long for character type
  code:      8001
  context:   Value too long for type character varying(8)
  query:     3743263
  location:  funcs_string.hpp:392
  process:   query4_35 [pid=18194]
  -----------------------------------------------
```

With this release, dbt will detect when column types are incongruent and will attempt to reconcile these different types if possible. Specifically, dbt will alter the incremental model table schema from `character varying(x)` to `character varying(y)` for some `y > x`. This should drastically reduce the occurrence of this class of error.

### 3. First-class Postgres support https://github.com/dbt-labs/dbt-core/pull/183

With this release, Postgres became a first-class dbt target. You can configure a postgres database target in your `~/.dbt/profiles.yml` file:

```yaml
warehouse:
  outputs:
    dev:
      type: postgres    # configure a target for Postgres
      host: localhost
      user: Drew
      ....
  run-target: dev
```

While Redshift is built on top of Postgres, the two are subtly different. For instance, Redshift supports sort and dist keys, while Postgres does not! dbt will use the database target `type` parameter to generate the appropriate SQL for the target database.

### 4. Root-level configs https://github.com/dbt-labs/dbt-core/issues/161

Configurations in `dbt_project.yml` can now be declared at the `models:` level. These configurations will apply to the primary project, as well as any dependency projects. This feature is particularly useful for setting pre- or post- hooks that run for _every_ model. In practice, this looks like:

```yaml
name: "My DBT Project"

models:
  post-hook:
    - "grant select on {{this}} to looker_user" # Applied to 'My DBT Project' and 'Snowplow' dependency
  "My DBT Project":
    enabled: true
  "Snowplow":
    enabled: true
```

### 5. --threads CLI option https://github.com/dbt-labs/dbt-core/issues/143

The number of threads that DBT uses can now be overridden with a CLI argument. The number of threads used must be between 1 and 8.

```bash
dbt run --threads 1    # fine
# or
dbt run --threads 4    # great
# or
dbt run --threads 42    # too many!
```

In addition to this new CLI argument, the output from multi-threaded dbt runs should be a little more orderly now. Models won't show as `START`ed until they're actually queued to run. Previously, the output here was a little confusing. Happy threading!

### Upgrading

To upgrade to version 0.5.1 of dbt, run:

```bash
pip install --upgrade dbt
```

### And another thing

- Join us on [slack](http://community.getdbt.com/) with questions or comments

Made with ♥️ by 🐟🏙 📈

---

### 0. tl;dr

- use a temp table when executing incremental models
- arbitrary configuration (using config variables)
- specify branches for dependencies
- more & better docs

### 1. new incremental model generation https://github.com/dbt-labs/dbt-core/issues/138

In previous versions of dbt, an edge case existed which caused the `sql_where` query to select different rows in the `delete` and `insert` steps. As a result, it was possible to construct incremental models which would insert duplicate records into the specified table. With this release, DBT uses a temp table which will 1) circumvent this issue and 2) improve query performance. For more information, check out the GitHub issue: https://github.com/dbt-labs/dbt-core/issues/138

### 2. Arbitrary configuration https://github.com/dbt-labs/dbt-core/issues/146

Configuration in dbt is incredibly powerful: it is what allows models to change their behavior without changing their code. Previously, all configuration was done using built-in parameters, but that actually limits the user in the power of configuration.

With this release, you can inject variables from `dbt_project.yml` into your top-level and dependency models. In practice, variables work like this:

```yml
# dbt_project.yml

models:
  my_project:
    vars:
      exclude_ip: "192.168.1.1"
```

```sql
-- filtered_events.sql

-- source code
select * from public.events where ip_address != '{{ var("exclude_ip") }}'

-- compiles to
select * from public.events where ip_address != '192.168.1.1'
```

The `vars` parameter in `dbt_project.yml` is compiled, so you can use jinja templating there as well! The primary use case for this is specifying "input" models to a dependency.

Previously, dependencies used `ref(...)` to select from a project's base models. That interface was brittle, and the idea that dependency code had unbridled access to all of your top-level models made us a little uneasy. As of this release, we're deprecating the ability for dependencies to `ref(...)` top-level models. Instead, the recommended way for this to work is with vars! An example:

```sql
-- dbt_modules/snowplow/models/events.sql

select * from {{ var('snowplow_events_table') }}
```

and

```yml
models:
  Snowplow:
    vars:
      snowplow_events_table: "{{ ref('base_events') }}"
```

This effectively mirrors the previous behavior, but it much more explicit about what's happening under the hood!

### 3. specify a dependency branch https://github.com/dbt-labs/dbt-core/pull/165

With this release, you can point DBT to a specific branch of a dependency repo. The syntax looks like this:

```
repositories:
    - https://github.com/dbt-labs/dbt-audit.git@development # use the "development" branch
```

### 4. More & Better Docs!

Check em out! And let us know if there's anything you think we can improve upon!

### Upgrading

To upgrade to version 0.5.0 of dbt, run:

```bash
pip install --upgrade dbt
```

---

### 0. tl;dr

- `--version` command
- pre- and post- run hooks
- windows support
- event tracking

### 1. --version https://github.com/dbt-labs/dbt-core/issues/135

The `--version` command was added to help aid debugging. Further, organizations can use it to ensure that everyone in their org is up-to-date with dbt.

```bash
$ dbt --version
installed version: 0.4.7
   latest version: 0.4.7
Up to date!
```

### 2. pre-and-post-hooks https://github.com/dbt-labs/dbt-core/pull/147

With this release, you can now specify `pre-` and `post-` hooks that are run before and after a model is run, respectively. Hooks are useful for running `grant` statements, inserting a log of runs into an audit table, and more! Here's an example of a grant statement implemented using a post-hook:

```yml
models:
  my_project:
    post-hook: "grant select on table {{this}} to looker_user"
    my_model:
      materialized: view
    some_model:
      materialized: table
      post-hook: "insert into my_audit_table (model_name, run_at) values ({{this.name}}, getdate())"
```

Hooks are recursively appended, so the `my_model` model will only receive the `grant select...` hook, whereas the `some_model` model will receive _both_ the `grant select...` and `insert into...` hooks.

Finally, note that the `grant` statement uses the (hopefully familiar) `{{this}}` syntax whereas the `insert` statement uses the `{{this.name}}` syntax. When DBT creates a model:

- A temp table is created
- The original model is dropped
- The temp table is renamed to the final model name

DBT will intelligently uses the right table/view name when you invoke `{{this}}`, but you have a couple of more specific options available if you need them:

```
{{this}} : "schema"."table__dbt_tmp"
{{this.schema}}: "schema"
{{this.table}}: "table__dbt_tmp"
{{this.name}}: "table"
```

### 3. Event tracking https://github.com/dbt-labs/dbt-core/issues/89

We want to build the best version of DBT possible, and a crucial part of that is understanding how users work with DBT. To this end, we've added some really simple event tracking to DBT (using Snowplow). We do not track credentials, model contents or model names (we consider these private, and frankly none of our business). This release includes basic event tracking that reports 1) when dbt is invoked 2) when models are run, and 3) basic platform information (OS + python version). The schemas for these events can be seen [here](https://github.com/dbt-labs/dbt-core/tree/development/events/schemas/com.fishtownanalytics)

You can opt out of event tracking at any time by adding the following to the top of you `~/.dbt/profiles.yml` file:

```yaml
config:
  send_anonymous_usage_stats: False
```

### 4. Windows support https://github.com/dbt-labs/dbt-core/pull/154

![windows](https://pbs.twimg.com/profile_images/571398080688181248/57UKydQS.png)

---

dbt v0.4.1 provides improvements to incremental models, performance improvements, and ssh support for db connections.

### 0. tl;dr

- slightly modified dbt command structure
- `unique_key` setting for incremental models
- connect to your db over ssh
- no more model-defaults
- multithreaded schema tests

If you encounter an SSL/cryptography error while upgrading to this version of dbt, check that your version of pip is up-to-date

```bash
pip install -U pip
pip install -U dbt
```

### 1. new dbt command structure https://github.com/dbt-labs/dbt-core/issues/109

```bash
# To run models
dbt run # same as before

# to dry-run models
dbt run --dry # previously dbt test

# to run schema tests
dbt test # previously dbt test --validate
```

### 2. Incremental model improvements https://github.com/dbt-labs/dbt-core/issues/101

Previously, dbt calculated "new" incremental records to insert by querying for rows which matched some `sql_where` condition defined in the model configuration. This works really well for atomic datasets like a clickstream event log -- once inserted, these records will never change. Other datasets, like a sessions table comprised of many pageviews for many users, can change over time. Consider the following scenario:

User 1 Session 1 Event 1 @ 12:00
User 1 Session 1 Event 2 @ 12:01
-- dbt run --
User 1 Session 1 Event 3 @ 12:02

In this scenario, there are two possible outcomes depending on the `sql_where` chosen: 1) Event 3 does not get included in the Session 1 record for User 1 (bad), or 2) Session 1 is duplicated in the sessions table (bad). Both of these outcomes are inadequate!

With this release, you can now add a `unique_key` expression to an incremental model config. Records matching the `unique_key` will be `delete`d from the incremental table, then `insert`ed as usual. This makes it possible to maintain data accuracy without recalculating the entire table on every run.

The `unique_key` can be any expression which uniquely defines the row, eg:

```yml
sessions:
  materialized: incremental
  sql_where: "session_end_tstamp > (select max(session_end_tstamp) from {{this}})"
  unique_key: user_id || session_index
```

### 3. Run schema validations concurrently https://github.com/dbt-labs/dbt-core/issues/100

The `threads` run-target config now applies to schema validations too. Try it with `dbt test`

### 4. Connect to database over ssh https://github.com/dbt-labs/dbt-core/issues/93

Add an `ssh-host` parameter to a run-target to connect to a database over ssh. The `ssh-host` parameter should be the name of a `Host` in your `~/.ssh/config` file [more info](http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/)

```yml
warehouse:
  outputs:
    dev:
      type: redshift
      host: my-redshift.amazonaws.com
      port: 5439
      user: my-user
      pass: my-pass
      dbname: my-db
      schema: dbt_dbanin
      threads: 8
      ssh-host: ssh-host-name # <------ Add this line
  run-target: dev
```

### Remove the model-defaults config https://github.com/dbt-labs/dbt-core/issues/111

The `model-defaults` config doesn't make sense in a dbt world with dependencies. To apply default configs to your package, add the configs immediately under the package definition:

```yml
models:
  My_Package:
    enabled: true
    materialized: table
    snowplow: ...
```

---

## dbt v0.4.0

dbt v0.4.0 provides new ways to materialize models in your database.

### 0. tl;dr

- new types of materializations: `incremental` and `ephemeral`
- if upgrading, change `materialized: true|false` to `materialized: table|view|incremental|ephemeral`
- optionally specify model configs within the SQL file

### 1. Feature: `{{this}}` template variable https://github.com/dbt-labs/dbt-core/issues/81

The `{{this}}` template variable expands to the name of the model being compiled. For example:

```sql
-- my_model.sql
select 'the fully qualified name of this model is {{ this }}'
-- compiles to
select 'the fully qualified name of this model is "the_schema"."my_model"'
```

### 2. Feature: `materialized: incremental` https://github.com/dbt-labs/dbt-core/pull/90

After initially creating a table, incremental models will `insert` new records into the table on subsequent runs. This drastically speeds up execution time for large, append-only datasets.

Each execution of dbt run will:

- create the model table if it doesn't exist
- insert new records into the table

New records are identified by a `sql_where` model configuration option. In practice, this looks like:

```yml
sessions:
  materialized: incremental
  sql_where: "session_start_time > (select max(session_start_time) from {{this}})"
```

There are a couple of new things here. Previously, `materialized` could either be set to `true` or `false`. Now, the valid options include `view`, `table,` `incremental`, and `ephemeral` (more on this last one below). Also note that incremental models generally require use of the {{this}} template variable to identify new records.

The `sql_where` field is supplied as a `where` condition on a subquery containing the model definition. This resultset is then inserted into the target model. This looks something like:

```sql
insert into schema.model (
    select * from (
        -- compiled model definition
    ) where {{sql_where}}
)
```

### 3. Feature: `materialized: ephemeral` https://github.com/dbt-labs/dbt-core/issues/78

Ephemeral models are injected as CTEs (`with` statements) into any model that `ref`erences them. Ephemeral models are part of the dependency graph and generally function like any other model, except ephemeral models are not compiled to their own files or directly created in the database. This is useful for intermediary models which are shared by other downstream models, but shouldn't be queried directly from outside of dbt.

To make a model ephemeral:

```yml
employees:
  materialized: ephemeral
```

Suppose you wanted to exclude `employees` from your `users` table, but you don't want to clutter your analytics schema with an `employees` table.

```sql
-- employees.sql
select * from public.employees where is_deleted = false

-- users.sql
select *
from {{ref('users')}}
where email not in (select email from {{ref('employees')}})
```

The compiled SQL would look something like:

```sql
with __dbt__CTE__employees as (
  select * from public.employees where is_deleted = false
)
select *
from users
where email not in (select email from __dbt__CTE__employees)
```

Ephemeral models play nice with other ephemeral models, incremental models, and regular table/view models. Feel free to mix and match different materialization options to optimize for performance and simplicity.

### 4. Feature: In-model configs https://github.com/dbt-labs/dbt-core/issues/88

Configurations can now be specified directly inside of models. These in-model configs work exactly the same as configs inside of the dbt_project.yml file.

An in-model-config looks like this:

```sql
-- users.sql

-- python function syntax
{{ config(materialized="incremental", sql_where="id > (select max(id) from {{this}})") }}
-- OR json syntax
{{
    config({"materialized:" "incremental", "sql_where" : "id > (select max(id) from {{this}})"})
}}

select * from public.users
```

The config resolution order is:

1. dbt_project.yml `model-defaults`
2. in-model config
3. dbt_project.yml `models` config

### 5. Fix: dbt seed null values https://github.com/dbt-labs/dbt-core/issues/102

Previously, `dbt seed` would insert empty CSV cells as `"None"`, whereas they should have been `NULL`. Not anymore!

---

## dbt v0.3.0

Version 0.3.0 comes with the following updates:

#### 1. Parallel model creation https://github.com/dbt-labs/dbt-core/pull/83

dbt will analyze the model dependency graph and can create models in parallel if possible. In practice, this can significantly speed up the amount of time it takes to complete `dbt run`. The number of threads dbt uses must be between 1 and 8. To configure the number of threads dbt uses, add the `threads` key to your dbt target in `~/.dbt/profiles.yml`, eg:

```yml
user:
  outputs:
    my-redshift:
      type: redshift
      threads: 4         # execute up to 4 models concurrently
      host: localhost
      ...
  run-target: my-redshift
```

For a complete example, check out [a sample profiles.yml file](https://github.com/dbt-labs/dbt-core/blob/master/sample.profiles.yml)

#### 2. Fail only within a single dependency chain https://github.com/dbt-labs/dbt-core/issues/63

If a model cannot be created, it won't crash the entire `dbt run` process. The errant model will fail and all of its descendants will be "skipped". Other models which do not depend on the failing model (or its descendants) will still be created.

#### 3. Logging https://github.com/dbt-labs/dbt-core/issues/64, https://github.com/dbt-labs/dbt-core/issues/65

dbt will log output from the `dbt run` and `dbt test` commands to a configurable logging directory. By default, this directory is called `logs/`. The log filename is `dbt.log` and it is rotated on a daily basic. Logs are kept for 7 days.

To change the name of the logging directory, add the following line to your `dbt_project.yml` file:

```yml
log-path: "my-logging-directory" # will write logs to my-logging-directory/dbt.log
```

#### 4. Minimize time models are unavailable in the database https://github.com/dbt-labs/dbt-core/issues/68

Previously, dbt would create models by:

1. dropping the existing model
2. creating the new model

This resulted in a significant amount of time in which the model was inaccessible to the outside world. Now, dbt creates models by:

1. creating a temporary model `{model-name}__dbt_tmp`
2. dropping the existing model
3. renaming the tmp model name to the actual model name

#### 5. Arbitrarily deep nesting https://github.com/dbt-labs/dbt-core/issues/50

Previously, all models had to be located in a directory matching `models/{model group}/{model_name}.sql`. Now, these models can be nested arbitrarily deeply within a given dbt project. For instance, `models/snowplow/sessions/transformed/transformed_sessions.sql` is a totally valid model location with this release.

To configure these deeply-nested models, just nest the config options within the `dbt_project.yml` file. The only caveat is that you need to specify the dbt project name as the first key under the `models` object, ie:

```yml
models:
  "Your Project Name":
    snowplow:
      sessions:
        transformed:
          transformed_sessions:
            enabled: true
```

More information is available on the [issue](https://github.com/dbt-labs/dbt-core/issues/50) and in the [sample dbt_project.yml file](https://github.com/dbt-labs/dbt-core/blob/master/sample.dbt_project.yml)

#### 6. don't try to create a schema if it already exists https://github.com/dbt-labs/dbt-core/issues/66

dbt run would execute `create schema if not exists {schema}`. This would fail if the dbt user didn't have sufficient permissions to create the schema, even if the schema already existed! Now, dbt checks for the schema existence and only attempts to create the schema if it doesn't already exist.

#### 7. Semantic Versioning

## The previous release of dbt was v0.2.3.0 which isn't a semantic version. This and all future dbt releases will conform to semantic version in the format `{major}.{minor}.{patch}`.

## dbt v0.2.3.0

Version 0.2.3.0 of dbt comes with the following updates:

#### 1. Fix: Flip referential integrity arguments (breaking)

Referential integrity validations in a `schema.yml` file were previously defined relative to the _parent_ table:

```yaml
account:
  constraints:
    relationships:
      - { from: id, to: people, field: account_id }
```

Now, these validations are specified relative to the _child_ table

```yaml
people:
  constraints:
    relationships:
      - { from: account_id, to: accounts, field: id }
```

For more information, run `dbt test -h`

#### 2. Feature: seed tables from a CSV

Previously, auxiliary data needed to be shoehorned into a view comprised of union statements, eg.

```sql
select 22 as "type", 'Chat Transcript' as type_name, 'chatted via olark' as event_name union all
select 21, 'Custom Redirect', 'clicked a custom redirect' union all
select 6, 'Email', 'email sent' union all
...
```

That's not a scalable solution. Now you can load CSV files into your data warehouse:

1. Add a CSV file (with a header) to the `data/` directory
2. Run `dbt seed` to create a table from the CSV file!
3. The table name with be the filename (sans `.csv`) and it will be placed in your `run-target`'s schema

Subsequent calls to `dbt seed` will truncate the seeded tables (if they exist) and re-insert the data. If the table schema changes, you can run `dbt seed --drop-existing` to drop the table and recreate it.

For more information, run `dbt seed -h`

#### 3. Feature: compile analytical queries

Versioning your SQL models with dbt is a great practice, but did you know that you can also version your analyses? Any SQL files in the `analysis/` dir will be compiled (ie. table names will be interpolated) and placed in the `target/build-analysis/` directory. These analytical queries will _not_ be run against your data warehouse with `dbt run` -- you should copy/paste them into the data analysis tool of your choice.

#### 4. Feature: accepted values validation

In your `schema.yml` file, you can now add `accepted-values` validations:

```yaml
accounts:
  constraints:
    accepted-values:
      - { field: type, values: ["paid", "free"] }
```

This test will determine how many records in the `accounts` model have a `type` other than `paid` or `free`.

#### 5. Feature: switch profiles and targets on the command line

Switch between profiles with `--profile [profile-name]` and switch between run-targets with `--target [target-name]`.

Targets should be something like "prod" or "dev" and profiles should be something like "my-org" or "my-side-project"

```yaml
side-project:
  outputs:
    prod:
      type: redshift
      host: localhost
      port: 5439
      user: Drew
      pass:
      dbname: data_generator
      schema: ac_drew
    dev:
      type: redshift
      host: localhost
      port: 5439
      user: Drew
      pass:
      dbname: data_generator
      schema: ac_drew_dev
  run-target: dev
```

To compile models using the `dev` environment of my `side-project` profile:
`$ dbt compile --profile side-project --target dev`
or for `prod`:
`$ dbt compile --profile side-project --target prod`

You can also add a "profile' config to the `dbt_config.yml` file to fix a dbt project to a specific profile:

```yaml
...
test-paths: ["test"]
data-paths: ["data"]

# Fix this project to the "side-project" profile
# You can still use --target to switch between environments!
profile: "side-project"

model-defaults:
....
```
