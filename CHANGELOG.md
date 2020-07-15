## dbt 0.18.0 (Release TBD)

### Breaking changes
- Previously, dbt put macros from all installed plugins into the namespace. This version of dbt will not include adapter plugin macros unless they are from the currently-in-use adapter or one of its dependencies [#2590](https://github.com/fishtown-analytics/dbt/pull/2590)

### Features
- Added support for Snowflake query tags at the connection and model level ([#1030](https://github.com/fishtown-analytics/dbt/issues/1030), [#2555](https://github.com/fishtown-analytics/dbt/pull/2555/))
- Added new node selector methods (`config`, `test_type`, `test_name`, `package`) ([#2425](https://github.com/fishtown-analytics/dbt/issues/2425), [#2629](https://github.com/fishtown-analytics/dbt/pull/2629))
- Added option to specify profile when connecting to Redshift via IAM ([#2437](https://github.com/fishtown-analytics/dbt/issues/2437), [#2581](https://github.com/fishtown-analytics/dbt/pull/2581))

### Fixes
- Adapter plugins can once again override plugins defined in core ([#2548](https://github.com/fishtown-analytics/dbt/issues/2548), [#2590](https://github.com/fishtown-analytics/dbt/pull/2590))

Contributors:
- [@brunomurino](https://github.com/brunomurino) ([#2437](https://github.com/fishtown-analytics/dbt/pull/2581))
- [@DrMcTaco](https://github.com/DrMcTaco) ([#1030](https://github.com/fishtown-analytics/dbt/issues/1030)),[#2555](https://github.com/fishtown-analytics/dbt/pull/2555/))


## dbt 0.18.0b1 (June 08, 2020)


### Features
- Made project-level warnings more apparent ([#2545](https://github.com/fishtown-analytics/dbt/issues/2545))
- Added a `full_refresh` config item that overrides the behavior of the `--full-refresh` flag ([#1009](https://github.com/fishtown-analytics/dbt/issues/1009), [#2348](https://github.com/fishtown-analytics/dbt/pull/2348))
- Added a "docs" field to macros, with a "show" subfield to allow for hiding macros from the documentation site ([#2430](https://github.com/fishtown-analytics/dbt/issues/2430))
- Added intersection syntax for model selector ([#2167](https://github.com/fishtown-analytics/dbt/issues/2167), [#2417](https://github.com/fishtown-analytics/dbt/pull/2417))
- Extends model selection syntax with at most n-th parent/children `dbt run --models 3+m1+2` ([#2052](https://github.com/fishtown-analytics/dbt/issues/2052), [#2485](https://github.com/fishtown-analytics/dbt/pull/2485))
- Added support for renaming BigQuery relations ([#2520](https://github.com/fishtown-analytics/dbt/issues/2520), [#2521](https://github.com/fishtown-analytics/dbt/pull/2521))
- Added support for BigQuery authorized views ([#1718](https://github.com/fishtown-analytics/dbt/issues/1718), [#2517](https://github.com/fishtown-analytics/dbt/pull/2517))
- Added support for altering BigQuery column types ([#2546](https://github.com/fishtown-analytics/dbt/issues/2546), [#2547](https://github.com/fishtown-analytics/dbt/pull/2547))
- Include row counts and bytes processed in log output for all BigQuery statement types ([#2526](https://github.com/fishtown-analytics/dbt/issues/2526))


### Fixes
- Fixed an error in create_adapter_plugins.py script when -dependency arg not passed ([#2507](https://github.com/fishtown-analytics/dbt/issues/2507), [#2508](https://github.com/fishtown-analytics/dbt/pull/2508))
- Remove misleading "Opening a new connection" log message in set_connection_name. ([#2511](https://github.com/fishtown-analytics/dbt/issues/2511))
- Now all the BigQuery statement types return the number of bytes processed ([#2526](https://github.com/fishtown-analytics/dbt/issues/2526)).

Contributors:
 - [@raalsky](https://github.com/Raalsky) ([#2417](https://github.com/fishtown-analytics/dbt/pull/2417), [#2485](https://github.com/fishtown-analytics/dbt/pull/2485))
 - [@alf-mindshift](https://github.com/alf-mindshift) ([#2431](https://github.com/fishtown-analytics/dbt/pull/2431))
 - [@scarrucciu](https://github.com/scarrucciu) ([#2508](https://github.com/fishtown-analytics/dbt/pull/2508))
 - [@southpolemonkey](https://github.com/southpolemonkey) ([#2511](https://github.com/fishtown-analytics/dbt/issues/2511))
 - [@azhard](https://github.com/azhard) ([#2517](https://github.com/fishtown-analytics/dbt/pull/2517), ([#2521](https://github.com/fishtown-analytics/dbt/pull/2521)), [#2547](https://github.com/fishtown-analytics/dbt/pull/2547))
 - [@alepuccetti](https://github.com/alepuccetti) ([#2526](https://github.com/fishtown-analytics/dbt/issues/2526))


## dbt 0.17.1 (Release TBD)


## dbt 0.17.1rc4 (July 08, 2020)


### Fixes
- dbt native rendering now requires an opt-in with the `as_native` filter. Added `as_bool` and `as_number` filters, which are like `as_native` but also type-check. ([#2612](https://github.com/fishtown-analytics/dbt/issues/2612), [#2618](https://github.com/fishtown-analytics/dbt/pull/2618))


## dbt 0.17.1rc3 (July 01, 2020)


### Fixes
- dbt native rendering now avoids turning quoted strings into unquoted strings ([#2597](https://github.com/fishtown-analytics/dbt/issues/2597), [#2599](https://github.com/fishtown-analytics/dbt/pull/2599))
- Hash name of local packages ([#2600](https://github.com/fishtown-analytics/dbt/pull/2600))
- On bigquery, also persist docs for seeds ([#2598](https://github.com/fishtown-analytics/dbt/issues/2598), [#2601](https://github.com/fishtown-analytics/dbt/pull/2601))
- Swallow all file-writing related errors on Windows, regardless of path length or exception type. ([#2603](https://github.com/fishtown-analytics/dbt/pull/2603))


## dbt 0.17.1rc2 (June 25, 2020)

### Fixes
- dbt config-version: 2 now properly defers rendering `+pre-hook` and `+post-hook` fields. ([#2583](https://github.com/fishtown-analytics/dbt/issues/2583), [#2854](https://github.com/fishtown-analytics/dbt/pull/2854))
- dbt handles too-long paths on windows that do not report that the path is too long ([#2591](https://github.com/fishtown-analytics/dbt/pull/2591))


## dbt 0.17.1rc1 (June 19, 2020)


### Fixes
- dbt compile and ls no longer create schemas if they don't already exist ([#2525](https://github.com/fishtown-analytics/dbt/issues/2525), [#2528](https://github.com/fishtown-analytics/dbt/pull/2528))
- `dbt deps` now respects the `--project-dir` flag, so using `dbt deps --project-dir=/some/path` and then `dbt run --project-dir=/some/path` will properly find dependencies ([#2519](https://github.com/fishtown-analytics/dbt/issues/2519), [#2534](https://github.com/fishtown-analytics/dbt/pull/2534))
- `packages.yml` revision/version fields can be float-like again (`revision: '1.0'` is valid). ([#2518](https://github.com/fishtown-analytics/dbt/issues/2518), [#2535](https://github.com/fishtown-analytics/dbt/pull/2535))
<<<<<<< HEAD
- dbt again respects config aliases in config() calls ([#2557](https://github.com/fishtown-analytics/dbt/issues/2557), [#2559](https://github.com/fishtown-analytics/dbt/pull/2559))


=======
- Parallel RPC requests no longer step on each others' arguments ([[#2484](https://github.com/fishtown-analytics/dbt/issues/2484), [#2554](https://github.com/fishtown-analytics/dbt/pull/2554)])
- `persist_docs` now takes into account descriptions for nested columns in bigquery ([#2549](https://github.com/fishtown-analytics/dbt/issues/2549), [#2550](https://github.com/fishtown-analytics/dbt/pull/2550))
- On windows (depending upon OS support), dbt no longer fails with errors when writing artifacts ([#2558](https://github.com/fishtown-analytics/dbt/issues/2558), [#2566](https://github.com/fishtown-analytics/dbt/pull/2566))
- dbt again respects config aliases in config() calls and dbt_project.yml ([#2557](https://github.com/fishtown-analytics/dbt/issues/2557), [#2559](https://github.com/fishtown-analytics/dbt/pull/2559), [#2575](https://github.com/fishtown-analytics/dbt/pull/2575))
- fix unclickable nodes in the dbt Docs DAG viz ([#101](https://github.com/fishtown-analytics/dbt-docs/pull/101))
- fix null database names for Spark projects in dbt Docs site ([#96](https://github.com/fishtown-analytics/dbt-docs/pull/96))

Contributors:
 - [@bodschut](https://github.com/bodschut) ([#2550](https://github.com/fishtown-analytics/dbt/pull/2550))
>>>>>>> dev/0.17.1

## dbt 0.17.0 (June 08, 2020)

### Fixes
- Removed `pytest-logbook` dependency from `dbt-core` ([#2505](https://github.com/fishtown-analytics/dbt/pull/2505))

Contributors:
 - [@aburgel](https://github.com/aburgel) ([#2505](https://github.com/fishtown-analytics/dbt/pull/2505))

## dbt 0.17.0rc4 (June 2, 2020)

### Fixes
- On snowflake, get_columns_in_relation now returns an empty list again if the relation does not exist, instead of raising an exception. ([#2504](https://github.com/fishtown-analytics/dbt/issues/2504), [#2509](https://github.com/fishtown-analytics/dbt/pull/2509))
- Added filename, project, and the value that failed to render to the exception raised when rendering fails. ([#2499](https://github.com/fishtown-analytics/dbt/issues/2499), [#2501](https://github.com/fishtown-analytics/dbt/pull/2501))


### Under the hood
- Lock protobufs to the last version that had fully functioning releases on all supported platforms ([#2490](https://github.com/fishtown-analytics/dbt/issues/2490), [#2491](https://github.com/fishtown-analytics/dbt/pull/2491))


### dbt 0.17.0rc3 (May 27, 2020)


### Fixes
- When no columns are documented and persist_docs.columns is True, skip creating comments instead of failing with errors ([#2439](https://github.com/fishtown-analytics/dbt/issues/2439), [#2440](https://github.com/fishtown-analytics/dbt/pull/2440))
- Fixed an argument issue with the `create_schema` macro on bigquery ([#2445](https://github.com/fishtown-analytics/dbt/issues/2445), [#2448](https://github.com/fishtown-analytics/dbt/pull/2448))
- dbt now logs using the adapter plugin's ideas about how relations should be displayed ([dbt-spark/#74](https://github.com/fishtown-analytics/dbt-spark/issues/74), [#2450](https://github.com/fishtown-analytics/dbt/pull/2450))
- The create_adapter_plugin.py script creates a version 2 dbt_project.yml file ([#2451](https://github.com/fishtown-analytics/dbt/issues/2451), [#2455](https://github.com/fishtown-analytics/dbt/pull/2455))
- Fixed dbt crashing with an AttributeError on duplicate sources ([#2463](https://github.com/fishtown-analytics/dbt/issues/2463), [#2464](https://github.com/fishtown-analytics/dbt/pull/2464))
- Fixed a number of issues with globally-scoped vars ([#2473](https://github.com/fishtown-analytics/dbt/issues/2473), [#2472](https://github.com/fishtown-analytics/dbt/issues/2472), [#2469](https://github.com/fishtown-analytics/dbt/issues/2469), [#2477](https://github.com/fishtown-analytics/dbt/pull/2477))
- Fixed DBT Docker entrypoint ([#2470](https://github.com/fishtown-analytics/dbt/issues/2470), [#2475](https://github.com/fishtown-analytics/dbt/pull/2475))
- Fixed a performance regression that occurred even when a user was not using the relevant feature ([#2474](https://github.com/fishtown-analytics/dbt/issues/2474), [#2478](https://github.com/fishtown-analytics/dbt/pull/2478))
- Substantial performance improvements for parsing on large projects, especially projects with many docs definition. ([#2480](https://github.com/fishtown-analytics/dbt/issues/2480), [#2481](https://github.com/fishtown-analytics/dbt/pull/2481))
- Expose Snowflake query id in case of an exception raised by connector ([#2201](https://github.com/fishtown-analytics/dbt/issues/2201), [#2358](https://github.com/fishtown-analytics/dbt/pull/2358))

### Under the hood
- Better support for optional database fields in adapters ([#2487](https://github.com/fishtown-analytics/dbt/issues/2487) [#2489](https://github.com/fishtown-analytics/dbt/pull/2489))

Contributors:
- [@dmateusp](https://github.com/dmateusp) ([#2475](https://github.com/fishtown-analytics/dbt/pull/2475))
- [@ChristianKohlberg](https://github.com/ChristianKohlberg) (#2358](https://github.com/fishtown-analytics/dbt/pull/2358))

## dbt 0.17.0rc1 (May 12, 2020)

### Breaking changes
- The `list_relations_without_caching`, `drop_schema`, and `create_schema` macros and methods now accept a single argument of a Relation object with no identifier field. ([#2411](https://github.com/fishtown-analytics/dbt/pull/2411))

### Features
- Added warning to nodes selector if nothing was matched ([#2115](https://github.com/fishtown-analytics/dbt/issues/2115), [#2343](https://github.com/fishtown-analytics/dbt/pull/2343))
- Suport column descriptions for BigQuery models ([#2335](https://github.com/fishtown-analytics/dbt/issues/2335), [#2402](https://github.com/fishtown-analytics/dbt/pull/2402))
- Added BigQuery option maximum_bytes_billed to set an upper limit for query costs ([#2346](https://github.com/fishtown-analytics/dbt/issues/2346), [#2427](https://github.com/fishtown-analytics/dbt/pull/2427))

### Fixes
- When tracking is disabled due to errors, do not reset the invocation ID ([#2398](https://github.com/fishtown-analytics/dbt/issues/2398), [#2400](https://github.com/fishtown-analytics/dbt/pull/2400))
- Fix for logic error in compilation errors for duplicate data test names ([#2406](https://github.com/fishtown-analytics/dbt/issues/2406), [#2407](https://github.com/fishtown-analytics/dbt/pull/2407))
- Fix list_schemas macro failing for BigQuery ([#2412](https://github.com/fishtown-analytics/dbt/issues/2412), [#2413](https://github.com/fishtown-analytics/dbt/issues/2413))
- When plugins are installed in the same folder as dbt core, report their versions. ([#2410](https://github.com/fishtown-analytics/dbt/issues/2410), [#2418](https://github.com/fishtown-analytics/dbt/pull/2418))
- Fix for making schema tests work for community plugin [dbt-sqlserver](https://github.com/mikaelene/dbt-sqlserver) [#2414](https://github.com/fishtown-analytics/dbt/pull/2414)
- Fix a bug where quoted uppercase schemas on snowflake were not processed properly during cache building. ([#2403](https://github.com/fishtown-analytics/dbt/issues/2403), [#2411](https://github.com/fishtown-analytics/dbt/pull/2411))
- Fix for extra spacing and parentheses when creating views in BigQuery ([#2421](https://github.com/fishtown-analytics/dbt/issues/2421), [#2422](https://github.com/fishtown-analytics/dbt/issues/2422))

### Docs
- Do not render hidden models in the search bar ([docs#89](https://github.com/fishtown-analytics/dbt-docs/issues/89), [docs#90](https://github.com/fishtown-analytics/dbt-docs/pull/90))

### Under the hood
- Track distinct project hashes in anonymous usage metrics for package downloads ([#2351](https://github.com/fishtown-analytics/dbt/issues/2351), [#2429](https://github.com/fishtown-analytics/dbt/pull/2429))

Contributors:
 - [@azhard](https://github.com/azhard) ([#2413](https://github.com/fishtown-analytics/dbt/pull/2413), [#2422](https://github.com/fishtown-analytics/dbt/pull/2422))
 - [@mikaelene](https://github.com/mikaelene) [#2414](https://github.com/fishtown-analytics/dbt/pull/2414)
 - [@raalsky](https://github.com/Raalsky) ([#2343](https://github.com/fishtown-analytics/dbt/pull/2343))
 - [@haukeduden](https://github.com/haukeduden) ([#2427](https://github.com/fishtown-analytics/dbt/pull/2427))
 - [@alf-mindshift](https://github.com/alf-mindshift) ([docs#90](https://github.com/fishtown-analytics/dbt-docs/pull/90))

## dbt 0.17.0b1 (May 5, 2020)

### Breaking changes
- Added a new dbt_project.yml version format. This emits a deprecation warning currently, but support for the existing version will be removed in a future dbt version ([#2300](https://github.com/fishtown-analytics/dbt/issues/2300), [#2312](https://github.com/fishtown-analytics/dbt/pull/2312))
- The `graph` object available in some dbt contexts now has an additional member `sources` (along side the existing `nodes`). Sources have been removed from `nodes` and added to `sources` instead ([#2312](https://github.com/fishtown-analytics/dbt/pull/2312))
- The 'location' field has been removed from bigquery catalogs ([#2382](https://github.com/fishtown-analytics/dbt/pull/2382))

### Features
- Added --fail-fast argument for dbt run and dbt test to fail on first test failure or runtime error. ([#1649](https://github.com/fishtown-analytics/dbt/issues/1649), [#2224](https://github.com/fishtown-analytics/dbt/pull/2224))
- Support for appending query comments to SQL queries. ([#2138](https://github.com/fishtown-analytics/dbt/issues/2138), [#2199](https://github.com/fishtown-analytics/dbt/pull/2199))
- Added a `get-manifest` API call. ([#2168](https://github.com/fishtown-analytics/dbt/issues/2168), [#2232](https://github.com/fishtown-analytics/dbt/pull/2232))
- Support adapter-specific aliases (like `project` and `dataset` on BigQuery) in source definitions. ([#2133](https://github.com/fishtown-analytics/dbt/issues/2133), [#2244](https://github.com/fishtown-analytics/dbt/pull/2244))
- Users can now use jinja as arguments to tests. Test arguments are rendered in the native context and injected into the test execution context directly. ([#2149](https://github.com/fishtown-analytics/dbt/issues/2149), [#2220](https://github.com/fishtown-analytics/dbt/pull/2220))
- Added support for `db_groups` and `autocreate` flags in Redshift configurations.  ([#1995](https://github.com/fishtown-analytics/dbt/issues/1995), [#2262](https://github.com/fishtown-analytics/dbt/pull/2262))
- Users can supply paths as arguments to `--models` and `--select`, either explicitily by prefixing with `path:` or implicitly with no prefix. ([#454](https://github.com/fishtown-analytics/dbt/issues/454), [#2258](https://github.com/fishtown-analytics/dbt/pull/2258))
- dbt now builds the relation cache for "dbt compile" and "dbt ls" as well as "dbt run" ([#1705](https://github.com/fishtown-analytics/dbt/issues/1705), [#2319](https://github.com/fishtown-analytics/dbt/pull/2319))
- Snowflake now uses "show terse objects" to build the relations cache instead of selecting from the information schema ([#2174](https://github.com/fishtown-analytics/dbt/issues/2174), [#2322](https://github.com/fishtown-analytics/dbt/pull/2322))
- Snowflake now uses "describe table" to get the columns in a relation ([#2260](https://github.com/fishtown-analytics/dbt/issues/2260), [#2324](https://github.com/fishtown-analytics/dbt/pull/2324))
- Add a 'depends_on' attribute to the log record extra field ([#2316](https://github.com/fishtown-analytics/dbt/issues/2316), [#2341](https://github.com/fishtown-analytics/dbt/pull/2341))
- Added a '--no-browser' argument to "dbt docs serve" so you can serve docs in an environment that only has a CLI browser which would otherwise deadlock dbt ([#2004](https://github.com/fishtown-analytics/dbt/issues/2004), [#2364](https://github.com/fishtown-analytics/dbt/pull/2364))
-  Snowflake now uses "describe table" to get the columns in a relation ([#2260](https://github.com/fishtown-analytics/dbt/issues/2260), [#2324](https://github.com/fishtown-analytics/dbt/pull/2324))
- Sources (and therefore freshness tests) can be enabled and disabled via dbt_project.yml ([#2283](https://github.com/fishtown-analytics/dbt/issues/2283), [#2312](https://github.com/fishtown-analytics/dbt/pull/2312), [#2357](https://github.com/fishtown-analytics/dbt/pull/2357))
- schema.yml files are now fully rendered in a context that is aware of vars declared in from dbt_project.yml files ([#2269](https://github.com/fishtown-analytics/dbt/issues/2269), [#2357](https://github.com/fishtown-analytics/dbt/pull/2357))
- Sources from dependencies can be overridden in schema.yml files ([#2287](https://github.com/fishtown-analytics/dbt/issues/2287), [#2357](https://github.com/fishtown-analytics/dbt/pull/2357))
- Implement persist_docs for both `relation` and `comments` on postgres and redshift, and extract them when getting the catalog. ([#2333](https://github.com/fishtown-analytics/dbt/issues/2333), [#2378](https://github.com/fishtown-analytics/dbt/pull/2378))
- Added a filter named `as_text` to the native environment rendering code that allows users to mark a value as always being a string ([#2384](https://github.com/fishtown-analytics/dbt/issues/2384), [#2395](https://github.com/fishtown-analytics/dbt/pull/2395))
- Relation comments supported for Snowflake tables and views. Column comments supported for tables. ([#1722](https://github.com/fishtown-analytics/dbt/issues/1722), [#2321](https://github.com/fishtown-analytics/dbt/pull/2321))

### Fixes
- When a jinja value is undefined, give a helpful error instead of failing with cryptic "cannot pickle ParserMacroCapture" errors ([#2110](https://github.com/fishtown-analytics/dbt/issues/2110), [#2184](https://github.com/fishtown-analytics/dbt/pull/2184))
- Added timeout to registry download call ([#2195](https://github.com/fishtown-analytics/dbt/issues/2195), [#2228](https://github.com/fishtown-analytics/dbt/pull/2228))
- When a macro is called with invalid arguments, include the calling model in the output ([#2073](https://github.com/fishtown-analytics/dbt/issues/2073), [#2238](https://github.com/fishtown-analytics/dbt/pull/2238))
- When a warn exception is not in a jinja do block, return an empty string instead of None ([#2222](https://github.com/fishtown-analytics/dbt/issues/2222), [#2259](https://github.com/fishtown-analytics/dbt/pull/2259))
- Add dbt plugin versions to --version([#2272](https://github.com/fishtown-analytics/dbt/issues/2272), [#2279](https://github.com/fishtown-analytics/dbt/pull/2279))
- When a Redshift table is defined as "auto", don't provide diststyle ([#2246](https://github.com/fishtown-analytics/dbt/issues/2246), [#2298](https://github.com/fishtown-analytics/dbt/pull/2298))
- Made file names lookups case-insensitve (.sql, .SQL, .yml, .YML) and if .yaml files are found, raise a warning indicating dbt will parse these files in future releases. ([#1681](https://github.com/fishtown-analytics/dbt/issues/1681), [#2263](https://github.com/fishtown-analytics/dbt/pull/2263))
- Return error message when profile is empty in profiles.yml. ([#2292](https://github.com/fishtown-analytics/dbt/issues/2292), [#2297](https://github.com/fishtown-analytics/dbt/pull/2297))
- Fix skipped node count in stdout at the end of a run ([#2095](https://github.com/fishtown-analytics/dbt/issues/2095), [#2310](https://github.com/fishtown-analytics/dbt/pull/2310))
- Fix an issue where BigQuery incorrectly used a relation's quote policy as the basis for the information schema's include policy, instead of the relation's include policy. ([#2188](https://github.com/fishtown-analytics/dbt/issues/2188), [#2325](https://github.com/fishtown-analytics/dbt/pull/2325))
- Fix "dbt deps" command so it respects the "--project-dir" arg if specified. ([#2338](https://github.com/fishtown-analytics/dbt/issues/2338), [#2339](https://github.com/fishtown-analytics/dbt/issues/2339))
- On `run_cli` API calls that are passed `--vars` differing from the server's `--vars`, the RPC server rebuilds the manifest for that call. ([#2265](https://github.com/fishtown-analytics/dbt/issues/2265), [#2363](https://github.com/fishtown-analytics/dbt/pull/2363))
- Remove the query job SQL from bigquery exceptions ([#2383](https://github.com/fishtown-analytics/dbt/issues/2383), [#2393](https://github.com/fishtown-analytics/dbt/pull/2393))
- Fix "Object of type Decimal is not JSON serializable" error when BigQuery queries returned numeric types in nested data structures ([#2336](https://github.com/fishtown-analytics/dbt/issues/2336), [#2348](https://github.com/fishtown-analytics/dbt/pull/2348))
- No longer query the information_schema.schemata view on bigquery ([#2320](https://github.com/fishtown-analytics/dbt/issues/2320), [#2382](https://github.com/fishtown-analytics/dbt/pull/2382))
- Preserve original subdirectory structure in compiled files. ([#2173](https://github.com/fishtown-analytics/dbt/issues/2173), [#2349](https://github.com/fishtown-analytics/dbt/pull/2349))
- Add support for `sql_header` config in incremental models ([#2136](https://github.com/fishtown-analytics/dbt/issues/2136), [#2200](https://github.com/fishtown-analytics/dbt/pull/2200))
- The ambiguous alias check now examines the node's database value as well as the schema/identifier ([#2326](https://github.com/fishtown-analytics/dbt/issues/2326), [#2387](https://github.com/fishtown-analytics/dbt/pull/2387))
- Postgres array types can now be returned via `run_query` macro calls ([#2337](https://github.com/fishtown-analytics/dbt/issues/2337), [#2376](https://github.com/fishtown-analytics/dbt/pull/2376))
- Add missing comma to `dbt compile` help text  ([#2388](https://github.com/fishtown-analytics/dbt/issues/2388) [#2389](https://github.com/fishtown-analytics/dbt/pull/2389))
- Fix for non-atomic snapshot staging table creation ([#1884](https://github.com/fishtown-analytics/dbt/issues/1884), [#2390](https://github.com/fishtown-analytics/dbt/pull/2390))
- Fix for snapshot errors when strategy changes from `check` to `timestamp` between runs ([#2350](https://github.com/fishtown-analytics/dbt/issues/2350), [#2391](https://github.com/fishtown-analytics/dbt/pull/2391))

### Under the hood
- Added more tests for source inheritance ([#2264](https://github.com/fishtown-analytics/dbt/issues/2264), [#2291](https://github.com/fishtown-analytics/dbt/pull/2291))
- Update documentation website for 0.17.0 ([#2284](https://github.com/fishtown-analytics/dbt/issues/2284))

Contributors:
 - [@raalsky](https://github.com/Raalsky) ([#2224](https://github.com/fishtown-analytics/dbt/pull/2224), [#2228](https://github.com/fishtown-analytics/dbt/pull/2228))
 - [@ilkinulas](https://github.com/ilkinulas) [#2199](https://github.com/fishtown-analytics/dbt/pull/2199)
 - [@kyleabeauchamp](https://github.com/kyleabeauchamp) [#2262](https://github.com/fishtown-analytics/dbt/pull/2262)
 - [@jeremyyeo](https://github.com/jeremyyeo) [#2259](https://github.com/fishtown-analytics/dbt/pull/2259)
 - [@rodrigodelmonte](https://github.com/rodrigodelmonte) [#2298](https://github.com/fishtown-analytics/dbt/pull/2298)
 - [@sumanau7](https://github.com/sumanau7) ([#2279](https://github.com/fishtown-analytics/dbt/pull/2279), [#2263](https://github.com/fishtown-analytics/dbt/pull/2263), [#2297](https://github.com/fishtown-analytics/dbt/pull/2297))
 - [@nickwu241](https://github.com/nickwu241) [#2339](https://github.com/fishtown-analytics/dbt/issues/2339)
 - [@Fokko](https://github.com/Fokko) [#2361](https://github.com/fishtown-analytics/dbt/pull/2361)
 - [@franloza](https://github.com/franloza) [#2349](https://github.com/fishtown-analytics/dbt/pull/2349)
 - [@sethwoodworth](https://github.com/sethwoodworth) [#2389](https://github.com/fishtown-analytics/dbt/pull/2389)
 - [@snowflakeseitz](https://github.com/snowflakeseitz) [#2321](https://github.com/fishtown-analytics/dbt/pull/2321)

## dbt 0.16.1 (April 14, 2020)

### Features
- Support for appending query comments to SQL queries. ([#2138](https://github.com/fishtown-analytics/dbt/issues/2138) [#2199](https://github.com/fishtown-analytics/dbt/issues/2199))

### Fixes
- dbt now renders the project name in the "base" context, in particular giving it access to `var` and `env_var` ([#2230](https://github.com/fishtown-analytics/dbt/issues/2230), [#2251](https://github.com/fishtown-analytics/dbt/pull/2251))
- Fix an issue with raw blocks where multiple raw blocks in the same file resulted in an error ([#2241](https://github.com/fishtown-analytics/dbt/issues/2241), [#2252](https://github.com/fishtown-analytics/dbt/pull/2252))
- Fix a redshift-only issue that caused an error when `dbt seed` found a seed with an entirely empty column that was set to a `varchar` data type. ([#2250](https://github.com/fishtown-analytics/dbt/issues/2250), [#2254](https://github.com/fishtown-analytics/dbt/pull/2254))
- Fix a bug where third party plugins that used the default `list_schemas` and `information_schema_name` macros with database quoting enabled double-quoted the database name in their queries ([#2267](https://github.com/fishtown-analytics/dbt/issues/2267), [#2281](https://github.com/fishtown-analytics/dbt/pull/2281))
- The BigQuery "partitions" config value can now be used in `dbt_project.yml` ([#2256](https://github.com/fishtown-analytics/dbt/issues/2256), [#2280](https://github.com/fishtown-analytics/dbt/pull/2280))
- dbt deps once again does not require a profile, but if profile-specific fields are accessed users will get an error ([#2231](https://github.com/fishtown-analytics/dbt/issues/2231), [#2290](https://github.com/fishtown-analytics/dbt/pull/2290))
- Macro name collisions between dbt and plugins now raise an appropriate exception, instead of an AttributeError ([#2288](https://github.com/fishtown-analytics/dbt/issues/2288), [#2293](https://github.com/fishtown-analytics/dbt/pull/2293))
- The create_adapter_plugin.py script has been updated to support 0.16.X adapters ([#2145](https://github.com/fishtown-analytics/dbt/issues/2145), [#2294](https://github.com/fishtown-analytics/dbt/pull/2294))

### Under the hood
- Pin google libraries to higher minimum values, add more dependencies as explicit ([#2233](https://github.com/fishtown-analytics/dbt/issues/2233), [#2249](https://github.com/fishtown-analytics/dbt/pull/2249))

Contributors:
 - [@ilkinulas](https://github.com/ilkinulas) [#2199](https://github.com/fishtown-analytics/dbt/pull/2199)

## dbt 0.16.0 (March 23, 2020)

## dbt 0.16.0rc4 (March 20, 2020)

### Fixes
- When dbt encounters databases, schemas, or tables with names that look like numbers, treat them as strings ([#2206](https://github.com/fishtown-analytics/dbt/issues/2206), [#2208](https://github.com/fishtown-analytics/dbt/pull/2208))
- Increased the lower bound for google-cloud-bigquery ([#2213](https://github.com/fishtown-analytics/dbt/issues/2213), [#2214](https://github.com/fishtown-analytics/dbt/pull/2214))

## dbt 0.16.0rc3 (March 11, 2020)

### Fixes
- If database quoting is enabled, do not attempt to create schemas that already exist ([#2186](https://github.com/fishtown-analytics/dbt/issues/2186), [#2187](https://github.com/fishtown-analytics/dbt/pull/2187))

### Features
- Support for appending query comments to SQL queries. ([#2138](https://github.com/fishtown-analytics/dbt/issues/2138))

## dbt 0.16.0rc2 (March 4, 2020)

### Under the hood
- Pin cffi to <1.14 to avoid a version conflict with snowflake-connector-python ([#2180](https://github.com/fishtown-analytics/dbt/issues/2180), [#2181](https://github.com/fishtown-analytics/dbt/pull/2181))

## dbt 0.16.0rc1 (March 4, 2020)

### Breaking changes
- When overriding the snowflake__list_schemas macro, you must now run a result with a column named 'name' instead of the first column ([#2171](https://github.com/fishtown-analytics/dbt/pull/2171))
- dbt no longer supports databases with greater than 10,000 schemas ([#2171](https://github.com/fishtown-analytics/dbt/pull/2171))

### Features
- Remove the requirement to have a passphrase when using Snowflake key pair authentication ([#1805](https://github.com/fishtown-analytics/dbt/issues/1805), [#2164](https://github.com/fishtown-analytics/dbt/pull/2164))
- Adding optional "sslmode" parameter for postgres ([#2152](https://github.com/fishtown-analytics/dbt/issues/2152), [#2154](https://github.com/fishtown-analytics/dbt/pull/2154))
- Docs website changes:
  - Handle non-array `accepted_values` test arguments ([dbt-docs#70](https://github.com/fishtown-analytics/dbt-docs/pull/70))
  - Support filtering by resource type ([dbt-docs#77](https://github.com/fishtown-analytics/dbt-docs/pull/77))
  - Render analyses, macros, and custom data tests ([dbt-docs#72](https://github.com/fishtown-analytics/dbt-docs/pull/72), [dbt-docs#77](https://github.com/fishtown-analytics/dbt-docs/pull/77), [dbt-docs#69](https://github.com/fishtown-analytics/dbt-docs/pull/69))
  - Support hiding models from the docs (these nodes still render in the DAG view as "hidden") ([dbt-docs#71](https://github.com/fishtown-analytics/dbt-docs/pull/71))
  - Render `meta` fields as "details" in node views ([dbt-docs#73](https://github.com/fishtown-analytics/dbt-docs/pull/73))
  - Default to lower-casing Snowflake columns specified in all-caps ([dbt-docs#74](https://github.com/fishtown-analytics/dbt-docs/pull/74))
  - Upgrade site dependencies
- Support `insert_overwrite` materializtion for BigQuery incremental models ([#2153](https://github.com/fishtown-analytics/dbt/pull/2153))


### Under the hood
- Use `show terse schemas in database` (chosen based on data collected by Michael Weinberg) instead of `select ... from information_schema.schemata` when collecting the list of schemas in a database ([#2166](https://github.com/fishtown-analytics/dbt/issues/2166), [#2171](https://github.com/fishtown-analytics/dbt/pull/2171))
- Parallelize filling the cache and listing schemas in each database during startup ([#2127](https://github.com/fishtown-analytics/dbt/issues/2127), [#2157](https://github.com/fishtown-analytics/dbt/pull/2157))

Contributors:
 - [@mhmcdonald](https://github.com/mhmcdonald) ([#2164](https://github.com/fishtown-analytics/dbt/pull/2164))
 - [@dholleran-lendico](https://github.com/dholleran-lendico) ([#2154](https://github.com/fishtown-analytics/dbt/pull/2154))

## dbt 0.16.0b3 (February 26, 2020)

### Breaking changes
- Arguments to source tests are not parsed in the config-rendering context, and are passed as their literal unparsed values to macros ([#2150](https://github.com/fishtown-analytics/dbt/pull/2150))
- `generate_schema_name` macros that accept a single argument are no longer supported ([#2143](https://github.com/fishtown-analytics/dbt/pull/2143))

### Features
- Add a "docs" field to models, with a "show" subfield ([#1671](https://github.com/fishtown-analytics/dbt/issues/1671), [#2107](https://github.com/fishtown-analytics/dbt/pull/2107))
- Add an optional "sslmode" parameter for postgres ([#2152](https://github.com/fishtown-analytics/dbt/issues/2152), [#2154](https://github.com/fishtown-analytics/dbt/pull/2154))
- Remove the requirement to have a passphrase when using Snowflake key pair authentication ([#1804](https://github.com/fishtown-analytics/dbt/issues/1805), [#2164](https://github.com/fishtown-analytics/dbt/pull/2164))
- Support a cost-effective approach for incremental models on BigQuery using scription ([#1034](https://github.com/fishtown-analytics/dbt/issues/1034), [#2140](https://github.com/fishtown-analytics/dbt/pull/2140))
- Add a dbt-{dbt_version} user agent field to the bigquery connector ([#2121](https://github.com/fishtown-analytics/dbt/issues/2121), [#2146](https://github.com/fishtown-analytics/dbt/pull/2146))
- Add support for `generate_database_name` macro ([#1695](https://github.com/fishtown-analytics/dbt/issues/1695), [#2143](https://github.com/fishtown-analytics/dbt/pull/2143))
- Expand the search path for schema.yml (and by extension, the default docs path) to include macro-paths and analysis-paths (in addition to source-paths, data-paths, and snapshot-paths) ([#2155](https://github.com/fishtown-analytics/dbt/issues/2155), [#2160](https://github.com/fishtown-analytics/dbt/pull/2160))

### Fixes
- Fix issue where dbt did not give an error in the presence of duplicate doc names ([#2054](https://github.com/fishtown-analytics/dbt/issues/2054), [#2080](https://github.com/fishtown-analytics/dbt/pull/2080))
- Include vars provided to the cli method when running the actual method ([#2092](https://github.com/fishtown-analytics/dbt/issues/2092), [#2104](https://github.com/fishtown-analytics/dbt/pull/2104))
- Improved error messages with malformed packages.yml ([#2017](https://github.com/fishtown-analytics/dbt/issues/2017), [#2078](https://github.com/fishtown-analytics/dbt/pull/2078))
- Fix an issue where dbt rendered source test args, fix issue where dbt ran an extra compile pass over the wrapped SQL. ([#2114](https://github.com/fishtown-analytics/dbt/issues/2114), [#2150](https://github.com/fishtown-analytics/dbt/pull/2150))
- Set more upper bounds for jinja2,requests, and idna dependencies, upgrade snowflake-connector-python ([#2147](https://github.com/fishtown-analytics/dbt/issues/2147), [#2151](https://github.com/fishtown-analytics/dbt/pull/2151))

Contributors:
 - [@bubbomb](https://github.com/bubbomb) ([#2080](https://github.com/fishtown-analytics/dbt/pull/2080))
 - [@sonac](https://github.com/sonac) ([#2078](https://github.com/fishtown-analytics/dbt/pull/2078))

## dbt 0.16.0b1 (February 11, 2020)

### Breaking changes
- Update the debug log format ([#2099](https://github.com/fishtown-analytics/dbt/pull/2099))
- Removed `docrefs` from output ([#2096](https://github.com/fishtown-analytics/dbt/pull/2096))
- Contexts updated to be more consistent and well-defined ([#1053](https://github.com/fishtown-analytics/dbt/issues/1053), [#1981](https://github.com/fishtown-analytics/dbt/issues/1981), [#1255](https://github.com/fishtown-analytics/dbt/issues/1255), [#2085](https://github.com/fishtown-analytics/dbt/pull/2085))
- The syntax of the `get_catalog` macro has changed ([#2037](https://github.com/fishtown-analytics/dbt/pull/2037))
- Agate type inference is no longer locale-specific. Only a small number of date/datetime formats are supported. If a seed has a specified column type, agate will not perform any type inference (it will instead be cast from a string). ([#999](https://github.com/fishtown-analytics/dbt/issues/999), [#1639](https://github.com/fishtown-analytics/dbt/issues/1639), [#1920](https://github.com/fishtown-analytics/dbt/pull/1920))

### Features
- Add column-level quoting control for tests ([#2106](https://github.com/fishtown-analytics/dbt/issues/2106), [#2047](https://github.com/fishtown-analytics/dbt/pull/2047))
- Add the macros every node uses to its `depends_on.macros` list ([#2082](https://github.com/fishtown-analytics/dbt/issues/2082), [#2103](https://github.com/fishtown-analytics/dbt/pull/2103))
- Add `arguments` field to macros ([#2081](https://github.com/fishtown-analytics/dbt/issues/2081), [#2083](https://github.com/fishtown-analytics/dbt/issues/2083), [#2096](https://github.com/fishtown-analytics/dbt/pull/2096))
- Batch the anonymous usage statistics requests to improve performance ([#2008](https://github.com/fishtown-analytics/dbt/issues/2008), [#2089](https://github.com/fishtown-analytics/dbt/pull/2089))
- Add documentation for macros/analyses ([#1041](https://github.com/fishtown-analytics/dbt/issues/1041), [#2068](https://github.com/fishtown-analytics/dbt/pull/2068))
- Search for docs in 'data' and 'snapshots' folders, in addition to 'models' ([#1832](https://github.com/fishtown-analytics/dbt/issues/1832), [#2058](https://github.com/fishtown-analytics/dbt/pull/2058))
- Add documentation for snapshots and seeds ([#1974](https://github.com/fishtown-analytics/dbt/issues/1974), [#2051](https://github.com/fishtown-analytics/dbt/pull/2051))
- Add `Column.is_number`/`Column.is_float` methods ([#1969](https://github.com/fishtown-analytics/dbt/issues/1969), [#2046](https://github.com/fishtown-analytics/dbt/pull/2046))
- Detect duplicate macros and cause an error when they are detected ([#1891](https://github.com/fishtown-analytics/dbt/issues/1891), [#2045](https://github.com/fishtown-analytics/dbt/pull/2045))
- Add support for `--select` on `dbt seed` ([#1711](https://github.com/fishtown-analytics/dbt/issues/1711), [#2042](https://github.com/fishtown-analytics/dbt/pull/2042))
- Add tags for sources (like model tags) and columns (tags apply to tests of that column) ([#1906](https://github.com/fishtown-analytics/dbt/issues/1906), [#1586](https://github.com/fishtown-analytics/dbt/issues/1586), [#2039](https://github.com/fishtown-analytics/dbt/pull/2039))
- Improve the speed of catalog generation by performing multiple smaller queries instead of one huge query ([#2009](https://github.com/fishtown-analytics/dbt/issues/2009), [#2037](https://github.com/fishtown-analytics/dbt/pull/2037))
- Add`toyaml` and `fromyaml` methods to the base context ([#1911](https://github.com/fishtown-analytics/dbt/issues/1911), [#2036](https://github.com/fishtown-analytics/dbt/pull/2036))
- Add `database_schemas` to the on-run-end context ([#1924](https://github.com/fishtown-analytics/dbt/issues/1924), [#2031](https://github.com/fishtown-analytics/dbt/pull/2031))
- Add the concept of `builtins` to the dbt context, make it possible to override functions like `ref` ([#1603](https://github.com/fishtown-analytics/dbt/issues/1603), [#2028](https://github.com/fishtown-analytics/dbt/pull/2028))
- Add a `meta` key to most `schema.yml` objects ([#1362](https://github.com/fishtown-analytics/dbt/issues/1362), [#2015](https://github.com/fishtown-analytics/dbt/pull/2015))
- Add clickable docs URL link in CLI output ([#2027](https://github.com/fishtown-analytics/dbt/issues/2027), [#2131](https://github.com/fishtown-analytics/dbt/pull/2131))
- Add `role` parameter in Postgres target configuration ([#1955](https://github.com/fishtown-analytics/dbt/issues/1955), [#2137](https://github.com/fishtown-analytics/dbt/pull/2137))
- Parse model hooks and collect `ref` statements ([#1957](https://github.com/fishtown-analytics/dbt/issues/1957), [#2025](https://github.com/fishtown-analytics/dbt/pull/2025))

### Fixes
- Fix the help output for `dbt docs` and `dbt source` to not include misleading flags ([#2038](https://github.com/fishtown-analytics/dbt/issues/2038), [#2105](https://github.com/fishtown-analytics/dbt/pull/2105))
- Allow `dbt debug` from subdirectories ([#2086](https://github.com/fishtown-analytics/dbt/issues/2086), [#2094](https://github.com/fishtown-analytics/dbt/pull/2094))
- Fix the `--no-compile` flag to `dbt docs generate` not crash dbt ([#2090](https://github.com/fishtown-analytics/dbt/issues/2090), [#2093](https://github.com/fishtown-analytics/dbt/pull/2093))
- Fix issue running `dbt debug` with an empty `dbt_project.yml` file ([#2116](https://github.com/fishtown-analytics/dbt/issues/2116), [#2120](https://github.com/fishtown-analytics/dbt/pull/2120))
- Ovewrwrite source config fields that should clobber, rather than deep merging them ([#2049](https://github.com/fishtown-analytics/dbt/issues/2049), [#2062](https://github.com/fishtown-analytics/dbt/pull/2062))
- Fix a bug in macro search where built-in macros could not be overridden for `dbt run-operation` ([#2032](https://github.com/fishtown-analytics/dbt/issues/2032), [#2035](https://github.com/fishtown-analytics/dbt/pull/2035))
- dbt now detects dependencies with the same name as the current project as an error instead of silently clobbering each other ([#2029](https://github.com/fishtown-analytics/dbt/issues/2029), [#2030](https://github.com/fishtown-analytics/dbt/pull/2030))
- Exclude tests of disabled models in compile statistics ([#1804](https://github.com/fishtown-analytics/dbt/issues/1804), [#2026](https://github.com/fishtown-analytics/dbt/pull/2026))
- Do not show ephemeral models as being cancelled during ctrl+c ([#1993](https://github.com/fishtown-analytics/dbt/issues/1993), [#2024](https://github.com/fishtown-analytics/dbt/pull/2024))
- Improve errors on plugin import failure ([#2006](https://github.com/fishtown-analytics/dbt/issues/2006), [#2022](https://github.com/fishtown-analytics/dbt/pull/2022))
- Fix the behavior of the `project-dir` argument when running `dbt debug` ([#1733](https://github.com/fishtown-analytics/dbt/issues/1733), [#1989](https://github.com/fishtown-analytics/dbt/pull/1989))

### Under the hood
- Improve the CI process for externally-contributed PRs ([#2033](https://github.com/fishtown-analytics/dbt/issues/2033), [#2097](https://github.com/fishtown-analytics/dbt/pull/2097))
- lots and lots of mypy/typing fixes ([#2010](https://github.com/fishtown-analytics/dbt/pull/2010))

Contributors:
 - [@aaronsteers](https://github.com/aaronsteers) ([#2131](https://github.com/fishtown-analytics/dbt/pull/2131))
 - [@alanmcruickshank](https://github.com/alanmcruickshank) ([#2028](https://github.com/fishtown-analytics/dbt/pull/2028))
 - [@franloza](https://github.com/franloza) ([#1989](https://github.com/fishtown-analytics/dbt/pull/1989))
 - [@heisencoder](https://github.com/heisencoder) ([#2099](https://github.com/fishtown-analytics/dbt/pull/2099))
 - [@nchammas](https://github.com/nchammas) ([#2120](https://github.com/fishtown-analytics/dbt/pull/2120))
 - [@NiallRees](https://github.com/NiallRees) ([#2026](https://github.com/fishtown-analytics/dbt/pull/2026))
 - [@shooka](https://github.com/shooka) ([#2137](https://github.com/fishtown-analytics/dbt/pull/2137))
 - [@tayloramurphy](https://github.com/tayloramurphy) ([#2015](https://github.com/fishtown-analytics/dbt/pull/2015))

## dbt 0.15.3 (February 19, 2020)

This is a bugfix release.

### Fixes
- Use refresh tokens in snowflake instead of access tokens ([#2126](https://github.com/fishtown-analytics/dbt/issues/2126), [#2141](https://github.com/fishtown-analytics/dbt/pull/2141))

## dbt 0.15.2 (February 2, 2020)

This is a bugfix release.

### Features
- Add support for Snowflake OAuth authentication ([#2050](https://github.com/fishtown-analytics/dbt/issues/2050), [#2069](https://github.com/fishtown-analytics/dbt/pull/2069))
- Add a -t flag as an alias for `dbt run --target` ([#1281](https://github.com/fishtown-analytics/dbt/issues/1281), [#2057](https://github.com/fishtown-analytics/dbt/pull/2057))

### Fixes
- Fix for UnicodeDecodeError when installing dbt via pip ([#1771](https://github.com/fishtown-analytics/dbt/issues/1771), [#2076](https://github.com/fishtown-analytics/dbt/pull/2076))
- Fix for ability to clean "protected" paths in the `dbt clean` command and improve logging ([#2059](https://github.com/fishtown-analytics/dbt/issues/2059), [#2060](https://github.com/fishtown-analytics/dbt/pull/2060))
- Fix for dbt server error when `{% docs %}` tags are malformed ([#2066](https://github.com/fishtown-analytics/dbt/issues/2066), [#2067](https://github.com/fishtown-analytics/dbt/pull/2067))
- Fix for errant duplicate resource errors when models are disabled and partial parsing is enabled ([#2055](https://github.com/fishtown-analytics/dbt/issues/2055), [#2056](https://github.com/fishtown-analytics/dbt/pull/2056))
- Fix for errant duplicate resource errors when a resource is included in multiple source paths ([#2064](https://github.com/fishtown-analytics/dbt/issues/2064), [#2065](https://github.com/fishtown-analytics/dbt/pull/2065/files))

Contributors:
 - [@markberger](https://github.com/markeberger) ([#2076](https://github.com/fishtown-analytics/dbt/pull/2076))
 - [@emilieschario](https://github.com/emilieschario) ([#2060](https://github.com/fishtown-analytics/dbt/pull/2060))

## dbt 0.15.1 (January 17, 2020)

This is a bugfix release.

### Features
- Lazily load database connections ([#1584](https://github.com/fishtown-analytics/dbt/issues/1584), [#1992](https://github.com/fishtown-analytics/dbt/pull/1992))
- Support raising warnings in user-space ([#1970](https://github.com/fishtown-analytics/dbt/issues/1970), [#1977](https://github.com/fishtown-analytics/dbt/pull/1977))
- Suppport BigQuery label configuration for models ([#1942](https://github.com/fishtown-analytics/dbt/issues/1942), [#1964](https://github.com/fishtown-analytics/dbt/pull/1964))
- Support retrying when BigQuery models fail with server errors ([#1579](https://github.com/fishtown-analytics/dbt/issues/1579), [#1963](https://github.com/fishtown-analytics/dbt/pull/1963))
- Support sql headers in create table/view statements ([#1879](https://github.com/fishtown-analytics/dbt/issues/1879), [#1967](https://github.com/fishtown-analytics/dbt/pull/1967))
- Add source snapshot-freshness to dbt rpc ([#2040](https://github.com/fishtown-analytics/dbt/issues/2040), [#2041](https://github.com/fishtown-analytics/dbt/pull/2041))

### Fixes
- Fix for catalog generation error when datasets are missing on BigQuery ([#1984](https://github.com/fishtown-analytics/dbt/issues/1984), [#2005](https://github.com/fishtown-analytics/dbt/pull/2005))
- Fix for invalid SQL generated when "check" strategy is used in Snapshots with changing schemas ([#1797](https://github.com/fishtown-analytics/dbt/issues/1797), [#2001](https://github.com/fishtown-analytics/dbt/pull/2001)(
- Fix for gaps in valid_from and valid_to timestamps when "check" strategy is used in Snapshots on some databases ([#1736](https://github.com/fishtown-analytics/dbt/issues/1736), [#1994](https://github.com/fishtown-analytics/dbt/pull/1994))
- Fix incorrect thread names in dbt server logs ([#1905](https://github.com/fishtown-analytics/dbt/issues/1905), [#2002](https://github.com/fishtown-analytics/dbt/pull/2002))
- Fix for ignored catalog data when user schemas begin with `pg*` on Postgres and Redshift ([#1960](https://github.com/fishtown-analytics/dbt/issues/1960), [#2003](https://github.com/fishtown-analytics/dbt/pull/2003))
- Fix for poorly defined materialization resolution logic ([#1962](https://github.com/fishtown-analytics/dbt/issues/1962), [#1976](https://github.com/fishtown-analytics/dbt/pull/1976))
- Fix missing `drop_schema` method in adapter namespace ([#1980](https://github.com/fishtown-analytics/dbt/issues/1980), [#1983](https://github.com/fishtown-analytics/dbt/pull/1983))
- Fix incorrect `generated_at` value in the catalog ([#1988](https://github.com/fishtown-analytics/dbt/pull/1988))

### Under the hood
- Fail more gracefully at install time when setuptools is downlevel ([#1975](https://github.com/fishtown-analytics/dbt/issues/1975), [#1978](https://github.com/fishtown-analytics/dbt/pull/1978))
- Make the `DBT_TEST_ALT` integration test warehouse configurable on Snowflake ([#1939](https://github.com/fishtown-analytics/dbt/issues/1939), [#1979](https://github.com/fishtown-analytics/dbt/pull/1979))
- Pin upper bound on `google-cloud-bigquery` dependency to `1.24.0`. ([#2007](https://github.com/fishtown-analytics/dbt/pull/2007))
- Remove duplicate `get_context_modules` method ([#1996](https://github.com/fishtown-analytics/dbt/pull/1996))
- Add type annotations to base adapter code ([#1982](https://github.com/fishtown-analytics/dbt/pull/1982))

Contributors:
 - [@Fokko](https://github.com/Fokko) ([#1996](https://github.com/fishtown-analytics/dbt/pull/1996), [#1988](https://github.com/fishtown-analytics/dbt/pull/1988), [#1982](https://github.com/fishtown-analytics/dbt/pull/1982))
 - [@kconvey](https://github.com/kconvey) ([#1967](https://github.com/fishtown-analytics/dbt/pull/1967))


## dbt 0.15.0 (November 25, 2019)

### Breaking changes
- Support for Python 2.x has been dropped [as it will no longer be supported on January 1, 2020](https://www.python.org/dev/peps/pep-0373/)
- Compilation errors in .yml files are now treated as errors instead of warnings ([#1493](https://github.com/fishtown-analytics/dbt/issues/1493), [#1751](https://github.com/fishtown-analytics/dbt/pull/1751))
- The 'table_name' field field has been removed from Relations
- The existing `compile` and `execute` rpc tasks have been renamed to `compile_sql` and `execute_sql` ([#1779](https://github.com/fishtown-analytics/dbt/issues/1779), [#1798](https://github.com/fishtown-analytics/dbt/pull/1798)) ([docs](https://docs.getdbt.com/v0.15/docs/rpc))
- Custom materializations must now manage dbt's Relation cache ([docs](https://docs.getdbt.com/v0.15/docs/creating-new-materializations#section-6-update-the-relation-cache))

### Installation notes:

dbt v0.15.0 uses the `psycopg2-binary` dependency (instead of `psycopg2`) to simplify installation on platforms that do not have a compiler toolchain installed. If you experience segmentation faults, crashes, or installation errors, you can set the  `DBT_PSYCOPG2_NAME` environment variable to `psycopg2` to change the dependency that dbt installs. This may require a compiler toolchain and development libraries.

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
- Add a JSON logger ([#1237](https://github.com/fishtown-analytics/dbt/issues/1237), [#1791](https://github.com/fishtown-analytics/dbt/pull/1791)) ([docs](https://docs.getdbt.com/v0.15/docs/global-cli-flags#section-log-formatting))
- Add structured logging to dbt ([#1704](https://github.com/fishtown-analytics/dbt/issues/1704), [#1799](https://github.com/fishtown-analytics/dbt/issues/1799), [#1715](https://github.com/fishtown-analytics/dbt/pull/1715), [#1806](https://github.com/fishtown-analytics/dbt/pull/1806))
- Add partial parsing option to the profiles.yml file ([#1835](https://github.com/fishtown-analytics/dbt/issues/1835), [#1836](https://github.com/fishtown-analytics/dbt/pull/1836), [#1487](https://github.com/fishtown-analytics/dbt/issues/1487)) ([docs](https://docs.getdbt.com/v0.15/docs/configure-your-profile#section-partial-parsing))
- Support configurable query comments in SQL queries ([#1643](https://github.com/fishtown-analytics/dbt/issues/1643), [#1864](https://github.com/fishtown-analytics/dbt/pull/1864)) ([docs](https://docs.getdbt.com/v0.15/docs/configuring-query-comments))
- Support atomic full-refreshes for incremental models ([#525](https://github.com/fishtown-analytics/dbt/issues/525), [#1682](https://github.com/fishtown-analytics/dbt/pull/1682))
- Support snapshot configs in dbt_project.yml ([#1613](https://github.com/fishtown-analytics/dbt/issues/1613), [#1759](https://github.com/fishtown-analytics/dbt/pull/1759)) ([docs](https://docs.getdbt.com/v0.15/docs/snapshots#section-configuring-snapshots-in-dbt_project-yml))
- Support cache modifications in materializations ([#1683](https://github.com/fishtown-analytics/dbt/issues/1683), [#1770](https://github.com/fishtown-analytics/dbt/pull/1770)) ([docs](https://docs.getdbt.com/v0.15/docs/creating-new-materializations#section-6-update-the-relation-cache))
- Support `quote` parameter to Accepted Values schema tests ([#1873](https://github.com/fishtown-analytics/dbt/issues/1873), [#1876](https://github.com/fishtown-analytics/dbt/pull/1876)) ([docs](https://docs.getdbt.com/v0.15/docs/testing#section-accepted-values))
- Support Python 3.8 ([#1886](https://github.com/fishtown-analytics/dbt/pull/1886))
- Support filters in sources for `dbt source snapshot-freshness` invocation ([#1495](https://github.com/fishtown-analytics/dbt/issues/1495), [#1776](https://github.com/fishtown-analytics/dbt/pull/1776)) ([docs](https://docs.getdbt.com/v0.15/docs/using-sources#section-filtering-sources))
- Support external table configuration in yml source specifications ([#1784](https://github.com/fishtown-analytics/dbt/pull/1784))
- Improve CLI output when running snapshots ([#1768](https://github.com/fishtown-analytics/dbt/issues/1768), [#1769](https://github.com/fishtown-analytics/dbt/pull/1769))

#### Fixes
- Fix for unhelpful error message for malformed source/ref inputs ([#1660](https://github.com/fishtown-analytics/dbt/issues/1660), [#1809](https://github.com/fishtown-analytics/dbt/pull/1809))
- Fix for lingering backup tables when incremental models are full-refreshed ([#1933](https://github.com/fishtown-analytics/dbt/issues/1933), [#1931](https://github.com/fishtown-analytics/dbt/pull/1931))
- Fix for confusing error message when errors are encountered during compilation ([#1807](https://github.com/fishtown-analytics/dbt/issues/1807), [#1839](https://github.com/fishtown-analytics/dbt/pull/1839))
- Fix for logic error affecting the two-argument flavor of the `ref` function ([#1504](https://github.com/fishtown-analytics/dbt/issues/1504), [#1515](https://github.com/fishtown-analytics/dbt/pull/1515))
- Fix for invalid reference to dbt.exceptions ([#1569](https://github.com/fishtown-analytics/dbt/issues/1569), [#1609](https://github.com/fishtown-analytics/dbt/pull/1609))
- Fix for "cannot run empty query" error when pre/post-hooks are empty ([#1108](https://github.com/fishtown-analytics/dbt/issues/1108), [#1719](https://github.com/fishtown-analytics/dbt/pull/1719))
- Fix for confusing error when project names shadow context attributes ([#1696](https://github.com/fishtown-analytics/dbt/issues/1696), [#1748](https://github.com/fishtown-analytics/dbt/pull/1748))
- Fix for incorrect database logic in docs generation which resulted in columns being "merged" together across tables ([#1708](https://github.com/fishtown-analytics/dbt/issues/1708), [#1774](https://github.com/fishtown-analytics/dbt/pull/1774))
- Fix for seed errors located in dependency packages ([#1723](https://github.com/fishtown-analytics/dbt/issues/1723), [#1723](https://github.com/fishtown-analytics/dbt/issues/1723))
- Fix for confusing error when schema tests return unexpected results ([#1808](https://github.com/fishtown-analytics/dbt/issues/1808), [#1903](https://github.com/fishtown-analytics/dbt/pull/1903))
- Fix for twice-compiled `statement` block contents ([#1717](https://github.com/fishtown-analytics/dbt/issues/1717), [#1719](https://github.com/fishtown-analytics/dbt/pull/1719))
- Fix for inaccurate output in `dbt run-operation --help` ([#1767](https://github.com/fishtown-analytics/dbt/issues/1767), [#1777](https://github.com/fishtown-analytics/dbt/pull/1777))
- Fix for file rotation issues concerning the `logs/dbt.log` file ([#1863](https://github.com/fishtown-analytics/dbt/issues/1863), [#1865](https://github.com/fishtown-analytics/dbt/issues/1865), [#1871](https://github.com/fishtown-analytics/dbt/pull/1871))
- Fix for missing quotes in incremental model build queries ([#1847](https://github.com/fishtown-analytics/dbt/issues/1847), [#1888](https://github.com/fishtown-analytics/dbt/pull/1888))
- Fix for incorrect log level in `printer.print_run_result_error` ([#1818](https://github.com/fishtown-analytics/dbt/issues/1818), [#1823](https://github.com/fishtown-analytics/dbt/pull/1823))

### Docs
- Show seeds and snapshots in the Project and Database views ([docs#37](https://github.com/fishtown-analytics/dbt-docs/issues/37), [docs#25](https://github.com/fishtown-analytics/dbt-docs/issues/25), [docs#52](https://github.com/fishtown-analytics/dbt-docs/pull/52))
- Show sources in the Database tree view ([docs#20](https://github.com/fishtown-analytics/dbt-docs/issues/20), [docs#52](https://github.com/fishtown-analytics/dbt-docs/pull/52))
- Show edges in the DAG between models and seeds ([docs#15](https://github.com/fishtown-analytics/dbt-docs/issues/15), [docs#52](https://github.com/fishtown-analytics/dbt-docs/pull/52))
- Show Accepted Values tests and custom schema tests in the column list for models ([docs#52](https://github.com/fishtown-analytics/dbt-docs/pull/52))
- Fix links for "Refocus on node" and "View documentation" in DAG context menu for seeds ([docs#52](https://github.com/fishtown-analytics/dbt-docs/pull/52))

### Server
- Support docs generation ([#1781](https://github.com/fishtown-analytics/dbt/issues/1781), [#1801](https://github.com/fishtown-analytics/dbt/pull/1801))
- Support custom tags ([#1822](https://github.com/fishtown-analytics/dbt/issues/1822), [#1828](https://github.com/fishtown-analytics/dbt/pull/1828))
- Support invoking `deps` on the rpc server ([#1834](https://github.com/fishtown-analytics/dbt/issues/1834), [#1837](https://github.com/fishtown-analytics/dbt/pull/1837))
- Support invoking `run-operation` and `snapshot` on the rpc server ([#1875](https://github.com/fishtown-analytics/dbt/issues/1875), [#1878](https://github.com/fishtown-analytics/dbt/pull/1878))
- Suppport `--threads` argument to `cli_args` method ([#1897](https://github.com/fishtown-analytics/dbt/issues/1897), [#1909](https://github.com/fishtown-analytics/dbt/pull/1909))
- Support reloading the manifest when a SIGHUP signal is received ([#1684](https://github.com/fishtown-analytics/dbt/issues/1684), [#1699](https://github.com/fishtown-analytics/dbt/pull/1699))
- Support invoking `compile`, `run`, `test`, and `seed` on the rpc server ([#1488](https://github.com/fishtown-analytics/dbt/issues/1488), [#1652](https://github.com/fishtown-analytics/dbt/pull/1652))
- Support returning compilation logs from the last compile in the `status` method ([#1703](https://github.com/fishtown-analytics/dbt/issues/1703), [#1775](https://github.com/fishtown-analytics/dbt/pull/1715))
- Support asyncronous `compile_sql` and `run_sql` methods ([#1706](https://github.com/fishtown-analytics/dbt/issues/1706), [#1735](https://github.com/fishtown-analytics/dbt/pull/1735))
- Improve re-compilation performance ([#1824](https://github.com/fishtown-analytics/dbt/issues/1824), [#1830](https://github.com/fishtown-analytics/dbt/pull/1830))

### Postgres / Redshift
- Support running dbt against schemas which contain materialized views on Postgres ([#1698](https://github.com/fishtown-analytics/dbt/issues/1698), [#1833](https://github.com/fishtown-analytics/dbt/pull/1833))
- Support distyle AUTO in Redshift model configs ([#1882](https://github.com/fishtown-analytics/dbt/issues/1882), [#1885](https://github.com/fishtown-analytics/dbt/pull/1885)) ([docs](https://docs.getdbt.com/v0.15/docs/redshift-configs#section-using-sortkey-and-distkey))
- Fix for internal errors when run against mixed-case logical databases ([#1800](https://github.com/fishtown-analytics/dbt/issues/1800), [#1936](https://github.com/fishtown-analytics/dbt/pull/1936))

### Snowflake
- Support `copy grants` option in Snowflake model configs ([#1744](https://github.com/fishtown-analytics/dbt/issues/1744), [#1747](https://github.com/fishtown-analytics/dbt/pull/1747)) ([docs](https://docs.getdbt.com/v0.15/docs/snowflake-configs#section-copying-grants))
- Support warehouse configuration in Snowflake model configs ([#1358](https://github.com/fishtown-analytics/dbt/issues/1358), [#1899](https://github.com/fishtown-analytics/dbt/issues/1899), [#1788](https://github.com/fishtown-analytics/dbt/pull/1788), [#1901](https://github.com/fishtown-analytics/dbt/pull/1901)) ([docs](https://docs.getdbt.com/v0.15/docs/snowflake-configs#section-configuring-virtual-warehouses))
- Support secure views in Snowflake model configs ([#1730](https://github.com/fishtown-analytics/dbt/issues/1730), [#1743](https://github.com/fishtown-analytics/dbt/pull/1743)) ([docs](https://docs.getdbt.com/v0.15/docs/snowflake-configs#section-secure-views))
- Fix for unclosed connections preventing dbt from exiting when Snowflake is used with client_session_keep_alive ([#1271](https://github.com/fishtown-analytics/dbt/issues/1271), [#1749](https://github.com/fishtown-analytics/dbt/pull/1749))
- Fix for errors on Snowflake when dbt schemas contain `LOCAL TEMPORARY` tables ([#1869](https://github.com/fishtown-analytics/dbt/issues/1869), [#1872](https://github.com/fishtown-analytics/dbt/pull/1872))

### BigQuery
- Support KMS Encryption in BigQuery model configs ([#1829](https://github.com/fishtown-analytics/dbt/issues/1829), [#1851](https://github.com/fishtown-analytics/dbt/issues/1829)) ([docs](https://docs.getdbt.com/v0.15/docs/bigquery-configs#section-managing-kms-encryption))
- Improve docs generation speed by leveraging the information schema ([#1576](https://github.com/fishtown-analytics/dbt/issues/1576), [#1795](https://github.com/fishtown-analytics/dbt/pull/1795))
- Fix for cache errors on BigQuery when dataset names are capitalized ([#1810](https://github.com/fishtown-analytics/dbt/issues/1810), [#1881](https://github.com/fishtown-analytics/dbt/pull/1881))
- Fix for invalid query generation when multiple `options` are provided to a `create table|view` query ([#1786](https://github.com/fishtown-analytics/dbt/issues/1786), [#1787](https://github.com/fishtown-analytics/dbt/pull/1787))
- Use `client.delete_dataset` to drop BigQuery datasets atomically ([#1887](https://github.com/fishtown-analytics/dbt/issues/1887), [#1881](https://github.com/fishtown-analytics/dbt/pull/1881))

### Under the Hood
#### Dependencies
- Drop support for `networkx 1.x` ([#1577](https://github.com/fishtown-analytics/dbt/issues/1577), [#1814](https://github.com/fishtown-analytics/dbt/pull/1814))
- Upgrade `werkzeug` to 0.15.6 ([#1697](https://github.com/fishtown-analytics/dbt/issues/1697), [#1814](https://github.com/fishtown-analytics/dbt/pull/1814))
- Pin `psycopg2` dependency to 2.8.x to prevent segfaults ([#1221](https://github.com/fishtown-analytics/dbt/issues/1221), [#1898](https://github.com/fishtown-analytics/dbt/pull/1898))
- Set a strict upper bound for `jsonschema` dependency ([#1817](https://github.com/fishtown-analytics/dbt/issues/1817), [#1821](https://github.com/fishtown-analytics/dbt/pull/1821), [#1932](https://github.com/fishtown-analytics/dbt/pull/1932))
#### Everything else
- Provide test names and kwargs in the manifest ([#1154](https://github.com/fishtown-analytics/dbt/issues/1154), [#1816](https://github.com/fishtown-analytics/dbt/pull/1816))
- Replace JSON Schemas with data classes ([#1447](https://github.com/fishtown-analytics/dbt/issues/1447), [#1589](https://github.com/fishtown-analytics/dbt/pull/1589))
- Include test name and kwargs in test nodes in the manifest ([#1154](https://github.com/fishtown-analytics/dbt/issues/1154), [#1816](https://github.com/fishtown-analytics/dbt/pull/1816))
- Remove logic around handling `archive` blocks in the `dbt_project.yml` file ([#1580](https://github.com/fishtown-analytics/dbt/issues/1580), [#1581](https://github.com/fishtown-analytics/dbt/pull/1581))
- Remove the APIObject class ([#1762](https://github.com/fishtown-analytics/dbt/issues/1762), [#1780](https://github.com/fishtown-analytics/dbt/pull/1780))

## Contributors
Thanks all for your contributions to dbt! :tada:

- [@captainEli](https://github.com/captainEli) ([#1809](https://github.com/fishtown-analytics/dbt/pull/1809))
- [@clausherther](https://github.com/clausherther) ([#1876](https://github.com/fishtown-analytics/dbt/pull/1876))
- [@jtcohen6](https://github.com/jtcohen6) ([#1784](https://github.com/fishtown-analytics/dbt/pull/1784))
- [@tbescherer](https://github.com/tbescherer) ([#1515](https://github.com/fishtown-analytics/dbt/pull/1515))
- [@aminamos](https://github.com/aminamos) ([#1609](https://github.com/fishtown-analytics/dbt/pull/1609))
- [@JusLarsen](https://github.com/JusLarsen) ([#1903](https://github.com/fishtown-analytics/dbt/pull/1903))
- [@heisencoder](https://github.com/heisencoder) ([#1823](https://github.com/fishtown-analytics/dbt/pull/1823))
- [@tjengel](https://github.com/tjengel) ([#1885](https://github.com/fishtown-analytics/dbt/pull/1885))
- [@Carolus-Holman](https://github.com/tjengel) ([#1747](https://github.com/fishtown-analytics/dbt/pull/1747), [#1743](https://github.com/fishtown-analytics/dbt/pull/1743))
- [@kconvey](https://github.com/tjengel) ([#1851](https://github.com/fishtown-analytics/dbt/pull/1851))
- [@darrenhaken](https://github.com/darrenhaken) ([#1787](https://github.com/fishtown-analytics/dbt/pull/1787))


## dbt 0.14.4 (November 8, 2019)

This release changes the version ranges of some of dbt's dependencies. These changes address installation issues in 0.14.3 when dbt is installed from pip. You can view the full list of dependency version changes [in this commit](https://github.com/fishtown-analytics/dbt/commit/b4dd265cb433480a59bbd15d140d46ebf03644eb).

Note: If you are installing dbt into an environment alongside other Python libraries, you can install individual dbt plugins with:
```
pip install dbt-postgres
pip install dbt-redshift
pip install dbt-snowflake
pip install dbt-bigquery
```

Installing specific plugins may help mitigate issues regarding incompatible versions of dependencies between dbt and other libraries.

### Fixes:
 - Fix dependency issues caused by a bad release of `snowflake-connector-python` ([#1892](https://github.com/fishtown-analytics/dbt/issues/1892), [#1895](https://github.com/fishtown-analytics/dbt/pull/1895/files))


## dbt 0.14.3 (October 10, 2019)

This is a bugfix release.

### Fixes:
- Fix for `dictionary changed size during iteration` race condition ([#1740](https://github.com/fishtown-analytics/dbt/issues/1740), [#1750](https://github.com/fishtown-analytics/dbt/pull/1750))
- Fix upper bound on jsonschema dependency to 3.1.1 ([#1817](https://github.com/fishtown-analytics/dbt/issues/1817), [#1819](https://github.com/fishtown-analytics/dbt/pull/1819))

### Under the hood:
- Provide a programmatic method for validating profile targets ([#1754](https://github.com/fishtown-analytics/dbt/issues/1754), [#1775](https://github.com/fishtown-analytics/dbt/pull/1775))

## dbt 0.14.2 (September 13, 2019)

### Overview

This is a bugfix release.

### Fixes:
- Fix for dbt hanging at the end of execution in `dbt source snapshot-freshness` tasks ([#1728](https://github.com/fishtown-analytics/dbt/issues/1728), [#1729](https://github.com/fishtown-analytics/dbt/pull/1729))
- Fix for broken "packages" and "tags" selector dropdowns in the dbt Documentation website ([docs#47](https://github.com/fishtown-analytics/dbt-docs/issues/47), [#1726](https://github.com/fishtown-analytics/dbt/pull/1726))


## dbt 0.14.1 (September 3, 2019)

### Overview

This is primarily a bugfix release which contains a few minor improvements too. Note: this release includes an important change in how the `check` snapshot strategy works. See [#1614](https://github.com/fishtown-analytics/dbt/pull/1614) for more information. If you are using snapshots with the `check` strategy on dbt v0.14.0, it is strongly recommended that you upgrade to 0.14.1 at your soonest convenience.

### Breaking changes
 - The undocumented `macros` attribute was removed from the `graph` context variable ([#1615](https://github.com/fishtown-analytics/dbt/pull/1615))

### Features:
 - Summarize warnings at the end of dbt runs ([#1597](https://github.com/fishtown-analytics/dbt/issues/1597), [#1654](https://github.com/fishtown-analytics/dbt/pull/1654))
 - Speed up catalog generation on postgres by using avoiding use of the `information_schema` ([#1540](https://github.com/fishtown-analytics/dbt/pull/1540))
 - Docs site updates ([#1621](https://github.com/fishtown-analytics/dbt/issues/1621))
   - Fix for incorrect node selection logic in DAG view ([docs#38](https://github.com/fishtown-analytics/dbt-docs/pull/38))
   - Update page title, meta tags, and favicon ([docs#39](https://github.com/fishtown-analytics/dbt-docs/pull/39))
   - Bump the version of `dbt-styleguide`, changing file tree colors from orange to black :)
 - Add environment variables for macro debugging flags ([#1628](https://github.com/fishtown-analytics/dbt/issues/1628), [#1629](https://github.com/fishtown-analytics/dbt/pull/1629))
 - Speed up node selection by making it linear, rather than quadratic, in complexity ([#1611](https://github.com/fishtown-analytics/dbt/issues/1611), [#1615](https://github.com/fishtown-analytics/dbt/pull/1615))
 - Specify the `application` field in Snowflake connections ([#1622](https://github.com/fishtown-analytics/dbt/issues/1622), [#1623](https://github.com/fishtown-analytics/dbt/pull/1623))
 - Add support for clustering on Snowflake ([#634](https://github.com/fishtown-analytics/dbt/issues/634), [#1591](https://github.com/fishtown-analytics/dbt/pull/1591), [#1689](https://github.com/fishtown-analytics/dbt/pull/1689)) ([docs](https://docs.getdbt.com/docs/snowflake-configs#section-configuring-table-clustering))
 - Add support for job priority on BigQuery ([#1456](https://github.com/fishtown-analytics/dbt/issues/1456), [#1673](https://github.com/fishtown-analytics/dbt/pull/1673)) ([docs](https://docs.getdbt.com/docs/profile-bigquery#section-priority))
 - Add `node.config` and `node.tags` to the `generate_schema_name` and `generate_alias_name` macro context ([#1700](https://github.com/fishtown-analytics/dbt/issues/1700), [#1701](https://github.com/fishtown-analytics/dbt/pull/1701))

### Fixes:
 - Fix for reused `check_cols` values in snapshots ([#1614](https://github.com/fishtown-analytics/dbt/pull/1614), [#1709](https://github.com/fishtown-analytics/dbt/pull/1709))
 - Fix for rendering column descriptions in sources ([#1619](https://github.com/fishtown-analytics/dbt/issues/1619), [#1633](https://github.com/fishtown-analytics/dbt/pull/1633))
 - Fix for `is_incremental()` returning True for models that are not materialized as incremental models ([#1249](https://github.com/fishtown-analytics/dbt/issues/1249), [#1608](https://github.com/fishtown-analytics/dbt/pull/1608))
 - Fix for serialization of BigQuery results which contain nested or repeated records ([#1626](https://github.com/fishtown-analytics/dbt/issues/1626), [#1638](https://github.com/fishtown-analytics/dbt/pull/1638))
 - Fix for loading seed files which contain non-ascii characters ([#1632](https://github.com/fishtown-analytics/dbt/issues/1632), [#1644](https://github.com/fishtown-analytics/dbt/pull/1644))
 - Fix for creation of user cookies in incorrect directories when `--profile-dir` or `$DBT_PROFILES_DIR` is provided ([#1645](https://github.com/fishtown-analytics/dbt/issues/1645), [#1656](https://github.com/fishtown-analytics/dbt/pull/1656))
 - Fix for error handling when transactions are being rolled back ([#1647](https://github.com/fishtown-analytics/dbt/pull/1647))
 - Fix for incorrect references to `dbt.exceptions` in jinja code ([#1569](https://github.com/fishtown-analytics/dbt/issues/1569), [#1609](https://github.com/fishtown-analytics/dbt/pull/1609))
 - Fix for duplicated schema creation due to case-sensitive comparison ([#1651](https://github.com/fishtown-analytics/dbt/issues/1651), [#1663](https://github.com/fishtown-analytics/dbt/pull/1663))
 - Fix for "schema stub" created automatically by dbt ([#913](https://github.com/fishtown-analytics/dbt/issues/913), [#1663](https://github.com/fishtown-analytics/dbt/pull/1663))
 - Fix for incremental merge query on old versions of postgres (<=9.6) ([#1665](https://github.com/fishtown-analytics/dbt/issues/1665), [#1666](https://github.com/fishtown-analytics/dbt/pull/1666))
 - Fix for serializing results of queries which return `TIMESTAMP_TZ` columns on Snowflake in the RPC server ([#1670](https://github.com/fishtown-analytics/dbt/pull/1670))
 - Fix typo in InternalException ([#1640](https://github.com/fishtown-analytics/dbt/issues/1640), [#1672](https://github.com/fishtown-analytics/dbt/pull/1672))
 - Fix typo in CLI help for snapshot migration subcommand ([#1664](https://github.com/fishtown-analytics/dbt/pull/1664))
 - Fix for error handling logic when empty queries are submitted on Snowflake ([#1693](https://github.com/fishtown-analytics/dbt/issues/1693), [#1694](https://github.com/fishtown-analytics/dbt/pull/1694))
 - Fix for non-atomic column expansion logic in Snowflake incremental models and snapshots ([#1687](https://github.com/fishtown-analytics/dbt/issues/1687), [#1690](https://github.com/fishtown-analytics/dbt/pull/1690))
 - Fix for unprojected `count(*)` expression injected by custom data tests ([#1688](https://github.com/fishtown-analytics/dbt/pull/1688))
 - Fix for `dbt run` and `dbt docs generate` commands when running against Panoply Redshift ([#1479](https://github.com/fishtown-analytics/dbt/issues/1479), [#1686](https://github.com/fishtown-analytics/dbt/pull/1686))


 ### Contributors:
Thanks for your contributions to dbt!

- [@levimalott](https://github.com/levimalott) ([#1647](https://github.com/fishtown-analytics/dbt/pull/1647))
- [@aminamos](https://github.com/aminamos) ([#1609](https://github.com/fishtown-analytics/dbt/pull/1609))
- [@elexisvenator](https://github.com/elexisvenator) ([#1540](https://github.com/fishtown-analytics/dbt/pull/1540))
- [@edmundyan](https://github.com/edmundyan) ([#1663](https://github.com/fishtown-analytics/dbt/pull/1663))
- [@vitorbaptista](https://github.com/vitorbaptista) ([#1664](https://github.com/fishtown-analytics/dbt/pull/1664))
- [@sjwhitworth](https://github.com/sjwhitworth) ([#1672](https://github.com/fishtown-analytics/dbt/pull/1672), [#1673](https://github.com/fishtown-analytics/dbt/pull/1673))
- [@mikaelene](https://github.com/mikaelene) ([#1688](https://github.com/fishtown-analytics/dbt/pull/1688), [#1709](https://github.com/fishtown-analytics/dbt/pull/1709))
- [@bastienboutonnet](https://github.com/bastienboutonnet) ([#1591](https://github.com/fishtown-analytics/dbt/pull/1591), [#1689](https://github.com/fishtown-analytics/dbt/pull/1689))



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
- Stub out adapter methods at parse-time to speed up parsing ([#1413](https://github.com/fishtown-analytics/dbt/pull/1413))
- Removed support for the `--non-destructive` flag ([#1419](https://github.com/fishtown-analytics/dbt/pull/1419), [#1415](https://github.com/fishtown-analytics/dbt/issues/1415))
- Removed support for the `sql_where` config to incremental models ([#1408](https://github.com/fishtown-analytics/dbt/pull/1408), [#1351](https://github.com/fishtown-analytics/dbt/issues/1351))
- Changed `expand_target_column_types` to take a Relation instead of a string ([#1478](https://github.com/fishtown-analytics/dbt/pull/1478))
- Replaced Archives with Snapshots
  - Normalized meta-column names in Snapshot tables ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#251](https://github.com/fishtown-analytics/dbt/issues/251))

### Features
- Add `run-operation` command which invokes macros directly from the CLI ([#1328](https://github.com/fishtown-analytics/dbt/pull/1328)) ([docs](https://docs.getdbt.com/v0.14/docs/run-operation))
- Add a `dbt ls` command which lists resources in your project ([#1436](https://github.com/fishtown-analytics/dbt/pull/1436), [#467](https://github.com/fishtown-analytics/dbt/issues/467)) ([docs](https://docs.getdbt.com/v0.14/docs/list))
- Add Snapshots, an improvement over Archives ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#1175](https://github.com/fishtown-analytics/dbt/issues/1175)) ([docs](https://docs.getdbt.com/v0.14/docs/snapshots))
  - Add the 'check' snapshot strategy ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#706](https://github.com/fishtown-analytics/dbt/issues/706))
  - Support Snapshots across logical databases ([#1455](https://github.com/fishtown-analytics/dbt/issues/1455))
  - Implement Snapshots using a merge statement where supported ([#1478](https://github.com/fishtown-analytics/dbt/pull/1478))
  - Support Snapshot selection using `--select` ([#1520](https://github.com/fishtown-analytics/dbt/pull/1520), [#1512](https://github.com/fishtown-analytics/dbt/issues/1512))
- Add an RPC server via `dbt rpc` ([#1301](https://github.com/fishtown-analytics/dbt/pull/1301), [#1274](https://github.com/fishtown-analytics/dbt/issues/1274)) ([docs](https://docs.getdbt.com/v0.14/docs/rpc))
  - Add `ps` and `kill` commands to the rpc server ([#1380](https://github.com/fishtown-analytics/dbt/pull/1380/), [#1369](https://github.com/fishtown-analytics/dbt/issues/1369), [#1370](https://github.com/fishtown-analytics/dbt/issues/1370))
  - Add support for ephemeral nodes to the rpc server ([#1373](https://github.com/fishtown-analytics/dbt/pull/1373), [#1368](https://github.com/fishtown-analytics/dbt/issues/1368))
  - Add support for inline macros to the rpc server ([#1375](https://github.com/fishtown-analytics/dbt/pull/1375), [#1372](https://github.com/fishtown-analytics/dbt/issues/1372), [#1348](https://github.com/fishtown-analytics/dbt/pull/1348))
  - Improve error handling in the rpc server ([#1341](https://github.com/fishtown-analytics/dbt/pull/1341), [#1309](https://github.com/fishtown-analytics/dbt/issues/1309), [#1310](https://github.com/fishtown-analytics/dbt/issues/1310))
- Made printer width configurable ([#1026](https://github.com/fishtown-analytics/dbt/issues/1026), [#1247](https://github.com/fishtown-analytics/dbt/pull/1247)) ([docs](https://docs.getdbt.com/v0.14/docs/configure-your-profile#section-additional-profile-configurations))
- Retry package downloads from the hub.getdbt.com ([#1451](https://github.com/fishtown-analytics/dbt/issues/1451), [#1491](https://github.com/fishtown-analytics/dbt/pull/1491))
- Add a test "severity" level, presented as a keyword argument to schema tests ([#1410](https://github.com/fishtown-analytics/dbt/pull/1410), [#1005](https://github.com/fishtown-analytics/dbt/issues/1005)) ([docs](https://docs.getdbt.com/v0.14/docs/testing#section-test-severity))
- Add a `generate_alias_name` macro to configure alias names dynamically ([#1363](https://github.com/fishtown-analytics/dbt/pull/1363)) ([docs](https://docs.getdbt.com/v0.14/docs/using-custom-aliases#section-generate_alias_name))
- Add a `node` argument to `generate_schema_name` to configure schema names dynamically ([#1483](https://github.com/fishtown-analytics/dbt/pull/1483), [#1463](https://github.com/fishtown-analytics/dbt/issues/1463)) ([docs](https://docs.getdbt.com/v0.14/docs/using-custom-schemas#section-generate_schema_name-arguments))
- Use `create or replace` on Snowflake to rebuild tables and views atomically ([#1101](https://github.com/fishtown-analytics/dbt/issues/1101), [#1409](https://github.com/fishtown-analytics/dbt/pull/1409))
- Use `merge` statement for incremental models on Snowflake ([#1414](https://github.com/fishtown-analytics/dbt/issues/1414), [#1307](https://github.com/fishtown-analytics/dbt/pull/1307), [#1409](https://github.com/fishtown-analytics/dbt/pull/1409)) ([docs](https://docs.getdbt.com/v0.14/docs/snowflake-configs#section-merge-behavior-incremental-models-))
- Add support seed CSV files that start with a UTF-8 Byte Order Mark (BOM) ([#1452](https://github.com/fishtown-analytics/dbt/pull/1452), [#1177](https://github.com/fishtown-analytics/dbt/issues/1177))
- Add a warning when git packages are not pinned to a version ([#1453](https://github.com/fishtown-analytics/dbt/pull/1453), [#1446](https://github.com/fishtown-analytics/dbt/issues/1446))
- Add logging for `on-run-start` and `on-run-end hooks` to console output ([#1440](https://github.com/fishtown-analytics/dbt/pull/1440), [#696](https://github.com/fishtown-analytics/dbt/issues/696))
- Add modules and tracking information to the rendering context for configuration files ([#1441](https://github.com/fishtown-analytics/dbt/pull/1441), [#1320](https://github.com/fishtown-analytics/dbt/issues/1320))
- Add support for `null` vars, and distinguish `null` vars from unset vars ([#1426](https://github.com/fishtown-analytics/dbt/pull/1426), [#608](https://github.com/fishtown-analytics/dbt/issues/608))
- Add support for the `search_path` configuration in Postgres/Redshift profiles ([#1477](https://github.com/fishtown-analytics/dbt/issues/1477), [#1476](https://github.com/fishtown-analytics/dbt/pull/1476)) ([docs (postgres)](https://docs.getdbt.com/v0.14/docs/profile-postgres), [docs (redshift)](https://docs.getdbt.com/v0.14/docs/profile-redshift))
- Add support for persisting documentation as `descriptions` for tables and views on BigQuery ([#1031](https://github.com/fishtown-analytics/dbt/issues/1031), [#1285](https://github.com/fishtown-analytics/dbt/pull/1285)) ([docs](https://docs.getdbt.com/v0.14/docs/bigquery-configs#section-persisting-model-descriptions))
- Add a `--project-dir` path which will invoke dbt in the specified directory ([#1549](https://github.com/fishtown-analytics/dbt/pull/1549), [#1544](https://github.com/fishtown-analytics/dbt/issues/1544))

### dbt docs Changes
- Add searching by tag name ([#32](https://github.com/fishtown-analytics/dbt-docs/pull/32))
- Add context menu link to export graph viz as a PNG ([#34](https://github.com/fishtown-analytics/dbt-docs/pull/34))
- Fix for clicking models in left-nav while search results are open ([#31](https://github.com/fishtown-analytics/dbt-docs/pull/31))

### Fixes
- Fix for unduly long timeouts when anonymous event tracking is blocked ([#1445](https://github.com/fishtown-analytics/dbt/pull/1445), [#1063](https://github.com/fishtown-analytics/dbt/issues/1063))
- Fix for error with mostly-duplicate git urls in packages, picking the one that came first. ([#1428](https://github.com/fishtown-analytics/dbt/pull/1428), [#1084](https://github.com/fishtown-analytics/dbt/issues/1084))
- Fix for unrendered `description` field as jinja in top-level Source specification ([#1484](https://github.com/fishtown-analytics/dbt/issues/1484), [#1494](https://github.com/fishtown-analytics/dbt/issues/1494))
- Fix for API error when very large temp tables are created in BigQuery ([#1423](https://github.com/fishtown-analytics/dbt/issues/1423), [#1478](https://github.com/fishtown-analytics/dbt/pull/1478))
- Fix for compiler errors that occurred if jinja code was present outside of a docs blocks in .md files ([#1513](https://github.com/fishtown-analytics/dbt/pull/1513), [#988](https://github.com/fishtown-analytics/dbt/issues/988))
- Fix `TEXT` handling on postgres and redshift ([#1420](https://github.com/fishtown-analytics/dbt/pull/1420), [#781](https://github.com/fishtown-analytics/dbt/issues/781))
- Fix for compiler error when vars are undefined but only used in disabled models ([#1429](https://github.com/fishtown-analytics/dbt/pull/1429), [#434](https://github.com/fishtown-analytics/dbt/issues/434))
- Improved the error message when iterating over the results of a macro that doesn't exist ([#1425](https://github.com/fishtown-analytics/dbt/pull/1425), [#1424](https://github.com/fishtown-analytics/dbt/issues/1424))
- Improved the error message when tests have invalid parameter definitions ([#1427](https://github.com/fishtown-analytics/dbt/pull/1427), [#1325](https://github.com/fishtown-analytics/dbt/issues/1325))
- Improved the error message when a user tries to archive a non-existent table ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#1066](https://github.com/fishtown-analytics/dbt/issues/1066))
- Fix for archive logic which tried to create already-existing destination schemas ([#1398](https://github.com/fishtown-analytics/dbt/pull/1398), [#758](https://github.com/fishtown-analytics/dbt/issues/758))
- Fix for incorrect error codes when Operations exit with an error ([#1406](https://github.com/fishtown-analytics/dbt/pull/1406), [#1377](https://github.com/fishtown-analytics/dbt/issues/1377))
- Fix for missing compiled SQL when the rpc server encounters a database error ([#1381](https://github.com/fishtown-analytics/dbt/pull/1381), [#1371](https://github.com/fishtown-analytics/dbt/issues/1371))
- Fix for broken link in the profile.yml generated by `dbt init` ([#1366](https://github.com/fishtown-analytics/dbt/pull/1366), [#1344](https://github.com/fishtown-analytics/dbt/issues/1344))
- Fix the sample test.env file's redshift password field ([#1364](https://github.com/fishtown-analytics/dbt/pull/1364))
- Fix collisions on models running concurrently that have duplicate names but have distinguishing aliases ([#1342](https://github.com/fishtown-analytics/dbt/pull/1342), [#1321](https://github.com/fishtown-analytics/dbt/issues/1321))
- Fix for a bad error message when a `version` is missing from a package spec in `packages.yml` ([#1551](https://github.com/fishtown-analytics/dbt/pull/1551), [#1546](https://github.com/fishtown-analytics/dbt/issues/1546))
- Fix for wrong package scope when the two-arg method of `ref` is used ([#1515](https://github.com/fishtown-analytics/dbt/pull/1515), [#1504](https://github.com/fishtown-analytics/dbt/issues/1504))
- Fix missing import in test suite ([#1572](https://github.com/fishtown-analytics/dbt/pull/1572))
- Fix for a Snowflake error when an external table exists in a schema that dbt operates on ([#1571](https://github.com/fishtown-analytics/dbt/pull/1571), [#1505](https://github.com/fishtown-analytics/dbt/issues/1505))


### Under the hood
- Use pytest for tests ([#1417](https://github.com/fishtown-analytics/dbt/pull/1417))
- Use flake8 for linting ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#1333](https://github.com/fishtown-analytics/dbt/issues/1333))
- Added a flag for wrapping models and tests in jinja blocks ([#1407](https://github.com/fishtown-analytics/dbt/pull/1407), [#1400](https://github.com/fishtown-analytics/dbt/issues/1400))
- Connection management: Bind connections threads rather than to names ([#1336](https://github.com/fishtown-analytics/dbt/pull/1336), [#1312](https://github.com/fishtown-analytics/dbt/issues/1312))
- Add deprecation warning for dbt users on Python2 ([#1534](https://github.com/fishtown-analytics/dbt/pull/1534), [#1531](https://github.com/fishtown-analytics/dbt/issues/1531))
- Upgrade networkx to v2.x ([#1509](https://github.com/fishtown-analytics/dbt/pull/1509), [#1496](https://github.com/fishtown-analytics/dbt/issues/1496))
- Anonymously track adapter type and rpc requests when tracking is enabled ([#1575](https://github.com/fishtown-analytics/dbt/pull/1575), [#1574](https://github.com/fishtown-analytics/dbt/issues/1574))
- Fix for test warnings and general test suite cleanup ([#1578](https://github.com/fishtown-analytics/dbt/pull/1578))

### Contributors:
Over a dozen contributors wrote code for this release of dbt! Thanks for taking the time, and nice work y'all! :)

- [@nydnarb](https://github.com/nydnarb) ([#1363](https://github.com/fishtown-analytics/dbt/issues/1363))
- [@emilieschario](https://github.com/emilieschario) ([#1366](https://github.com/fishtown-analytics/dbt/pull/1366))
- [@bastienboutonnet](https://github.com/bastienboutonnet) ([#1409](https://github.com/fishtown-analytics/dbt/pull/1409))
- [@kasanchez](https://github.com/kasanchez) ([#1247](https://github.com/fishtown-analytics/dbt/pull/1247))
- [@Blakewell](https://github.com/Blakewell) ([#1307](https://github.com/fishtown-analytics/dbt/pull/1307))
- [@buremba](https://github.com/buremba) ([#1476](https://github.com/fishtown-analytics/dbt/pull/1476))
- [@darrenhaken](https://github.com/darrenhaken) ([#1285](https://github.com/fishtown-analytics/dbt/pull/1285))
- [@tbescherer](https://github.com/tbescherer) ([#1504](https://github.com/fishtown-analytics/dbt/issues/1504))
- [@heisencoder](https://github.com/heisencoder) ([#1509](https://github.com/fishtown-analytics/dbt/pull/1509), [#1549](https://github.com/fishtown-analytics/dbt/pull/1549). [#1578](https://github.com/fishtown-analytics/dbt/pull/1578))
- [@cclauss](https://github.com/cclauss) ([#1572](https://github.com/fishtown-analytics/dbt/pull/1572))
- [@josegalarza](https://github.com/josegalarza) ([#1571](https://github.com/fishtown-analytics/dbt/pull/1571))
- [@rmgpinto](https://github.com/rmgpinto) ([docs#31](https://github.com/fishtown-analytics/dbt-docs/pull/31), [docs#32](https://github.com/fishtown-analytics/dbt-docs/pull/32))
- [@groodt](https://github.com/groodt) ([docs#34](https://github.com/fishtown-analytics/dbt-docs/pull/34))
- [@dcereijodo](https://github.com/dcereijodo) ([#2341](https://github.com/fishtown-analytics/dbt/pull/2341))


## dbt 0.13.1 (May 13, 2019)

### Overview
This is a bugfix release.

### Bugfixes
- Add "MaterializedView" relation type to the Snowflake adapter ([#1430](https://github.com/fishtown-analytics/dbt/issues/1430), [#1432](https://github.com/fishtown-analytics/dbt/pull/1432)) ([@adriank-convoy](https://github.com/adriank-convoy))
- Quote databases properly ([#1396](https://github.com/fishtown-analytics/dbt/issues/1396), [#1402](https://github.com/fishtown-analytics/dbt/pull/1402))
- Use "ilike" instead of "=" for database equality when listing schemas ([#1411](https://github.com/fishtown-analytics/dbt/issues/1411), [#1412](https://github.com/fishtown-analytics/dbt/pull/1412))
- Pass the model name along in get_relations ([#1384](https://github.com/fishtown-analytics/dbt/issues/1384), [#1388](https://github.com/fishtown-analytics/dbt/pull/1388))
- Add logging to dbt clean ([#1261](https://github.com/fishtown-analytics/dbt/issues/1261), [#1383](https://github.com/fishtown-analytics/dbt/pull/1383), [#1391](https://github.com/fishtown-analytics/dbt/pull/1391)) ([@emilieschario](https://github.com/emilieschario))

### dbt Docs
- Search by columns ([dbt-docs#23](https://github.com/fishtown-analytics/dbt-docs/pull/23)) ([rmgpinto](https://github.com/rmgpinto))
- Support @ selector ([dbt-docs#27](https://github.com/fishtown-analytics/dbt-docs/pull/27))
- Fix number formatting on Snowflake and BQ in table stats ([dbt-docs#28](https://github.com/fishtown-analytics/dbt-docs/pull/28))

### Contributors:
Thanks for your contributions to dbt!

- [@emilieschario](https://github.com/emilieschario)
- [@adriank-convoy](https://github.com/adriank-convoy)
- [@rmgpinto](https://github.com/rmgpinto)


## dbt 0.13.0 - Stephen Girard (March 21, 2019)

### Overview

This release provides [a stable API for building new adapters](https://docs.getdbt.com/v0.13/docs/building-a-new-adapter) and reimplements dbt's adapters as "plugins". Additionally, a new adapter for [Presto](https://github.com/fishtown-analytics/dbt-presto) was added using this architecture. Beyond adapters, this release of dbt also includes [Sources](https://docs.getdbt.com/v0.13/docs/using-sources) which can be used to document and test source data tables. See the full list of features added in 0.13.0 below.

### Breaking Changes
- version 1 schema.yml specs are no longer implemented. Please use the version 2 spec instead ([migration guide](https://docs.getdbt.com/docs/upgrading-from-0-10-to-0-11#section-schema-yml-v2-syntax))
- `{{this}}` is no longer implemented for `on-run-start` and `on-run-end` hooks. Use `{{ target }}` or an [`on-run-end` context variable](https://docs.getdbt.com/docs/on-run-end-context#section-schemas) instead ([#1176](https://github.com/fishtown-analytics/dbt/pull/1176), implementing [#878](https://github.com/fishtown-analytics/dbt/issues/878))
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
- Add `source`s to dbt, use them to calculate source data freshness ([docs](https://docs.getdbt.com/v0.13/docs/using-sources) ) ([#814](https://github.com/fishtown-analytics/dbt/issues/814), [#1240](https://github.com/fishtown-analytics/dbt/issues/1240))
- Add support for Presto ([docs](https://docs.getdbt.com/v0.13/docs/profile-presto), [repo](https://github.com/fishtown-analytics/dbt-presto)) ([#1106](https://github.com/fishtown-analytics/dbt/issues/1106))
- Add `require-dbt-version` option to `dbt_project.yml` to state the supported versions of dbt for packages ([docs](https://docs.getdbt.com/v0.13/docs/requiring-dbt-versions)) ([#581](https://github.com/fishtown-analytics/dbt/issues/581))
- Add an output line indicating the installed version of dbt to every run ([#1134](https://github.com/fishtown-analytics/dbt/issues/1134))
- Add a new model selector (`@`) which build models, their children, and their children's parents ([docs](https://docs.getdbt.com/v0.13/reference#section-the-at-operator)) ([#1156](https://github.com/fishtown-analytics/dbt/issues/1156))
- Add support for Snowflake Key Pair Authentication ([docs](https://docs.getdbt.com/v0.13/docs/profile-snowflake#section-key-pair-authentication)) ([#1232](https://github.com/fishtown-analytics/dbt/pull/1232))
- Support SSO Authentication for Snowflake ([docs](https://docs.getdbt.com/v0.13/docs/profile-snowflake#section-sso-authentication)) ([#1172](https://github.com/fishtown-analytics/dbt/issues/1172))
- Add support for Snowflake's transient tables ([docs](https://docs.getdbt.com/v0.13/docs/snowflake-configs#section-transient-tables)) ([#946](https://github.com/fishtown-analytics/dbt/issues/946))
- Capture build timing data in `run_results.json` to visualize project performance ([#1179](https://github.com/fishtown-analytics/dbt/issues/1179))
- Add CLI flag to toggle warnings as errors ([docs](https://docs.getdbt.com/v0.13/reference#section-treat-warnings-as-errors)) ([#1243](https://github.com/fishtown-analytics/dbt/issues/1243))
- Add tab completion script for Bash ([docs](https://github.com/fishtown-analytics/dbt-completion.bash)) ([#1197](https://github.com/fishtown-analytics/dbt/issues/1197))
- Added docs on how to build a new adapter ([docs](https://docs.getdbt.com/v0.13/docs/building-a-new-adapter)) ([#560](https://github.com/fishtown-analytics/dbt/issues/560))
- Use new logo ([#1349](https://github.com/fishtown-analytics/dbt/pull/1349))

### Fixes
- Fix for Postgres character columns treated as string types ([#1194](https://github.com/fishtown-analytics/dbt/issues/1194))
- Fix for hard to reach edge case in which dbt could hang ([#1223](https://github.com/fishtown-analytics/dbt/issues/1223))
- Fix for `dbt deps` in non-English shells ([#1222](https://github.com/fishtown-analytics/dbt/issues/1222))
- Fix for over eager schema creation when models are run with `--models` ([#1239](https://github.com/fishtown-analytics/dbt/issues/1239))
- Fix for `dbt seed --show` ([#1288](https://github.com/fishtown-analytics/dbt/issues/1288))
- Fix for `is_incremental()` which should only return `True` if the target relation is a `table` ([#1292](https://github.com/fishtown-analytics/dbt/issues/1292))
- Fix for error in Snowflake table materializations with custom schemas ([#1316](https://github.com/fishtown-analytics/dbt/issues/1316))
- Fix errored out concurrent transactions on Redshift and Postgres ([#1356](https://github.com/fishtown-analytics/dbt/pull/1356))
- Fix out of order execution on model select ([#1354](https://github.com/fishtown-analytics/dbt/issues/1354), [#1355](https://github.com/fishtown-analytics/dbt/pull/1355))
- Fix adapter macro namespace issue ([#1352](https://github.com/fishtown-analytics/dbt/issues/1352), [#1353](https://github.com/fishtown-analytics/dbt/pull/1353))
- Re-add CLI flag to toggle warnings as errors ([#1347](https://github.com/fishtown-analytics/dbt/pull/1347))
- Fix release candidate regression that runs run hooks on test invocations ([#1346](https://github.com/fishtown-analytics/dbt/pull/1346))
- Fix Snowflake source quoting ([#1338](https://github.com/fishtown-analytics/dbt/pull/1338), [#1317](https://github.com/fishtown-analytics/dbt/issues/1317), [#1332](https://github.com/fishtown-analytics/dbt/issues/1332))
- Handle unexpected max_loaded_at types ([#1330](https://github.com/fishtown-analytics/dbt/pull/1330))

### Under the hood
- Replace all SQL in Python code with Jinja in macros ([#1204](https://github.com/fishtown-analytics/dbt/issues/1204))
- Loosen restrictions of boto3 dependency ([#1234](https://github.com/fishtown-analytics/dbt/issues/1234))
- Rewrote Postgres introspective queries to be faster on large databases ([#1192](https://github.com/fishtown-analytics/dbt/issues/1192)


### Contributors:
Thanks for your contributions to dbt!

- [@patrickgoss](https://github.com/patrickgoss) [#1193](https://github.com/fishtown-analytics/dbt/issues/1193)
- [@brianhartsock](https://github.com/brianhartsock) [#1191](https://github.com/fishtown-analytics/dbt/pull/1191)
- [@alexyer](https://github.com/alexyer) [#1232](https://github.com/fishtown-analytics/dbt/pull/1232)
- [@adriank-convoy](https://github.com/adriank-convoy) [#1224](https://github.com/fishtown-analytics/dbt/pull/1224)
- [@mikekaminsky](https://github.com/mikekaminsky) [#1216](https://github.com/fishtown-analytics/dbt/pull/1216)
- [@vijaykiran](https://github.com/vijaykiran) [#1198](https://github.com/fishtown-analytics/dbt/pull/1198), [#1199](https://github.com/fishtown-analytics/dbt/pull/1199)

## dbt 0.12.2 - Grace Kelly (January 8, 2019)

### Overview

This release reduces the runtime of dbt projects by improving dbt's approach to model running. Additionally, a number of workflow improvements have been added.

### Deprecations
- Deprecate `sql_where` ([#744](https://github.com/fishtown-analytics/dbt/issues/744)) ([docs](https://docs.getdbt.com/v0.12/docs/configuring-incremental-models))

### Features
- More intelligently order and execute nodes in the graph. This _significantly_ speeds up the runtime of most dbt projects ([#813](https://github.com/fishtown-analytics/dbt/issues/813))
- Add `-m` flag as an alias for `--models` ([#1160](https://github.com/fishtown-analytics/dbt/issues/1160))
- Add `post_hook` and `pre_hook` as aliases for `post-hook` and `pre-hook`, respectively ([#1124](https://github.com/fishtown-analytics/dbt/issues/1124)) ([docs](https://docs.getdbt.com/v0.12/docs/using-hooks))
- Better handling of git errors in `dbt deps` + full support for Windows ([#994](https://github.com/fishtown-analytics/dbt/issues/994), [#778](https://github.com/fishtown-analytics/dbt/issues/778), [#895](https://github.com/fishtown-analytics/dbt/issues/895))
- Add support for specifying a `location` in BigQuery datasets ([#969](https://github.com/fishtown-analytics/dbt/issues/969)) ([docs](https://docs.getdbt.com/v0.12/docs/supported-databases#section-dataset-locations))
- Add support for Jinja expressions using the `{% do ... %}` block ([#1113](https://github.com/fishtown-analytics/dbt/issues/1113))
- The `dbt debug` command is actually useful now ([#1061](https://github.com/fishtown-analytics/dbt/issues/1061))
- The `config` function can now be called multiple times in a model ([#558](https://github.com/fishtown-analytics/dbt/issues/558))
- Source the latest version of dbt from PyPi instead of GitHub ([#1122](https://github.com/fishtown-analytics/dbt/issues/1122))
- Add a peformance profiling mechnanism to dbt ([#1001](https://github.com/fishtown-analytics/dbt/issues/1001))
- Add caching for dbt's macros-only manifest to speedup parsing ([#1098](https://github.com/fishtown-analytics/dbt/issues/1098))

### Fixes
- Fix for custom schemas used alongside the `generate_schema_name` macro ([#801](https://github.com/fishtown-analytics/dbt/issues/801))
- Fix for silent failure of tests that reference nonexistent models ([#968](https://github.com/fishtown-analytics/dbt/issues/968))
- Fix for `generate_schema_name` macros that return whitespace-padded schema names ([#1074](https://github.com/fishtown-analytics/dbt/issues/1074))
- Fix for incorrect relation type for backup tables on Snowflake ([#1103](https://github.com/fishtown-analytics/dbt/issues/1103))
- Fix for incorrectly cased values in the relation cache ([#1140](https://github.com/fishtown-analytics/dbt/issues/1140))
- Fix for JSON decoding error on Python2 installed with Anaconda ([#1155](https://github.com/fishtown-analytics/dbt/issues/1155))
- Fix for unhandled exceptions that occur in anonymous event tracking ([#1180](https://github.com/fishtown-analytics/dbt/issues/1180))
- Fix for analysis files that contain `raw` tags ([#1152](https://github.com/fishtown-analytics/dbt/issues/1152))
- Fix for packages which reference the [hubsite](hub.getdbt.com) ([#1095](https://github.com/fishtown-analytics/dbt/issues/1095))

## dbt 0.12.1 - (November 15, 2018)

### Overview

This is a bugfix release.

### Fixes

- Fix for relation caching when views outside of a dbt schema depend on relations inside of a dbt schema ([#1119](https://github.com/fishtown-analytics/dbt/issues/1119))


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

- Cache the existence of relations to speed up dbt runs ([#1025](https://github.com/fishtown-analytics/dbt/pull/1025))
- Add support for tag configuration and selection ([#1014](https://github.com/fishtown-analytics/dbt/pull/1014))
  - Add tags to the model and graph views in the docs UI ([#7](https://github.com/fishtown-analytics/dbt-docs/pull/7))
- Add the set of schemas that dbt built models into in the `on-run-end` hook context ([#908](https://github.com/fishtown-analytics/dbt/issues/908))
- Warn for unused resource config paths in dbt_project.yml ([#725](https://github.com/fishtown-analytics/dbt/pull/725))
- Add more information to the `dbt --help` output ([#1058](https://github.com/fishtown-analytics/dbt/issues/1058))
- Add support for configuring the profiles directory with an env var ([#1055](https://github.com/fishtown-analytics/dbt/issues/1055))
- Add support for cli and env vars in most `dbt_project.yml` and `profiles.yml` fields ([#1033](https://github.com/fishtown-analytics/dbt/pull/1033))
- Provide a better error message when seed file loading fails on BigQuery ([#1079](https://github.com/fishtown-analytics/dbt/pull/1079))
- Improved error handling and messaging on Redshift ([#997](https://github.com/fishtown-analytics/dbt/issues/997))
- Include datasets with underscores when listing BigQuery datasets ([#954](https://github.com/fishtown-analytics/dbt/pull/954))
- Forgo validating the user's profile for `dbt deps` and `dbt clean` commands ([#947](https://github.com/fishtown-analytics/dbt/issues/947), [#1022](https://github.com/fishtown-analytics/dbt/issues/1022))
- Don't read/parse CSV files outside of the `dbt seed` command ([#1046](https://github.com/fishtown-analytics/dbt/pull/1046))

### Fixes

- Fix for incorrect model selection with the `--models` CLI flag when projects and directories share the same name ([#1023](https://github.com/fishtown-analytics/dbt/issues/1023))
- Fix for table clustering configuration with multiple columns on BigQuery ([#1013](https://github.com/fishtown-analytics/dbt/issues/1013))
- Fix for incorrect output when a single row fails validation in `dbt test` ([#1040](https://github.com/fishtown-analytics/dbt/issues/1040))
- Fix for unwieldly Jinja errors regarding undefined variables at parse time ([#1086](https://github.com/fishtown-analytics/dbt/pull/1086), [#1080](https://github.com/fishtown-analytics/dbt/issues/1080), [#935](https://github.com/fishtown-analytics/dbt/issues/935))
- Fix for incremental models that have a line comment on the last line of the file ([#1018](https://github.com/fishtown-analytics/dbt/issues/1018))
- Fix for error messages when ephemeral models fail to compile ([#1053](https://github.com/fishtown-analytics/dbt/pull/1053))


### Under the hood
- Create adapters as singleton objects instead of classes ([#961](https://github.com/fishtown-analytics/dbt/issues/961))
- Combine project and profile into a single, coherent object ([#973](https://github.com/fishtown-analytics/dbt/pull/973))
- Investigate approaches for providing more complete compilation output ([#588](https://github.com/fishtown-analytics/dbt/issues/588))


### Contributors

Thanks for contributing!

- [@mikekaminsky](https://github.com/mikekaminsky) ([#1049](https://github.com/fishtown-analytics/dbt/pull/1049), [#1060](https://github.com/fishtown-analytics/dbt/pull/1060))
- [@joshtemple](https://github.com/joshtemple) ([#1079](https://github.com/fishtown-analytics/dbt/pull/1079))
- [@k4y3ff](https://github.com/k4y3ff) ([#954](https://github.com/fishtown-analytics/dbt/pull/954))
- [@elexisvenator](https://github.com/elexisvenator) ([#1019](https://github.com/fishtown-analytics/dbt/pull/1019))
- [@clrcrl](https://github.com/clrcrl) ([#725](https://github.com/fishtown-analytics/dbt/pull/725)


## dbt 0.11.1 - Lucretia Mott (September 18, 2018)

### Overview

This is a patch release containing a few bugfixes and one quality of life change for dbt docs.

### Features

- dbt
  - Add `--port` parameter to dbt docs serve ([#987](https://github.com/fishtown-analytics/dbt/pull/987))

### Fixes

- dbt
  - Fix hooks in model configs not running ([#985](https://github.com/fishtown-analytics/dbt/pull/985))
  - Fix integration test on redshift catalog generation ([#977](https://github.com/fishtown-analytics/dbt/pull/977))
  - Snowflake: Fix docs generation errors when QUOTED_IDENTIFIER_IGNORE_CASE is set ([#998](https://github.com/fishtown-analytics/dbt/pull/998))
  - Translate empty strings to null in seeds ([#995](https://github.com/fishtown-analytics/dbt/pull/995))
  - Filter out null schemas during catalog generation ([#992](https://github.com/fishtown-analytics/dbt/pull/992))
  - Fix quoting on drop, truncate, and rename ([#991](https://github.com/fishtown-analytics/dbt/pull/991))
- dbt-docs
  - Fix for non-existent column in schema.yml ([#3](https://github.com/fishtown-analytics/dbt-docs/pull/3))
  - Fixes for missing tests in docs UI when columns are upcased ([#2](https://github.com/fishtown-analytics/dbt-docs/pull/2))
  - Fix "copy to clipboard" ([#4](https://github.com/fishtown-analytics/dbt-docs/issues/4))

## dbt 0.11.0 - Isaac Asimov (September 6, 2018)

### Overview

This release adds support for auto-generated dbt documentation, adds a new syntax for `schema.yml` files, and fixes a number of minor bugs. With the exception of planned changes to Snowflake's default quoting strategy, this release should not contain any breaking changes. Check out the [blog post](https://blog.fishtownanalytics.com/using-dbt-docs-fae6137da3c3) for more information about this release.

### Breaking Changes
- Change default Snowflake quoting strategy to "unquoted" ([docs](https://docs.getdbt.com/v0.11/docs/configuring-quoting)) ([#824](https://github.com/fishtown-analytics/dbt/issues/824))

### Features

- Add autogenerated dbt project documentation ([docs](https://docs.getdbt.com/v0.11/docs/testing-and-documentation)) ([#375](https://github.com/fishtown-analytics/dbt/issues/375), [#863](https://github.com/fishtown-analytics/dbt/issues/863), [#941](https://github.com/fishtown-analytics/dbt/issues/941), [#815](https://github.com/fishtown-analytics/dbt/issues/815))
- Version 2 of schema.yml, which allows users to create table and column comments that end up in the manifest ([docs](https://docs.getdbt.com/v0.11/docs/schemayml-files)) ([#880](https://github.com/fishtown-analytics/dbt/pull/880))
- Extend catalog and manifest to also support Snowflake, BigQuery, and Redshift, in addition to existing Postgres support ([#866](https://github.com/fishtown-analytics/dbt/pull/866), [#857](https://github.com/fishtown-analytics/dbt/pull/857), [#849](https://github.com/fishtown-analytics/dbt/pull/849))
- Add a 'generated_at' field to both the manifest and the catalog. ([#887](https://github.com/fishtown-analytics/dbt/pull/877))
- Add `docs` blocks that users can put into `.md` files and `doc()` value for schema v2 description fields ([#888](https://github.com/fishtown-analytics/dbt/pull/888))
- Write out a 'run_results.json' after dbt invocations. ([#904](https://github.com/fishtown-analytics/dbt/pull/904))
- Type inference for interpreting CSV data is now less aggressive ([#905](https://github.com/fishtown-analytics/dbt/pull/905))
- Remove distinction between `this.table` and `this.schema` by refactoring materialization SQL ([#940](https://github.com/fishtown-analytics/dbt/pull/940))

### Fixes
- Fix for identifier clashes in BigQuery merge statements ([#914](https://github.com/fishtown-analytics/dbt/issues/914))
- Fix for unneccessary downloads of `bumpversion.cfg`, handle failures gracefully ([#907](https://github.com/fishtown-analytics/dbt/issues/907))
- Fix for incompatible `boto3` requirements ([#959](https://github.com/fishtown-analytics/dbt/issues/959))
- Fix for invalid `relationships` test when the parent column contains null values ([#921](https://github.com/fishtown-analytics/dbt/pull/921))

### Contributors

Thanks for contributing!

- [@rsmichaeldunn](https://github.com/rsmichaeldunn) ([#799](https://github.com/fishtown-analytics/dbt/pull/799))
- [@lewish](https://github.com/fishtown-analytics/dbt/pull/915) ([#915](https://github.com/fishtown-analytics/dbt/pull/915))
- [@MartinLue](https://github.com/MartinLue) ([#872](https://github.com/fishtown-analytics/dbt/pull/872))

## dbt 0.10.2 - Betsy Ross (August 3, 2018)

### Overview

This release makes it possible to alias relation names, rounds out support for BigQuery with incremental, archival, and hook support, adds the IAM Auth method for Redshift, and builds the foundation for autogenerated dbt project documentation, to come in the next release.

Additionally, a number of bugs have been fixed including intermittent BigQuery 404 errors, Redshift "table dropped by concurrent query" errors, and a probable fix for Redshift connection timeout issues.

### Contributors

We want to extend a big thank you to our outside contributors for this release! You all are amazing.

- [@danielchalef](https://github.com/danielchalef) ([#818](https://github.com/fishtown-analytics/dbt/pull/818))
- [@mjumbewu](https://github.com/mjumbewu) ([#796](https://github.com/fishtown-analytics/dbt/pull/796))
- [@abelsonlive](https://github.com/abelsonlive) ([#800](https://github.com/fishtown-analytics/dbt/pull/800))
- [@jon-rtr](https://github.com/jon-rtr) ([#800](https://github.com/fishtown-analytics/dbt/pull/800))
- [@mturzanska](https://github.com/mturzanska) ([#797](https://github.com/fishtown-analytics/dbt/pull/797))
- [@cpdean](https://github.com/cpdean) ([#780](https://github.com/fishtown-analytics/dbt/pull/780))

### Features

- BigQuery
  - Support incremental models ([#856](https://github.com/fishtown-analytics/dbt/pull/856)) ([docs](https://docs.getdbt.com/docs/configuring-models#section-configuring-incremental-models))
  - Support archival ([#856](https://github.com/fishtown-analytics/dbt/pull/856)) ([docs](https://docs.getdbt.com/docs/archival))
  - Add pre/post hook support ([#836](https://github.com/fishtown-analytics/dbt/pull/836)) ([docs](https://docs.getdbt.com/docs/using-hooks))
- Redshift: IAM Auth ([#818](https://github.com/fishtown-analytics/dbt/pull/818)) ([docs](https://docs.getdbt.com/docs/supported-databases#section-iam-authentication))
- Model aliases ([#800](https://github.com/fishtown-analytics/dbt/pull/800))([docs](https://docs.getdbt.com/docs/using-custom-aliases))
- Write JSON manifest file to disk during compilation ([#761](https://github.com/fishtown-analytics/dbt/pull/761))
- Add forward and backward graph edges to the JSON manifest file ([#762](https://github.com/fishtown-analytics/dbt/pull/762))
- Add a 'dbt docs generate' command to generate a JSON catalog file ([#774](https://github.com/fishtown-analytics/dbt/pull/774), [#808](https://github.com/fishtown-analytics/dbt/pull/808))

### Bugfixes

- BigQuery: fix concurrent relation loads ([#835](https://github.com/fishtown-analytics/dbt/pull/835))
- BigQuery: support external relations ([#828](https://github.com/fishtown-analytics/dbt/pull/828))
- Redshift: set TCP keepalive on connections ([#826](https://github.com/fishtown-analytics/dbt/pull/826))
- Redshift: fix "table dropped by concurrent query" ([#825](https://github.com/fishtown-analytics/dbt/pull/825))
- Fix the error handling for profiles.yml validation ([#820](https://github.com/fishtown-analytics/dbt/pull/820))
- Make the `--threads` parameter actually change the number of threads used ([#819](https://github.com/fishtown-analytics/dbt/pull/819))
- Ensure that numeric precision of a column is not `None` ([#796](https://github.com/fishtown-analytics/dbt/pull/796))
- Allow for more complex version comparison ([#797](https://github.com/fishtown-analytics/dbt/pull/797))

### Changes

- Use a subselect instead of CTE when building incremental models ([#787](https://github.com/fishtown-analytics/dbt/pull/787))
- Internals
  - Improved dependency selection, rip out some unused dependencies ([#848](https://github.com/fishtown-analytics/dbt/pull/848))
  - Stop tracking `run_error` in tracking code ([#817](https://github.com/fishtown-analytics/dbt/pull/817))
  - Use Mapping instead of dict as the base class for APIObject ([#756](https://github.com/fishtown-analytics/dbt/pull/756))
  - Split out parsers ([#809](https://github.com/fishtown-analytics/dbt/pull/809))
  - Fix `__all__` parameter in submodules ([#780](https://github.com/fishtown-analytics/dbt/pull/780))
  - Switch to CircleCI 2.0 ([#843](https://github.com/fishtown-analytics/dbt/pull/843), [#850](https://github.com/fishtown-analytics/dbt/pull/850))
  - Added tox environments that have the user specify what tests should be run ([#837](https://github.com/fishtown-analytics/dbt/pull/837))

## dbt 0.10.1 (May 18, 2018)

This release focuses on achieving functional parity between all of dbt's adapters. With this release, most dbt functionality should work on every adapter except where noted [here](https://docs.getdbt.com/v0.10/docs/supported-databases#section-caveats).

### tl;dr
 - Configure model schema and name quoting in your `dbt_project.yml` file ([Docs](https://docs.getdbt.com/v0.10/docs/configuring-quoting))
 - Add a `Relation` object to the context to simplify model quoting [Docs](https://docs.getdbt.com/v0.10/reference#relation)
 - Implement BigQuery materializations using new `create table as (...)` syntax, support `partition by` clause ([Docs](https://docs.getdbt.com/v0.10/docs/warehouse-specific-configurations#section-partition-clause))
 - Override seed column types ([Docs](https://docs.getdbt.com/v0.10/reference#section-override-column-types))
 - Add `get_columns_in_table` context function for BigQuery ([Docs](https://docs.getdbt.com/v0.10/reference#get_columns_in_table))

### Changes
 - Consistent schema and identifier quoting ([#727](https://github.com/fishtown-analytics/dbt/pull/727))
   - Configure quoting settings in the `dbt_project.yml` file ([#742](https://github.com/fishtown-analytics/dbt/pull/742))
   - Add a `Relation` object to the context to make quoting consistent and simple ([#742](https://github.com/fishtown-analytics/dbt/pull/742))
 - Use the new `create table as (...)` syntax on BigQuery ([#717](https://github.com/fishtown-analytics/dbt/pull/717))
   - Support `partition by` clause
 - CSV Updates:
   - Use floating point as default seed column type to avoid issues with type inference ([#694](https://github.com/fishtown-analytics/dbt/pull/694))
   - Provide a mechanism for overriding seed column types in the `dbt_project.yml` file ([#708](https://github.com/fishtown-analytics/dbt/pull/708))
   - Fix seeding for files with more than 16k rows on Snowflake ([#694](https://github.com/fishtown-analytics/dbt/pull/694))
   - Implement seeds using a materialization
 - Improve `get_columns_in_table` context function ([#709](https://github.com/fishtown-analytics/dbt/pull/709))
   - Support numeric types on Redshift, Postgres
   - Support BigQuery (including nested columns in `struct` types)
   - Support cross-database `information_schema` queries for Snowflake
   - Retain column ordinal positions

### Bugfixes
 - Fix for incorrect var precendence when using `--vars` on the CLI ([#739](https://github.com/fishtown-analytics/dbt/pull/739))
 - Fix for closed connections in `on-run-end` hooks for long-running dbt invocations ([#693](https://github.com/fishtown-analytics/dbt/pull/693))
 - Fix: don't try to run empty hooks ([#620](https://github.com/fishtown-analytics/dbt/issues/620), [#693](https://github.com/fishtown-analytics/dbt/pull/693))
 - Fix: Prevent seed data from being serialized into `graph.gpickle` file ([#720](https://github.com/fishtown-analytics/dbt/pull/720))
 - Fix: Disallow seed and model files with the same name ([#737](https://github.com/fishtown-analytics/dbt/pull/737))

## dbt 0.10.0 (March 8, 2018)

This release overhauls dbt's package management functionality, makes seeding csv files work across all adapters, and adds date partitioning support for BigQuery.

### Upgrading Instructions:
 - Check out full installation and upgrading instructions [here](https://docs.getdbt.com/docs/installation)
 - Transition the `repositories:` section of your `dbt_project.yml` file to a `packages.yml` file as described [here](https://docs.getdbt.com/docs/package-management)
 - You may need to clear out your `dbt_modules` directory if you use packages like [dbt-utils](https://github.com/fishtown-analytics/dbt-utils). Depending how your project is configured, you can do this by running `dbt clean`.
 - We're using a new CSV parsing library, `agate`, so be sure to check that all of your seed tables are parsed as you would expect!


### Changes
- Support for variables defined on the CLI with `--vars` ([#640](https://github.com/fishtown-analytics/dbt/pull/640)) ([docs](https://docs.getdbt.com/docs/using-variables))
- Improvements to `dbt seed` ([docs](https://docs.getdbt.com/v0.10/reference#seed))
  - Support seeding csv files on all adapters ([#618](https://github.com/fishtown-analytics/dbt/pull/618))
  - Make seed csv's `ref()`-able in models ([#668](https://github.com/fishtown-analytics/dbt/pull/668))
  - Support seed file configuration (custom schemas, enabled / disabled) in the `dbt_project.yml` file ([#561](https://github.com/fishtown-analytics/dbt/issues/561))
  - Support `--full-refresh` instead of `--drop-existing` (deprecated) for seed files ([#515](https://github.com/fishtown-analytics/dbt/issues/515))
  - Add `--show` argument to `dbt seed` to display a sample of data in the CLI ([#74](https://github.com/fishtown-analytics/dbt/issues/74))
- Improvements to package management ([docs](https://docs.getdbt.com/docs/package-management))
  - Deprecated `repositories:` config option in favor of `packages:` ([#542](https://github.com/fishtown-analytics/dbt/pull/542))
  - Deprecated package listing in `dbt_project.yml` in favor of `packages.yml` ([#681](https://github.com/fishtown-analytics/dbt/pull/681))
  - Support stating local file paths as dependencies ([#542](https://github.com/fishtown-analytics/dbt/pull/542))
- Support date partitioning in BigQuery ([#641](https://github.com/fishtown-analytics/dbt/pull/641)) ([docs](https://docs.getdbt.com/docs/creating-date-partitioned-tables))
- Move schema creation to _after_ `on-run-start` hooks ([#652](https://github.com/fishtown-analytics/dbt/pull/652))
- Replace `csvkit` dependency with `agate` ([#598](https://github.com/fishtown-analytics/dbt/issues/598))
- Switch snowplow endpoint to pipe directly to Fishtown Analytics ([#682](https://github.com/fishtown-analytics/dbt/pull/682))

### Bugfixes
- Throw a compilation exception if a required test macro is not present in the context ([#655](https://github.com/fishtown-analytics/dbt/issues/655))
- Make the `adapter_macro` use the `return()` function ([#635](https://github.com/fishtown-analytics/dbt/issues/635))
- Fix bug for introspective query on late binding views (redshift) ([#647](https://github.com/fishtown-analytics/dbt/pull/647))
- Disable any non-dbt log output on the CLI ([#663](https://github.com/fishtown-analytics/dbt/pull/663))


## dbt 0.9.1 (January 2, 2018)

This release fixes bugs and adds supports for late binding views on Redshift.

### Changes
- Support late binding views on Redshift ([#614](https://github.com/fishtown-analytics/dbt/pull/614)) ([docs](https://docs.getdbt.com/docs/warehouse-specific-configurations#section-late-binding-views))
- Make `run_started_at` timezone-aware ([#553](https://github.com/fishtown-analytics/dbt/pull/553)) (Contributed by [@mturzanska](https://github.com/mturzanska)) ([docs](https://docs.getdbt.com/v0.9/reference#run_started_at))

### Bugfixes

- Include hook run time in reported model run time ([#607](https://github.com/fishtown-analytics/dbt/pull/607))
- Add warning for missing test constraints ([#600](https://github.com/fishtown-analytics/dbt/pull/600))
- Fix for schema tests used or defined in packages ([#599](https://github.com/fishtown-analytics/dbt/pull/599))
- Run hooks in defined order ([#601](https://github.com/fishtown-analytics/dbt/pull/601))
- Skip tests that depend on nonexistent models ([#617](https://github.com/fishtown-analytics/dbt/pull/617))
- Fix for `adapter_macro` called within a package ([#630](https://github.com/fishtown-analytics/dbt/pull/630))


## dbt 0.9.0 (October 25, 2017)

This release focuses on improvements to macros, materializations, and package management. Check out [the blog post](https://blog.fishtownanalytics.com/whats-new-in-dbt-0-9-0-dd36f3572ac6) to learn more about what's possible in this new version of dbt.

### Installation

Full installation instructions for macOS, Windows, and Linux can be found [here](https://docs.getdbt.com/v0.9/docs/installation). If you use Windows or Linux, installation works the same as with previous versions of dbt. If you use macOS and Homebrew to install dbt, note that installation instructions have changed:

#### macOS Installation Instructions
```bash
brew update
brew tap fishtown-analytics/dbt
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
- Handle lingering `__dbt_tmp` relations ([#511](https://github.com/fishtown-analytics/dbt/pull/511))
- Run tests defined in an ephemeral directory ([#509](https://github.com/fishtown-analytics/dbt/pull/509))


### Changes
- use `adapter`, `ref`, and `var` inside of macros ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Build custom tests and materializations in dbt packages ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Support pre- and post- hooks that run outside of a transaction ([#510](https://github.com/fishtown-analytics/dbt/pull/510))
- Support table materializations for BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))
- Support querying external data sources in BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))
- Override which schema models are materialized in ([#522](https://github.com/fishtown-analytics/dbt/pull/522)) ([docs](https://docs.getdbt.com/v0.9/docs/using-custom-schemas))
- Make `{{ ref(...) }}` return the same type of object as `{{ this }} `([#530](https://github.com/fishtown-analytics/dbt/pull/530))
- Replace schema test CTEs with subqueries to speed them up for Postgres ([#536](https://github.com/fishtown-analytics/dbt/pull/536)) ([@ronnyli](https://github.com/ronnyli))
 - Bump Snowflake dependency, remove pyasn1 ([#570](https://github.com/fishtown-analytics/dbt/pull/570))


### Documentation
- Document how to [create a package](https://docs.getdbt.com/v0.9/docs/building-packages)
- Document how to [make a materialization](https://docs.getdbt.com/v0.9/docs/creating-new-materializations)
- Document how to [make custom schema tests](https://docs.getdbt.com/v0.9/docs/custom-schema-tests)
- Document how to [use hooks to vacuum](https://docs.getdbt.com/v0.9/docs/using-hooks#section-using-hooks-to-vacuum)
- Document [all context variables](https://docs.getdbt.com/v0.9/reference)


### New Contributors
- [@ronnyli](https://github.com/ronnyli) ([#536](https://github.com/fishtown-analytics/dbt/pull/536))


## dbt 0.9.0 Alpha 5 (October 24, 2017)

### Overview
 - Bump Snowflake dependency, remove pyasn1 ([#570](https://github.com/fishtown-analytics/dbt/pull/570))

## dbt 0.9.0 Alpha 4 (October 3, 2017)

### Bugfixes
 - Fix for federated queries on BigQuery with Service Account json credentials ([#547](https://github.com/fishtown-analytics/dbt/pull/547))

## dbt 0.9.0 Alpha 3 (October 3, 2017)

### Overview
 - Bugfixes
 - Faster schema tests on Postgres
 - Fix for broken environment variables

### Improvements

- Replace schema test CTEs with subqueries to speed them up for Postgres ([#536](https://github.com/fishtown-analytics/dbt/pull/536)) ([@ronnyli](https://github.com/ronnyli))

### Bugfixes
- Fix broken integration tests ([#539](https://github.com/fishtown-analytics/dbt/pull/539))
- Fix for `--non-destructive` on views ([#539](https://github.com/fishtown-analytics/dbt/pull/539))
- Fix for package models materialized in the wrong schema ([#538](https://github.com/fishtown-analytics/dbt/pull/538))
- Fix for broken environment variables ([#543](https://github.com/fishtown-analytics/dbt/pull/543))

### New Contributors

- [@ronnyli](https://github.com/ronnyli)
  - https://github.com/fishtown-analytics/dbt/pull/536

## dbt 0.9.0 Alpha 2 (September 20, 2017)

### Overview

- Custom model schemas
- BigQuery updates
- `ref` improvements

### Bugfixes
- Parity for `statement` interface on BigQuery ([#526](https://github.com/fishtown-analytics/dbt/pull/526))

### Changes
- Override which schema models are materialized in ([#522](https://github.com/fishtown-analytics/dbt/pull/522)) ([docs](https://docs.getdbt.com/v0.9/docs/using-custom-schemas))
- Make `{{ ref(...) }}` return the same type of object as `{{ this }} `([#530](https://github.com/fishtown-analytics/dbt/pull/530))


## dbt 0.9.0 Alpha 1 (August 29, 2017)

### Overview

- More powerful macros
- BigQuery improvements
- Bugfixes
- Documentation (0.9.0 docs can be found [here](https://docs.getdbt.com/v0.9/))

### Breaking Changes
dbt 0.9.0 Alpha 1 introduces a number of new features intended to help dbt-ers write flexible, reusable code. The majority of these changes involve the `macro` and `materialization` Jinja blocks. As this is an alpha release, there may exist bugs or incompatibilites, particularly surrounding these two blocks. A list of known breaking changes is provided below. If you find new bugs, or have questions about dbt 0.9.0, please don't hesitate to reach out in [slack](http://community.getdbt.com/) or [open a new issue](https://github.com/fishtown-analytics/dbt/issues/new?milestone=0.9.0+alpha-1).

##### 1. Adapter functions must be namespaced to the `adapter` context variable
This will manifest as a compilation error that looks like:
```
Compilation Error in model {your_model} (models/path/to/your_model.sql)
  'already_exists' is undefined
```

To fix this error, use `adapter.already_exists` instead of just `already_exists`, or similar for other [adapter functions](https://docs.getdbt.com/docs/adapter).

### Bugfixes
- Handle lingering `__dbt_tmp` relations ([#511](https://github.com/fishtown-analytics/dbt/pull/511))
- Run tests defined in an ephemeral directory ([#509](https://github.com/fishtown-analytics/dbt/pull/509))

### Changes
- use `adapter`, `ref`, and `var` inside of macros ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Build custom tests and materializations in dbt packages ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Support pre- and post- hooks that run outside of a transaction ([#510](https://github.com/fishtown-analytics/dbt/pull/510))
- Support table materializations for BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))
- Support querying external data sources in BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))

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

- Fix errant warning for `dbt archive` commands ([#476](https://github.com/fishtown-analytics/dbt/pull/476))
- Show error (instead of backtrace) for failed hook statements ([#478](https://github.com/fishtown-analytics/dbt/pull/478))
- `dbt init` no longer leaves the repo in an invalid state ([#487](https://github.com/fishtown-analytics/dbt/pull/487))
- Fix bug which ignored git tag specs for package repos ([#463](https://github.com/fishtown-analytics/dbt/issues/463))

### Changes

- Support BigQuery as a target ([#437](https://github.com/fishtown-analytics/dbt/issues/437)) ([#438](https://github.com/fishtown-analytics/dbt/issues/438))
- Make dbt exit codes significant (0 = success, 1/2 = error) ([#297](https://github.com/fishtown-analytics/dbt/issues/297))
- Add context function to pull in environment variables ([#450](https://github.com/fishtown-analytics/dbt/issues/450))

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

- Fix bug for interleaved sort keys on Redshift ([#430](https://github.com/fishtown-analytics/dbt/pull/430))

### Changes
- Don't try to create schema if it already exists ([#446](https://github.com/fishtown-analytics/dbt/pull/446))
- Summarize failures for dbt invocations ([#443](https://github.com/fishtown-analytics/dbt/pull/443))
- Colorized dbt output ([#441](https://github.com/fishtown-analytics/dbt/pull/441))
- Cancel running queries on ctrl-c ([#444](https://github.com/fishtown-analytics/dbt/pull/444))
- Better error messages for common failure modes ([#445](https://github.com/fishtown-analytics/dbt/pull/445))
- Upgrade dependencies ([#431](https://github.com/fishtown-analytics/dbt/pull/431))
- Improvements to `dbt init` and first time dbt usage experience ([#439](https://github.com/fishtown-analytics/dbt/pull/439))

### Documentation
- Document full-refresh requirements for incremental models ([#417](https://github.com/fishtown-analytics/dbt/issues/417))
- Document archival ([#433](https://github.com/fishtown-analytics/dbt/issues/433))
- Document the two-version variant of `ref` ([#432](https://github.com/fishtown-analytics/dbt/issues/432))


## dbt 0.8.1 (May 10, 2017)


### Overview
- Bugfixes
- Reintroduce `compile` command
- Moved docs to [readme.io](https://docs.getdbt.com/)


### Bugfixes

- Fix bug preventing overriding a disabled package model in the current project ([#391](https://github.com/fishtown-analytics/dbt/pull/391))
- Fix bug which prevented multiple sort keys (provided as an array) on Redshift ([#397](https://github.com/fishtown-analytics/dbt/pull/397))
- Fix race condition while compiling schema tests in an empty `target` directory ([#398](https://github.com/fishtown-analytics/dbt/pull/398))

### Changes

- Reintroduce dbt `compile` command ([#407](https://github.com/fishtown-analytics/dbt/pull/407))
- Compile `on-run-start` and `on-run-end` hooks to a file ([#412](https://github.com/fishtown-analytics/dbt/pull/412))

### Documentation
- Move docs to readme.io ([#414](https://github.com/fishtown-analytics/dbt/pull/414))
- Add docs for event tracking opt-out ([#399](https://github.com/fishtown-analytics/dbt/issues/399))


## dbt 0.8.0 (April 17, 2017)


### Overview

- Bugfixes
- True concurrency
- More control over "advanced" incremental model configurations [more info](http://dbt.readthedocs.io/en/master/guide/configuring-models/)

### Bugfixes

- Fix ephemeral load order bug ([#292](https://github.com/fishtown-analytics/dbt/pull/292), [#285](https://github.com/fishtown-analytics/dbt/pull/285))
- Support composite unique key in archivals ([#324](https://github.com/fishtown-analytics/dbt/pull/324))
- Fix target paths ([#331](https://github.com/fishtown-analytics/dbt/pull/331), [#329](https://github.com/fishtown-analytics/dbt/issues/329))
- Ignore commented-out schema tests ([#330](https://github.com/fishtown-analytics/dbt/pull/330), [#328](https://github.com/fishtown-analytics/dbt/issues/328))
- Fix run levels ([#343](https://github.com/fishtown-analytics/dbt/pull/343), [#340](https://github.com/fishtown-analytics/dbt/issues/340), [#338](https://github.com/fishtown-analytics/dbt/issues/338))
- Fix concurrency, open a unique transaction per model ([#345](https://github.com/fishtown-analytics/dbt/pull/345), [#336](https://github.com/fishtown-analytics/dbt/issues/336))
- Handle concurrent `DROP ... CASCADE`s in Redshift ([#349](https://github.com/fishtown-analytics/dbt/pull/349))
- Always release connections (use `try .. finally`) ([#354](https://github.com/fishtown-analytics/dbt/pull/354))

### Changes

- Changed: different syntax for "relationships" schema tests ([#339](https://github.com/fishtown-analytics/dbt/pull/339))
- Added: `already_exists` context function ([#372](https://github.com/fishtown-analytics/dbt/pull/372))
- Graph refactor: fix common issues with load order ([#292](https://github.com/fishtown-analytics/dbt/pull/292))
- Graph refactor: multiple references to an ephemeral models should share a CTE ([#316](https://github.com/fishtown-analytics/dbt/pull/316))
- Graph refactor: macros in flat graph ([#332](https://github.com/fishtown-analytics/dbt/pull/332))
- Refactor: factor out jinja interactions ([#309](https://github.com/fishtown-analytics/dbt/pull/309))
- Speedup: detect cycles at the end of compilation ([#307](https://github.com/fishtown-analytics/dbt/pull/307))
- Speedup: write graph file with gpickle instead of yaml ([#306](https://github.com/fishtown-analytics/dbt/pull/306))
- Clone dependencies with `--depth 1` to make them more compact ([#277](https://github.com/fishtown-analytics/dbt/issues/277), [#342](https://github.com/fishtown-analytics/dbt/pull/342))
- Rewrite materializations as macros ([#356](https://github.com/fishtown-analytics/dbt/pull/356))

## dbt 0.7.1 (February 28, 2017)

### Overview

- [Improved graph selection](http://dbt.readthedocs.io/en/master/guide/usage/#run)
- A new home for dbt
- Snowflake improvements

#### New Features

- improved graph selection for `dbt run` and `dbt test` ([more information](http://dbt.readthedocs.io/en/master/guide/usage/#run)) ([#279](https://github.com/fishtown-analytics/dbt/pull/279))
- profiles.yml now supports Snowflake `role` as an option ([#291](https://github.com/fishtown-analytics/dbt/pull/291))

#### A new home for dbt

In v0.7.1, dbt was moved from the analyst-collective org to the fishtown-analytics org ([#300](https://github.com/fishtown-analytics/dbt/pull/300))

#### Bugfixes

- nicer error if `run-target` was not changed to `target` during upgrade to dbt>=0.7.0


## dbt 0.7.0 (February 9, 2017)

### Overview

- Snowflake Support
- Deprecations

### Snowflake Support

dbt now supports [Snowflake](https://www.snowflake.net/) as a target in addition to Postgres and Redshift! All dbt functionality is supported in this new warehouse. There is a sample snowflake profile in [sample.profiles.yml](https://github.com/fishtown-analytics/dbt/blob/development/sample.profiles.yml) -- you can start using it right away.

### Deprecations

There are a few deprecations in 0.7:

 - `run-target` in profiles.yml is no longer supported. Use `target` instead.
 - Project names (`name` in dbt_project.yml) can now only contain letters, numbers, and underscores, and must start with a letter. Previously they could contain any character.
 - `--dry-run` is no longer supported.

### Notes

#### New Features

- dbt now supports [Snowflake](https://www.snowflake.net/) as a warehouse ([#259](https://github.com/fishtown-analytics/dbt/pull/259))

#### Bugfixes

- use adapter for sort/dist ([#274](https://github.com/fishtown-analytics/dbt/pull/274))

#### Deprecations

- run-target and name validations ([#280](https://github.com/fishtown-analytics/dbt/pull/280))
- dry-run removed ([#281](https://github.com/fishtown-analytics/dbt/pull/281))

#### Changes

- fixed a typo in the docs related to post-run hooks ([#271](https://github.com/fishtown-analytics/dbt/pull/271))
- refactored tracking code to refresh invocation id in a multi-run context ([#273](https://github.com/fishtown-analytics/dbt/pull/273))
- added unit tests for the graph ([#270](https://github.com/fishtown-analytics/dbt/pull/270))

## dbt 0.6.2 (January 16, 2017)

#### Changes

- condense error output when `--debug` is not set ([#265](https://github.com/fishtown-analytics/dbt/pull/265))

## dbt 0.6.1 (January 11, 2017)

#### Bugfixes

- respect `config` options in profiles.yml ([#255](https://github.com/fishtown-analytics/dbt/pull/255))
- use correct `on-run-end` option for post-run hooks ([#261](https://github.com/fishtown-analytics/dbt/pull/261))

#### Changes

- add `--debug` flag, replace calls to `print()` with a global logger ([#256](https://github.com/fishtown-analytics/dbt/pull/256))
- add pep8 check to continuous integration tests and bring codebase into compliance ([#257](https://github.com/fishtown-analytics/dbt/pull/257))

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

For detailed information on how to use Macros, check out the pull request [here](https://github.com/fishtown-analytics/dbt/pull/245)


### Runtime Materialization Configs
DBT Version 0.6.0 introduces two new ways to control the materialization of models:

#### Non-destructive dbt run [more info](https://github.com/fishtown-analytics/dbt/issues/137)

If you provide the `--non-destructive` argument to `dbt run`, dbt will minimize the amount of time during which your models are unavailable. Specfically, dbt
will
 1. Ignore models materialized as `views`
 2. Truncate tables and re-insert data instead of dropping and re-creating

This flag is useful for recurring jobs which only need to update table models and incremental models.

```bash
dbt run --non-destructive
```

#### Incremental Model Full Refresh [more info](https://github.com/fishtown-analytics/dbt/issues/140)

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

### Minor improvements [more info](https://github.com/fishtown-analytics/dbt/milestone/15?closed=1)

#### Add a `{{ target }}` variable to the dbt runtime [more info](https://github.com/fishtown-analytics/dbt/issues/149)
Use `{{ target }}` to interpolate profile variables into your model definitions. For example:

```sql
-- only use the last week of data in development
select * from events

{% if target.name == 'dev' %}
where created_at > getdate() - interval '1 week'
{% endif %}
```

#### User-specified `profiles.yml` dir [more info](https://github.com/fishtown-analytics/dbt/issues/213)
DBT looks for a file called `profiles.yml` in the `~/.dbt/` directory. You can now overide this directory with
```bash
$ dbt run --profiles-dir /path/to/my/dir
```
#### Add timestamp to console output [more info](https://github.com/fishtown-analytics/dbt/issues/125)
Informative _and_ pretty

#### Run dbt from subdirectory of project root [more info](https://github.com/fishtown-analytics/dbt/issues/129)
A story in three parts:
```bash
cd models/snowplow/sessions
vim sessions.sql
dbt run # it works!
```

#### Pre and post run hooks [more info](https://github.com/fishtown-analytics/dbt/issues/226)
```yaml
# dbt_project.yml
name: ...
version: ...

...

# supply either a string, or a list of strings
on-run-start: "create table public.cool_table (id int)"
on-run-end:
  - insert into public.cool_table (id) values (1), (2), (3)
  - insert into public.cool_table (id) values (4), (5), (6)
```

### Bug fixes

We fixed 10 bugs in this release! See the full list [here](https://github.com/fishtown-analytics/dbt/milestone/11?closed=1)

---

## dbt release 0.5.4

### tl;dr
- added support for custom SQL data tests
  - SQL returns 0 results --> pass
  - SQL returns > 0 results --> fail
- dbt-core integration tests
  - running in Continuous Integration environments
    - windows ([appveyor](https://ci.appveyor.com/project/DrewBanin/dbt/branch/development))
    - linux ([circle](https://circleci.com/gh/fishtown-analytics/dbt/tree/master))
  - with [code coverage](https://circleci.com/api/v1/project/fishtown-analytics/dbt/latest/artifacts/0/$CIRCLE_ARTIFACTS/htmlcov/index.html?branch=development)


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

With the dbt 0.5.4 release, dbt now features a robust integration test suite. These integration tests will help mitigate the risk of software regressions, and in so doing, will help us develop dbt more quickly. You can check out the tests [here](https://github.com/fishtown-analytics/dbt/tree/development/test/integration), and the test results [here (linux/osx)](https://circleci.com/gh/fishtown-analytics/dbt/tree/master) and [here (windows)](https://ci.appveyor.com/project/DrewBanin/dbt/branch/development).


### The Future

You can check out the DBT roadmap [here](https://github.com/fishtown-analytics/dbt/milestones). In the next few weeks, we'll be working on [bugfixes](https://github.com/fishtown-analytics/dbt/milestone/11), [minor features](https://github.com/fishtown-analytics/dbt/milestone/15), [improved macro support](https://github.com/fishtown-analytics/dbt/milestone/14), and  [expanded control over runtime materialization configs](https://github.com/fishtown-analytics/dbt/milestone/9).

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

We attempted to refactor the way profiles work in dbt. Previously, a default `user` profile was loaded, and the profiles specified in `dbt_project.yml` or on the command line (`with --profile`) would be applied on top of the `user` config. This implementation is [some of the earliest code](https://github.com/fishtown-analytics/dbt/commit/430d12ad781a48af6a754442693834efdf98ffb1) that was committed to dbt.

As `dbt` has grown, we found this implementation to be a little unwieldy and hard to maintain. The 0.5.2 release made it so that only one profile could be loaded at a time. This profile needed to be specified in either `dbt_project.yml` or on the command line with `--profile`. A bug was errantly introduced during this change which broke the handling of dependency projects.

### The future

The additions of automated testing and a more comprehensive manual testing process will go a long way to ensuring the future stability of dbt. We're going to get started on these tasks soon, and you can follow our progress here: https://github.com/fishtown-analytics/dbt/milestone/16 .

As always, feel free to [reach out to us on Slack](http://community.getdbt.com/) with any questions or concerns:




---

## dbt release 0.5.2

Patch release fixing a bug that arises when profiles are overridden on the command line with the `--profile` flag.

See https://github.com/fishtown-analytics/dbt/releases/tag/v0.5.1

---

## dbt release 0.5.1

### 0. tl;dr

1. Raiders of the Lost Archive -- version your raw data to make historical queries more accurate
2. Column type resolution for incremental models (no more `Value too long for character type` errors)
3. Postgres support
4. Top-level configs applied to your project + all dependencies
5. --threads CLI option + better multithreaded output

### 1. Source table archival https://github.com/fishtown-analytics/dbt/pull/183

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

### 2. Incremental column expansion https://github.com/fishtown-analytics/dbt/issues/175

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

### 3. First-class Postgres support https://github.com/fishtown-analytics/dbt/pull/183

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

### 4. Root-level configs https://github.com/fishtown-analytics/dbt/issues/161

Configurations in `dbt_project.yml` can now be declared at the `models:` level. These configurations will apply to the primary project, as well as any dependency projects. This feature is particularly useful for setting pre- or post- hooks that run for *every* model. In practice, this looks like:

```yaml
name: 'My DBT Project'

models:
    post-hook:
        - "grant select on {{this}} to looker_user"     # Applied to 'My DBT Project' and 'Snowplow' dependency
    'My DBT Project':
        enabled: true
    'Snowplow':
        enabled: true
```

### 5. --threads CLI option https://github.com/fishtown-analytics/dbt/issues/143

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

``` bash
pip install --upgrade dbt
```

### And another thing

- Join us on [slack](http://community.getdbt.com/) with questions or comments

Made with ♥️ by 🐟🏙  📈

---

### 0. tl;dr

- use a temp table when executing incremental models
- arbitrary configuration (using config variables)
- specify branches for dependencies
- more & better docs

### 1. new incremental model generation https://github.com/fishtown-analytics/dbt/issues/138

In previous versions of dbt, an edge case existed which caused the `sql_where` query to select different rows in the `delete` and `insert` steps. As a result, it was possible to construct incremental models which would insert duplicate records into the specified table. With this release, DBT uses a temp table which will 1) circumvent this issue and 2) improve query performance. For more information, check out the GitHub issue: https://github.com/fishtown-analytics/dbt/issues/138

### 2. Arbitrary configuration https://github.com/fishtown-analytics/dbt/issues/146

Configuration in dbt is incredibly powerful: it is what allows models to change their behavior without changing their code. Previously, all configuration was done using built-in parameters, but that actually limits the user in the power of configuration.

With this release, you can inject variables from `dbt_project.yml` into your top-level and dependency models. In practice, variables work like this:

```yml
# dbt_project.yml

models:
  my_project:
    vars:
      exclude_ip: '192.168.1.1'
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

### 3. specify a dependency branch https://github.com/fishtown-analytics/dbt/pull/165

With this release, you can point DBT to a specific branch of a dependency repo. The syntax looks like this:

```
repositories:
    - https://github.com/fishtown-analytics/dbt-audit.git@development # use the "development" branch
```

### 4. More & Better Docs!

Check em out! And let us know if there's anything you think we can improve upon!


### Upgrading

To upgrade to version 0.5.0 of dbt, run:

``` bash
pip install --upgrade dbt
```

---

### 0. tl;dr

- `--version` command
- pre- and post- run hooks
- windows support
- event tracking


### 1. --version https://github.com/fishtown-analytics/dbt/issues/135

The `--version` command was added to help aid debugging. Further, organizations can use it to ensure that everyone in their org is up-to-date with dbt.

```bash
$ dbt --version
installed version: 0.4.7
   latest version: 0.4.7
Up to date!
```

### 2. pre-and-post-hooks https://github.com/fishtown-analytics/dbt/pull/147

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

### 3. Event tracking https://github.com/fishtown-analytics/dbt/issues/89

We want to build the best version of DBT possible, and a crucial part of that is understanding how users work with DBT. To this end, we've added some really simple event tracking to DBT (using Snowplow). We do not track credentials, model contents or model names (we consider these private, and frankly none of our business). This release includes basic event tracking that reports 1) when dbt is invoked 2) when models are run, and 3) basic platform information (OS + python version). The schemas for these events can be seen [here](https://github.com/fishtown-analytics/dbt/tree/development/events/schemas/com.fishtownanalytics)

You can opt out of event tracking at any time by adding the following to the top of you `~/.dbt/profiles.yml` file:

```yaml
config:
    send_anonymous_usage_stats: False
```

### 4. Windows support https://github.com/fishtown-analytics/dbt/pull/154

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

### 1. new dbt command structure https://github.com/fishtown-analytics/dbt/issues/109
```bash
# To run models
dbt run # same as before

# to dry-run models
dbt run --dry # previously dbt test

# to run schema tests
dbt test # previously dbt test --validate
```

### 2. Incremental model improvements https://github.com/fishtown-analytics/dbt/issues/101

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

### 3. Run schema validations concurrently https://github.com/fishtown-analytics/dbt/issues/100

The `threads` run-target config now applies to schema validations too. Try it with `dbt test`

### 4. Connect to database over ssh https://github.com/fishtown-analytics/dbt/issues/93

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
      ssh-host: ssh-host-name  # <------ Add this line
  run-target: dev
```

### Remove the model-defaults config https://github.com/fishtown-analytics/dbt/issues/111

The `model-defaults` config doesn't make sense in a dbt world with dependencies. To apply default configs to your package, add the configs immediately under the package definition:

```yml
models:
    My_Package:
        enabled: true
        materialized: table
        snowplow:
            ...
```

---

## dbt v0.4.0

dbt v0.4.0 provides new ways to materialize models in your database.

### 0. tl;dr
 - new types of materializations: `incremental` and `ephemeral`
 - if upgrading, change `materialized: true|false` to `materialized: table|view|incremental|ephemeral`
 - optionally specify model configs within the SQL file

### 1. Feature: `{{this}}` template variable https://github.com/fishtown-analytics/dbt/issues/81
The `{{this}}` template variable expands to the name of the model being compiled. For example:

```sql
-- my_model.sql
select 'the fully qualified name of this model is {{ this }}'
-- compiles to
select 'the fully qualified name of this model is "the_schema"."my_model"'
```

### 2. Feature: `materialized: incremental` https://github.com/fishtown-analytics/dbt/pull/90

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

### 3. Feature: `materialized: ephemeral` https://github.com/fishtown-analytics/dbt/issues/78

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


### 4. Feature: In-model configs https://github.com/fishtown-analytics/dbt/issues/88

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

### 5. Fix: dbt seed null values https://github.com/fishtown-analytics/dbt/issues/102

Previously, `dbt seed` would insert empty CSV cells as `"None"`, whereas they should have been `NULL`. Not anymore!


---

## dbt v0.3.0

Version 0.3.0 comes with the following updates:

#### 1. Parallel model creation https://github.com/fishtown-analytics/dbt/pull/83
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

For a complete example, check out [a sample profiles.yml file](https://github.com/fishtown-analytics/dbt/blob/master/sample.profiles.yml)

#### 2. Fail only within a single dependency chain https://github.com/fishtown-analytics/dbt/issues/63
If a model cannot be created, it won't crash the entire `dbt run` process. The errant model will fail and all of its descendants will be "skipped". Other models which do not depend on the failing model (or its descendants) will still be created.

#### 3. Logging https://github.com/fishtown-analytics/dbt/issues/64, https://github.com/fishtown-analytics/dbt/issues/65
dbt will log output from the `dbt run` and `dbt test` commands to a configurable logging directory. By default, this directory is called `logs/`. The log filename is `dbt.log` and it is rotated on a daily basic. Logs are kept for 7 days.

To change the name of the logging directory, add the following line to your `dbt_project.yml` file:
```yml
log-path: "my-logging-directory" # will write logs to my-logging-directory/dbt.log
```

#### 4. Minimize time models are unavailable in the database https://github.com/fishtown-analytics/dbt/issues/68
Previously, dbt would create models by:
1. dropping the existing model
2. creating the new model

This resulted in a significant amount of time in which the model was inaccessible to the outside world. Now, dbt creates models by:
1. creating a temporary model `{model-name}__dbt_tmp`
2. dropping the existing model
3. renaming the tmp model name to the actual model name

#### 5. Arbitrarily deep nesting https://github.com/fishtown-analytics/dbt/issues/50
Previously, all models had to be located in a directory matching `models/{model group}/{model_name}.sql`. Now, these models can be nested arbitrarily deeply within a given dbt project. For instance, `models/snowplow/sessions/transformed/transformed_sessions.sql` is a totally valid model location with this release.

To configure these deeply-nested models, just nest the config options within the `dbt_project.yml` file. The only caveat is that you need to specify the dbt project name as the first key under the `models` object, ie:

```yml
models:
  'Your Project Name':
    snowplow:
      sessions:
        transformed:
          transformed_sessions:
            enabled: true
```

More information is available on the [issue](https://github.com/fishtown-analytics/dbt/issues/50) and in the [sample dbt_project.yml file](https://github.com/fishtown-analytics/dbt/blob/master/sample.dbt_project.yml)

#### 6. don't try to create a schema if it already exists https://github.com/fishtown-analytics/dbt/issues/66
dbt run would execute `create schema if not exists {schema}`. This would fail if the dbt user didn't have sufficient permissions to create the schema, even if the schema already existed! Now, dbt checks for the schema existence and only attempts to create the schema if it doesn't already exist.

#### 7. Semantic Versioning
The previous release of dbt was v0.2.3.0 which isn't a semantic version. This and all future dbt releases will conform to semantic version in the format `{major}.{minor}.{patch}`.
---

## dbt v0.2.3.0
Version 0.2.3.0 of dbt comes with the following updates:

#### 1. Fix: Flip referential integrity arguments (breaking)
Referential integrity validations in a `schema.yml` file were previously defined relative to the *parent* table:
```yaml
account:
  constraints:
    relationships:
      - {from: id, to: people, field: account_id}
```

Now, these validations are specified relative to the *child* table
```yaml
people:
  constraints:
    relationships:
      - {from: account_id, to: accounts, field: id}
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
      - {field: type, values: ['paid', 'free']}
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
