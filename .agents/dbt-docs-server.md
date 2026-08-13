# dbt-docs-server — Agent context

Paths in this file are relative to the directory containing `AGENTS.md`.

## What it is

The successor to Core v1's `dbt docs generate` + `dbt docs serve` for the Rust/Fusion
runtime. Apache 2.0 crate at `crates/dbt-docs-server/`.

**`dbt docs generate` writes a static site.** There is no server-side query engine and
no HTTP API. The crate exports parquet next to a React SPA; the browser loads
DuckDB-WASM from a CDN and runs every query itself. The output directory works on any
plain file host — GitLab Pages, GitHub Pages, S3 — with no process and no state.
`dbt docs serve` is a static file host for local preview, and generates first when the
site is missing or older than the index.

**Critical:** it does NOT read `manifest.json` — not on the Rust side and not in the
browser. Data comes from parquet files written to `<target>/index/` by a run with
`--write-index`. The exporter reads those through the `dbt-index-core::Backend` trait;
the site reads its own copies over HTTP.

## Key decisions (see `crates/dbt-docs-server/API-CONTRACTS.md` for full rationale)

The ADRs predate the static rearchitecture and are written in terms of REST endpoints.
The *decisions* still bind — they are about what data the UI gets and in what shape —
so read "endpoint" as "the query behind that surface".

| Decision | Rule | Revisit when |
|---|---|---|
| Type-specific detail queries | One projection per resource type. No generic "any node" detail lookup. | MCP is added to dbt-docs-server |
| `execution_info` placement | Nested on the resource detail, null when run results are absent. | Run history (last N runs) is required |
| Shared base projection | Typed detail queries compose the common node fields rather than restating them. | Never remove; only extend |
| Field naming | `snake_case` for every column a mapper reads (CC-1). | Never |
| Nested objects | Preserve nested objects from the Discovery API shape; do not flatten (CC-2). | Only flatten singleton wrappers |
| Capability gating | Nullable fields gated by a capability flag, never by query variants (CC-3). | Never |
| Tests fold together | Test detail serves both `test.*` and `unit_test.*` as a discriminated union on `resource_type`. Both render on the same page (ADR-3). | Unit tests and generic tests need separate detail pages |
| `execution_info` field naming | Bare names only: `status`, `completed_at`, `error`. No `last_run_*` or phase-scoped prefixes — this is a snapshot, not a history. (ADR-4) | Multi-run history is required |
| Browser-direct telemetry | Events are encoded client-side and POSTed straight to Vortex; no relay (ADR-10, supersedes ADR-9). | Consent needs a server-side authority again |

## Data flow

```
dbt project
    │  dbt compile --write-index [--static-analysis strict]
    ▼
<target>/index/*.parquet        ← data source (NOT manifest.json)
    │  dbt docs generate — COPY … TO … per artifact, + the embedded SPA
    ▼
<target>/index.html + assets/   ← the site; the parquet stays in index/
    │  fetched whole by the browser (no range requests)
    ▼
DuckDB-WASM from jsDelivr, in the page
    ▼
React SPA (hash routing, relative asset base)
```

`--static-analysis strict` on the compile is what produces column lineage. Without it
the export writes no `dbt.column_lineage.parquet`, which is the normal case, not an
edge case.

Typical local loop:

```
dbtd compile --write-index          # dbtd = locally-built Fusion binary alias
dbtd docs generate                  # writes into target/, like core v1
cd target && python3 -m http.server # a host with no SPA rewrite and no ranges
```

That last step is the load-bearing check: if it works under `http.server` it works on
GitLab Pages.

## Capability gating

`dbt-docs-server` depends only on `dbt-index-core`'s public traits. Which parquet exists
varies by how the index was produced, so features are detected rather than assumed —
but the detection now happens in the browser:

- `has_column_lineage` ⟸ `dbt.column_lineage.parquet` is present and has rows. One
  source of truth; there is no flag that can disagree with the artifact set.
- A gated surface with no artifact renders the upsell/fallback state. No 412, no error.

Two things follow, and both have bitten:

1. **Absence must look deliberate.** The degraded path is the default path, so a gated
   surface rendering as an empty list next to a non-zero sidebar count reads as data
   loss. Use `isSupported` on `useSourceQuery` and the unsupported-surface message.
2. **The exporter writes every artifact, schema-only when empty**, so client SQL can
   assume every relation exists and skip "view might be missing" variants.

## Tech stack

**Frontend** — paths relative to `crates/dbt-docs-server/web/`:
- React 19 + TypeScript + Vite, standalone pnpm project (not part of any workspace)
- Routing: `react-router-dom` **`HashRouter`**, Vite `base: './'`, `LinkPrefixProvider
  prefix="#/"`. All three are required for subpath-agnostic static hosting: a plain file
  host rewrites nothing, so real paths would 404 on a deep-link reload. The hash also
  never reaches the server, which keeps `document.baseURI` stable for resolving `data/`.
- Data: `@tanstack/react-query` over the `MetadataDataSource` adapter in `src/shared/`.
  `createDuckDbDataSource` is the only production implementation.
- Styling: Tailwind (configured in `tailwind.config.cjs`, with the sourdough and
  dbt-dag presets) **plus** hand-written CSS in `src/app.css` keyed by BEM-style
  class prefixes (e.g. `locate-pane__`). Both are in use — check `app.css` before
  adding a utility-class-only solution, and prefer sourdough CSS custom properties
  (`var(--fgMain)`, `var(--bgMainHover)`, …) over literal colors.
- Icons: `@dbt-labs/sourdough` (Ryecon) and `@dbt-labs/dbt-dag` (`DbtResourceIcon`)
- `src/shared/` is a **fork** of the docs-v2 slice of `@dbt-labs/metadata-shared`
  (dbt-ui). It mirrors upstream's layout and barrel so the two can be diffed. Treat
  it as vendored: prefer additive local changes, and expect upstream drift.
- Private deps (`@dbt-labs/sourdough`, `dbt-dag`, `biga`) come from GitHub Packages,
  so `pnpm install` needs `GITHUB_TOKEN` with `read:packages`. `cargo build` does
  not — `web/dist/` is committed.
- **After any `web/` source edit, run `pnpm build` and commit `web/dist/` in the same
  change.** Nothing enforces this — no CI job, no git hook — so a source-only commit
  silently ships a stale UI.
- `pnpm dev` needs a generated site to read: point `DBT_DOCS_DEV_SITE` at one and the
  `devSite` plugin injects a dev bootstrap and serves its `data/`.

**Backend** — paths relative to `crates/dbt-docs-server/`:
- Rust: `axum` + `tokio` + `arrow-array` + `rust-embed`
- `src/export/` — the whole product: artifact selection, `COPY … TO`, bootstrap
  injection
- `src/server.rs` — a router with **no routes**, only a static fallback
- `src/state.rs` — `AppState`: index dir + provider trait objects

## Key files

All paths relative to `crates/dbt-docs-server/`.

| File | Role |
|---|---|
| `src/export/mod.rs` | The exporter. Refuses to write a site with zero nodes; decides lineage absence from the source file on disk, not from a failed `COPY` |
| `src/export/artifacts.rs` | Which artifacts get written, `DATA_DIR`, and how the index's `dbt.nodes` is split three ways |
| `src/export/bootstrap.rs` | `window.__DBT_DOCS__` injection, with `<`/`>`/`&` escaped so the payload cannot steer the HTML tokenizer |
| `src/server.rs` | Static host. No routes |
| `src/state.rs` | AppState: index dir + providers |
| `web/src/main.tsx` | The only production data-source construction site; throws if the bootstrap is missing |
| `web/src/types.ts` | Shared wire vocabulary that outlived the API (`NodeSummary`, the telemetry event union) |
| `web/src/lib/siteBootstrap.ts` | Reads and version-checks `window.__DBT_DOCS__`; resolves `data/` against `document.baseURI` |
| `web/src/lib/vortexSink.ts` | Browser Vortex producer. `enabled: false` under denied consent, so no call site can leak through |
| `web/src/shared/data-sources/duckdb/` | `engine.ts` (CDN load, `registerFileBuffer`), `bootstrap.ts` (hyparquet first paint), `sql.ts`, `lists.ts`, `details.ts`, `search.ts` |
| `web/src/shared/data-sources/mappers/fromWire.ts` | The single mapping layer, 46 tests. Column names in every SQL projection match what these mappers read — that is the guard against drift |
| `web/src/shared/data-sources/conformance.test.ts` | Protocol-agnostic suite over `MetadataDataSource`; holds the fake and the DuckDB source to one contract |
| `web/src/shared/typings/domain/` | Domain types (`Asset`, `ModelSummary`, `Capabilities`, …) |
| `web/src/lib/resourceType.tsx` | Resource type order, labels, icons, badge colors — single source of truth |
| `web/src/components/LocatePane.tsx` | Sidebar: AssetMode, TreeMode, FilterMode |
| `web/src/app.css` | Hand-written CSS (alongside Tailwind utilities in the TSX) |
| `web/dist/` | Committed build embedded by `rust-embed`; regenerate with `pnpm build` |

## The artifact set

Names follow `dbt.<table>.parquet` / `dbt_rt.<table>.parquet`, mirroring `DBT_TABLES` /
`DBT_RT_TABLES` in `crates/dbt-index-core/src/db.rs`, so the exporter and the browser
key off filenames identically. Three derived splits of `dbt.nodes` keep code blobs off
the cold path:

**There is no exported artifact set.** The site reads `<target>/index/` as
`--write-index` wrote it, so the index is the only contract — no projection, no split,
no copy. `window.__DBT_DOCS__.data_dir` carries the directory (`index/`), and
`--output-dir` copies the index verbatim so a standalone site reads the same files.

Two consequences worth knowing:

1. **First paint reads the whole `dbt.nodes`.** hyparquet projects the nine
   `NodeSummary` columns so the ~1 MB of code is never decoded, but it does cross the
   wire — artifacts are fetched whole, not by range. 1.79 MB at 6,472 nodes against the
   343 KB a dedicated projection would cost. Re-adding a first-paint artifact is the
   documented lever if that becomes a problem.
2. **Empty index tables are declared client-side.** `--write-index` writes a table only
   when it has rows, so `dbt_rt.run_results`, `dbt.source_freshness`,
   `dbt.catalog_stats`, and `dbt.catalog_tables` can be absent. `EMPTY_RELATION_DDL` in
   `engine.ts` declares each as a zero-row relation on 404, which is what keeps one
   version of every query instead of a present/absent pair. A query that reads a new
   column from one of those tables must add it there too.

**Not available:** UDF/function resources — count is always 0 until Fusion writes UDF
parquet.

## Adding UI or queries

Contracts + ADRs: `crates/dbt-docs-server/API-CONTRACTS.md`

1. Read `API-CONTRACTS.md` — check existing ADRs before proposing any design; do not
   re-litigate closed ADRs.
2. **Run the SQL against real parquet before wiring it up.** This is the single
   highest-leverage habit in this crate. Nine bugs in the original port were column
   names that do not exist (`n.language`), columns that are JSON strings rather than
   structs (`meta`), or names that differ from the obvious guess (`dbt.project` stores
   `project_name`, not `name`). None would have been caught by types or by tests
   against fixtures.
3. Keep projection column names identical to what `fromWire.ts` reads. The mappers are
   the contract; renaming a column silently nulls a field.
4. Gate nullable data on artifact presence, never on a build flag or a query variant.
5. Icons: use `DbtResourceIcon resource={type}` (from `@dbt-labs/dbt-dag`) for all
   resource types including group — no special cases needed.
6. CSS: use sourdough CSS vars, follow existing BEM class prefixes in `app.css`.

## Icon reference

`DbtResourceIcon` from `@dbt-labs/dbt-dag` is the canonical icon component for all resource types.
Its `resourceIconMap` maps each type to a Ryecon icon from `@dbt-labs/sourdough`.

| Resource type | `resourceIconMap` entry | Notes |
|---|---|---|
| model | `RyeconModel` | |
| source | `RyeconDatabase` | |
| test, unit_test | `RyeconClipboardSuccess` | unit_test folds into test in the UI |
| exposure | `RyeconMeter` | |
| group | `RyeconGroup` | |
| metric | `RyeconChartColumn` | |
| semantic_model | `RyeconGraphNodes` | |
| seed | `RyeconSeed` | |
| macro | `RyeconFile` | |
| snapshot | `RyeconCamera` | |
| saved_query | `RyeconSave` | |
| function | `RyeconFunction` | count is 0 until Fusion writes UDF parquet |
| analysis | `RyeconCrosshair` | kept for older project compat |

`DbtResourceIcon`'s `resource` prop expects a specific union type. Cast with `resource={t as any}`
if TypeScript complains; verify with `cargo xtask check-llm -p dbt-docs-server`.

## Telemetry

Events go from the browser straight to the Vortex collector — `@dbt-labs/vortex` +
`@dbt-labs/proto` + `@bufbuild/protobuf`, encoded client-side. ADR-10 in
`API-CONTRACTS.md` has the reasoning and supersedes ADR-9's server-side relay.

- **Consent** rides in `window.__DBT_DOCS__.telemetry.enabled`, resolved at export time:
  only the machine running `dbt docs generate` can read the project and the profile.
  It fails closed — an unreadable bootstrap denies consent.
- **Hydration** that the relay used to do authoritatively (`dbt_version`,
  `distribution`, `is_logged_in`, the `dbt_cloud_*` context) is now baked into the
  bootstrap at export time from `DistInfoProvider::telemetry_hydration()`.
- `@dbt-labs/vortex` does `await import("node:fs")` in a dev-mode branch behind
  `PLATFORM === "nodejs"`. It is browser-safe, but Vite will warn about the
  externalized `node:fs` on every build. Do not "fix" this by vendoring the producer.
- The event schema itself is inherited from the relay and is reviewed separately. Do not
  add fields to it as a side effect of a UI change.
