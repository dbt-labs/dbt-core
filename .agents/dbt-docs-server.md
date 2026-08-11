# dbt-docs-server — Agent context

Paths in this file are relative to the directory containing `AGENTS.md`.

## What it is

The successor to Core v1's `dbt docs generate` + `dbt docs serve` for the Rust/Fusion runtime.
Ships as `dbt docs serve` inside the Fusion binary. Apache 2.0 crate at
`crates/dbt-docs-server/`.

**Critical:** it does NOT read `manifest.json`. Data comes from parquet files written to
`<target>/index/` by `dbt --use-index` (or `dbtd compile ... --with-index` via a locally-built
Fusion binary). DuckDB is loaded in-memory at server boot and queries those parquet files through
views defined by the `dbt-index-core::Backend` trait.

## Key decisions (see `crates/dbt-docs-server/API-CONTRACTS.md` for full rationale)

| Decision | Rule | Revisit when |
|---|---|---|
| Single-resource detail endpoints | Type-specific endpoints (`/models/:id`, `/sources/:id`). No generic `/nodes/:id` single-resource lookup. | MCP is added to dbt-docs-server |
| `execution_info` placement | Inline in resource detail response, null-gated by `has_run_results` capability. | Run history (last N runs) is required |
| Shared Rust base type | All typed detail handlers must compose a `NodeBase` struct for common fields. | Never remove; only extend |
| Field naming | `snake_case` for all JSON field names and REST path segments (CC-1). | Never |
| Nested objects | Preserve nested objects from Discovery API shape; do not flatten (CC-2). | Only flatten singleton wrappers |
| Capability gating | Nullable fields gated by `Capabilities` flags; no query variants (CC-3). | Never |
| Tests endpoint sub-types | `GET /api/v1/tests/:id` serves both `test.*` and `unit_test.*` as a discriminated union on `resource_type`. Exception to ADR-1 — both are the same concept rendered on the same page (ADR-3). | Unit tests and generic tests need separate detail pages |
| `execution_info` field naming | Bare names only: `status`, `completed_at`, `error`. No `last_run_*` or phase-scoped prefixes — this is a snapshot server, not a history server. `last_known_result` dropped (requires run history). (ADR-4) | Multi-run history is required |

## Data flow

```
dbt project
    │  dbtd compile --with-index
    ▼
<target>/index/*.parquet        ← data source (NOT manifest.json)
    │  loaded at boot via dbt-index-core Backend trait
    ▼
in-memory DuckDB (dbt.* + dbt_rt.* views)
    │  queried by axum handlers (Arrow → JSON)
    ▼
REST API  /api/v1/*             ← no GraphQL, no MCP in v1
    ▼
React SPA (rust-embed, hash routing)
```

Typical local invocation from a project that has already run `dbtd compile --with-index`:

```
dbtd docs serve    # dbtd = locally-built Fusion binary alias
```

## Capability gating

`dbt-docs-server` depends only on `dbt-index-core`'s public traits. Which parquet data exists
varies by how the index was produced, so features are advertised at runtime rather than
assumed:

- `GET /api/v1/capabilities` → `{ has_column_lineage: bool }`
- Calling a gated endpoint without the capability → HTTP 412 `{ code, message, upgrade_path? }`

Never assume a gated field is present; always branch on the capability flag.

## Tech stack

**Frontend** — paths relative to `crates/dbt-docs-server/web/`:
- React 19 + TypeScript + Vite, standalone pnpm project (not part of any workspace)
- Routing: `react-router-dom` `BrowserRouter` with **real paths** (`/details/:dbtUniqueId/`),
  not hash routes. The server answers unknown paths with `index.html`, and the Vite
  base is absolute (`/`) so deep-link reloads resolve assets correctly.
- Data: `@tanstack/react-query` over the `MetadataDataSource` adapter in `src/shared/`
  (`createRestDataSource` maps `/api/v1/*` onto domain types). `src/api.ts` is a
  thin legacy surface that survives for `api.nodes`.
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

**Backend** — paths relative to `crates/dbt-docs-server/`:
- Rust: `axum` + `tokio` + `arrow-array` + `rust-embed`
- All routes registered in `src/server.rs`
- Handler files in `src/handlers/` — one per endpoint group
- `src/state.rs` — `AppState`: DuckDB backend + provider trait objects

## Key files

All paths relative to `crates/dbt-docs-server/`.

| File | Role |
|---|---|
| `web/src/api.ts` | Thin legacy REST surface; `api.nodes` is the one load-bearing caller left |
| `web/src/shared/index.ts` | Barrel for the forked `metadata-shared` layer — mirrors upstream, pruned |
| `web/src/shared/data-sources/rest/` | `createRestDataSource` + `fromRest` mapper: `/api/v1/*` → domain types. **The REST contract lives here**, with ~107 tests |
| `web/src/shared/typings/domain/` | Domain types (`Asset`, `ModelSummary`, `Capabilities`, …) |
| `web/src/shared/typings/discoveryEnums.ts` | Enums transcribed from the Discovery API schema in place of upstream's GraphQL codegen |
| `web/src/lib/resourceType.tsx` | Resource type order, labels, icons, badge colors — single source of truth |
| `web/src/components/LocatePane.tsx` | Sidebar: AssetMode, TreeMode, FilterMode |
| `web/src/components/ModelFilterView.tsx` | Server-side paginated model list with filters |
| `web/src/app.css` | Hand-written CSS (alongside Tailwind utilities in the TSX) |
| `web/dist/` | Committed build embedded by `rust-embed`; regenerate with `pnpm build` |
| `src/server.rs` | axum router — complete route list |
| `src/handlers/` | One file per endpoint group |
| `src/state.rs` | AppState: backend + providers |

## Current API surface

Authoritative sources: `web/src/api.ts` (types) and `src/server.rs` (routes).

| Endpoint | Notes |
|---|---|
| `GET /api/v1/health` | `{ ok, version, project_loaded, generation }` |
| `GET /api/v1/capabilities` | `{ has_column_lineage }` — check before calling gated endpoints |
| `GET /api/v1/project` | Single project metadata |
| `GET /api/v1/nodes` | All nodes, filterable by type/package/q; paginated |
| `GET /api/v1/nodes/counts` | Count per resource type — typed `NodeCountsResponse`, always all 13 fields |
| `GET /api/v1/nodes/:id` | Node detail with columns and deps |
| `GET /api/v1/nodes/:id/lineage` | Model-level lineage graph |
| `GET /api/v1/nodes/:id/column-lineage` | **Gated** — 412 if `has_column_lineage: false` |
| `GET /api/v1/models` | Models only, filterable (modeling_layer, access, owner); paginated |
| `GET /api/v1/models/facets` | Distinct filter values for the models filters |
| `GET /api/v1/tables` | List queryable parquet views with schemas |
| `POST /api/v1/query` | Arbitrary SQL against parquet views (1000-row cap) |

**Not available:**
- UDF/function resources — count is always 0 until Fusion writes UDF parquet

## Adding UI or endpoints

API contracts + ADRs: `crates/dbt-docs-server/API-CONTRACTS.md`

1. Read `crates/dbt-docs-server/API-CONTRACTS.md` — check existing ADRs before proposing any
   design; do not re-litigate closed ADRs.
2. Gate nullable/optional data behind the capabilities endpoint pattern (ADR-1, ADR-2) rather
   than query variants or build-time flags.
3. For data not yet served: add `// TODO: needs GET /api/v1/...` — do not implement the endpoint
   as a side effect of a UI change.
4. Icons: use `DbtResourceIcon resource={type}` (from `@dbt-labs/dbt-dag`) for all resource types
   including group — no special cases needed.
5. CSS: use sourdough CSS vars, follow existing BEM class prefixes in `app.css`.

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
if TypeScript complains; verify with `cargo check -p dbt-docs-server`.

## Analytics relay (`POST /api/v1/analytics/events`)

Server-side relay that consent-gates a batch of docs events and forwards each to
Vortex (`src/handlers/analytics.rs`). The server **hydrates** the fields it can
know authoritatively, so the browser sends a slim event and **omits**:

- `distribution` (DistInfo name)
- `dbt_version`
- `is_logged_in`
- context: `dbt_cloud_account_identifier`, `dbt_cloud_project_id`,
  `dbt_cloud_environment_id`

Any of these sent by the client is **ignored** — the server always wins. Hydration
comes from the `DistInfoProvider::telemetry_hydration()` seam; the env-backed impl
(`dbt-features::EnvDistInfoProvider`) reads `dbt_env::env::InternalEnv::global()`.

The client still supplies its own context (`event_id`, `session_id`, `snowplow_*`,
`referrer_url`, numeric `dbt_cloud_account_id`, `dbt_cloud_user_id`, `feature`) and
the per-event payload fields.
