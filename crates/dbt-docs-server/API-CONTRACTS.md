# dbt-docs-server API contracts

## Contents

- [dbt-docs-server API contracts](#dbt-docs-server-api-contracts)
  - [Contents](#contents)
  - [How to use this document](#how-to-use-this-document)
  - [Conventions](#conventions)
  - [ADR-1: Single-resource detail endpoint structure](#adr-1-single-resource-detail-endpoint-structure)
    - [Options considered](#options-considered)
    - [Backend prerequisite](#backend-prerequisite)
  - [ADR-2: `execution_info` placement](#adr-2-execution_info-placement)
    - [Options considered](#options-considered-1)
  - [Backend conventions](#backend-conventions)
    - [`NodeBase` struct](#nodebase-struct)
    - [Capability flags](#capability-flags)
  - [`GET /api/v1/models/:id`](#get-apiv1modelsid)
    - [Example response](#example-response)
    - [Field reference](#field-reference)
    - [Type definition](#type-definition)
    - [Risk register](#risk-register)
  - [`GET /api/v1/nodes/:id` (deferred)](#get-apiv1nodesid-deferred)
  - [ADR-3: `GET /api/v1/tests/:id` response shape](#adr-3-get-apiv1testsid-response-shape-for-test-vs-unit_test)
  - [ADR-4: `execution_info` field naming — single-run semantics](#adr-4-execution_info-field-naming--single-run-semantics)
  - [`GET /api/v1/sources/:id`](#get-apiv1sourcesid)

---

## How to use this document

All architectural decisions about the dbt-docs-server REST API are recorded here as ADRs.
When an ADR status is "Decided", the decision is **closed** — do not re-litigate it in
PRs or planning. To change a closed decision, add a new ADR that supersedes the old one.

Every new endpoint contract must be appended here before implementation begins. A PR that
implements an endpoint without a corresponding contract entry should be rejected in review.

Use `claude/prompts/dbt-docs-parity.md` (local, untracked) to run a parity analysis
that produces the next contract entry.

---

## Conventions

Field naming, data classification, and pagination follow the methodology in
`/Users/eddowh/codaz/poc-dbt-index-docs/FEATURE-TO-ENDPOINT-MAPPING.md`
(outside this repo). Key cross-cutting rules:

| Code | Rule |
|---|---|
| CC-1 | `snake_case` for all JSON field names and REST path segments |
| CC-2 | Preserve nested objects from Discovery API shape; do not flatten (exception: singleton wrappers may be flattened) |
| CC-3 | Nullable fields gated by `Capabilities` flags; no query variants |
| CC-4 | Cursor-based pagination (`?first=&after=`) for list endpoints at scale |
| CC-5 | Three classes of "internal" data: **A** = Discovery-internal but parquet-backed → promote to public REST; **B** = no parquet path → exclude; **C** = public in Discovery but CodexDB-only → stub 412 with `upgrade_path` |

---

## ADR-1: Single-resource detail endpoint structure

**Status:** Decided — type-specific endpoints chosen for v0. Generic dispatcher deferred.
**Trigger to revisit:** MCP is added to dbt-docs-server.

### Options considered

**Generic endpoint only: `GET /api/v1/nodes/:id`**

Returns all resource types as a discriminated union keyed by `resource_type`.

- Pro: single endpoint; no surface proliferation; aligns with `GET /api/v1/nodes` list.
- Pro: FEATURE-TO-ENDPOINT-MAPPING.md Appendix B3 recommended this initially.
- **Con:** TypeScript union types are a UX tax — on a dedicated Model detail page the
  frontend already knows it's rendering a model; narrowing adds overhead at every call
  site on every dedicated detail page.
- **Con:** OpenAPI `oneOf` + discriminator codegen ergonomics depend on toolchain;
  `openapi-typescript` handles it, `swagger-codegen` does not. Adding a codegen pipeline
  as a prerequisite for basic productivity is the wrong tradeoff for v0.
- **Con:** Diverges from Discovery API's per-type operation structure; FE engineers
  familiar with that API face an impedance mismatch.

→ **Rejected.** Union overhead at every call site; benefit accrues only to server-side
maintainers.

---

**Type-specific endpoints: `GET /api/v1/models/:id`, `GET /api/v1/sources/:id`, etc.**

Each resource type has its own endpoint returning a standalone TypeScript type.

- Pro: `ModelDetail`, `SourceDetail`, `TestDetail` are clean standalone types.
- Pro: Mirrors Discovery API's per-type operation structure.
- Pro: FE engineers explicitly requested this.
- Pro: Each endpoint is independently testable and evolvable.
- Con: N endpoints to maintain as resource types grow.
- Con: No generic "give me any resource by `unique_id`" — but this use case does not
  exist in v0 UI: detail pages know the type from routing; lineage components know from
  `resource_type` already present in `NodeSummary`.
- Con: Common fields repeated across Rust structs unless a `NodeBase` is factored out —
  mitigated by the backend prerequisite below.

→ **Chosen for v0.**

---

**Type-specific + generic dispatcher**

Type-specific endpoints exist as above. `GET /api/v1/nodes/:id` also exists as a thin
router that parses the `unique_id` prefix (`model.`, `source.`, etc.) and delegates to
the appropriate typed handler.

- Pro: Adds back the "I have a `unique_id` and don't know the type" escape hatch —
  useful for MCP tools and AI agents.
- Pro: Additive over the chosen option; no rework required.
- Con: No identified v0 UI use case.
- Con: Unnecessary surface invites misuse and undermines the clean-type story.

→ **Deferred, not rejected.** Trigger: MCP lands in dbt-docs-server. At that point a
one-afternoon addition — provided `NodeBase` is already factored out.

### Backend prerequisite

**All typed detail handlers must compose a shared `NodeBase` Rust struct.** Without it,
adding the generic dispatcher later requires duplicating SQL queries across N handlers.
With it, the dispatcher is a string split plus a match expression.

---

## ADR-2: `execution_info` placement

**Status:** Decided — inline in each resource detail response, null-gated by capability.
**Trigger to revisit:** Run history (last N runs) becomes a product requirement.

### Options considered

**Inline in model detail response**

`execution_info` is a nested object in `ModelDetail`. `null` when `dbt build` hasn't
run (gated by `has_run_results` capability).

- Pro: One request for everything the page needs.
- Pro: Consistent with how `columns[]` is already inlined.
- Pro: Recommended by FEATURE-TO-ENDPOINT-MAPPING.md POC analysis for v0.
- Con: If run history (last N runs) is added later, the inline field forces a breaking
  schema change rather than an additive one.

**Separate sub-resource: `GET /api/v1/models/:id/run-results`**

- Pro: Run history can be added later without breaking the model detail contract.
- Con: Two round trips per page render; more client complexity.
- Con: Over-engineered for v0 where only latest run is needed.

→ **Inline chosen.** If run history is added later, promote to a sub-resource — that
change requires a deprecation period or version bump.

---

## Backend conventions

### `NodeBase` struct

All typed detail handlers compose this struct for fields shared across all resource types.

```rust
// Fields common to every resource type — all typed handlers compose this.
// Precondition for ADR-1's deferred generic dispatcher to remain cheap to add.
struct NodeBase {
    unique_id: String,
    name: String,
    resource_type: String,
    package_name: Option<String>,
    description: Option<String>,
    original_file_path: Option<String>,
    tags: Vec<String>,    // dbt.nodes — not yet queried in existing handler; add to SELECT
    fqn: Vec<String>,     // dbt.nodes — not yet queried in existing handler; add to SELECT
}
```

### Capability flags

New flags introduced by these contracts (existing: `has_column_lineage`):

| Flag | Gated surface | Parquet source | Precondition |
|---|---|---|---|
| `has_run_results` | `execution_info` on all resource detail responses | `dbt_rt.run_results` | `dbt build` ran |
| `has_catalog_stats` | `catalog.*` on model/source detail; `catalog_type` on columns | `dbt.catalog_tables` | `dbt docs generate` ran |
| `has_source_freshness` | `freshness` on source detail responses | `dbt.source_freshness` | `dbt source freshness` ran — see Risk #2 in source contract |

These flags must be added to: (1) the `Capabilities` Rust struct, (2) the
`/api/v1/capabilities` handler, (3) the TypeScript `Capabilities` interface in
`web/src/api.ts`. Implementation is tracked here; it is a separate task.

---

## `GET /api/v1/models/:id`

Powers: `ModelView` / `ResourceDetailsPage` in dbt-ui.

dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/ModelView.tsx`

GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/discovery/model.ts`

### Example response

Fields marked `// conditional` are `null` when their capability gate is absent.
Fields marked `// 🔧` are not yet returned — they require a backend change.

```json
{
  "unique_id": "model.jaffle_shop.orders",
  "name": "orders",
  "resource_type": "model",
  "package_name": "jaffle_shop",
  "description": "Final orders model combining payments and order status.",
  "original_file_path": "models/orders.sql",
  "tags": ["finance", "core"],
  "fqn": ["jaffle_shop", "orders"],
  "materialized": "table",
  "database_name": "prod",
  "schema_name": "dbt_prod",
  "relation_name": "prod.dbt_prod.orders",
  "identifier": "orders",
  "access_level": "public",
  "group_name": "finance",
  "contract_enforced": true,
  "raw_code": "select order_id, ...\nfrom {{ ref('stg_orders') }}",
  "compiled_code": "select order_id, ...\nfrom prod.dbt_prod.stg_orders",
  "columns": [
    {
      "name": "order_id",
      "index": 0,
      "data_type": "integer",
      "declared_type": "int",
      "inferred_type": null,
      "catalog_type": "INT64",
      "description": "Unique order identifier.",
      "label": null,
      "granularity": null
    }
  ],
  "depends_on": [
    { "unique_id": "model.jaffle_shop.stg_orders", "edge_type": "model" },
    { "unique_id": "model.jaffle_shop.stg_payments", "edge_type": "model" }
  ],
  "referenced_by": [
    { "unique_id": "exposure.jaffle_shop.revenue_dashboard", "edge_type": "exposure" }
  ],
  "execution_info": {
    "status": "success",
    "completed_at": "2026-05-15T10:32:11Z",
    "execution_time": 4.2
  },
  "catalog": {
    "type": "table",
    "owner": "dbt_runner",
    "bytes_stat": 1048576,
    "row_count_stat": 10500
  }
}
```

`execution_info` is `null` when `has_run_results` is false (i.e., `dbt build` has not run).
`catalog` is `null` when `has_catalog_stats` is false (i.e., `dbt docs generate` has not run).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | ✅ | — | e.g., `"model.pkg.name"` |
| `name` | `string` | Core | ✅ | — | |
| `resource_type` | `"model"` | Core | ✅ | — | Always `"model"` for this endpoint |
| `package_name` | `string \| null` | Core | ✅ | — | |
| `description` | `string \| null` | Core | ✅ | — | |
| `original_file_path` | `string \| null` | Core | ✅ | — | Relative to project root |
| `tags` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `fqn` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `materialized` | `string \| null` | Core | ✅ | — | `"table"` · `"view"` · `"incremental"` · `"ephemeral"` |
| `database_name` | `string \| null` | Core | ✅ | — | |
| `schema_name` | `string \| null` | Core | ✅ | — | |
| `relation_name` | `string \| null` | Core | ✅ | — | Fully qualified: `db.schema.name` |
| `identifier` | `string \| null` | Core | ✅ | — | |
| `access_level` | `string \| null` | Core | ✅ | — | `"public"` · `"protected"` · `"private"` — see Risk #6 |
| `group_name` | `string \| null` | Core | ✅ | — | |
| `contract_enforced` | `boolean \| null` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `raw_code` | `string \| null` | Core | ✅ | — | |
| `compiled_code` | `string \| null` | Core | 🔍 | — | Likely in `dbt.nodes` parquet — confirm schema before implementing |
| `columns` | `ModelColumn[]` | Core | ✅ | — | Empty array if no columns declared |
| `columns[*].name` | `string` | Core | ✅ | — | |
| `columns[*].index` | `number \| null` | Core | ✅ | — | Column order |
| `columns[*].data_type` | `string \| null` | Core | ✅ | — | Declared in YAML |
| `columns[*].declared_type` | `string \| null` | Core | ✅ | — | |
| `columns[*].inferred_type` | `string \| null` | Proprietary | ✅ | — | `null` in Core; populated by Fusion static analysis |
| `columns[*].catalog_type` | `string \| null` | Core-conditional | ✅ | `has_catalog_stats` | Warehouse-verified type; `null` unless `dbt docs generate` ran |
| `columns[*].description` | `string \| null` | Core | ✅ | — | |
| `columns[*].label` | `string \| null` | Core | ✅ | — | |
| `columns[*].granularity` | `string \| null` | Core | ✅ | — | Semantic layer use |
| `depends_on` | `EdgeRef[]` | Core | ✅ | — | 1-hop upstream; see Risk #5 re: pagination |
| `depends_on[*].unique_id` | `string` | Core | ✅ | — | |
| `depends_on[*].edge_type` | `string` | Core | ✅ | — | |
| `referenced_by` | `EdgeRef[]` | Core | ✅ | — | 1-hop downstream; see Risk #5 re: pagination |
| `referenced_by[*].unique_id` | `string` | Core | ✅ | — | |
| `referenced_by[*].edge_type` | `string` | Core | ✅ | — | |
| `execution_info` | `ExecutionInfo \| null` | Core-conditional | 🔧 | `has_run_results` | `null` when `dbt build` hasn't run |
| `execution_info.status` | `string` | Core-conditional | 🔧 | `has_run_results` | `"success"` · `"error"` · `"skipped"` |
| `execution_info.completed_at` | `string \| null` | Core-conditional | 🔧 | `has_run_results` | ISO 8601 timestamp |
| `execution_info.execution_time` | `number \| null` | Core-conditional | 🔧 | `has_run_results` | Seconds (float) |
| `catalog` | `CatalogInfo \| null` | Core-conditional | 🔧 | `has_catalog_stats` | `null` when `dbt docs generate` hasn't run |
| `catalog.type` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | `"table"` · `"view"` · `"materialized view"` |
| `catalog.owner` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Warehouse role that owns the relation |
| `catalog.bytes_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Bytes; warehouse-specific |
| `catalog.row_count_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Approximate row count |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; Discovery-API-internal — see Risk #7 |
| `usage_query_count` | *(absent)* | — | ❌ | — | Class B: no parquet path; Discovery-API-internal |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface ModelDetail {
  unique_id: string;
  name: string;
  resource_type: "model";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  tags: string[];
  fqn: string[];
  materialized: string | null;
  database_name: string | null;
  schema_name: string | null;
  relation_name: string | null;
  identifier: string | null;
  access_level: string | null;
  group_name: string | null;
  contract_enforced: boolean | null;
  raw_code: string | null;
  compiled_code: string | null;
  columns: ModelColumn[];
  depends_on: EdgeRef[];
  referenced_by: EdgeRef[];
  execution_info: ExecutionInfo | null;
  catalog: CatalogInfo | null;
}

interface ModelColumn {
  name: string;
  index: number | null;
  data_type: string | null;
  declared_type: string | null;
  inferred_type: string | null;
  catalog_type: string | null;
  description: string | null;
  label: string | null;
  granularity: string | null;
}

interface ExecutionInfo {
  status: string;
  completed_at: string | null;
  execution_time: number | null;
}

interface CatalogInfo {
  type: string | null;
  owner: string | null;
  bytes_stat: number | null;
  row_count_stat: number | null;
}

interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **`execution_info` absent from current API.** ModelView's run-status badge won't
   render. Requires a 5th parallel query against `dbt_rt.run_results_latest`. Gate
   behind new capability `has_run_results`.

2. **`tags`, `fqn`, `contract_enforced` in parquet but not queried.** The handler
   (`src/handlers/nodes.rs`) has a fixed column SELECT. Add these columns and update
   `ModelDetail`. Low-risk once parquet schema is confirmed.

3. **`compiled_code` presence in parquet is unverified.** Confirm against actual parquet
   schema before implementing. Mark as TODO and omit from the contract if absent.

4. **Catalog node-level fields require a new join.** `catalog_type` already exists at
   the column level. Node-level `catalog.*` fields require joining `dbt.catalog_tables`.
   Gate behind new capability `has_catalog_stats`.

5. **`depends_on`/`referenced_by` have no pagination cap.** A high-fan-out model (100+
   downstream consumers) returns an unbounded array. For v0: add a `?first=` cap with
   `truncated: true` in the response. Promote to cursor pagination when a lineage
   sub-resource is built.

6. **`access_level` enum values need verification.** dbt-ui uses `AccessLevel`
   (`public | protected | private`). The current field is `string | null`. Confirm the
   string values match before the FE renders access badges to avoid silent mismatches.

7. **`health_issues` is Class B — no parquet path.** It is `subGraphs: ['internal']` in
   codex-api AND absent from all 34 parquet tables. The FE must render a graceful null
   state; do not add the field. Document explicitly so FE engineers don't chase it.

8. **Per-node test list has no handler coverage.** Requires a join across
   `dbt.test_metadata` and `dbt_rt.run_results`. **Open question:** inline a test summary
   array in `ModelDetail`, or introduce `GET /api/v1/models/:id/tests`? Resolve before
   implementing.

---

## ADR-3: `GET /api/v1/tests/:id` response shape for `test` vs `unit_test`

**Status:** Decided — single endpoint, discriminated union on `resource_type`.
**Trigger to revisit:** Unit tests and generic tests diverge enough to require a
dedicated UI page (currently both render via `TestView.tsx`).

### Context

`GET /api/v1/tests/:id` must serve two structurally distinct resource types:

| `resource_type` | `unique_id` prefix | Distinctive fields |
|---|---|---|
| `test` | `test.` | `column_name`, `test_metadata.kwargs`, `status`, `error` |
| `unit_test` | `unit_test.` | `given` rows, `expect` rows |

Both fold into the same "Tests" tab in dbt-ui (`TestView.tsx` handles both).
ADR-1 chose type-specific endpoints over a generic discriminated union across all
resource types — but `GET /api/v1/tests/:id` is itself a union of two sub-types.

### Options considered

**Single endpoint, discriminated union on `resource_type`**

`TestDetail` is `GenericTestDetail | UnitTestDetail`, narrowed by `resource_type`.
The two types share a common base and extend it with type-specific fields.

- Pro: one endpoint, one fetch per test detail page.
- Pro: both types are conceptually "test results" rendered on the same page — the
  distinction is an implementation detail, not a product-level one.
- Pro: consistent with how `GET /api/v1/nodes/:id` (deferred) would dispatch.
- Con: the response type is a union; FE must narrow on `resource_type`.

**Two endpoints: `/tests/:id` and `/unit_tests/:id`**

- Pro: fully separate types, no narrowing needed.
- Pro: strictest alignment with ADR-1's "one type per endpoint" principle.
- Con: FE must inspect `resource_type` (or parse the `unique_id` prefix) before
  routing to the right endpoint — the branching just moves to the call site.
- Con: tests and unit tests share a page and a concept; splitting endpoints
  diverges from the user's mental model.

→ **Single endpoint chosen.** There is no world where tests and unit tests belong
on separate detail pages. The union is an exception to ADR-1's general principle,
justified by the fact that `test` and `unit_test` are the same concept (test
coverage of a model) rendered identically in the UI.

---

## `GET /api/v1/nodes/:id` (deferred)

**Status:** Deferred — no v0 UI use case identified.
**Trigger to add:** MCP is added to dbt-docs-server.

When added: parse the `unique_id` prefix (`model.` → models handler, `source.` →
sources handler, etc.) and delegate to the appropriate typed handler. No logic
duplication. OpenAPI response type: `oneOf [ModelDetail, SourceDetail, ...]` with
`resource_type` as discriminator.

**Precondition:** `NodeBase` struct must already exist (ADR-1 backend prerequisite).

---

## ADR-4: `execution_info` field naming — single-run semantics

**Status:** Decided — bare field names without `last_run_*` or phase-scoped prefixes.
**Trigger to revisit:** Multi-run history (last N runs) becomes a product requirement.

dbt-docs-server is a **snapshot server**, not a history server. Every query reflects a
single indexed state: the output of one `dbt build` / `dbt seed` / `dbt snapshot` run
captured in parquet files at `<target>/index/`. There is no run timeline, no "previous
run" to contrast with a "last run." The word "last" implies a sequence; dbt-docs-server
exposes only the current snapshot.

Discovery API fields like `lastRunStatus`, `executeCompletedAt`, and `lastRunError` carry
prefixes because CodexDB has access to full run history. Importing those prefixes into
dbt-docs-server would be semantically misleading — `last_run_status` implies there could
be a `second_to_last_run_status`. The `execute_` prefix on `executeCompletedAt` is an
internal timing-phase name (compile vs. execute) that has no relevance to API consumers.

`lastKnownResult` (Discovery API for tests) tracks whether a test passed *before* a schema
change invalidated it — a concept that requires run history to be meaningful. In
dbt-docs-server the index is always one coherent snapshot: either the test ran and `status`
reflects the result, or the test hasn't run and `execution_info` is `null`. The "known vs.
actual" distinction does not exist and the field is **dropped**.

### Decision

`execution_info` fields use bare names:

| Discovery API field | dbt-docs-server field | Reason |
|---|---|---|
| `lastRunStatus` | `status` | No "last" — only one indexed run |
| `executeCompletedAt` | `completed_at` | `execute_` is an internal timing phase name |
| `lastRunError` | `error` | Same prefix problem |
| `lastKnownResult` | *(dropped)* | Requires run history; meaningless in snapshot world |

### If multi-run history is ever required

The path is a **new `runs[]` sub-resource**, not retrofitting `last_*` prefixes onto
existing fields. For example: `GET /api/v1/models/:id/runs` returns `Run[]` where each
`Run` has `status`, `completed_at`, `error`, `execution_time`. The inline `execution_info`
on the detail response becomes a convenience shortcut for the most-recent entry. This is
an additive change with no breaking impact on existing contracts.

---

## `GET /api/v1/sources/:id`

Powers: `SourceView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/SourceView.tsx`
GraphQL hooks: `packages/metadata/dbt-explorer/src/hooks/dbtStrategy/useSource.ts` → `src/hooks/discovery/source.ts` (`GetSourceByUniqueId`)

**No new ADR needed.** This endpoint follows ADR-1 (type-specific) and ADR-2 (conditional
data inlined, null-gated by capability) without exception. `freshness` replaces
`execution_info` as the Core-conditional surface for sources.

### Example response

`freshness` is `null` when `has_source_freshness` is false (i.e., `dbt source freshness` has not run).
`catalog` is `null` when `has_catalog_stats` is false (i.e., `dbt docs generate` has not run).
Fields marked `// 🔧` are not yet returned — they require a backend change.

```json
{
  "unique_id": "source.jaffle_shop.raw_jaffle.orders",
  "name": "orders",
  "resource_type": "source",
  "package_name": "jaffle_shop",
  "description": "Raw orders table from the production Postgres database.",
  "original_file_path": "models/staging/sources.yml",
  "tags": ["raw", "jaffle"],
  "fqn": ["jaffle_shop", "raw_jaffle", "orders"],
  "database_name": "raw",
  "schema_name": "jaffle_shop",
  "identifier": "orders",
  "source_name": "raw_jaffle",
  "source_description": "Raw tables synced from the Jaffle Shop production database.",
  "loader": "fivetran",
  "meta": { "owner": "data-eng" },
  "referenced_by": [
    { "unique_id": "model.jaffle_shop.stg_orders", "edge_type": "model" }
  ],
  "columns": [
    {
      "name": "id",
      "index": 0,
      "data_type": "integer",
      "declared_type": "int",
      "inferred_type": null,
      "catalog_type": "INT64",
      "description": "Unique order identifier.",
      "label": null,
      "granularity": null
    }
  ],
  "freshness": {
    "status": "pass",
    "snapshotted_at": "2026-05-15T10:00:00Z",
    "max_loaded_at": "2026-05-15T09:45:00Z",
    "max_loaded_at_time_ago": 900.0,
    "criteria": {
      "error_after": { "count": 24, "period": "hour" },
      "warn_after": { "count": 12, "period": "hour" }
    }
  },
  "catalog": {
    "type": "table",
    "owner": "fivetran",
    "comment": "Raw orders synced from production PostgreSQL.",
    "primary_key": ["id"],
    "row_count_stat": 50000,
    "bytes_stat": 2097152,
    "stats": [
      {
        "id": "has_stats",
        "label": "Has Stats?",
        "value": "true",
        "description": "Indicates whether there are statistics for this table",
        "include": false
      }
    ]
  }
}
```

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | ✅ | — | e.g., `"source.pkg.source_name.table_name"` — 4-part unique_id |
| `name` | `string` | Core | ✅ | — | Table name within the source block |
| `resource_type` | `"source"` | Core | ✅ | — | Always `"source"` for this endpoint |
| `package_name` | `string \| null` | Core | ✅ | — | |
| `description` | `string \| null` | Core | ✅ | — | Per-table description from YAML |
| `original_file_path` | `string \| null` | Core | ✅ | — | Path to the `.yml` file containing the source definition |
| `tags` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `fqn` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; 3-part for sources: `[pkg, source_name, table]` |
| `database_name` | `string \| null` | Core | ✅ | — | |
| `schema_name` | `string \| null` | Core | ✅ | — | |
| `identifier` | `string \| null` | Core | ✅ | — | Overrides table name if set; falls back to `name` |
| `source_name` | `string \| null` | Core | 🔧 | — | dbt source block name (e.g., `"raw_jaffle"`) — in `dbt.nodes` parquet |
| `source_description` | `string \| null` | Core | 🔧 | — | Block-level description from YAML — in `dbt.nodes` parquet |
| `loader` | `string \| null` | Core | 🔧 | — | e.g., `"fivetran"`, `"airbyte"` — in `dbt.nodes` parquet |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSONB blob — confirm `dbt.nodes` parquet includes a `meta` column |
| `referenced_by` | `EdgeRef[]` | Core | ✅ | — | Downstream models; sources have **no** `depends_on` |
| `referenced_by[*].unique_id` | `string` | Core | ✅ | — | |
| `referenced_by[*].edge_type` | `string` | Core | ✅ | — | |
| `columns` | `SourceColumn[]` | Core | ✅ | — | Identical shape to `ModelColumn[]` |
| `columns[*].name` | `string` | Core | ✅ | — | |
| `columns[*].index` | `number \| null` | Core | ✅ | — | |
| `columns[*].data_type` | `string \| null` | Core | ✅ | — | Declared in YAML |
| `columns[*].declared_type` | `string \| null` | Core | ✅ | — | |
| `columns[*].inferred_type` | `string \| null` | Proprietary | ✅ | — | `null` in Core; populated by Fusion static analysis |
| `columns[*].catalog_type` | `string \| null` | Core-conditional | ✅ | `has_catalog_stats` | Warehouse-verified type |
| `columns[*].description` | `string \| null` | Core | ✅ | — | |
| `columns[*].label` | `string \| null` | Core | ✅ | — | |
| `columns[*].granularity` | `string \| null` | Core | ✅ | — | |
| `freshness` | `FreshnessInfo \| null` | Core-conditional | 🔧 | `has_source_freshness` | `null` if `dbt source freshness` hasn't run — see Risk #2 |
| `freshness.status` | `string` | Core-conditional | 🔧 | `has_source_freshness` | `"pass"` · `"warn"` · `"error"` · `"runtime error"` |
| `freshness.snapshotted_at` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | ISO 8601; when freshness was last checked |
| `freshness.max_loaded_at` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | ISO 8601; most recent row timestamp from the source table |
| `freshness.max_loaded_at_time_ago` | `number \| null` | Core-conditional | 🔧 | `has_source_freshness` | Seconds elapsed since `max_loaded_at` |
| `freshness.criteria.error_after.count` | `number \| null` | Core-conditional | 🔧 | `has_source_freshness` | |
| `freshness.criteria.error_after.period` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | `"minute"` · `"hour"` · `"day"` |
| `freshness.criteria.warn_after.count` | `number \| null` | Core-conditional | 🔧 | `has_source_freshness` | |
| `freshness.criteria.warn_after.period` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | `"minute"` · `"hour"` · `"day"` |
| `catalog` | `SourceCatalogInfo \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Superset of model `CatalogInfo` — adds `comment`, `primary_key`, `stats[]` |
| `catalog.type` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.owner` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.comment` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Warehouse table comment — source-only field |
| `catalog.primary_key` | `string[]` | Core-conditional | 🔧 | `has_catalog_stats` | Column names constituting the PK; empty array if none — source-only field |
| `catalog.row_count_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.bytes_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats` | `CatalogStat[]` | Core-conditional | 🔧 | `has_catalog_stats` | Arbitrary warehouse statistics — source-only field |
| `catalog.stats[*].id` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Stat identifier |
| `catalog.stats[*].label` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Human-readable label |
| `catalog.stats[*].value` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Always a string; parse as number if needed |
| `catalog.stats[*].description` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats[*].include` | `boolean` | Core-conditional | 🔧 | `has_catalog_stats` | Whether the stat should be displayed in the UI |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api |

**Fields absent from `SourceDetail` that exist on `ModelDetail`:**
Sources have no SQL, no materialization strategy, no dbt-managed relation, and no run
execution. The following fields from `ModelDetail` are intentionally omitted:
`materialized`, `relation_name`, `access_level`, `group_name`, `contract_enforced`,
`raw_code`, `compiled_code`, `depends_on`, `execution_info`.

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface SourceDetail {
  unique_id: string;
  name: string;
  resource_type: "source";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  tags: string[];
  fqn: string[];
  database_name: string | null;
  schema_name: string | null;
  identifier: string | null;
  source_name: string | null;
  source_description: string | null;
  loader: string | null;
  meta: Record<string, unknown> | null;
  referenced_by: EdgeRef[];
  columns: SourceColumn[];
  freshness: FreshnessInfo | null;
  catalog: SourceCatalogInfo | null;
}

// SourceColumn is identical in shape to ModelColumn
interface SourceColumn {
  name: string;
  index: number | null;
  data_type: string | null;
  declared_type: string | null;
  inferred_type: string | null;
  catalog_type: string | null;
  description: string | null;
  label: string | null;
  granularity: string | null;
}

interface FreshnessInfo {
  status: string;
  snapshotted_at: string | null;
  max_loaded_at: string | null;
  max_loaded_at_time_ago: number | null;
  criteria: {
    error_after: { count: number | null; period: string | null } | null;
    warn_after: { count: number | null; period: string | null } | null;
  } | null;
}

// SourceCatalogInfo extends model CatalogInfo with source-specific fields
interface SourceCatalogInfo {
  type: string | null;
  owner: string | null;
  comment: string | null;
  primary_key: string[];
  row_count_stat: number | null;
  bytes_stat: number | null;
  stats: CatalogStat[];
}

interface CatalogStat {
  id: string;
  label: string;
  value: string;
  description: string;
  include: boolean;
}

// EdgeRef is shared with ModelDetail
interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **`source_name`, `source_description`, `loader` not yet queried.** These are
   source-specific fields in `dbt.nodes` parquet that aren't in the current handler
   SELECT. Add them alongside `tags`, `fqn`, and `contract_enforced` in the same
   handler change.

2. **Freshness parquet coexistence is unverified.** `dbt.source_freshness.parquet`
   contains all the fields needed, but it's written by a separate command (`dbt source
   freshness`) from the one that writes `dbt.nodes.parquet` (`dbt build` / `dbt parse`).
   The question from FEATURE-TO-ENDPOINT-MAPPING.md Appendix B1: does `dbt --use-index
   source freshness` use MergePrune semantics that preserve `nodes.parquet`, or does it
   overwrite the index directory? If it overwrites: freshness is not available in
   stateless docs and must be treated as Platform-tier. **Verify with the dbt-index team
   before implementing `has_source_freshness`.**

3. **`meta` JSONB presence in parquet is unverified.** The `meta` field is a JSONB
   object in codex-api's Prisma schema. Confirm it's serialized into `dbt.nodes.parquet`
   as a JSON string column before adding it to the SELECT.

4. **`SourceCatalogInfo` is a superset of `CatalogInfo`.** The model catalog type
   (`CatalogInfo`) does not include `comment`, `primary_key`, or `stats[]`. The Rust
   handler will need a separate response struct for source catalog data, or `CatalogInfo`
   must be extended. Decide before implementing to avoid a breaking change to `ModelDetail`.

5. **`catalog.stats[]` schema is warehouse-dependent.** The stat entries (e.g.,
   `has_stats`, `row_count`, `bytes`) vary by adapter. The `value` field is always a
   string regardless of the underlying type — document this explicitly so FE engineers
   don't attempt numeric parsing without a string-to-number conversion.

6. **Sources have no `depends_on`.** The current `NodeDetail` handler always returns
   both `depends_on` and `referenced_by`. The `SourceDetail` handler must omit
   `depends_on` entirely (not return an empty array) to avoid FE engineers
   misinterpreting an empty array as "no upstream sources found."
