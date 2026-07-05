# agents-demo

A minimal dbt project that demonstrates the v0 **`dbt agent`** workflow: author
governed agents as YAML in the project, resolve their scope against the manifest,
and push each one to a Fivetran AI MCP contextset.

## Layout

```
agents-demo/
├── dbt_project.yml         project + groups + access defaults
├── profiles.yml            zero-setup DuckDB target
├── models/
│   ├── _models.yml         model metadata (groups, tags, access)
│   ├── sales/              group:sales — dim_customers, fct_orders, fct_orders_pii
│   ├── support/            group:support — tickets
│   └── finance/            group:finance, access:private — revenue
└── agents/
    ├── sales_analyst.yml   group:sales,tag:certified minus tag:pii
    ├── support_triage.yml  group:support + tickets lineage
    └── finance_read.yml    group:finance (private scope)
```

## Prerequisites

- A `dbt` binary built from this branch (`agents/v0-node-type`):
  ```
  cargo build --release --package dbt-features
  ```
  The binary lands at `target/release/dbt` (from the repo root).
- For deploy: a reachable Fivetran AI MCP endpoint with a valid bearer token.

## Runbook

### 1. Parse the project

Produce `target/manifest.json`. The agent tool reads it — no warehouse connection
needed for scope resolution.

```bash
cd examples/agents-demo
dbt parse --profiles-dir .
```

### 2. Enumerate agents

```bash
dbt agent list --project-dir .
```

Expected:

```
finance_read    Read-only access to recognized-revenue rollups.
sales_analyst   Answers questions about the sales pipeline. Excludes PII columns.
support_triage  Routes incoming tickets. Scope: the support group and its lineage.
```

### 3. Show resolved scope (dry-run, no network)

```bash
dbt agent show sales_analyst --project-dir .
```

Expected shape (abridged):

```json
[
  {
    "name": "sales_analyst",
    "scope": { ... },
    "resolved_scope": {
      "schema_fqns": ["dev.main"],
      "table_fqns": [
        "dev.main.dim_customers",
        "dev.main.fct_orders"
      ],
      "model_unique_ids": [
        "model.agents_demo.dim_customers",
        "model.agents_demo.fct_orders"
      ]
    }
  }
]
```

Note that `fct_orders_pii` was pulled in by `group:sales` but subtracted by
`exclude: tag:pii` — a subtractive selector, not a filter.

The command also writes `target/agents.json` for inspection.

### 4. Dry-run the deploy

```bash
dbt agent deploy sales_analyst \
  --project-dir . \
  --dry-run \
  --url https://mcp.fivetran.example.com \
  --token dummy \
  --group-id demo-group
```

Prints the PUT payload without hitting the network. Use this in reviews to show
exactly what will be sent.

### 5. Live deploy

Set your credentials once:

```bash
export DBT_FIVETRAN_MCP_URL=https://mcp.fivetran.example.com
export DBT_FIVETRAN_MCP_TOKEN=<bearer>
export DBT_FIVETRAN_GROUP_ID=<group id>
```

Deploy all agents:

```bash
dbt agent deploy --project-dir .
```

Each agent PUTs to `PUT /contextsets/<name>?group_id=<group>` with a body of
`{schema_fqns, table_fqns}`.

### 6. Verify

Confirm the contextset landed in Fivetran using the low-level admin command:

```bash
dbt contextset get sales_analyst
```

Or exercise the enforcement path — call the Fivetran MCP `execute_aisql` tool
with `contextset=sales_analyst` for an in-scope table (succeeds) and again for
`fct_orders_pii` (blocked).

## What each agent demonstrates

| Agent            | Selector shape                                | Point of the demo                                     |
| ---------------- | --------------------------------------------- | ----------------------------------------------------- |
| `sales_analyst`  | `group:sales,tag:certified` + `dim_customers+` minus `tag:pii` | Union of selectors, graph op, subtractive exclude     |
| `support_triage` | `group:support` + `+tickets`                  | Ancestor graph op — pulls in upstream models          |
| `finance_read`   | `group:finance`                               | Access modifier on a private surface                  |

## Editing loop

1. Edit any `agents/*.yml`.
2. `dbt agent show <name>` to see the new resolved scope.
3. `dbt agent deploy <name>` to push. The Fivetran server treats each PUT as a
   full replacement of the contextset by that name.

## Known v0 caveats

- `tools.include` is stored on the agent but **not** server-enforced yet — the
  Fivetran MCP still exposes every tool it has. Enforcement is a v0.5 add.
- `mcp_server: fivetran_primary` in the YAML is metadata only. In v0 the actual
  URL/token/group_id come from CLI flags or environment variables. A first-class
  `mcp-servers:` block in `dbt_project.yml` is deferred to v0.5.
- Only models with a resolved `relation_name` land in the contextset. Ephemerals,
  disabled models, and sources are excluded by design.
- Supported selector syntax subset: bare name, `resource_type:`, `tag:`, `group:`,
  and `+`/`N+`/`+N`/`N+…+N` graph ops. `,` inside one expression is intersect;
  list entries are union. Not supported in v0: `state:`, `source:`, `test:`,
  `path:`, wildcards, or parenthesized boolean expressions.
