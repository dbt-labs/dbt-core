<div align="center">
  <h1>dbt-docs-server</h1>
  <p><strong>The next generation of dbt docs.</strong></p>
  <p>
    Serves an interactive docs site and a JSON REST API for your dbt project, straight from the parquet artifacts the Fusion engine writes on every run.
  </p>
  <p>
    <a href="./API-CONTRACTS.md">API contracts</a> ·
    <a href="https://github.com/dbt-labs/dbt-core">dbt Core repo</a> ·
    <a href="https://docs.getdbt.com/docs/fusion/about-fusion">About Fusion</a> ·
    <a href="https://docs.getdbt.com">Official dbt docs</a>
  </p>
  <p>
    <img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" />
    <img alt="Rust" src="https://img.shields.io/badge/built%20with-Rust-orange.svg" />
  </p>
</div>

---

## 👋 Introduction

`dbt-docs-server` is the successor to dbt Core v1's `dbt docs generate` + `dbt docs serve`, rebuilt for the Rust/Fusion runtime. It ships inside the Fusion binary as `dbt docs serve` and can also be self-hosted in a container.

The server is **read-only** and **stateless**: it loads parquet artifacts into an in-memory analytical engine at boot and answers queries against them. Point it at a fresh set of artifacts and restart to refresh.

## ⚙️ How it works

Data comes from parquet files that the Fusion engine writes to `<target>/index/` when you run with `--write-index`.

```
dbt project
    │  dbt --write-index <run | build | compile>
    ▼
<target>/index/*.parquet          ← the data source (not manifest.json)
    │  loaded at boot into an in-memory DuckDB (via ADBC), one view per table
    ▼
axum HTTP handlers  (Arrow → JSON)
    ▼
REST API  /api/v1/*
    ▼
React SPA  (embedded in the binary, hash routing)
```

The SPA is baked into the binary at compile time (the `embed-ui` feature, on by default), so a single self-contained executable serves both the API and the UI.

## ✨ Features

- **Full project catalog** — models, sources, seeds, snapshots, tests, unit tests, exposures, groups, macros, metrics, semantic models, and saved queries.
- **Interactive lineage** — node-to-node DAG lineage.
- **Execution info** — last-run status, completion time, and errors surfaced inline on resource detail pages (when run results are present in the artifacts).
- **Single self-contained binary** — API + embedded SPA, no separate static-asset hosting.
- **No live warehouse dependency** — everything is served from the parquet snapshot.

## 🪜 Tiers

The richness of the docs depends on how the artifacts were produced.

| Tier | How | What you get |
|---|---|---|
| **dbt Core** | `dbt --write-index` without a Fusion login | Core catalog: nodes, project info, node-to-node lineage, test coverage |
| **Fusion** | Signed in to Fusion | Richer artifacts: column-level lineage, inferred types, sample data |

## 🚀 Getting started

### 📋 Prerequisites

Generate the artifacts first. From your dbt project, run dbt with `--write-index`:

```bash
dbt --write-index compile      # or: run / build
```

This writes the parquet to `./target/index/`.

### 💻 Option A: command line

If you already have a dbt binary installed (core v2 or fusion), just run:

```bash
dbt docs serve                 # binds 127.0.0.1:8580, opens a browser tab
```

Useful flags:

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--target-path <DIR>` | `DBT_DOCS_TARGET_PATH` | `./target` | Directory whose `index/` holds the parquet |
| `--host <HOST>` | `DBT_DOCS_HOST` | `127.0.0.1` | Bind address |
| `--port <PORT>` | `DBT_DOCS_PORT` | `8580` | Listen port |
| `--no-open` | — | off | Don't auto-open a browser tab |

### 📦 Option B: docker compose

A `docker-compose.yml` is included that wires the build, the artifact mount, the port, and a persistent driver-cache volume. Point `DBT_TARGET_PATH` at your project's `target/` directory (an absolute path is recommended) and bring it up:

```bash
DBT_TARGET_PATH=/abs/path/to/project/target docker compose up --build
```

Then open <http://localhost:8580>.

### 🐋 Option C: Docker

A `Dockerfile` is included. It installs the released dbt binary (it does not build from source), so the build is quick and needs no cargo toolchain:

```bash
docker build -f crates/dbt-docs-server/Dockerfile -t dbt-docs-server .
```

Run it, mounting a project `target/` that already contains `target/index/*.parquet`:

```bash
docker run --rm -p 8580:8580 \
  -v "$PWD/target:/data/target:ro" \
  dbt-docs-server
```

Then open <http://localhost:8580>.

**First-run network note.** The parquet is queried through an ADBC DuckDB driver that is **not** bundled in the image. On first boot the driver is downloaded from `public.cdn.getdbt.com` into the cache dir (`/var/cache/dbt`). The container therefore needs outbound HTTPS the first time it runs. 

To avoid re-downloading on every run — and to run fully offline once warmed — persist the cache with a named volume:

```bash
docker run --rm -p 8580:8580 \
  -v "$PWD/target:/data/target:ro" \
  -v dbt-adbc-cache:/var/cache/dbt \
  dbt-docs-server
```

### 🔨 Option D: build from source

> **Coming soon.**

## 🤝 Contributing

Development happens in the [`dbt-labs/dbt-core`](https://github.com/dbt-labs/dbt-core) monorepo, under `crates/dbt-docs-server`.

## 📄 License

Licensed under the Apache License, Version 2.0.
