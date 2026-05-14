# dbt-docs-web

The React + Vite + TS SPA embedded into the `dbt` binary by
`dbt-docs-server` via `rust-embed`.

## Build

```sh
cd fs/sa/crates/dbt-docs-server/web
npm install   # requires GITHUB_TOKEN (see "Authenticated dependencies")
npm run build
```

The Rust crate's `build.rs` then picks up `web/dist/` automatically the next
time you run `cargo build -p dbt-cli`.

## Without `npm run build`

If `web/dist/index.html` doesn't exist when the Rust crate compiles,
`build.rs` falls back to the checked-in `web/placeholder/` directory — a
tiny page that explains how to build the real SPA. The HTTP API is fully
functional in either mode; only the UI changes.

This means:
- A fresh checkout produces a working binary (`cargo build -p dbt-cli`)
  even without Node.js or `GITHUB_TOKEN`. Useful for Rust-only iteration
  and external contributors.
- CI must run `npm install && npm run build` before `cargo build` to ship
  the real SPA in release artifacts. The build emits a `cargo:warning` if
  it falls back to the placeholder so CI failures of that kind are loud.
- `web/dist/` is gitignored — build output never lands in commits.

## Authenticated dependencies

This app depends on `@dbt-labs/sourdough` (the dbt design system), which
is hosted on the **dbt-labs GitHub Package Registry**, not the public npm
registry. To install:

1. Generate a GitHub PAT (classic) with `read:packages` scope.
2. Export it as `GITHUB_TOKEN` in your shell.
3. Run `npm install` from this directory.

`.npmrc` here routes the `@dbt-labs` scope to `npm.pkg.github.com` and
reads the token from the env var. CI must provide a token with the same
scope.

## Layout

```
web/
├── src/                    # React source
├── placeholder/index.html  # tracked; embedded if dist/ is missing
├── dist/                   # gitignored; populated by `npm run build`
├── package.json
├── vite.config.ts
└── ...
```
