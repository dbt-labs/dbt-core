# dbt-ci

Release-pipeline commands for dbt-fusion. Exposed as `cargo ci` via the
workspace `.cargo/config.toml` alias.

## Subcommands

- `cargo ci bump-cargo-version <version> [--no-lockfile]` — writes `version` into `[workspace.package].version` and refreshes `Cargo.lock`.
- `cargo ci pypi pack --binaries-dir DIR --version X.Y.Z [--out DIR] [--bin-name NAME]` — writes one `py3-none-{platform}` wheel per pre-built binary in `--binaries-dir` (files named by cargo target triple, `.exe` for Windows).
- `cargo ci pypi publish --environment {staging|prod|test-pypi} [--version V] [--dist DIR]` — publishes wheels in `--dist` (default `target/wheels`) matching the workspace pyproject's `[project].name`. `--version` filters to that PEP 440 version.

## Version shapes

| SemVer            | PEP 440 |
|-------------------|---------|
| `X.Y.Z`           | `X.Y.Z` |
| `X.Y.Z-alpha.N`   | `X.Y.ZaN` |
| `X.Y.Z-beta.N`    | `X.Y.ZbN` |
| `X.Y.Z-rc.N`      | `X.Y.ZrcN` |
| `X.Y.Z-preview.N` | `X.Y.ZrcN` |
| `X.Y.Z-dev.N`     | `X.Y.Z.devN` |

## Env vars

- `staging`: `DBT_PYPI_STAGING_DOMAIN`, `DBT_PYPI_STAGING_DOMAIN_OWNER`, `DBT_PYPI_STAGING_REGION`, `DBT_PYPI_STAGING_REPOSITORY`, `DBT_PYPI_STAGING_PROFILE` (named AWS profile used to assume into the CodeArtifact-owning account — must be configured in `~/.aws/config` with the right cross-account role and a valid SSO/credential session)
- `test-pypi`: `DBT_PYPI_TEST_TOKEN`
- `prod`: `DBT_PYPI_PROD_TOKEN`

## Typical CI sequence

1. Bump the workspace version:

   ```bash
   cargo ci bump-cargo-version X.Y.Z
   ```

2. For each target triple, build the binary and stage it into `binaries/`,
   named by its triple (`.exe` on Windows — e.g.
   `binaries/x86_64-unknown-linux-gnu`,
   `binaries/x86_64-pc-windows-msvc.exe`):

   ```bash
   cargo build --release --bin <bin> --target <triple>
   ```

3. Pack one wheel per binary, then publish:

   ```bash
   cargo ci pypi pack --binaries-dir binaries --version X.Y.Z
   cargo ci pypi publish --environment staging --version X.Y.Z
   cargo ci pypi publish --environment prod --version X.Y.Z
   ```
