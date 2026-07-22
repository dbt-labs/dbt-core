# Self-hosted dbt docs server. Installs the released dbt (Fusion) binary — no
# source build, no cargo, no build context.
#
# Build:
#   docker build -f crates/dbt-docs-server/Dockerfile -t dbt-docs-server .
#
# Run (mount a target/ dir with target/index/*.parquet from
# `dbt --write-index <run|build|compile>`). The named volume caches the ADBC
# DuckDB driver, downloaded from public.cdn.getdbt.com on first boot (needs
# outbound HTTPS once; offline after):
#   docker run --rm -p 8580:8580 \
#     -v "$PWD/target:/data/target:ro" \
#     -v dbt-adbc-cache:/var/cache/dbt \
#     dbt-docs-server

FROM debian:bookworm-slim

# curl + ca-certificates (TLS fetches), tar (unpack release), libstdc++6 (the
# ADBC DuckDB driver .so is C++).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tar \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Install the released dbt binary and hoist onto PATH (`cp -L` dereferences the
# installer's symlink). `|| true`: the installer exits non-zero in a container
# (no shell rc to update) even on success — the real gate is `cp` + `dbt
# --version`, which fail the build if the binary is missing/broken.
#
# DBT_VERSION is pinned for reproducible builds and may drift; override with
# `--build-arg DBT_VERSION=...`. Releases: https://public.cdn.getdbt.com/fs/versions.json
ARG DBT_VERSION=2.0.0-preview.196
ENV HOME=/root
ENV SHELL=/bin/sh
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --version "$DBT_VERSION" || true; \
    cp -L "$HOME/.local/bin/dbt" /usr/local/bin/dbt \
    && dbt --version

# Bind all interfaces; map the port at `docker run`.
ENV DBT_DOCS_HOST=0.0.0.0
ENV DBT_DOCS_PORT=8580
ENV DBT_DOCS_TARGET_PATH=/data/target

# Pin the ADBC driver cache to a stable, writable, mountable path.
ENV XDG_CACHE_HOME=/var/cache/dbt
RUN mkdir -p /var/cache/dbt

EXPOSE 8580

ENTRYPOINT ["dbt", "docs", "serve", "--no-open"]
