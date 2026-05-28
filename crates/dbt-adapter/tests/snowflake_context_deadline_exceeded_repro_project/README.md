# Snowflake `context deadline exceeded` repro

Reproduce gosnowflake's login error end-to-end (no real Snowflake needed):

```text
context deadline exceeded (Client.Timeout exceeded while awaiting headers)
```

## Why this error happens (Go)

In gosnowflake, each HTTP call uses `http.Client` with a timeout. When the request (connect, wait for headers, read body) exceeds that limit, Go's `context.Context` is cancelled and you get **`context deadline exceeded`**. It is a timeout signal: stop waiting and fail the request so the client does not hang forever.

Here the black-hole server never sends headers, so the client waits until `AUTH_CLIENT_TIMEOUT` fires.

## Run

**Terminal 1** — black-hole on `127.0.0.1:9999` (must match `profiles.yml`):

```sh
python3 blackhole.py
```

**Terminal 2** — set `DBT_SNOWFLAKE_AUTH_CLIENT_TIMEOUT` so the repro fails fast (default is 900s):

```sh
export DBT_SNOWFLAKE_AUTH_CLIENT_TIMEOUT=1ms
fsd run --profiles-dir . --target snowflake
```

Expect a connection error containing `Client.Timeout exceeded while awaiting headers`.

## Rust unit test

`../snowflake_context_deadline_exceeded.rs` — same setup in-process; sets `AUTH_CLIENT_TIMEOUT=1ms` on the builder instead of the env var.
