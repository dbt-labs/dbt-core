# Protocol Buffers Definitions

## Synchronizing Protocol Buffers Definitions

Clone the [dbt-labs/proto](https://github.com/dbt-labs/proto) repository as
sibling of this repository and run the `./scripts/sync_protos.sh` script to
synchronize the Protocol Buffers definitions with the latest version.

## Generating Rust Code

Check `protogen.toml` to make sure the `.proto` files you want to generate
are listed. Then run the following command to generate the Rust code from them:

```shell
cargo xtask protogen
```

The explicit lists in `protogen.toml` are used to ensure that we only generate
code for the `.proto` files that we actually use in this repository. This
prevents unnecessary code generation and very slow Rust builds.

Include the files with `git`. We check-in the generated code to avoid having to
regenerate it on every build. `.gitattributes` declares these files are
generated so Github will collapse them in the UI.

## Manually tweaking lib.rs

We don't rely on auto-generated `_includes.rs` or `mod.rs` files. Instead we
manually maintain `lib.rs` files to have more control over the module structure
and visibility of the generated code. After running the code generation, you may
need to manually tweak the `lib.rs` files in the `proto-rust` and
`proto-rust-private` crates.

This let's us, for example, re-export certain types from the public crate so
they can be used by the private crate.
