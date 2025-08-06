# Protocol Buffers Definitions

Change or add the .proto file of your choice in the `include/dbtlabs/proto` directory
and run the following command to generate the Rust code:

```shell
cargo xtask protogen
```

Run `cargo fmt` to ensure the generated code has consistent formatting:

```shell
cargo fmt
```
