<div style="text-align: center;">
  <img src="assets/dbt-fusion-engine.png" alt="dbt Fusion Engine" width="400" style="border-radius: 6px;" />
</div>

---

# dbt F✦SION engine (BETA)

This repo hosts components of the dbt Fusion engine, the foundation for future innovation in `dbt`. The dbt Fusion engine is written in Rust and is designed for speed, correctness, and has a native understanding of SQL across multiple data warehouse SQL dialects.

> [!IMPORTANT]  
> **Note: the dbt Fusion Engine is in Beta!**
Bugs and missing functionality compared to dbt Core will be resolved continuously in the leadup to a final release (see [this post](https://docs.getdbt.com/blog/dbt-fusion-engine-path-to-ga) for more details). 

The dbt Fusion engine is a ground up, first principles rewrite of the dbt Core execution engine, built to be interoperable with the standard dbt authoring layer. Fusion enforces some ambiguous areas of the authoring spec more strictly than dbt Core to ensure correctness (for example, dbt Core does not proactively validate most YAML configurations). Many of these discrepancies can be fixed automatically with the [dbt Autofix](https://github.com/dbt-labs/dbt-autofix) tool.

Beyond conformance with dbt Core, Fusion also contains new SQL Comprehension capabilities, a language server, modern ADBC drivers for warehouse connections, and more. While dbt Core was written in Python, the dbt Fusion engine is written in Rust, and compiled to a single application binary.

You can install dbt-fusion onto your local machine, a docker container, or a machine in the cloud. It is designed for flexible installation, with no dependencies on other libraries. The only libraries that dbt Fusion will load are it's corresponding database drivers.

The dbt Fusion engine is being released to this repository incrementally, so, until this note is removed this repository contains only a subset of the crates that make the core of the engine work. These crates are published incrementally starting on May 28.

## Getting Started with the dbt Fusion engine

> [!TIP]  
> You don't have to build this project from source to use the new dbt! We recommend using the precompiled binary with additional capabilities: 

There are several ways to get started with Fusion (for more, see dbt Fusion's quickstart documentation [here](https://docs.getdbt.com/guides/fusion?step=1))
1. **Download dbt the vs-code extension** - For most people the best experience. This will install the dbt fusion CLI and Language Server on your system - see the install guide [here](https://docs.getdbt.com/docs/install-dbt-extension).
2. **Install Fusion Directly** Install just the fusion CLI with the command below or see dbt's documentation [here](https://docs.getdbt.com/docs/fusion/install-fusion)
``` bash
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
```
3. **Build Fusion from Source** - See the *Compiling from Source* section below


### Supported Operating Systems and CPU Microarchitectures
Fusion & associated drivers are compiled for each CPU microarchitecture and operating system independently. This allows for hardware level optimization.

Legend:
* 🟢 - Supported today
* 🟡 - Unsupported today

| Operating System    | X86-64 | ARM  |
|-------------------|----------|------|
| MacOS             |   🟢     |  🟢  |
| Linux             |   🟢     |  🟢  |
| Windows           |   🟡     |  🟡  |


## Timeline


| Target Date | Milestone                   | Description                                  |
|-------------|-----------------------------|----------------------------------------------|
| 2025-05-28  | Initial release of Fusion   | Published source code of parser, schemas, dbt-jinja, and Snowflake ADBC driver. |
| 2025-06-09  | Databricks Adapter release  | Databricks ADBC driver, and adapter for Fusion |
| 2025-06-30  | BigQuery Adapter release    | BigQuery ADBC driver, and adapter for Fusion |
| 2025-07-31  | Redshift Adapter release    | Redshift ADBC driver, and adapter for Fusion |
| 2025-08-30  | ANTLR Grammars release + SQL Parser  | The SQL grammar used by the ANTLR parser generator.  |

### Top Level Components Released to Date
Releases of various Fusion components will be iterative as each component reaches maturity & readiness for contribution.

- [x] `dbt-jinja` - All Rust extension of mini-jinja to support dbt's jinja functions & other capabilities
- [x] `dbt-parser` - Rust parser for dbt projects
- [x] `dbt-snowflake` - database driver
- [x] `dbt-schemas` - complete, correct, machine generated json schemas for dbt's authoring surface
- [ ] `dbt-sql` - ANTLR grammars and generated parsers
  - [ ] snowflake.g4 
  - [ ] bigquery.g4
  - [ ] redshift.g4
  - [ ] databricks.g4
- [ ] Fusion: the comprehensive dbt fusion engine release.

## FAQ

<details>
  <summary><i>Can I con contribute to the dbt Fusion engine?</i></summary>

  Yes absolutely!. Please see our contribution guidelines [here](CONTRIBUTING.md)
</details>

<details>
  <summary><i>How is dbt Fusion different from dbt Core?</i></summary>
  The dbt Fusion engine is a ground-up rewrite of dbt Core, with many additional capabilities.
  *Things that are the same:*
  * The YML authoring format including profiles, configuration, seeds, data tests, and unit tests
  * The materialization libraries
  * dbt's library managemenet system (although `dbt deps` are installed automatically)

  *Additional capabilities provided by Fusion:*
  * All new Arrow Database Connector (ADBC) drivers for faster data transfers and unified connection handling
  * A language server and corresponding VS-Code extension (compatible with Cursor) for ease of development
  * Multi-dialect SQL compilation, validation, & static analysis
  * Standalone distribution. No JVM, or python required. 
  * Automatic installation of dependencies, whether that's a dbt package, or database driver
  * dbt code-signed & secure distributions
</details>

<details>
  <summary><i>This repo doesn't have all of dbt's functionality, when will the rest come?</i></summary>
  dbt Fusion's source code is being published as components are finalized. Please see the Timeline section above
</details>

<details>
  <summary><i>Can I use dbt Fusion today?</i></summary>

  | State	       | Description | Workaround	| Resolvable by |
  |--------------|-------------|------------|---------------|
  | Unblocked    |  You can adopt the dbt Fusion engine with no changes to your project   | --- | --- |
  | Soft blocked | Your project contains functionality [deprecated in dbt Core v1.10](https://www.getdbt.com/blog/how-to-get-ready-for-the-new-dbt-engine). | Resolve deprecations with the dbt-autofix script or workflow in dbt Studio| Users |
  | Hard blocked | Your project contains Python models or uses a not-yet-supported adapter | Remove unsupported functionality if possible | dbt Labs |
</details>



## Compiling from Source

The primary CLI in this repository is the `dbt-sa-cli`. To compile the CLI, you need the Rust toolchain. 

Let's start with Rust, run the following command to install Rust on your machine:

Linux:

```shell
sudo ./scripts/setup_dev_env_linux.sh
```

Mac:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Great! We have Rust installed. To confirm, run the following command:

```shell
cargo -v
```

You should see a printout like:
```
Rust's package manager

Usage: cargo [+toolchain] [OPTIONS] [COMMAND]
       cargo [+toolchain] [OPTIONS] -Zscript <MANIFEST_RS> [ARGS]...

Options:
  -V, --version                  Print version info and exit
...
```

Cargo is Rust's build system and package manager. If you're familiar with Python, pip would be a sufficient comparison. We'll use cargo to run command to build the local `dbt-sa-cli` binary and run helper scripts via `cargo xtask`. More on that later.

To build the binary locally, `cd` to the this repo's directory and run:

```shell
cargo build
```

This will compile our Rust code into the `dbt-sa-cli` binary. After this completes, you should see a new executable in `target/debug/dbt-sa-cli`. You can run this executable by passing the path directly into the CLI, so if you're in the root of this git repo, you can run:

```shell
target/debug/dbt-sa-cli
```

If built correctly, you should see output like:
```shell
> ./target/debug/dbt
Usage: dbt <COMMAND>

Commands:
  parse    Parse models
  ...
```

You might be wondering why it was built into the `debug` directory - this is because our default profile is `debug` when running `cargo build`. Our `debug` profile compiles the code faster, but sacrifices optimizations to do so. Therefore, if you want to benchmark the parser, build with the flag `cargo build --release`. The compile will take longer, but the build will mimic the experience of the end user.

If you expect to use this executable often, we recommend creating an alias for it in your `~/.zshrc`. To do so, start by getting the absolute path to the executable with:

```shell
cd target/debug && pwd
```

## Running Tests

To run tests, increase the stack size and use nextest.

```
 RUST_MIN_STACK=8388608 cargo nextest run --no-fail-fast
```

# License
The dbt Fusion engine is a monorepo and contains more than one License. Most code is licensed under ELv2. For more, please see our [licenses](LICENSES.md) section.

# Acknowledgments
*To the dbt community:* dbt the tool & dbt Labs the company would not be here without the incredible community of authors, contributors, practitioners, and enthusiasts. dbt Fusion is an evolution of that work & stands on the shoulders of what has come before. 

*To the Arrow Community:* dbt Labs is committing fully to the Arrow ecosystem. Fusion exclusively uses the Arrow type system from drivers through adapters into the internals of the compiler & runtime.

*To the DataFusion Community:* The intermediate representation of the SQL compiler is the DataFusion logical plan which has proven to be pragmatic, extensible, and easy to work with in all the right ways.

Thank you all. dbt, Arrow, and DataFusion have become truly global software projects. dbt Labs is committed to contributing meaningfully to these efforts over the coming months and years.
