![logo](https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico)

[![crates.io](https://img.shields.io/crates/v/adbc_snowflake.svg)](https://crates.io/crates/adbc_snowflake)
[![docs.rs](https://docs.rs/adbc_snowflake/badge.svg)](https://docs.rs/c)

# Rust wrapper for ADBC and ODBC drivers

All [ADBC (Arrow Database Connectivity)](https://arrow.apache.org/adbc/) drivers
(shared libraries) are loaded dynamically.

Drivers are automatically downloaded from the dbt CDN when dbt needs to connect to a
data warehouse.

[ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity) drivers are
also loaded dynamically, but should be installed on the system.

Run this to download and install the ODBC drivers (for Redshift and Databricks).

```
./scripts/install_odbc_drivers.sh
```

To compile with ODBC support on non-Windows platforms, you need to install the
[unixODBC](http://www.unixodbc.org/) library.

```
sudo apt-get install unixodbc-dev  # Ubuntu
brew install unixodbc  # macOS
```

Enable the `"odbc"` feature to compile with ODBC support (enabled by default
only on Windows).

```
cargo build --bin dbt --features odbc
```

## Snowflake example

We use the
[ADBC Snowflake Go driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/snowflake)
for [Snowflake](https://www.snowflake.com).

```rust,no_run
use adbc_core::options::AdbcVersion;
use adbc_core::{Connection, Statement};
use dbt_xdbc::{connection, database, driver, Backend};
use arrow_array::{cast::AsArray, types::Decimal128Type};

# fn main() -> Result<(), Box<dyn std::error::Error>> {

// Load the driver
let mut driver = driver::Builder::new(Backend::Snowflake)
    .with_version(AdbcVersion::V110)
    .build()?;

// Construct a database using system configuration
let mut database = database::Builder::from_snowsql_config()?.build(&mut driver)?;
// ..or from a URI.
let mut builder = database::Builder::new(Backend::Snowflake);
builder.with_parse_uri("my_account/my_db/my_schema?role=R&warehouse=WH")?;
let mut database = builder.build(&mut driver)?;

// Create a connection to the database
let mut connection = connection::Builder::default().build(&mut database)?;

// Construct a statement to execute a query
let mut statement = connection.new_statement()?;

// Execute a query
statement.set_sql_query("SELECT 21 + 21")?;
let mut reader = statement.execute()?;

// Check the result
let batch = reader.next().expect("a record batch")?;
assert_eq!(
    batch.column(0).as_primitive::<Decimal128Type>().value(0),
    42
);

# Ok(()) }
```

## Bumping an ADBC driver version

See an example PR at [dbt-labs/fs#2166](https://github.com/dbt-labs/fs/pull/2166).
To get the checksums into the source code, re-generate with
`./scripts/gen_cdn_driver_checksums.sh`.

If the checksums for the existing drivers change, something is very broken. You
should be able to run the script and get the same checksums as before plus the
new ones based on the new driver version list in the shell script.

## Working on ADBC drivers

Most ADBC drivers we use are written in Go, using the official Go SDKs for each
data warehouse backend.

When you need to extend the funcionality of an ADBC diver, you can do so by
cloning our fork of `apache/arrow-adbc` and modifying the Go code of the driver
you want to work on.

```bash
git clone git@github.com:dbt-labs/arrow-adbc.git
cd arrow-adbc
cd go/adbc/driver/bigquery  # source directory
cd go/adbc/pkg              # build directory for all Go drivers
make clean || make libadbc_driver_bigquery.dylib
```

Use `make libadbc_driver_bigquery.so` to build the Linux driver, or
`make libadbc_driver_bigquery.dll` to build the Windows driver.

You can replace the `bigquery` driver with `snowflake` if you're building the
Snowflake driver instead.

To get `fs` to skip loading the production drivers from the CDN, you need to set
an environment variable that is checked by, and only by debug builds of `fs` and
link to the local driver from the `lib/` folder at the root of the `fs` repo.

```bash
export DISABLE_CDN_DRIVER_CACHE=
# assuming you keep all the repos in the ~/code folder:
ln -s ~/code/arrow-adbc/go/adbc/pkg/libadbc_driver_bigquery.dylib ~/code/fs/lib/libadbc_driver_bigquery.dylib
```

Change the path to the driver to match your local setup and the driver extension
that your system uses (.dylib, .so, .dll).

You should see a message like this when you run `fs` or tests that trigger the
loading of ADBC drivers:

```
$ cargo test -p dbt-xdbc -- --nocapture
...
WARNING: BigQuery ADBC driver is being loaded from /Users/felipe/code/fs/lib in debug mode.
...
```

When you're done with your changes, open a PR against [](https://github.com/dbt-labs/arrow-adbc),
after review and merge, trigger an `ADBC Release` workflow in the `fs`
repository and bump the driver version in `fs`.
