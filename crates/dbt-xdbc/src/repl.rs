use std::{env, sync::Arc};

use adbc_core::{
    error::{Error, Result},
    options::AdbcVersion,
};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use dbt_common::pretty_table::{pretty_data_table, DisplayFormat};
use dialoguer::{theme::ColorfulTheme, BasicHistory, Input};

use crate::{
    bigquery, connection,
    database::{self, LogLevel},
    driver, snowflake, Backend, Connection, Database, Driver, QueryCtx,
};

pub struct ReplState {
    _driver: Box<dyn Driver>,
    _database: Box<dyn Database>,
    connection: Box<dyn Connection>,
    // TODO(jasonlin45): figure out the lifetime restriction here so we can directly store RecordBatchReader
    current_schema: Option<SchemaRef>,
    current_batches: Vec<RecordBatch>,
    current_batch_idx: usize,
}

impl ReplState {
    pub fn new(backend: Backend, version: AdbcVersion) -> Result<Self> {
        let mut driver = driver::Builder::new(backend)
            .with_adbc_version(version)
            .try_load()?;

        let mut database = Self::database_builder_for(backend)?.build(&mut driver)?;

        let connection = connection::Builder::default().build(&mut database)?;

        Ok(Self {
            _driver: driver,
            _database: database,
            connection,
            current_schema: None,
            current_batches: Vec::new(),
            current_batch_idx: 0,
        })
    }

    // TODO(jasonlin45): wet code - extract default initialization to be shared with the tests
    // TODO(jasonlin45): allow customization of adbc options via command
    fn database_builder_for(backend: Backend) -> Result<database::Builder> {
        let mut database_builder = match backend {
            Backend::Snowflake => database::Builder::from_snowsql_config(),
            Backend::BigQuery => {
                let mut builder = database::Builder::new(backend);
                let project_id = env::var("ADBC_BIGQUERY_PROJECT").unwrap_or_default();
                let dataset_id = env::var("ADBC_BIGQUERY_DATASET").unwrap_or_default();
                let auth_credentials =
                    env::var("ADBC_BIGQUERY_CREDENTIAL_FILE").unwrap_or_default();

                builder
                    .with_named_option(
                        bigquery::AUTH_TYPE,
                        bigquery::auth_type::JSON_CREDENTIAL_FILE,
                    )?
                    .with_named_option(bigquery::PROJECT_ID, project_id)?
                    .with_named_option(bigquery::DATASET_ID, dataset_id)?
                    .with_named_option(bigquery::AUTH_CREDENTIALS, auth_credentials)?;
                Ok(builder)
            }
            Backend::Postgres => {
                // Configuration for Postgres:
                //     CREATE ROLE username WITH LOGIN PASSWORD 'an_secure_password';
                //     CREATE DATABASE adbc_test;
                //     GRANT CONNECT ON DATABASE adbc_test TO username;
                //     GRANT ALL PRIVILEGES ON DATABASE adbc_test TO username;
                // Shell:
                //     export ADBC_POSTGRES_URI="postgres://username:an_secure_password@localhost/adbc_test"
                let uri = env::var("ADBC_POSTGRES_URI")
                    .unwrap_or("postgres://username:rocks_password@localhost/adbc_test".to_owned());
                let mut builder = database::Builder::new(backend);
                builder.with_parse_uri(uri)?;
                Ok(builder)
            }
            Backend::Databricks => {
                const HOST: &str = "adbc.databricks.host";
                const CATALOG: &str = "adbc.databricks.catalog";
                const SCHEMA: &str = "adbc.databricks.schema";
                const WAREHOUSE: &str = "adbc.databricks.warehouse";
                const TOKEN: &str = "adbc.databricks.token";

                let host = env::var("DATABRICKS_HOST").unwrap();
                let warehouse = env::var("DATABRICKS_WAREHOUSE").unwrap();
                let token = env::var("DATABRICKS_TOKEN").unwrap();

                let mut builder = database::Builder::new(backend);
                // optional
                if let Ok(catalog) = env::var("DATABRICKS_CATALOG") {
                    builder.with_named_option(CATALOG, catalog)?;
                }
                if let Ok(schema) = env::var("DATABRICKS_SCHEMA") {
                    builder.with_named_option(SCHEMA, schema)?;
                }

                builder
                    .with_named_option(HOST, host)?
                    .with_named_option(WAREHOUSE, warehouse)?
                    .with_named_option(TOKEN, token)?;
                Ok(builder)
            }
            Backend::Generic { .. } => unimplemented!(),
            _ => unimplemented!(),
        }?;
        if backend == Backend::Snowflake {
            database_builder
                .with_named_option(snowflake::LOG_TRACING, LogLevel::Warn.to_string())?;
        }
        Ok(database_builder)
    }

    pub fn execute_query(&mut self, query: &str) -> Result<(usize, usize)> {
        if query.trim().is_empty() {
            return Ok((0, 0));
        }

        let conn = self.connection.as_mut();
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query(&QueryCtx::new("repl").with_sql(query))?;
        let reader = stmt.execute()?;

        let num_cols = reader.schema().fields().len();
        self.current_schema = Some(reader.schema());

        // grab all the batches
        self.current_batches = reader
            .map(|r| {
                r.map_err(|e| {
                    Error::with_message_and_status(e.to_string(), adbc_core::error::Status::IO)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let num_batches = self.current_batches.len();
        self.current_batch_idx = 0;

        Ok((num_batches, num_cols))
    }

    pub fn show_schema(&self) -> Result<Option<SchemaRef>> {
        Ok(self.current_schema.clone())
    }

    pub fn show_batch(&self) -> Result<Option<RecordBatch>> {
        if self.current_batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.current_batches[self.current_batch_idx].clone()))
        }
    }

    pub fn move_pointer(&mut self, delta: isize) -> Result<()> {
        let new_idx = if delta < 0 {
            self.current_batch_idx.checked_sub(delta.unsigned_abs())
        } else {
            self.current_batch_idx.checked_add(delta as usize)
        };

        if let Some(idx) = new_idx {
            if idx >= self.current_batches.len() {
                Err(Error::with_message_and_status(
                    format!("Out of range {}", idx),
                    adbc_core::error::Status::InvalidArguments,
                ))
            } else {
                self.current_batch_idx = idx;
                Ok(())
            }
        } else {
            Err(Error::with_message_and_status(
                "Index overflow".to_string(),
                adbc_core::error::Status::InvalidArguments,
            ))
        }
    }
}

enum Command {
    Query { query: String },
    // move current pointer in batch by some amount
    Move { delta: isize },
    ReloadDriver,
    ShowSchema,
    ShowBatch,
    Help,
    Quit,
    Invalid,
}

fn parse_command(line: &str) -> Option<Command> {
    let line = if let Some(rest) = line.strip_prefix(':') {
        rest.trim()
    } else {
        return Some(Command::Query {
            query: line.to_string(),
        });
    };

    match line {
        "exit" | "quit" => Some(Command::Quit),
        "help" => Some(Command::Help),
        "reload" => Some(Command::ReloadDriver),
        "show-schema" => Some(Command::ShowSchema),
        "show-batch" => Some(Command::ShowBatch),
        "prev" => Some(Command::Move { delta: -1 }),
        "next" => Some(Command::Move { delta: 1 }),
        "move" => {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 2 {
                Some(Command::Invalid)
            } else if let Ok(delta) = parts[1].parse::<isize>() {
                Some(Command::Move { delta })
            } else {
                Some(Command::Invalid)
            }
        }
        _ => Some(Command::Invalid),
    }
}

// Prints a visualization of a schema to stdout
fn visualize_schema(schema: Arc<Schema>) {
    println!("Schema");
    println!("├─ Fields");

    for field in schema.fields.iter() {
        println!("│  ├─ {}", field.name());
        println!("│  │  ├─ Type: {:?}", field.data_type());
        println!("│  │  ├─ Nullable: {}", field.is_nullable());

        if !field.metadata().is_empty() {
            println!("│  │  └─ Metadata");
            let entries: Vec<_> = field.metadata().iter().collect();
            for (i, (key, value)) in entries.iter().enumerate() {
                let prefix = if i == entries.len() - 1 {
                    "└─"
                } else {
                    "├─"
                };
                println!("│  │     {} {}: {}", prefix, key, value);
            }
        }
    }

    // todo: break this out to a function with indent levels
    if !&schema.metadata.is_empty() {
        println!("└─ Metadata");
        for (key, value) in &schema.metadata {
            println!("   └─ {}: {}", key, value);
        }
    }
}

pub async fn run_repl(backend_str: &str) -> Result<()> {
    let backend = match backend_str.to_lowercase().as_str() {
        "snowflake" => Backend::Snowflake,
        "bigquery" => Backend::BigQuery,
        "postgres" => Backend::Postgres,
        "databricks" => Backend::Databricks,
        "redshift" => Backend::RedshiftODBC,
        _ => {
            return Err(Error::with_message_and_status(
                format!("Unsupported backend: {}", backend_str),
                adbc_core::error::Status::InvalidArguments,
            ))
        }
    };

    let mut history = BasicHistory::new().max_entries(8).no_duplicates(true);
    let mut state = ReplState::new(backend, AdbcVersion::V110)?;
    let theme = ColorfulTheme::default();

    println!("Welcome to dbt-xdbc REPL!");
    println!("Type :help for available commands");
    println!("Type :quit to exit");

    loop {
        let input: String = Input::with_theme(&theme)
            .with_prompt(format!("dbt-xdbc | {}>", backend_str))
            .history_with(&mut history)
            .interact_text()
            .map_err(|e| {
                Error::with_message_and_status(e.to_string(), adbc_core::error::Status::IO)
            })?;

        match parse_command(&input) {
            Some(Command::Query { query }) => {
                println!("Executing query...");
                match state.execute_query(&query) {
                    Ok((batches, cols)) => {
                        println!("Successfully executed query.");
                        println!("{} batches with {} columns returned.", batches, cols);
                        println!("  :show-schema    - Show schema");
                        println!("  :show-batch     - Show current batch");
                    }
                    Err(e) => {
                        eprintln!("Error executing query: {}", e);
                        continue;
                    }
                }
            }
            Some(Command::Move { delta }) => {
                if let Err(e) = state.move_pointer(delta) {
                    eprintln!("Error moving pointer: {}", e);
                    continue;
                }
            }
            Some(Command::Help) => {
                println!("Available commands:");
                println!("  :help           - Show this help message");
                println!("  <query>         - Execute SQL query");
                println!("  :show-schema    - Show current schema");
                println!("  :show-batch     - Show current batch");
                println!("  :move <int>     - Move current batch pointer. Negative values move backwards, positive values move forwards.");
                println!("  :prev           - Move to previous batch");
                println!("  :next           - Advance to next batch");
                println!("  :reload         - Reload the xdbc driver");
                println!("  :quit           - Exit the REPL");
            }
            Some(Command::ShowSchema) => {
                if let Some(schema) = state.show_schema()? {
                    visualize_schema(schema);
                } else {
                    println!("No schema found");
                }
            }
            Some(Command::ShowBatch) => {
                if let Ok(Some(batch)) = state.show_batch() {
                    // Get column names from the schema
                    let column_names: Vec<String> = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().to_string())
                        .collect();

                    // Format and display the table
                    if let Ok(table) = pretty_data_table(
                        "Query Results",
                        "",
                        &column_names,
                        &[batch.clone()],
                        &DisplayFormat::Table,
                        Some(10),
                        true,
                        Some(batch.num_rows()),
                    ) {
                        println!("{}", table);
                    } else {
                        eprintln!("Failed to pretty print as table.");
                        // fallback: dump as a debug print
                        println!("{:#?}", batch);
                    }
                } else {
                    println!("No batch found!");
                }
            }
            Some(Command::ReloadDriver) => {
                // TODO(jasonlin45) the actual binary ends up cached in driver.rs
                println!("Reloading driver...");
                state = ReplState::new(backend, AdbcVersion::V110)?;
                println!("Driver reloaded successfully");
            }
            Some(Command::Quit) => break,
            Some(Command::Invalid) => {
                eprintln!("Invalid command. Type :help for available commands");
            }
            None => {}
        }
    }

    Ok(())
}
