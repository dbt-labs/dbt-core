use adbc_core::error::Result;
use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The backend driver to use
    #[arg(short, long)]
    backend: String,

    /// The driver version to use
    #[arg(short, long, default_value = "110")]
    adbc_version: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    dbt_xdbc::repl::run_repl(&args.backend).await
}
