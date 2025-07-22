use clap::Error;
use clap::Parser;
use clap::error::ErrorKind;

use dbt_common::cancellation::CancellationTokenSource;
use dbt_common::{constants::PANIC, pretty_string::GREEN, pretty_string::RED};
use dbt_sa_lib::dbt_sa_clap::Cli;
use dbt_sa_lib::dbt_sa_clap::from_main;
use dbt_sa_lib::dbt_sa_lib::execute_fs;
use std::io::{self, Write};
use std::process::ExitCode;

const FS_DEFAULT_STACK_SIZE: usize = 8 * 1024 * 1024;

/// Maximum number of threads used for running blocking operations (based on the tokio runtime
/// default).
///
/// These threads are used mostly for blocking I/O operations, so they don't really
/// consume CPU resources. That's why we can afford and should have a lot of them.
const FS_DEFAULT_MAX_BLOCKING_THREADS: usize = 512;

fn main() -> ExitCode {
    let cst = CancellationTokenSource::new();
    // TODO(felipecrv): cancel the token (through the cst) on Ctrl-C
    let token = cst.token();

    let cli = match Cli::try_parse() {
        Ok(cli) => {
            // Continue as normal
            cli
        }
        Err(e) => {
            if e.kind() == ErrorKind::UnknownArgument {
                // todo make this for more than just unknown arguments
                // Only show the actual error message
                print_trimmed_error(e); // prints to stderr
                std::process::exit(1);
            } else {
                // For other errors, show full help as usual
                e.exit();
            }
        }
    };

    let arg = from_main(&cli);

    // Setup tokio runtime and set stack-size to 8MB
    // DO NOT USE Rayon, it is not compatible with Tokio

    let tokio_rt = match arg.num_threads {
        Some(1) => {
            // Simiulate single-threaded runtime
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(FS_DEFAULT_STACK_SIZE)
                .worker_threads(1)
                .max_blocking_threads(1)
                .build()
                .expect("failed to initialize 'single-threaded' tokio runtime")
        }
        // Uncomment this if you want to limit the number of threads in multi-threaded runtime
        // Some(num_threads) if num_threads > 1 => {
        //     // Multi-threaded runtime: limit to num_threads
        //     tokio::runtime::Builder::new_multi_thread()
        //         .enable_all()
        //         .worker_threads(num_threads)
        //         .max_blocking_threads(FS_DEFAULT_MAX_BLOCKING_THREADS)
        //         .thread_stack_size(FS_DEFAULT_STACK_SIZE)
        //         .build()
        //         .expect("failed to initialize multi-threaded tokio runtime")
        // }
        _ => {
            // Multi-threaded runtime: use default (max parallelism)
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .max_blocking_threads(FS_DEFAULT_MAX_BLOCKING_THREADS)
                .thread_stack_size(FS_DEFAULT_STACK_SIZE)
                .build()
                .expect("failed to initialize default multi-threaded tokio runtime")
        }
    };

    // If execution panics, exit with a status 2 (but not if RUST_BACKTRACE is
    // set to 1, in which case we want to see the backtrace):
    if std::env::var("RUST_BACKTRACE").unwrap_or_default() != "1" {
        std::panic::set_hook(Box::new(|info| {
            eprintln!("{} {}", RED.apply_to(PANIC), info);
            let _ = io::stdout().flush();
            let _ = io::stderr().flush();

            std::process::exit(2);
        }));
    }
    // Run
    let future = Box::pin(execute_fs(arg, cli, token));
    let result = tokio_rt.block_on(async { tokio_rt.spawn(future).await.unwrap() });
    // Remove the panic hook
    let _ = std::panic::take_hook();

    // Handle regular execution
    match result {
        Ok(code) => {
            // If exec succeeds, exit with status 0 or 1
            // for 1 it is assumed that the  error was already printed)
            assert!(code == 0 || code == 1);
            ExitCode::from(code as u8)
        }
        Err(_err) => {
            // If any step fails, assume error is already printed, just exit with a status 1
            // show_progress_exit!(arg, start);
            ExitCode::from(1)
        }
    }
}

fn print_trimmed_error(e: Error) {
    let mut stderr = io::stderr();

    let msg = e.to_string(); // includes both "error:" and possibly "tip:"
    let mut lines = msg.lines();
    let mut command = String::new();

    for line in lines.by_ref() {
        if let Some(rest) = line.strip_prefix("error:") {
            let _ = write!(stderr, "{}:", RED.apply_to("error"));
            let _ = writeln!(stderr, "{rest}");
        } else if let Some(rest) = line.trim_start().strip_prefix("tip:") {
            let prefix = if line.starts_with("tip:") { "" } else { "  " };
            let _ = write!(stderr, "{}{}", prefix, GREEN.apply_to("tip"));
            let _ = writeln!(stderr, ":{rest}");
        } else if line.trim().starts_with("Usage:") {
            //let command = drop "Usage:"; take everything until the first '<'; trim
            command = line.strip_prefix("Usage:").unwrap_or(line).to_string();
            command = command
                .split_once('<')
                .unwrap_or(("", ""))
                .0
                .trim()
                .to_string();
            break; // stop before dumping giant usage block
        } else {
            let _ = writeln!(stderr, "{line}");
        }
    }

    // Always print this footer
    let _ = writeln!(stderr, "\nFor more information, try '{command} --help'.");
}
