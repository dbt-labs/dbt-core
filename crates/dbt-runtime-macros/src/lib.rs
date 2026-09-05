#![allow(unused_qualifications)]

use proc_macro::TokenStream;

mod entry;

/// Marks async function to be executed by the selected tokio runtime. This macro
/// helps set up a `Runtime` without requiring the user to use `Runtime` or
/// `Builder` directly.
///
/// In addition to setting up the tokio runtime, this macro also creates a
/// `dbt_runtime::Runtime` and propagates its handle via thread-local storage
/// so that `dbt_runtime::Handle::current()` is available throughout the
/// program.
///
/// Note: This macro is designed to be simplistic and targets applications that
/// do not require a complex setup. If the provided functionality is not
/// sufficient, you may be interested in using the builders for the tokio
/// and dbt-runtime contexts.
///
/// # Multi-threaded runtime (default)
///
/// ```ignore
/// #[dbt_runtime::main]
/// async fn main() {
///     println!("Hello world");
/// }
/// ```
///
/// ## Current-thread runtime
///
/// ```ignore
/// #[dbt_runtime::main(flavor = "current_thread")]
/// async fn main() {
///     println!("Hello world");
/// }
/// ```
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    entry::main(args.into(), item.into(), true).into()
}

/// Drop-in replacement for `#[tokio::test]` that also creates a
/// `dbt_runtime::Runtime` and propagates its handle into the tokio
/// runtime's current thread.
///
/// Marks async function to be executed by runtime, suitable to test environment.
/// This macro helps set up a tokio Runtime and dbt-runtime without requiring the
/// user to use `Runtime` or `Builder` directly.
///
/// ```ignore
/// #[dbt_runtime::test]
/// async fn my_test() {
///     // dbt_runtime::Handle::current() works here so it's safe
///     // to run code that calls dbt_runtim::spawn_blocking().
/// }
/// ```
///
/// Note: This macro is designed to be simplistic and targets applications that
/// do not require a complex setup. If the provided functionality is not
/// sufficient, you may be interested in using the builders for the tokio
/// and dbt-runtime contexts.
///
/// The default tokio flavor is `current_thread` (same as `#[tokio::test]`).
/// To use the multi-threaded tokio runtime:
///
/// ```ignore
/// #[dbt_runtime::test(flavor = "multi_thread")]
/// async fn my_test() { }
/// ```
///
/// The `worker_threads` option configures the number of tokio worker threads,
/// and defaults to the number of cpus on the system.
///
/// The default tokio test runtime is single-threaded. Each test gets a
/// separate current-thread runtime.
///
/// The dbt-runtime is always multi-threaded but it's threads are only
/// created when needed (i.e. when blocking tasks are spawned on it),
/// and there is no way to configure the maximum number of dbt-runtime
/// threads (a default value is used).
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    entry::test(args.into(), item.into(), true).into()
}
