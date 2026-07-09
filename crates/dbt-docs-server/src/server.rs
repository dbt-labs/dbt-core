use std::io;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::{get, post};
use tracing::info;

use crate::DocsServeArgs;

use crate::assets::serve_assets;
use crate::handlers::{
    analytics, capabilities, column_lineage, distribution, exposures, files, groups, health,
    identity, lineage, macros, metrics, models, nodes, project, saved_queries, search, seeds,
    semantic_models, snapshots, sources, tests,
};
use crate::providers::Providers;
use crate::resolve_index_dir;
use crate::state::AppState;

/// Run the docs server. Must be called from within a tokio runtime —
/// `dbt-main` already initialises one before dispatching commands, so
/// this crate intentionally does not build its own. The caller is
/// responsible for constructing the [`Providers`]; the SA crate itself
/// never touches `dbt-index` or any other proprietary surface.
pub async fn run_with_args(args: Arc<DocsServeArgs>, providers: Providers) -> io::Result<()> {
    let index_dir = resolve_index_dir(&args);
    let state = Arc::new(AppState::new(
        index_dir,
        providers,
        args.has_dbt_state,
        args.send_anonymous_usage_stats,
    ));
    serve(args, state).await
}

async fn serve(args: Arc<DocsServeArgs>, state: Arc<AppState>) -> io::Result<()> {
    serve_with_shutdown(args, state, shutdown_signal()).await
}

async fn serve_with_shutdown<F>(
    args: Arc<DocsServeArgs>,
    state: Arc<AppState>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = Router::new()
        .route("/api/v1/health", get(health::get_health))
        .route("/api/v1/identity", get(identity::get_identity))
        .route("/api/v1/capabilities", get(capabilities::get_capabilities))
        .route("/api/v1/distribution", get(distribution::get_distribution))
        .route("/api/v1/project", get(project::get_project))
        .route("/api/v1/models", get(models::list_models))
        .route("/api/v1/models/facets", get(models::list_model_facets))
        .route("/api/v1/models/{unique_id}", get(models::get_model))
        .route("/api/v1/sources", get(sources::list_sources))
        .route("/api/v1/sources/facets", get(sources::list_source_facets))
        .route("/api/v1/sources/{unique_id}", get(sources::get_source))
        .route("/api/v1/groups", get(groups::list_groups))
        .route("/api/v1/groups/facets", get(groups::list_group_facets))
        .route("/api/v1/groups/{unique_id}", get(groups::get_group))
        .route("/api/v1/macros", get(macros::list_macros))
        .route("/api/v1/macros/facets", get(macros::list_macro_facets))
        .route("/api/v1/macros/{unique_id}", get(macros::get_macro))
        .route("/api/v1/metrics", get(metrics::list_metrics))
        .route("/api/v1/metrics/facets", get(metrics::list_metric_facets))
        .route("/api/v1/metrics/{unique_id}", get(metrics::get_metric))
        .route(
            "/api/v1/saved_queries",
            get(saved_queries::list_saved_queries),
        )
        .route(
            "/api/v1/saved_queries/facets",
            get(saved_queries::list_saved_query_facets),
        )
        .route(
            "/api/v1/saved_queries/{unique_id}",
            get(saved_queries::get_saved_query),
        )
        .route("/api/v1/seeds", get(seeds::list_seeds))
        .route("/api/v1/seeds/facets", get(seeds::list_seed_facets))
        .route("/api/v1/seeds/{unique_id}", get(seeds::get_seed))
        .route(
            "/api/v1/semantic_models",
            get(semantic_models::list_semantic_models),
        )
        .route(
            "/api/v1/semantic_models/facets",
            get(semantic_models::list_semantic_model_facets),
        )
        .route(
            "/api/v1/semantic_models/{unique_id}",
            get(semantic_models::get_semantic_model),
        )
        .route("/api/v1/snapshots", get(snapshots::list_snapshots))
        .route(
            "/api/v1/snapshots/facets",
            get(snapshots::list_snapshot_facets),
        )
        .route(
            "/api/v1/snapshots/{unique_id}",
            get(snapshots::get_snapshot),
        )
        .route("/api/v1/tests", get(tests::list_tests))
        .route("/api/v1/tests/facets", get(tests::list_test_facets))
        .route("/api/v1/tests/{unique_id}", get(tests::get_test))
        .route("/api/v1/exposures", get(exposures::list_exposures))
        .route(
            "/api/v1/exposures/facets",
            get(exposures::list_exposure_facets),
        )
        .route(
            "/api/v1/exposures/{unique_id}",
            get(exposures::get_exposure),
        )
        .route("/api/v1/nodes", get(nodes::list_nodes))
        .route("/api/v1/nodes/counts", get(nodes::list_node_counts))
        .route("/api/v1/nodes/{unique_id}", get(nodes::get_node))
        .route("/api/v1/files", get(files::list_files))
        .route(
            "/api/v1/nodes/{unique_id}/lineage",
            get(lineage::get_lineage),
        )
        .route(
            "/api/v1/nodes/{unique_id}/column-lineage",
            get(column_lineage::get_column_lineage),
        )
        .route("/api/v1/search", get(search::search))
        .route("/api/v1/search/facets", get(search::search_facets))
        .route("/api/v1/analytics/events", post(analytics::post_events))
        .fallback(serve_assets)
        .with_state(state.clone());

    let bind = format!("{}:{}", args.host, args.port);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let local_addr = listener.local_addr()?;
    let url = format!("http://{local_addr}");

    eprintln!("dbt docs serve: serving from {}", state.index_dir.display());
    eprintln!("dbt docs serve: listening on {url}");
    info!(target: "dbt_docs_server", index_dir = %state.index_dir.display(), %url, "started");

    if !args.no_open {
        if let Err(err) = try_open_browser(&url) {
            eprintln!("dbt docs serve: could not open browser ({err}); visit {url} manually");
        }
    }

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// Upper bound on how long to drain in-flight requests after a shutdown
/// signal before forcing exit. Kept under the typical Kubernetes
/// `terminationGracePeriodSeconds` (30s) so we exit cleanly before SIGKILL.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(25);

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                std::future::pending::<()>().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    // TODO(META-7739): analytics events buffered in the vortex_sender worker are
    // not flushed on shutdown. Loss on Ctrl-C is acceptable (worker flushes every
    // ~500ms); if that changes, call `vortex_sender::log_proto_and_shutdown` here.
    info!(
        target: "dbt_docs_server",
        grace_secs = SHUTDOWN_GRACE.as_secs(),
        "shutdown signal received; draining in-flight requests"
    );
    eprintln!(
        "dbt docs serve: shutdown signal received; draining in-flight requests (grace {}s)",
        SHUTDOWN_GRACE.as_secs()
    );

    // Bound the drain: force exit if in-flight requests do not complete in time.
    tokio::spawn(async {
        tokio::time::sleep(SHUTDOWN_GRACE).await;
        eprintln!("dbt docs serve: drain grace period elapsed; forcing exit");
        std::process::exit(0);
    });
}

fn try_open_browser(url: &str) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).status()?;
        Ok(())
    }
    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open").arg(url).status()?;
        Ok(())
    }
    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .status()?;
        Ok(())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let _ = url;
        Err(io::Error::other("auto-open not supported on this platform"))
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod server_tests;
