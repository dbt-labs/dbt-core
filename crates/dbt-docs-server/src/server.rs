use std::io;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use dbt_clap_core::DocsServeArgs;
use tracing::info;

use crate::embed::serve_assets;
use crate::handlers::{capabilities, column_lineage, health, lineage, nodes, project, query};
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
    let state = Arc::new(AppState::new(index_dir, providers));
    serve(args, state).await
}

async fn serve(args: Arc<DocsServeArgs>, state: Arc<AppState>) -> io::Result<()> {
    let app = Router::new()
        .route("/api/v1/health", get(health::get_health))
        .route("/api/v1/capabilities", get(capabilities::get_capabilities))
        .route("/api/v1/project", get(project::get_project))
        .route("/api/v1/nodes", get(nodes::list_nodes))
        .route("/api/v1/nodes/{unique_id}", get(nodes::get_node))
        .route(
            "/api/v1/nodes/{unique_id}/lineage",
            get(lineage::get_lineage),
        )
        .route(
            "/api/v1/nodes/{unique_id}/column-lineage",
            get(column_lineage::get_column_lineage),
        )
        .route("/api/v1/tables", get(query::list_tables))
        .route("/api/v1/query", post(query::run_query))
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

    axum::serve(listener, app).await?;
    Ok(())
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
