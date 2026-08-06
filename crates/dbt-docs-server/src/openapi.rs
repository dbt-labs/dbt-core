//! Code-generated OpenAPI spec for the dbt-docs-server REST API.
//!
//! Scope is intentionally narrow: only `/api/v1/health` is documented today,
//! as a proof of the generated-spec pattern. Additional endpoints are
//! annotated with `#[utoipa::path]` and registered here incrementally.

use axum::Json;
use utoipa::OpenApi;

use crate::handlers::health;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "dbt-docs-server API",
        description = "REST API backing dbt docs v2, generated from the running server's route and type annotations."
    ),
    paths(health::get_health),
    components(schemas(health::HealthResponse)),
    tags((name = "health", description = "Server liveness and project-load status"))
)]
pub struct ApiDoc;

// Only called when `openapi-ui` is off; `SwaggerUi::url` serves the spec
// itself when the feature is on (see `server.rs`).
#[cfg_attr(feature = "openapi-ui", allow(dead_code))]
pub async fn get_openapi_spec() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

#[cfg(test)]
#[path = "openapi_tests.rs"]
mod openapi_tests;
