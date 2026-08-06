use utoipa::OpenApi as _;

use super::ApiDoc;

#[test]
fn builds_without_panicking_and_documents_health() {
    let spec = ApiDoc::openapi();
    assert_eq!(spec.paths.paths.len(), 1);
    assert!(spec.paths.paths.contains_key("/api/v1/health"));
    assert!(
        spec.components
            .as_ref()
            .expect("components present")
            .schemas
            .contains_key("HealthResponse")
    );
}
