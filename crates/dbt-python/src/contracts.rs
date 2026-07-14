//! Python `#[pyclass]` wrappers for dbt's top-level artifact contracts.
//!
//! Each wraps the corresponding `dbt-schemas` Rust type so that the legacy
//! import paths resolve and `isinstance` checks pass (the `module = "..."` arg
//! sets `__module__`, which the Python shim packages mirror). Nested data is
//! exposed lazily as plain Python objects via `pythonize`; per-node `#[pyclass]`
//! promotion is intentionally out of scope for this tier.

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::Path;

use dbt_schemas::schemas::legacy_catalog::DbtCatalog as RsCatalog;
use dbt_schemas::schemas::manifest::DbtManifest as RsManifest;
use dbt_schemas::schemas::{
    FreshnessResultsArtifact as RsFreshness, RunResultsArtifact as RsRunResults,
};

/// Serialize a Rust value and hand it to Python as a plain dict/list/scalar.
fn to_py<T: Serialize>(py: Python<'_>, value: &T) -> PyResult<Py<PyAny>> {
    Ok(pythonize::pythonize(py, value)
        .map_err(|e| PyValueError::new_err(format!("pythonize: {e}")))?
        .unbind())
}

/// Read + deserialize a JSON artifact from disk.
fn read_json<T: DeserializeOwned>(path: &str) -> PyResult<T> {
    let bytes = std::fs::read(Path::new(path))
        .map_err(|e| PyIOError::new_err(format!("read {path}: {e}")))?;
    serde_json::from_slice(&bytes).map_err(|e| PyValueError::new_err(format!("parse {path}: {e}")))
}

// ------------------------------------------------------------------------------------------------
// Manifest (manifest.json) — `from dbt.contracts.graph.manifest import Manifest`
// ------------------------------------------------------------------------------------------------

#[pyclass(module = "dbt.contracts.graph.manifest", name = "Manifest")]
pub struct Manifest {
    pub(crate) inner: RsManifest,
}

impl Manifest {
    pub(crate) fn from_inner(inner: RsManifest) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl Manifest {
    // NOTE: no `read(path)` yet — the on-disk manifest.json doesn't round-trip
    // through `DbtManifest`'s derived Deserialize (a required `__other__`
    // catch-all field is dropped on serialize). The in-memory `.result` path
    // doesn't deserialize, so it's unaffected. Reading from disk is a follow-up
    // pending a dbt-schemas deserialize fix.

    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.metadata)
    }
    #[getter]
    fn nodes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.nodes)
    }
    #[getter]
    fn sources(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.sources)
    }
    #[getter]
    fn macros(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.macros)
    }
    #[getter]
    fn metrics(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.metrics)
    }
    #[getter]
    fn exposures(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.exposures)
    }
    #[getter]
    fn semantic_models(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.semantic_models)
    }
    #[getter]
    fn saved_queries(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.saved_queries)
    }
    #[getter]
    fn unit_tests(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.unit_tests)
    }
    #[getter]
    fn docs(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.docs)
    }
    #[getter]
    fn functions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.functions)
    }
    #[getter]
    fn groups(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.groups)
    }

    /// Full manifest as a plain dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner)
    }

    fn __repr__(&self) -> String {
        format!(
            "Manifest(nodes={}, sources={}, macros={})",
            self.inner.nodes.len(),
            self.inner.sources.len(),
            self.inner.macros.len()
        )
    }
}

// ------------------------------------------------------------------------------------------------
// RunResultsArtifact (run_results.json) — `from dbt.contracts.results import RunResultsArtifact`
// ------------------------------------------------------------------------------------------------

#[pyclass(module = "dbt.contracts.results", name = "RunResultsArtifact")]
pub struct RunResultsArtifact {
    pub(crate) inner: RsRunResults,
}

impl RunResultsArtifact {
    pub(crate) fn from_inner(inner: RsRunResults) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl RunResultsArtifact {
    #[staticmethod]
    fn read(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: read_json(path)?,
        })
    }

    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.metadata)
    }
    #[getter]
    fn results(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.results)
    }
    #[getter]
    fn elapsed_time(&self) -> f64 {
        self.inner.elapsed_time
    }
    #[getter]
    fn args(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.args)
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner)
    }

    fn __repr__(&self) -> String {
        format!("RunResultsArtifact(results={})", self.inner.results.len())
    }
}

// ------------------------------------------------------------------------------------------------
// CatalogArtifact (catalog.json) — `from dbt.contracts.results import CatalogArtifact`
// ------------------------------------------------------------------------------------------------

#[pyclass(module = "dbt.contracts.results", name = "CatalogArtifact")]
pub struct CatalogArtifact {
    pub(crate) inner: RsCatalog,
}

impl CatalogArtifact {
    pub(crate) fn from_inner(inner: RsCatalog) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl CatalogArtifact {
    #[staticmethod]
    fn read(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: read_json(path)?,
        })
    }

    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.metadata)
    }
    #[getter]
    fn nodes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.nodes)
    }
    #[getter]
    fn sources(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.sources)
    }
    #[getter]
    fn errors(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.errors)
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner)
    }

    fn __repr__(&self) -> String {
        format!(
            "CatalogArtifact(nodes={}, sources={})",
            self.inner.nodes.len(),
            self.inner.sources.len()
        )
    }
}

// ------------------------------------------------------------------------------------------------
// FreshnessResultsArtifact (sources.json) — `from dbt.contracts.results import FreshnessResultsArtifact`
// ------------------------------------------------------------------------------------------------

// Source freshness isn't captured into CommandExecutionResult yet, so this is
// reachable only via `read()` (importable + isinstance parity for sources.json).
#[pyclass(module = "dbt.contracts.results", name = "FreshnessResultsArtifact")]
pub struct FreshnessResultsArtifact {
    pub(crate) inner: RsFreshness,
}

#[pymethods]
impl FreshnessResultsArtifact {
    #[staticmethod]
    fn read(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: read_json(path)?,
        })
    }

    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.metadata)
    }
    #[getter]
    fn results(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner.results)
    }
    #[getter]
    fn elapsed_time(&self) -> f64 {
        self.inner.elapsed_time
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, &self.inner)
    }

    fn __repr__(&self) -> String {
        format!(
            "FreshnessResultsArtifact(results={})",
            self.inner.results.len()
        )
    }
}

/// Register all contract pyclasses on the module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Manifest>()?;
    m.add_class::<RunResultsArtifact>()?;
    m.add_class::<CatalogArtifact>()?;
    m.add_class::<FreshnessResultsArtifact>()?;
    Ok(())
}
