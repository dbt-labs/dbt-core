//! Filesystem-backed implementation of the schema store.
//!
//! The `dbt-schema-store` persists canonical schemas and, optionally, materialized
//! data for each dbt node.  This module contains the production implementation,
//! which understands the different entry classes (analyzed, frontier, deferred,
//! external) and maps them to their respective on-disk namespaces.

use crate::{
    CanonicalFqn, CanonicalIdentifier, DataStoreTrait, SchemaEntry, SchemaStoreResult,
    SchemaStoreTrait,
};
use arrow::{
    array::RecordBatch, ipc::reader::StreamReader as ArrowIpcStreamReader,
    ipc::writer::StreamWriter as ArrowIpcStreamWriter,
};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use bimap::BiMap;
use futures::StreamExt;
use parquet::arrow::{
    ArrowWriter as ParquetArrowWriter,
    arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
};
use scc::{HashMap as SccHashMap, HashSet as SccHashSet};
use std::{
    collections::{BTreeSet, HashMap},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime},
};

type UniqueId = String;
type Timestamp = u128;

const ANALYZED_DIR_NAME: &str = "analyzed";
const REMOTE_DIR_NAME: &str = "sourced_remote";
const INTERNAL_DIR_NAME: &str = "internal";
const DEFERRED_DIR_NAME: &str = "deferred";
const EXTERNAL_DIR_NAME: &str = "external";
const DATA_DIR_NAME: &str = "data";
const SCHEMA_DIR_NAME: &str = "schemas";
const DBT_ORIGINAL_SCHEMA_KEY: &str = "DBT:original_schema";

/// Lookup key representing the origin of a schema entry.
///
/// The entry type encodes the guarantees required by the schema store:
/// * [`LookupEntry::Selected`] – models analyzed during the current invocation.
/// * [`LookupEntry::Frontier`] – sources, frontier nodes, and cross-project
///   references whose schemas come from the remote warehouse.
/// * [`LookupEntry::Deferred`] – nodes deferred to another manifest; they also
///   hydrate from remote storage.
/// * [`LookupEntry::External`] – tables outside of the project graph, discovered
///   lazily as DataFusion resolves them.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum LookupEntry {
    Selected(UniqueId),
    Frontier(CanonicalFqn),
    Deferred(CanonicalFqn),
    External(CanonicalFqn),
}

impl std::fmt::Display for LookupEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupEntry::Selected(unique_id) => write!(f, "Selected({})", unique_id),
            LookupEntry::Frontier(cfqn) => write!(f, "Frontier({})", cfqn),
            LookupEntry::Deferred(cfqn) => write!(f, "Deferred({})", cfqn),
            LookupEntry::External(cfqn) => write!(f, "External({})", cfqn),
        }
    }
}

/// Lazily materialized schema cached in memory.
///
/// We retain the filesystem timestamp to enable future invalidation strategies.
#[derive(Debug, Clone)]
struct SchemaEntryWrapper {
    schema_entry: OnceLock<SchemaEntry>,
    #[allow(dead_code)]
    timestamp: u128,
}

impl SchemaEntryWrapper {
    pub fn empty(timestamp: u128) -> Self {
        Self {
            schema_entry: OnceLock::new(),
            timestamp,
        }
    }

    pub fn new(schema_entry: SchemaEntry, timestamp: u128) -> Self {
        let once_lock = OnceLock::new();
        once_lock
            .set(schema_entry)
            .expect("OnceLock should not be already set");
        Self {
            schema_entry: once_lock,
            timestamp,
        }
    }
}

/// Interior mutable state shared by [`SchemaStore`].
#[derive(Debug)]
struct SchemaStoreState {
    store_dir: PathBuf,
    store_fmt: StoreFormat,
    cached_entries: SccHashMap<LookupEntry, Arc<SchemaEntryWrapper>>,
}

impl SchemaStoreState {
    /// Pre-populates the state with any schemas already persisted on disk.
    pub fn init(target_dir: &Path, cache_fmt: StoreFormat, entries: &[LookupEntry]) -> Self {
        let store_dir = target_dir.join(SCHEMA_DIR_NAME);
        // For each selected and frontier, check if a cache entry exists or not
        let cached_schemas = SccHashMap::new();
        for entry in entries {
            // Do not register selected entries, these will be registered during compile
            if matches!(entry, LookupEntry::Selected(..)) {
                continue;
            }
            Self::try_register_entry_inner(&store_dir, &cached_schemas, entry);
        }
        Self {
            store_dir,
            store_fmt: cache_fmt,
            cached_entries: cached_schemas,
        }
    }

    /// Returns `true` if the requested lookup entry already exists on disk.
    pub fn exists(&self, entry: &LookupEntry) -> bool {
        self.cached_entries.contains_sync(entry)
    }

    /// Async equivalent of [`SchemaStoreState::exists`].
    async fn exists_async(&self, entry: &LookupEntry) -> bool {
        self.cached_entries.contains_async(entry).await
    }

    /// Ensures the given lookup entry is tracked by the cache without eagerly
    /// hydrating the underlying schema.
    pub fn try_register_entry(&self, entry: &LookupEntry) -> Option<Arc<SchemaEntryWrapper>> {
        Self::try_register_entry_inner(&self.store_dir, &self.cached_entries, entry)
    }

    /// Retrieves the schema from the cache, hydrating it from disk on first use.
    pub fn get_schema(&self, entry: &LookupEntry) -> Option<SchemaEntry> {
        self.cached_entries
            .read_sync(entry, |_, v| Arc::clone(v))
            .and_then(|schema| self.try_get_or_init_schema(schema, entry))
    }

    /// Async variant of [`SchemaStoreState::get_schema`].
    pub async fn get_schema_async(&self, entry: &LookupEntry) -> Option<SchemaEntry> {
        self.cached_entries
            .read_async(entry, |_, v| Arc::clone(v))
            .await
            .and_then(|schema| self.try_get_or_init_schema(schema, entry))
    }

    /// Writes the canonical schema to disk and updates the cache.
    pub fn register_schema(
        &self,
        entry: &LookupEntry,
        original_schema: Option<SchemaRef>,
        schema: SchemaRef,
        overwrite: bool,
    ) -> SchemaStoreResult<SchemaEntry> {
        if !overwrite && self.exists(entry) {
            return Ok(self.get_schema(entry).expect("Entry should exist"));
        }
        let path = Self::resolve_entry_path(&self.store_dir, entry);
        std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| {
            ArrowError::IoError(format!("Failed to create directory: {}", path.display()), e)
        })?;
        let (schema_entry, timestamp) = self.write_cached_schema(&path, original_schema, schema)?;
        let schema_entry_wrapper =
            Arc::new(SchemaEntryWrapper::new(schema_entry.clone(), timestamp));
        let _ = self
            .cached_entries
            .upsert_sync(entry.clone(), schema_entry_wrapper);
        Ok(schema_entry)
    }

    /// Hydrates the schema on-demand and caches it in the underlying [`OnceLock`].
    fn try_get_or_init_schema(
        &self,
        schema_entry_wrapper: Arc<SchemaEntryWrapper>,
        entry: &LookupEntry,
    ) -> Option<SchemaEntry> {
        match catch_unwind(AssertUnwindSafe(|| {
            if let Some(schema_entry) = schema_entry_wrapper.schema_entry.get() {
                schema_entry.clone()
            } else {
                let path = Self::resolve_entry_path(&self.store_dir, entry);
                let (schema_entry, _) = self
                    .read_cached_schema(&path)
                    .expect("Failed to read cached schema");
                let _ = schema_entry_wrapper.schema_entry.set(schema_entry.clone());
                schema_entry
            }
        })) {
            Ok(schema_entry) => Some(schema_entry),
            Err(_) => {
                debug_assert!(false, "Failed to read cached schema");
                None
            }
        }
    }

    /// Reads and deserializes the schema persisted for the lookup entry.
    fn read_cached_schema(&self, table_path: &Path) -> SchemaStoreResult<(SchemaEntry, Timestamp)> {
        match self.store_fmt {
            StoreFormat::ArrowIpc => unimplemented!(),
            StoreFormat::Parquet => read_cached_schema_from_parquet(table_path),
            StoreFormat::Yaml => unimplemented!(),
        }
    }

    /// Persists the schema for the lookup entry using the configured store format.
    fn write_cached_schema(
        &self,
        path: &Path,
        original_schema: Option<SchemaRef>,
        schema: SchemaRef,
    ) -> SchemaStoreResult<(SchemaEntry, Timestamp)> {
        match self.store_fmt {
            StoreFormat::ArrowIpc => unimplemented!(),
            StoreFormat::Parquet => {
                persist_schema_as_parquet_file(original_schema, schema, true, path)
            }
            StoreFormat::Yaml => unimplemented!(),
        }
    }

    /// Builds the canonical filesystem path for the given lookup entry.
    ///
    /// Selected nodes live under `schemas/analyzed/<unique_id>`, while frontier,
    /// deferred, and external nodes use the `schemas/sourced_remote/...`
    /// hierarchy described in the crate-level documentation.  Data paths swap
    /// `schemas/` for `data/`.
    fn resolve_entry_path(cache_dir: &Path, entry: &LookupEntry) -> PathBuf {
        match entry {
            LookupEntry::Frontier(cfqn) => cache_dir
                .join(REMOTE_DIR_NAME)
                .join(INTERNAL_DIR_NAME)
                .join(cfqn.catalog())
                .join(cfqn.schema())
                .join(cfqn.table())
                .join("output.parquet"),
            LookupEntry::Selected(unique_id) => cache_dir
                .join(ANALYZED_DIR_NAME)
                .join(unique_id)
                .join("output.parquet"),
            LookupEntry::Deferred(cfqn) => cache_dir
                .join(REMOTE_DIR_NAME)
                .join(DEFERRED_DIR_NAME)
                .join(cfqn.catalog())
                .join(cfqn.schema())
                .join(cfqn.table())
                .join("output.parquet"),
            LookupEntry::External(cfqn) => cache_dir
                .join(REMOTE_DIR_NAME)
                .join(EXTERNAL_DIR_NAME)
                .join(cfqn.catalog())
                .join(cfqn.schema())
                .join(cfqn.table())
                .join("output.parquet"),
        }
    }

    /// Inserts the lookup entry into the cache if a persisted schema already exists.
    fn try_register_entry_inner(
        cache_dir: &Path,
        cached_schemas: &SccHashMap<LookupEntry, Arc<SchemaEntryWrapper>>,
        entry: &LookupEntry,
    ) -> Option<Arc<SchemaEntryWrapper>> {
        let path = Self::resolve_entry_path(cache_dir, entry);
        let timestamp = get_timestamp(&path);
        if let Some(timestamp) = timestamp {
            let schema_entry_wrapper = Arc::new(SchemaEntryWrapper::empty(timestamp));
            let _ = cached_schemas.upsert_sync(entry.clone(), schema_entry_wrapper.clone());
            Some(schema_entry_wrapper)
        } else {
            None
        }
    }
}

/// Supported on-disk encodings for schemas and data.
#[derive(Debug)]
pub enum StoreFormat {
    ArrowIpc,
    Parquet,
    Yaml,
}

/// Primary filesystem-backed implementation of [`SchemaStoreTrait`].
#[derive(Debug)]
pub struct SchemaStore {
    selected: BiMap<CanonicalFqn, UniqueId>,
    frontier: BiMap<CanonicalFqn, UniqueId>,
    deferred: OnceLock<BiMap<CanonicalFqn, UniqueId>>,
    external: SccHashSet<CanonicalFqn>,
    state: SchemaStoreState,
}

impl SchemaStore {
    // TODO: We can create an initialize which will check the filesystem and automatically refresh the cache if needed
    /// Creates a new filesystem-backed schema store rooted at `cache_dir`.
    pub fn new(
        cache_dir: PathBuf,
        selected: HashMap<CanonicalFqn, UniqueId>,
        frontier: HashMap<CanonicalFqn, UniqueId>,
        cache_fmt: StoreFormat,
    ) -> Self {
        let entries = selected
            .values()
            .map(|cfqn| LookupEntry::Selected(cfqn.clone()))
            .chain(
                frontier
                    .keys()
                    .map(|cfqn| LookupEntry::Frontier(cfqn.clone())),
            )
            .collect::<Vec<_>>();
        let state = SchemaStoreState::init(&cache_dir, cache_fmt, &entries);
        Self {
            selected: selected.into_iter().collect(),
            frontier: frontier.into_iter().collect(),
            deferred: OnceLock::new(),
            external: SccHashSet::new(),
            state,
        }
    }

    /// Finds the [`LookupEntry`] corresponding to a canonical FQN.
    pub fn resolve_lookup_entry_by_cfqn(&self, cfqn: &CanonicalFqn) -> Option<LookupEntry> {
        if let Some(unique_id) = self.selected.get_by_left(cfqn) {
            Some(LookupEntry::Selected(unique_id.clone()))
        } else if self.frontier.contains_left(cfqn) {
            Some(LookupEntry::Frontier(cfqn.clone()))
        } else if self
            .deferred
            .get()
            .and_then(|d| d.get_by_left(cfqn))
            .is_some()
        {
            Some(LookupEntry::Deferred(cfqn.clone()))
        } else if self.external.contains_sync(cfqn) {
            Some(LookupEntry::External(cfqn.clone()))
        } else {
            None
        }
    }

    /// Finds the [`LookupEntry`] corresponding to a dbt `unique_id`.
    pub fn resolve_lookup_entry_by_unique_id(&self, unique_id: &str) -> Option<LookupEntry> {
        if self.selected.contains_right(unique_id) {
            Some(LookupEntry::Selected(unique_id.to_string()))
        } else if let Some(cfqn) = self.frontier.get_by_right(unique_id) {
            Some(LookupEntry::Frontier(cfqn.clone()))
        } else if self
            .deferred
            .get()
            .and_then(|d| d.get_by_right(unique_id))
            .is_some()
        {
            debug_assert!(
                false,
                "Deferred entry should be found in either selected or frontier"
            );
            None
        } else {
            None
        }
    }

    /// Registers deferred nodes whose schemas must be sourced from remote storage.
    pub fn set_deferred(&self, deferred: HashMap<CanonicalFqn, UniqueId>) -> bool {
        let canonical_fqns = deferred.keys().cloned().collect::<Vec<_>>();
        if self.deferred.set(deferred.into_iter().collect()).is_ok() {
            canonical_fqns.into_iter().for_each(|cfqn| {
                self.state.try_register_entry(&LookupEntry::Deferred(cfqn));
            });
            true
        } else {
            false
        }
    }

    fn visit_cfqn<F>(&self, mut f: F)
    where
        F: FnMut(&CanonicalFqn),
    {
        for (cfqn, _) in self.selected.iter() {
            f(cfqn);
        }
        for (cfqn, _) in self.frontier.iter() {
            f(cfqn);
        }
        if let Some(deferred) = self.deferred.get() {
            for (cfqn, _) in deferred.iter() {
                f(cfqn);
            }
        }
        self.external.iter_sync(|cfqn| {
            f(cfqn);
            true
        });
    }
}

#[async_trait::async_trait]
impl SchemaStoreTrait for SchemaStore {
    fn exists(&self, cfqn: &CanonicalFqn) -> bool {
        self.resolve_lookup_entry_by_cfqn(cfqn)
            .is_some_and(|entry| self.state.exists(&entry))
    }

    async fn exists_async(&self, cfqn: &CanonicalFqn) -> bool {
        if let Some(entry) = self.resolve_lookup_entry_by_cfqn(cfqn) {
            self.state.exists_async(&entry).await
        } else {
            false
        }
    }

    fn exists_by_unique_id(&self, unique_id: &str) -> bool {
        self.resolve_lookup_entry_by_unique_id(unique_id)
            .is_some_and(|entry| self.state.exists(&entry))
    }

    fn get_schema(&self, cfqn: &CanonicalFqn) -> Option<SchemaEntry> {
        // return None if the entry is not found
        let entry = self.resolve_lookup_entry_by_cfqn(cfqn)?;
        self.state.get_schema(&entry)
    }

    async fn get_schema_async(&self, cfqn: &CanonicalFqn) -> Option<SchemaEntry> {
        let entry = self.resolve_lookup_entry_by_cfqn(cfqn)?;
        self.state.get_schema_async(&entry).await
    }

    fn get_schema_by_unique_id(&self, unique_id: &str) -> Option<SchemaEntry> {
        // Validate that unique_id is in either selected or frontier
        let entry = self.resolve_lookup_entry_by_unique_id(unique_id)?;
        self.state.get_schema(&entry)
    }

    async fn get_schema_by_unique_id_async(&self, unique_id: &str) -> Option<SchemaEntry> {
        let entry = self.resolve_lookup_entry_by_unique_id(unique_id)?;
        self.state.get_schema_async(&entry).await
    }

    fn register_schema(
        &self,
        cfqn: &CanonicalFqn,
        original_schema: Option<SchemaRef>,
        schema: SchemaRef,
        overwrite: bool,
    ) -> SchemaStoreResult<SchemaEntry> {
        let entry = if let Some(entry) = self.resolve_lookup_entry_by_cfqn(cfqn) {
            // Either selected, frontier, deferred, or already registered external
            entry
        } else {
            // Must be external
            LookupEntry::External(cfqn.clone())
        };
        let schema = self
            .state
            .register_schema(&entry, original_schema, schema, overwrite)?;
        // For external entries, we need to register the schema in the external set
        if let LookupEntry::External(cfqn) = entry {
            let _ = self.external.insert_sync(cfqn);
        }
        Ok(schema)
    }

    fn catalog_names(&self) -> Vec<CanonicalIdentifier> {
        let mut catalogs = BTreeSet::new();
        self.visit_cfqn(|cfqn| {
            catalogs.insert(cfqn.catalog().clone());
        });
        catalogs.into_iter().collect()
    }

    fn schema_names(&self, catalog: &CanonicalIdentifier) -> Vec<CanonicalIdentifier> {
        let mut schemas = BTreeSet::new();
        self.visit_cfqn(|cfqn| {
            if cfqn.catalog() == catalog {
                schemas.insert(cfqn.schema().clone());
            }
        });
        schemas.into_iter().collect()
    }

    fn table_names(
        &self,
        catalog: &CanonicalIdentifier,
        schema: &CanonicalIdentifier,
    ) -> Vec<CanonicalIdentifier> {
        let mut tables = BTreeSet::new();
        self.visit_cfqn(|cfqn| {
            if cfqn.catalog() == catalog && cfqn.schema() == schema {
                tables.insert(cfqn.table().clone());
            }
        });
        tables.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct DataStore {
    store_dir: PathBuf,
    store_fmt: StoreFormat,
}

impl DataStore {
    pub fn new(target_dir: PathBuf, store_fmt: StoreFormat) -> Self {
        let store_dir = target_dir.join(DATA_DIR_NAME);
        Self {
            store_dir,
            store_fmt,
        }
    }
}

#[async_trait::async_trait]
impl DataStoreTrait for DataStore {
    fn persist_data(
        &self,
        cfqn: &CanonicalFqn,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> SchemaStoreResult<usize> {
        let path = self.get_path_to_data(cfqn);
        std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| {
            ArrowError::IoError(format!("Failed to create directory: {}", path.display()), e)
        })?;
        match self.store_fmt {
            StoreFormat::ArrowIpc => unimplemented!(),
            StoreFormat::Parquet => persist_data_as_parquet_file(schema, true, batches, &path),
            StoreFormat::Yaml => unimplemented!(),
        }
    }

    async fn persist_data_async(
        &self,
        cfqn: &CanonicalFqn,
        schema: SchemaRef,
        stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = SchemaStoreResult<RecordBatch>> + Send + 'static>,
        >,
    ) -> SchemaStoreResult<usize> {
        let path = self.get_path_to_data(cfqn);
        std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| {
            ArrowError::IoError(format!("Failed to create directory: {}", path.display()), e)
        })?;
        match self.store_fmt {
            StoreFormat::ArrowIpc => unimplemented!(),
            StoreFormat::Parquet => {
                persist_data_as_parquet_file_async(schema, true, stream, &path).await
            }
            StoreFormat::Yaml => unimplemented!(),
        }
    }

    fn get_path_to_data(&self, cfqn: &CanonicalFqn) -> PathBuf {
        // XXX: Normalize to lowercase to ensure case-insensitive lookups work on
        // case-sensitive filesystems. Using file paths to encode case sensitivity is volatile
        self.store_dir
            .join(cfqn.catalog().to_ascii_lowercase())
            .join(cfqn.schema().to_ascii_lowercase())
            .join(cfqn.table().to_ascii_lowercase())
            .join("output.parquet")
    }
}

/// Deserialize an Arrow schema from Arrow IPC format.
fn deserialize_arrow_schema(bytes: &[u8]) -> SchemaStoreResult<SchemaRef> {
    let projection = None;
    ArrowIpcStreamReader::try_new(bytes, projection).map(|r| r.schema())
}

/// Serialize an Arrow schema to Arrow IPC format.
fn serialize_arrow_schema(schema: &SchemaRef) -> SchemaStoreResult<Vec<u8>> {
    let mut buf = Vec::<u8>::new();
    ArrowIpcStreamWriter::try_new(&mut buf, schema.as_ref()).and_then(|mut w| w.finish())?; // no data, just the schema
    Ok(buf)
}

/// Read a cached schema from a Parquet file.
fn read_cached_schema_from_parquet(
    table_path: &Path,
) -> SchemaStoreResult<(SchemaEntry, Timestamp)> {
    let file = std::fs::File::open(table_path).map_err(|e| {
        ArrowError::IoError(format!("Failed to open file: {}", table_path.display()), e)
    })?;
    // Use options that preserve Arrow metadata
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(false);
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let arrow_schema = reader_builder.schema();
    let original_schema = arrow_schema
        .metadata()
        .get(DBT_ORIGINAL_SCHEMA_KEY)
        .and_then(|base64_encoded| {
            use base64::Engine as _;
            use base64::prelude::BASE64_STANDARD as BASE64_ENGINE;
            let res = BASE64_ENGINE.decode(base64_encoded);
            debug_assert!(res.is_ok());
            let serialized = res.ok();

            serialized.map(|bytes| deserialize_arrow_schema(&bytes))
        })
        .transpose()?;

    let arrow_schema = if arrow_schema
        .metadata()
        .contains_key(DBT_ORIGINAL_SCHEMA_KEY)
    {
        // Remove the original schema metadata key to avoid confusion
        let mut metadata = arrow_schema.metadata().clone();
        metadata.remove(DBT_ORIGINAL_SCHEMA_KEY);
        Arc::new(Schema::new_with_metadata(
            arrow_schema.fields().clone(),
            metadata,
        ))
    } else {
        Arc::clone(arrow_schema)
    };
    Ok((
        SchemaEntry::from_sdf_arrow_schema(original_schema, arrow_schema),
        get_timestamp(table_path).expect("Failed to get timestamp for parquet file"),
    ))
}

fn make_parquet_writer(
    schema: SchemaRef,
    delete_on_error: bool,
    output_path: &Path,
) -> SchemaStoreResult<parquet::arrow::ArrowWriter<std::fs::File>> {
    let parquet_file = std::fs::File::create(output_path).map_err(|e| {
        ArrowError::IoError(
            format!("Failed to create file: {}", output_path.display()),
            e,
        )
    })?;
    match ParquetArrowWriter::try_new(parquet_file, schema, None) {
        Ok(writer) => Ok(writer),
        Err(e) => {
            if delete_on_error {
                // Delete the empty file - writer creation failed
                std::fs::remove_file(output_path).map_err(|e| {
                    ArrowError::IoError(
                        format!("Failed to remove file: {}", output_path.display()),
                        e,
                    )
                })?;
            }
            Err(ArrowError::ParquetError(format!(
                "Failed to create ParquetArrowWriter: {}",
                e
            )))
        }
    }
}

/// Persists the given schema as a Parquet file at the specified path.
///
/// If `batches` is provided, they will be written to the Parquet file as well
/// and the number of rows written will be returned.
///
/// PRE-CONDITION: The parent directory of `schema_path` must already exist.
fn persist_schema_as_parquet_file(
    original_schema: Option<SchemaRef>,
    schema: SchemaRef,
    delete_on_error: bool,
    output_path: &Path,
) -> SchemaStoreResult<(SchemaEntry, Timestamp)> {
    // Include the original schema in the metadata if provided. It's serialized as Arrow IPC
    // format and base64-encoded to ensure safe storage in the Parquet schema metadata.
    let sdf_schema = original_schema
        .as_ref()
        .map(serialize_arrow_schema)
        .transpose()?
        .map_or_else(
            || Arc::clone(&schema),
            |serialized| {
                use base64::Engine as _;
                use base64::prelude::BASE64_STANDARD as BASE64_ENGINE;
                let base64_encoded_schema = BASE64_ENGINE.encode(&serialized);

                let mut metadata = schema.metadata().clone();
                metadata.insert(DBT_ORIGINAL_SCHEMA_KEY.to_string(), base64_encoded_schema);
                let sdf_schema = Schema::new_with_metadata(schema.fields().clone(), metadata);
                Arc::new(sdf_schema)
            },
        );
    let parquet_writer = make_parquet_writer(sdf_schema, delete_on_error, output_path)?;
    parquet_writer.close().map_err(|e| {
        ArrowError::ParquetError(format!(
            "Failed to close ParquetArrowWriter at {}: {}",
            output_path.display(),
            e,
        ))
    })?;
    Ok((
        SchemaEntry::from_sdf_arrow_schema(original_schema, schema),
        get_timestamp(output_path).expect("Failed to get timestamp for parquet file"),
    ))
}

/// Writes the provided record batches to disk using the canonical schema.
fn persist_data_as_parquet_file(
    schema: SchemaRef,
    delete_on_error: bool,
    batches: Vec<RecordBatch>,
    output_path: &Path,
) -> SchemaStoreResult<usize> {
    let mut parquet_writer = make_parquet_writer(schema, delete_on_error, output_path)?;
    let mut num_rows = 0;
    for batch in batches {
        num_rows += batch.num_rows();
        parquet_writer.write(&batch)?;
    }
    parquet_writer.close().map_err(|e| {
        ArrowError::ParquetError(format!(
            "Failed to close ParquetArrowWriter at {}: {}",
            output_path.display(),
            e,
        ))
    })?;
    Ok(num_rows)
}

/// Async variant of [`persist_data_as_parquet_file`].
async fn persist_data_as_parquet_file_async(
    schema: SchemaRef,
    delete_on_error: bool,
    mut stream: std::pin::Pin<
        Box<dyn futures::Stream<Item = SchemaStoreResult<RecordBatch>> + Send + 'static>,
    >,
    output_path: &Path,
) -> SchemaStoreResult<usize> {
    let mut parquet_writer = make_parquet_writer(schema, delete_on_error, output_path)?;
    let mut num_rows = 0;
    while let Some(res) = stream.next().await {
        let batch = res?;
        num_rows += batch.num_rows();
        parquet_writer.write(&batch)?;
    }
    parquet_writer.close().map_err(|e| {
        ArrowError::ParquetError(format!(
            "Failed to close ParquetArrowWriter at {}: {}",
            output_path.display(),
            e,
        ))
    })?;
    Ok(num_rows)
}

fn get_timestamp(path: &Path) -> Option<u128> {
    std::fs::metadata(path)
        .map(|m| {
            m.modified()
                .unwrap_or_else(|_| SystemTime::now())
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_millis()
        })
        .ok()
}
