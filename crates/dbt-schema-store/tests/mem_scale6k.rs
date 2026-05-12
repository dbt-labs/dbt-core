//! Memory and timing measurements for the parquet schema cache at scale_6k size.
//!
//! Requires a pre-built epoch file. Build once with:
//!   DBT_SCALE6K_TARGET=~/tmp/scale_6k/target \
//!   DBT_SCALE6K_EPOCH_DIR=/tmp/scale6k_epochs \
//!   cargo test -p dbt-schema-store --test mem_scale6k build_epoch --release -- --nocapture
//!
//! Then run measurements with:
//!   DBT_SCALE6K_EPOCH_DIR=/tmp/scale6k_epochs \
//!   cargo test -p dbt-schema-store --test mem_scale6k reload_only --release -- --nocapture
//!
//!   DBT_SCALE6K_EPOCH_DIR=/tmp/scale6k_epochs DBT_ACCESS_N=50 \
//!   cargo test -p dbt-schema-store --test mem_scale6k partial_access --release -- --nocapture

use std::{collections::HashMap, hint::black_box, path::PathBuf};

use dbt_ident::Ident;
use dbt_schema_store::{
    CanonicalFqn, SchemaStoreTrait,
    parquet_cache::ParquetSchemaCache,
    store::{SchemaStore, StoreFormat, read_cached_schema_from_parquet},
};

fn rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
        / 1024
}

fn epoch_remote_dir(epoch_dir: &std::path::Path) -> PathBuf {
    epoch_dir.join("warehouse_state/schemas_remote")
}

fn cfqn_from_uid(uid: &str) -> CanonicalFqn {
    CanonicalFqn::new(&Ident::new("db"), &Ident::new("s"), &Ident::new(uid))
}

/// Build the epoch file from legacy per-entry parquet files.
#[test]
fn build_epoch() {
    let target_dir = match std::env::var("DBT_SCALE6K_TARGET") {
        Ok(p) => PathBuf::from(p),
        Err(_) => {
            println!("[build] skipped — set DBT_SCALE6K_TARGET");
            return;
        }
    };
    let epoch_dir = match std::env::var("DBT_SCALE6K_EPOCH_DIR") {
        Ok(p) => PathBuf::from(p),
        Err(_) => {
            println!("[build] skipped — set DBT_SCALE6K_EPOCH_DIR");
            return;
        }
    };
    std::fs::create_dir_all(&epoch_dir).unwrap();

    let legacy_analyzed = target_dir.join("schemas/analyzed");
    let entries: Vec<(String, PathBuf)> = std::fs::read_dir(&legacy_analyzed)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let uid = e.file_name().to_string_lossy().to_string();
            let p = e.path().join("output.parquet");
            p.exists().then_some((uid, p))
        })
        .collect();

    println!("[build] {} legacy schemas", entries.len());

    let mut frontier: HashMap<CanonicalFqn, String> = HashMap::new();
    for (uid, _) in &entries {
        frontier.insert(cfqn_from_uid(uid), uid.clone());
    }

    let store = SchemaStore::new(
        epoch_dir.clone(),
        HashMap::new(),
        frontier,
        HashMap::new(),
        vec![],
        StoreFormat::ParquetCache,
        HashMap::new(),
        None,
    );

    let t0 = std::time::Instant::now();
    for (uid, path) in &entries {
        if let Ok((se, _)) = read_cached_schema_from_parquet(path) {
            let c = cfqn_from_uid(uid);
            let _ = store.register_schema(&c, se.original().cloned(), se.into_inner(), false);
        }
    }
    println!("[build] register {}ms", t0.elapsed().as_millis());

    let t1 = std::time::Instant::now();
    store.save(&epoch_dir).unwrap();
    let remote_size: u64 = std::fs::read_dir(epoch_remote_dir(&epoch_dir))
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0);
    println!(
        "[build] save {}ms | epoch file {:.1} MB",
        t1.elapsed().as_millis(),
        remote_size as f64 / 1_048_576.0
    );
}

/// Cold-load measurement: load all bytes, report RSS delta. No deserialization.
#[test]
fn reload_only() {
    let epoch_dir = match std::env::var("DBT_SCALE6K_EPOCH_DIR") {
        Ok(p) => PathBuf::from(p),
        Err(_) => {
            println!("[reload] skipped — set DBT_SCALE6K_EPOCH_DIR");
            return;
        }
    };
    let remote_dir = epoch_remote_dir(&epoch_dir);
    if !remote_dir.exists() {
        println!("[reload] skipped — run build_epoch first");
        return;
    }

    let baseline = rss_mb();
    let t0 = std::time::Instant::now();
    let cache = ParquetSchemaCache::load(&remote_dir, &[], true);
    let load_ms = t0.elapsed().as_millis();
    let after = rss_mb();

    println!(
        "[reload] {} entries in {}ms | RSS {} MB (+{} MB bytes-only vs baseline {} MB)",
        cache.len(),
        load_ms,
        after,
        after.saturating_sub(baseline),
        baseline
    );
}

/// Measures: what does it cost when only N out of 5844 schemas are actually accessed?
/// This is the incremental-compile / small-project scenario.
/// Compares unfiltered load vs. row-filtered load.
#[test]
fn partial_access() {
    let epoch_dir = match std::env::var("DBT_SCALE6K_EPOCH_DIR") {
        Ok(p) => PathBuf::from(p),
        Err(_) => {
            println!("[partial] skipped — set DBT_SCALE6K_EPOCH_DIR");
            return;
        }
    };
    let remote_dir = epoch_remote_dir(&epoch_dir);
    if !remote_dir.exists() {
        println!("[partial] skipped — run build_epoch first");
        return;
    }

    let access_n: usize = std::env::var("DBT_ACCESS_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    // ── Phase 1: unfiltered load (load all bytes, deser only access_n) ──────────
    let baseline = rss_mb();
    let t0 = std::time::Instant::now();
    let cache_all = ParquetSchemaCache::load(&remote_dir, &[], true);
    let load_all_ms = t0.elapsed().as_millis();
    let after_load_all = rss_mb();

    let keys: Vec<String> = cache_all
        .keys()
        .take(access_n)
        .map(|s| s.to_string())
        .collect();
    let t1 = std::time::Instant::now();
    let mut hit = 0usize;
    for k in &keys {
        if black_box(cache_all.get(k)).is_some() {
            hit += 1;
        }
    }
    let deser_ms = t1.elapsed().as_millis();
    let after_deser = rss_mb();
    drop(cache_all);

    println!(
        "[partial n={access_n} unfiltered] load_all={}ms (+{} MB) | \
         deser_{access_n}={}ms (+{} MB) | hit={hit}/5844",
        load_all_ms,
        after_load_all.saturating_sub(baseline),
        deser_ms,
        after_deser.saturating_sub(after_load_all),
    );

    // ── Phase 2: row-filtered load (only decode blobs for the N needed keys) ────
    let intervals: Vec<(String, Option<std::time::Duration>)> =
        keys.iter().map(|k| (k.clone(), None)).collect();

    let baseline2 = rss_mb();
    let t2 = std::time::Instant::now();
    let cache_filtered = ParquetSchemaCache::load(&remote_dir, &intervals, false);
    let load_filtered_ms = t2.elapsed().as_millis();
    let after_filtered = rss_mb();

    let t3 = std::time::Instant::now();
    let mut hit2 = 0usize;
    for k in &keys {
        if black_box(cache_filtered.get(k)).is_some() {
            hit2 += 1;
        }
    }
    let deser2_ms = t3.elapsed().as_millis();
    let after_deser2 = rss_mb();

    println!(
        "[partial n={access_n}   filtered] load_N  ={}ms (+{} MB) | \
         deser_{access_n}={}ms (+{} MB) | hit={hit2}/{access_n}",
        load_filtered_ms,
        after_filtered.saturating_sub(baseline2),
        deser2_ms,
        after_deser2.saturating_sub(after_filtered),
    );
}

/// Full access: deserialize every schema. Represents a cold full compile.
#[test]
fn full_access() {
    let epoch_dir = match std::env::var("DBT_SCALE6K_EPOCH_DIR") {
        Ok(p) => PathBuf::from(p),
        Err(_) => {
            println!("[full] skipped — set DBT_SCALE6K_EPOCH_DIR");
            return;
        }
    };
    let remote_dir = epoch_remote_dir(&epoch_dir);
    if !remote_dir.exists() {
        println!("[full] skipped — run build_epoch first");
        return;
    }

    let baseline = rss_mb();
    let t0 = std::time::Instant::now();
    let cache = ParquetSchemaCache::load(&remote_dir, &[], true);
    let load_ms = t0.elapsed().as_millis();
    let after_load = rss_mb();

    let keys: Vec<String> = cache.keys().map(|s| s.to_string()).collect();
    let t1 = std::time::Instant::now();
    let mut hit = 0usize;
    for k in &keys {
        if black_box(cache.get(k)).is_some() {
            hit += 1;
        }
    }
    let deser_ms = t1.elapsed().as_millis();
    let after_deser = rss_mb();

    println!(
        "[full n={}] load={}ms (+{} MB bytes) | deser_all={}ms (+{} MB Arrow schemas) | hit={hit}",
        cache.len(),
        load_ms,
        after_load.saturating_sub(baseline),
        deser_ms,
        after_deser.saturating_sub(after_load),
    );
}
