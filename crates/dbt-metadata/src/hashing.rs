use std::{collections::HashMap, path::Path, vec};

use dbt_common::hashing::code_hash;
use dbt_common::{FsResult, io_args::IoArgs, stdfs};
use dbt_schemas::state::{DbtAsset, DbtState};

use crate::file_registry::CompleteStateWithKind;
use crate::parquet_node::InputFile;

pub fn input_file_hashes(arg: &IoArgs, dbt_state: &DbtState) -> FsResult<HashMap<String, String>> {
    let mut hashes: HashMap<String, String> = HashMap::new();
    for package in &dbt_state.packages {
        hash_assets(arg, &mut hashes, &package.macro_files)?;
        hash_assets(arg, &mut hashes, &package.model_sql_files)?;
        hash_assets(arg, &mut hashes, &package.test_files)?;
        hash_assets(arg, &mut hashes, &package.seed_files)?;
        hash_assets(arg, &mut hashes, &package.docs_files)?;
        hash_assets(arg, &mut hashes, &package.snapshot_files)?;
        hash_assets(arg, &mut hashes, &package.analysis_files)?;
        hash_assets(arg, &mut hashes, &package.dbt_properties)?;
    }
    Ok(hashes)
}

pub fn hash_assets(
    io: &IoArgs,
    hashes: &mut HashMap<String, String>,
    assets: &Vec<DbtAsset>,
) -> FsResult<()> {
    for asset in assets {
        let absolute_path = asset.base_path.join(&asset.path);
        let relative_path = absolute_path
            .strip_prefix(&io.in_dir)
            .unwrap_or(&absolute_path)
            .to_owned();
        let code = stdfs::read_to_string(absolute_path)?;
        hashes.insert(relative_path.display().to_string(), code_hash(&code));
    }
    Ok(())
}

pub fn file_suffix(path: &Path) -> String {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            if ext == "yml" {
                "yaml".to_string()
            } else {
                ext
            }
        })
        .unwrap_or_default()
}

pub fn add_input_file_if_exists(
    io: &IoArgs,
    cas: &mut HashMap<String, String>,
    rel_path: &Path,
    input_files: &mut Vec<InputFile>,
    registry: &CompleteStateWithKind,
) -> FsResult<()> {
    if rel_path.as_os_str().is_empty() || io.is_generated_file(rel_path) {
        return Ok(());
    }

    let absolute_path = io.in_dir.join(rel_path);
    if !stdfs::exists(&absolute_path)? {
        return Ok(());
    }

    // Get the input kind from the registry
    let path_str = rel_path.to_string_lossy();
    let input_kind = match registry.get_kind(&path_str) {
        Some(kind) => kind,
        None => {
            eprintln!(
                "Warning: File '{path_str}' not found in FileKindRegistry. This is likely a bug in the file registration process."
            );
            // Use a default input kind or skip this file
            return Ok(());
        }
    };

    let code = stdfs::read_to_string(&absolute_path)?;
    let hash = code_hash(&code);

    cas.insert(hash.clone(), code);
    input_files.push(InputFile {
        path: rel_path.display().to_string(),
        file_cas_hash: hash,
        input_kind,
    });

    Ok(())
}

pub fn add_session_input_files(
    io: &IoArgs,
    cas: &mut HashMap<String, String>,
    registry: &CompleteStateWithKind,
) -> FsResult<Vec<InputFile>> {
    let mut input_files = vec![];

    // Get session files from registry
    for path in registry.get_session_files() {
        // Skip packages.yml if dependencies.yml was already found
        if path == "packages.yml"
            && input_files
                .iter()
                .any(|f: &InputFile| f.path.ends_with("dependencies.yml"))
        {
            continue;
        }

        add_input_file_if_exists(io, cas, Path::new(path), &mut input_files, registry)?;
    }

    Ok(input_files)
}
