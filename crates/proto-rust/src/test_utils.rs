//! Test utilities for working with generated protobuf metadata.
//!
//! This module is compiled in this crate’s tests and in dependents that enable
//! the `test-utils` feature. Keep helpers broadly useful and focused on testing.

/// Return fully-qualified names (e.g. `v1.public.events.fusion.log.LogMessage`)
/// for all top-level messages across all packages.
///
/// Notes:
/// - Uses the pre-generated `dbtlabs_proto_public.bin` emitted by `xtask protogen`.
/// - Only considers top-level messages (nested message types are ignored).
pub fn all_message_full_names() -> Vec<String> {
    // FileDescriptorSet generated in `src/gen/dbtlabs_proto_public.bin` alongside rust code.
    // This is a stable artifact that doesn't affect production code paths.
    let bytes = include_bytes!("gen/dbtlabs_proto_public.bin");
    let fds = match prost_types::FileDescriptorSet::decode(bytes.as_ref()) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    for file in fds.file.iter() {
        let package = file.package.as_deref().unwrap_or("");
        // Only enumerate top-level messages in the file (ignore nested types).
        for m in file.message_type.iter() {
            if let Some(name) = m.name.as_deref() {
                out.push(format!("{package}.{name}"));
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

// Prost types needed for decoding the descriptor set
use prost::Message as _;
use prost_types as _;
